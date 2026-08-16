"""
Coinfish Journal - tastytrade transaction-history pairing engine.

Unlike the IBKR execution feed (see pairing.py), tastytrade's exported
transaction CSV gives us exact strike/expiry/call-or-put per leg, an
explicit Action (BUY_TO_OPEN / SELL_TO_OPEN / BUY_TO_CLOSE / SELL_TO_CLOSE),
and an Order # that groups every leg of a single spread order together. So
we don't need to infer open-vs-close from a realized_pnl field the way the
IBKR pairing does - tastytrade tells us directly.

The one wrinkle: a multi-leg position can close across MORE THAN ONE event.
E.g. an iron condor's put side gets bought back in one order, while the call
side expires worthless in a separate "Receive Deliver / Expiration" event
with no Order #. So matching happens at the individual LEG level (keyed by
underlying + strike + expiry + right), then the matched leg-pairs are
re-grouped by their *opening* Order # to reconstruct the trade the way a
trader thinks about it - one row per strategy, even if it closed piecemeal.

QUANTITY-AWARE MATCHING (fixed 2026-08-16): a single order can fill across
multiple partial executions, each its own CSV row at the same key (e.g. HAL
2026-03-03, order 443714714, filled as two separate 1-lot executions per
leg instead of one 2-lot execution). The open side and close side don't
have to split the same way - HAL's two closes were each a single 2-lot row.
The original version matched leg RECORDS 1-for-1 regardless of quantity,
which left one of the two 1-lot opens stranded as a phantom "still open"
position even though the position was fully closed 2026-03-25. Matching is
now size-aware (same FIFO-with-remaining-quantity pattern pairing.py already
uses for IBKR), splitting/prorating value, commission, and fees by the
fraction of a row's quantity actually consumed by each match.

"Symbol Change" and "Reverse Split" rows are corporate-action bookkeeping
(e.g. HON1/HON2 renames seen mid-2026) - each one nets to exactly zero cash
and has no Order #, so they're dropped entirely rather than handled as real
trades. The strike/expiry/right-based leg key already survives a raw symbol
rename, so nothing is lost by skipping them.
"""
import csv
import hashlib
from collections import defaultdict

from pairing import strategy_label


def parse_money(s):
    if s is None:
        return 0.0
    s = s.strip()
    if s in ("", "--"):
        return 0.0
    return float(s.replace(",", ""))


def load_rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def parse_legs(rows):
    legs = []
    for r in rows:
        rtype = r.get("Type", "")
        subtype = r.get("Sub Type", "")
        if rtype == "Money Movement":
            continue
        if rtype == "Receive Deliver" and subtype in ("Symbol Change", "Reverse Split"):
            continue
        if r.get("Instrument Type") != "Equity Option":
            continue

        action = r.get("Action", "")
        is_open = action.endswith("_TO_OPEN")
        is_close = action.endswith("_TO_CLOSE")
        if not (is_open or is_close):
            continue

        key = (r["Underlying Symbol"], r["Strike Price"], r["Expiration Date"], r["Call or Put"])
        legs.append({
            "date": r["Date"],
            "underlying": r["Underlying Symbol"],
            "key": key,
            "is_open": is_open,
            "value": parse_money(r.get("Value")),
            "commission": parse_money(r.get("Commissions")),
            "fees": parse_money(r.get("Fees")),
            "quantity": int(float(r["Quantity"])) if r.get("Quantity") else 0,
            "order_id": r.get("Order #") or None,
        })
    legs.sort(key=lambda l: l["date"])
    return legs


def match_legs(legs):
    """FIFO-match open legs to close legs sharing the same (underlying,
    strike, expiry, right) key, quantity-aware: a row's quantity is tracked
    as "remaining" and can be split across multiple matches on either side,
    so partial-fill records (same order, multiple executions) reconcile
    correctly against a close that settles in a different split. Returns a
    list of (open_leg, close_leg, qty) triples - qty is the amount actually
    matched by that pair, which may be less than either leg's full row
    quantity, and the caller must prorate value/commission/fees by qty /
    leg["quantity"] rather than assuming the full row applies."""
    open_queues = defaultdict(list)  # key -> list of {"leg": leg, "remaining": int}
    matched = []  # (open_leg, close_leg, qty)
    orphaned = []  # (None, close_leg, qty) - close with no open in this data window

    for leg in legs:
        if leg["is_open"]:
            open_queues[leg["key"]].append({"leg": leg, "remaining": leg["quantity"]})
            continue

        remaining_to_close = leg["quantity"]
        q = open_queues[leg["key"]]
        while remaining_to_close > 0 and q:
            lot = q[0]
            take = min(lot["remaining"], remaining_to_close)
            matched.append((lot["leg"], leg, take))
            lot["remaining"] -= take
            remaining_to_close -= take
            if lot["remaining"] <= 0:
                q.pop(0)

        if remaining_to_close > 0:
            orphaned.append((None, leg, remaining_to_close))

    still_open = []
    for key, q in open_queues.items():
        for lot in q:
            if lot["remaining"] > 0:
                leg = lot["leg"]
                frac = lot["remaining"] / leg["quantity"] if leg["quantity"] else 0
                still_open.append({
                    **leg,
                    "quantity": lot["remaining"],
                    "value": round(leg["value"] * frac, 4),
                    "commission": round(leg["commission"] * frac, 4),
                    "fees": round(leg["fees"] * frac, 4),
                })

    return matched, orphaned, still_open


def _leg_frac_value(leg, qty):
    """Prorated (value, commission+fees) for `qty` of a leg's full row quantity."""
    frac = qty / leg["quantity"] if leg["quantity"] else 0
    return leg["value"] * frac, (leg["commission"] + leg["fees"]) * frac


def _group_pnl(pairs):
    """pairs: list of (open_leg, close_leg, qty) triples, prorated per-pair."""
    net_open = 0.0
    net_close = 0.0
    commission_total = 0.0
    for open_leg, close_leg, qty in pairs:
        ov, oc = _leg_frac_value(open_leg, qty)
        cv, cc = _leg_frac_value(close_leg, qty)
        net_open += ov
        net_close += cv
        commission_total += oc + cc
    net_open = round(net_open, 4)
    pnl = round(net_open + net_close, 4)
    commission_total = round(commission_total, 4)
    return net_open, pnl, commission_total


def build_round_trips(matched_pairs):
    groups = defaultdict(list)
    for open_leg, close_leg, qty in matched_pairs:
        gkey = open_leg["order_id"] or (open_leg["underlying"], open_leg["date"])
        groups[gkey].append((open_leg, close_leg, qty))

    round_trips = []
    for gkey, pairs in groups.items():
        underlying = pairs[0][0]["underlying"]
        open_time = min(p[0]["date"] for p in pairs)
        close_time = max(p[1]["date"] for p in pairs)
        first_key = pairs[0][0]["key"]
        size = sum(qty for open_leg, _, qty in pairs if open_leg["key"] == first_key)
        leg_count = len({p[0]["key"] for p in pairs})
        net_open, pnl, commission_total = _group_pnl(pairs)
        # net_open sign convention (SELL credit / BUY debit) already lives in
        # each leg's "value" field, same as pairing.py's per-unit premium.
        strategy = strategy_label(leg_count, net_open)

        pct_return_on_credit = None
        if net_open and net_open > 0:
            pct_return_on_credit = round((pnl / net_open) * 100, 2)

        trade_key = hashlib.sha1(
            f"tt|{underlying}|{open_time}|{close_time}|{gkey}".encode()
        ).hexdigest()[:16]

        round_trips.append({
            "trade_key": trade_key,
            "symbol": underlying,
            "company_name": underlying,
            "strategy": strategy,
            "leg_count": leg_count,
            "size": size,
            "open_time": open_time,
            "close_time": close_time,
            "net_premium_open": net_open,
            "pnl": pnl,
            "commission_total": commission_total,
            # tastytrade's Commissions/Fees columns are already negative-signed
            # costs (e.g. -1.00), unlike IBKR's positive-magnitude commission
            # field that pairing.py subtracts - so here the cost is ADDED.
            "pnl_after_commission": round(pnl + commission_total, 4),
            "pct_return_on_credit": pct_return_on_credit,
            "result": "win" if pnl > 0.005 else ("loss" if pnl < -0.005 else "breakeven"),
            "source": "tastytrade",
        })
    return round_trips


def build_orphan_trips(orphan_triples):
    """Closing legs with no matching open inside this CSV's date window
    (position was opened before the export's start date)."""
    groups = defaultdict(list)
    for _, close_leg, qty in orphan_triples:
        gkey = close_leg["order_id"] or (close_leg["underlying"], close_leg["date"])
        groups[gkey].append((close_leg, qty))

    trips = []
    for gkey, items in groups.items():
        underlying = items[0][0]["underlying"]
        close_time = max(leg["date"] for leg, _ in items)
        pnl = 0.0
        commission_total = 0.0
        for leg, qty in items:
            v, c = _leg_frac_value(leg, qty)
            pnl += v
            commission_total += c
        pnl = round(pnl, 4)
        commission_total = round(commission_total, 4)
        leg_count = len({leg["key"] for leg, _ in items})
        size = items[0][1]
        strategy = strategy_label(leg_count, pnl) + " (opened before data window)"

        trade_key = hashlib.sha1(f"tt-orphan|{underlying}|{close_time}|{gkey}".encode()).hexdigest()[:16]

        trips.append({
            "trade_key": trade_key,
            "symbol": underlying,
            "company_name": underlying,
            "strategy": strategy,
            "leg_count": leg_count,
            "size": size,
            "open_time": None,
            "close_time": close_time,
            "net_premium_open": None,
            "pnl": pnl,
            "commission_total": commission_total,
            "pnl_after_commission": round(pnl + commission_total, 4),
            "pct_return_on_credit": None,
            "result": "win" if pnl > 0.005 else ("loss" if pnl < -0.005 else "breakeven"),
            "source": "tastytrade",
        })
    return trips


def build_open_positions(still_open_legs):
    groups = defaultdict(list)
    for leg in still_open_legs:
        gkey = leg["order_id"] or (leg["underlying"], leg["date"])
        groups[gkey].append(leg)

    positions = []
    for gkey, legs in groups.items():
        underlying = legs[0]["underlying"]
        net_open = round(sum(l["value"] for l in legs), 4)
        leg_count = len(legs)
        positions.append({
            "symbol": underlying,
            "company_name": underlying,
            "strategy": strategy_label(leg_count, net_open),
            "leg_count": leg_count,
            "size": legs[0]["quantity"],
            "open_time": legs[0]["date"],
            "net_premium_open": net_open,
        })
    return positions


def process(csv_path):
    rows = load_rows(csv_path)
    legs = parse_legs(rows)
    matched, orphaned, still_open_legs = match_legs(legs)

    round_trips = build_round_trips(matched) + build_orphan_trips(orphaned)
    round_trips.sort(key=lambda t: t["close_time"])
    open_positions = build_open_positions(still_open_legs)
    return round_trips, open_positions


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "data/tastytrade_transactions.csv"
    rt, open_pos = process(path)
    print(f"{len(rt)} closed round-trip trades, {len(open_pos)} still-open legs")
    wins = [t for t in rt if t["result"] == "win"]
    losses = [t for t in rt if t["result"] == "loss"]
    if rt:
        print(f"Win rate: {len(wins)}/{len(rt)} = {len(wins)/len(rt)*100:.1f}%")
        print(f"Total P&L: {sum(t['pnl'] for t in rt):.2f}")
        print(f"Total commission: {sum(t['commission_total'] for t in rt):.2f}")
    for t in rt:
        print(t["close_time"][:10], t["symbol"], t["strategy"], t["leg_count"], t["result"], round(t["pnl"], 2))
    print("\nOpen positions:")
    for p in open_pos:
        print(p["open_time"][:10], p["symbol"], p["strategy"], p["leg_count"], round(p["net_premium_open"], 2))
