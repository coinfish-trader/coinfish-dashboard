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
    """FIFO-match each open leg to the next close leg sharing the same
    (underlying, strike, expiry, right) key."""
    open_queues = defaultdict(list)
    closed_pairs = []
    for leg in legs:
        if leg["is_open"]:
            open_queues[leg["key"]].append(leg)
        else:
            q = open_queues[leg["key"]]
            if q:
                closed_pairs.append((q.pop(0), leg))
            else:
                closed_pairs.append((None, leg))

    still_open = [leg for q in open_queues.values() for leg in q]
    return closed_pairs, still_open


def _group_pnl(pairs):
    net_open = round(sum(p[0]["value"] for p in pairs), 4)
    net_close = round(sum(p[1]["value"] for p in pairs), 4)
    pnl = round(net_open + net_close, 4)
    commission_total = round(
        sum(p[0]["commission"] + p[0]["fees"] + p[1]["commission"] + p[1]["fees"] for p in pairs), 4
    )
    return net_open, pnl, commission_total


def build_round_trips(matched_pairs):
    groups = defaultdict(list)
    for open_leg, close_leg in matched_pairs:
        gkey = open_leg["order_id"] or (open_leg["underlying"], open_leg["date"])
        groups[gkey].append((open_leg, close_leg))

    round_trips = []
    for gkey, pairs in groups.items():
        underlying = pairs[0][0]["underlying"]
        open_time = min(p[0]["date"] for p in pairs)
        close_time = max(p[1]["date"] for p in pairs)
        size = pairs[0][0]["quantity"]
        leg_count = len(pairs)
        net_open, pnl, commission_total = _group_pnl(pairs)
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
            "pnl_after_commission": round(pnl - commission_total, 4),
            "pct_return_on_credit": pct_return_on_credit,
            "result": "win" if pnl > 0.005 else ("loss" if pnl < -0.005 else "breakeven"),
            "source": "tastytrade",
        })
    return round_trips


def build_orphan_trips(orphan_pairs):
    """Closing legs with no matching open inside this CSV's date window
    (position was opened before the export's start date)."""
    groups = defaultdict(list)
    for _, close_leg in orphan_pairs:
        gkey = close_leg["order_id"] or (close_leg["underlying"], close_leg["date"])
        groups[gkey].append(close_leg)

    trips = []
    for gkey, legs in groups.items():
        underlying = legs[0]["underlying"]
        close_time = max(l["date"] for l in legs)
        pnl = round(sum(l["value"] for l in legs), 4)
        commission_total = round(sum(l["commission"] + l["fees"] for l in legs), 4)
        leg_count = len(legs)
        strategy = strategy_label(leg_count, pnl) + " (opened before data window)"

        trade_key = hashlib.sha1(f"tt-orphan|{underlying}|{close_time}|{gkey}".encode()).hexdigest()[:16]

        trips.append({
            "trade_key": trade_key,
            "symbol": underlying,
            "company_name": underlying,
            "strategy": strategy,
            "leg_count": leg_count,
            "size": legs[0]["quantity"],
            "open_time": None,
            "close_time": close_time,
            "net_premium_open": None,
            "pnl": pnl,
            "commission_total": commission_total,
            "pnl_after_commission": round(pnl - commission_total, 4),
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
    closed_pairs, still_open_legs = match_legs(legs)
    matched = [(o, c) for o, c in closed_pairs if o is not None]
    orphaned = [(o, c) for o, c in closed_pairs if o is None]

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
