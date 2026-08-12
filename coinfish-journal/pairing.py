"""
Coinfish Journal - trade pairing engine.

Takes raw IBKR execution-level trades (from get_account_trades) and groups them into
round-trip trades a trader would recognize: one open event, one close event, a P&L.

WHY THIS EXISTS: TradesViz (Billy's current tool) has a documented bug where it
fabricates phantom closing executions during re-pairing (see coinfish-hq/memory.md,
2026-08-10/11 entry). This engine uses realized_pnl straight from IBKR as ground
truth and never invents executions, so it can't reproduce that bug.

KNOWN LIMITATION: the IBKR MCP get_account_trades endpoint returns symbol + side +
price + size + realized_pnl but NOT strike/expiry/right (call vs put). So this
journal cannot show "165/170 PUT" the way TradesViz can from a Flex Query. It can
show leg count, net credit/debit, and P&L, which is enough for win rate / expectancy
tracking but not strike-level detail. If Billy wants that later, the fix is wiring
in IBKR Flex Query (same source TradesViz uses) instead of / alongside this MCP tool.
"""
import json
import hashlib
from collections import defaultdict
from datetime import datetime


def load_raw(path):
    with open(path) as f:
        data = json.load(f)
    # Only options legs belong in the strategy journal. The account also runs
    # stock liquidations (e.g. the Fidelity-transfer sell-off) which are not
    # part of the credit spread / iron condor strategy and are excluded here.
    return [t for t in data["trades"] if t.get("sec_type") == "OPT"]


def group_into_fill_events(raw_trades):
    """Group leg-level executions into fill events: one event = all legs of a
    spread order that executed at the same instant. Grouping key is (symbol,
    trade_time) rather than trade_id, because trade_id sub-segments can split
    a single multi-lot fill into several same-instant records."""
    groups = defaultdict(list)
    for t in raw_trades:
        key = (t["symbol"], t["trade_time"])
        groups[key].append(t)
    events = []
    for (symbol, trade_time), legs in groups.items():
        legs_sorted = sorted(legs, key=lambda l: l["trade_id"])
        size = max(l["size"] for l in legs_sorted)
        total_realized = sum(l.get("realized_pnl", 0) or 0 for l in legs_sorted)
        # net premium: SELL adds credit, BUY spends debit. net_amount is already
        # price * size * 100 (options multiplier), so this is total $ for the event.
        net_premium = sum(
            l["net_amount"] if l["side"] == "SELL" else -l["net_amount"]
            for l in legs_sorted
        )
        commission = sum(l.get("commission", 0) or 0 for l in legs_sorted)
        is_close = abs(total_realized) > 0.005
        events.append({
            "symbol": symbol,
            "company_name": next((l.get("company_name") for l in legs_sorted if l.get("company_name")), symbol),
            "trade_time": trade_time,
            "legs": legs_sorted,
            "leg_count": len(legs_sorted),
            "size": size,
            "net_premium": round(net_premium, 4),
            "commission": round(commission, 4),
            "realized_pnl": round(total_realized, 4),
            "is_close": is_close,
        })
    events.sort(key=lambda e: e["trade_time"])
    return events


def strategy_label(leg_count, net_premium):
    if leg_count == 1:
        return "Single Option"
    if leg_count == 2:
        return "Credit Vertical" if net_premium >= 0 else "Debit Vertical"
    if leg_count == 4:
        return "Iron Condor" if net_premium >= 0 else "Debit Combo (4-leg)"
    return f"{leg_count}-Leg Combo"


def build_round_trips(events):
    """FIFO-match close events against open events per symbol, size-aware so a
    2-lot open can be closed by two later 1-lot closes (this happens in the real
    data, e.g. HD 2026-07-30 -> two closes on 2026-08-05)."""
    open_queues = defaultdict(list)  # symbol -> list of open "lots"
    round_trips = []
    still_open = []

    for e in events:
        if not e["is_close"]:
            open_queues[e["symbol"]].append({
                "event": e,
                "remaining": e["size"],
            })
            continue

        # closing event: consume from oldest open lots for this symbol
        remaining_to_close = e["size"]
        consumed_opens = []
        q = open_queues[e["symbol"]]
        while remaining_to_close > 0 and q:
            lot = q[0]
            take = min(lot["remaining"], remaining_to_close)
            consumed_opens.append((lot["event"], take))
            lot["remaining"] -= take
            remaining_to_close -= take
            if lot["remaining"] <= 0:
                q.pop(0)

        if not consumed_opens:
            # closing P&L with no matching open in this data window (position
            # opened before the pull window). Still record it, flagged.
            consumed_opens = [(None, e["size"])]

        open_event = consumed_opens[0][0]
        matched_size = sum(sz for _, sz in consumed_opens)
        if open_event:
            open_premium_per_unit = open_event["net_premium"] / open_event["size"]
            open_commission_per_unit = open_event["commission"] / open_event["size"]
            open_net_premium = round(open_premium_per_unit * matched_size, 4)
            open_commission = round(open_commission_per_unit * matched_size, 4)
            open_time = open_event["trade_time"]
            leg_count = open_event["leg_count"]
            strategy = strategy_label(leg_count, open_premium_per_unit)
        else:
            open_net_premium = None
            open_commission = 0
            open_time = None
            leg_count = e["leg_count"]
            strategy = strategy_label(leg_count, e["net_premium"]) + " (opened before data window)"

        pnl = e["realized_pnl"]
        total_commission = round(open_commission + e["commission"], 4)
        pct_return_on_credit = None
        if open_net_premium and open_net_premium > 0:
            pct_return_on_credit = round((pnl / open_net_premium) * 100, 2)

        trade_key_src = f"{e['symbol']}|{open_time}|{e['trade_time']}|{matched_size}"
        trade_key = hashlib.sha1(trade_key_src.encode()).hexdigest()[:16]

        round_trips.append({
            "trade_key": trade_key,
            "symbol": e["symbol"],
            "company_name": e["company_name"],
            "strategy": strategy,
            "leg_count": leg_count,
            "size": matched_size,
            "open_time": open_time,
            "close_time": e["trade_time"],
            "net_premium_open": open_net_premium,
            "pnl": pnl,
            "commission_total": total_commission,
            "pnl_after_commission": round(pnl - total_commission, 4) if pnl is not None else None,
            "pct_return_on_credit": pct_return_on_credit,
            "result": "win" if pnl > 0.005 else ("loss" if pnl < -0.005 else "breakeven"),
        })

    for symbol, q in open_queues.items():
        for lot in q:
            if lot["remaining"] > 0:
                e = lot["event"]
                still_open.append({
                    "symbol": symbol,
                    "company_name": e["company_name"],
                    "strategy": strategy_label(e["leg_count"], e["net_premium"]),
                    "leg_count": e["leg_count"],
                    "size": lot["remaining"],
                    "open_time": e["trade_time"],
                    "net_premium_open": round((e["net_premium"] / e["size"]) * lot["remaining"], 4),
                })

    round_trips.sort(key=lambda t: t["close_time"])
    return round_trips, still_open


def process(raw_path):
    raw = load_raw(raw_path)
    events = group_into_fill_events(raw)
    round_trips, still_open = build_round_trips(events)
    return round_trips, still_open


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "data/raw_trades_seed.json"
    rt, open_pos = process(path)
    print(f"{len(rt)} closed round-trip trades, {len(open_pos)} still-open legs")
    wins = [t for t in rt if t["result"] == "win"]
    losses = [t for t in rt if t["result"] == "loss"]
    print(f"Win rate: {len(wins)}/{len(rt)} = {len(wins)/len(rt)*100:.1f}%")
    print(f"Total P&L: {sum(t['pnl'] for t in rt):.2f}")
    print(f"Total commission: {sum(t['commission_total'] for t in rt):.2f}")
    for t in rt:
        print(t["close_time"][:10], t["symbol"], t["strategy"], t["result"], round(t["pnl"], 2))
