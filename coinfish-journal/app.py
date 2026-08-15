import os
import json
import datetime
from collections import defaultdict
from flask import Flask, jsonify, request, render_template, send_from_directory

import db

app = Flask(__name__)
db.init_db()

# Favicon files (Coinfish logo) served directly - kept out of templates/
# and out of Flask's static folder so only these four files are exposed,
# not the rest of this directory (db.py, the trades database, raw data).
app.add_url_rule("/favicon.ico", "favicon_ico", lambda: send_from_directory(os.path.dirname(__file__), "favicon.ico", mimetype="image/vnd.microsoft.icon"))
app.add_url_rule("/favicon-16x16.png", "favicon_16", lambda: send_from_directory(os.path.dirname(__file__), "favicon-16x16.png", mimetype="image/png"))
app.add_url_rule("/favicon-32x32.png", "favicon_32", lambda: send_from_directory(os.path.dirname(__file__), "favicon-32x32.png", mimetype="image/png"))
app.add_url_rule("/apple-touch-icon.png", "apple_touch_icon", lambda: send_from_directory(os.path.dirname(__file__), "apple-touch-icon.png", mimetype="image/png"))


def compute_stats(trades):
    closed = trades
    n = len(closed)
    if n == 0:
        return {
            "count": 0, "win_rate": None, "total_pnl": 0, "total_pnl_after_commission": 0,
            "avg_win": None, "avg_loss": None, "largest_win": None, "largest_loss": None,
            "profit_factor": None, "total_commission": 0, "expectancy": None,
            "current_streak": 0, "current_streak_type": None,
        }

    wins = [t for t in closed if t["result"] == "win"]
    losses = [t for t in closed if t["result"] == "loss"]
    total_pnl = sum(t["pnl"] for t in closed)
    total_pnl_ac = sum(t["pnl_after_commission"] for t in closed if t["pnl_after_commission"] is not None)
    total_commission = sum(t["commission_total"] for t in closed)
    avg_win = (sum(t["pnl"] for t in wins) / len(wins)) if wins else None
    avg_loss = (sum(t["pnl"] for t in losses) / len(losses)) if losses else None
    largest_win = max((t["pnl"] for t in wins), default=None)
    largest_loss = min((t["pnl"] for t in losses), default=None)
    gross_win = sum(t["pnl"] for t in wins)
    gross_loss = abs(sum(t["pnl"] for t in losses))
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else None
    win_rate = len(wins) / n * 100

    expectancy = None
    if avg_win is not None and avg_loss is not None:
        wr = win_rate / 100
        expectancy = wr * avg_win + (1 - wr) * avg_loss

    # current streak: sort by close_time ascending, walk backward
    ordered = sorted(closed, key=lambda t: t["close_time"])
    streak = 0
    streak_type = None
    for t in reversed(ordered):
        if t["result"] == "breakeven":
            break
        if streak_type is None:
            streak_type = t["result"]
            streak = 1
        elif t["result"] == streak_type:
            streak += 1
        else:
            break

    return {
        "count": n,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_after_commission": round(total_pnl_ac, 2),
        "avg_win": round(avg_win, 2) if avg_win is not None else None,
        "avg_loss": round(avg_loss, 2) if avg_loss is not None else None,
        "largest_win": round(largest_win, 2) if largest_win is not None else None,
        "largest_loss": round(largest_loss, 2) if largest_loss is not None else None,
        "profit_factor": round(profit_factor, 2) if profit_factor is not None else None,
        "total_commission": round(total_commission, 2),
        "expectancy": round(expectancy, 2) if expectancy is not None else None,
        "current_streak": streak,
        "current_streak_type": streak_type,
    }


def equity_curve(trades):
    ordered = sorted(trades, key=lambda t: t["close_time"])
    curve = []
    running = 0.0
    for t in ordered:
        running += t["pnl"]
        curve.append({"date": t["close_time"][:10], "cum_pnl": round(running, 2), "trade_pnl": round(t["pnl"], 2), "symbol": t["symbol"]})
    return curve


def pnl_by_symbol(trades):
    agg = defaultdict(lambda: {"pnl": 0.0, "count": 0, "wins": 0})
    for t in trades:
        a = agg[t["symbol"]]
        a["pnl"] += t["pnl"]
        a["count"] += 1
        if t["result"] == "win":
            a["wins"] += 1
    out = []
    for sym, a in agg.items():
        out.append({
            "symbol": sym, "pnl": round(a["pnl"], 2), "count": a["count"],
            "win_rate": round(a["wins"] / a["count"] * 100, 1) if a["count"] else None,
        })
    out.sort(key=lambda x: x["pnl"], reverse=True)
    return out


def pnl_by_strategy(trades):
    agg = defaultdict(lambda: {"pnl": 0.0, "count": 0, "wins": 0})
    for t in trades:
        a = agg[t["strategy"]]
        a["pnl"] += t["pnl"]
        a["count"] += 1
        if t["result"] == "win":
            a["wins"] += 1
    out = []
    for strat, a in agg.items():
        out.append({
            "strategy": strat, "pnl": round(a["pnl"], 2), "count": a["count"],
            "win_rate": round(a["wins"] / a["count"] * 100, 1) if a["count"] else None,
        })
    out.sort(key=lambda x: x["pnl"], reverse=True)
    return out


def calendar_pnl(trades):
    agg = defaultdict(lambda: {"pnl": 0.0, "count": 0})
    for t in trades:
        day = t["close_time"][:10]
        agg[day]["pnl"] += t["pnl"]
        agg[day]["count"] += 1
    return {day: {"pnl": round(v["pnl"], 2), "count": v["count"]} for day, v in agg.items()}


@app.route("/")
def index():
    return render_template("journal.html")


@app.route("/api/trades")
def api_trades():
    trades = db.get_all_trades()
    return jsonify({
        "trades": trades,
        "open_positions": db.get_open_positions(),
        "stats": compute_stats(trades),
        "equity_curve": equity_curve(trades),
        "by_symbol": pnl_by_symbol(trades),
        "by_strategy": pnl_by_strategy(trades),
        "calendar": calendar_pnl(trades),
        "last_import": db.last_import(),
        "entries": db.get_all_entries(),
    })


@app.route("/api/entries", methods=["POST"])
def api_create_entry():
    payload = request.get_json(force=True)
    if not payload.get("symbol") or not payload.get("strategy"):
        return jsonify({"ok": False, "error": "Symbol and strategy are required."}), 400
    new_id = db.create_entry(payload)
    return jsonify({"ok": True, "id": new_id})


@app.route("/api/entries/<int:entry_id>/link", methods=["POST"])
def api_link_entry(entry_id):
    payload = request.get_json(force=True)
    db.link_entry(entry_id, payload.get("trade_key"))
    return jsonify({"ok": True})


@app.route("/api/entries/<int:entry_id>", methods=["DELETE"])
def api_delete_entry(entry_id):
    db.delete_entry(entry_id)
    return jsonify({"ok": True})


@app.route("/api/trade/<trade_key>", methods=["POST"])
def api_update_trade(trade_key):
    payload = request.get_json(force=True)
    db.update_trade_meta(
        trade_key,
        notes=payload.get("notes"),
        tags=payload.get("tags"),
        mistake_flag=payload.get("mistake_flag"),
    )
    return jsonify({"ok": True})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Re-run the importers against whatever source files are on disk. IBKR
    refresh requires a fresh pull saved to data/raw_trades_seed.json first
    (see README) since this Flask process can't call the IBKR MCP tools
    itself. TastyTrade refresh is picked up automatically if a transaction
    CSV export has been placed at data/tastytrade_transactions.csv - re-drop
    a fresh export there any time to pull in new trades."""
    import pairing
    ibkr_path = os.path.join(os.path.dirname(__file__), "data", "raw_trades_seed.json")
    round_trips, open_positions = pairing.process(ibkr_path)
    source_label = ibkr_path

    tt_path = os.path.join(os.path.dirname(__file__), "data", "tastytrade_transactions.csv")
    if os.path.exists(tt_path):
        import tastytrade_pairing
        tt_round_trips, tt_open_positions = tastytrade_pairing.process(tt_path)
        round_trips += tt_round_trips
        open_positions += tt_open_positions
        source_label += " + tastytrade_transactions.csv"

    db.replace_trades(round_trips, open_positions, source_label)
    return jsonify({"ok": True, "count": len(round_trips), "open_count": len(open_positions)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5060))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
