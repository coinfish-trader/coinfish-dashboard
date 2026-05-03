"""
Coinfish Dashboard — Flask API

Endpoints:
  GET  /api/health             — liveness check
  GET  /api/watchlist          — return the 56-ticker watchlist
  GET  /api/scan/stream        — SSE: streams per-ticker progress then final results
  GET  /api/scan/cached        — return last cached scan without re-running
  GET  /api/ticker/<SYMBOL>    — on-demand single-ticker scan
"""

import json
import time
import threading
import os

from flask import Flask, Response, jsonify, request
from flask_cors import CORS

from scanner import run_full_scan, scan_ticker, WATCHLIST

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

_cache = {
    "results":    None,
    "timestamp":  None,
    "is_scanning": False,
}
_cache_lock   = threading.Lock()
CACHE_TTL_SEC = 1800

@app.route("/api/health")
def health():
    return jsonify({
        "status":       "ok",
        "tickers":      len(WATCHLIST),
        "cache_fresh":  _is_cache_fresh(),
        "is_scanning":  _cache["is_scanning"],
    })

@app.route("/api/watchlist")
def watchlist():
    return jsonify({"tickers": WATCHLIST})

@app.route("/api/scan/cached")
def get_cached():
    with _cache_lock:
        if _cache["results"] is not None:
            age = time.time() - (_cache["timestamp"] or 0)
            return jsonify({
                "cached":      True,
                "age_seconds": round(age),
                "fresh":       age < CACHE_TTL_SEC,
                "data":        _cache["results"],
            })
    return jsonify({"cached": False, "data": None})

@app.route("/api/scan/stream")
def scan_stream():
    def generate():
        if _is_cache_fresh():
            with _cache_lock:
                payload = _cache["results"]
            yield _sse({"type": "cached", "data": payload})
            return

        with _cache_lock:
            if _cache["is_scanning"]:
                yield _sse({"type": "error", "message": "A scan is already running. Try /api/scan/cached."})
                return
            _cache["is_scanning"] = True

        events        = []
        events_lock   = threading.Lock()
        scan_result   = [None]
        scan_error    = [None]

        def progress_cb(ticker, completed, total, result):
            with events_lock:
                events.append({
                    "type":      "progress",
                    "ticker":    ticker,
                    "completed": completed,
                    "total":     total,
                    "result":    result,
                })

        def do_scan():
            try:
                scan_result[0] = run_full_scan(progress_cb)
            except Exception as exc:
                scan_error[0] = str(exc)

        yield _sse({"type": "started", "total": len(WATCHLIST)})

        scan_thread = threading.Thread(target=do_scan, daemon=True)
        scan_thread.start()

        sent = 0
        try:
            while scan_thread.is_alive() or sent < len(events):
                with events_lock:
                    while sent < len(events):
                        yield _sse(events[sent])
                        sent += 1
                if scan_thread.is_alive():
                    time.sleep(0.15)

            scan_thread.join()

            if scan_error[0]:
                yield _sse({"type": "error", "message": scan_error[0]})
            else:
                with _cache_lock:
                    _cache["results"]   = scan_result[0]
                    _cache["timestamp"] = time.time()
                yield _sse({"type": "complete", "data": scan_result[0]})

        finally:
            with _cache_lock:
                _cache["is_scanning"] = False

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )

@app.route("/api/ticker/<symbol>")
def single_ticker(symbol):
    symbol = symbol.upper().strip()
    if not symbol or len(symbol) > 5 or not symbol.isalpha():
        return jsonify({"error": "Invalid ticker symbol"}), 400
    result = scan_ticker(symbol)
    return jsonify(result)

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

def _is_cache_fresh() -> bool:
    with _cache_lock:
        return (
            _cache["results"] is not None
            and _cache["timestamp"] is not None
            and (CACHE_TTL_SEC == 0 or time.time() - _cache["timestamp"] < CACHE_TTL_SEC)
        )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
