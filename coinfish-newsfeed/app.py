import os
import time
import threading
from datetime import datetime, timedelta

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

import news_sources as ns

# Same universe as the Coinfish scanner watchlist, plus the core index
# ETFs. Kept as a plain list here (not imported from scanner.py) so this
# tool doesn't drag in pandas/numpy/bs4/curl_cffi just for a name list.
# Edit this list directly if the watchlist changes.
#
# Synced 2026-07-14 to the current 56-name watchlist (was still running the
# pre-2026-06-23 version - the newsfeed never got that revamp when
# backend/scanner.py and coinfish-playbook.html did). Billy caught the drift.
# Changes vs. the old list: added AVGO, V, MA, ISRG, ETN, PH; removed PFE,
# BMY, HAL, MMM, UPS (cut in the 2026-06-23 revamp, see coinfish-hq/memory.md).
WATCHLIST = [
    "SPY", "QQQ", "IWM",
    "NVDA", "AMD", "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "ORCL", "AVGO", "TSLA",
    "JPM", "BAC", "GS", "AXP", "SCHW", "COF", "MS", "WFC", "C", "V", "MA",
    "XOM", "CVX", "COP", "OXY", "EOG", "SLB", "MPC", "VLO",
    "LLY", "ABBV", "AMGN", "TMO", "JNJ", "MRK", "ISRG",
    "COST", "HD", "WMT", "MCD", "LOW", "NKE", "DIS", "SBUX", "TGT",
    "RTX", "BA", "HON", "CAT", "GE", "DE", "ETN", "PH", "LMT", "UNP",
]

# Sector grouping for the watchlist heatmap - mirrors the block structure
# WATCHLIST is already written in above (Tech / Financials / Energy /
# Healthcare / Consumer / Industrials), just made explicit as a lookup so
# the heatmap can group tickers into sector clusters. SPY/QQQ/IWM
# deliberately excluded - they're index ETFs, not single-sector companies,
# and don't carry a real market cap/sector the way an equity does.
WATCHLIST_SECTORS = {
    "Technology": ["NVDA", "AMD", "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "ORCL", "AVGO", "TSLA"],
    "Financials": ["JPM", "BAC", "GS", "AXP", "SCHW", "COF", "MS", "WFC", "C", "V", "MA"],
    "Energy": ["XOM", "CVX", "COP", "OXY", "EOG", "SLB", "MPC", "VLO"],
    "Healthcare": ["LLY", "ABBV", "AMGN", "TMO", "JNJ", "MRK", "ISRG"],
    "Consumer": ["COST", "HD", "WMT", "MCD", "LOW", "NKE", "DIS", "SBUX", "TGT"],
    "Industrials": ["RTX", "BA", "HON", "CAT", "GE", "DE", "ETN", "PH", "LMT", "UNP"],
}
TICKER_TO_SECTOR = {t: sector for sector, tickers in WATCHLIST_SECTORS.items() for t in tickers}

# Broader universe for the "Top 10 by Market Cap" widget - Billy wants the
# ACTUAL top 10 largest companies, not just the top 10 of his 58-name
# trading watchlist (WATCHLIST above is scoped to what he actually trades;
# this is scoped to "what's actually huge" and is a separate, wider list).
# No paid screener API here, so this is a static candidate list of every
# realistic top-10/top-20-by-market-cap contender (mega and large caps,
# US-listed or US-ADR so yfinance can price them) - fetched each poll, then
# sorted so whichever names are actually largest on a given day win the top
# 10 slots. Not exhaustive of the whole market, but wide enough that a name
# outside WATCHLIST (e.g. AAPL wasn't previously eligible, or names like
# BRK-B, AVGO, TSM, V, MA, WMT that aren't on the trading watchlist at all)
# now gets included and can win a slot on its own merits. Saudi Aramco
# (2222.SR) is excluded - yfinance can't reliably price/convert its native
# Riyadh-listing currency. GOOG (Alphabet Class C) deliberately excluded -
# GOOGL (Class A) is already in here and the two are the same company; only
# one should occupy a ranking slot, not two. SpaceX (Space Exploration
# Technologies Corp.) IPO'd 2026-06-12 on Nasdaq under SPCX, valued in the
# trillions post-IPO - included below now that it has a real public ticker.
MARKET_CAP_UNIVERSE = sorted((set(WATCHLIST) | {
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "TSLA", "SPCX",
    "BRK-B", "TSM", "WMT", "LLY", "JPM", "V", "MA", "NFLX", "ORCL", "XOM",
    "COST", "UNH", "JNJ", "HD", "PG", "NVO", "ASML", "SAP", "BAC", "CVX",
    "KO", "TMUS", "PM", "WFC", "ABBV", "IBM", "CRM", "CSCO", "MCD", "ABT",
    "PEP", "DIS", "VZ", "T", "CMCSA", "ADBE", "QCOM", "TXN", "INTU", "INTC",
    "NOW", "AMD", "UBER", "PDD", "BABA", "SHEL", "TM", "HSBC", "RY", "PLTR",
}) - {"SPY", "QQQ", "IWM"})  # index ETFs never belong in a market-cap ranking; parens
# forced here because Python's set "-" binds tighter than "|", so without
# them the subtraction only ever applied to the literal mega-cap set above,
# never to WATCHLIST (which is where SPY/QQQ/IWM actually live) - bug found
# during live verification, fixed same day.

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Economic calendar is scoped to the countries Billy actually trades around
# (US underlyings, plus Canada and China as macro-adjacent watch items) -
# all three calendar views (Today, This Week, Next Week - all Nasdaq-backed,
# see news_sources.py) get filtered down to this set instead of showing
# every country's releases.
ECON_COUNTRIES_NASDAQ = {"United States", "Canada", "China"}

CACHE_TTL = {
    "news": 90,          # seconds
    "filings": 60,
    "calendar": 900,
    "tape": 30,
    "macro": 120,
    "movers": 60,
    "marketcap": 300,    # market cap barely moves intraday, longer TTL than movers
    "heatmap": 120,
}

_cache = {}
_locks = {
    "news": threading.Lock(),
    "filings": threading.Lock(),
    "calendar": threading.Lock(),
    "tape": threading.Lock(),
    "macro": threading.Lock(),
    "movers": threading.Lock(),
    "marketcap": threading.Lock(),
    "heatmap": threading.Lock(),
}


def _cached(key, ttl, builder):
    now = time.time()
    entry = _cache.get(key)
    if entry and (now - entry["ts"]) < ttl:
        return entry["data"], entry["ts"]
    with _locks[key]:
        entry = _cache.get(key)
        if entry and (time.time() - entry["ts"]) < ttl:
            return entry["data"], entry["ts"]
        data = builder()
        _cache[key] = {"data": data, "ts": time.time()}
        return data, _cache[key]["ts"]


@app.after_request
def _no_store_api(resp):
    # Flask sets no Cache-Control by default, which leaves browsers free to
    # apply heuristic caching to GET responses (including fetch() calls) -
    # added after Billy reported the live site showing stale Movers data
    # while the raw endpoint (hit fresh via direct navigation) was already
    # returning updated numbers. Force every /api/* response to be
    # refetched every time, never served from the browser's HTTP cache.
    if request.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@app.route("/")
def index():
    return send_from_directory(".", "newsfeed.html")


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "watchlist_size": len(WATCHLIST), "time": datetime.now().isoformat()})


@app.route("/api/tape")
def tape():
    def build():
        return ns.fetch_ticker_tape(WATCHLIST)
    data, ts = _cached("tape", CACHE_TTL["tape"], build)
    return jsonify({"data": data, "as_of": ts})


@app.route("/api/news")
def news():
    def build():
        items, errors = ns.fetch_all_news_multi(WATCHLIST)
        return {"items": items, "errors": errors}
    data, ts = _cached("news", CACHE_TTL["news"], build)
    return jsonify({"data": data["items"], "errors": data["errors"], "as_of": ts})


@app.route("/api/filings")
def filings():
    def build():
        rows, errors = ns.fetch_sec_filings_multi()
        rows = ns.annotate_filings_watchlist(rows, WATCHLIST)
        form4, f4_err = ns.fetch_sec_form4_recent()
        if f4_err:
            errors.append(f4_err)
        form4 = ns.annotate_form4_watchlist(form4, WATCHLIST)
        return {"filings": rows, "form4": form4, "errors": errors}
    data, ts = _cached("filings", CACHE_TTL["filings"], build)
    return jsonify(data | {"as_of": ts})


@app.route("/api/macro")
def macro():
    def build():
        return {
            "vix": ns.fetch_vix_snapshot(),
            "yields": ns.fetch_treasury_yields(),
            "fomc": ns.get_fomc_status(),
            "sectors": ns.fetch_sector_heatmap(),
            "fear_greed": ns.fetch_fear_greed(),
        }
    data, ts = _cached("macro", CACHE_TTL["macro"], build)
    return jsonify(data | {"as_of": ts})


@app.route("/api/movers")
def movers():
    def build():
        rows, errors = ns.fetch_premarket_movers(WATCHLIST)
        return {"movers": rows, "errors": errors}
    data, ts = _cached("movers", CACHE_TTL["movers"], build)
    return jsonify(data | {"as_of": ts})


@app.route("/api/marketcap")
def marketcap():
    def build():
        # Uses MARKET_CAP_UNIVERSE (broad mega/large-cap list), not
        # WATCHLIST - Billy wants the actual top 10 largest companies here,
        # not just the top 10 of his trading watchlist.
        rows, errors = ns.fetch_market_cap_leaders(MARKET_CAP_UNIVERSE)
        return {"leaders": rows, "errors": errors}
    data, ts = _cached("marketcap", CACHE_TTL["marketcap"], build)
    return jsonify(data | {"as_of": ts})


@app.route("/api/heatmap")
def heatmap():
    def build():
        # Exclude the 3 index ETFs (SPY/QQQ/IWM) - they're not in
        # TICKER_TO_SECTOR since an index fund doesn't belong to a single
        # sector the way an equity does, and the heatmap is a per-sector
        # equity view, not an index tracker.
        tickers = [t for t in WATCHLIST if t in TICKER_TO_SECTOR]
        rows, errors = ns.fetch_heatmap_data(tickers, TICKER_TO_SECTOR)
        return {"cells": rows, "errors": errors}
    data, ts = _cached("heatmap", CACHE_TTL["heatmap"], build)
    return jsonify(data | {"as_of": ts})


# Sparkline is fetched on-demand only when a heatmap cell's popup opens
# (not polled/cached like everything else above) - pulling intraday
# 5-minute bars for all 55 watchlist names on every page load/poll would be
# needlessly heavy when Billy is only ever looking at one ticker at a time.
@app.route("/api/sparkline/<ticker>")
def sparkline(ticker):
    points, err = ns.fetch_intraday_sparkline(ticker.upper())
    return jsonify({"ticker": ticker.upper(), "points": points, "error": err})


@app.route("/api/calendar")
def calendar():
    def build():
        today = datetime.now().strftime("%Y-%m-%d")
        month = datetime.now().strftime("%Y-%m")
        errors = []

        divs, div_err = ns.fetch_nasdaq_dividends(today)
        if div_err:
            errors.append(div_err)

        ipos, ipo_err = ns.fetch_nasdaq_ipo_calendar(month)
        if ipo_err:
            errors.append(ipo_err)

        # Nasdaq's public economicevents endpoint is a full calendar day
        # ahead of reality: requesting date=<today> actually returns
        # yesterday's events (confirmed 2026-07-09: Initial Jobless Claims,
        # a Thursday-only release, only showed up when requesting
        # date=<tomorrow>, not date=<today>). Compensate by requesting
        # tomorrow's bucket to get today's real events. See news_sources.py
        # fetch_nasdaq_economic_events() for the full writeup. This only
        # applies to this one endpoint - dividends/IPO calendars below are
        # unaffected (not tested/reported as wrong, left as-is).
        nasdaq_econ_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        econ, econ_err = ns.fetch_nasdaq_economic_events(nasdaq_econ_date)
        if econ_err:
            errors.append(econ_err)

        econ = [r for r in econ if r.get("country") in ECON_COUNTRIES_NASDAQ]

        econ, ff_err = ns.annotate_economic_importance(econ, today)
        if ff_err:
            errors.append(ff_err)

        # "This Week" used to come from ForexFactory (get_econ_week), but
        # that feed never carries post-release Actual values - Billy wants
        # to see Actual vs Previous vs Forecast for events that already
        # released earlier in the week, which only Nasdaq's endpoint has.
        # Switched to the same Nasdaq-backed week-builder used for Next
        # Week (see fetch_econ_week_range's docstring in news_sources.py),
        # just pointed at the current week (Mon-Sun, offset 0) instead of
        # next week (offset 1). Also no longer drops past days - Billy
        # specifically wants to see the whole week including releases that
        # already happened.
        econ_week, week_err = ns.fetch_econ_this_week(ECON_COUNTRIES_NASDAQ)
        if week_err:
            errors.append(week_err)

        econ_next_week, next_week_err = ns.fetch_econ_next_week(ECON_COUNTRIES_NASDAQ)
        if next_week_err:
            errors.append(next_week_err)

        earnings, earn_errs = ns.fetch_watchlist_earnings(WATCHLIST)
        errors.extend(earn_errs)

        watch_set = set(WATCHLIST)
        divs_watchlist = [d for d in divs if d.get("symbol") in watch_set]

        return {
            "dividends_today": divs,
            "dividends_watchlist": divs_watchlist,
            "ipo": ipos,
            "economic_today": econ,
            "economic_week": econ_week,
            "economic_next_week": econ_next_week,
            "earnings_watchlist": earnings,
            "errors": errors,
        }

    data, ts = _cached("calendar", CACHE_TTL["calendar"], build)
    return jsonify(data | {"as_of": ts})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    # 0.0.0.0 so other devices on the same home network can reach this
    # (e.g. http://<this-machine's-LAN-IP>:5050). Still not reachable from
    # outside the network - Windows Firewall may prompt to allow it the
    # first time it starts listening.
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
