import os
import time
import hmac
import hashlib
import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from urllib.parse import urlparse

from flask import Flask, jsonify, request, send_from_directory, session, redirect, Response
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

import news_sources as ns

# Same universe as the Coinfish scanner watchlist, plus the core index
# ETFs. Kept as a plain list here (not imported from scanner.py) so this
# tool doesn't drag in pandas/numpy/bs4/curl_cffi just for a name list.
# Edit this list directly if the watchlist changes.
#
# Revamped 2026-07-31: liquidity/spread cleanup. Billy flagged that a chunk
# of the 56-name sector list traded thin, wide-spread options - fine for
# stock liquidity, bad for actually working credit spread/condor fills.
# Cut 16 names: COF, SLB, MPC, VLO, EOG, AMGN, TMO, ISRG, RTX, HON, CAT, DE,
# UNP, LMT, ETN, PH. Down to 40 names + SPY/QQQ/IWM (43 total). See
# coinfish-hq/memory.md for the full liquidity tiering behind this cut.
#
# Same-day follow-up: added 5 names from a broader top-50-liquid-options scan
# (CRM, UBER, PYPL, GM, MU) - all deep liquidity plus well-behaved (no
# meme/gap-prone history), unlike some other liquid names considered and
# rejected (PLTR, COIN, SMCI, APP - liquid but real gap/event risk; see
# memory.md). Then added ABNB same day too (reasonably tame mega-cap, more
# consumer/travel exposure) - DKNG, RBLX, MRVL considered and left off
# (regulatory/controversy/customer-concentration gap risk). Now 46 names +
# SPY/QQQ/IWM (49 total).
WATCHLIST = [
    "SPY", "QQQ", "IWM",
    "NVDA", "AMD", "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "ORCL", "AVGO", "TSLA", "CRM", "MU",
    "JPM", "BAC", "GS", "AXP", "SCHW", "MS", "WFC", "C", "V", "MA", "PYPL",
    "XOM", "CVX", "COP", "OXY",
    "LLY", "ABBV", "JNJ", "MRK",
    "COST", "HD", "WMT", "MCD", "LOW", "NKE", "DIS", "SBUX", "TGT", "GM", "ABNB",
    "BA", "GE", "UBER", "HON", "CAT",
]

# Sector grouping for the watchlist heatmap - mirrors the block structure
# WATCHLIST is already written in above (Tech / Financials / Energy /
# Healthcare / Consumer / Industrials), just made explicit as a lookup so
# the heatmap can group tickers into sector clusters. SPY/QQQ/IWM
# deliberately excluded - they're index ETFs, not single-sector companies,
# and don't carry a real market cap/sector the way an equity does.
WATCHLIST_SECTORS = {
    "Technology": ["NVDA", "AMD", "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "ORCL", "AVGO", "TSLA", "CRM", "MU"],
    "Financials": ["JPM", "BAC", "GS", "AXP", "SCHW", "MS", "WFC", "C", "V", "MA", "PYPL"],
    "Energy": ["XOM", "CVX", "COP", "OXY"],
    "Healthcare": ["LLY", "ABBV", "JNJ", "MRK"],
    "Consumer": ["COST", "HD", "WMT", "MCD", "LOW", "NKE", "DIS", "SBUX", "TGT", "GM", "ABNB"],
    "Industrials": ["BA", "GE", "UBER", "HON", "CAT"],
}
TICKER_TO_SECTOR = {t: sector for sector, tickers in WATCHLIST_SECTORS.items() for t in tickers}

# Broader universe for the "Top 10 by Market Cap" widget - Billy wants the
# ACTUAL top 10 largest companies, not just the top 10 of his 51-name
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
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)  # Railway terminates TLS in front of us
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ---------------------------------------------------------------------------
# Login gate (added 2026-09-16)
# ---------------------------------------------------------------------------
# The Market Feed pop-up shows full article text from other outlets, which
# should only be readable privately. Set NEWSFEED_PASSWORD in Railway to turn
# the login on. With it unset (e.g. local runs), the site stays open as
# before but /api/article refuses to serve full text, so full articles are
# never shown on an open site.
# The gate sits in front of EVERYTHING (not just the page) because
# static_folder="." would otherwise also serve the .py source files.
NEWSFEED_PASSWORD = os.environ.get("NEWSFEED_PASSWORD", "")
AUTH_ENABLED = bool(NEWSFEED_PASSWORD)
if AUTH_ENABLED:
    # Derived from the password so sessions survive restarts and are shared
    # across gunicorn workers; changing the password logs everyone out.
    app.secret_key = os.environ.get("SECRET_KEY") or hashlib.sha256(
        ("coinfish-newsfeed:" + NEWSFEED_PASSWORD).encode()).hexdigest()
    app.config.update(
        PERMANENT_SESSION_LIFETIME=timedelta(days=90),
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=bool(os.environ.get("RAILWAY_ENVIRONMENT")),
    )

PUBLIC_PATHS = {"/login", "/favicon.ico", "/favicon-16x16.png", "/favicon-32x32.png",
                "/apple-touch-icon.png", "/api/health"}

LOGIN_PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Coinfish Newsfeed · Sign in</title>
<link rel="icon" href="/favicon.ico">
<style>
  body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
       background:linear-gradient(180deg,#0A2342,#061629);color:#fff;
       font-family:Inter,'Open Sans',system-ui,sans-serif;padding:16px}
  form{width:100%;max-width:340px;background:#0D2C52;border:1px solid rgba(255,255,255,.1);
       border-radius:14px;padding:26px 22px}
  h1{margin:0 0 4px;font-size:1.2rem}
  h1 span{color:#E6C24F}
  p{margin:0 0 18px;color:#9DB2CC;font-size:.85rem}
  input{width:100%;box-sizing:border-box;padding:11px 12px;border-radius:9px;
        border:1px solid rgba(255,255,255,.18);background:#061629;color:#fff;font-size:1rem}
  input:focus{outline:none;border-color:#25C5D4}
  button{margin-top:14px;width:100%;padding:11px;border:0;border-radius:100px;font-weight:700;
         font-size:.95rem;color:#061629;cursor:pointer;
         background:linear-gradient(100deg,#25C5D4,#E6C24F)}
  .err{color:#E8505B;font-size:.82rem;margin:10px 0 0}
</style></head><body>
<form method="post" action="/login">
  <h1>Coin<span>fish</span> Newsfeed</h1>
  <p>Private feed. Enter the password to continue.</p>
  <input type="password" name="password" placeholder="Password" autocomplete="current-password" autofocus required>
  <input type="hidden" name="next" value="{next}">
  <button type="submit">Sign in</button>
  {error}
</form></body></html>"""


def _is_authed():
    return (not AUTH_ENABLED) or session.get("auth") is True


def _safe_next(target):
    target = target or "/"
    return target if target.startswith("/") and not target.startswith("//") else "/"


@app.before_request
def _require_login():
    if _is_authed() or request.path in PUBLIC_PATHS or request.method == "OPTIONS":
        return None
    if request.path.startswith("/api/"):
        return jsonify({"error": "login_required"}), 401
    return redirect("/login?next=" + request.full_path.rstrip("?"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if not AUTH_ENABLED:
        return redirect("/")
    from html import escape
    nxt = _safe_next(request.values.get("next"))
    if request.method == "POST":
        if hmac.compare_digest(request.form.get("password", ""), NEWSFEED_PASSWORD):
            session.clear()
            session["auth"] = True
            session.permanent = True
            return redirect(nxt)
        time.sleep(1.0)  # slow down guessing
        body = LOGIN_PAGE.replace("{next}", escape(nxt)).replace(
            "{error}", '<p class="err">Wrong password.</p>')
        return Response(body, status=401, mimetype="text/html")
    if _is_authed():
        return redirect(nxt)
    body = LOGIN_PAGE.replace("{next}", escape(nxt)).replace("{error}", "")
    return Response(body, mimetype="text/html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login" if AUTH_ENABLED else "/")

# Economic calendar is scoped to the countries Billy actually trades around
# (US underlyings, plus Canada and China as macro-adjacent watch items) -
# all three calendar views (Today, This Week, Next Week - all Nasdaq-backed,
# see news_sources.py) get filtered down to this set instead of showing
# every country's releases.
ECON_COUNTRIES_NASDAQ = {"United States", "Canada", "China"}

CACHE_TTL = {
    "news": 90,          # seconds
    "filings": 60,
    "calendar": 60,      # short TTL so actuals (e.g. CPI) show up fast after release
    "tape": 30,
    "macro": 120,
    "auctions": 1800,   # auction results publish once a day per security
    "movers": 60,
    "marketcap": 300,    # market cap barely moves intraday, longer TTL than movers
    "heatmap": 120,
    "bls_cpi": 900,       # separate from "calendar" - BLS unregistered API is rate-limited to 25 req/day
}

_cache = {}
_locks = {
    "news": threading.Lock(),
    "filings": threading.Lock(),
    "calendar": threading.Lock(),
    "tape": threading.Lock(),
    "macro": threading.Lock(),
    "auctions": threading.Lock(),
    "movers": threading.Lock(),
    "marketcap": threading.Lock(),
    "heatmap": threading.Lock(),
    "bls_cpi": threading.Lock(),
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
        # gzip JSON: /api/news is several hundred KB raw and polls every 90s,
        # which matters on a phone. Compresses ~5x.
        if (resp.status_code == 200 and not resp.direct_passthrough
                and "gzip" in request.headers.get("Accept-Encoding", "")
                and resp.mimetype == "application/json"
                and "Content-Encoding" not in resp.headers):
            data = resp.get_data()
            if len(data) > 2048:
                import gzip
                resp.set_data(gzip.compress(data, compresslevel=5))
                resp.headers["Content-Encoding"] = "gzip"
                resp.headers["Vary"] = "Accept-Encoding"
    return resp


@app.route("/")
def index():
    return send_from_directory(".", "newsfeed.html")


def _page_version():
    # Fingerprint of the page actually being served. Open tabs poll this and
    # reload themselves when it changes, so a tab left open across a deploy
    # doesn't keep running old code.
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "newsfeed.html"), "rb") as fh:
            return hashlib.sha1(fh.read()).hexdigest()[:12]
    except OSError:
        return "unknown"


PAGE_VERSION = _page_version()


@app.route("/api/version")
def version():
    return jsonify({"version": PAGE_VERSION, "auth": AUTH_ENABLED})


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "watchlist_size": len(WATCHLIST), "time": datetime.now().isoformat()})


@app.route("/api/tape")
def tape():
    def build():
        return ns.fetch_ticker_tape(WATCHLIST)
    data, ts = _cached("tape", CACHE_TTL["tape"], build)
    return jsonify({"data": data, "as_of": ts})


# link -> feed item, kept for 12h (longer than the 90s news cache) so a
# pop-up opened a few minutes after the list loaded can still find its item
# without forcing a full feed rebuild.
_known_items = {}
_known_lock = threading.Lock()
KNOWN_ITEM_TTL = 12 * 3600


def _build_news():
    items, errors = ns.fetch_all_news_multi(WATCHLIST)
    now = time.time()
    with _known_lock:
        for it in items:
            if it.get("link"):
                _known_items[it["link"]] = (now, it)
        for k in [k for k, (ts, _) in _known_items.items() if now - ts > KNOWN_ITEM_TTL]:
            del _known_items[k]
    return {"items": items, "errors": errors}


def _lookup_item(url):
    with _known_lock:
        hit = _known_items.get(url)
    if hit:
        return hit[1]
    data, _ = _cached("news", CACHE_TTL["news"], _build_news)
    return next((it for it in data["items"] if it.get("link") == url), None)


@app.route("/api/news")
def news():
    data, ts = _cached("news", CACHE_TTL["news"], _build_news)
    # Full article bodies stay server-side (served one at a time by
    # /api/article); the list only says whether a full read is available.
    items = []
    for it in data["items"]:
        row = {k: v for k, v in it.items() if k != "content"}
        row["full"] = AUTH_ENABLED and ns.full_text_possible(it)
        items.append(row)
    return jsonify({"data": items, "errors": data["errors"], "as_of": ts, "auth": AUTH_ENABLED})


# Extracted article text, keyed by URL. Per gunicorn worker, bounded.
_article_cache = OrderedDict()
_article_lock = threading.Lock()
ARTICLE_CACHE_MAX = 400
ARTICLE_TTL_OK = 6 * 3600
ARTICLE_TTL_FAIL = 30 * 60


@app.route("/api/article")
def article():
    if not AUTH_ENABLED:
        return jsonify({"error": "full_text_requires_login"}), 403
    url = request.args.get("url", "")
    # Only articles that are actually in the current feed can be fetched, so
    # this endpoint can't be used as a general-purpose proxy.
    item = _lookup_item(url)
    if item is None:
        return jsonify({"error": "not_in_feed"}), 404
    if urlparse(url).scheme not in ("http", "https"):
        return jsonify({"error": "bad_url"}), 400

    if item.get("content"):
        return jsonify({"text": item["content"], "origin": "feed"})
    if item.get("source") in ns.INLINE_FULL_SOURCES:
        return jsonify({"text": item.get("summary", ""), "origin": "feed"})
    if not ns.full_text_possible(item):
        return jsonify({"error": "not_available"}), 404

    now = time.time()
    with _article_lock:
        hit = _article_cache.get(url)
        if hit and now - hit["ts"] < (ARTICLE_TTL_OK if hit["text"] else ARTICLE_TTL_FAIL):
            _article_cache.move_to_end(url)
            cached = hit
        else:
            cached = None
    if cached is None:
        text, err = ns.fetch_article_text(url)
        cached = {"text": text, "err": err, "ts": time.time()}
        with _article_lock:
            _article_cache[url] = cached
            while len(_article_cache) > ARTICLE_CACHE_MAX:
                _article_cache.popitem(last=False)
    if not cached["text"]:
        return jsonify({"error": "extract_failed", "detail": cached["err"]}), 502
    return jsonify({"text": cached["text"], "origin": "page"})


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
        yields = ns.fetch_treasury_yields()
        return {
            "vix": ns.fetch_vix_snapshot(),
            "yields": yields,
            "yield_spreads": ns.build_yield_spreads(yields),
            "fed": ns.fetch_fed_rates(),
            "fomc": ns.get_fomc_status(),
            "sectors": ns.fetch_sector_heatmap(),
            "fear_greed": ns.fetch_fear_greed(),
        }
    data, ts = _cached("macro", CACHE_TTL["macro"], build)
    return jsonify(data | {"as_of": ts})


@app.route("/api/auctions")
def auctions():
    # Treasury only publishes auction results once a day per security, so a
    # long TTL is plenty and keeps TreasuryDirect from being hammered.
    def build():
        return ns.fetch_treasury_auctions()
    data, ts = _cached("auctions", CACHE_TTL["auctions"], build)
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

        # Nasdaq's actual field can lag the real release by hours (see
        # fetch_bls_cpi_actuals docstring in news_sources.py - found
        # 2026-07-14 when CPI was already out and reported everywhere but
        # Nasdaq's feed still showed blank). Patch CPI/Core CPI rows from
        # BLS's own public API, which has no such lag. Cached separately
        # from "calendar" (CACHE_TTL["bls_cpi"]) to respect BLS's
        # unregistered rate limit of 25 req/day.
        bls_cpi, _ = _cached("bls_cpi", CACHE_TTL["bls_cpi"], ns.fetch_bls_cpi_actuals)
        ns.patch_cpi_actuals_with_bls(econ, bls_cpi)
        ns.patch_cpi_actuals_with_bls(econ_week, bls_cpi)

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
