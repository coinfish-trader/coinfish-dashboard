import os
import time
import json
import math
import threading
import urllib.request
import numpy as np
from pathlib import Path
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf


WATCHLIST = [
    # Technology (13)
    "NVDA", "AMD", "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "ORCL", "AVGO", "TSLA", "CRM", "MU",
    # Financials (11)
    "JPM", "BAC", "GS", "AXP", "SCHW", "MS", "WFC", "C", "V", "MA", "PYPL",
    # Energy (4)
    "XOM", "CVX", "OXY", "COP",
    # Healthcare (4)
    "LLY", "ABBV", "JNJ", "MRK",
    # Consumer (9)
    "COST", "HD", "WMT", "MCD", "LOW", "NKE", "DIS", "SBUX", "TGT",
    # Industrials / Transport / Travel (7)
    "BA", "GE", "HON", "CAT", "UBER", "GM", "ABNB",
]

COMPANY_NAMES = {
    "NVDA": "NVIDIA Corp", "TSLA": "Tesla Inc", "AAPL": "Apple Inc",
    "AMD": "Advanced Micro Devices", "AMZN": "Amazon.com", "GOOGL": "Alphabet Inc",
    "NFLX": "Netflix Inc", "MSFT": "Microsoft Corp", "ORCL": "Oracle Corp",
    "META": "Meta Platforms", "CRM": "Salesforce Inc", "MU": "Micron Technology",
    "BAC": "Bank of America", "WFC": "Wells Fargo",
    "C": "Citigroup Inc", "JPM": "JPMorgan Chase", "MS": "Morgan Stanley",
    "SCHW": "Charles Schwab", "AXP": "American Express",
    "GS": "Goldman Sachs", "PYPL": "PayPal Holdings",
    "XOM": "ExxonMobil", "CVX": "Chevron Corp",
    "OXY": "Occidental Petroleum", "COP": "ConocoPhillips",
    "MRK": "Merck & Co", "JNJ": "Johnson & Johnson",
    "ABBV": "AbbVie Inc", "LLY": "Eli Lilly",
    "WMT": "Walmart Inc", "NKE": "Nike Inc", "DIS": "Walt Disney Co",
    "SBUX": "Starbucks Corp", "HD": "Home Depot", "TGT": "Target Corp",
    "LOW": "Lowe's Companies", "COST": "Costco Wholesale", "MCD": "McDonald's Corp",
    "HON": "Honeywell International", "BA": "Boeing Co",
    "GE": "GE Aerospace", "CAT": "Caterpillar Inc",
    "AVGO": "Broadcom Inc", "V": "Visa Inc", "MA": "Mastercard Inc",
    "UBER": "Uber Technologies", "GM": "General Motors", "ABNB": "Airbnb Inc",
}

SECTORS = {
    "Tech":        ["NVDA", "AMD", "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "ORCL", "AVGO", "TSLA", "CRM", "MU"],
    "Financials":  ["JPM", "BAC", "GS", "AXP", "SCHW", "MS", "WFC", "C", "V", "MA", "PYPL"],
    "Energy":      ["XOM", "CVX", "OXY", "COP"],
    "Healthcare":  ["LLY", "ABBV", "JNJ", "MRK"],
    "Consumer":    ["COST", "HD", "WMT", "MCD", "LOW", "NKE", "DIS", "SBUX", "TGT"],
    "Industrials": ["BA", "GE", "HON", "CAT", "UBER", "GM", "ABNB"],
}


# ---------------------------------------------------------------------------
# Implied volatility source: CBOE delayed quotes (free, no login)
#
# CBOE publishes a 30-day implied volatility (IV30) for every optionable
# stock. Same measure as IBKR's "Opt. Implied Volatility". 15-min delayed.
#
# Premium richness is scored two ways:
#   IV/HV   -- IV30 vs. 30-day realized (historical) volatility. Works from
#              the first scan. > 1.0 means options are priced for more
#              movement than the stock is actually delivering.
#   IV rank -- where today's IV30 sits in its own 52-week range. Built from
#              daily IV30 readings stored on disk. Shown once there are
#              IV_RANK_MIN_DAYS readings.
# ---------------------------------------------------------------------------
CBOE_QUOTE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/quotes/{sym}.json"
IV_RANK_MIN_DAYS = 20

# IV/HV thresholds (premium points)
IVHV_STRONG = 1.25   # +2
IVHV_OK     = 1.10   # +1


def _history_dir():
    """Persistent storage: Railway volume if attached, else this folder."""
    d = (os.environ.get("IV_HISTORY_DIR")
         or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
         or str(Path(__file__).parent))
    Path(d).mkdir(parents=True, exist_ok=True)
    return Path(d)


_IV_HISTORY_PATH = _history_dir() / "iv_history.json"
_history_lock = threading.Lock()


def _load_iv_history():
    try:
        if _IV_HISTORY_PATH.exists():
            with open(_IV_HISTORY_PATH) as f:
                data = json.load(f)
                if isinstance(data, dict) and data.get("_source") == "cboe_iv30":
                    return data
    except Exception:
        pass
    # Old Black-Scholes readings are not comparable to CBOE IV30; start clean.
    return {"_source": "cboe_iv30"}


def _save_iv_history(hist):
    tmp = _IV_HISTORY_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(hist, f)
    os.replace(tmp, _IV_HISTORY_PATH)


def record_iv_readings(readings):
    """readings: {ticker: (session_date_iso, iv30_pct)}. One value per ticker per session."""
    if not readings:
        return
    cutoff = (date.today() - timedelta(days=366)).isoformat()
    with _history_lock:
        lockf = open(_IV_HISTORY_PATH.with_suffix(".lock"), "w")
        try:
            try:
                import fcntl  # cross-process lock (gunicorn runs 2 workers)
                fcntl.flock(lockf, fcntl.LOCK_EX)
            except ImportError:
                pass
            hist = _load_iv_history()
            for tk, (d, iv) in readings.items():
                entries = [e for e in hist.get(tk, []) if e[0] != d and e[0] >= cutoff]
                entries.append([d, round(float(iv), 3)])
                entries.sort(key=lambda e: e[0])
                hist[tk] = entries
            _save_iv_history(hist)
        finally:
            lockf.close()


def iv_rank_from_history(ticker, current_iv, hist=None):
    """Returns (rank_pct or None, n_days)."""
    if hist is None:
        hist = _load_iv_history()
    entries = hist.get(ticker, [])
    n = len(entries)
    if n < IV_RANK_MIN_DAYS:
        return None, n
    ivs = [v for _, v in entries]
    lo, hi = min(ivs), max(ivs)
    if hi <= lo:
        return 50.0, n
    rank = (current_iv - lo) / (hi - lo) * 100.0
    return round(min(max(rank, 0.0), 100.0), 1), n


def fetch_cboe_quote(ticker, timeout=10):
    """Returns dict(price, iv30, session_date) or raises."""
    req = urllib.request.Request(
        CBOE_QUOTE_URL.format(sym=ticker.replace("-", ".")),
        headers={"User-Agent": "Mozilla/5.0 (coinfish-scanner)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.load(r)["data"]
    iv30 = d.get("iv30")
    if iv30 is None or float(iv30) <= 0:
        raise ValueError("CBOE: no IV30")
    ltt = d.get("last_trade_time") or ""
    session = ltt[:10] if len(ltt) >= 10 else date.today().isoformat()
    return {
        "price":        float(d.get("current_price") or 0) or None,
        "iv30":         float(iv30),
        "session_date": session,
    }


def record_all_iv30():
    """Pull IV30 for the whole watchlist and store it. Used by the daily recorder."""
    readings = {}
    def _one(tk):
        try:
            q = fetch_cboe_quote(tk)
            return tk, (q["session_date"], q["iv30"])
        except Exception:
            return tk, None
    with ThreadPoolExecutor(max_workers=8) as ex:
        for tk, val in ex.map(_one, WATCHLIST):
            if val:
                readings[tk] = val
    record_iv_readings(readings)
    return len(readings)


def fetch_hv30(t):
    """30-session realized vol, annualized, in percent (matches IBKR HV window). Completed sessions only."""
    hist = t.history(period="3mo", auto_adjust=True)
    if hist is None or len(hist) < 32:
        return None
    closes = hist["Close"]
    try:
        last_idx = closes.index[-1]
        now_mkt = datetime.now(last_idx.tz)
        if last_idx.date() == now_mkt.date() and now_mkt.hour < 16:
            closes = closes.iloc[:-1]   # drop today's partial bar during market hours
    except Exception:
        pass
    log_ret = np.log(closes / closes.shift(1)).dropna().iloc[-30:]
    if len(log_ret) < 20:
        return None
    return float(log_ret.std() * math.sqrt(252) * 100.0)


def _sector(ticker):
    for s, tickers in SECTORS.items():
        if ticker in tickers:
            return s
    return "Other"


def _get_price(t):
    """Resilient price fetch across yfinance versions."""
    try:
        fi = t.fast_info
        for attr in ("last_price", "regularMarketPrice", "previousClose"):
            val = getattr(fi, attr, None)
            if val and float(val) > 0:
                return float(val)
    except Exception:
        pass
    try:
        hist = t.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


def fetch_pc_ratio_yfinance(ticker):
    try:
        t = yf.Ticker(ticker)
        exps = t.options
        if not exps:
            return None, "yfinance: no option expirations"

        total_call_oi = 0
        total_put_oi = 0
        used = 0
        for exp in exps[:min(6, len(exps))]:
            try:
                chain = t.option_chain(exp)
                total_call_oi += int(chain.calls["openInterest"].fillna(0).sum())
                total_put_oi  += int(chain.puts["openInterest"].fillna(0).sum())
                used += 1
            except Exception:
                continue

        if total_call_oi == 0 or used == 0:
            return None, "yfinance: zero call OI"

        return round(total_put_oi / total_call_oi, 3), None
    except Exception as exc:
        return None, f"yfinance P/C: {exc}"


def fetch_earnings_status(ticker):
    """
    Returns (earnings_date_iso_or_None, status).
    status values:
      "clear"             -- earnings confirmed more than 21 days away (or > 7 days past)
      "earn_risk"         -- earnings within the next 1-24 days
      "vol_crushed"       -- earnings in the last 7 days (IV already collapsed)
      "earn_date_unknown" -- date not found or API error; treat as potentially risky
    """
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        today = date.today()
        earnings_date = None

        if cal is None:
            return None, "earn_date_unknown"

        if isinstance(cal, dict):
            raw_dates = cal.get("Earnings Date")
            if raw_dates is not None:
                if hasattr(raw_dates, "__iter__") and not isinstance(raw_dates, str):
                    raw_dates = list(raw_dates)
                    raw = raw_dates[0] if raw_dates else None
                else:
                    raw = raw_dates
                if raw is not None:
                    if hasattr(raw, "date"):
                        earnings_date = raw.date()
                    elif isinstance(raw, date):
                        earnings_date = raw
                    elif isinstance(raw, str):
                        earnings_date = datetime.strptime(raw[:10], "%Y-%m-%d").date()
        else:
            try:
                if "Earnings Date" in cal.index:
                    row = cal.loc["Earnings Date"]
                    raw = row.iloc[0] if hasattr(row, "iloc") else row
                    if hasattr(raw, "date"):
                        earnings_date = raw.date()
            except Exception:
                pass

        if earnings_date is None:
            return None, "earn_date_unknown"

        delta = (earnings_date - today).days
        if delta < -7:
            status = "clear"
        elif -7 <= delta <= 0:
            status = "vol_crushed"
        elif 1 <= delta <= 24:   # 24d window — flag earnings risk within typical DTE range
            status = "earn_risk"
        else:
            status = "clear"

        return earnings_date.isoformat(), status

    except Exception:
        return None, "earn_date_unknown"



def premium_points(iv_hv, iv_rank):
    """Premium richness: best of IV/HV and IV rank (when available). 0-2."""
    pts = 0
    if iv_hv is not None:
        if iv_hv >= IVHV_STRONG:
            pts = 2
        elif iv_hv >= IVHV_OK:
            pts = 1
    if iv_rank is not None:
        if iv_rank > 50:
            pts = max(pts, 2)
        elif iv_rank >= 35:
            pts = max(pts, 1)
    return pts


def compute_score(prem_pts, pc_ratio):
    score = prem_pts
    if pc_ratio is not None:
        if pc_ratio < 0.70:
            score += 2
        elif pc_ratio <= 0.80:
            score += 1
    return score


def compute_setup(iv30, prem_pts, pc_ratio):
    if iv30 is None:
        return "No Data"
    if prem_pts >= 2:
        if pc_ratio is not None and pc_ratio < 0.70:
            return "Bull put spread"
        if pc_ratio is not None and pc_ratio > 1.0:
            return "Bear call spread"
        return "High IV / neutral"
    return "Watch"


def scan_ticker(ticker):
    result = {
        "ticker":          ticker,
        "company":         COMPANY_NAMES.get(ticker, ticker),
        "sector":          _sector(ticker),
        "price":           None,
        "iv30":            None,
        "hv30":            None,
        "iv_hv":           None,
        "iv_rank":         None,
        "iv_rank_days":    0,
        "iv_source":       None,
        "premium_pts":     0,
        "pc_ratio":        None,
        "pc_source":       None,
        "earnings_date":   None,
        "earnings_status": "earn_date_unknown",
        "score":           0,
        "setup":           "No Data",
        "sources":         [],
        "errors":          [],
    }

    t = yf.Ticker(ticker)

    # IV30 + price from CBOE
    try:
        q = fetch_cboe_quote(ticker)
        result["iv30"]      = round(q["iv30"], 1)
        result["iv_source"] = "cboe_iv30"
        result["price"]     = round(q["price"], 2) if q["price"] else None
        result["sources"].append(f"CBOE IV30 ({q['session_date']})")
        record_iv_readings({ticker: (q["session_date"], q["iv30"])})
    except Exception as exc:
        result["errors"].append(f"CBOE IV30 unavailable: {exc}")

    if result["price"] is None:
        try:
            p = _get_price(t)
            if p:
                result["price"] = round(p, 2)
        except Exception:
            pass

    # 30-day realized vol and IV/HV
    try:
        hv = fetch_hv30(t)
        if hv:
            result["hv30"] = round(hv, 1)
            if result["iv30"]:
                result["iv_hv"] = round(result["iv30"] / hv, 2)
    except Exception as exc:
        result["errors"].append(f"HV30 unavailable: {exc}")

    # IV rank from stored IV30 history
    if result["iv30"] is not None:
        rank, n = iv_rank_from_history(ticker, result["iv30"])
        result["iv_rank"]      = rank
        result["iv_rank_days"] = n

    # P/C ratio via yfinance options chain
    yf_pc, yf_pc_err = fetch_pc_ratio_yfinance(ticker)
    if yf_pc is not None:
        result["pc_ratio"]  = yf_pc
        result["pc_source"] = "yfinance_options"
        result["sources"].append(f"yfinance options chain ({ticker})")
    else:
        result["errors"].append(yf_pc_err or "P/C ratio unavailable")

    earn_date, earn_status = fetch_earnings_status(ticker)
    result["earnings_date"]   = earn_date
    result["earnings_status"] = earn_status

    result["premium_pts"] = premium_points(result["iv_hv"], result["iv_rank"])
    result["score"] = compute_score(result["premium_pts"], result["pc_ratio"])
    result["setup"] = compute_setup(result["iv30"], result["premium_pts"], result["pc_ratio"])

    return result


def run_full_scan(progress_cb=None):
    results = []
    total = len(WATCHLIST)
    completed_count = [0]
    lock = threading.Lock()

    def _scan(ticker):
        r = scan_ticker(ticker)
        with lock:
            completed_count[0] += 1
            if progress_cb:
                progress_cb(ticker, completed_count[0], total, r)
        return r

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_scan, t): t for t in WATCHLIST}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as exc:
                t = futures[fut]
                results.append({
                    "ticker": t, "company": COMPANY_NAMES.get(t, t),
                    "sector": _sector(t), "price": None,
                    "iv30": None, "hv30": None, "iv_hv": None,
                    "iv_rank": None, "iv_rank_days": 0, "iv_source": None,
                    "premium_pts": 0,
                    "pc_ratio": None, "pc_source": None,
                    "earnings_date": None, "earnings_status": "earn_date_unknown",
                    "score": 0, "setup": "Error",
                    "sources": [], "errors": [str(exc)],
                })

    results.sort(key=lambda x: (x["score"], x["iv_hv"] or 0), reverse=True)

    # top_3: clean bull put setups, no earnings exposure
    top_3 = [
        r for r in results
        if r["setup"] == "Bull put spread"
        and r["earnings_status"] not in ("earn_risk", "vol_crushed", "earn_date_unknown")
        and r["score"] >= 3
    ][:3]

    earn_risk         = sum(1 for r in results if r["earnings_status"] == "earn_risk")
    vol_crushed       = sum(1 for r in results if r["earnings_status"] == "vol_crushed")
    earn_date_unknown = sum(1 for r in results if r["earnings_status"] == "earn_date_unknown")
    eligible          = sum(1 for r in results if r["earnings_status"] not in ("earn_risk", "vol_crushed", "earn_date_unknown"))
    rank_days         = max([r.get("iv_rank_days") or 0 for r in results] + [0])

    return {
        "scan_time":               datetime.now().isoformat(),
        "tickers_scanned":         len(results),
        "earnings_risk_count":     earn_risk,
        "vol_crushed_count":       vol_crushed,
        "earn_date_unknown_count": earn_date_unknown,
        "eligible_count":          eligible,
        "iv_rank_days":            rank_days,
        "iv_rank_min_days":        IV_RANK_MIN_DAYS,
        "ivhv_strong":             IVHV_STRONG,
        "ivhv_ok":                 IVHV_OK,
        "top_3":                   top_3,
        "results":                 results,
    }
