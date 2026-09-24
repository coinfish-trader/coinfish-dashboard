import time
import json
import math
import threading
import numpy as np
from pathlib import Path
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf


# ---------------------------------------------------------------------------
# Black-Scholes IV solver (avoids relying on yfinance impliedVolatility field,
# which returns near-zero garbage when bid/ask quotes are unavailable)
# ---------------------------------------------------------------------------
def _bs_call(S, K, T, r, sigma):
    """Black-Scholes call price."""
    if T <= 0 or sigma <= 0:
        return max(S - K * math.exp(-r * T), 0.0)
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    N = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2)))
    return S * N(d1) - K * math.exp(-r * T) * N(d2)


def _solve_iv(S, K, T, r, market_price, tol=1e-4, max_iter=80):
    """Return annualized IV (decimal) or None if unsolvable."""
    intrinsic = max(S - K * math.exp(-r * T), 0.0)
    if market_price <= intrinsic + 1e-6:
        return None
    lo, hi = 0.005, 5.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        val = _bs_call(S, K, T, r, mid)
        if abs(val - market_price) < tol:
            return mid
        if val < market_price:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0

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
# IV history store
#
# Each full scan records the calculated ATM IV for every ticker into a JSON
# file (iv_history.json, same directory as this file). IV rank is then
# computed as where today's IV sits within the range of past readings --
# IV vs. IV, the correct comparison.
#
# The file persists across requests within a deploy but resets on redeploy.
# 10+ readings are required before IV rank is shown; the scanner falls back
# to an HV-based proxy (labeled "hv_proxy") while history is building.
# One reading per ticker per calendar day; last 365 days retained.
# ---------------------------------------------------------------------------
_IV_HISTORY_PATH = Path(__file__).parent / "iv_history.json"
_iv_history = {}  # {ticker: [[date_str, iv_float], ...]}


def _load_iv_history():
    global _iv_history
    try:
        if _IV_HISTORY_PATH.exists():
            with open(_IV_HISTORY_PATH) as f:
                _iv_history = json.load(f)
    except Exception:
        _iv_history = {}


def _save_iv_history():
    try:
        with open(_IV_HISTORY_PATH, "w") as f:
            json.dump(_iv_history, f)
    except Exception:
        pass


def _record_iv(ticker, iv_value):
    today = date.today().isoformat()
    if ticker not in _iv_history:
        _iv_history[ticker] = []
    entries = _iv_history[ticker]
    # One entry per day -- replace if already recorded today
    entries = [[d, v] for d, v in entries if d != today]
    entries.append([today, round(iv_value, 4)])
    # Trim to last 365 calendar days
    cutoff = (date.today() - timedelta(days=365)).isoformat()
    _iv_history[ticker] = [[d, v] for d, v in entries if d >= cutoff]


def _iv_rank_from_history(ticker, current_iv):
    """
    IV Rank: position of current IV within the 52-week range of stored IV readings.
    Returns (rank_pct, n_readings). rank_pct is None when n_readings < 10.
    """
    entries = _iv_history.get(ticker, [])
    n = len(entries)
    if n < 10:
        return None, n
    ivs = [v for _, v in entries]
    lo, hi = min(ivs), max(ivs)
    if hi <= lo:
        return 50.0, n
    rank = (current_iv - lo) / (hi - lo) * 100.0
    return round(min(max(rank, 0.0), 100.0), 1), n


_load_iv_history()


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


def fetch_iv_rank_approx(ticker):
    """
    Calculates ATM implied vol via Black-Scholes, records it in IV history,
    and ranks it against that history (IV vs. IV -- correct comparison).

    Returns (rank_pct, source_label) on success, (None, error_msg) on failure.
    source_label values:
      "iv_history"       -- ranked against own IV history (correct)
      "hv_proxy (N/10)"  -- fallback while history builds; IV vs. realized HV,
                            inflated by vol risk premium, treat as approximate
    """
    try:
        t = yf.Ticker(ticker)

        price = _get_price(t)
        if not price:
            return None, "yfinance: no price"

        exps = t.options
        if not exps:
            return None, "yfinance: no expirations"

        # Sample ATM IV from the first 3 expirations with >= 7 DTE
        iv_samples = []
        today = date.today()
        r = 0.045  # risk-free rate estimate
        sampled = 0
        for exp in exps:
            if sampled >= 3:
                break
            dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
            if dte < 7:
                continue  # skip ultra-short-dated; IV unreliable
            T = dte / 365.0  # calendar days -- NOT 252 trading days
            try:
                chain = t.option_chain(exp)
                for leg in (chain.calls, chain.puts):
                    if leg.empty:
                        continue
                    leg = leg.copy()
                    leg["dist"] = abs(leg["strike"] - price)
                    atm_row = leg.nsmallest(1, "dist").iloc[0]
                    K = float(atm_row["strike"])
                    # prefer mid-price; fall back to lastPrice
                    bid = float(atm_row.get("bid", 0) or 0)
                    ask = float(atm_row.get("ask", 0) or 0)
                    opt_price = (bid + ask) / 2.0 if bid > 0 and ask > 0 else float(atm_row.get("lastPrice", 0) or 0)
                    if opt_price <= 0:
                        continue
                    iv = _solve_iv(price, K, T, r, opt_price)
                    if iv and 0.01 <= iv <= 3.0:
                        iv_samples.append(iv)
            except Exception:
                continue
            sampled += 1

        if not iv_samples:
            return None, "yfinance: could not sample ATM IV"

        current_iv = float(np.mean(iv_samples))

        # Record today's reading into IV history
        _record_iv(ticker, current_iv)

        # Primary: rank against own IV history (IV vs. IV)
        iv_rank, n_readings = _iv_rank_from_history(ticker, current_iv)
        if iv_rank is not None:
            return iv_rank, "iv_history"

        # Fallback: rank against realized HV while history is building.
        # This overstates IV rank because IV carries a vol risk premium above HV.
        # The label makes the approximation visible in the output.
        hist = t.history(period="1y")
        if len(hist) < 60:
            return None, f"yfinance: insufficient price history ({n_readings}/10 IV readings)"

        log_ret = np.log(hist["Close"] / hist["Close"].shift(1)).dropna()
        hv_series = log_ret.rolling(21).std() * np.sqrt(252)
        hv_vals = hv_series.dropna().values

        if len(hv_vals) == 0:
            return None, "yfinance: could not compute HV"

        pct_rank = float(np.mean(hv_vals < current_iv)) * 100
        return round(pct_rank, 1), f"hv_proxy ({n_readings}/10 IV readings)"

    except Exception as exc:
        return None, f"yfinance IV approx: {exc}"


def fetch_earnings_status(ticker):
    """
    Returns (earnings_date_iso_or_None, status).
    status values:
      "clear"             -- earnings confirmed more than 21 days away (or > 7 days past)
      "earn_risk"         -- earnings within the next 1-14 days
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
        elif 1 <= delta <= 14:   # 14d window — flag only near-term earnings risk
            status = "earn_risk"
        else:
            status = "clear"

        return earnings_date.isoformat(), status

    except Exception:
        return None, "earn_date_unknown"


def compute_score(iv_rank, pc_ratio):
    score = 0
    if iv_rank is not None:
        if iv_rank > 50:
            score += 2
        elif iv_rank >= 35:
            score += 1
    if pc_ratio is not None:
        if pc_ratio < 0.70:
            score += 2
        elif pc_ratio <= 0.80:
            score += 1
    return score


def compute_setup(iv_rank, pc_ratio):
    if iv_rank is None:
        return "No Data"
    if iv_rank > 50:
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
        "iv_rank":         None,
        "iv_source":       None,
        "pc_ratio":        None,
        "pc_source":       None,
        "earnings_date":   None,
        "earnings_status": "earn_date_unknown",
        "score":           0,
        "setup":           "No Data",
        "sources":         [],
        "errors":          [],
    }

    try:
        t = yf.Ticker(ticker)
        p = _get_price(t)
        if p:
            result["price"] = round(p, 2)
    except Exception:
        pass

    # IV rank
    yf_iv, yf_iv_info = fetch_iv_rank_approx(ticker)
    if yf_iv is not None:
        result["iv_rank"]   = round(yf_iv, 1)
        result["iv_source"] = yf_iv_info
        result["sources"].append(f"IV rank ({yf_iv_info}) for {ticker}")
        if "hv_proxy" in yf_iv_info:
            result["errors"].append(f"IV rank approximate (HV proxy, inflated by VRP): {yf_iv_info}")
    else:
        result["errors"].append(yf_iv_info or "IV rank unavailable")

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

    result["score"] = compute_score(result["iv_rank"], result["pc_ratio"])
    result["setup"] = compute_setup(result["iv_rank"], result["pc_ratio"])

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
                    "iv_rank": None, "iv_source": None,
                    "pc_ratio": None, "pc_source": None,
                    "earnings_date": None, "earnings_status": "earn_date_unknown",
                    "score": 0, "setup": "Error",
                    "sources": [], "errors": [str(exc)],
                })

    # Persist IV history after every full scan
    _save_iv_history()

    results.sort(key=lambda x: (x["score"], x["iv_rank"] or 0), reverse=True)

    # top_3: exclude earn_date_unknown -- unknown date is not a clean setup
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

    return {
        "scan_time":               datetime.now().isoformat(),
        "tickers_scanned":         len(results),
        "earnings_risk_count":     earn_risk,
        "vol_crushed_count":       vol_crushed,
        "earn_date_unknown_count": earn_date_unknown,
        "eligible_count":          eligible,
        "top_3":                   top_3,
        "results":                 results,
    }
