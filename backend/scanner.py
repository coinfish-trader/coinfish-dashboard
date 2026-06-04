import re
import json
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import yfinance as yf

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

WATCHLIST = [
    "NVDA", "TSLA", "AAPL", "AMD", "AMZN", "GOOGL", "NFLX", "MSFT", "ORCL", "META",
    "BAC", "WFC", "C", "JPM", "MS", "SCHW", "COF", "AXP", "GS",
    "XOM", "SLB", "CVX", "OXY", "COP", "EOG", "VLO", "MPC",
    "PFE", "MRK", "JNJ", "BMY", "ABBV", "LLY", "TMO", "AMGN",
    "WMT", "NKE", "DIS", "SBUX", "HD", "TGT", "LOW", "COST", "MCD",
    "HAL", "HON", "BA", "MMM", "RTX", "UPS", "GE", "CAT", "DE", "UNP", "LMT",
]

COMPANY_NAMES = {
    "NVDA": "NVIDIA Corp", "TSLA": "Tesla Inc", "AAPL": "Apple Inc",
    "AMD": "Advanced Micro Devices", "AMZN": "Amazon.com", "GOOGL": "Alphabet Inc",
    "NFLX": "Netflix Inc", "MSFT": "Microsoft Corp", "ORCL": "Oracle Corp",
    "META": "Meta Platforms", "BAC": "Bank of America", "WFC": "Wells Fargo",
    "C": "Citigroup Inc", "JPM": "JPMorgan Chase", "MS": "Morgan Stanley",
    "SCHW": "Charles Schwab", "COF": "Capital One", "AXP": "American Express",
    "GS": "Goldman Sachs", "XOM": "ExxonMobil", "SLB": "SLB (Schlumberger)",
    "CVX": "Chevron Corp", "OXY": "Occidental Petroleum", "COP": "ConocoPhillips",
    "EOG": "EOG Resources", "VLO": "Valero Energy", "MPC": "Marathon Petroleum",
    "PFE": "Pfizer Inc", "MRK": "Merck & Co", "JNJ": "Johnson & Johnson",
    "BMY": "Bristol-Myers Squibb", "ABBV": "AbbVie Inc", "LLY": "Eli Lilly",
    "TMO": "Thermo Fisher Scientific", "AMGN": "Amgen Inc", "WMT": "Walmart Inc",
    "NKE": "Nike Inc", "DIS": "Walt Disney Co", "SBUX": "Starbucks Corp",
    "HD": "Home Depot", "TGT": "Target Corp", "LOW": "Lowe's Companies",
    "COST": "Costco Wholesale", "MCD": "McDonald's Corp", "HAL": "Halliburton",
    "HON": "Honeywell International", "BA": "Boeing Co", "MMM": "3M Company",
    "RTX": "RTX Corporation", "UPS": "United Parcel Service", "GE": "GE Aerospace",
    "CAT": "Caterpillar Inc", "DE": "Deere & Company", "UNP": "Union Pacific",
    "LMT": "Lockheed Martin",
}

SECTORS = {
    "Tech":        ["NVDA", "TSLA", "AAPL", "AMD", "AMZN", "GOOGL", "NFLX", "MSFT", "ORCL", "META"],
    "Financials":  ["BAC", "WFC", "C", "JPM", "MS", "SCHW", "COF", "AXP", "GS"],
    "Energy":      ["XOM", "SLB", "CVX", "OXY", "COP", "EOG", "VLO", "MPC"],
    "Healthcare":  ["PFE", "MRK", "JNJ", "BMY", "ABBV", "LLY", "TMO", "AMGN"],
    "Consumer":    ["WMT", "NKE", "DIS", "SBUX", "HD", "TGT", "LOW", "COST", "MCD"],
    "Industrials": ["HAL", "HON", "BA", "MMM", "RTX", "UPS", "GE", "CAT", "DE", "UNP", "LMT"],
}

def _sector(ticker):
    for s, tickers in SECTORS.items():
        if ticker in tickers:
            return s
    return "Other"

_BC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}

_bc_session = None
_bc_session_lock = threading.Lock()

def _get_bc_session():
    global _bc_session
    with _bc_session_lock:
        if _bc_session is None:
            if HAS_CURL_CFFI:
                from curl_cffi import requests as cr
                _bc_session = cr.Session(impersonate="chrome124")
            else:
                _bc_session = requests.Session()
                _bc_session.headers.update(_BC_HEADERS)
    return _bc_session

def _parse_barchart_html(html, ticker):
    iv_rank = None
    pc_ratio = None
    source_note = "barchart_html"

    if iv_rank is None:
        m = re.search(r'"ivRank(?:52Week)?"\s*:\s*([0-9.]+)', html)
        if m:
            iv_rank = float(m.group(1))
    if pc_ratio is None:
        m = re.search(r'"pcRatio"\s*:\s*([0-9.]+)', html)
        if m:
            pc_ratio = float(m.group(1))

    if iv_rank is None or pc_ratio is None:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        if iv_rank is None:
            for pattern in [
                r'IV\s+Rank\s*:?\s*(\d+(?:\.\d+)?)\s*%?',
                r'IV\s+Percentile\s*:?\s*(\d+(?:\.\d+)?)\s*%?',
                r'ivRank["\s:]+(\d+(?:\.\d+)?)',
            ]:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    iv_rank = float(m.group(1))
                    break

        if pc_ratio is None:
            for pattern in [
                r'Put[/\s-]?Call\s+(?:OI\s+)?Ratio\s*:?\s*(\d+(?:\.\d+)?)',
                r'P/C\s+(?:OI\s+)?Ratio\s*:?\s*(\d+(?:\.\d+)?)',
                r'Put[/\-]Call\s*:\s*(\d+(?:\.\d+)?)',
            ]:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    pc_ratio = float(m.group(1))
                    break

        if iv_rank is None or pc_ratio is None:
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                    for i, cell in enumerate(cells):
                        cl = cell.lower()
                        if iv_rank is None and "iv rank" in cl and i + 1 < len(cells):
                            m = re.search(r"(\d+(?:\.\d+)?)", cells[i + 1])
                            if m:
                                iv_rank = float(m.group(1))
                        if pc_ratio is None and ("put/call" in cl or "p/c" in cl) and i + 1 < len(cells):
                            m = re.search(r"(\d+(?:\.\d+)?)", cells[i + 1])
                            if m:
                                pc_ratio = float(m.group(1))

    return iv_rank, pc_ratio, source_note

def fetch_barchart(ticker):
    url = f"https://www.barchart.com/stocks/quotes/{ticker}/put-call-ratios"
    try:
        session = _get_bc_session()
        if HAS_CURL_CFFI:
            resp = session.get(url, timeout=15, headers={"Referer": "https://www.barchart.com/"})
        else:
            resp = session.get(url, timeout=15, headers={**_BC_HEADERS, "Referer": "https://www.barchart.com/"})

        if resp.status_code != 200:
            return None, None, None, f"Barchart HTTP {resp.status_code}"

        iv_rank, pc_ratio, note = _parse_barchart_html(resp.text, ticker)
        if iv_rank is None and pc_ratio is None:
            return None, None, None, "Barchart: could not parse IV rank or P/C ratio"

        return iv_rank, pc_ratio, url, None

    except Exception as exc:
        return None, None, None, f"Barchart: {exc}"

def fetch_pc_ratio_yfinance(ticker):
    try:
        t = yf.Ticker(ticker)
        exps = t.options
        if not exps:
            return None, "yfinance: no option expirations"

        total_call_oi = 0
        total_put_oi = 0
        used = 0
        for exp in exps[:min(4, len(exps))]:
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
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = getattr(info, "last_price", None) or getattr(info, "regularMarketPrice", None)
        if not price:
            hist_price = t.history(period="1d")
            if hist_price.empty:
                return None, "yfinance: no price"
            price = float(hist_price["Close"].iloc[-1])

        exps = t.options
        if not exps:
            return None, "yfinance: no expirations"

        chain = t.option_chain(exps[0])
        calls = chain.calls.copy()
        if calls.empty:
            return None, "yfinance: no calls"

        calls["dist"] = abs(calls["strike"] - price)
        atm = calls.nsmallest(1, "dist").iloc[0]
        current_iv = float(atm["impliedVolatility"])
        if current_iv <= 0 or current_iv != current_iv:
            return None, "yfinance: invalid ATM IV"

        hist = t.history(period="1y")
        if len(hist) < 60:
            return None, "yfinance: insufficient price history"

        log_ret = np.log(hist["Close"] / hist["Close"].shift(1)).dropna()
        hv_series = log_ret.rolling(30).std() * np.sqrt(252)
        hv_clean = hv_series.dropna()
        if hv_clean.empty:
            return None, "yfinance: could not compute HV"

        hv_min, hv_max = float(hv_clean.min()), float(hv_clean.max())
        if hv_max == hv_min:
            return 50.0, None

        rank = round(((current_iv - hv_min) / (hv_max - hv_min)) * 100, 1)
        return max(0.0, min(100.0, rank)), None

    except Exception as exc:
        return None, f"yfinance IV approx: {exc}"

def fetch_squeeze(ticker):
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="3mo")
        if len(hist) < 25:
            return None, None, False, "insufficient history"

        close = hist["Close"]
        high  = hist["High"]
        low   = hist["Low"]
        n = 20

        sma      = close.rolling(n).mean()
        std      = close.rolling(n).std(ddof=0)
        bb_upper = sma + 2.0 * std
        bb_lower = sma - 2.0 * std

        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(n).mean()

        kc_mid   = close.ewm(span=n, adjust=False).mean()
        kc_upper = kc_mid + 1.5 * atr
        kc_lower = kc_mid - 1.5 * atr

        cur_bb_upper = float(bb_upper.iloc[-1])
        cur_bb_lower = float(bb_lower.iloc[-1])
        cur_kc_upper = float(kc_upper.iloc[-1])
        cur_kc_lower = float(kc_lower.iloc[-1])

        if any(np.isnan([cur_bb_upper, cur_bb_lower, cur_kc_upper, cur_kc_lower])):
            return None, None, False, "NaN in squeeze calculation"

        # Squeeze state for current AND previous bar
        sq_series   = (bb_lower > kc_lower) & (bb_upper < kc_upper)
        current_sq  = bool(sq_series.iloc[-1])
        prev_sq     = bool(sq_series.iloc[-2]) if len(sq_series) >= 2 else current_sq
        # Fired = squeeze was ON last bar, OFF this bar (the release signal)
        fired       = (prev_sq is True) and (current_sq is False)

        highest_high = high.rolling(n).max()
        lowest_low   = low.rolling(n).min()
        momentum     = close - ((highest_high + lowest_low) / 2 + sma) / 2

        cur_mom  = float(momentum.iloc[-1])
        prev_mom = float(momentum.iloc[-2]) if len(momentum) >= 2 else cur_mom

        if cur_mom > 0 and cur_mom > prev_mom:
            squeeze_dir = "bull_strong"
        elif cur_mom > 0 and cur_mom <= prev_mom:
            squeeze_dir = "bull_weak"
        elif cur_mom < 0 and cur_mom < prev_mom:
            squeeze_dir = "bear_strong"
        elif cur_mom < 0 and cur_mom >= prev_mom:
            squeeze_dir = "bear_weak"
        else:
            squeeze_dir = "neutral"

        return current_sq, squeeze_dir, fired, None

    except Exception as exc:
        return None, None, False, f"squeeze: {exc}"

# Earnings
def fetch_earnings_status(ticker):
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        today = date.today()
        earnings_date = None

        if cal is None:
            return None, "unknown"

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
            return None, "unknown"

        delta = (earnings_date - today).days
        if delta < -7:
            status = "clear"
        elif -7 <= delta <= 0:
            status = "vol_crushed"
        elif 1 <= delta <= 14:
            status = "earn_risk"
        else:
            status = "clear"

        return earnings_date.isoformat(), status

    except Exception:
        return None, "unknown"

def compute_score(iv_rank, pc_ratio, squeeze_on, squeeze_dir):
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
    # Squeeze bonus: loaded and pointing up = +1
    if squeeze_on and squeeze_dir in ("bull", "bull_fading"):
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
        "squeeze_on":      None,
        "squeeze_dir":     None,
        "earnings_date":   None,
        "earnings_status": "unknown",
        "score":           0,
        "setup":           "No Data",
        "sources":         [],
        "errors":          [],
    }

    # Price
    try:
        info = yf.Ticker(ticker).fast_info
        p = getattr(info, "last_price", None) or getattr(info, "regularMarketPrice", None)
        if p:
            result["price"] = round(float(p), 2)
    except Exception:
        pass

    # Barchart IV + P/C
    bc_url = f"https://www.barchart.com/stocks/quotes/{ticker}/put-call-ratios"
    result["sources"].append(bc_url)

    iv_rank, pc_ratio, bc_source, bc_error = fetch_barchart(ticker)

    if iv_rank is not None:
        result["iv_rank"]   = round(iv_rank, 1)
        result["iv_source"] = "barchart"
    else:
        result["errors"].append(bc_error or "Barchart IV unavailable")

    if pc_ratio is not None:
        result["pc_ratio"]  = pc_ratio
        result["pc_source"] = "barchart"

    # yfinance P/C fallback
    if result["pc_ratio"] is None:
        yf_pc, yf_pc_err = fetch_pc_ratio_yfinance(ticker)
        if yf_pc is not None:
            result["pc_ratio"]  = yf_pc
            result["pc_source"] = "yfinance_options"
            result["sources"].append(f"yfinance options chain ({ticker})")
        else:
            result["errors"].append(yf_pc_err or "yfinance P/C unavailable")

    # yfinance IV fallback
    if result["iv_rank"] is None:
        yf_iv, yf_iv_err = fetch_iv_rank_approx(ticker)
        if yf_iv is not None:
            result["iv_rank"]   = round(yf_iv, 1)
            result["iv_source"] = "yfinance_approx"
            result["sources"].append(f"yfinance HV approx ({ticker})")
        else:
            result["errors"].append(yf_iv_err or "yfinance IV approx unavailable")

    # TTM Squeeze
    sq_on, sq_dir, sq_fired, sq_err = fetch_squeeze(ticker)
    result["squeeze_on"]  = sq_on
    result["squeeze_dir"] = sq_dir
    if sq_err:
        result["errors"].append(sq_err)

    # Earnings
    earn_date, earn_status = fetch_earnings_status(ticker)
    result["earnings_date"]   = earn_date
    result["earnings_status"] = earn_status

    result["score"] = compute_score(
        result["iv_rank"], result["pc_ratio"],
        result["squeeze_on"], result["squeeze_dir"]
    )
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
                    "squeeze_on": None, "squeeze_dir": None,
                    "earnings_date": None, "earnings_status": "unknown",
                    "score": 0, "setup": "Error",
                    "sources": [], "errors": [str(exc)],
                })

    results.sort(key=lambda x: (x["score"], x["iv_rank"] or 0), reverse=True)

    top_3 = [
        r for r in results
        if r["setup"] == "Bull put spread"
        and r["earnings_status"] not in ("earn_risk", "vol_crushed")
        and r["score"] >= 3
    ][:3]

    earn_risk   = sum(1 for r in results if r["earnings_status"] == "earn_risk")
    vol_crushed = sum(1 for r in results if r["earnings_status"] == "vol_crushed")
    eligible    = sum(1 for r in results if r["earnings_status"] not in ("earn_risk", "vol_crushed"))
    squeeze_count = sum(1 for r in results if r["squeeze_on"] is True)

    return {
        "scan_time":           datetime.now().isoformat(),
        "tickers_scanned":     len(results),
        "earnings_risk_count": earn_risk,
        "vol_crushed_count":   vol_crushed,
        "eligible_count":      eligible,
        "squeeze_count":       squeeze_count,
        "top_3":               top_3,
        "results":             results,
    }


def run_squeeze_scan():
    """
    Standalone TTM Squeeze scan across the full watchlist.
    Returns summary counts + per-ticker squeeze results, sorted by
    fired first, then squeeze_on, then absolute momentum.
    """
    results = []
    lock = threading.Lock()

    def _scan(ticker):
        sq_on, sq_dir, fired, sq_err = fetch_squeeze(ticker)
        r = {
            "ticker":             ticker,
            "company":            COMPANY_NAMES.get(ticker, ticker),
            "sector":             _sector(ticker),
            "squeeze_on":         sq_on,
            "squeeze_dir":        sq_dir,
            "momentum_direction": sq_dir,
            "fired":              fired,
            "error":              sq_err,
        }
        with lock:
            results.append(r)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_scan, WATCHLIST))

    results.sort(
        key=lambda x: (
            1 if x.get("fired")      else 0,
            1 if x.get("squeeze_on") else 0,
        ),
        reverse=True,
    )

    in_squeeze  = sum(1 for r in results if r.get("squeeze_on") is True)
    fired_count = sum(1 for r in results if r.get("fired") is True)
    bull_count  = sum(1 for r in results if (r.get("squeeze_dir") or "").startswith("bull"))
    bear_count  = sum(1 for r in results if (r.get("squeeze_dir") or "").startswith("bear"))

    return {
        "scan_time":     datetime.now().isoformat(),
        "in_squeeze":    in_squeeze,
        "fired":         fired_count,
        "bull_momentum": bull_count,
        "bear_momentum": bear_count,
        "results":       results,
    }
