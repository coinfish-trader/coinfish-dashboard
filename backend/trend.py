"""
trend.py  —  Bullish trend / debit-spread scanner for Coinfish Dashboard

Five conditions (all must pass to "qualify"):
  1. Price above the 50-day EMA
  2. 9 EMA above 21 EMA
  3. MACD bullish (MACD line above signal line) — 12/26/9
  4. Elder Force Index (13) positive
  5. IV Rank below 50 (so you're not overpaying)

For qualified names, pull the option chain and propose a 30-60 DTE
bull call debit spread (specific strikes, debit, max profit/risk, breakeven).

IV Rank reuses the same source as the options scanner (Barchart primary,
yfinance HV approximation fallback).
"""

import time
import threading
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

import scanner
from scanner import WATCHLIST, COMPANY_NAMES, _sector


# ── IV Rank (reuse options-scanner sources) ───────────────────────────────────

def _get_iv_rank(ticker):
    try:
        iv_rank, _pc, _url, _err = scanner.fetch_barchart(ticker)
        if iv_rank is not None:
            return round(float(iv_rank), 1), "barchart"
    except Exception:
        pass
    try:
        yf_iv, _err = scanner.fetch_iv_rank_approx(ticker)
        if yf_iv is not None:
            return round(float(yf_iv), 1), "yfinance_approx"
    except Exception:
        pass
    return None, None


# ── Debit spread suggestion ───────────────────────────────────────────────────

def _mid_price(row):
    """Best available price for an option row: mid of bid/ask, else lastPrice."""
    try:
        bid = float(row.get("bid", 0) or 0)
        ask = float(row.get("ask", 0) or 0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2.0, 2)
        last = float(row.get("lastPrice", 0) or 0)
        return round(last, 2) if last > 0 else None
    except Exception:
        return None


def suggest_debit_spread(ticker, price, attempts=3):
    """
    Propose a 30-60 DTE bull call debit spread:
      buy ~ATM call, sell a higher (~5% OTM) call.
    Retries a few times because Yahoo's options endpoint rate-limits
    cloud IPs harder than the price endpoint.
    Returns a dict or None if a spread can't be built.
    """
    for attempt in range(attempts):
        out = _try_spread(ticker, price)
        if out is not None:
            return out
        time.sleep(0.8 * (attempt + 1))
    return None


def _try_spread(ticker, price):
    try:
        t = yf.Ticker(ticker)
        exps = t.options
        if not exps:
            return None

        today = date.today()
        scored = []
        for e in exps:
            try:
                d = datetime.strptime(e, "%Y-%m-%d").date()
            except Exception:
                continue
            dte = (d - today).days
            if dte <= 0:
                continue
            scored.append((e, dte))
        if not scored:
            return None

        in_window = [s for s in scored if 30 <= s[1] <= 60]
        pool = in_window if in_window else scored
        exp, dte = min(pool, key=lambda s: abs(s[1] - 45))
        out_of_window = not in_window

        calls = t.option_chain(exp).calls
        if calls is None or calls.empty:
            return None
        calls = calls.dropna(subset=["strike"]).sort_values("strike")
        strikes = [float(s) for s in calls["strike"].tolist()]
        if len(strikes) < 2:
            return None

        # Long ~ ATM (nearest strike to price)
        long_strike = min(strikes, key=lambda s: abs(s - price))
        # Short ~ 5% OTM, must be above long
        target_short = price * 1.05
        higher = [s for s in strikes if s > long_strike]
        if not higher:
            return None
        short_strike = min(higher, key=lambda s: abs(s - target_short))

        long_row = calls[calls["strike"] == long_strike].iloc[0]
        short_row = calls[calls["strike"] == short_strike].iloc[0]
        long_mid = _mid_price(long_row)
        short_mid = _mid_price(short_row)
        if long_mid is None or short_mid is None:
            return None

        net_debit = round(long_mid - short_mid, 2)
        width = round(short_strike - long_strike, 2)
        max_profit = round(width - net_debit, 2)
        max_risk = net_debit
        breakeven = round(long_strike + net_debit, 2)
        rr = round(max_profit / max_risk, 2) if max_risk and max_risk > 0 else None

        return {
            "expiration":  exp,
            "dte":         dte,
            "out_of_window": out_of_window,
            "long_strike":  round(long_strike, 2),
            "short_strike": round(short_strike, 2),
            "width":        width,
            "debit":        net_debit,
            "max_profit":   max_profit,
            "max_risk":     max_risk,
            "breakeven":    breakeven,
            "risk_reward":  rr,
        }
    except Exception:
        return None


# ── Per-ticker trend evaluation ───────────────────────────────────────────────

def compute_trend(ticker: str) -> dict:
    try:
        df = yf.download(
            ticker, period="6mo", interval="1d",
            auto_adjust=True, progress=False,
        )
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Drop the in-progress / unsettled trailing bar with NaN OHLC.
        if df is not None:
            df = df.dropna(subset=["Close", "High", "Low"])

        if df is None or len(df) < 55:
            return _err(ticker, "Insufficient data")

        close = df["Close"].astype(float)
        volume = df["Volume"].fillna(0).astype(float) if "Volume" in df else pd.Series(0, index=close.index)

        ema9  = close.ewm(span=9,  adjust=False).mean()
        ema21 = close.ewm(span=21, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()

        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()

        # Elder Force Index (13-period EMA of (close change * volume))
        efi_raw = (close - close.shift(1)) * volume
        efi = efi_raw.ewm(span=13, adjust=False).mean()

        price = float(close.iloc[-1])
        v_ema9, v_ema21, v_ema50 = float(ema9.iloc[-1]), float(ema21.iloc[-1]), float(ema50.iloc[-1])
        v_macd, v_signal = float(macd_line.iloc[-1]), float(signal_line.iloc[-1])
        v_efi = float(efi.iloc[-1])

        c1 = price > v_ema50
        c2 = v_ema9 > v_ema21
        c3 = v_macd > v_signal
        c4 = v_efi > 0

        # IV Rank gate
        iv_rank, iv_source = _get_iv_rank(ticker)
        c5 = (iv_rank is not None) and (iv_rank < 50)

        conds = {
            "price_above_ema50": bool(c1),
            "ema9_above_ema21":  bool(c2),
            "macd_bullish":      bool(c3),
            "efi_positive":      bool(c4),
            "iv_below_50":       bool(c5),
        }
        passed = sum(1 for v in conds.values() if v)
        qualified = all(conds.values())

        # Spread is fetched in a deferred sequential pass (see run_trend_scan)
        # to avoid the concurrent options-endpoint rate limit.
        spread = None

        return {
            "ticker":      ticker,
            "company":     COMPANY_NAMES.get(ticker, ticker),
            "sector":      _sector(ticker),
            "price":       round(price, 2),
            "ema9":        round(v_ema9, 2),
            "ema21":       round(v_ema21, 2),
            "ema50":       round(v_ema50, 2),
            "macd":        round(v_macd, 3),
            "macd_signal": round(v_signal, 3),
            "efi":         round(v_efi, 1),
            "iv_rank":     iv_rank,
            "iv_source":   iv_source,
            "conditions":  conds,
            "passed":      passed,
            "qualified":   qualified,
            "spread":      spread,
            "error":       False,
        }

    except Exception as exc:
        return _err(ticker, str(exc))


def _err(ticker, msg):
    return {
        "ticker":    ticker,
        "company":   COMPANY_NAMES.get(ticker, ticker),
        "error":     True,
        "error_msg": msg,
    }


# ── Full watchlist scan ───────────────────────────────────────────────────────

def run_trend_scan(progress_cb=None) -> dict:
    results         = []
    total           = len(WATCHLIST)
    completed_count = [0]
    lock            = threading.Lock()

    def _scan(ticker):
        r = compute_trend(ticker)
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
                results.append(_err(t, str(exc)))

    # Deferred spread pass: sequential (with retries) over qualified names only,
    # so the options-endpoint calls don't get caught in the concurrent burst that
    # Yahoo rate-limits from cloud IPs.
    for r in results:
        if not r.get("error") and r.get("qualified"):
            r["spread"] = suggest_debit_spread(r["ticker"], r["price"])

    # Sort: qualified first, then by conditions passed (desc), then ticker
    results.sort(key=lambda x: (
        0 if x.get("qualified") else 1,
        -(x.get("passed") or 0),
        x.get("ticker", ""),
    ))

    clean = [r for r in results if not r.get("error")]

    return {
        "scan_time":       datetime.now().isoformat(),
        "tickers_scanned": len(results),
        "qualified_count": sum(1 for r in clean if r.get("qualified")),
        "near_miss_count": sum(1 for r in clean if not r.get("qualified") and (r.get("passed") or 0) == 4),
        "results":         results,
    }
