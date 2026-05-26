"""
squeeze.py  —  TTM Squeeze scanner for Coinfish Dashboard
Drop this file into the backend/ folder alongside scanner.py and app.py.

TTM Squeeze logic:
  Bollinger Bands (20, 2.0) inside Keltner Channels (20, 1.5x ATR) = compression.
  When BBs expand back outside KCs the squeeze "fires."
  Momentum direction (bullish/bearish) indicates the likely breakout bias.
"""

import threading
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from scanner import WATCHLIST, COMPANY_NAMES, _sector


# ── TTM Squeeze calculation ────────────────────────────────────────────────────

def calc_squeeze(ticker: str) -> dict:
    """
    Returns a dict with:
      ticker, company, sector, price,
      squeeze_state   : 'squeezing' | 'fired' | 'fired_recently' | 'none'
      bars_in_squeeze : int (only set when state == 'squeezing')
      momentum_dir    : 'bullish_up' | 'bullish_down' | 'bearish_down' | 'bearish_up'
      momentum_val    : float (raw TTM momentum value)
      error           : bool
      error_msg       : str (only set on error)
    """
    try:
        df = yf.download(
            ticker,
            period="6mo",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )

        # yfinance v0.2+ returns multi-index columns for single tickers
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if df is None or len(df) < 25:
            return _err(ticker, "Insufficient data")

        close = df["Close"]
        high  = df["High"]
        low   = df["Low"]
        n     = 20

        # ── Bollinger Bands (20, 2.0) ──────────────────────────────────────────
        bb_mid   = close.rolling(n).mean()
        bb_std   = close.rolling(n).std()
        bb_upper = bb_mid + 2.0 * bb_std
        bb_lower = bb_mid - 2.0 * bb_std

        # ── True Range and ATR (20) ────────────────────────────────────────────
        prev_close = close.shift(1)
        tr = pd.concat(
            [high - low,
             (high - prev_close).abs(),
             (low  - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(n).mean()

        # ── Keltner Channels (20, 1.5x ATR) ───────────────────────────────────
        kc_mid   = close.rolling(n).mean()
        kc_upper = kc_mid + 1.5 * atr
        kc_lower = kc_mid - 1.5 * atr

        # ── Squeeze: BB fully inside KC ────────────────────────────────────────
        squeeze_on = (bb_upper < kc_upper) & (bb_lower > kc_lower)

        # ── Momentum (TTM-style) ───────────────────────────────────────────────
        # val = close - ((highest_high_20 + lowest_low_20) / 2 + bb_mid) / 2
        hh  = high.rolling(n).max()
        ll  = low.rolling(n).min()
        mom = close - ((hh + ll) / 2.0 + bb_mid) / 2.0

        # Drop NaN rows introduced by rolling windows
        valid_idx = squeeze_on.dropna().index
        if len(valid_idx) < 5:
            return _err(ticker, "Insufficient clean data after rolling")

        sq = squeeze_on.reindex(valid_idx)
        mo = mom.reindex(valid_idx)
        pr = close.reindex(valid_idx)

        cur_sq   = bool(sq.iloc[-1])
        prev_sq  = bool(sq.iloc[-2])
        mom_now  = float(mo.iloc[-1])
        mom_prev = float(mo.iloc[-2])

        # ── State ──────────────────────────────────────────────────────────────
        bars_in_squeeze = 0

        if cur_sq:
            state = "squeezing"
            for i in range(len(sq) - 1, -1, -1):
                if sq.iloc[i]:
                    bars_in_squeeze += 1
                else:
                    break

        elif prev_sq and not cur_sq:
            # Current bar is the first bar out of the squeeze
            state = "fired"

        else:
            # Look back up to 5 bars for a squeeze-end transition
            state = "none"
            lookback = sq.iloc[-6:]  # last 6 bars (current + 5 prior)
            for i in range(len(lookback) - 2, 0, -1):
                was_sq  = bool(lookback.iloc[i])
                next_sq = bool(lookback.iloc[i + 1])
                if was_sq and not next_sq:
                    state = "fired_recently"
                    break

        # ── Momentum direction ─────────────────────────────────────────────────
        if mom_now >= 0:
            mom_dir = "bullish_up"   if mom_now > mom_prev else "bullish_down"
        else:
            mom_dir = "bearish_down" if mom_now < mom_prev else "bearish_up"

        return {
            "ticker":          ticker,
            "company":         COMPANY_NAMES.get(ticker, ticker),
            "sector":          _sector(ticker),
            "price":           round(float(pr.iloc[-1]), 2),
            "squeeze_state":   state,
            "bars_in_squeeze": bars_in_squeeze if state == "squeezing" else None,
            "momentum_dir":    mom_dir,
            "momentum_val":    round(mom_now, 4),
            "error":           False,
        }

    except Exception as exc:
        return _err(ticker, str(exc))


def _err(ticker: str, msg: str) -> dict:
    return {
        "ticker":    ticker,
        "company":   COMPANY_NAMES.get(ticker, ticker),
        "error":     True,
        "error_msg": msg,
    }


# ── Full watchlist scan (mirrors run_full_scan pattern in scanner.py) ─────────

def run_squeeze_scan(progress_cb=None) -> dict:
    """
    Scans every ticker in WATCHLIST for squeeze state.
    progress_cb(ticker, completed, total, result) called after each ticker.
    Returns a summary dict matching the shape expected by the frontend.
    """
    results         = []
    total           = len(WATCHLIST)
    completed_count = [0]
    lock            = threading.Lock()

    def _scan(ticker):
        r = calc_squeeze(ticker)
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

    # Sort: fired first, then squeezing (longest first), recently fired, none, errors last
    _order = {"fired": 0, "squeezing": 1, "fired_recently": 2, "none": 3}
    results.sort(key=lambda x: (
        99 if x.get("error") else _order.get(x.get("squeeze_state", "none"), 3),
        -(x.get("bars_in_squeeze") or 0),
    ))

    clean = [r for r in results if not r.get("error")]

    return {
        "scan_time":          datetime.now().isoformat(),
        "tickers_scanned":    len(results),
        "squeezing_count":    sum(1 for r in clean if r["squeeze_state"] == "squeezing"),
        "fired_count":        sum(1 for r in clean if r["squeeze_state"] in ("fired", "fired_recently")),
        "bullish_fire_count": sum(
            1 for r in clean
            if r["squeeze_state"] in ("fired", "fired_recently")
            and r.get("momentum_dir", "").startswith("bullish")
        ),
        "results": results,
    }
