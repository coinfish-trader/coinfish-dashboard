"""
Coinfish Newsfeed - data source fetchers.

All sources here are free / keyless public endpoints:
  - SEC EDGAR "Latest Filings" real-time feed (getcurrent)
  - SEC EDGAR full-text search (used to pull recent Form 4 insider buys/sells)
  - Yahoo Finance per-ticker RSS + top-stories RSS
  - CNBC top news RSS, MarketWatch top stories RSS, Federal Reserve press releases RSS
  - Nasdaq public calendar JSON endpoints (dividends / IPOs / economic events)
  - ForexFactory weekly calendar XML (economic event importance ratings)
  - yfinance for watchlist price/% change (ticker tape)

None of these require an API key or account. Coverage is a step below true
paid wires (Benzinga's real API, PR Newswire structured feed, X/Twitter
monitoring, "Trump's Truths") which the Stock Trader Network product pays
for - those are not included here. See README for what's in vs out.
"""

import re
import html
import time
import difflib
import xml.etree.ElementTree as ET
from datetime import datetime as _dt, timedelta as _timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import feedparser
import yfinance as yf

SEC_HEADERS = {
    # SEC requires a descriptive User-Agent identifying the requester.
    "User-Agent": "Coinfish Trading Company billy@coinfishtraders.com",
    "Accept-Encoding": "gzip, deflate",
}

NASDAQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}

_session = requests.Session()


# ---------------------------------------------------------------------------
# SEC EDGAR - real-time filings feed
# ---------------------------------------------------------------------------

FORM_TYPES_DEFAULT = ["8-K", "4", "13D", "13G", "S-1", "424B", "6-K"]

def fetch_sec_current_filings(form_type="", count=100, timeout=12):
    """
    Pulls the SEC EDGAR 'Latest Filings' Atom feed.
    form_type="" returns all recent filings across all form types.
    """
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcurrent&type={form_type}&company=&dateb=&owner=include"
        f"&count={count}&output=atom"
    )
    try:
        resp = _session.get(url, headers=SEC_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"SEC EDGAR HTTP {resp.status_code}"

        root = ET.fromstring(resp.content)
        ns = {"a": "http://www.w3.org/2005/Atom"}
        out = []
        for entry in root.findall("a:entry", ns):
            title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
            link_el = entry.find("a:link", ns)
            link = link_el.get("href") if link_el is not None else None
            updated = entry.findtext("a:updated", default="", namespaces=ns)

            # Title looks like "8-K - Jet.AI Inc. (0001861622) (Filer)"
            m = re.match(r"^([\w\-/]+)\s*-\s*(.+?)\s*\((\d{10})\)", title)
            form, company, cik = (m.group(1), m.group(2), m.group(3)) if m else (None, title, None)

            out.append({
                "form": form,
                "company": company,
                "cik": cik,
                "title": title,
                "link": link,
                "updated": updated,
                "source": "SEC EDGAR",
            })
        return out, None
    except Exception as exc:
        return [], f"SEC EDGAR: {exc}"


def fetch_sec_filings_multi(form_types=None, count_per_type=40):
    """Fetch several form types in parallel and merge, newest first."""
    form_types = form_types or FORM_TYPES_DEFAULT
    results = []
    errors = []

    def _one(ft):
        rows, err = fetch_sec_current_filings(form_type=ft, count=count_per_type)
        return rows, err

    with ThreadPoolExecutor(max_workers=len(form_types)) as ex:
        futures = {ex.submit(_one, ft): ft for ft in form_types}
        for fut in as_completed(futures):
            rows, err = fut.result()
            results.extend(rows)
            if err:
                errors.append(err)

    # de-dupe by link, sort by updated desc
    seen = set()
    deduped = []
    for r in results:
        key = r.get("link") or r.get("title")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    deduped.sort(key=lambda r: r.get("updated") or "", reverse=True)
    return deduped, errors


# ---------------------------------------------------------------------------
# SEC EDGAR - full text search (used for insider Form 4 buy/sell headlines)
# ---------------------------------------------------------------------------

def fetch_sec_form4_recent(limit=25, timeout=12):
    """
    Recent Form 4 (insider transaction) filings via EDGAR full text search.
    Note: full text search indexes the filing, not the parsed transaction
    detail, so this returns filer/company + link, not dollar amounts.
    """
    url = "https://efts.sec.gov/LATEST/search-index?q=&forms=4"
    try:
        resp = _session.get(url, headers=SEC_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"SEC full-text search HTTP {resp.status_code}"
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])[:limit]
        out = []
        for h in hits:
            src = h.get("_source", {})
            names = src.get("display_names", [])
            out.append({
                "form": "4",
                "companies": names,
                "filed": src.get("file_date") or src.get("period_ending"),
                "id": h.get("_id"),
                "source": "SEC Form 4",
            })
        return out, None
    except Exception as exc:
        return [], f"SEC full-text search: {exc}"


# ---------------------------------------------------------------------------
# Yahoo Finance RSS - per-ticker + general market
# ---------------------------------------------------------------------------

def fetch_yahoo_ticker_news(ticker, timeout=10):
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        resp = _session.get(url, headers=YAHOO_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"Yahoo RSS HTTP {resp.status_code}"
        feed = feedparser.parse(resp.content)
        out = []
        for e in feed.entries:
            out.append({
                "ticker": ticker,
                "headline": e.get("title"),
                "link": e.get("link"),
                "published": e.get("published"),
                "source": "Yahoo Finance",
            })
        return out, None
    except Exception as exc:
        return [], f"Yahoo RSS ({ticker}): {exc}"


def fetch_yahoo_top_stories(timeout=10):
    url = "https://finance.yahoo.com/rss/topstories"
    try:
        resp = _session.get(url, headers=YAHOO_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"Yahoo top stories HTTP {resp.status_code}"
        feed = feedparser.parse(resp.content)
        out = []
        for e in feed.entries:
            out.append({
                "ticker": None,
                "headline": e.get("title"),
                "link": e.get("link"),
                "published": e.get("published"),
                "source": "Yahoo Finance",
            })
        return out, None
    except Exception as exc:
        return [], f"Yahoo top stories: {exc}"


def fetch_yahoo_news_multi(tickers, max_workers=10):
    """Fetch per-ticker Yahoo news across the watchlist in parallel."""
    all_items = []
    errors = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_yahoo_ticker_news, t): t for t in tickers}
        for fut in as_completed(futures):
            items, err = fut.result()
            all_items.extend(items)
            if err:
                errors.append(err)

    top, top_err = fetch_yahoo_top_stories()
    all_items.extend(top)
    if top_err:
        errors.append(top_err)

    # de-dupe by link
    seen = set()
    deduped = []
    for it in all_items:
        key = it.get("link")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    def _sort_key(it):
        try:
            return time.mktime(time.strptime(it["published"][:25], "%a, %d %b %Y %H:%M:%S"))
        except Exception:
            return 0

    deduped.sort(key=_sort_key, reverse=True)
    return deduped, errors


# ---------------------------------------------------------------------------
# Additional free RSS sources: CNBC, MarketWatch, Federal Reserve
# ---------------------------------------------------------------------------

def _fetch_generic_rss(url, source_label, timeout=10):
    try:
        resp = _session.get(url, headers=YAHOO_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"{source_label} HTTP {resp.status_code}"
        feed = feedparser.parse(resp.content)
        out = []
        for e in feed.entries:
            out.append({
                "ticker": None,
                "headline": e.get("title"),
                "link": e.get("link"),
                "published": e.get("published"),
                "source": source_label,
            })
        return out, None
    except Exception as exc:
        return [], f"{source_label}: {exc}"


def fetch_cnbc_top_news(timeout=10):
    # CNBC's public "Top News" RSS (partnerId wrss01, id 100003114).
    url = "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114"
    return _fetch_generic_rss(url, "CNBC", timeout=timeout)


def fetch_marketwatch_top_stories(timeout=10):
    url = "https://feeds.content.dowjones.io/public/rss/mw_topstories"
    return _fetch_generic_rss(url, "MarketWatch", timeout=timeout)


def fetch_fed_press_releases(timeout=10):
    # Federal Reserve Board press releases - low volume, high relevance for
    # rate/macro-driven SPY/QQQ moves.
    url = "https://www.federalreserve.gov/feeds/press_all.xml"
    return _fetch_generic_rss(url, "Federal Reserve", timeout=timeout)


def fetch_bloomberg_markets(timeout=10):
    # Bloomberg's public Markets RSS feed - free, keyless, headlines + link
    # (article body is Bloomberg's usual paywall, same as clicking through
    # from any other aggregator).
    url = "https://feeds.bloomberg.com/markets/news.rss"
    return _fetch_generic_rss(url, "Bloomberg", timeout=timeout)


def fetch_fox_business(timeout=10):
    # Fox Business's "Latest" + "Markets" feeds, both free/keyless. Combined
    # under one source label (same pattern as Yahoo's per-ticker + top
    # stories both being labeled "Yahoo Finance").
    all_items = []
    errors = []
    for url in (
        "https://feeds.foxbusiness.com/foxbusiness/latest",
        "https://feeds.foxbusiness.com/foxbusiness/markets",
    ):
        items, err = _fetch_generic_rss(url, "Fox Business", timeout=timeout)
        all_items.extend(items)
        if err:
            errors.append(err)
    # Keep the (items, error) shape consistent with the other single-feed
    # fetchers below (a single string or None), not a list.
    return all_items, ("; ".join(errors) if errors else None)


def fetch_barchart_options_news(timeout=10):
    # Barchart's Options News RSS - specifically options market activity,
    # the most on-thesis free feed for a credit-spread/iron-condor shop.
    url = "https://www.barchart.com/news/rss/financials/options-news"
    return _fetch_generic_rss(url, "Barchart", timeout=timeout)


def fetch_wsj_markets(timeout=10):
    # WSJ's Markets RSS, served fresh via the dowjones.io content API.
    # NOTE: the older feeds.a.dj.com/rss/RSSMarketsMain.xml URL (widely
    # referenced in old guides) returns HTTP 200 but is frozen/dead - every
    # item on it is dated Jan 2025 no matter when you fetch it. Confirmed
    # this feeds.content.dowjones.io endpoint is the live one before wiring
    # it in (checked item pubDates match today, not a stale cached copy).
    url = "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain"
    return _fetch_generic_rss(url, "WSJ", timeout=timeout)


def fetch_all_news_multi(tickers, max_workers=10):
    """
    Combines per-ticker Yahoo news, Yahoo top stories, CNBC, MarketWatch,
    Fed press releases, Bloomberg, Fox Business, Barchart Options News, and
    WSJ Markets into one deduped, time-sorted list.
    """
    all_items = []
    errors = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_yahoo_ticker_news, t): t for t in tickers}
        for fut in as_completed(futures):
            items, err = fut.result()
            all_items.extend(items)
            if err:
                errors.append(err)

    for fetcher in (
        fetch_yahoo_top_stories,
        fetch_cnbc_top_news,
        fetch_marketwatch_top_stories,
        fetch_fed_press_releases,
        fetch_bloomberg_markets,
        fetch_fox_business,
        fetch_barchart_options_news,
        fetch_wsj_markets,
    ):
        items, err = fetcher()
        all_items.extend(items)
        if err:
            errors.append(err)

    seen = set()
    deduped = []
    for it in all_items:
        key = it.get("link")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    def _sort_key(it):
        try:
            return time.mktime(time.strptime(it["published"][:25], "%a, %d %b %Y %H:%M:%S"))
        except Exception:
            return 0

    deduped.sort(key=_sort_key, reverse=True)
    return deduped, errors


# ---------------------------------------------------------------------------
# Nasdaq public calendars (dividends / IPO / economic events)
# ---------------------------------------------------------------------------

def fetch_nasdaq_dividends(date_str, timeout=10):
    url = f"https://api.nasdaq.com/api/calendar/dividends?date={date_str}"
    try:
        resp = _session.get(url, headers=NASDAQ_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"Nasdaq dividends HTTP {resp.status_code}"
        data = resp.json()
        rows = (data.get("data") or {}).get("calendar", {}).get("rows") or []
        return rows, None
    except Exception as exc:
        return [], f"Nasdaq dividends: {exc}"


def fetch_nasdaq_ipo_calendar(month_str, timeout=10):
    url = f"https://api.nasdaq.com/api/ipo/calendar?date={month_str}"
    try:
        resp = _session.get(url, headers=NASDAQ_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return {}, f"Nasdaq IPO calendar HTTP {resp.status_code}"
        data = resp.json().get("data") or {}
        return data, None
    except Exception as exc:
        return {}, f"Nasdaq IPO calendar: {exc}"


def fetch_nasdaq_economic_events(date_str, timeout=10):
    # QUIRK (confirmed 2026-07-09): this endpoint's date bucketing runs a
    # full day ahead of reality. Requesting date=<real today> returns
    # events that actually belong to <real today minus 1 day>, and today's
    # real events sit under date=<real today plus 1 day> instead. Verified
    # three independent ways: (1) Initial Jobless Claims - a release that
    # ONLY ever happens on Thursdays - was absent from the date=<Thursday>
    # response and present under date=<Friday>; (2) FOMC Meeting Minutes
    # (confirmed via web search to have released Wed 2026-07-08 2pm ET) only
    # showed up under date=<Thursday>, one day late; (3) Existing Home Sales
    # (confirmed via web search to release Thu 2026-07-09 10am ET) only
    # showed up under date=<Friday>, one day late. Each row itself has no
    # date field (just a "gmt" time-of-day), so the date is purely which
    # bucket the caller requests - meaning this is safe to compensate for
    # by requesting tomorrow's date to get today's real events, without
    # touching anything inside each row. Whether this is Nasdaq's own bug
    # or an intentional-but-undocumented convention on their end is unknown;
    # this is their informal public endpoint, not an official/versioned API.
    # The caller (app.py's /api/calendar) does the +1 day compensation.
    # Not verified whether Nasdaq's dividends/IPO calendar endpoints share
    # this quirk - untouched pending a specific report of them being wrong.
    url = f"https://api.nasdaq.com/api/calendar/economicevents?date={date_str}"
    try:
        resp = _session.get(url, headers=NASDAQ_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"Nasdaq economic events HTTP {resp.status_code}"
        data = resp.json()
        rows = (data.get("data") or {}).get("rows") or []
        return rows, None
    except Exception as exc:
        return [], f"Nasdaq economic events: {exc}"


# ---------------------------------------------------------------------------
# Economic event importance (High/Medium/Low) via ForexFactory's free
# weekly calendar feed, cross-referenced onto the Nasdaq event rows above
# (Nasdaq gives real actual/consensus/previous figures but no importance
# rating; ForexFactory's feed has the rating but no post-release actuals).
# ---------------------------------------------------------------------------

FF_COUNTRY_TO_NASDAQ = {
    "USD": ["United States"],
    "GBP": ["United Kingdom"],
    "JPY": ["Japan"],
    "EUR": ["Eurozone", "Germany", "France", "Italy", "Spain", "Netherlands"],
    "CAD": ["Canada"],
    "AUD": ["Australia"],
    "NZD": ["New Zealand"],
    "CHF": ["Switzerland"],
    "CNY": ["China"],
}
# Reverse map: Nasdaq country name -> set of FF currency codes that could match it.
_NASDAQ_TO_FF_CCY = {}
for _ccy, _countries in FF_COUNTRY_TO_NASDAQ.items():
    for _c in _countries:
        _NASDAQ_TO_FF_CCY.setdefault(_c, set()).add(_ccy)

HIGH_IMPORTANCE_KEYWORDS = [
    "cpi", "consumer price index", "ppi", "producer price index",
    "nonfarm payroll", "non-farm payroll", "payrolls", "gdp",
    "fomc", "interest rate decision", "rate decision", "fed funds",
    "unemployment rate", "core pce", "pce price index", "retail sales",
    "employment change",
]
MEDIUM_IMPORTANCE_KEYWORDS = [
    "pmi", "consumer confidence", "durable goods", "housing starts",
    "industrial production", "trade balance", "building permits",
    "existing home sales", "new home sales", "ism", "consumer sentiment",
    "jobless claims", "unemployment claims", "factory orders",
]


def _normalize_title(s):
    s = (s or "").lower()
    s = re.sub(r"\(.*?\)", " ", s)          # drop parenthetical qualifiers
    s = re.sub(r"[^a-z0-9 ]", " ", s)       # drop punctuation like m/m, q/q
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _keyword_importance(title):
    norm = _normalize_title(title)

    # Routine Fed/BoE/etc speaker events and nowcast models get swept up by
    # broad keywords (e.g. "FOMC Member Bowman Speaks" contains "fomc";
    # "Atlanta Fed GDPNow" contains "gdp") but aren't real market-moving
    # releases the way an actual rate decision or GDP print is. Cap those.
    if "speaks" in norm or "speech" in norm or "testimony" in norm:
        return "Medium"
    if "gdpnow" in norm or "nowcast" in norm:
        return "Medium"

    if any(k in norm for k in HIGH_IMPORTANCE_KEYWORDS):
        return "High"
    if any(k in norm for k in MEDIUM_IMPORTANCE_KEYWORDS):
        return "Medium"
    return "Low"


_ff_cache = {"data": None, "err": None, "ts": 0.0}
FF_CACHE_TTL = 300  # 5 min - protects against the feed's rate limiting (HTTP 429)
                    # when multiple callers (today's importance + week view)
                    # need it within the same request cycle.


def fetch_ff_calendar(timeout=10):
    """
    ForexFactory's (FairEconomy-hosted) weekly economic calendar XML.
    Free, keyless. Returns events with an editorial impact rating but no
    post-release actual values. Cached briefly since this feed 429s under
    even light repeat traffic.
    """
    now = time.time()
    if _ff_cache["data"] is not None and (now - _ff_cache["ts"]) < FF_CACHE_TTL:
        return _ff_cache["data"], _ff_cache["err"]

    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
    try:
        resp = _session.get(url, headers=YAHOO_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            err = f"ForexFactory calendar HTTP {resp.status_code}"
            # Serve stale data on a rate limit / transient failure rather than
            # returning empty if we have anything cached at all.
            if _ff_cache["data"] is not None:
                return _ff_cache["data"], err
            return [], err
        root = ET.fromstring(resp.content)
        out = []
        for e in root.findall("event"):
            out.append({
                "title": e.findtext("title"),
                "country": e.findtext("country"),
                "date": e.findtext("date"),   # MM-DD-YYYY
                "time": e.findtext("time"),
                "impact": e.findtext("impact"),
                "forecast": e.findtext("forecast"),
                "previous": e.findtext("previous"),
                "url": e.findtext("url"),
            })
        _ff_cache["data"] = out
        _ff_cache["err"] = None
        _ff_cache["ts"] = now
        return out, None
    except Exception as exc:
        err = f"ForexFactory calendar: {exc}"
        if _ff_cache["data"] is not None:
            return _ff_cache["data"], err
        return [], err


def _parse_econ_number(v):
    """Best-effort numeric parse of a Nasdaq stat field ("0.3%", "&nbsp;",
    "336.12", "-0.1%") - returns None for blanks/unparseable values."""
    if v is None:
        return None
    s = str(v).replace("&nbsp;", "").replace("%", "").replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


_BARE_INFLATION_TITLES = ("CPI", "Core CPI", "PPI", "Core PPI")


def tag_cpi_period(row):
    """
    Nasdaq's feed returns bare "CPI"/"Core CPI"/"PPI"/"Core PPI" rows twice
    a month with no month-over-month vs year-over-year label - one row is
    the MoM print (small, typically 0-1%), the other is the YoY print
    (typically 1-9%). Billy asked to distinguish them at a glance (CPI
    first, then asked for the same treatment on PPI), so this appends
    "(MoM)" or "(YoY)" to the event name based on the magnitude of
    whichever figure is available (consensus, then previous, then actual -
    whichever's populated first). Only touches these bare titles;
    index-level variants ("Core CPI Index", "CPI Index, n.s.a.", etc.)
    already carry their own distinct names and are left untouched.
    Threshold of 1.5% is comfortably between typical MoM (rarely above ~1%)
    and YoY (rarely below ~2%) prints for both CPI and PPI.
    """
    name = (row.get("eventName") or "").strip()
    if name not in _BARE_INFLATION_TITLES:
        return
    for key in ("consensus", "previous", "actual"):
        val = _parse_econ_number(row.get(key))
        if val is not None:
            row["eventName"] = f"{name} ({'YoY' if abs(val) >= 1.5 else 'MoM'})"
            return
    # No usable figure on any field to infer from - leave the bare name as is.


def annotate_economic_importance(rows, date_str):
    """
    Takes Nasdaq economic-event rows (for date_str, format YYYY-MM-DD) and
    adds an "importance" field (High/Medium/Low) to each, sourced from the
    ForexFactory calendar where a confident title+country+date match is
    found, falling back to a keyword heuristic otherwise.
    """
    ff_events, ff_err = fetch_ff_calendar()

    target_date = None
    try:
        d = _dt.strptime(date_str, "%Y-%m-%d")
        target_date = d.strftime("%m-%d-%Y")
    except Exception:
        pass

    ff_today = [e for e in ff_events if e.get("date") == target_date] if target_date else []

    for row in rows:
        tag_cpi_period(row)
        matched_impact = None
        nasdaq_country = row.get("country")
        allowed_ccys = _NASDAQ_TO_FF_CCY.get(nasdaq_country)
        norm_title = _normalize_title(row.get("eventName"))

        best_ratio = 0.0
        best_impact = None
        for ff_ev in ff_today:
            if allowed_ccys and ff_ev.get("country") not in allowed_ccys:
                continue
            ratio = difflib.SequenceMatcher(None, norm_title, _normalize_title(ff_ev.get("title"))).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_impact = ff_ev.get("impact")

        if best_ratio >= 0.6 and best_impact in ("High", "Medium", "Low"):
            matched_impact = best_impact
            row["importance_source"] = "forexfactory"
        else:
            matched_impact = _keyword_importance(row.get("eventName"))
            row["importance_source"] = "heuristic"

        row["importance"] = matched_impact

        edu = build_econ_education(row.get("eventName"), row.get("description"))
        row["edu_what"] = edu["what"]
        row["edu_trade"] = edu["trade"]

    return rows, ff_err


# ---------------------------------------------------------------------------
# Economic event education content ("what it measures" / "how it may affect
# trading"), shown in a click popup on each calendar row. Category-based so
# it covers any event title (including one-off international prints) without
# hand-curating every possible name. Reuses the same normalize/keyword-match
# approach as the importance classifier above.
# ---------------------------------------------------------------------------

ECON_CATEGORY_KEYWORDS = [
    ("fed", ["fomc", "interest rate decision", "rate decision", "rate statement",
              "press conference", "fed funds", "meeting minutes", "speaks",
              "speech", "testimony", "monetary policy"]),
    ("inflation", ["cpi", "consumer price index", "ppi", "producer price index",
                    "pce price index", "core pce", "inflation"]),
    ("employment", ["nonfarm payroll", "non-farm payroll", "payrolls",
                      "employment change", "unemployment rate", "jobless claims",
                      "unemployment claims", "adp employment", "employment"]),
    ("growth", ["gdp", "gross domestic product"]),
    ("sentiment", ["pmi", "consumer confidence", "consumer sentiment", "ism",
                     "economic optimism", "business confidence"]),
    ("housing", ["housing starts", "building permits", "existing home sales",
                  "new home sales", "house price", "home sales", "housing"]),
    ("manufacturing", ["durable goods", "industrial production", "factory orders",
                         "capacity utilization", "manufacturing"]),
    ("trade", ["trade balance", "current account"]),
    ("retail", ["retail sales"]),
    ("credit", ["consumer credit"]),
]

ECON_CATEGORY_WHAT = {
    "fed": "A Federal Reserve (or foreign central bank) event: a rate decision, policy statement, meeting minutes, or public remarks on monetary policy.",
    "inflation": "Measures the pace of price changes across the economy, at the consumer or producer level. Central to the Fed's rate decisions.",
    "employment": "Measures job creation, wages, or unemployment. A key input into the Fed's dual mandate and a read on overall economic health.",
    "growth": "Measures the total value of goods and services produced by the economy over the period.",
    "sentiment": "A survey-based gauge of how businesses or consumers feel about current and future economic conditions.",
    "housing": "Measures activity in the housing market: home sales, construction, or home prices.",
    "manufacturing": "Measures factory-sector output, orders, or business investment.",
    "trade": "Measures the gap between a country's exports and imports.",
    "retail": "Measures consumer spending at the retail level.",
    "credit": "Measures the change in outstanding consumer credit, loans and credit cards.",
    "default": "A scheduled economic data release tracked by traders for its potential to move markets.",
}

ECON_CATEGORY_TRADE = {
    "fed": "Anything tied to the Fed can move markets on tone alone, not just the headline number. Rate decision days especially carry the risk of a late-day reversal as the press conference gets parsed. Running an iron condor through an FOMC day usually calls for wider strikes than normal, or waiting until after the decision to enter.",
    "inflation": "Inflation prints are some of the market's biggest volatility triggers. A surprise vs. forecast can move SPY/QQQ sharply within minutes. If you're short premium (credit spreads, iron condors) into a High-impact inflation release, expect the possibility of a fast move through one side of your position. Many traders avoid opening new short-premium trades in the hour before this print and instead look to sell into the IV crush right after it lands.",
    "employment": "Jobs data drives Fed rate expectations, which ripples through stocks and the VIX alike. A big beat or miss can trigger a sharp intraday move and a volatility spike. Same playbook as inflation prints: be cautious holding tight short-premium positions into a High-impact jobs release, and watch for an IV crush opportunity once the number is digested.",
    "growth": "GDP moves markets less violently than inflation or jobs data, since it's often partly priced in already from earlier indicators, but a big surprise can still shift rate-cut expectations and jolt SPY/QQQ. Worth checking your open positions' width against the day's historical move if this print is flagged High.",
    "sentiment": "Survey data is a read on mood rather than hard numbers, so the market reaction is usually more contained than inflation or jobs releases. Still worth a glance before entering a same-day trade, since a big surprise (especially the ISM prints) can nudge the tape.",
    "housing": "Housing data moves rate-sensitive sectors and can shift Fed expectations at the margin, but rarely causes a broad SPY/QQQ shock on its own. Low to Medium priority for position sizing.",
    "manufacturing": "A read on business investment, usually a secondary market mover unless it diverges sharply from expectations. Fine to trade around normally.",
    "trade": "A slower-moving, lower-volatility release. Rarely a reason to adjust position sizing.",
    "retail": "A read on the health of the consumer, one of the more closely watched mid-tier releases. Can move markets meaningfully on a sharp surprise, worth some caution on a High-flagged print.",
    "credit": "A slower-moving, lower-priority release. Rarely worth adjusting a trading plan around.",
    "default": "Check the importance flag on this event. High-impact prints can trigger a short burst of realized volatility right around the release, worth factoring into position size and strike width if you're holding short premium through it. Medium and Low-impact releases are usually background noise for SPY/QQQ/IWM options trading.",
}


def _econ_category(title):
    norm = _normalize_title(title)
    for category, keywords in ECON_CATEGORY_KEYWORDS:
        if any(k in norm for k in keywords):
            return category
    return "default"


def clean_econ_description(desc):
    """Nasdaq's native event description: strip HTML remnants/entities and
    collapse whitespace so it reads as clean prose in the popup."""
    if not desc:
        return None
    text = html.unescape(desc)
    text = re.sub(r"<[^>]+>", " ", text)          # stray tags like <BR/>
    text = re.sub(r"[\r\n]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    if len(text) > 550:
        text = text[:547].rsplit(" ", 1)[0] + "..."
    return text


def build_econ_education(title, native_description=None):
    """Returns {"what": ..., "trade": ...} for the calendar click popup.
    Prefers Nasdaq's real description (Today view) when available, falls
    back to a category-based generic explanation (Week view / no description).
    The trading-impact note is always category-based."""
    category = _econ_category(title)
    cleaned = clean_econ_description(native_description)
    what = cleaned or ECON_CATEGORY_WHAT.get(category, ECON_CATEGORY_WHAT["default"])
    trade = ECON_CATEGORY_TRADE.get(category, ECON_CATEGORY_TRADE["default"])
    return {"what": what, "trade": trade, "category": category}


# ---------------------------------------------------------------------------
# Ticker tape (price + % change) via yfinance, for the Coinfish watchlist
# ---------------------------------------------------------------------------

def fetch_ticker_tape(tickers, max_workers=12):
    """
    NOTE: this used to read straight off fast_info.last_price /
    regular_market_previous_close. Found broken 2026-07-13 in the same
    debugging pass as Movers: fast_info.last_price freezes outside regular
    hours, AND its previous-close fields can be stale by MULTIPLE sessions
    (NVDA showed a previous_close two trading days old on a Monday
    premarket) - both bugs compounding to make the top ticker tape show
    numbers that didn't match reality. Switched to the same
    prepost-history + daily-history combo used by fetch_premarket_movers
    (see those functions' docstrings below for the full story).
    """
    results = []

    def _one(t):
        try:
            price, session, perr = _yf_prepost_price_retry(t)
            if session == "closed":
                # Overnight dead zone / weekend - no live tick exists at
                # all, fall back to today's close vs yesterday's.
                price, prev, cerr = _yf_daily_close_pair_retry(t)
            else:
                prev = None
                cerr = None
                if price is not None:
                    prev, cerr = _yf_prev_close_retry(t)
            if price is None or prev is None or prev == 0:
                return {"ticker": t, "price": None, "pct_change": None, "error": perr or cerr or "no price"}
            pct = round((price - prev) / prev * 100, 2)
            return {"ticker": t, "price": round(float(price), 2), "pct_change": pct, "error": None}
        except Exception as exc:
            return {"ticker": t, "price": None, "pct_change": None, "error": str(exc)}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for r in ex.map(_one, tickers):
            results.append(r)

    order = {t: i for i, t in enumerate(tickers)}
    results.sort(key=lambda r: order.get(r["ticker"], 999))
    return results


# ---------------------------------------------------------------------------
# Pre-market movers - biggest % movers on the watchlist ahead of the open.
# yfinance's lightweight fast_info (used by fetch_ticker_tape above) doesn't
# carry premarket fields, so this uses the fuller .info call instead - still
# fast threaded (~2s for the full 58-name watchlist, tested). Falls back to
# the regular-session % change when premarket data isn't available (market
# already open, or a name with thin premarket volume Yahoo doesn't quote),
# so the widget still shows something meaningful outside premarket hours.
# ---------------------------------------------------------------------------

def _yf_fast_info_retry(ticker, retries=3, base_delay=0.6):
    """
    yf.Ticker(t).fast_info wrapped with retry+backoff - NOT the full .info.

    History: Movers/Market Cap/Heatmap originally used the full .info call
    and came back 100% "Expecting value: line 1 column 1 (char 0)" (a
    JSON-decode error, meaning Yahoo returned an empty body) once this app
    was deployed on Railway, even with retries and low concurrency. Root
    cause turned out to be the endpoint, not the pacing: .info needs
    Yahoo's quoteSummary API behind a cookie+crumb auth handshake, and that
    handshake is what's actually blocked from Railway's cloud IP - so every
    retry failed identically no matter how it was paced. fast_info uses the
    plain chart API instead (no crumb needed), which is the exact same
    thing fetch_ticker_tape/fetch_vix_snapshot/fetch_treasury_yields already
    use and which works fine live on Railway. Switching these three
    fetchers to fast_info fixed it. Tradeoff: fast_info has no company
    name/industry/premarket fields, so those are degraded (see call sites).
    Kept the retry+backoff wrapper anyway for ordinary transient hiccups.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            info = yf.Ticker(ticker).fast_info
            if info and getattr(info, "last_price", None) is not None:
                return info, None
            last_exc = "empty/incomplete response"
        except Exception as exc:
            last_exc = str(exc)
        time.sleep(base_delay * (attempt + 1))
    return None, last_exc


# US market session boundaries (Eastern time, which is what yfinance's
# history() index comes back tz-localized to for US tickers).
_MARKET_OPEN_ET = _dt.strptime("09:30", "%H:%M").time()
_MARKET_CLOSE_ET = _dt.strptime("16:00", "%H:%M").time()
# Yahoo's pre/post-market minute data only exists inside a fixed daily
# window - roughly premarket open (4:00am ET) through the end of the
# official afterhours session (8:00pm ET). Outside that window (the
# overnight "dead zone", ~8pm to ~4am) there are no new minute bars at all.
_EXTENDED_START_ET = _dt.strptime("04:00", "%H:%M").time()
_EXTENDED_END_ET = _dt.strptime("20:00", "%H:%M").time()
_ET_ZONE = ZoneInfo("America/New_York")


def _yf_prepost_price_retry(ticker, retries=2, base_delay=0.6):
    """
    Real premarket/afterhours price via yf.Ticker(t).history(interval="1m",
    prepost=True). Same underlying chart-API endpoint as fast_info (no
    crumb/cookie auth, confirmed working on Railway), but unlike
    fast_info.last_price - which just freezes at the prior REGULAR session's
    close and doesn't move again until the next regular session opens -
    this actually carries live pre/post-market minute ticks.

    Found 2026-07-13 debugging "movers doesn't update": fast_info.last_price
    for AAPL was stuck at $315.32 (Friday's close) while the real premarket
    tape had it trading ~$316.09 - fast_info simply has no premarket data at
    all, it's not a caching or staleness bug, it's a field the endpoint
    doesn't carry. history(prepost=True) does.

    Extended 2026-07-14: found a second, related bug the same way - checked
    at ~10:20pm ET and every ticker's "last bar" was actually timestamped
    ~8:00pm, over two hours stale, because Yahoo simply stops writing
    minute bars once the official afterhours session ends. The old code
    took whatever the last available bar was and called it live regardless
    of how old it actually was, still labeling it "afterhours." Same root
    disease as the fast_info bug (a frozen value mistaken for a live one),
    different clock window. Fix: check the actual current wall-clock time
    in ET FIRST: if it falls in the overnight dead zone
    (_EXTENDED_END_ET to _EXTENDED_START_ET next day), skip the minute-bar
    fetch entirely and return session="closed" - callers fall back to
    plain daily-close pricing instead (see _yf_daily_close_pair_retry).

    Returns (price, session, error). session is one of "premarket" /
    "regular" / "afterhours" / "closed". When session == "closed", price is
    always None (there's nothing live to report) and error is always None
    (this isn't a failure, it's an expected state overnight/on weekends).
    """
    now_et = _dt.now(_ET_ZONE).time()
    if not (_EXTENDED_START_ET <= now_et < _EXTENDED_END_ET):
        return None, "closed", None

    last_exc = None
    for attempt in range(retries):
        try:
            hist = yf.Ticker(ticker).history(period="2d", interval="1m", prepost=True)
            if hist is not None and not hist.empty:
                last_ts = hist.index[-1]
                price = hist["Close"].iloc[-1]
                if price is None or (isinstance(price, float) and price != price):  # NaN check
                    last_exc = "empty last bar"
                else:
                    t_local = last_ts.time()
                    if t_local < _MARKET_OPEN_ET:
                        session = "premarket"
                    elif t_local >= _MARKET_CLOSE_ET:
                        session = "afterhours"
                    else:
                        session = "regular"
                    return float(price), session, None
            else:
                last_exc = "empty history"
        except Exception as exc:
            last_exc = str(exc)
        time.sleep(base_delay * (attempt + 1))
    return None, None, last_exc


def _yf_daily_close_pair_retry(ticker, retries=2, base_delay=0.6):
    """
    Today's close + the close before it, both from daily bars - used only
    when _yf_prepost_price_retry reports session="closed" (the overnight
    dead zone, ~8pm-4am ET, or a weekend). There's no live intraday tick to
    show at all in that window, so instead of a stale afterhours print this
    shows the regular session's actual performance: today's close vs
    yesterday's, which is what most brokers/sites default to once the
    trading day is fully done and extended hours have ended too.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            daily = yf.Ticker(ticker).history(period="5d", interval="1d")
            if daily is not None and len(daily) >= 2:
                today_close = daily["Close"].iloc[-1]
                prior_close = daily["Close"].iloc[-2]
                if any(isinstance(v, float) and v != v for v in (today_close, prior_close)):  # NaN check
                    last_exc = "empty daily bar"
                else:
                    return float(today_close), float(prior_close), None
            else:
                last_exc = "not enough daily history"
        except Exception as exc:
            last_exc = str(exc)
        time.sleep(base_delay * (attempt + 1))
    return None, None, last_exc


def _yf_prev_close_retry(ticker, retries=2, base_delay=0.6):
    """
    Previous REGULAR-session close via daily-interval history - NOT
    fast_info.previous_close / regular_market_previous_close.

    Found 2026-07-13, same day as the last_price bug: on Monday premarket,
    fast_info.regular_market_previous_close for NVDA returned $202.78 -
    Wednesday's close, TWO sessions stale - while the real previous close
    (Friday's) was $210.96. fast_info.previous_close returned yet another,
    also-wrong number ($210.57). Every %-change in Movers was computed off
    this bad baseline, which is why the panel looked "wrong"/frozen even
    though price and session were already fixed and genuinely live.

    Daily-interval bars don't have this problem: Yahoo only writes a daily
    bar once that session is fully closed, so during premarket the last row
    is always the correct, most-recently-completed close - no separate
    "current vs previous" field to get confused about.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            daily = yf.Ticker(ticker).history(period="5d", interval="1d")
            if daily is not None and not daily.empty:
                close = daily["Close"].iloc[-1]
                if close is None or (isinstance(close, float) and close != close):  # NaN check
                    last_exc = "empty last daily bar"
                else:
                    return float(close), None
            else:
                last_exc = "empty daily history"
        except Exception as exc:
            last_exc = str(exc)
        time.sleep(base_delay * (attempt + 1))
    return None, last_exc


def fetch_premarket_movers(tickers, max_workers=4, top_n=10):
    """
    Biggest movers by % change. Price/session come from
    _yf_prepost_price_retry (real pre/post-market ticks). prev_close comes
    from _yf_prev_close_retry (daily bars) - NOT fast_info, whose
    previous-close fields were found stale by up to 2 sessions (see that
    function's docstring). Both sources needed per ticker; fast_info is no
    longer used anywhere in this function.

    Outside the extended-hours window entirely (session == "closed", see
    _yf_prepost_price_retry) there's no live tick to show at all, so this
    falls back to _yf_daily_close_pair_retry - today's close vs
    yesterday's, i.e. the regular session's actual performance.
    """
    results = []
    errors = []

    def _one(t):
        try:
            price, session, perr = _yf_prepost_price_retry(t)
            if session == "closed":
                price, prev_close, cerr = _yf_daily_close_pair_retry(t)
            else:
                prev_close, cerr = (None, None) if price is None else _yf_prev_close_retry(t)
            if price is None:
                return {"ticker": t, "error": perr or cerr or "no price data"}
            if prev_close is None or prev_close == 0:
                return {"ticker": t, "error": cerr or "no prev close data"}
            pct = (price - prev_close) / prev_close * 100
            return {
                "ticker": t,
                "price": round(float(price), 2),
                "pct_change": round(float(pct), 2),
                "session": session,
                "market_state": None,
                "prev_close": round(float(prev_close), 2),
                "error": None,
            }
        except Exception as exc:
            return {"ticker": t, "error": str(exc)}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for r in ex.map(_one, tickers):
            results.append(r)

    valid = [r for r in results if r.get("error") is None]
    for r in results:
        if r.get("error"):
            errors.append(f"{r['ticker']}: {r['error']}")

    valid.sort(key=lambda r: abs(r["pct_change"]), reverse=True)
    return valid[:top_n], errors


def fetch_market_cap_leaders(tickers, max_workers=4, top_n=10):
    """
    Top N watchlist names by market cap, with current price - a companion
    widget to fetch_premarket_movers above (that one sorts by % move, this
    one by size). Uses fast_info - see _yf_fast_info_retry docstring.
    """
    results = []
    errors = []

    def _one(t):
        try:
            info, err = _yf_fast_info_retry(t)
            if info is None:
                return {"ticker": t, "error": err or "no data"}
            cap = getattr(info, "market_cap", None)
            price = getattr(info, "last_price", None)
            if cap is None or price is None:
                return {"ticker": t, "error": "no market cap/price data"}
            return {
                "ticker": t,
                "market_cap": int(cap),
                "price": round(float(price), 2),
                "error": None,
            }
        except Exception as exc:
            return {"ticker": t, "error": str(exc)}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for r in ex.map(_one, tickers):
            results.append(r)

    valid = [r for r in results if r.get("error") is None]
    for r in results:
        if r.get("error"):
            errors.append(f"{r['ticker']}: {r['error']}")

    valid.sort(key=lambda r: r["market_cap"], reverse=True)
    return valid[:top_n], errors


def fetch_heatmap_data(tickers, sector_map, max_workers=4):
    """
    Per-ticker snapshot for the sector heatmap widget (modeled on
    tradingterminal.com/heatmap, scoped to the watchlist instead of the
    whole market): market cap (box size), % change (box color), plus the
    fields the click-through popup shows (price, open/high/low/previous
    close, sector, industry, company name). `sector_map` is the
    ticker->sector lookup app.py builds from WATCHLIST_SECTORS.

    Uses fast_info ONLY for market_cap/open/day_high/day_low - see
    _yf_fast_info_retry docstring for why (the full .info call this used to
    use is blocked on Railway's cloud IP). Tradeoff: fast_info has no
    company-name or industry fields, so "name" falls back to the ticker
    symbol and "industry" falls back to "-" on the hosted deploy (both are
    populated normally when running locally, where .info still works fine).

    price/pct_change do NOT come from fast_info anymore. Found 2026-07-13,
    same debugging pass as Movers and the ticker tape: fast_info.last_price
    freezes outside regular hours, and its previous-close fields can be
    stale by multiple sessions - both bugs made every heatmap box's color
    and % wrong outside market hours. Switched to the same
    prepost-history + daily-history combo those two use.
    """
    results = []
    errors = []

    def _one(t):
        try:
            info, err = _yf_fast_info_retry(t)
            if info is None:
                return {"ticker": t, "error": err or "no data"}
            cap = getattr(info, "market_cap", None)
            price, session, perr = _yf_prepost_price_retry(t)
            if session == "closed":
                price, prev_close, cerr = _yf_daily_close_pair_retry(t)
            else:
                prev_close, cerr = _yf_prev_close_retry(t) if price is not None else (None, None)
            if cap is None or price is None:
                return {"ticker": t, "error": perr or cerr or "no market cap/price data"}
            pct = (price - prev_close) / prev_close * 100 if prev_close else 0.0
            return {
                "ticker": t,
                "name": t,
                "sector": sector_map.get(t, "Other"),
                "industry": "-",
                "market_cap": int(cap),
                "price": round(float(price), 2),
                "pct_change": round(float(pct), 2),
                "open": getattr(info, "open", None),
                "high": getattr(info, "day_high", None),
                "low": getattr(info, "day_low", None),
                "prev_close": round(float(prev_close), 2) if prev_close is not None else None,
                "error": None,
            }
        except Exception as exc:
            return {"ticker": t, "error": str(exc)}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for r in ex.map(_one, tickers):
            results.append(r)

    valid = [r for r in results if r.get("error") is None]
    for r in results:
        if r.get("error"):
            errors.append(f"{r['ticker']}: {r['error']}")

    return valid, errors


def fetch_intraday_sparkline(ticker, timeout=10):
    """
    A lightweight intraday price series for the heatmap popup's sparkline
    (mirrors the little chart in tradingterminal.com's ticker popup).
    Fetched on-demand per click (see app.py's /api/sparkline route), not
    upfront for the whole watchlist - 5-minute bars for one ticker is a
    cheap yfinance call, doing that for all 55 names on every page load
    would not be. Returns a plain list of closing prices (frontend draws
    the line itself, no need to round-trip full OHLC).
    """
    try:
        hist = yf.Ticker(ticker).history(period="1d", interval="5m")
        if hist is None or hist.empty:
            return [], "no intraday data"
        closes = hist["Close"].dropna().tolist()
        return [round(float(c), 4) for c in closes], None
    except Exception as exc:
        return [], str(exc)


# ---------------------------------------------------------------------------
# CNN Fear & Greed Index - free, keyless, but needs browser-like headers
# (CNN's dataviz endpoint 418s a bare curl/requests UA without Referer/Origin).
# Also exposes the 7 underlying components, one of which (put_call_options)
# is CNN's own options put/call read - the closest we've got to a live
# put/call signal since CBOE's public CSV archives turned out to be stale.
# ---------------------------------------------------------------------------

FEAR_GREED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.cnn.com/markets/fear-and-greed",
    "Origin": "https://www.cnn.com",
}

FEAR_GREED_COMPONENTS = [
    ("market_momentum_sp500", "S&P 500 Momentum"),
    ("stock_price_strength", "Price Strength"),
    ("stock_price_breadth", "Price Breadth"),
    ("put_call_options", "Put/Call Options"),
    ("market_volatility_vix", "Market Volatility"),
    ("junk_bond_demand", "Junk Bond Demand"),
    ("safe_haven_demand", "Safe Haven Demand"),
]


def fetch_fear_greed(timeout=10):
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        resp = _session.get(url, headers=FEAR_GREED_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return {"score": None, "rating": None, "components": [], "error": f"HTTP {resp.status_code}"}
        data = resp.json()
        fg = data.get("fear_and_greed", {})
        components = []
        for key, label in FEAR_GREED_COMPONENTS:
            c = data.get(key, {})
            components.append({
                "label": label,
                "score": round(c.get("score"), 1) if isinstance(c.get("score"), (int, float)) else None,
                "rating": c.get("rating"),
            })
        return {
            "score": round(fg.get("score"), 1) if isinstance(fg.get("score"), (int, float)) else None,
            "rating": fg.get("rating"),
            "previous_close": fg.get("previous_close"),
            "previous_1_week": fg.get("previous_1_week"),
            "previous_1_month": fg.get("previous_1_month"),
            "previous_1_year": fg.get("previous_1_year"),
            "components": components,
            "error": None,
        }
    except Exception as exc:
        return {"score": None, "rating": None, "components": [], "error": str(exc)}


# ---------------------------------------------------------------------------
# Sector heatmap - SPDR sector ETFs, reuses the tape's price/pct_change logic
# ---------------------------------------------------------------------------

SECTOR_ETFS = [
    ("XLK", "Technology"), ("XLF", "Financials"), ("XLE", "Energy"),
    ("XLV", "Health Care"), ("XLY", "Cons. Discretionary"), ("XLP", "Cons. Staples"),
    ("XLI", "Industrials"), ("XLB", "Materials"), ("XLRE", "Real Estate"),
    ("XLU", "Utilities"), ("XLC", "Communication"),
]


def fetch_sector_heatmap():
    labels = dict(SECTOR_ETFS)
    rows = fetch_ticker_tape([t for t, _ in SECTOR_ETFS])
    for r in rows:
        r["sector"] = labels.get(r["ticker"], r["ticker"])
    return rows


# ---------------------------------------------------------------------------
# VIX / VIX9D term structure - free via yfinance, no options chain needed.
# VIX9D < VIX (contango) = calm/normal. VIX9D > VIX (backwardation) = stress.
# ---------------------------------------------------------------------------

def fetch_vix_snapshot():
    try:
        vix = getattr(yf.Ticker("^VIX").fast_info, "last_price", None)
        vix9d = getattr(yf.Ticker("^VIX9D").fast_info, "last_price", None)
        if vix is None or vix9d is None:
            return {"vix": None, "vix9d": None, "term_structure": None, "spread": None, "error": "no price"}
        return {
            "vix": round(float(vix), 2),
            "vix9d": round(float(vix9d), 2),
            "term_structure": "contango" if vix9d < vix else "backwardation",
            "spread": round(float(vix - vix9d), 2),
            "error": None,
        }
    except Exception as exc:
        return {"vix": None, "vix9d": None, "term_structure": None, "spread": None, "error": str(exc)}


# ---------------------------------------------------------------------------
# Treasury yields - free via yfinance index tickers, no scaling needed
# (fast_info.last_price is already the direct yield %, e.g. 4.57 = 4.57%).
# ---------------------------------------------------------------------------

TREASURY_TICKERS = [
    ("^IRX", "3-Month"), ("^FVX", "5-Year"), ("^TNX", "10-Year"), ("^TYX", "30-Year"),
]


def fetch_treasury_yields():
    out = []
    for ticker, label in TREASURY_TICKERS:
        try:
            last = getattr(yf.Ticker(ticker).fast_info, "last_price", None)
            out.append({
                "label": label, "ticker": ticker,
                "yield_pct": round(float(last), 3) if last is not None else None,
                "error": None,
            })
        except Exception as exc:
            out.append({"label": label, "ticker": ticker, "yield_pct": None, "error": str(exc)})
    return out


# ---------------------------------------------------------------------------
# FOMC meeting schedule - hardcoded from federalreserve.gov/monetarypolicy/
# fomccalendars.htm (published a year in advance, doesn't move). Update this
# list every December/January when the Fed posts next year's dates.
# ---------------------------------------------------------------------------

FOMC_MEETINGS_2026 = [
    ("2026-01-27", "2026-01-28"),
    ("2026-03-17", "2026-03-18"),
    ("2026-04-28", "2026-04-29"),
    ("2026-06-16", "2026-06-17"),
    ("2026-07-28", "2026-07-29"),
    ("2026-09-15", "2026-09-16"),
    ("2026-10-27", "2026-10-28"),
    ("2026-12-08", "2026-12-09"),
]


def get_fomc_status():
    today = _dt.now().date()
    for start_s, end_s in FOMC_MEETINGS_2026:
        end = _dt.strptime(end_s, "%Y-%m-%d").date()
        if end >= today:
            start = _dt.strptime(start_s, "%Y-%m-%d").date()
            return {
                "next_start": start_s,
                "next_end": end_s,
                "days_until": max((start - today).days, 0),
                "in_progress": start <= today <= end,
            }
    return {"next_start": None, "next_end": None, "days_until": None, "in_progress": False}


# ---------------------------------------------------------------------------
# Watchlist ticker <-> CIK map (SEC's free company_tickers.json), used to
# flag which filings / insider filings touch a name on the Coinfish
# watchlist so those panels can be filtered instead of showing the whole
# market. Cached for hours since the mapping barely changes.
# ---------------------------------------------------------------------------

_cik_map_cache = {"data": None, "ts": 0.0}
CIK_MAP_TTL = 6 * 3600


def get_watchlist_cik_map(watchlist, timeout=15):
    now = time.time()
    if _cik_map_cache["data"] is not None and (now - _cik_map_cache["ts"]) < CIK_MAP_TTL:
        return _cik_map_cache["data"]
    try:
        resp = _session.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_HEADERS, timeout=timeout)
        data = resp.json()
        full_map = {}
        for v in data.values():
            full_map[v["ticker"].upper()] = str(v["cik_str"]).zfill(10)
        cik_to_ticker = {full_map[t]: t for t in watchlist if t in full_map}
        _cik_map_cache["data"] = cik_to_ticker
        _cik_map_cache["ts"] = now
        return cik_to_ticker
    except Exception:
        return _cik_map_cache["data"] or {}


_CIK_RE = re.compile(r"CIK\s*(\d{10})")


def annotate_filings_watchlist(rows, watchlist):
    cik_to_ticker = get_watchlist_cik_map(watchlist)
    for row in rows:
        cik = (row.get("cik") or "").zfill(10) if row.get("cik") else None
        ticker = cik_to_ticker.get(cik) if cik else None
        row["watchlist_ticker"] = ticker
        row["in_watchlist"] = ticker is not None
    return rows


def annotate_form4_watchlist(rows, watchlist):
    cik_to_ticker = get_watchlist_cik_map(watchlist)
    for row in rows:
        matched = None
        for name in row.get("companies", []) or []:
            m = _CIK_RE.search(name)
            if m:
                ticker = cik_to_ticker.get(m.group(1).zfill(10))
                if ticker:
                    matched = ticker
                    break
        row["watchlist_ticker"] = matched
        row["in_watchlist"] = matched is not None
    return rows


# ---------------------------------------------------------------------------
# Earnings calendar (Nasdaq public endpoint, same pattern as dividends/IPO),
# filtered down to the Coinfish watchlist over the next N days.
# ---------------------------------------------------------------------------

def fetch_nasdaq_earnings_day(date_str, timeout=10):
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={date_str}"
    try:
        resp = _session.get(url, headers=NASDAQ_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return [], f"Nasdaq earnings HTTP {resp.status_code}"
        data = resp.json()
        rows = (data.get("data") or {}).get("rows") or []
        for r in rows:
            r["date"] = date_str
        return rows, None
    except Exception as exc:
        return [], f"Nasdaq earnings ({date_str}): {exc}"


def fetch_watchlist_earnings(watchlist, days_ahead=14, max_workers=8):
    watch_set = set(watchlist)
    today = _dt.now().date()
    dates = [(today + _timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days_ahead)]

    out = []
    errors = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_nasdaq_earnings_day, d): d for d in dates}
        for fut in as_completed(futures):
            rows, err = fut.result()
            if err:
                errors.append(err)
                continue
            for r in rows:
                if r.get("symbol") in watch_set:
                    out.append(r)

    out.sort(key=lambda r: (r.get("date") or "", r.get("symbol") or ""))
    return out, errors


# ---------------------------------------------------------------------------
# Weekly economic calendar - sourced directly from the ForexFactory weekly
# feed (it already spans Mon-Sun so no per-day looping needed), sorted by
# date then importance. Complements the Nasdaq-sourced "today" view.
# ---------------------------------------------------------------------------

def fetch_econ_week_range(countries_nasdaq, week_offset=0, timeout=10):
    """
    Builds a Nasdaq-sourced economic calendar for one calendar week (Mon-Sun),
    offset by `week_offset` weeks from the current week (0 = this week,
    1 = next week). Looped over Nasdaq's per-day economicevents endpoint -
    the same one already used for "Today" - including the same +1 day
    compensation for its confirmed date-bucketing quirk (see
    fetch_nasdaq_economic_events for the full writeup).

    Originally "This Week" came from ForexFactory's free feed instead, but
    that feed never carries post-release Actual values (confirmed: its XML
    has no <actual> element at all, see fetch_ff_calendar's docstring) -
    Billy wants to see Actual vs Previous vs Forecast for events that
    already released earlier in the week, so both "This Week" and "Next
    Week" now share this one Nasdaq-backed function instead. ForexFactory's
    feed (fetch_ff_calendar/get_econ_week below) is kept around only for
    annotate_economic_importance's editorial High/Medium/Low cross-match
    against "Today".

    Each Nasdaq row has no date field of its own (just a "gmt" time), so the
    real calendar date is attached manually per the bucket requested.
    Importance is keyword-heuristic only (not ForexFactory-matched like
    "Today" is) since FF's editorial ratings are keyed by title text, not a
    stable ID, and matching a full week reliably isn't worth the fragility -
    same heuristic used as the fallback everywhere else.
    """
    today = _dt.now().date()
    monday_this_week = today - _timedelta(days=today.weekday())
    target_monday = monday_this_week + _timedelta(weeks=week_offset)
    target_dates = [target_monday + _timedelta(days=i) for i in range(7)]

    out = []
    errors = []

    def _one(d):
        real_date_str = d.strftime("%Y-%m-%d")
        nasdaq_date_str = (d + _timedelta(days=1)).strftime("%Y-%m-%d")
        rows, err = fetch_nasdaq_economic_events(nasdaq_date_str, timeout=timeout)
        if err:
            return [], err
        kept = []
        for row in rows:
            if row.get("country") not in countries_nasdaq:
                continue
            tag_cpi_period(row)
            row["date"] = real_date_str
            row["importance"] = _keyword_importance(row.get("eventName"))
            row["importance_source"] = "heuristic"
            edu = build_econ_education(row.get("eventName"), row.get("description"))
            row["edu_what"] = edu["what"]
            row["edu_trade"] = edu["trade"]
            kept.append(row)
        return kept, None

    with ThreadPoolExecutor(max_workers=7) as ex:
        futures = {ex.submit(_one, d): d for d in target_dates}
        for fut in as_completed(futures):
            rows, err = fut.result()
            out.extend(rows)
            if err:
                errors.append(err)

    impact_rank = {"High": 0, "Medium": 1, "Low": 2}
    out.sort(key=lambda r: (r.get("date") or "", impact_rank.get(r.get("importance"), 3), r.get("gmt") or ""))
    return out, ("; ".join(errors) if errors else None)


def fetch_econ_this_week(countries_nasdaq, timeout=10):
    return fetch_econ_week_range(countries_nasdaq, week_offset=0, timeout=timeout)


def fetch_econ_next_week(countries_nasdaq, timeout=10):
    return fetch_econ_week_range(countries_nasdaq, week_offset=1, timeout=timeout)


def get_econ_week(currencies=None):
    events, err = fetch_ff_calendar()
    if currencies:
        events = [e for e in events if e.get("country") in currencies]

    impact_rank = {"High": 0, "Medium": 1, "Low": 2}

    def _sort_key(e):
        try:
            d = _dt.strptime(e.get("date") or "", "%m-%d-%Y")
        except Exception:
            d = _dt.max
        return (d, impact_rank.get(e.get("impact"), 3))

    events.sort(key=_sort_key)

    for e in events:
        edu = build_econ_education(e.get("title"))
        e["edu_what"] = edu["what"]
        e["edu_trade"] = edu["trade"]

    return events, err
