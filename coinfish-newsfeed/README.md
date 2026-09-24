# Coinfish Newsfeed

Personal, local-only news dashboard modeled on the Stock Trader Network newsfeed. Not deployed publicly, no login, no public URL.

## Run it

```
cd coinfish-newsfeed
pip install flask flask-cors requests feedparser yfinance trafilatura --break-system-packages
python app.py
```

Open http://127.0.0.1:5050 in a browser. Leave the terminal running; the page polls every 90 seconds and also has a manual Refresh button.

### Opening it from another device on your home network

The server listens on all network interfaces (`0.0.0.0`), not just this machine, so any other device on the same WiFi/router can reach it while `python app.py` is running here.

1. Find this computer's local IP: Settings > Network & Internet > Wi-Fi (or Ethernet) > click the connection > look for "IPv4 address" (something like `192.168.1.x`).
2. On the other device, open `http://<that-ip>:5050` in a browser (e.g. `http://192.168.1.25:5050`).
3. If it doesn't load, Windows Firewall may be blocking it the first time — accept the "Allow python.exe to communicate on private networks" prompt if one appears.

This only works on the same network and only while this computer is on and the server is running. For access from anywhere (phone on cellular, laptop on the road), it would need an actual deployment (e.g. Railway, same as `coinfish-dashboard`) or a tunnel like Tailscale/ngrok.

## What's in it

- **Ticker tape** — live price + % change for your 58-name watchlist (SPY/QQQ/IWM + the full sector list), via yfinance.
- **Market Feed** — headlines from Yahoo Finance per-ticker RSS across the whole watchlist, Yahoo top stories, CNBC top news, MarketWatch top stories, Federal Reserve press releases, and Trump's Truth Social posts (via the public trumpstruth.org archive RSS, since Truth Social's own API blocks scripts; gold-tagged in the feed). Deduped and filterable by source.
- **SEC Filings** — real-time feed straight from SEC EDGAR (`getcurrent`), pulling 8-K, 4, 13D, 13G, S-1, 424B, 6-K. Filterable by form type.
- **Insider Activity (Form 4)** — recent Form 4 filings via EDGAR full-text search (filer/company + link; SEC's full-text index doesn't expose parsed dollar amounts, just the filing).
- **Sidebar calendar** — today's economic events, watchlist ex-dividend dates, and this month's IPO calendar — all from Nasdaq's public JSON endpoints.

All sources are free and keyless. No signup, no rate-limit surprises tied to a paid plan.

## Other free sources considered and skipped

- **Benzinga's public `/feed` RSS** — checked, but it's generic evergreen content ("Best Biotech Stocks Right Now", "Best Stocks Under $10"), not real-time news. Not worth including. Benzinga's actual news API (the one STN uses) is paid.
- **PR Newswire's public RSS feeds** (both general and financial-services category) — checked, mostly small-cap PR filler (tequila launches, hotel brand refreshes) with very low signal for a watchlist-focused feed. Skipped.
- **GlobeNewswire** — couldn't find a working general/keyless feed in the time spent; their org-specific feeds need an org ID.
- **StockTwits public symbol API** — works and is free, but it's a social/sentiment stream, not news. Could be added later as a separate "chatter" panel if you want that flavor.

## What's NOT in it (vs. the STN feed you showed me)

STN's feed is built on paid wire access: Benzinga's real API, PR Newswire/GlobeNewswire/BusinessWire/AccessWire structured feeds, and a selective X/Twitter monitor. Those require paid API contracts I don't have credentials for, so they're not replicated. If you want closer parity later:
- Benzinga News API (paid) is the single biggest gap — it's the backbone of STN's speed and structured ticker-tagging.
- PR wire feeds' structured/paid tiers would give ticker-tagged, faster delivery than the public RSS.
- Insider buy/sell dollar amounts (the "$401K" style STN shows) would need parsing the actual Form 4 XML, not just the filing index — doable but a separate build.

## Files

- `app.py` — Flask server, caches each data source for a short TTL (news 90s, filings 60s, calendar 15min, tape 30s) so you're not hammering SEC/Nasdaq/Yahoo on every page load.
- `news_sources.py` — all the fetchers, one function per source.
- `newsfeed.html` — the single-page dashboard, styled to the Coinfish brand board (navy/teal/gold, Inter/Open Sans).

## Editing the watchlist

Edit the `WATCHLIST` list at the top of `app.py`. It's a plain Python list, not imported from the scanner, so this tool doesn't drag in pandas/numpy/bs4 just to get ticker names.

## Login and full-article reading (added 2026-09-16)

- **Login:** set the `NEWSFEED_PASSWORD` environment variable (Railway service > Variables) to put the whole site behind a password page. Sessions last 90 days. Changing the password signs everyone out. Sign out at `/logout`.
- **Full articles in the pop-up:** clicking a Market Feed headline opens a pop-up. For sources that allow it (Fox Business, Yahoo Finance, Federal Reserve, Nasdaq, Zero Hedge, BBC, Business Insider, Axios, Trump's Truths) the full article text is loaded into the pop-up via `/api/article`. Paywalled outlets (WSJ, MarketWatch, Bloomberg, NYT, Financial Times) are never fetched, and CNBC / Investing.com / Seeking Alpha block server requests, so those show the summary plus a link.
- **Full text only works with the login on.** With `NEWSFEED_PASSWORD` unset, `/api/article` refuses to serve article text, so other outlets' full articles are never shown on an open site.
- Article text is extracted with `trafilatura` and cached in memory for 6 hours.
- Added outlets: NYT (Business + Economy), Financial Times (Markets), Seeking Alpha (Market Currents), Investing.com (Stock Market + Economy), Nasdaq (Markets + Stocks), Zero Hedge, BBC Business, Business Insider, Axios. Business Insider and Axios only offer general feeds, so some non-market stories come through from those two.

## Rates and auctions (added 2026-09-24)

- **Yield curve in the Macro Snapshot:** 3-Month, 2-Year, 5-Year, 7-Year, 10-Year, 30-Year, each with the day's move in basis points. Red = yields up (bonds sold off), green = yields down.
  - 3M / 5Y / 10Y / 30Y are live intraday via yfinance (`^IRX`, `^FVX`, `^TNX`, `^TYX`), change measured against the previous close.
  - 2Y and 7Y have no reliable free intraday series (Yahoo's `2YY=F` futures quote disagrees with the cash curve by ~40bp), so they come from Treasury's official daily par yield curve XML and are labeled "Treasury close <date>". That file publishes around 3:30pm ET, so during the session those two show the prior day.
- **Curve spreads:** 2s10s (10Y minus 2Y) and 3m10s (10Y minus 3M), with the day's change. Turns red when inverted.
- **Fed funds:** current FOMC target range plus EFFR (where fed funds actually traded), from the NY Fed's reference-rates API (`markets.newyorkfed.org/api/rates/unsecured/effr/last/2.json`). `targetRateFrom`/`targetRateTo` on that record is the official target range, so no scraping of FOMC statements.
- **Treasury Auctions panel** (right column, under the IPO calendar), from TreasuryDirect's public API:
  - *Upcoming* — what is being auctioned, when, and size. *Notes & Bonds* filters out the bill noise.
  - *Results* — high yield (or discount rate), bid-to-cover, and the indirect share (proxy for foreign/central-bank demand). Bid-to-cover under ~2.1 on a coupon auction is flagged red, 2.4+ green. Weak auctions push yields up, which is the part that matters for short-premium positions.
  - Bill rows are dimmed; `/api/auctions` caches for 30 minutes since results publish once per security per day.
