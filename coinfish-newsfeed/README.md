# Coinfish Newsfeed

Personal, local-only news dashboard modeled on the Stock Trader Network newsfeed. Not deployed publicly, no login, no public URL.

## Run it

```
cd coinfish-newsfeed
pip install flask flask-cors requests feedparser yfinance --break-system-packages
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
- **Market Feed** — headlines from Yahoo Finance per-ticker RSS across the whole watchlist, Yahoo top stories, CNBC top news, MarketWatch top stories, and Federal Reserve press releases. Deduped and filterable by source.
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

STN's feed is built on paid wire access: Benzinga's real API, PR Newswire/GlobeNewswire/BusinessWire/AccessWire structured feeds, a selective X/Twitter monitor, and a "Trump's Truths" feed. Those require paid API contracts I don't have credentials for, so they're not replicated. If you want closer parity later:
- Benzinga News API (paid) is the single biggest gap — it's the backbone of STN's speed and structured ticker-tagging.
- PR wire feeds' structured/paid tiers would give ticker-tagged, faster delivery than the public RSS.
- Insider buy/sell dollar amounts (the "$401K" style STN shows) would need parsing the actual Form 4 XML, not just the filing index — doable but a separate build.

## Files

- `app.py` — Flask server, caches each data source for a short TTL (news 90s, filings 60s, calendar 15min, tape 30s) so you're not hammering SEC/Nasdaq/Yahoo on every page load.
- `news_sources.py` — all the fetchers, one function per source.
- `newsfeed.html` — the single-page dashboard, styled to the Coinfish brand board (navy/teal/gold, Inter/Open Sans).

## Editing the watchlist

Edit the `WATCHLIST` list at the top of `app.py`. It's a plain Python list, not imported from the scanner, so this tool doesn't drag in pandas/numpy/bs4 just to get ticker names.
