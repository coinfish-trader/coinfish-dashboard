# Coinfish Journal

Your own trade journal, v1. Local-only, modeled on TradesViz but built and controlled by you.

## Run it

Double-click `start.bat`, it installs Flask if needed, builds/refreshes the local database from `data/raw_trades_seed.json`, then starts the app. Or from a terminal:

```
pip install flask
python import_trades.py
python app.py
```

Then open **http://localhost:5060**.

The database (`data/journal.db`) is not shipped in this folder, it gets built the first time you run `import_trades.py` (or `start.bat`, which does it for you). Your notes/tags always survive a rebuild; see below.

## What v1 does

- **Dashboard**, win rate, total P&L, avg win/loss, profit factor, expectancy per trade, largest win/loss, commissions paid, current streak, equity curve, open positions, P&L broken out by ticker and by strategy.
- **Trade Log**, every closed round-trip trade: ticker, strategy, size, credit received, P&L, return on credit, win/loss. Sortable columns, filters by ticker/strategy/result, free-text search over your notes.
- **Notes & tags per trade**, click into any trade's notes/tags cell and type. Autosaves after you stop typing.
- **Calendar**, daily P&L heatmap, grouped by month.

## How trades get in (read this before you ask "why isn't today's trade showing")

This app does **not** pull live from IBKR itself, since a local Flask server has no way to call the IBKR MCP tools that only exist inside a Claude session. The workflow is:

1. Ask me (Claude, in a CoworkOS session) to pull fresh trades: `get_account_trades`.
2. I save that raw pull to `data/raw_trades_seed.json`.
3. Run `python import_trades.py` (or click "Reload from data file" in the app, which re-runs the same logic against whatever is already in that file).

This mirrors how the performance tracker xlsx gets refreshed: either ask me on demand, or set up a scheduled task to do it automatically on a cadence (daily/weekly), same pattern as `coinfish-monthly-performance-update`. Not built yet; say the word if you want it.

**Your notes and tags are never touched by a re-import.** The matching key for each trade is a hash of symbol + open time + close time + size, so as long as IBKR doesn't change historical data, your journal entries survive every refresh.

## How trades are paired (and why the numbers can be trusted)

Raw IBKR executions are leg-level (one row per option leg per fill). `pairing.py`:

1. Groups legs that filled at the same instant into one "fill event" (an opening or closing order).
2. Labels the strategy by leg count: 2 legs = vertical spread (credit or debit depending on premium direction), 4 legs = iron condor, 1 leg = single option.
3. FIFO-matches closing events against the oldest still-open lot for that ticker, size-aware (handles partial closes, e.g. a 2-lot open closed by two separate 1-lot closes).
4. Uses IBKR's own `realized_pnl` field directly as the P&L truth for every closed trade, it is never recalculated or re-derived.

That last point is the important one. TradesViz has a documented bug (see `coinfish-hq/memory.md`, 2026-08-10/11) where its own re-pairing logic fabricated phantom losing trades by inventing synthetic $0.00 closing executions. This journal can't reproduce that failure mode because it never invents an execution, every number traces back to a real IBKR record, and P&L is read, not recomputed.

Sanity-checked against trades Billy had already hand-verified in the memory file: UNP (+$55.90 6/29→7/7) and NFLX (+$36.78 6/29→7/27) both matched exactly on the first import.

## Known limitation: no strike/expiry detail

`get_account_trades` (the IBKR MCP tool this pulls from) returns symbol, side, price, size, and realized P&L, but **not** strike, expiry, or call/put. So the log can tell you "AAPL, Credit Vertical, 2 legs, +$41" but not "AAPL 220/225 Put." That's fine for win rate, expectancy, and P&L tracking, which is what v1 was built for. It is not enough to reconstruct exact spread width, so true R-multiples (P&L ÷ max risk) aren't shown, "Return on Credit" (P&L ÷ credit received) is shown instead, labeled honestly so it doesn't get mistaken for a real R-multiple.

If you want strike-level detail later (to match what TradesViz shows), the fix is wiring in IBKR Flex Query as a second data source, same feed TradesViz already uses via your Flex Web Service token. That's a clean v2 addition, doesn't require rebuilding anything here.

## What's deliberately not built yet

- Live/automatic refresh (currently manual, see above)
- Strike/expiry detail (see limitation above)
- Screenshots/chart attachments per trade
- Mistake-flag tracking (the `trade_meta` table has a `mistake_flag` column ready, just not surfaced in the UI yet)
- Multi-account support (built against the single "Coinfish" IBKR account)
- Deployment (currently local-only; can move to Railway like the scanner/newsfeed if wanted)

Say what to build next and it gets added, this was built specifically so it's easy to extend rather than a finished product.

## Files

- `app.py`, Flask app and API routes
- `pairing.py`, raw execution → round-trip trade logic
- `db.py`, SQLite schema and access (`data/journal.db`)
- `import_trades.py`, CLI to (re)load `data/raw_trades_seed.json` into the database
- `templates/journal.html`, the entire frontend (single page, Coinfish brand board styling, no external chart libraries so it never depends on a CDN being reachable)
- `data/raw_trades_seed.json`, last raw IBKR pull (YEAR_TO_DATE as of 2026-08-12, options legs only; stock liquidations from the Fidelity transfer are excluded since they're not part of the strategy)
