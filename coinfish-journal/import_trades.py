"""
Import / refresh trade data into the journal. Runs automatically on every
server boot (see Procfile), so this has to pull in every source the journal
knows about - not just IBKR - or a restart silently reverts the dashboard to
IBKR-only. Mirrors the merge logic in app.py's /api/refresh route exactly;
if that route changes, update this too.

    python3 import_trades.py [path/to/raw_trades.json]
"""
import os
import sys
import db
import pairing

if __name__ == "__main__":
    ibkr_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw_trades_seed.json"
    db.init_db()
    round_trips, open_positions = pairing.process(ibkr_path)
    source_label = ibkr_path

    tt_path = os.path.join(os.path.dirname(__file__), "data", "tastytrade_transactions.csv")
    if os.path.exists(tt_path):
        import tastytrade_pairing
        tt_round_trips, tt_open_positions = tastytrade_pairing.process(tt_path)
        round_trips += tt_round_trips
        open_positions += tt_open_positions
        source_label += " + tastytrade_transactions.csv"

    db.replace_trades(round_trips, open_positions, source_label)
    print(f"Imported {len(round_trips)} closed trades and {len(open_positions)} open legs from {source_label}")
