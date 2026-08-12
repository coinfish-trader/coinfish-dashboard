"""
Import / refresh trade data into the journal.

Run this after saving a fresh pull from IBKR (get_account_trades) to
data/raw_trades_seed.json (or pass a different path). Safe to re-run any time;
it never touches your notes/tags, only the trade facts.

    python3 import_trades.py [path/to/raw_trades.json]
"""
import sys
import db
import pairing

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "data/raw_trades_seed.json"
    db.init_db()
    round_trips, open_positions = pairing.process(path)
    db.replace_trades(round_trips, open_positions, path)
    print(f"Imported {len(round_trips)} closed trades and {len(open_positions)} open legs from {path}")
