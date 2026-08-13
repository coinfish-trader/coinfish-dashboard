import sqlite3
import os

# DATA_DIR lets a deployment point the database at a persistent volume
# (e.g. Railway) instead of the code directory, so notes/tags and trade
# history survive redeploys. Defaults to the local data/ folder.
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "journal.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        trade_key TEXT PRIMARY KEY,
        symbol TEXT,
        company_name TEXT,
        strategy TEXT,
        leg_count INTEGER,
        size INTEGER,
        open_time TEXT,
        close_time TEXT,
        net_premium_open REAL,
        pnl REAL,
        commission_total REAL,
        pnl_after_commission REAL,
        pct_return_on_credit REAL,
        result TEXT,
        source TEXT DEFAULT 'ibkr'
    );

    CREATE TABLE IF NOT EXISTS open_positions (
        symbol TEXT,
        company_name TEXT,
        strategy TEXT,
        leg_count INTEGER,
        size INTEGER,
        open_time TEXT,
        net_premium_open REAL
    );

    CREATE TABLE IF NOT EXISTS trade_meta (
        trade_key TEXT PRIMARY KEY,
        notes TEXT DEFAULT '',
        tags TEXT DEFAULT '',
        mistake_flag INTEGER DEFAULT 0,
        updated_at TEXT
    );

    CREATE TABLE IF NOT EXISTS import_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT,
        source_file TEXT,
        trade_count INTEGER,
        open_count INTEGER
    );

    CREATE TABLE IF NOT EXISTS trade_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_time TEXT,
        symbol TEXT,
        strategy TEXT,
        strikes TEXT,
        expiry TEXT,
        size INTEGER,
        planned_credit REAL,
        vix REAL,
        price_vs_9ema TEXT,
        price_vs_21ema TEXT,
        ema_cross TEXT,
        macd TEXT,
        efi TEXT,
        squeeze_state TEXT,
        squeeze_bars INTEGER,
        iv_rank REAL,
        trend_read TEXT,
        thesis TEXT,
        profit_target TEXT,
        stop_plan TEXT,
        followed_process INTEGER,
        process_note TEXT,
        linked_trade_key TEXT,
        created_at TEXT
    );
    """)
    # Migration: older databases (deployed before the tastytrade importer)
    # won't have this column yet. Safe to run every startup - SQLite raises
    # "duplicate column" if it's already there, which we just swallow.
    try:
        conn.execute("ALTER TABLE trades ADD COLUMN source TEXT DEFAULT 'ibkr'")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()


def replace_trades(round_trips, open_positions, source_file):
    """Re-import: wipes and reloads trades + open_positions, but NEVER touches
    trade_meta (notes/tags), so Billy's journal entries survive a refresh."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM trades")
    cur.execute("DELETE FROM open_positions")
    for t in round_trips:
        cur.execute("""
            INSERT OR REPLACE INTO trades
            (trade_key, symbol, company_name, strategy, leg_count, size, open_time,
             close_time, net_premium_open, pnl, commission_total, pnl_after_commission,
             pct_return_on_credit, result, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            t["trade_key"], t["symbol"], t["company_name"], t["strategy"], t["leg_count"],
            t["size"], t["open_time"], t["close_time"], t["net_premium_open"], t["pnl"],
            t["commission_total"], t["pnl_after_commission"], t["pct_return_on_credit"], t["result"],
            t.get("source", "ibkr")
        ))
        cur.execute("""
            INSERT OR IGNORE INTO trade_meta (trade_key, notes, tags, mistake_flag, updated_at)
            VALUES (?, '', '', 0, NULL)
        """, (t["trade_key"],))
    for p in open_positions:
        cur.execute("""
            INSERT INTO open_positions
            (symbol, company_name, strategy, leg_count, size, open_time, net_premium_open)
            VALUES (?,?,?,?,?,?,?)
        """, (p["symbol"], p["company_name"], p["strategy"], p["leg_count"], p["size"],
              p["open_time"], p["net_premium_open"]))

    import datetime
    cur.execute("""
        INSERT INTO import_log (imported_at, source_file, trade_count, open_count)
        VALUES (?, ?, ?, ?)
    """, (datetime.datetime.utcnow().isoformat(), source_file, len(round_trips), len(open_positions)))

    conn.commit()
    conn.close()


def get_all_trades():
    conn = get_conn()
    rows = conn.execute("""
        SELECT t.*, m.notes, m.tags, m.mistake_flag
        FROM trades t
        LEFT JOIN trade_meta m ON t.trade_key = m.trade_key
        ORDER BY t.close_time DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_open_positions():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM open_positions ORDER BY open_time DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_trade_meta(trade_key, notes=None, tags=None, mistake_flag=None):
    import datetime
    conn = get_conn()
    cur = conn.cursor()
    existing = cur.execute("SELECT * FROM trade_meta WHERE trade_key=?", (trade_key,)).fetchone()
    if existing is None:
        cur.execute("INSERT INTO trade_meta (trade_key, notes, tags, mistake_flag, updated_at) VALUES (?,?,?,?,?)",
                    (trade_key, notes or "", tags or "", int(bool(mistake_flag)), datetime.datetime.utcnow().isoformat()))
    else:
        new_notes = notes if notes is not None else existing["notes"]
        new_tags = tags if tags is not None else existing["tags"]
        new_flag = int(bool(mistake_flag)) if mistake_flag is not None else existing["mistake_flag"]
        cur.execute("UPDATE trade_meta SET notes=?, tags=?, mistake_flag=?, updated_at=? WHERE trade_key=?",
                    (new_notes, new_tags, new_flag, datetime.datetime.utcnow().isoformat(), trade_key))
    conn.commit()
    conn.close()


def last_import():
    conn = get_conn()
    row = conn.execute("SELECT * FROM import_log ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None


ENTRY_FIELDS = [
    "entry_time", "symbol", "strategy", "strikes", "expiry", "size", "planned_credit",
    "vix", "price_vs_9ema", "price_vs_21ema", "ema_cross", "macd", "efi",
    "squeeze_state", "squeeze_bars", "iv_rank", "trend_read", "thesis",
    "profit_target", "stop_plan", "followed_process", "process_note",
]


def create_entry(payload):
    import datetime
    conn = get_conn()
    cur = conn.cursor()
    cols = [f for f in ENTRY_FIELDS if f in payload]
    values = [payload[c] for c in cols]
    cols.append("created_at")
    values.append(datetime.datetime.utcnow().isoformat())
    placeholders = ",".join(["?"] * len(cols))
    cur.execute(f"INSERT INTO trade_entries ({','.join(cols)}) VALUES ({placeholders})", values)
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id


def get_all_entries():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM trade_entries ORDER BY entry_time DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def link_entry(entry_id, trade_key):
    conn = get_conn()
    conn.execute("UPDATE trade_entries SET linked_trade_key=? WHERE id=?", (trade_key, entry_id))
    conn.commit()
    conn.close()


def delete_entry(entry_id):
    conn = get_conn()
    conn.execute("DELETE FROM trade_entries WHERE id=?", (entry_id,))
    conn.commit()
    conn.close()
