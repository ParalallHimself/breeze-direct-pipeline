import sqlite3
import aiosqlite
from config.settings import settings

def init_db():
    """
    Synchronously initializes database tables, performance indexes,
    and enables PRAGMA WAL mode. Runs once at application startup.
    """
    # 1. Ensure the parent storage directory (data/) exists
    settings.DB_DIR.mkdir(parents=True, exist_ok=True)

    # 2. Open standard connection
    conn = sqlite3.connect(settings.DB_PATH)
    cursor = conn.cursor()

    # 3. Enable PRAGMA WAL mode for fast concurrent read/write support
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")

    # 4. Table 1: Granular Ticks (Real-time Stream)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            stock_code TEXT NOT NULL,
            exchange_code TEXT,
            last_price REAL NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            best_bid_price REAL,
            best_bid_qty INTEGER,
            best_ask_price REAL,
            best_ask_qty INTEGER,
            last_traded_qty INTEGER,
            total_traded_vol INTEGER,
            raw_payload TEXT
        );
    """)

    # 5. Table 2: Pre-Aggregated OHLC Time-Series
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlc_candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            stock_code TEXT NOT NULL,
            interval TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER DEFAULT 0,
            UNIQUE(stock_code, timestamp, interval)
        );
    """)

    # 6. Low-Latency Performance Indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticks_stock_time ON ticks (stock_code, timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_candles_lookup ON ohlc_candles (stock_code, interval, timestamp);")

    conn.commit()
    conn.close()


async def get_db_connection():
    """
    Asynchronous connection manager for non-blocking I/O queries.
    """
    db = await aiosqlite.connect(settings.DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def purge_old_ticks(retention_days: int = 1):
    """
    Maintenance task to delete old raw ticks and keep SQLite lightweight.
    Automatically forces a WAL checkpoint to truncate journal files.
    """
    async with await get_db_connection() as db:
        await db.execute(
            "DELETE FROM ticks WHERE timestamp < datetime('now', ?);",
            (f"-{retention_days} day",)
        )
        await db.commit()
        await db.execute("PRAGMA wal_checkpoint(TRUNCATE);")