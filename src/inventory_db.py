# inventory_db.py
import sqlite3
import pandas as pd
from datetime import datetime
import glob
from pathlib import Path

from config import (
    PATHS, DATABASE_PATH, DEFAULT_CURRENCY, DATE_FORMAT_DB,
    BACKUP_FILENAME_TEMPLATE, BACKUP_TIMESTAMP_FORMAT,
    MAX_BACKUP_COUNT, BACKUP_WARNING_MESSAGE,
    get_backup_path, ensure_all_directories
)

print("=== Electronics & Materials Tracker ===")
print(f"Today: {datetime.now().strftime('%Y-%m-%d')}")
print(f"Database: {DATABASE_PATH}\n")


def backup_existing_database(reason: str = "pre-operation") -> bool:
    if not DATABASE_PATH.exists():
        print("→ No existing database found → no backup needed")
        return True

    max_count = MAX_BACKUP_COUNT
    pattern = str(PATHS["BACKUPS"] / "backup_*.db")
    current_count = len(glob.glob(pattern))

    if current_count >= max_count:
        print(f"→ Backup limit reached ({current_count}/{max_count}). Skipping backup.")
        return False

    print(f"→ Creating backup ({reason}) ...")
    backup_path = get_backup_path(reason)

    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute(f"VACUUM INTO '{backup_path}'")
        conn.close()
        print(f"  Backup created: {backup_path.name}")
        return True
    except Exception as e:
        print(f"  ERROR: Backup failed → {e}")
        return False


def create_database(force_recreate: bool = False):
    if DATABASE_PATH.exists():
        if force_recreate:
            print("→ Force recreate requested. Backing up old database once...")
            backup_existing_database(reason="force-recreate")
            try:
                DATABASE_PATH.unlink()
                print(f"→ Removed old database file: {DATABASE_PATH.name}")
            except Exception as e:
                print(f"→ Warning: could not delete old DB → {e}")
                return
        else:
            print("→ Database already exists → ensuring tables exist...")
    else:
        print("→ Creating new database...")

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS raw_materials (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        category      TEXT,
        name          TEXT,
        unit          TEXT,
        price         REAL,
        currency      TEXT DEFAULT 'USD',
        stock         REAL DEFAULT 0.0,
        last_updated  TEXT,
        source_file   TEXT
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS components (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        category      TEXT,
        part_number   TEXT UNIQUE,
        description   TEXT,
        price         REAL,
        currency      TEXT DEFAULT 'USD',
        stock         INTEGER DEFAULT 0,
        last_updated  TEXT,
        source_file   TEXT
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS price_history (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        item_table    TEXT NOT NULL,
        item_id       INTEGER NOT NULL,
        price         REAL NOT NULL,
        currency      TEXT DEFAULT 'USD',
        source        TEXT,
        date          TEXT NOT NULL,
        source_file   TEXT,
        UNIQUE(item_table, item_id, date)
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS currency_rates (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        base          TEXT DEFAULT 'USD',
        quote         TEXT NOT NULL,
        rate          REAL NOT NULL,
        date          TEXT NOT NULL,
        fetched_at    TEXT,
        source        TEXT,
        UNIQUE(base, quote, date)
    )""")

    conn.commit()
    conn.close()
    print(f"→ Database ready: {DATABASE_PATH}")


def show_inventory():
    conn = sqlite3.connect(DATABASE_PATH)
    print("\n=== DATABASE SUMMARY ===")
    for table in ["raw_materials", "components", "price_history", "currency_rates"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table.ljust(20)} : {count:,} rows")
    conn.close()


if __name__ == "__main__":
    ensure_all_directories()
    create_database(force_recreate=False)
    show_inventory()