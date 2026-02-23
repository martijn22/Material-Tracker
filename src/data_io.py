# src/data_io.py
"""
Unified loader for both FAKE and real data files into ProtoDataBase.db
Uses paths from config.py — same logic for fake & real.
Records source_file for traceability.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse

from config import *
from inventory_db import backup_existing_database

# Folders from config
RAW_DIR         = PATHS["STRUCTURED_INPUT"] / "raw_materials"
ELECTRONICS_DIR = PATHS["STRUCTURED_INPUT"] / "electronics"
CURRENCIES_DIR  = PATHS["STRUCTURED_INPUT"] / "currencies"

# Config flag — can be overridden via command line later
USE_FAKE_PREFIX = True  # Set to False when using real data


def get_file_path(base_name: str, folder: Path) -> Path | None:
    """Find file with FAKE_ or real_ prefix, or plain name"""
    prefixes = ["FAKE_", "real_", ""] if USE_FAKE_PREFIX else ["real_", "", "FAKE_"]
    for prefix in prefixes:
        candidate = folder / f"{prefix}{base_name}"
        if candidate.exists():
            return candidate
    return None


def backup_before_load():
    """Always backup before loading"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Creating backup before load...")
    success = backup_existing_database(reason="before-data-load")
    if not success:
        print("Backup failed → aborting load for safety.")
        return False
    return True


def clear_all_tables():
    """Dangerous: wipe all data from relevant tables (use only for reset)"""
    print("WARNING: Clearing all tables in database!")
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    tables = ["raw_materials", "components", "price_history", "currency_rates"]
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            print(f"Cleared table: {table}")
        except sqlite3.OperationalError as e:
            print(f"Warning: Could not clear {table} — {e}")
    conn.commit()
    conn.close()


def load_metal_current():
    file_name = "metal_prices_current.csv"
    path = get_file_path(file_name, RAW_DIR)
    if not path:
        print(f"No metal current file found for {file_name}")
        return 0

    df = pd.read_csv(path)
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        metal = row.get("metal")
        if metal not in METALS_TO_TRACK:
            continue

        cursor.execute("""
            INSERT OR REPLACE INTO raw_materials
            (category, name, unit, price, currency, stock, last_updated, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "metal",
            metal,
            row.get("unit", "unknown"),
            row.get("price_usd_per_unit", row.get("price", 0)),
            DEFAULT_CURRENCY,
            0.0,
            row.get("date", row.get("last_updated", datetime.now().strftime(DATE_FORMAT_DB))),
            path.name
        ))

        cursor.execute("SELECT id FROM raw_materials WHERE name = ?", (metal,))
        item_id = cursor.fetchone()
        if item_id:
            cursor.execute("""
                INSERT OR IGNORE INTO price_history
                (item_table, item_id, price, currency, source, date, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                "raw_materials",
                item_id[0],
                row.get("price_usd_per_unit", row.get("price", 0)),
                DEFAULT_CURRENCY,
                row.get("source", "unknown"),
                row.get("date", datetime.now().strftime(DATE_FORMAT_DB)),
                path.name
            ))
            inserted += 1

    conn.commit()
    conn.close()
    print(f"→ Loaded/updated {inserted} current metal prices from {path.name}")
    return inserted


def load_metal_history():
    file_name = "metal_price_history.csv"
    path = get_file_path(file_name, RAW_DIR)
    if not path:
        print(f"No metal history file found for {file_name}")
        return 0

    df = pd.read_csv(path)
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        metal = row.get("metal")
        cursor.execute("SELECT id FROM raw_materials WHERE name = ?", (metal,))
        item_id = cursor.fetchone()
        if item_id:
            cursor.execute("""
                INSERT OR IGNORE INTO price_history
                (item_table, item_id, price, currency, source, date, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                "raw_materials",
                item_id[0],
                row.get("price_usd_per_unit", row.get("price", 0)),
                DEFAULT_CURRENCY,
                row.get("source", "unknown"),
                row.get("date"),
                path.name
            ))
            inserted += 1

    conn.commit()
    conn.close()
    print(f"→ Added {inserted:,} metal history rows from {path.name}")
    return inserted


def load_components_and_modules():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    total_inserted = 0

    for base_name in ["electronic_components.csv", "electronic_modules.csv"]:
        path = get_file_path(base_name, ELECTRONICS_DIR)
        if not path:
            print(f"No file found for {base_name}")
            continue

        df = pd.read_csv(path)
        inserted = 0

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR IGNORE INTO components
                (category, part_number, description, price, currency, stock, last_updated, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get("category", "unknown"),
                row.get("part_number"),
                row.get("description", ""),
                row.get("price", 0),
                row.get("currency", DEFAULT_CURRENCY),
                row.get("stock", 0),
                row.get("last_updated", row.get("date", datetime.now().strftime(DATE_FORMAT_DB))),
                path.name
            ))
            inserted += cursor.rowcount

        total_inserted += inserted
        print(f"→ Added/updated {inserted} rows from {path.name}")

    conn.commit()
    conn.close()
    return total_inserted


def load_currency_current():
    file_name = "currency_rates_current.csv"
    path = get_file_path(file_name, CURRENCIES_DIR)
    if not path:
        print(f"No current currency file found for {file_name}")
        return 0

    df = pd.read_csv(path)
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO currency_rates
            (base, quote, rate, date, fetched_at, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row.get("base", CURRENCY_BASE),
            row.get("quote"),
            row.get("rate"),
            row.get("date"),
            row.get("fetched_at"),
            row.get("source", "unknown")
        ))
        inserted += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"→ Loaded {inserted} current currency rates from {path.name}")
    return inserted


def load_currency_history():
    file_name = "currency_rates_history.csv"
    path = get_file_path(file_name, CURRENCIES_DIR)
    if not path:
        print(f"No currency history file found for {file_name}")
        return 0

    df = pd.read_csv(path)
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO currency_rates
            (base, quote, rate, date, fetched_at, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row.get("base", CURRENCY_BASE),
            row.get("quote"),
            row.get("rate"),
            row.get("date"),
            row.get("fetched_at"),
            row.get("source", "unknown")
        ))
        inserted += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"→ Added {inserted:,} currency history rows from {path.name}")
    return inserted


def show_summary():
    conn = sqlite3.connect(DATABASE_PATH)
    tables = {
        "raw_materials": "raw_materials",
        "components": "components",
        "price_history": "price_history",
        "currency_rates": "currency_rates"
    }
    print("\nDatabase summary:")
    for name, table in tables.items():
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {name.ljust(18)} : {count:,} rows")
        except sqlite3.OperationalError as e:
            print(f"  {name.ljust(18)} : ERROR ({e})")
    conn.close()


def load_all(clear_first=False):
    """Main entry point"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Loading data...")

    if not backup_before_load():
        return

    if clear_first:
        print("Clearing tables first...")
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        for table in ["raw_materials", "components", "price_history", "currency_rates"]:
            try:
                cursor.execute(f"DELETE FROM {table}")
                print(f"  Cleared {table}")
            except sqlite3.OperationalError as e:
                print(f"  Warning: Could not clear {table} — {e}")
        conn.commit()
        conn.close()

    load_metal_current()
    load_metal_history()
    load_components_and_modules()
    load_currency_current()
    load_currency_history()

    show_summary()
    print("Load finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load FAKE or real data into database")
    parser.add_argument("--clear", action="store_true", help="Clear tables before loading")
    parser.add_argument("--fake", action="store_true", help="Force FAKE_ prefix mode")
    parser.add_argument("--real", action="store_true", help="Force real_ prefix or plain name mode")
    args = parser.parse_args()

    if args.fake:
        USE_FAKE_PREFIX = True
    elif args.real:
        USE_FAKE_PREFIX = False

    load_all(clear_first=args.clear)