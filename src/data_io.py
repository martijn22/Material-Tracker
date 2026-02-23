# src/data_io.py
"""
Unified loader for both FAKE and real data files into ProtoDataBase.db
Supports both classic CSV files and raw API-style JSON responses.
Records source_file for traceability where applicable.
"""

import sqlite3
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import argparse

from config import (
    PATHS, DATABASE_PATH, DEFAULT_CURRENCY, DATE_FORMAT_DB,
    METALS_TO_TRACK, USE_FAKE_PREFIX
)
from inventory_db import backup_existing_database, show_inventory


# ── Folder shortcuts ────────────────────────────────────────────────────────
RAW_DIR         = PATHS["STRUCTURED_INPUT"] / "raw_materials"
ELECTRONICS_DIR = PATHS["STRUCTURED_INPUT"] / "electronics"
CURRENCIES_DIR  = PATHS["STRUCTURED_INPUT"] / "currencies"
API_RAW_DIR     = PATHS["STRUCTURED_INPUT"] / "api_raw_responses"


def get_file_path(base_name: str, folder: Path) -> Path | None:
    """Find file with FAKE_ or real_ prefix, or plain name"""
    prefixes = ["FAKE_", "real_", ""] if USE_FAKE_PREFIX else ["real_", "", "FAKE_"]
    for prefix in prefixes:
        candidate = folder / f"{prefix}{base_name}"
        if candidate.exists():
            return candidate
    return None


def get_api_file_path(base_name: str) -> Path | None:
    """Same logic, but for api_raw_responses folder"""
    prefixes = ["FAKE_", "real_", ""] if USE_FAKE_PREFIX else ["real_", "", "FAKE_"]
    for prefix in prefixes:
        candidate = API_RAW_DIR / f"{prefix}{base_name}"
        if candidate.exists():
            return candidate
    return None


def backup_before_load():
    """Always backup before any destructive or large load operation"""
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


# ── CSV-based loaders (original logic) ──────────────────────────────────────

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
                row.get("source", "MARKET_ANCHORED_20260220"),
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
                row.get("source", "HISTORICAL_ANCHORED"),
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
            row.get("base", "USD"),
            row.get("quote"),
            row.get("rate"),
            row.get("date"),
            row.get("fetched_at"),
            row.get("source", "MARKET_ANCHORED_20260220")
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
            row.get("base", "USD"),
            row.get("quote"),
            row.get("rate"),
            row.get("date"),
            row.get("fetched_at"),
            row.get("source", "HISTORICAL_ANCHORED")
        ))
        inserted += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"→ Added {inserted:,} currency history rows from {path.name}")
    return inserted


# ── API JSON loader ─────────────────────────────────────────────────────────

def load_from_api_raw(response_base: str | None = None, clear_first: bool = False):
    """
    Load data from raw API-style JSON files (FAKE_* or real_*).
    Expects files with structure: {"data": [...]} or flat list of records.
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Loading from API raw responses...")

    if not backup_before_load():
        return

    if clear_first:
        clear_all_tables()

    files_to_try = [
        "metals_current.json",
        "metals_history.json",
        "currencies_current.json",
        "currencies_history.json",
        "electronics_components.json",
        "electronics_modules.json",
    ] if response_base is None else [response_base]

    for base_name in files_to_try:
        path = get_api_file_path(base_name)
        if not path:
            print(f"  No file: {base_name} (tried FAKE_/real_/plain)")
            continue

        print(f"  Processing {path.name} ...")
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        # Normalize: support {"data": [...]} and flat list
        raw_data = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
        if not isinstance(raw_data, list):
            print(f"  → Skipping: invalid format in {path.name}")
            continue

        df = pd.DataFrame(raw_data)
        fname = path.name.lower()
        source_file = path.name

        if "metals_current" in fname:
            load_metal_current_from_df(df, source_file)   # we'll define helpers below
        elif "metals_history" in fname:
            load_metal_history_from_df(df, source_file)
        elif "currencies" in fname:
            load_currency_from_df(df)
        elif "electronics_components" in fname or "electronics_modules" in fname:
            load_components_from_df(df, source_file)
        else:
            print(f"  → Unknown API type: {path.name} — skipping")

    show_inventory()
    print("API raw loading complete.\n")


# ── Reusable DF → DB insert helpers (to avoid code duplication) ─────────────

def load_metal_current_from_df(df: pd.DataFrame, source_file: str):
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
            row.get("price_usd_per_unit", row.get("price", 0.0)),
            DEFAULT_CURRENCY,
            0.0,
            row.get("date", datetime.now().strftime(DATE_FORMAT_DB)),
            source_file
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
                row.get("price_usd_per_unit", row.get("price", 0.0)),
                DEFAULT_CURRENCY,
                row.get("source", "API_RAW"),
                row.get("date", datetime.now().strftime(DATE_FORMAT_DB)),
                source_file
            ))
            inserted += 1
    conn.commit()
    conn.close()
    print(f"  → Loaded {inserted} current metals from {source_file}")


def load_metal_history_from_df(df: pd.DataFrame, source_file: str):
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
                row.get("price_usd_per_unit", row.get("price", 0.0)),
                DEFAULT_CURRENCY,
                row.get("source", "API_RAW"),
                row.get("date"),
                source_file
            ))
            inserted += 1
    conn.commit()
    conn.close()
    print(f"  → Added {inserted:,} metal history entries from {source_file}")


def load_components_from_df(df: pd.DataFrame, source_file: str):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
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
            row.get("price", 0.0),
            row.get("currency", DEFAULT_CURRENCY),
            row.get("stock", 0),
            row.get("last_updated", row.get("date", datetime.now().strftime(DATE_FORMAT_DB))),
            source_file
        ))
        inserted += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  → Loaded {inserted} components/modules from {source_file}")


def load_currency_from_df(df: pd.DataFrame):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO currency_rates
            (base, quote, rate, date, fetched_at, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row.get("base", "USD"),
            row.get("quote"),
            row.get("rate"),
            row.get("date"),
            row.get("fetched_at"),
            row.get("source", "API_RAW")
        ))
        inserted += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  → Loaded {inserted} currency rates from API JSON")


# ── Classic unified entry point (CSV style) ─────────────────────────────────

def load_all(clear_first=False):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Loading classic CSV data...")
    if not backup_before_load():
        return
    if clear_first:
        clear_all_tables()
    load_metal_current()
    load_metal_history()
    load_components_and_modules()
    load_currency_current()
    load_currency_history()
    show_inventory()
    print("Classic CSV load finished.")


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data into ProtoDataBase.db")
    parser.add_argument("--clear", action="store_true", help="Clear tables before loading")
    parser.add_argument("--fake", action="store_true", help="Force FAKE_ prefix mode")
    parser.add_argument("--real", action="store_true", help="Force real_ prefix / plain name mode")
    parser.add_argument("--api", action="store_true", help="Load from API-style JSON files instead of CSV")
    parser.add_argument("--file", type=str, default=None, help="Load only this specific API file (e.g. metals_current.json)")
    args = parser.parse_args()

    if args.fake:
        USE_FAKE_PREFIX = True
    elif args.real:
        USE_FAKE_PREFIX = False

    if args.api:
        load_from_api_raw(response_base=args.file, clear_first=args.clear)
    else:
        load_all(clear_first=args.clear)