# src/data_io.py
"""
Unified multi-format loader for the inventory project.
Supports CSV, Excel (.xlsx), Parquet, JSON (API-style), HTML tables.
Uses config.DATA_SOURCES for centralized configuration.
"""

import sqlite3
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import argparse

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

from config import (
    PATHS, DATABASE_PATH, DEFAULT_CURRENCY, DATE_FORMAT_DB,
    METALS_TO_TRACK, USE_FAKE_PREFIX,
    RAW_DIR, ELECTRONICS_DIR, CURRENCIES_DIR, API_RAW_DIR,
    DATA_SOURCES
)
from inventory_db import backup_existing_database, show_inventory


def backup_before_load():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Creating backup...")
    return backup_existing_database(reason="multi-load")


def clear_all_tables():
    print("Clearing all tables...")
    conn = sqlite3.connect(DATABASE_PATH)
    tables = ["raw_materials", "components", "price_history", "currency_rates",
              "components_history", "modules_history"]
    for t in tables:
        try:
            conn.execute(f"DELETE FROM {t}")
        except:
            pass
    conn.commit()
    conn.close()


# ── Multi-format file reader ────────────────────────────────────────────────

def find_and_read_file(stem: str, folder: Path) -> list[tuple[pd.DataFrame, str]]:
    """
    Find ALL matching files for a stem and return list of (df, source_file)
    """
    extensions = [".csv", ".xlsx", ".parquet", ".json", ".html"]
    prefixes = ["FAKE_", "real_", ""] if USE_FAKE_PREFIX else ["real_", "", "FAKE_"]
    search_paths = [folder, API_RAW_DIR]
    results = []

    for base_folder in search_paths:
        for prefix in prefixes:
            for ext in extensions:
                candidate = base_folder / f"{prefix}{stem}{ext}"
                if candidate.exists():
                    try:
                        if ext == ".csv":
                            df = pd.read_csv(candidate)
                        elif ext == ".xlsx":
                            df = pd.read_excel(candidate)
                        elif ext == ".parquet":
                            df = pd.read_parquet(candidate)
                        elif ext == ".json":
                            with open(candidate, "r", encoding="utf-8") as f:
                                payload = json.load(f)
                            raw = payload.get("data", [])
                            df = pd.DataFrame(raw) if isinstance(raw, list) else None
                        elif ext == ".html" and HAS_BS4:
                            tables = pd.read_html(str(candidate))
                            df = tables[0] if tables else None
                        else:
                            df = None

                        if df is not None and not df.empty:
                            print(f"  Read {ext.upper()}: {candidate.name} ({len(df)} rows)")
                            results.append((df, candidate.name))
                    except Exception as e:
                        print(f"  Error reading {candidate.name}: {e}")

    if not results:
        print(f"  No files found for '{stem}'")
    return results


# ── Insert functions ────────────────────────────────────────────────────────

def insert_raw_materials_current(df: pd.DataFrame, source_file: str):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO raw_materials
            (category, name, unit, price, currency, stock, last_updated, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("category", "unknown"),
            row.get("name"),
            row.get("unit", "unknown"),
            row.get("price", 0.0),
            DEFAULT_CURRENCY,
            0.0,
            row.get("date", datetime.now().strftime(DATE_FORMAT_DB)),
            source_file
        ))
        inserted += 1
    conn.commit()
    conn.close()
    print(f"  → Loaded {inserted} current raw materials from {source_file}")
    return inserted


def insert_raw_materials_history(df: pd.DataFrame, source_file: str):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("SELECT id FROM raw_materials WHERE name = ?", (row.get("name"),))
        item_id = cursor.fetchone()
        if item_id:
            cursor.execute("""
                INSERT OR IGNORE INTO price_history
                (item_table, item_id, price, currency, source, date, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                "raw_materials", item_id[0],
                row.get("price", 0.0),
                DEFAULT_CURRENCY,
                row.get("source", "IMPORT"),
                row.get("date"),
                source_file
            ))
            inserted += 1
    conn.commit()
    conn.close()
    print(f"  → Added {inserted:,} raw material history rows from {source_file}")
    return inserted


def insert_currency_current(df: pd.DataFrame, source_file: str):
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
            row.get("source", "IMPORT")
        ))
        inserted += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  → Loaded {inserted} currency rates from {source_file}")
    return inserted


def insert_currency_history(df: pd.DataFrame, source_file: str):
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
            row.get("source", "IMPORT_HISTORY")
        ))
        inserted += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  → Loaded {inserted:,} historical currency rates from {source_file}")
    return inserted


def insert_components_current(df: pd.DataFrame, source_file: str):
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
            DEFAULT_CURRENCY,
            row.get("stock", 0),
            row.get("last_updated", datetime.now().strftime(DATE_FORMAT_DB)),
            source_file
        ))
        inserted += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  → Loaded {inserted} components/modules from {source_file}")
    return inserted


def insert_components_history(df: pd.DataFrame, source_file: str):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO components_history
            (category, part_number, price, stock, date, fetched_at, source_file, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("category", "unknown"),
            row.get("part_number"),
            row.get("price", 0.0),
            row.get("stock", 0),
            row.get("date"),
            row.get("fetched_at"),
            source_file,
            row.get("source", "IMPORT")
        ))
        inserted += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  → Inserted {inserted:,} rows into components_history from {source_file}")
    return inserted


def insert_modules_history(df: pd.DataFrame, source_file: str):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO modules_history
            (category, part_number, price, stock, date, fetched_at, source_file, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("category", "module"),
            row.get("part_number"),
            row.get("price", 0.0),
            row.get("stock", 0),
            row.get("date"),
            row.get("fetched_at"),
            source_file,
            row.get("source", "IMPORT")
        ))
        inserted += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  → Inserted {inserted:,} rows into modules_history from {source_file}")
    return inserted


# ── Unified loader ──────────────────────────────────────────────────────────

def load_all_sources(clear_first=False):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Loading from config.DATA_SOURCES...")

    if not backup_before_load():
        return
    if clear_first:
        clear_all_tables()

    loaded_stats = {}

    for stem, info in DATA_SOURCES.items():
        folder = info["folder"]
        insert_func_name = info.get("insert_func")

        print(f"\n→ {stem} ({info.get('description', 'unknown')})")

        results = find_and_read_file(stem, folder)
        if not results:
            continue

        for df, source_file in results:
            if insert_func_name and insert_func_name in globals():
                try:
                    insert_func = globals()[insert_func_name]
                    count = insert_func(df, source_file)
                    loaded_stats.setdefault(stem, 0)
                    loaded_stats[stem] += count
                except Exception as e:
                    print(f"  Insert failed for {source_file}: {e}")
            else:
                print(f"  No insert function defined for {stem} — skipping {source_file}")

    show_inventory()
    print("\nLoaded stats:", loaded_stats)
    print("Loading complete.\n")


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data into ProtoDataBase.db")
    parser.add_argument("--clear", action="store_true")
    args = parser.parse_args()

    load_all_sources(clear_first=args.clear)