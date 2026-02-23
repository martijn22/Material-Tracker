# src/populate_random_data.py
"""
2026-02-20 accurate fake generator using real market levels from that date.
Designed for seamless switch to real API (same file structure).
Includes full reset capability.
"""

import random
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import sys

from config import (
    PATHS, DEFAULT_CURRENCY, DATE_FORMAT_DB,
    METALS_TO_TRACK, METAL_UNITS,
    CURRENCIES_TO_TRACK, CURRENCY_BASE, CURRENCY_ANCHORS_20260220
)

RAW_DIR = PATHS["STRUCTURED_INPUT"] / "raw_materials"
ELECTRONICS_DIR = PATHS["STRUCTURED_INPUT"] / "electronics"
CURRENCIES_DIR = PATHS["STRUCTURED_INPUT"] / "currencies"
API_RAW_DIR = PATHS["STRUCTURED_INPUT"] / "api_raw_responses"

# Ensure directories exist
for d in [RAW_DIR, ELECTRONICS_DIR, CURRENCIES_DIR, API_RAW_DIR]:
    d.mkdir(parents=True, exist_ok=True)

random.seed(20260220)

# ── Real anchors Feb 20, 2026 ──
METAL_ANCHORS = {
    "Copper": {"price": 5.75, "unit": "lb"},
    "Gold": {"price": 5025, "unit": "oz"},
    "Silver": {"price": 79.5, "unit": "oz"},
    "Palladium": {"price": 1717, "unit": "oz"},
    "Platinum": {"price": 2075, "unit": "oz"},
    "Nickel": {"price": 7.9, "unit": "lb"},
    "Aluminum": {"price": 1.39, "unit": "lb"},
    "Zinc": {"price": 1.515, "unit": "lb"},
    "Lithium": {"price": 20.8, "unit": "kg"},
    "Cobalt": {"price": 25.5, "unit": "lb"},
    "Gallium": {"price": 650, "unit": "kg"},
    "Germanium": {"price": 950, "unit": "kg"},
}

# ── Generators ──

def generate_current_metals():
    today = datetime.now().strftime(DATE_FORMAT_DB)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = []
    for metal in METALS_TO_TRACK:
        if metal not in METAL_ANCHORS: continue
        base = METAL_ANCHORS[metal]["price"]
        noise = random.uniform(-0.015, 0.015)
        price = round(base * (1 + noise), 3 if "lb" in METAL_ANCHORS[metal]["unit"] else 1)
        data.append({
            "metal": metal,
            "price_usd_per_unit": price,
            "unit": METAL_ANCHORS[metal]["unit"],
            "date": today,
            "fetched_at": now_str,
            "source": "MARKET_ANCHORED_20260220"
        })
    return pd.DataFrame(data)


def generate_metal_history():
    end = datetime.now()
    start = end - pd.DateOffset(years=10)
    dates = pd.date_range(start, end, freq='MS').strftime('%Y-%m-%d').tolist()
    data = []
    for metal in METALS_TO_TRACK:
        if metal not in METAL_ANCHORS: continue
        base = METAL_ANCHORS[metal]["price"] * random.uniform(0.5, 1.5)
        price = base
        for d in dates:
            change = random.uniform(-0.08, 0.09)
            price = round(max(0.5, price * (1 + change)), 3)
            fetched_at = f"{d} {random.randint(8,20):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
            data.append({
                "metal": metal,
                "price_usd_per_unit": price,
                "unit": METAL_ANCHORS[metal]["unit"],
                "date": d,
                "fetched_at": fetched_at,
                "source": "HISTORICAL_ANCHORED"
            })
    return pd.DataFrame(data)


def generate_current_currencies():
    today = datetime.now().strftime(DATE_FORMAT_DB)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = []
    for curr in CURRENCIES_TO_TRACK:
        base_rate = CURRENCY_ANCHORS_20260220.get(curr, 1.0)
        noise = random.uniform(-0.002, 0.002)
        rate = round(base_rate * (1 + noise), 4)
        data.append({
            "base": CURRENCY_BASE,
            "quote": curr,
            "rate": rate,
            "date": today,
            "fetched_at": now_str,
            "source": "MARKET_ANCHORED_20260220"
        })
    return pd.DataFrame(data)


def generate_currency_history():
    end = datetime.now()
    start = end - pd.DateOffset(years=10)
    dates = pd.date_range(start, end, freq='MS').strftime('%Y-%m-%d').tolist()
    data = []
    for curr in CURRENCIES_TO_TRACK:
        base_rate = CURRENCY_ANCHORS_20260220.get(curr, 1.0)
        rate = base_rate
        for d in dates:
            change = random.uniform(-0.015, 0.016)
            rate = round(max(0.001, rate * (1 + change)), 4)
            fetched_at = f"{d} {random.randint(9,17):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
            data.append({
                "base": CURRENCY_BASE,
                "quote": curr,
                "rate": rate,
                "date": d,
                "fetched_at": fetched_at,
                "source": "HISTORICAL_ANCHORED"
            })
    return pd.DataFrame(data)


def generate_electronics(n=2000, seed=None):
    if seed is not None: random.seed(seed)
    categories = ["resistor", "capacitor", "inductor", "diode", "transistor", "ic",
                  "connector", "sensor", "led", "switch", "relay", "crystal", "fuse"]
    manufacturers = ["Vishay", "Murata", "TDK", "Samsung", "Texas Instruments", "ON Semi",
                     "Panasonic", "TE Connectivity", "Infineon", "STMicro", "NXP"]
    packages = ["0805", "1206", "0603", "SOT-23", "TO-220", "DIP-8", "QFN-32", "SOIC-8"]
    data = []
    used = set()
    for _ in range(n):
        cat = random.choice(categories)
        man = random.choice(manufacturers)
        pkg = random.choice(packages)
        while True:
            code = f"{cat[:3].upper()}{random.randint(10000,99999)}{random.choice('ABCDEFGH')}"
            if code not in used:
                used.add(code)
                break
        value = random.choice(["10k", "100k", "1k", "220uF", "100nF", "10uF", "1N4148", "2N3904", "LM358"])
        desc = f"{man} {value} {pkg} {cat.title()}"
        price = round(random.uniform(0.0008, 68.0), 4)
        stock = random.randint(25, 45000)
        data.append({
            "category": cat,
            "part_number": code,
            "description": desc,
            "price": price,
            "currency": DEFAULT_CURRENCY,
            "stock": stock,
            "date": datetime.now().strftime(DATE_FORMAT_DB),
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "FAKE_ELECTRONICS_20260220"
        })
    return pd.DataFrame(data)


def generate_electronic_modules(n=200, seed=None):
    if seed is not None: random.seed(seed)
    types = ["WiFi", "Bluetooth", "Power Supply", "Motor Driver", "Sensor", "Display", "Microcontroller"]
    makers = ["Espressif", "Seeed", "Adafruit", "SparkFun", "Waveshare", "TI", "STMicro"]
    data = []
    used = set()
    for _ in range(n):
        typ = random.choice(types)
        man = random.choice(makers)
        while True:
            code = f"MOD-{typ[:4].upper()}-{random.randint(1000,9999)}"
            if code not in used:
                used.add(code)
                break
        desc = f"{man} {typ} Module"
        price = round(random.uniform(1.85, 185.0), 2)
        stock = random.randint(5, 8500)
        data.append({
            "category": "module",
            "part_number": code,
            "description": desc,
            "price": price,
            "currency": DEFAULT_CURRENCY,
            "stock": stock,
            "date": datetime.now().strftime(DATE_FORMAT_DB),
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "FAKE_MODULES_20260220"
        })
    return pd.DataFrame(data)


# ── File saving functions ──

def save_current_metals():
    df = generate_current_metals()
    df.to_csv(RAW_DIR / "FAKE_metal_prices_current.csv", index=False)
    print(f"Saved: FAKE_metal_prices_current.csv ({len(df)} rows)")


def save_metal_history():
    df = generate_metal_history()
    df.to_csv(RAW_DIR / "FAKE_metal_price_history.csv", index=False)
    print(f"Saved: FAKE_metal_price_history.csv ({len(df)} rows)")


def save_current_currencies():
    df = generate_current_currencies()
    df.to_csv(CURRENCIES_DIR / "FAKE_currency_rates_current.csv", index=False)
    print(f"Saved: FAKE_currency_rates_current.csv ({len(df)} rows)")


def save_currency_history():
    df = generate_currency_history()
    df.to_csv(CURRENCIES_DIR / "FAKE_currency_rates_history.csv", index=False)
    print(f"Saved: FAKE_currency_rates_history.csv ({len(df)} rows)")


def save_electronics():
    df = generate_electronics()
    df.to_csv(ELECTRONICS_DIR / "FAKE_electronic_components.csv", index=False)
    print(f"Saved: FAKE_electronic_components.csv ({len(df)} rows)")


def save_modules():
    df = generate_electronic_modules()
    df.to_csv(ELECTRONICS_DIR / "FAKE_electronic_modules.csv", index=False)
    print(f"Saved: FAKE_electronic_modules.csv ({len(df)} rows)")


# ── Cleanup functions ──

def wipe_all_fake_files(dry_run=True, verbose=True):
    """Delete all files starting with FAKE_ in monitored folders"""
    deleted = 0
    monitored = [RAW_DIR, ELECTRONICS_DIR, CURRENCIES_DIR, API_RAW_DIR]
    
    for folder in monitored:
        if not folder.exists():
            continue
        for item in folder.iterdir():
            if item.is_file() and item.name.startswith("FAKE_"):
                if dry_run:
                    if verbose:
                        print(f"[DRY RUN] Would delete: {item}")
                else:
                    try:
                        item.unlink()
                        if verbose:
                            print(f"Deleted: {item}")
                        deleted += 1
                    except Exception as e:
                        print(f"Error deleting {item}: {e}")
    
    if dry_run:
        print(f"\nDry run: {deleted} files would be deleted.")
    else:
        print(f"\nDeleted {deleted} FAKE_ files.")
    return deleted


def wipe_fake_file(filename_pattern: str, folder_name: str = None, dry_run=True, verbose=True):
    """Delete specific FAKE_ file(s) matching pattern"""
    deleted = 0
    if not filename_pattern.startswith("FAKE_"):
        filename_pattern = "FAKE_" + filename_pattern.lstrip("FAKE_")

    target_dirs = []
    if folder_name:
        folder = PATHS["STRUCTURED_INPUT"] / folder_name
        if folder.exists():
            target_dirs = [folder]
    else:
        target_dirs = [RAW_DIR, ELECTRONICS_DIR, CURRENCIES_DIR, API_RAW_DIR]

    for folder in target_dirs:
        if not folder.exists():
            continue
        for item in folder.iterdir():
            if item.is_file() and item.name == filename_pattern:
                if dry_run:
                    if verbose:
                        print(f"[DRY RUN] Would delete: {item}")
                else:
                    try:
                        item.unlink()
                        if verbose:
                            print(f"Deleted: {item}")
                        deleted += 1
                    except Exception as e:
                        print(f"Error deleting {item}: {e}")

    if dry_run:
        print(f"\nDry run: {deleted} matching file(s) would be deleted.")
    else:
        print(f"\nDeleted {deleted} matching file(s).")
    return deleted


# ── Full Reset Function ──

def full_reset(confirm=False, load_after=True):
    """
    Complete reset workflow:
    1. Wipe all FAKE_ files
    2. Force recreate database
    3. Generate new fake files
    4. Optionally load them into DB
    """
    print("=== FULL PROJECT RESET ===")
    print("This will:")
    print("  - DELETE all FAKE_* files")
    print("  - DELETE and recreate the SQLite database")
    print("  - Generate fresh fake data")
    print("  - Optionally load into DB")
    
    if not confirm:
        answer = input("\nAre you sure? Type YES to continue: ").strip().upper()
        if answer != "YES":
            print("Reset cancelled.")
            return
    
    print("\nStep 1: Wiping old FAKE files...")
    wipe_all_fake_files(dry_run=False, verbose=True)
    
    print("\nStep 2: Recreating database...")
    from inventory_db import create_database
    create_database(force_recreate=True)
    
    print("\nStep 3: Generating new fake files...")
    save_current_metals()
    save_metal_history()
    save_current_currencies()
    save_currency_history()
    save_electronics()
    save_modules()
    
    if load_after:
        print("\nStep 4: Loading new data into database...")
        try:
            import data_io
            data_io.load_all(clear_first=True)
        except ImportError:
            print("Warning: data_io.py not found or load_all() missing.")
        except Exception as e:
            print(f"Loading failed: {e}")
    
    print("\n=== Reset complete ===")


# ── Normal generation (default run) ──

def generate_all():
    print("Generating fresh FAKE data...")
    save_current_metals()
    save_metal_history()
    save_current_currencies()
    save_currency_history()
    save_electronics()
    save_modules()
    print("Generation complete.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--reset":
        full_reset(confirm=True, load_after=True)
    else:
        generate_all()