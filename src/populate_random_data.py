# src/populate_random_data.py
"""
2026-02-24 — Advanced multi-format fake data generator
Generates truly different data per file type + rich individual histories.
Configurable at the top.
"""

import random
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
import sys

from config import (
    PATHS, DEFAULT_CURRENCY, DATE_FORMAT_DB,
    METALS_TO_TRACK, CURRENCIES_TO_TRACK, CURRENCY_BASE, CURRENCY_ANCHORS_20260220
)

# ==================== CONFIGURABLE VARIABLES ====================

# Raw materials
NUM_METALS = 20
NUM_PCBA = 2
NUM_PACKAGING = 4

# Electronics
NUM_COMPONENTS = 100          # total unique components
NUM_MODULES = 20               # total unique modules

# History
HISTORY_YEARS = 5
HISTORY_POINTS_PER_ITEM = 40    # approx monthly points per item

# BOM
BOM_LINES = 25                  # lines in sample BOM
MAX_BOM_COUNT = 5               # maximum BOMs to generate

# ============================================================

RAW_DIR         = PATHS["STRUCTURED_INPUT"] / "raw_materials"
ELECTRONICS_DIR = PATHS["STRUCTURED_INPUT"] / "electronics"
CURRENCIES_DIR  = PATHS["STRUCTURED_INPUT"] / "currencies"

for d in [RAW_DIR, ELECTRONICS_DIR, CURRENCIES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

random.seed(20260224)

# ── Expanded Raw Materials ──────────────────────────────────────────────────

def generate_raw_materials_current():
    today = datetime.now().strftime(DATE_FORMAT_DB)
    data = []

    # Metals
    metals = [
        "Copper", "Aluminum", "Gold", "Silver", "Nickel", "Zinc", "Tin", "Lithium",
        "Cobalt", "Palladium", "Platinum", "Gallium", "Germanium", "Titanium",
        "Tungsten", "Molybdenum", "Chromium", "Manganese", "Magnesium", "Beryllium"
    ][:NUM_METALS]
    for metal in metals:
        base = random.uniform(1.2, 5500)
        price = round(base * random.uniform(0.92, 1.08), 3)
        unit = random.choice(["kg", "lb", "oz"])
        data.append({
            "category": "metal",
            "name": metal,
            "unit": unit,
            "price": price,
            "date": today,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "raw_materials_current"
        })

    # PCBA
    pcba = ["FR4 Laminate", "High-Tg FR4", "Rogers 4350", "Aluminum Core PCB"][:NUM_PCBA]
    for item in pcba:
        data.append({
            "category": "pcba",
            "name": item,
            "unit": "sheet",
            "price": round(random.uniform(25, 210), 2),
            "date": today,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "raw_materials_current"
        })

    # Packaging
    packaging = [
        "ESD Bag", "Anti-static Foam", "Corrugated Box", "PET Tray",
        "Bubble Wrap Roll", "PE Film", "Cardboard Divider", "Silica Gel Packet"
    ][:NUM_PACKAGING]
    for item in packaging:
        data.append({
            "category": "packaging",
            "name": item,
            "unit": "piece" if "Packet" in item else "roll" if "Roll" in item else "sheet",
            "price": round(random.uniform(0.08, 12.5), 2),
            "date": today,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "raw_materials_current"
        })

    return pd.DataFrame(data)


def generate_raw_materials_history():
    end = datetime.now()
    start = end - pd.DateOffset(years=HISTORY_YEARS)
    dates = pd.date_range(start, end, freq='MS').strftime('%Y-%m-%d').tolist()
    data = []

    metals = [
        "Copper", "Aluminum", "Gold", "Silver", "Nickel", "Zinc", "Tin", "Lithium",
        "Cobalt", "Palladium", "Platinum", "Gallium", "Germanium", "Titanium",
        "Tungsten", "Molybdenum", "Chromium", "Manganese", "Magnesium", "Beryllium"
    ][:NUM_METALS]
    for metal in metals:
        price = random.uniform(1.0, 6000)
        for d in dates:
            price *= random.uniform(0.91, 1.12)
            price = round(max(0.3, price), 3)
            data.append({
                "category": "metal",
                "name": metal,
                "unit": random.choice(["kg", "lb", "oz"]),
                "price": price,
                "date": d,
                "fetched_at": f"{d} {random.randint(8,20):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
                "source": "raw_materials_history"
            })

    return pd.DataFrame(data)


# ── Electronics ─────────────────────────────────────────────────────────────

def generate_electronics_components():
    categories = ["resistor", "capacitor", "inductor", "diode", "transistor", "ic", "connector", "sensor", "led", "switch"]
    manufacturers = ["Vishay", "Murata", "TDK", "Samsung", "TI", "Infineon", "Panasonic", "ON Semi", "STMicro", "NXP"]
    packages = ["0805", "1206", "0603", "SOT-23", "TO-220", "DIP-8", "QFN-32", "SOIC-8"]
    data = []
    used = set()
    while len(data) < NUM_COMPONENTS:
        cat = random.choice(categories)
        man = random.choice(manufacturers)
        pkg = random.choice(packages)
        code = f"{cat[:3].upper()}{random.randint(10000,99999)}{random.choice('ABCDEFGH')}"
        if code in used: continue
        used.add(code)
        value = random.choice(["10k", "100k", "1k", "220uF", "100nF", "10uF", "1N4148", "2N3904", "LM358"])
        desc = f"{man} {value} {pkg} {cat.title()}"
        price = round(random.uniform(0.0005, 95.0), 4)
        stock = random.randint(25, 120000)
        data.append({
            "category": cat,
            "part_number": code,
            "description": desc,
            "price": price,
            "currency": DEFAULT_CURRENCY,
            "stock": stock,
            "date": datetime.now().strftime(DATE_FORMAT_DB),
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "electronics_components"
        })
    return pd.DataFrame(data)


def generate_electronics_modules():
    types = ["WiFi", "Bluetooth", "Power Supply", "Motor Driver", "Sensor", "Display", "Microcontroller", "ADC", "DAC"]
    makers = ["Espressif", "Seeed", "Adafruit", "SparkFun", "Waveshare", "TI", "STMicro"]
    data = []
    used = set()
    while len(data) < NUM_MODULES:
        typ = random.choice(types)
        man = random.choice(makers)
        code = f"MOD-{typ[:4].upper()}-{random.randint(1000,9999)}"
        if code in used: continue
        used.add(code)
        desc = f"{man} {typ} Module"
        price = round(random.uniform(1.85, 420.0), 2)
        stock = random.randint(5, 25000)
        data.append({
            "category": "module",
            "part_number": code,
            "description": desc,
            "price": price,
            "currency": DEFAULT_CURRENCY,
            "stock": stock,
            "date": datetime.now().strftime(DATE_FORMAT_DB),
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "electronics_modules"
        })
    return pd.DataFrame(data)


# ── Rich Histories ──────────────────────────────────────────────────────────

def generate_components_history():
    data = []
    end = datetime.now()
    for _ in range(NUM_COMPONENTS * HISTORY_POINTS_PER_ITEM):
        days_ago = random.randint(0, 365 * HISTORY_YEARS + 400)
        date = (end - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        price = round(random.uniform(0.0003, 160.0) * random.uniform(0.6, 1.5), 4)
        stock = random.randint(0, 200000)
        data.append({
            "category": random.choice(["resistor", "capacitor", "ic", "diode", "transistor", "led", "sensor", "connector"]),
            "part_number": f"C{random.randint(100000,999999)}-{random.choice('XYZ')}",
            "price": price,
            "stock": stock,
            "date": date,
            "fetched_at": f"{date} {random.randint(6,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "source": "components_history"
        })
    return pd.DataFrame(data)


def generate_modules_history():
    data = []
    end = datetime.now()
    for _ in range(NUM_MODULES * HISTORY_POINTS_PER_ITEM):
        days_ago = random.randint(0, 365 * HISTORY_YEARS + 200)
        date = (end - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        price = round(random.uniform(2.0, 650.0) * random.uniform(0.68, 1.42), 2)
        stock = random.randint(0, 35000)
        data.append({
            "category": "module",
            "part_number": f"M{random.randint(1000,9999)}{random.choice('ABCD')}",
            "price": price,
            "stock": stock,
            "date": date,
            "fetched_at": f"{date} {random.randint(8,21):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "source": "modules_history"
        })
    return pd.DataFrame(data)


# ── Currencies ──────────────────────────────────────────────────────────────

def generate_current_currencies():
    today = datetime.now().strftime(DATE_FORMAT_DB)
    data = []
    for curr in CURRENCIES_TO_TRACK:
        base_rate = CURRENCY_ANCHORS_20260220.get(curr, 1.0)
        rate = round(base_rate * random.uniform(0.995, 1.005), 4)
        data.append({
            "base": CURRENCY_BASE,
            "quote": curr,
            "rate": rate,
            "date": today,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "currencies_current"
        })
    return pd.DataFrame(data)


def generate_currency_history():
    end = datetime.now()
    start = end - pd.DateOffset(years=HISTORY_YEARS)
    dates = pd.date_range(start, end, freq='MS').strftime('%Y-%m-%d').tolist()
    data = []
    for curr in CURRENCIES_TO_TRACK:
        rate = CURRENCY_ANCHORS_20260220.get(curr, 1.0)
        for d in dates:
            rate *= random.uniform(0.98, 1.025)
            rate = round(max(0.001, rate), 4)
            data.append({
                "base": CURRENCY_BASE,
                "quote": curr,
                "rate": rate,
                "date": d,
                "fetched_at": f"{d} {random.randint(9,18):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
                "source": "currencies_history"
            })
    return pd.DataFrame(data)


# ── Sample BOM ──────────────────────────────────────────────────────────────

def generate_sample_bom(product_name="Widget v1", num_lines=BOM_LINES):
    # Use component pool
    components = generate_electronics_components()
    sample = components.sample(min(num_lines, len(components)))
    sample["quantity"] = [random.randint(1, 25) for _ in range(len(sample))]
    sample["reference"] = [f"R{i+1}" for i in range(len(sample))]
    sample["unit"] = "pcs"
    sample["notes"] = [random.choice(["SMT", "Through-hole", "Critical", ""] * 10) for _ in range(len(sample))]
    bom_path = ELECTRONICS_DIR / f"FAKE_bom_{product_name.lower().replace(' ', '_')}.xlsx"
    sample.to_excel(bom_path, index=False)
    print(f"  → Generated sample BOM: {bom_path.name} ({len(sample)} lines)")


# ── Multi-format save ───────────────────────────────────────────────────────

def save_in_all_formats(df: pd.DataFrame, stem: str, folder: Path):
    base = f"FAKE_{stem}"

    df.to_csv(folder / f"{base}.csv", index=False)
    print(f"  → {base}.csv")

    payload = {
        "success": True,
        "timestamp": int(time.time()),
        "fetched_at": datetime.now().isoformat(),
        "date": datetime.now().strftime(DATE_FORMAT_DB),
        "record_count": len(df),
        "data": df.to_dict(orient="records")
    }
    (folder / f"{base}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"  → {base}.json")

    df.to_parquet(folder / f"{base}.parquet", index=False)
    print(f"  → {base}.parquet")

    df.to_excel(folder / f"{base}.xlsx", index=False)
    print(f"  → {base}.xlsx")

    html = df.to_html(index=False, border=1, classes="table table-striped table-hover")
    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>{stem}</title>
<style>body{{font-family:Arial;margin:20px}} table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #999;padding:10px}}</style>
</head>
<body><h1>{stem.replace('_',' ').title()}</h1>{html}</body></html>"""
    (folder / f"{base}.html").write_text(full_html, encoding="utf-8")
    print(f"  → {base}.html")


# ── Main generation ─────────────────────────────────────────────────────────

def generate_all_fake_data():
    print("\n=== Generating all formats + rich histories ===\n")

    save_in_all_formats(generate_raw_materials_current(), "raw_materials_current", RAW_DIR)
    save_in_all_formats(generate_raw_materials_history(), "raw_materials_history", RAW_DIR)

    save_in_all_formats(generate_current_currencies(), "currencies_current", CURRENCIES_DIR)
    save_in_all_formats(generate_currency_history(), "currencies_history", CURRENCIES_DIR)

    save_in_all_formats(generate_electronics_components(), "electronics_components", ELECTRONICS_DIR)
    save_in_all_formats(generate_electronics_modules(), "electronics_modules", ELECTRONICS_DIR)

    print("\nGenerating large component/module history tables...")
    save_in_all_formats(generate_components_history(), "components_history", ELECTRONICS_DIR)
    save_in_all_formats(generate_modules_history(), "modules_history", ELECTRONICS_DIR)

    print("\nGenerating sample BOM...")
    generate_sample_bom("Widget v1")

    print("\n=== All formats & histories generated ===\n")


# ── Reset ───────────────────────────────────────────────────────────────────

def wipe_all_fake_files():
    count = 0
    for folder in [RAW_DIR, ELECTRONICS_DIR, CURRENCIES_DIR]:
        for f in folder.glob("FAKE_*.*"):
            f.unlink()
            count += 1
    print(f"Deleted {count} FAKE_* files.\n")


def full_reset():
    print("=== FULL MULTI-FORMAT RESET ===")
    print("Deletes all FAKE_* files, recreates DB, generates new data in 5 formats,")
    print("and loads everything (database will be large).\n")

    if input("Type YES to continue: ").strip().upper() != "YES":
        print("Cancelled.")
        return

    wipe_all_fake_files()

    from inventory_db import create_database
    create_database(force_recreate=True)

    generate_all_fake_data()

    print("Loading all generated files into database...")
    try:
        import data_io
        data_io.load_all_sources(clear_first=True)
    except Exception as e:
        print(f"Loading failed: {e}")

    print("\n=== Reset & generation complete ===\n")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--reset":
        full_reset()
    else:
        generate_all_fake_data()