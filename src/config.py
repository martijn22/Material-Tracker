"""
Configuration file - constants, paths, API keys, settings
"""

from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PATHS = {
    "RAW_DOCUMENTS":        PROJECT_ROOT / "input",
    "STRUCTURED_INPUT":     PROJECT_ROOT / "data" / "input_data",   # renamed for clarity
    "DATABASE_FOLDER":      PROJECT_ROOT / "data" / "database",     # renamed (only DB files)
    "BACKUPS":              PROJECT_ROOT / "data" / "backups",
    "EXPORT_CSV":           PROJECT_ROOT / "output" / "csv",
    "EXPORT_EXCEL":         PROJECT_ROOT / "output" / "excel",
    "EXPORT_PARQUET":       PROJECT_ROOT / "output" / "parquet",
    "CHARTS_PNG":           PROJECT_ROOT / "output" / "charts" / "png",
    "CHARTS_SVG":           PROJECT_ROOT / "output" / "charts" / "svg",
    "REPORTS":              PROJECT_ROOT / "output" / "reports",
    "NOTEBOOKS":            PROJECT_ROOT / "notebooks",
    "NOTEBOOK_SCRATCH":     PROJECT_ROOT / "notebooks" / "scratch",
    "SOURCE_CODE":          PROJECT_ROOT / "src",
    "LOGS":                 PROJECT_ROOT / "logs",
}

DATABASE_PATH = PATHS["DATABASE_FOLDER"] / "ProtoDataBase.db"

DEFAULT_CURRENCY = "USD"
DATE_FORMAT_DB = "%Y-%m-%d"
BACKUP_FILENAME_TEMPLATE = "backup_{reason}_{timestamp}.db"
BACKUP_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
MAX_BACKUP_COUNT = 10
BACKUP_WARNING_MESSAGE = (
    "→ Backup limit reached ({current}/{max_count}). "
    "Cannot create new backup without manual cleanup.\n"
    "   Location: {backup_dir}\n"
    "   Action needed: delete or move older backup_*.db files manually."
)

METALS_TO_TRACK = [
    "Copper", "Aluminum", "Iron", "Zinc", "Nickel", "Tin",
    "Gold", "Silver", "Palladium", "Platinum",
    "Lithium", "Cobalt", "Gallium", "Germanium"
]

METAL_UNITS = [
    "lb", "lb", "kg", "lb", "lb", "lb",
    "oz", "oz", "oz", "oz",
    "kg", "lb", "kg", "kg"
]

PCB_MATERIALS = [
    "FR4 Laminate Sheet",
    "Copper Clad Laminate (CCL FR4)",
    "High-Tg FR4 Laminate",
    "Aluminum Core PCB Substrate"
]

CURRENCIES_TO_TRACK = ["EUR", "CNY", "GBP", "JPY", "KRW", "TWD", "INR", "HKD"]
CURRENCY_BASE = "USD"

CURRENCY_ANCHORS_20260220 = {
    "EUR": 0.8497,
    "CNY": 6.908,
    "GBP": 0.782,
    "JPY": 148.5,
    "KRW": 1385,
    "TWD": 32.6,
    "INR": 83.7,
    "HKD": 7.81,
}

# ── Loader config ──
USE_FAKE_PREFIX = True  # Set to False when using real data files (real_*.csv or no prefix)

def get_backup_path(reason: str = "manual") -> Path:
    ts = datetime.now().strftime(BACKUP_TIMESTAMP_FORMAT)
    reason_clean = reason.replace(" ", "-").lower() if reason else "manual"
    filename = BACKUP_FILENAME_TEMPLATE.format(reason=reason_clean, timestamp=ts)
    return PATHS["BACKUPS"] / filename

def ensure_all_directories():
    for p in PATHS.values():
        p.mkdir(parents=True, exist_ok=True)