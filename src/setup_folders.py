# setup_folders.py
"""One-time or repeatable project folder structure creator"""

from pathlib import Path
import sys


def create_project_structure(root: Path | None = None, verbose: bool = True):
    """
    Create standard folder structure.
    Safe to run multiple times (idempotent).
    """
    if root is None:
        root = Path(__file__).resolve().parents[1]

    # ── Main folders ────────────────────────────────────────────────────────
    folders = [
        # Interactive & exploration
        "notebooks",
        "notebooks/scratch",
        "notebooks/outputs",

        # Code
        "src",

        # Top-level input (non-tabular + SQL)
        "input",
        "input/SQL",

        # ── Structured tabular input (renamed for clarity) ─────────────────
        "data/input_data",
        "data/input_data/raw_materials",
        "data/input_data/electronics",
        "data/input_data/currencies",
        "data/input_data/bom",
        "data/input_data/stock_monitoring",
        "data/input_data/api_raw_responses",
        "data/input_data/suppliers",
        "data/input_data/archived",

        # Database folder — ONLY contains DB files
        "data/database",

        # Backups
        "data/backups",

        # Generated results
        "output/csv",
        "output/excel",
        "output/parquet",
        "output/charts/png",
        "output/charts/svg",
        "output/reports",

        # Logs
        "logs",
    ]

    created = 0

    if verbose:
        print("Setting up project folder structure...")
        print(f"Root: {root.resolve()}\n")

    for rel in folders:
        path = root / rel
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            created += 1
            if verbose:
                print(f"  Created: {rel}")

    if verbose:
        if created == 0:
            print("\nAll folders already exist.")
        else:
            print(f"\nCreated {created} new folder(s).")


def create_product_bom_folder(product_id: str, root: Path | None = None, verbose: bool = True) -> Path:
    """Create per-product BOM subfolder under data/input_data/bom/"""
    if root is None:
        root = Path(__file__).resolve().parents[1]

    product_slug = product_id.strip().lower().replace(" ", "-").replace("_", "-")
    if not product_slug:
        raise ValueError("Product ID cannot be empty")

    bom_subfolder = root / "data" / "input_data" / "bom" / product_slug

    if bom_subfolder.exists():
        if verbose:
            print(f"→ BOM folder already exists: {bom_subfolder}")
        return bom_subfolder

    subfolders = ["", "pictures", "datasheets", "schematics", "test-reports"]

    for rel in subfolders:
        (bom_subfolder / rel).mkdir(parents=True, exist_ok=True)

    (bom_subfolder / "bom_main.xlsx").touch(exist_ok=True)
    (bom_subfolder / "CHANGELOG.md").write_text(
        f"# BOM Changelog – {product_id}\n\n## {product_id} – Initial version\n",
        encoding="utf-8"
    )

    if verbose:
        print(f"→ Created product BOM folder: {bom_subfolder}")
        print(f"   → subfolders: pictures, datasheets, schematics, test-reports")
        print(f"   → placeholder files: bom_main.xlsx, CHANGELOG.md")

    return bom_subfolder


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Setup project folders or create BOM subfolder")
    parser.add_argument("--product", type=str, default=None,
                        help="Create a BOM subfolder for this product (e.g. 'widget-v1')")
    parser.add_argument("--quiet", action="store_true", help="Reduce output")

    args = parser.parse_args()
    root_path = None

    if args.product:
        create_product_bom_folder(args.product, root_path, verbose=not args.quiet)
    else:
        create_project_structure(root_path, verbose=not args.quiet)

    if not args.quiet:
        print("\nDone.")