# setup_folders.py
"""One-time or repeatable project folder structure creator"""

from pathlib import Path
import sys


def create_project_structure(root: Path | None = None, verbose: bool = True):
    """
    Create standard folder structure for inventory / electronics project.
    Safe to run multiple times (idempotent).
    """
    if root is None:
        # If this file is in src/, go two levels up (src → root)
        root = Path(__file__).resolve().parents[1]

    # ── Main folders ────────────────────────────────────────────────────────
    folders = [
        # Interactive & exploration
        "notebooks",
        "notebooks/scratch",
        "notebooks/outputs",

        # Code
        "src",

        # ── Input folders ───────────────────────────────────────────────────
        "data/input",
        "data/input/raw_materials",
        "data/input/electronics",
        "data/input/currencies",
        "data/input/bom",
        "data/input/stock_monitoring",
        "data/input/api_raw_responses",
        "data/input/suppliers",
        "data/input/archived",

        # Working data & history
        "data/processed",
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
        # else: already exists → silent

    if verbose:
        if created == 0:
            print("\nAll main folders already exist.")
        else:
            print(f"\nCreated {created} new folder(s).")


def create_product_bom_folder(product_id: str, root: Path | None = None, verbose: bool = True) -> Path:
    """
    Create a new per-product BOM subfolder under data/input/bom/
    Returns the created Path object.
    """
    if root is None:
        root = Path(__file__).resolve().parents[1]

    product_slug = product_id.strip().lower().replace(" ", "-").replace("_", "-")
    if not product_slug:
        raise ValueError("Product ID cannot be empty")

    bom_subfolder = root / "data" / "input" / "bom" / product_slug

    if bom_subfolder.exists():
        if verbose:
            print(f"→ BOM folder already exists: {bom_subfolder}")
        return bom_subfolder

    # Create standard structure inside the product folder
    subfolders = [
        "",
        "pictures",
        "datasheets",
        "schematics",
        "test-reports",
    ]

    created = 0
    for rel in subfolders:
        path = bom_subfolder / rel
        path.mkdir(parents=True, exist_ok=True)
        created += 1

    # Optional: create placeholder files
    (bom_subfolder / "bom_main.xlsx").touch(exist_ok=True)
    (bom_subfolder / "CHANGELOG.md").write_text(
        f"# BOM Changelog – {product_id}\n\n## {product_id} – Initial version\n",
        encoding="utf-8"
    )

    if verbose:
        print(f"→ Created product BOM folder: {bom_subfolder}")
        print(f"   → subfolders: pictures, datasheets, gerber, schematics, test-reports")
        print(f"   → placeholder files: bom_main.xlsx, CHANGELOG.md")

    return bom_subfolder


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Setup project folders or create BOM subfolder")
    parser.add_argument("--product", type=str, default=None,
                        help="Create a BOM subfolder for this product (e.g. 'widget-v1')")
    parser.add_argument("--quiet", action="store_true",
                        help="Reduce output verbosity")

    args = parser.parse_args()

    root_path = None  # will auto-detect

    if args.product:
        # Create specific BOM folder
        create_product_bom_folder(
            product_id=args.product,
            root=root_path,
            verbose=not args.quiet
        )
    else:
        # Create / update main structure
        create_project_structure(
            root=root_path,
            verbose=not args.quiet
        )

    if not args.quiet:
        print("\nDone.")
        if not args.product:
            print("To create a BOM folder for a product, run:")
            print("  python setup_folders.py --product \"ac-power-300w\"")