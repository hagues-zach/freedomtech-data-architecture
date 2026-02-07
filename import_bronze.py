from __future__ import annotations

"""
Bronze Layer NCUA Call Report Import

Downloads NCUA 5300 Call Report ZIPs, parses each text file, and stores
raw data into per-file bronze tables. One row per credit union per quarter,
with all values preserved exactly as they appear in the source files.

Columns are created dynamically — the script reads the file headers and
adds any missing columns to the table via ALTER TABLE ADD COLUMN (TEXT).

Tables: bronze_foicu, bronze_fs220, bronze_fs220a, ... bronze_fs220s

Usage:
    python import_bronze.py 2025-Q3              # Single quarter
    python import_bronze.py 2024-Q3 2025-Q3      # Range (inclusive)
    python import_bronze.py --latest              # Auto-detect latest
"""

import argparse
import csv
import os
import re
import shutil
import sys
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NCUA_BASE_URL = "https://www.ncua.gov/files/publications/analysis"

QUARTER_MONTH = {1: "03", 2: "06", 3: "09", 4: "12"}

SOURCE_FILES = {
    "FOICU.txt": "bronze_foicu",
    "FS220.txt": "bronze_fs220",
    "FS220A.txt": "bronze_fs220a",
    "FS220B.txt": "bronze_fs220b",
    "FS220C.txt": "bronze_fs220c",
    "FS220D.txt": "bronze_fs220d",
    "FS220G.txt": "bronze_fs220g",
    "FS220H.txt": "bronze_fs220h",
    "FS220I.txt": "bronze_fs220i",
    "FS220J.txt": "bronze_fs220j",
    "FS220K.txt": "bronze_fs220k",
    "FS220L.txt": "bronze_fs220l",
    "FS220M.txt": "bronze_fs220m",
    "FS220N.txt": "bronze_fs220n",
    "FS220P.txt": "bronze_fs220p",
    "FS220Q.txt": "bronze_fs220q",
    "FS220R.txt": "bronze_fs220r",
    "FS220S.txt": "bronze_fs220s",
}

ALLOWED_PROJECTS = ["jhlkpogfkytfedqupytv", "ckpihayqxwplgxdmijyp"]
BATCH_SIZE = 200

# Columns managed by the schema — not sourced from the file
KEY_COLUMNS = {"id", "cu_number", "cycle_date", "year", "quarter", "period",
               "source_url", "imported_at"}

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


def load_env(env_file: str | None = None):
    project_root = Path(__file__).resolve().parent

    if env_file:
        path = project_root / env_file
        if not path.exists():
            sys.exit(f"ERROR: Env file not found: {path}")
        load_dotenv(path)
    elif (project_root / ".env.local").exists():
        load_dotenv(project_root / ".env.local")
    else:
        load_dotenv(project_root / ".env")

    url = (os.getenv("VITE_SUPABASE_URL") or "").strip().strip("'\"")
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip().strip("'\"")

    if not url or not key:
        sys.exit("ERROR: Missing VITE_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    if not any(ref in url for ref in ALLOWED_PROJECTS):
        sys.exit(f"IMPORT BLOCKED — target project not in allow list\nTarget: {url}")

    return create_client(url, key), url


# ---------------------------------------------------------------------------
# Quarter utilities
# ---------------------------------------------------------------------------


def parse_quarter(s: str) -> tuple[int, int]:
    m = re.match(r"^(\d{4})-Q([1-4])$", s)
    if not m:
        sys.exit(f'Invalid quarter format: "{s}". Expected YYYY-QN (e.g. 2025-Q3)')
    return int(m.group(1)), int(m.group(2))


def quarter_url(year: int, quarter: int) -> str:
    return f"{NCUA_BASE_URL}/call-report-data-{year}-{QUARTER_MONTH[quarter]}.zip"


def quarter_range(start: str, end: str) -> list[str]:
    sy, sq = parse_quarter(start)
    ey, eq = parse_quarter(end)
    quarters = []
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        quarters.append(f"{y}-Q{q}")
        q += 1
        if q > 4:
            q, y = 1, y + 1
    if not quarters:
        sys.exit(f"Invalid range: {start} to {end}")
    return quarters


def detect_latest() -> str:
    from datetime import datetime, timedelta

    probe = datetime.now() - timedelta(days=45)
    year, quarter = probe.year, (probe.month - 1) // 3 + 1

    for _ in range(4):
        url = quarter_url(year, quarter)
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    result = f"{year}-Q{quarter}"
                    print(f"  Latest available quarter: {result}")
                    return result
        except Exception:
            pass
        quarter -= 1
        if quarter < 1:
            quarter, year = 4, year - 1

    sys.exit("Could not detect latest available quarter from NCUA")


# ---------------------------------------------------------------------------
# Download & extract
# ---------------------------------------------------------------------------


def download_and_extract(year: int, quarter: int) -> tuple[str, str, str]:
    url = quarter_url(year, quarter)
    tmp = tempfile.mkdtemp(prefix=f"ncua-bronze-{year}-Q{quarter}-")

    print(f"  Downloading {url}...")
    zip_path = os.path.join(tmp, "data.zip")
    urllib.request.urlretrieve(url, zip_path)

    extract_dir = os.path.join(tmp, "data")
    with zipfile.ZipFile(zip_path) as zf:
        size_mb = os.path.getsize(zip_path) / 1024 / 1024
        print(f"  Extracting ZIP ({size_mb:.1f} MB)...")
        zf.extractall(extract_dir)
    os.remove(zip_path)

    data_dir = resolve_data_dir(extract_dir)
    return tmp, data_dir, url


def resolve_data_dir(extract_dir: str) -> str:
    if os.path.exists(os.path.join(extract_dir, "FOICU.txt")):
        return extract_dir
    for entry in os.scandir(extract_dir):
        if entry.is_dir() and os.path.exists(os.path.join(entry.path, "FOICU.txt")):
            return entry.path
    sys.exit(f"FOICU.txt not found in extracted data at {extract_dir}")


# ---------------------------------------------------------------------------
# Dynamic schema: ensure columns exist in the table
# ---------------------------------------------------------------------------


def ensure_columns(supabase, table_name: str, file_headers: list[str]):
    """Add any columns from the file that don't yet exist in the table."""

    # Get current columns from the table
    resp = supabase.rpc("get_column_names", {"p_table_name": table_name}).execute()
    existing = set(resp.data) if resp.data else set()

    new_cols = [h for h in file_headers if h not in existing and h not in KEY_COLUMNS]

    if not new_cols:
        return

    print(f"    Adding {len(new_cols)} new column(s) to {table_name}...")
    for col in new_cols:
        supabase.rpc("add_text_column", {
            "p_table_name": table_name,
            "p_column_name": col,
        }).execute()

    # Tell PostgREST to reload its schema cache so it sees the new columns
    supabase.rpc("reload_schema_cache", {}).execute()
    time.sleep(1)


# ---------------------------------------------------------------------------
# Parse & ingest a single text file
# ---------------------------------------------------------------------------


def ingest_file(
    supabase, file_path: str, table_name: str, year: int, quarter: int,
    period: str, source_url: str,
):
    if not os.path.exists(file_path):
        print(f"    {os.path.basename(file_path)}: not found, skipping")
        return 0

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        # Ensure all file columns exist in the table
        ensure_columns(supabase, table_name, headers)

        rows = []
        for record in reader:
            cu_number = (record.get("CU_NUMBER") or record.get("CU_Number") or "").strip()
            if not cu_number:
                continue

            row = {
                "cu_number": cu_number,
                "cycle_date": (record.get("CYCLE_DATE") or record.get("Cycle_Date") or "").strip() or None,
                "year": year,
                "quarter": quarter,
                "period": period,
                "source_url": source_url,
            }

            # Spread every file column into the row as-is
            for header in headers:
                if header not in KEY_COLUMNS:
                    value = record.get(header, "")
                    row[header] = value if value != "" else None

            rows.append(row)

    # Batch upsert
    errors = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        resp = supabase.table(table_name).upsert(
            batch, on_conflict="cu_number,year,quarter"
        ).execute()
        if hasattr(resp, "error") and resp.error:
            print(f"    ERROR upserting {table_name}: {resp.error}")
            errors += 1

    fname = os.path.basename(file_path)
    print(f"    {fname} -> {table_name}: {len(rows)} rows ({len(headers)} columns)"
          f"{f' ({errors} batch errors)' if errors else ''}")
    return len(rows)


# ---------------------------------------------------------------------------
# Import a single quarter
# ---------------------------------------------------------------------------


def import_quarter(supabase, quarter_str: str):
    year, quarter = parse_quarter(quarter_str)
    period = quarter_str

    print(f"\n{'=' * 50}")
    print(f"  Bronze Import: {quarter_str}")
    print(f"{'=' * 50}\n")

    tmp, data_dir, source_url = download_and_extract(year, quarter)
    total_rows = 0

    try:
        for filename, table_name in SOURCE_FILES.items():
            file_path = os.path.join(data_dir, filename)
            total_rows += ingest_file(
                supabase, file_path, table_name, year, quarter, period, source_url
            )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n  Complete: {total_rows} total rows imported for {quarter_str}")
    return total_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Bronze Layer NCUA Import")
    parser.add_argument("quarters", nargs="*", help="Quarter(s) in YYYY-QN format")
    parser.add_argument("--latest", action="store_true", help="Auto-detect latest quarter")
    parser.add_argument("--env", type=str, help="Custom env file (e.g. .env.medallion)")
    args = parser.parse_args()

    supabase, url = load_env(args.env)
    print(f"Bronze Layer NCUA Import")
    print(f"Target: {url}\n")

    if args.latest:
        quarters = [detect_latest()]
    elif len(args.quarters) == 1:
        quarters = args.quarters
    elif len(args.quarters) == 2:
        quarters = quarter_range(args.quarters[0], args.quarters[1])
    else:
        parser.print_help()
        sys.exit(1)

    print(f"Quarters to import: {', '.join(quarters)}")

    total = 0
    for q in quarters:
        total += import_quarter(supabase, q)

    print(f"\n{'=' * 50}")
    print(f"  All done! {len(quarters)} quarter(s), {total} total rows")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
