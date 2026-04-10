"""
import_csv_data.py — Import CSV university data into UniSearch database
=======================================================================
Imports: name, city, state, school_url, catalog_url, and all numeric facts
from Uni_data_filled_universities_summary_.csv

Run ONCE (or re-run safely — uses ON CONFLICT):
    python import_csv_data.py

Options:
    python import_csv_data.py --preview     # show what would be imported
    python import_csv_data.py --file path/to/file.csv
"""

from pathlib import Path as _Path
import sys as _sys
_sys.path.insert(0, str(_Path(__file__).parent))
from db_config import DB_CONFIG, get_conn


import os, csv, re, sys, argparse
from datetime import datetime, timezone
import psycopg2, psycopg2.extras

CSV_FILE = "Uni_data_filled_universities_summary_.csv"
NOW      = datetime.now(timezone.utc)

# Maps CSV column → (section, label, formatter)
FACT_MAP = {
    "in_state_tuition":        ("tuition",      "In-State Tuition",        lambda v: f"${int(float(v)):,}"),
    "out_of_state_tuition":    ("tuition",      "Out-of-State Tuition",    lambda v: f"${int(float(v)):,}"),
    "total_cost_of_attendance":("tuition",      "Total Cost of Attendance",lambda v: f"${int(float(v)):,}"),
    "student_size":            ("student_life", "Total Enrollment",        lambda v: f"{int(float(v)):,}"),
    "average_sat":             ("admissions",   "Average SAT Score",       lambda v: str(int(float(v)))),
    "average_act":             ("admissions",   "Average ACT Score",       lambda v: str(int(float(v)))),
    "graduation_rate_4yr":     ("outcomes",     "4-Year Graduation Rate",  lambda v: f"{float(v)}%"),
    "median_earnings_10yr":    ("outcomes",     "Median Earnings (10yr)",  lambda v: f"${int(float(v)):,}"),
    "median_debt":             ("outcomes",     "Median Student Debt",     lambda v: f"${int(float(v)):,}"),
}


def normalize_url(raw: str) -> str | None:
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    if not raw.startswith("http"):
        raw = "https://" + raw
    return raw.rstrip("/")




def ensure_catalog_url_column():
    """Add catalog_url column to universities table if missing."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE universities
                ADD COLUMN IF NOT EXISTS catalog_url TEXT
            """)
            conn.commit()
    print("  ✓ catalog_url column ready")


def load_csv(filepath: str) -> list[dict]:
    rows = []
    with open(filepath, encoding="latin-1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def import_data(rows: list[dict], preview: bool = False):
    if preview:
        print(f"\n{'='*60}")
        print(f"  PREVIEW — {len(rows)} universities in CSV")
        print(f"{'='*60}")
        with_catalog = sum(1 for r in rows if normalize_url(r.get("course catalog links","")))
        with_tuition = sum(1 for r in rows if r.get("in_state_tuition","").strip())
        with_sat     = sum(1 for r in rows if r.get("average_sat","").strip())
        print(f"  With catalog links  : {with_catalog:,}")
        print(f"  With tuition data   : {with_tuition:,}")
        print(f"  With SAT data       : {with_sat:,}")
        print(f"\n  Sample rows:")
        for r in rows[:5]:
            print(f"  → {r['name'][:45]}")
            print(f"    Catalog: {(r.get('course catalog links','') or 'none')[:70]}")
            print(f"    Tuition: {r.get('in_state_tuition','—')}  "
                  f"SAT: {r.get('average_sat','—')}  "
                  f"Size: {r.get('student_size','—')}")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            uni_count  = 0
            fact_count = 0
            cat_count  = 0

            for i, row in enumerate(rows):
                name = (row.get("name") or "").strip()
                if not name:
                    continue

                school_url  = normalize_url(row.get("school_url", ""))
                catalog_url = normalize_url(row.get("course catalog links", ""))
                city        = (row.get("city") or "").strip() or None
                state       = (row.get("state") or "").strip() or None

                # ── Upsert university ──────────────────────────
                cur.execute("""
                    INSERT INTO universities (name, city, state, school_url, catalog_url)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        city        = COALESCE(EXCLUDED.city,        universities.city),
                        state       = COALESCE(EXCLUDED.state,       universities.state),
                        school_url  = COALESCE(EXCLUDED.school_url,  universities.school_url),
                        catalog_url = COALESCE(EXCLUDED.catalog_url, universities.catalog_url),
                        updated_at  = NOW()
                    RETURNING id
                """, (name, city, state, school_url, catalog_url))
                result = cur.fetchone()
                uni_id = result["id"]
                uni_count += 1

                if catalog_url:
                    cat_count += 1

                # ── Upsert facts ───────────────────────────────
                for col, (section, label, fmt) in FACT_MAP.items():
                    raw_val = (row.get(col) or "").strip()
                    if not raw_val:
                        continue
                    try:
                        num_val = float(raw_val)
                        txt_val = fmt(raw_val)
                    except (ValueError, TypeError):
                        continue

                    cur.execute("""
                        INSERT INTO university_facts
                            (university_id, section, label, value, value_numeric,
                             source_url, extracted_at, extractor, confidence)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (university_id, section, label) DO UPDATE SET
                            value         = EXCLUDED.value,
                            value_numeric = EXCLUDED.value_numeric,
                            extracted_at  = EXCLUDED.extracted_at,
                            extractor     = EXCLUDED.extractor,
                            confidence    = EXCLUDED.confidence
                    """, (uni_id, section, label, txt_val, num_val,
                          school_url, NOW, "csv_import_v1", 0.90))
                    fact_count += 1

                if (i + 1) % 500 == 0:
                    conn.commit()
                    print(f"  {i+1}/{len(rows)} processed...")

            conn.commit()

    print(f"\n{'='*60}")
    print(f"  ✅ IMPORT COMPLETE")
    print(f"  Universities upserted : {uni_count:,}")
    print(f"  Facts upserted        : {fact_count:,}")
    print(f"  With catalog URLs     : {cat_count:,}")
    print(f"{'='*60}\n")


def main():
    ap = argparse.ArgumentParser(description="Import CSV university data")
    ap.add_argument("--file",    default=CSV_FILE, help="CSV file path")
    ap.add_argument("--preview", action="store_true", help="Preview without saving")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        print(f"ERROR: File not found: {args.file}")
        sys.exit(1)

    print(f"\n📂 Loading {args.file}...")
    rows = load_csv(args.file)
    print(f"  {len(rows):,} rows loaded")

    if not args.preview:
        print("\n🔧 Ensuring catalog_url column exists...")
        ensure_catalog_url_column()

    import_data(rows, preview=args.preview)


if __name__ == "__main__":
    main()
