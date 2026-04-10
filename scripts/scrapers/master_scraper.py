"""
master_scraper.py — Scrapes ALL universities using CSV catalog URLs
===================================================================
Uses the exact URL from the CSV for each university.
Handles: Acalog, SmartCatalogIQ, generic HTML pages.

USAGE:
  python master_scraper.py --all              # scrape everything
  python master_scraper.py --all --reset      # wipe and re-scrape
  python master_scraper.py --id 3087          # one university
  python master_scraper.py --name "alabama"   # by name
  python master_scraper.py --fix-cats         # fix categories (no HTTP)
  python master_scraper.py --top20            # update top20 (no HTTP)
  python master_scraper.py --resume           # skip already-scraped
"""

import os
import re
import csv
import json
import time
import argparse
from pathlib import Path
from urllib.parse import urlparse

# ── Path setup ────────────────────────────────────────────────
# Adds this directory to sys.path so scraper_utils and
# university_knowledge (if present) are importable.
import sys
sys.path.insert(0, str(Path(__file__).parent))

from scraper_utils import (
    get_conn, fetch, is_acalog, is_smartcatalog,
    scrape_acalog_full, scrape_generic, apply_knowledge,
    save_programs, fix_categories, update_top20,
)


# ── High-level scrape entry point ─────────────────────────────

def scrape(uni_name: str, catalog_url: str, school_url: str = "") -> list[dict]:
    """
    Attempt to scrape programs for one university.

    Strategy (in order):
    1. Try catalog.domain and bulletin.domain subdomains
    2. Fall back to the exact catalog URL from the CSV
    3. Try SmartCatalogIQ generic extraction
    4. Try generic HTML extraction
    """
    if not catalog_url or catalog_url.lower().endswith(".pdf"):
        return []

    domain = re.sub(r"^https?://(www\.)?", "", school_url or catalog_url)
    domain = re.sub(r"^www\.", "", domain.split("/")[0].lower().rstrip("/"))

    # Step 1: catalog / bulletin subdomain (most reliable for Acalog sites)
    if domain:
        for try_url in [f"https://catalog.{domain}", f"https://bulletin.{domain}"]:
            final, soup, html = fetch(try_url)
            if not soup or len(html) < 3000:
                continue
            if is_acalog(html):
                progs = scrape_acalog_full(final, soup, html)
                if progs:
                    return apply_knowledge(progs, uni_name)

    # Step 2: exact CSV URL
    final, soup, html = fetch(catalog_url)
    if not soup:
        return []

    if is_acalog(html):
        progs = scrape_acalog_full(final, soup, html)
        if progs:
            return apply_knowledge(progs, uni_name)

    if is_smartcatalog(html, final):
        progs = scrape_generic(soup, final)
        if progs:
            return apply_knowledge(progs, uni_name)

    progs = scrape_generic(soup, final)
    if progs:
        return apply_knowledge(progs, uni_name)

    return []


# ── CLI ────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Master scraper — all universities from CSV",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--all",      action="store_true")
    ap.add_argument("--id",       type=int)
    ap.add_argument("--name",     type=str)
    ap.add_argument("--reset",    action="store_true", help="Delete existing programs first")
    ap.add_argument("--resume",   action="store_true", help="Skip already catalog-scraped")
    ap.add_argument("--fix-cats", action="store_true")
    ap.add_argument("--top20",    action="store_true")
    ap.add_argument("--delay",    type=float, default=1.0)
    ap.add_argument("--csv",      type=str, default="Uni_data_filled_universities_summary_.csv")
    args = ap.parse_args()

    if args.fix_cats:
        fix_categories()
        return
    if args.top20:
        update_top20()
        return
    if not any([args.all, args.id, args.name]):
        ap.print_help()
        return

    # Load CSV catalog URL lookup
    csv_data: dict = {}
    if os.path.exists(args.csv):
        with open(args.csv, encoding="latin-1") as f:
            for row in csv.DictReader(f):
                csv_data[row["name"].strip().lower()] = {
                    "catalog_url": row.get("course catalog links", "").strip(),
                    "school_url":  row.get("school_url", "").strip(),
                }

    with get_conn() as conn:
        with conn.cursor() as cur:
            if args.id:
                cur.execute(
                    "SELECT id, name, school_url, catalog_url FROM universities WHERE id = %s",
                    (args.id,),
                )
            elif args.name:
                cur.execute(
                    "SELECT id, name, school_url, catalog_url FROM universities WHERE name ILIKE %s",
                    (f"%{args.name}%",),
                )
            else:
                cur.execute("""
                    SELECT u.id, u.name, u.school_url, u.catalog_url, COUNT(p.id) AS cnt
                    FROM universities u
                    LEFT JOIN university_programs p ON p.university_id = u.id
                    GROUP BY u.id, u.name, u.school_url, u.catalog_url
                    ORDER BY u.name
                """)
            unis = [dict(r) for r in cur.fetchall()]

    if args.resume:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT university_id FROM university_programs WHERE generated_by = 'catalog'"
                )
                done_ids = {r["university_id"] for r in cur.fetchall()}
        unis = [u for u in unis if u["id"] not in done_ids]

    if args.name and len(unis) > 1 and not args.id:
        print("Multiple matches — use --id:")
        for u in unis:
            print(f"  {u['id']}: {u['name']}")
        return

    total = len(unis)
    done = good = failed = ins_total = 0

    if total > 1:
        print(f"\n{'='*65}")
        print(f"  Master Scraper — {total:,} universities")
        print(f"  CSV: {args.csv}")
        print(f"{'='*65}\n")

    log = []
    for u in unis:
        done += 1
        name = u["name"]

        csv_entry   = csv_data.get(name.lower(), {})
        catalog_url = csv_entry.get("catalog_url") or u.get("catalog_url", "") or ""
        school_url  = csv_entry.get("school_url")  or u.get("school_url", "")  or ""

        if catalog_url and not catalog_url.startswith("http"):
            catalog_url = ""

        programs = scrape(name, catalog_url, school_url)
        cnt      = len(programs)

        if cnt > 0:
            ins, _ = save_programs(u["id"], programs, reset=args.reset)
            ins_total += ins
            good += 1
            if total == 1:
                print(f"\n✅ {name}: {cnt} programs, {ins} saved")
                by_level: dict = {}
                for p in programs:
                    by_level.setdefault(p["degree_level"], []).append(p)
                for lvl in ["Undergraduate", "Graduate", "Doctoral", "Certificate"]:
                    grp = by_level.get(lvl, [])
                    if grp:
                        print(f"\n  {lvl} ({len(grp)}):")
                        for p in sorted(grp, key=lambda x: x["name"])[:15]:
                            print(f"    {'⭐' if p['is_featured'] else '  '} {p['name']}")
                        if len(grp) > 15:
                            print(f"    ... and {len(grp)-15} more")
            else:
                print(f"  ✅ [{done}/{total}] {name[:55]:<55} {cnt:>4} programs")
        else:
            failed += 1
            if total == 1:
                print(f"\n❌ No programs found for {name}")
                print(f"   Catalog URL tried: {catalog_url}")
            else:
                print(f"  ❌ [{done}/{total}] {name[:55]:<55}   no programs")

        log.append({"id": u["id"], "name": name, "catalog_url": catalog_url, "programs": cnt})
        if cnt > 0 and total > 1:
            time.sleep(args.delay)

    if total > 1:
        print(f"\n{'='*65}")
        print(f"  ✅ Scraped: {good:,} | ❌ Failed: {failed:,} | Programs: {ins_total:,}")
        print(f"{'='*65}")
        with open("master_scrape_log.json", "w") as f:
            json.dump(log, f, indent=2)
        print("  Log: master_scrape_log.json\n")


if __name__ == "__main__":
    main()
