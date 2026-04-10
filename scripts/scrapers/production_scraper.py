"""
production_scraper.py — Production-quality program scraper
==========================================================
Philosophy: Real data or nothing. Never fake data.

For each university:
1. Try catalog.domain (Acalog) → perfect data like UNT
2. Try given CSV URL → real programs if scrapeable
3. If nothing found → store catalog_url for "View Catalog" button
   NEVER show IPEDS or AI-generated fake programs

COMMANDS:
  python production_scraper.py --all           # scrape all
  python production_scraper.py --id 3087       # one university
  python production_scraper.py --name "texas"  # by name
  python production_scraper.py --fix-cats      # fix categories (instant)
  python production_scraper.py --top20         # fix top20 (instant)
  python production_scraper.py --wipe-ipeds    # remove all IPEDS/AI data
  python production_scraper.py --stats         # show DB stats
  python production_scraper.py --resume        # skip already scraped
"""

import os
import re
import csv
import json
import time
import argparse
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

from scraper_utils import (
    get_conn, fetch, is_acalog, is_smartcatalog,
    scrape_acalog_full, scrape_generic, apply_knowledge,
    save_programs, fix_categories, update_top20,
)

CSV_FILE = "Uni_data_filled_universities_summary_.csv"


# ── High-level scrape entry point ─────────────────────────────

def scrape(uni_name: str, catalog_url: str, school_url: str = "") -> list[dict]:
    """
    Try to get real programs. Returns [] if nothing found.
    NEVER returns fake data.
    """
    if not catalog_url or catalog_url.lower().endswith(".pdf"):
        return []

    domain = re.sub(r"^https?://(www\.)?", "", school_url or catalog_url)
    domain = re.sub(r"^www\.", "", domain.split("/")[0].lower().rstrip("/"))

    # Step 1: catalog / bulletin subdomain
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


# ── Production-specific DB operations ─────────────────────────

def wipe_non_catalog() -> None:
    """Remove all IPEDS and AI-generated programs. Keep only catalog-scraped ones."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM university_programs WHERE generated_by != 'catalog'"
            )
            count = cur.fetchone()["n"]
            print(f"Will delete {count:,} non-catalog programs (IPEDS/AI/scraper)...")
            confirm = input("Type 'yes' to confirm: ").strip().lower()
            if confirm != "yes":
                print("Cancelled.")
                return
            cur.execute(
                "DELETE FROM university_programs "
                "WHERE generated_by != 'catalog' AND generated_by != 'manual'"
            )
            conn.commit()
            print(f"Deleted {count:,} programs. Kept catalog + manual data only.")


def show_stats() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM universities")
            total = cur.fetchone()["n"]
            cur.execute("""
                SELECT generated_by,
                       COUNT(DISTINCT university_id) AS unis,
                       COUNT(*) AS progs
                FROM university_programs
                GROUP BY generated_by
                ORDER BY unis DESC
            """)
            rows = cur.fetchall()
            cur.execute("""
                SELECT COUNT(*) AS n FROM universities u
                WHERE NOT EXISTS (
                    SELECT 1 FROM university_programs p WHERE p.university_id = u.id
                )
            """)
            no_data = cur.fetchone()["n"]

    print(f"\nDatabase Statistics:")
    print(f"  Total universities: {total:,}")
    print(f"  With NO data:       {no_data:,}")
    print(f"\n  {'Source':<20} {'Universities':>12} {'Programs':>12}")
    print(f"  {'-'*46}")
    for r in rows:
        print(f"  {r['generated_by']:<20} {r['unis']:>12,} {r['progs']:>12,}")


# ── CLI ───────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Production scraper",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--all",        action="store_true")
    ap.add_argument("--id",         type=int)
    ap.add_argument("--name",       type=str)
    ap.add_argument("--resume",     action="store_true", help="Skip already catalog-scraped")
    ap.add_argument("--fix-cats",   action="store_true")
    ap.add_argument("--top20",      action="store_true")
    ap.add_argument("--wipe-ipeds", action="store_true", help="Delete all non-catalog data")
    ap.add_argument("--stats",      action="store_true")
    ap.add_argument("--delay",      type=float, default=1.0)
    args = ap.parse_args()

    if args.fix_cats:   fix_categories(); return
    if args.top20:      update_top20();   return
    if args.wipe_ipeds: wipe_non_catalog(); return
    if args.stats:      show_stats();     return
    if not any([args.all, args.id, args.name]):
        ap.print_help()
        return

    csv_data: dict = {}
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, encoding="latin-1") as f:
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
                cur.execute(
                    "SELECT id, name, school_url, catalog_url FROM universities ORDER BY name"
                )
            unis = [dict(r) for r in cur.fetchall()]

    if args.resume:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT university_id FROM university_programs WHERE generated_by = 'catalog'"
                )
                done = {r["university_id"] for r in cur.fetchall()}
        unis = [u for u in unis if u["id"] not in done]

    if args.name and len(unis) > 1:
        print("Multiple matches:")
        for u in unis:
            print(f"  {u['id']}: {u['name']}")
        return

    total = len(unis)
    done = good = skipped = 0
    ins_total = 0

    if total > 1:
        print(f"\n{'='*65}")
        print(f"  Production Scraper — {total:,} universities")
        print(f"  Real data only. No IPEDS. No AI.")
        print(f"{'='*65}\n")

    log = []
    for u in unis:
        done += 1
        name = u["name"]
        csv_e       = csv_data.get(name.lower(), {})
        catalog_url = csv_e.get("catalog_url") or u.get("catalog_url", "") or ""
        school_url  = csv_e.get("school_url")  or u.get("school_url", "")  or ""
        if catalog_url and not catalog_url.startswith("http"):
            catalog_url = ""

        programs = scrape(name, catalog_url, school_url)
        cnt      = len(programs)

        if cnt > 0:
            # Production mode: always wipe old catalog data before saving fresh
            ins = save_programs(u["id"], programs, wipe_catalog=True)[0]
            ins_total += ins
            good += 1
            if total == 1:
                print(f"\n✅ {name}: {cnt} programs")
                by_level: dict = {}
                for p in programs:
                    by_level.setdefault(p["degree_level"], []).append(p)
                for lvl in ["Undergraduate", "Graduate", "Doctoral", "Certificate"]:
                    grp = by_level.get(lvl, [])
                    if not grp:
                        continue
                    print(f"\n  {lvl} ({len(grp)}):")
                    for p in sorted(grp, key=lambda x: x["name"])[:20]:
                        print(f"    {'⭐' if p['is_featured'] else '  '} {p['name']}")
                    if len(grp) > 20:
                        print(f"    ... and {len(grp)-20} more")
            else:
                print(f"  ✅ [{done}/{total}] {name[:55]:<55} {cnt:>4}")
        else:
            skipped += 1
            if total == 1:
                print(f"\n❌ No programs found for {name}")
                print(f"   URL tried: {catalog_url or 'none'}")
            else:
                print(f"  ❌ [{done}/{total}] {name[:55]}")

        log.append({"id": u["id"], "name": name, "url": catalog_url, "programs": cnt})
        if cnt > 0 and total > 1:
            time.sleep(args.delay)

    if total > 1:
        print(f"\n{'='*65}")
        print(f"  Scraped: {good:,} | Failed: {skipped:,} | Programs: {ins_total:,}")
        print(f"{'='*65}")
        with open("production_scrape_log.json", "w") as f:
            json.dump(log, f, indent=2)
        print("  Log: production_scrape_log.json\n")


if __name__ == "__main__":
    main()
