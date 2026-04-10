"""
scrape_all.py — Scrape real programs for all universities
=========================================================
Tries catalog subdomains, catalog URLs, and school URLs in priority order.

USAGE:
  python scrape_all.py --all                 # scrape all
  python scrape_all.py --skip-done           # skip already scraped
  python scrape_all.py --id 3087             # one university by ID
  python scrape_all.py --name "texas"        # one university by name
  python scrape_all.py --reset               # clear and re-scrape everything
  python scrape_all.py --preview             # print results without saving
"""

import re
import json
import time
import argparse
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

from scraper_utils import (
    get_conn, fetch, is_acalog, is_smartcatalog,
    scrape_acalog_full, scrape_generic, apply_knowledge,
    save_programs,
)


# ── High-level scrape entry point ─────────────────────────────

def scrape_university(uni_name: str, school_url: str, catalog_url: str) -> list[dict]:
    """
    Try to scrape programs using a priority-ordered list of candidate URLs:
    1. catalog.domain and bulletin.domain subdomains
    2. The exact catalog URL from the DB
    3. The school's homepage
    """
    domain = re.sub(r"^https?://(www\.)?", "", school_url or catalog_url or "")
    domain = re.sub(r"^www\.", "", domain.split("/")[0].lower().rstrip("/"))

    candidates = []
    if domain:
        candidates += [f"https://catalog.{domain}", f"https://bulletin.{domain}"]
    if catalog_url and catalog_url.startswith("http") and not catalog_url.endswith(".pdf"):
        candidates.append(catalog_url)
    if school_url and school_url.startswith("http"):
        candidates.append(school_url)

    for url in candidates:
        final_url, soup, html = fetch(url)
        if not soup or len(html) < 2000:
            continue
        if is_acalog(html):
            progs = scrape_acalog_full(final_url, soup, html)
            if progs:
                return apply_knowledge(progs, uni_name)
        if is_smartcatalog(html, final_url):
            progs = scrape_generic(soup, final_url)
            if progs:
                return apply_knowledge(progs, uni_name)
        progs = scrape_generic(soup, final_url)
        if progs:
            return apply_knowledge(progs, uni_name)

    return []


# ── DB helpers ────────────────────────────────────────────────

def get_all_unis(skip_done: bool = False) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.name, u.school_url, u.catalog_url, COUNT(p.id) AS cnt
                FROM universities u
                LEFT JOIN university_programs p ON p.university_id = u.id
                GROUP BY u.id, u.name, u.school_url, u.catalog_url
                ORDER BY u.name
            """)
            rows = [dict(r) for r in cur.fetchall()]

    if skip_done:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT university_id FROM university_programs WHERE generated_by = 'catalog'"
                )
                done = {r["university_id"] for r in cur.fetchall()}
        rows = [r for r in rows if r["id"] not in done]

    return rows


# ── CLI ───────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Scrape programs for all universities",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--all",       action="store_true")
    ap.add_argument("--id",        type=int)
    ap.add_argument("--name",      type=str)
    ap.add_argument("--reset",     action="store_true")
    ap.add_argument("--preview",   action="store_true")
    ap.add_argument("--skip-done", action="store_true", help="Skip already catalog-scraped")
    ap.add_argument("--delay",     type=float, default=1.0)
    args = ap.parse_args()

    if not any([args.all, args.id, args.name]):
        ap.print_help()
        return

    def _show(name, url, programs):
        print(f"\n  {'='*60}")
        print(f"  {name}")
        print(f"  Catalog: {url}")
        print(f"  Programs: {len(programs)}")
        by_level: dict = {}
        for p in programs:
            by_level.setdefault(p["degree_level"], []).append(p)
        for lvl in ["Undergraduate", "Graduate", "Doctoral", "Certificate"]:
            grp = sorted(by_level.get(lvl, []), key=lambda x: x["name"])
            if not grp:
                continue
            print(f"\n  {lvl} ({len(grp)}):")
            for p in grp[:20]:
                star = "⭐" if p["is_featured"] else "  "
                print(f"    {star} {p['name']}")
            if len(grp) > 20:
                print(f"       ... and {len(grp)-20} more")
        print()

    # Single university mode
    if args.id or args.name:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if args.id:
                    cur.execute(
                        "SELECT id, name, school_url, catalog_url FROM universities WHERE id = %s",
                        (args.id,),
                    )
                else:
                    cur.execute(
                        "SELECT id, name, school_url, catalog_url FROM universities WHERE name ILIKE %s",
                        (f"%{args.name}%",),
                    )
                matches = [dict(r) for r in cur.fetchall()]

        if not matches:
            print("Not found.")
            return
        if len(matches) > 1 and not args.id:
            print("Multiple matches:")
            for u in matches:
                print(f"  {u['id']}: {u['name']}")
            return

        u     = matches[0]
        progs = scrape_university(u["name"], u.get("school_url", ""), u.get("catalog_url", ""))
        if args.preview:
            _show(u["name"], u.get("catalog_url"), progs)
        else:
            ins, _ = save_programs(u["id"], progs, reset=args.reset)
            print(f"\n✅ {u['name']}: {len(progs)} programs, {ins} saved")
        return

    # Batch mode
    unis  = get_all_unis(skip_done=args.skip_done)
    total = len(unis)
    done  = good = failed = ins_total = 0

    print(f"\n{'='*65}")
    print(f"  Scraping {total:,} universities | delay={args.delay}s")
    print(f"{'='*65}\n")

    log = []
    for u in unis:
        progs  = scrape_university(u["name"], u.get("school_url", ""), u.get("catalog_url", ""))
        done  += 1
        cnt    = len(progs)
        ins, _ = save_programs(u["id"], progs, reset=args.reset) if not args.preview else (0, 0)
        ins_total += ins

        if   cnt >= 10: good   += 1; icon = "✅"
        elif cnt >  0:               icon = "⚠️ "
        else:           failed += 1; icon = "❌"

        print(f"  {icon} [{done}/{total}] {u['name'][:55]:<55} {cnt:>4} programs")
        log.append({"id": u["id"], "name": u["name"], "programs": cnt, "inserted": ins})
        time.sleep(args.delay)

    print(f"\n{'='*65}")
    print(f"  ✅ DONE | Good:{good} | Partial:{done-good-failed} | Failed:{failed} | Inserted:{ins_total:,}")
    print(f"{'='*65}")
    with open("scrape_all_log.json", "w") as f:
        json.dump(log, f, indent=2)
    print("  Log: scrape_all_log.json\n")


if __name__ == "__main__":
    main()
