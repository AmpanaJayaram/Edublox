"""
UniSearch — Fast Async Program Scraper
========================================
Scrapes programs from ALL universities concurrently.
Can process 6,000 universities in ~2-4 hours.

SETUP:
    pip install aiohttp beautifulsoup4 psycopg2-binary lxml

USAGE:
    # Scrape everything (6000 universities):
    python scrape_fast.py --all

    # Scrape but skip universities that already have data:
    python scrape_fast.py --all --skip-existing

    # Scrape one university by ID:
    python scrape_fast.py --id 3087

    # Scrape one university by name:
    python scrape_fast.py --name "north texas"

    # Preview without saving to DB:
    python scrape_fast.py --id 3087 --preview

    # Wipe and re-scrape:
    python scrape_fast.py --id 3087 --reset

    # List universities with program counts:
    python scrape_fast.py --list

    # Control concurrency (default 15):
    python scrape_fast.py --all --workers 20

    # Resume after a crash (skip existing):
    python scrape_fast.py --all --skip-existing

SPEED ESTIMATE:
    ~15 concurrent workers × ~8 pages × 0.5s avg = ~6000 unis in ~3 hours
    Program links are scraped from real catalog pages, not guessed.
"""

import asyncio
import argparse
import re
import sys
import os
import time
import json
import signal
from urllib.parse import urljoin, urlparse
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

# Load knowledge base
try:
    from university_knowledge import KNOWN_FEATURED
except ImportError:
    print("⚠️  university_knowledge.py not found — featured programs won't be applied")
    KNOWN_FEATURED = {}

# ─────────────────────────────────────────────────────────────────
# DB CONFIG
# ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME",     "unisearch"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", "2000"),
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
}

# ─────────────────────────────────────────────────────────────────
# CLASSIFIERS
# ─────────────────────────────────────────────────────────────────
CATEGORY_RULES = [
    ("Engineering",       ["engineering","electrical","mechanical","civil","chemical",
                           "biomedical","aerospace","industrial","materials science",
                           "computer engineering","petroleum","nuclear"]),
    ("STEM",              ["computer science","data science","mathematics","statistics",
                           "physics","chemistry","biology","geology","astronomy",
                           "neuroscience","biochemistry","information science",
                           "cybersecurity","information technology","software",
                           "environmental science","ecology","bioinformatics",
                           "applied mathematics","data analytics","machine learning"]),
    ("Health & Medicine", ["nursing","medicine","public health","pharmacy","audiology",
                           "speech","pathology","kinesiology","nutrition","dental",
                           "physical therapy","occupational therapy","radiologic",
                           "clinical","health services","health administration",
                           "medical","healthcare","health science","rehabilitation",
                           "athletic training","dietetics"]),
    ("Business",          ["business","accounting","finance","marketing","management",
                           "economics","entrepreneurship","supply chain","real estate",
                           "hospitality","merchandising","fashion","retail","logistics",
                           "human resources","operations","mba"]),
    ("Education",         ["education","teaching","curriculum","instruction","counseling",
                           "educational leadership","special education","literacy",
                           "early childhood","higher education","school psychology"]),
    ("Law & Policy",      ["law","political science","public administration","public policy",
                           "government","criminal justice","legal","emergency management",
                           "homeland security","international relations","diplomacy",
                           "urban planning","nonprofit","forensic"]),
    ("Arts & Humanities", ["music","art","dance","theatre","theater","film","journalism",
                           "english","history","philosophy","languages","literature",
                           "creative writing","visual arts","studio art","design",
                           "media","communication","radio","television","digital media",
                           "humanities","linguistics","jazz","performance","composition",
                           "graphic design","animation","photography","architecture",
                           "advertising"]),
    ("Social Sciences",   ["sociology","psychology","anthropology","social work",
                           "women's studies","gender","ethnic studies","cultural studies",
                           "international studies","child development","family studies",
                           "behavioral science","cognitive science"]),
]
DEGREE_PREFIXES = [
    ("Doctoral",      ["doctor of philosophy","ph.d.","phd in","doctor of education",
                       "ed.d.","doctor of musical arts","d.m.a.","juris doctor","j.d.",
                       "doctor of pharmacy","pharm.d.","doctor of nursing","dnp",
                       "doctor of medicine","m.d.","doctor of business","d.b.a.",
                       "doctor of veterinary"]),
    ("Graduate",      ["master of","master's","m.s. in","m.a. in","m.b.a.","m.ed.",
                       "m.f.a.","m.p.h.","m.p.a.","m.eng.","m.arch.","m.mus.",
                       "m.s.w.","graduate certificate","post-baccalaureate"]),
    ("Certificate",   ["certificate in","minor in","concentration in","endorsement in"]),
    ("Undergraduate", ["bachelor of","bachelor's","b.s. in","b.a. in","b.f.a.","b.mus.",
                       "b.b.a.","b.s.w.","b.arch.","associate of","associate's","a.s.","a.a."]),
]
DEGREE_INDICATORS = [
    "bachelor","master","doctor","ph.d","phd","b.s.","b.a.",
    "m.s.","m.a.","mba","m.b.a","bfa","mfa","b.f.a","m.f.a",
    "certificate","minor in","associate","d.m.a","ed.d","m.ed",
    "dpt","dnp","pharm.d","j.d.","juris doctor","d.d.s"
]
NOISE_WORDS = {
    "click here","learn more","apply now","contact us","home","about",
    "news","events","login","register","search","menu","back to",
    "return to","view all","see all","read more","next","previous",
    "download","print","share","visit","tour","explore","skip to",
    "request info","request information","get started"
}

def classify_category(name: str) -> str:
    n = name.lower()
    for cat, kws in CATEGORY_RULES:
        if any(kw in n for kw in kws):
            return cat
    return "STEM"

def classify_degree(name: str) -> str:
    n = name.lower()
    for level, indicators in DEGREE_PREFIXES:
        if any(n.startswith(ind) or f" {ind}" in n for ind in indicators):
            return level
    return "Undergraduate"

import re as _re

# Generic headings that are NOT real programs
_GENERIC_ENDINGS = {
    "degrees", "programs", "certificates", "majors", "minors",
    "degrees and programs", "degrees & programs", "undergraduate programs",
    "graduate programs", "doctoral programs", "areas of study",
    "fields of study", "academic programs", "course catalog",
    "programs and degrees", "programs & degrees", "programs of study",
    "undergraduate degrees", "graduate degrees", "doctoral degrees",
    "bachelor's degrees", "master's degrees", "associate degrees",
    "certificate programs", "online programs", "degree programs",
}

# A real program MUST match one of these patterns (degree + specific subject)
_REAL_PROGRAM_PATTERNS = [
    # "Bachelor of X" / "Master of X" / "Doctor of X"
    r"\b(bachelor|master|doctor)\s+of\s+\w",
    # "Bachelor's in X" / "Master's in X"
    r"\b(bachelor|master)'s\s+in\s+\w",
    # Abbreviations: "B.S. in X", "M.A. in X", "Ph.D. in X"
    r"\b(b\.s\.|b\.a\.|m\.s\.|m\.a\.|m\.ed\.|m\.f\.a\.|b\.f\.a\.|b\.b\.a\.|b\.mus\.|m\.mus\.|m\.b\.a\.?|m\.p\.h\.|m\.p\.a\.|m\.eng\.|b\.arch\.|m\.arch\.|b\.s\.w\.|m\.s\.w\.)\s*(in|of)\s+\w",
    # "Ph.D." or "Ph.D. in X"
    r"\bph\.d\.?\b",
    r"\bphd\s+in\s+\w",
    # "Juris Doctor", "J.D.", "Pharm.D.", "D.M.A.", "Ed.D.", "D.N.P."
    r"\bjuris\s+doctor\b",
    r"\bj\.d\.?\b",
    r"\bpharm\.d\.?\b",
    r"\bd\.m\.a\.?\b",
    r"\bed\.d\.?\b",
    r"\bd\.n\.p\.?\b",
    r"\bd\.p\.t\.?\b",
    r"\bd\.b\.a\.?\b",
    r"\bm\.d\.?\b",
    r"\bd\.d\.s\.?\b",
    # "Certificate in X" / "Minor in X"
    r"\bcertificate\s+in\s+\w",
    r"\bminor\s+in\s+\w",
    r"\bgraduate\s+certificate\s+in\s+\w",
    # "Associate of X" / "Associate in X"
    r"\bassociate\s+(of|in)\s+\w",
    # "M.B.A." standalone
    r"\bmba\b",
    r"\bm\.b\.a\.?\b",
]
_COMPILED_PATTERNS = [_re.compile(p, _re.IGNORECASE) for p in _REAL_PROGRAM_PATTERNS]

def looks_like_program(text: str) -> bool:
    if not text or len(text) < 10 or len(text) > 200:
        return False
    t = text.lower().strip()

    # Reject generic navigation headings
    if t in _GENERIC_ENDINGS:
        return False
    # Reject anything that ends with a generic heading word
    if any(t.endswith(ending) for ending in _GENERIC_ENDINGS):
        return False
    # Reject pure noise
    if any(n in t for n in NOISE_WORDS):
        return False
    # Must match a real degree+subject pattern
    return any(pat.search(t) for pat in _COMPILED_PATTERNS)

def find_known_data(uni_name: str) -> dict | None:
    uni_lower = uni_name.lower()
    for key, data in KNOWN_FEATURED.items():
        if key in uni_lower:
            return data
    return None

# ─────────────────────────────────────────────────────────────────
# ASYNC SCRAPER
# ─────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Catalog paths to try per university (ordered by likelihood)
CATALOG_PATHS = [
    "/catalog",
    "/academics/programs",
    "/academics/degrees-and-programs",
    "/academics/undergraduate-programs",
    "/academics/graduate-programs",
    "/programs",
    "/degrees",
    "/majors",
    "/programs-of-study",
    "/academics",
    "/future-students/academics",
    "/admissions/academics",
    "/undergraduate/programs",
    "/graduate/programs",
    "/catalog/programs",
]

# Catalog subdomains to try
CATALOG_SUBDOMAINS = ["catalog", "graduate", "admissions", "academics"]


def resolve_url(href: str, base: str, base_domain: str) -> str | None:
    if not href:
        return None
    href = href.split("?")[0].split("#")[0].strip()
    if not href or href.startswith(("mailto:", "javascript:", "tel:")):
        return None
    full   = urljoin(base, href)
    parsed = urlparse(full)
    if base_domain and base_domain not in parsed.netloc:
        return None
    return full


def extract_programs(html: str, page_url: str, base_domain: str) -> tuple[list[dict], list[str]]:
    """
    Parse HTML page and extract:
      - programs: list of {name, url} dicts
      - sub_urls:  links that look like program sub-pages to crawl next
    """
    soup     = BeautifulSoup(html, "lxml")
    programs = []
    sub_urls = []
    seen     = set()

    def add_program(text, url):
        text = re.sub(r"\s+", " ", text.strip())
        text = re.sub(r"\s*[-–|].*$", "", text).strip()
        if not looks_like_program(text) or text.lower() in seen:
            return
        seen.add(text.lower())
        programs.append({"name": text, "url": url})

    # All anchor tags
    for a in soup.find_all("a", href=True):
        text     = a.get_text(separator=" ", strip=True)
        link_url = resolve_url(a["href"], page_url, base_domain)
        if not link_url:
            continue
        if looks_like_program(text):
            add_program(text, link_url)
        elif any(kw in link_url.lower() for kw in
                 ["/program","/degree","/major","/catalog/","/academics/",
                  "/graduate/","/undergraduate/"]):
            sub_urls.append(link_url)

    # Structured elements (headings, list items, table cells)
    for tag in soup.find_all(["h2","h3","h4","li","dt","td"]):
        text = tag.get_text(separator=" ", strip=True)
        a    = tag.find("a")
        link_url = resolve_url(a["href"], page_url, base_domain) if a and a.get("href") else None
        if looks_like_program(text):
            add_program(text, link_url or page_url)

    return programs, list(set(sub_urls))


async def fetch_page(session: aiohttp.ClientSession, url: str,
                     semaphore: asyncio.Semaphore) -> tuple[str, str | None]:
    """Fetch one page. Returns (url, html_or_None)."""
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10),
                                   allow_redirects=True, ssl=False) as resp:
                if resp.status == 200:
                    html = await resp.text(errors="replace")
                    return url, html
                return url, None
        except Exception:
            return url, None


async def scrape_university_async(
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        uid: int, name: str, school_url: str
) -> list[dict]:
    """
    Async scrape one university.
    Returns list of program dicts with name, category, degree_level, program_url etc.
    """
    if not school_url:
        return []

    base_domain = re.sub(r"^https?://(www\.)?", "", school_url).rstrip("/")
    if not base_domain:
        return []

    visited       = set()
    all_programs  = {}   # name_lower → {name, url}
    pages_fetched = 0
    MAX_PAGES     = 12   # cap per university

    async def try_fetch(url):
        nonlocal pages_fetched
        if url in visited or pages_fetched >= MAX_PAGES:
            return None
        visited.add(url)
        pages_fetched += 1
        _, html = await fetch_page(session, url, semaphore)
        return html

    def merge_programs(new_progs):
        for p in new_progs:
            key = p["name"].lower()
            if key not in all_programs:
                all_programs[key] = p

    # Build candidate URLs (catalog subdomains + paths)
    candidate_urls = []
    for sub in CATALOG_SUBDOMAINS:
        candidate_urls.append(f"https://{sub}.{base_domain}")
    for path in CATALOG_PATHS:
        candidate_urls.append(f"https://www.{base_domain}{path}")
        candidate_urls.append(f"https://{base_domain}{path}")

    # Deduplicate while preserving order
    seen_cands = set()
    unique_cands = []
    for u in candidate_urls:
        if u not in seen_cands:
            seen_cands.add(u)
            unique_cands.append(u)

    # Fetch first batch in parallel (up to 6 at once per university)
    first_batch = unique_cands[:8]
    tasks       = [try_fetch(u) for u in first_batch]
    results     = await asyncio.gather(*tasks)

    sub_urls_to_crawl = []
    for url, html in zip(first_batch, results):
        if html:
            progs, subs = extract_programs(html, url, base_domain)
            merge_programs(progs)
            sub_urls_to_crawl.extend(subs)

    # If we found a good catalog page (many programs), don't bother with sub-crawl
    if len(all_programs) < 15 and sub_urls_to_crawl:
        # Crawl up to 4 more sub-pages
        extra = [u for u in sub_urls_to_crawl if u not in visited][:4]
        extra_tasks   = [try_fetch(u) for u in extra]
        extra_results = await asyncio.gather(*extra_tasks)
        for url, html in zip(extra, extra_results):
            if html:
                progs, _ = extract_programs(html, url, base_domain)
                merge_programs(progs)

    # Convert to full program objects
    programs = []
    seen_names = set()
    for key, p in all_programs.items():
        name = p["name"]
        if name.lower() in seen_names:
            continue
        seen_names.add(name.lower())
        programs.append({
            "name":          name,
            "category":      classify_category(name),
            "degree_level":  classify_degree(name),
            "description":   None,
            "program_url":   p.get("url"),
            "is_featured":   False,
            "top20_rank":    None,
            "reputation_note": None,
        })

    # Apply known featured/rankings
    data = find_known_data(name)
    if not data:
        data = find_known_data(name)  # try university name

    known = find_known_data(name)
    if known:
        feat_name = known["featured_name"]
        rep_note  = known["reputation_note"]
        prog_url  = known.get("program_url")
        top20     = known.get("top20", [feat_name])

        found = False
        for p in programs:
            if p["name"].lower() == feat_name.lower():
                p["is_featured"] = True
                p["reputation_note"] = rep_note
                if prog_url and not p["program_url"]:
                    p["program_url"] = prog_url
                found = True
                break
        if not found and feat_name:
            programs.insert(0, {
                "name": feat_name, "category": classify_category(feat_name),
                "degree_level": classify_degree(feat_name), "description": None,
                "program_url": prog_url, "is_featured": True, "top20_rank": 1,
                "reputation_note": rep_note,
            })
        for rank, pname in enumerate(top20, 1):
            for p in programs:
                if p["name"].lower() == pname.lower():
                    p["top20_rank"] = rank
                    break
    elif programs:
        programs[0]["is_featured"] = True

    return programs


# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG,
                            cursor_factory=psycopg2.extras.RealDictCursor)

def get_all_universities(skip_existing: bool = False) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            if skip_existing:
                cur.execute("""
                    SELECT u.id, u.name, u.school_url
                    FROM universities u
                    WHERE NOT EXISTS (
                        SELECT 1 FROM university_programs p
                        WHERE p.university_id = u.id
                    )
                    ORDER BY u.name
                """)
            else:
                cur.execute("""
                    SELECT u.id, u.name, u.school_url,
                           COUNT(p.id) AS program_count
                    FROM universities u
                    LEFT JOIN university_programs p ON p.university_id = u.id
                    GROUP BY u.id, u.name, u.school_url
                    ORDER BY u.name
                """)
            return [dict(r) for r in cur.fetchall()]

def save_programs_bulk(uid: int, programs: list[dict], reset: bool = False) -> tuple[int, int]:
    if not programs:
        return 0, 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("DELETE FROM university_programs WHERE university_id=%s", (uid,))
            inserted = skipped = 0
            for p in programs:
                cur.execute("""
                    INSERT INTO university_programs
                        (university_id, name, category, degree_level, description,
                         is_featured, top20_rank, reputation_note, program_url, generated_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'scraper')
                    ON CONFLICT DO NOTHING RETURNING id
                """, (uid, p["name"][:200], p["category"][:100], p["degree_level"][:50],
                      p.get("description"), p["is_featured"], p["top20_rank"],
                      p.get("reputation_note"), p.get("program_url")))
                if cur.fetchone():
                    inserted += 1
                else:
                    skipped += 1
            conn.commit()
    return inserted, skipped


# ─────────────────────────────────────────────────────────────────
# MAIN ASYNC BATCH RUNNER
# ─────────────────────────────────────────────────────────────────
async def run_batch(unis: list[dict], workers: int = 15,
                    reset: bool = False, preview: bool = False):
    """Process all universities concurrently."""
    total      = len(unis)
    done       = 0
    total_ins  = 0
    errors     = 0
    zero_count = 0
    start_time = time.time()
    results    = []

    # Semaphore: max concurrent HTTP requests across ALL universities
    http_sem = asyncio.Semaphore(workers * 3)
    # Semaphore: max concurrent universities being processed
    uni_sem  = asyncio.Semaphore(workers)

    connector = aiohttp.TCPConnector(
        limit=workers * 5,
        ssl=False,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(
        headers=HEADERS,
        connector=connector,
        cookie_jar=aiohttp.CookieJar(unsafe=True),
    ) as session:

        async def process_one(u):
            nonlocal done, total_ins, errors, zero_count
            async with uni_sem:
                uid  = u["id"]
                name = u["name"]
                url  = u.get("school_url","")

                try:
                    programs = await scrape_university_async(session, http_sem, uid, name, url)
                    count    = len(programs)

                    if not preview and programs:
                        inserted, _ = save_programs_bulk(uid, programs, reset=reset)
                        total_ins  += inserted
                    else:
                        inserted = count

                    done += 1
                    if count == 0:
                        zero_count += 1

                    elapsed   = time.time() - start_time
                    rate      = done / elapsed * 60 if elapsed > 0 else 0
                    remaining = int((total - done) / (done / elapsed)) if done > 0 else 0
                    rem_str   = f"{remaining//3600}h{(remaining%3600)//60}m" if remaining > 60 else f"{remaining}s"

                    status = "✅" if count > 0 else "⚠️ "
                    print(f"  {status} [{done}/{total}] {name[:50]:<50} "
                          f"{count:>4} programs  "
                          f"ETA: {rem_str}  ({rate:.1f}/min)")

                    results.append({
                        "uid": uid, "name": name, "status": "ok",
                        "scraped": count, "inserted": inserted
                    })

                except Exception as e:
                    done   += 1
                    errors += 1
                    print(f"  ❌ [{done}/{total}] {name[:50]} — {e}")
                    results.append({"uid": uid, "name": name, "status": "error", "error": str(e)})

        # Launch all tasks
        tasks = [process_one(u) for u in unis]
        await asyncio.gather(*tasks)

    return results, total_ins, errors, zero_count


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="UniSearch Fast Async Scraper — handles 6,000+ universities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    ap.add_argument("--all",           action="store_true",  help="Scrape ALL universities")
    ap.add_argument("--id",            type=int,             help="Scrape one university by ID")
    ap.add_argument("--name",          type=str,             help="Scrape by name substring")
    ap.add_argument("--list",          action="store_true",  help="List all universities")
    ap.add_argument("--reset",         action="store_true",  help="Clear existing programs first")
    ap.add_argument("--preview",       action="store_true",  help="Don't save to DB")
    ap.add_argument("--skip-existing", action="store_true",  help="Skip unis with programs already")
    ap.add_argument("--workers",       type=int, default=15, help="Concurrent workers (default 15)")
    ap.add_argument("--clean-db",      action="store_true",  help="Remove generic/bad program names from DB")
    ap.add_argument("--reset-all",     action="store_true",  help="Wipe ALL programs and re-scrape everything")
    args = ap.parse_args()

    # ── Clean DB: remove generic headings scraped by mistake
    if args.clean_db:
        generic_names = [
            "bachelor's degrees", "master's degrees", "associate degrees",
            "doctorate degrees", "doctoral degrees", "undergraduate degrees",
            "graduate degrees", "certificate programs", "degree programs",
            "online programs", "majors, minors, certificates",
            "majors and minors", "programs of study", "areas of study",
            "academic programs", "degrees and programs", "degrees & programs",
            "bachelor's degree programs", "graduate programs", "undergraduate programs",
            "bachelor's degrees and programs", "certificates", "majors",
        ]
        with get_conn() as conn:
            with conn.cursor() as cur:
                deleted = 0
                for name in generic_names:
                    cur.execute("DELETE FROM university_programs WHERE LOWER(name) = %s", (name,))
                    deleted += cur.rowcount
                # Delete anything that is ONLY "X Degrees" or "X Programs" pattern (no specific subject)
                cur.execute("""
                    DELETE FROM university_programs
                    WHERE name ~* '^(bachelor|master|doctor|associate|undergraduate|graduate|doctoral)(''?s?)?(\\s+of)?\\s+(degrees?|programs?|certificates?|majors?|minors?)$'
                """)
                deleted += cur.rowcount
                conn.commit()
        print(f"\n✅ Removed {deleted} generic/bad program names from database")
        print(f"   Run --all to re-scrape those universities with the fixed scraper\n")
        return

    # ── Reset all: wipe everything and re-scrape
    if args.reset_all:
        print("\n⚠️  This will DELETE all programs and re-scrape all universities.")
        confirm = input("   Type yes to confirm: ").strip().lower()
        if confirm != "yes":
            print("Cancelled."); return
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM university_programs")
                conn.commit()
        print("   🗑  All programs deleted. Starting re-scrape...\n")
        unis = get_all_universities()
        asyncio.run(run_batch(unis, workers=args.workers))
        return

    if args.list:
        unis = get_all_universities()
        print(f"\n{'ID':<8} {'Programs':<11} {'KB':<4} {'Name':<55} URL")
        print("─" * 120)
        for u in unis:
            c  = u.get("program_count", 0)
            m  = "✅" if c > 10 else "⚠️ " if c > 0 else "❌"
            kb = "✓" if find_known_data(u["name"]) else " "
            print(f"{u['id']:<8} {m} {str(c):<8} {kb:<4} {u['name']:<55} {u['school_url'] or ''}")
        in_kb = sum(1 for u in unis if find_known_data(u["name"]))
        print(f"\n  Total: {len(unis)}  |  In knowledge base: {in_kb}")
        return

    if args.all:
        unis = get_all_universities(skip_existing=args.skip_existing)
        if not unis:
            print("No universities to scrape.")
            return
        in_kb = sum(1 for u in unis if find_known_data(u["name"]))
        print(f"\n{'='*65}")
        print(f"  🚀 UniSearch Fast Scraper")
        print(f"  Universities to scrape: {len(unis)}")
        print(f"  In knowledge base:      {in_kb} (correct featured programs)")
        print(f"  Workers:                {args.workers} concurrent")
        print(f"  Estimated time:         ~{len(unis) * 4 // args.workers // 60} minutes")
        print(f"  Started:                {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*65}\n")

        results, total_ins, errors, zero = asyncio.run(
            run_batch(unis, workers=args.workers, reset=args.reset, preview=args.preview)
        )

        elapsed = time.time()
        print(f"\n{'='*65}")
        print(f"  ✅ COMPLETE")
        print(f"  Universities processed: {len(unis)}")
        print(f"  Programs inserted:      {total_ins}")
        print(f"  Errors:                 {errors}")
        print(f"  Got 0 programs:         {zero}  (check scrape_log.json)")
        print(f"{'='*65}")

        with open("scrape_log.json","w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  📄 Log saved to scrape_log.json")
        print(f"  For any ❌ universities, use import_programs_csv.py to add manually")
        return

    if args.id:
        unis = get_all_universities()
        match = [u for u in unis if u["id"] == args.id]
        if not match:
            print(f"ERROR: University ID {args.id} not found"); return
        u = match[0]
        print(f"\nScraping: {u['name']}")
        async def _one():
            sem = asyncio.Semaphore(10)
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
                return await scrape_university_async(session, sem, u["id"], u["name"], u.get("school_url",""))
        programs = asyncio.run(_one())
        if args.preview:
            print(f"\n{len(programs)} programs found:\n")
            for p in programs:
                r = f"#{p['top20_rank']}" if p['top20_rank'] else "   "
                f = "⭐" if p['is_featured'] else "  "
                lnk = "🔗" if p['program_url'] else "  "
                print(f"  {f} {r:<5} {lnk} [{p['degree_level']:<14}] {p['name']}")
                if p['program_url']:
                    print(f"                          → {p['program_url']}")
        else:
            ins, skp = save_programs_bulk(u["id"], programs, reset=args.reset)
            print(f"\n✅ Done: {len(programs)} found, {ins} inserted, {skp} already existed")
        return

    if args.name:
        unis = get_all_universities()
        matches = [u for u in unis if args.name.lower() in u["name"].lower()]
        if not matches:
            print(f"No match for '{args.name}'"); return
        if len(matches) > 1:
            print("Multiple matches — use --id:")
            for u in matches: print(f"  {u['id']}: {u['name']}")
            return
        u = matches[0]
        print(f"\nScraping: {u['name']}")
        async def _one():
            sem = asyncio.Semaphore(10)
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
                return await scrape_university_async(session, sem, u["id"], u["name"], u.get("school_url",""))
        programs = asyncio.run(_one())
        if args.preview:
            print(f"\n{len(programs)} programs found:\n")
            for p in programs:
                r = f"#{p['top20_rank']}" if p['top20_rank'] else "   "
                f = "⭐" if p['is_featured'] else "  "
                lnk = "🔗" if p['program_url'] else "  "
                print(f"  {f} {r:<5} {lnk} [{p['degree_level']:<14}] {p['name']}")
        else:
            ins, skp = save_programs_bulk(u["id"], programs, reset=args.reset)
            print(f"\n✅ Done: {len(programs)} found, {ins} inserted, {skp} already existed")
        return

    ap.print_help()


if __name__ == "__main__":
    main()
