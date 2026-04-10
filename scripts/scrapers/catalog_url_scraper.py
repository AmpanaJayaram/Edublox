"""
catalog_url_scraper.py — Scrape programs from real university catalog URLs
=========================================================================
When the CSV catalog URL is a generic homepage, auto-detects the real catalog.

USAGE:
  python catalog_url_scraper.py --id 3087 --preview
  python catalog_url_scraper.py --name "north texas" --preview
  python catalog_url_scraper.py --all
  python catalog_url_scraper.py --all --reset
  python catalog_url_scraper.py --all --skip-done
  python catalog_url_scraper.py --all --min-programs 20
"""

import os, re, json, time, argparse
import requests, psycopg2, psycopg2.extras
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME",     "unisearch"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", "2000"),
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 20

# ── Category & degree helpers ─────────────────────────────────
CATEGORY_MAP = [
    ("Engineering",       ["engineering","mechanical","electrical","civil","chemical",
                           "aerospace","materials","biomedical","industrial","nuclear",
                           "petroleum","computer engineering","mechatronics","robotics"]),
    ("STEM",              ["computer science","cybersecurity","information technology",
                           "data science","mathematics","physics","chemistry","biology",
                           "biochemistry","ecology","geology","statistics","neuroscience",
                           "bioinformatics","astronomy","software","computing","data analytics",
                           "information systems","information science"]),
    ("Health & Medicine", ["nursing","medicine","pharmacy","public health","kinesiology",
                           "athletic training","physical therapy","occupational therapy",
                           "audiology","speech","health science","rehabilitation","nutrition",
                           "dietetics","dental","medical","clinical","health administration",
                           "behavior analysis","addiction","radiologic","respiratory"]),
    ("Business",          ["business","accounting","finance","marketing","management",
                           "economics","entrepreneurship","supply chain","hospitality",
                           "tourism","real estate","insurance","merchandising","retail",
                           "logistics","mba","commerce","banking","actuarial"]),
    ("Education",         ["education","teaching","curriculum","instruction","counseling",
                           "learning","early childhood","special education","educational",
                           "pedagogy","literacy"]),
    ("Law & Policy",      ["law","political science","criminal justice","public administration",
                           "international relations","public policy","emergency management",
                           "nonprofit","urban planning","government","legal","criminology"]),
    ("Arts & Humanities", ["music","art","design","dance","theatre","theater","film",
                           "journalism","media","communication","english","history",
                           "philosophy","religion","linguistics","language","creative writing",
                           "visual","studio art","graphic design","architecture","advertising",
                           "photography","animation","fashion","interior design","spanish",
                           "french","german","japanese"]),
    ("Social Sciences",   ["sociology","psychology","anthropology","social work","geography",
                           "social science","women's studies","gender","ethnic studies",
                           "family studies","behavioral science","international studies"]),
]

def get_category(name):
    n = name.lower()
    for cat, kws in CATEGORY_MAP:
        if any(kw in n for kw in kws):
            return cat
    return "STEM"

def get_degree_level(name):
    n = name.lower()
    if re.search(r'\bph\.?d\.?\b|\bdoctor\b|\bd\.m\.a\b|\bed\.d\b|\bj\.d\b|\bm\.d\b|\bpharm\.d\b|\bdpt\b|\bdnp\b', n):
        return "Doctoral"
    if re.search(r'\bmaster\b|\bm\.s\b|\bm\.a\b|\bmba\b|\bm\.ed\b|\bmfa\b|\bm\.arch\b|\bm\.mus\b|\bm\.p\.h\b|\bm\.p\.a\b', n):
        return "Graduate"
    if re.search(r'\bcertificate\b', n):
        return "Certificate"
    if re.search(r'\bminor\b', n):
        return "Minor"
    return "Undergraduate"

def clean_name(raw):
    name = re.sub(
        r',\s*(B[A-Z]{1,5}S?|M[A-Z]{1,5}|Ph\.?D\.?|Ed\.?D\.?|J\.D\.?|M\.D\.?|'
        r'D\.M\.A\.?|D\.N\.P\.?|D\.P\.T\.?|BAAS|BSW|BSET|BSBC|BSBIO|'
        r'BSCHM|BSMTH|BSMLS|BAS|BSECO|BSPHY)\s*$',
        '', raw, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s*\(not currently accepting students\)', '', name, flags=re.I).strip()
    return re.sub(r'\s+', ' ', name).strip()

SKIP_PHRASES = [
    'degree requirements', 'general university requirements', 'university core',
    'honors courses', 'catalog home', 'course descriptions', 'academic calendar',
    'financial information', 'contacts', 'archived catalog', 'print-friendly',
    'accrediting', 'campus maps', 'administration, faculty', 'request information',
    'schedule a tour', 'apply now', 'get started', 'learn more', 'click here',
]

# Generic headings that are NOT real programs
_GENERIC_ENDINGS = {
    "degrees", "programs", "certificates", "majors", "minors",
    "degrees and programs", "undergraduate programs", "graduate programs",
    "doctoral programs", "areas of study", "fields of study",
    "academic programs", "course catalog", "programs and degrees",
    "undergraduate degrees", "graduate degrees", "doctoral degrees",
    "bachelor's degrees", "master's degrees", "associate degrees",
    "certificate programs", "online programs", "degree programs",
}

_REAL_DEGREE_RE = re.compile(
    r"""^.{8,}\s+(in|of)\s+.{4,}$  # must have "in X" or "of X" after degree word
    |bachelor\s+of\s+\w             # Bachelor of X
    |master\s+of\s+\w               # Master of X  
    |doctor\s+of\s+\w               # Doctor of X
    |b\.[saf]\.\s+in\s+\w           # B.S. in X
    |m\.[saf]\.\s+in\s+\w           # M.S. in X
    |ph\.?d\.?\s+in\s+\w            # Ph.D. in X
    |associate\s+of\s+\w            # Associate of X
    |certificate\s+in\s+\w          # Certificate in X
    |juris\s+doctor                 # Juris Doctor
    |m\.b\.a\.?$                    # MBA
    |mba                        # MBA
    |pharm\.d                       # Pharm.D.
    """,
    re.IGNORECASE | re.VERBOSE
)

def is_valid_program(name, degree_level):
    if not name or len(name) < 8 or len(name) > 220:
        return False
    if degree_level == "Minor":
        return False
    n = name.lower().strip()
    # Reject pure generic headings
    if n in _GENERIC_ENDINGS:
        return False
    if any(n.endswith(e) for e in _GENERIC_ENDINGS):
        return False
    if any(p in n for p in SKIP_PHRASES):
        return False
    # Must match a real degree+subject pattern
    return bool(_REAL_DEGREE_RE.search(name))

# ── HTTP ──────────────────────────────────────────────────────
def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and 'text/html' in r.headers.get('content-type',''):
            return r.url, BeautifulSoup(r.text, 'html.parser'), r.text
    except Exception:
        pass
    return None, None, ""

# ── Acalog detection & scraping ───────────────────────────────
def is_acalog(html):
    return any(m in html for m in ['preview_program.php','catoid=','Modern Campus Catalog','Acalog ACMS'])

def extract_acalog_programs(soup, base_url):
    programs, seen = [], set()
    for a in soup.find_all('a', href=re.compile(r'preview_program\.php')):
        raw = a.get_text(strip=True)
        if not raw:
            continue
        level = get_degree_level(raw)
        name  = clean_name(raw)
        if not is_valid_program(name, level):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        programs.append({
            "name": name, "degree_level": level,
            "category": get_category(name),
            "program_url": urljoin(base_url, a['href']),
            "is_featured": False, "top20_rank": None, "reputation_note": None,
        })
    return programs

def find_acalog_programs_page(soup, base_url):
    """Find the 'programs by academic unit' page link."""
    patterns = [re.compile(p, re.I) for p in [
        r"programs.*academic.unit", r"degree.*programs", r"majors.*programs",
        r"programs.*majors", r"all.*programs", r"programs.*study",
        r"academic.*programs", r"programs.*offered",
    ]]
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        for pat in patterns:
            if pat.search(text):
                return urljoin(base_url, a['href'])
    # Try any navoid link mentioning programs
    for a in soup.find_all('a', href=re.compile(r'navoid=\d+')):
        text = a.get_text(strip=True).lower()
        if any(kw in text for kw in ['program','major','degree','academic unit']):
            return urljoin(base_url, a['href'])
    return None

# ── Auto-detect real catalog URL ──────────────────────────────
def find_real_catalog(school_url, given_url):
    """Try multiple strategies to find the real catalog URL."""
    raw_domain = re.sub(r'^https?://(www\.)?', '', school_url or given_url or '')
    domain = raw_domain.split('/')[0].rstrip('/').lower()
    base_domain = re.sub(r'^www\.', '', domain)

    candidates = []

    # 1. catalog./bulletin. subdomains FIRST — most reliable
    if base_domain:
        candidates += [
            f"https://catalog.{base_domain}",
            f"https://bulletin.{base_domain}",
            f"https://catalog.{base_domain}/index.php",
            f"https://catalogs.{base_domain}",
        ]

    # 2. SmartCatalogIQ
    if base_domain:
        candidates.append(f"https://{base_domain}.smartcatalogiq.com")

    # 3. Given URL only if it looks like a real catalog (not a homepage)
    if given_url and given_url.startswith('http'):
        url_lower = given_url.lower()
        looks_like_catalog = any(ind in url_lower for ind in [
            'catalog', 'bulletin', 'smartcatalog', '/programs',
            '/degree', '/major', '/courses', '/curriculum',
            'catoid=', 'navoid=',
        ])
        if looks_like_catalog:
            candidates.insert(0, given_url)

    # 4. Common catalog paths on school domain
    if domain:
        base = f"https://www.{domain}"
        candidates += [
            base + "/catalog",
            base + "/academics/programs",
            base + "/academics/degrees-and-programs",
            base + "/programs",
            base + "/degrees",
            base + "/majors",
            base + "/programs-of-study",
        ]

    seen_urls = set()
    for url in candidates:
        if url in seen_urls:
            continue
        seen_urls.add(url)

        final_url, soup, html = fetch(url)
        if not soup:
            continue

        # Is it Acalog?
        if is_acalog(html):
            return final_url, soup, html

        # Is it SmartCatalogIQ?
        if 'smartcatalogiq' in (final_url or '').lower():
            return final_url, soup, html

        # Does it have program-like links with real degree names?
        prog_links = sum(1 for a in soup.find_all('a')
                        if re.search(
                            r'bachelor.{1,30}(in|of)|master.{1,30}(in|of)|doctor.{1,30}(in|of)|'
                            r'associate.{1,30}(in|of)|certificate in',
                            a.get_text(), re.I))
        if prog_links >= 5:
            return final_url, soup, html

    return None, None, ""


def scrape_university(uni_name, school_url, catalog_url):
    """Scrape programs for one university."""

    # Step 1: try the given catalog_url
    if catalog_url and catalog_url.endswith('.pdf'):
        # Can't scrape PDFs — skip to auto-detect
        catalog_url = None

    # Step 2: find the real catalog
    real_url, soup, html = find_real_catalog(school_url, catalog_url)
    if not soup:
        return []

    programs = []

    # ── Acalog / Modern Campus ──
    if is_acalog(html):
        # Check if we're already on a programs listing page
        progs = extract_acalog_programs(soup, real_url)
        if progs:
            return apply_knowledge(progs, uni_name)

        # Find the programs page
        prog_page = find_acalog_programs_page(soup, real_url)
        if prog_page:
            _, prog_soup, _ = fetch(prog_page)
            if prog_soup:
                progs = extract_acalog_programs(prog_soup, prog_page)
                if progs:
                    return apply_knowledge(progs, uni_name)

        # Last resort: try fetching catalog root
        root = '/'.join(real_url.rstrip('/').split('/')[:3])
        if root != real_url:
            _, root_soup, root_html = fetch(root)
            if root_soup and is_acalog(root_html):
                prog_page = find_acalog_programs_page(root_soup, root)
                if prog_page:
                    _, prog_soup, _ = fetch(prog_page)
                    if prog_soup:
                        progs = extract_acalog_programs(prog_soup, prog_page)
                        if progs:
                            return apply_knowledge(progs, uni_name)

    # ── SmartCatalogIQ ──
    if 'smartcatalogiq' in (real_url or '').lower():
        programs = scrape_smartcatalog(soup, real_url)
        if programs:
            return apply_knowledge(programs, uni_name)

    # ── Generic HTML ──
    programs = scrape_generic(soup, real_url)
    return apply_knowledge(programs, uni_name)

def scrape_smartcatalog(soup, base_url):
    programs, seen = [], set()
    DEGREE_PAT = re.compile(
        r'\b(bachelor|master|doctor|associate|certificate in|b\.s|b\.a|m\.s|m\.a|mba|ph\.d)\b', re.I)
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        if not text or len(text) < 8 or len(text) > 200:
            continue
        if not DEGREE_PAT.search(text):
            continue
        level = get_degree_level(text)
        name  = clean_name(text)
        if not is_valid_program(name, level):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        programs.append({
            "name": name, "degree_level": level,
            "category": get_category(name),
            "program_url": urljoin(base_url, a['href']),
            "is_featured": False, "top20_rank": None, "reputation_note": None,
        })
    return programs

DEGREE_PAT = re.compile(
    r'\b(bachelor|master|doctor|associate|certificate in|b\.s\.|b\.a\.|'
    r'm\.s\.|m\.a\.|m\.b\.a|mba|b\.f\.a|m\.f\.a|ph\.d|phd|juris|pharm\.d)\b', re.I)

def scrape_generic(soup, base_url):
    programs, seen = [], set()

    # Strategy 1: links with degree keywords
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 200:
            continue
        if not DEGREE_PAT.search(text):
            continue
        level = get_degree_level(text)
        name  = clean_name(text)
        if not is_valid_program(name, level):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        href = a['href']
        prog_url = urljoin(base_url, href) if not href.startswith('http') else href
        programs.append({
            "name": name, "degree_level": level,
            "category": get_category(name),
            "program_url": prog_url,
            "is_featured": False, "top20_rank": None, "reputation_note": None,
        })

    # Strategy 2: headings/list items (if few links found)
    if len(programs) < 5:
        for tag in soup.find_all(['h2','h3','h4','li','td']):
            text = tag.get_text(strip=True)
            if not text or len(text) < 10 or len(text) > 200:
                continue
            if not DEGREE_PAT.search(text):
                continue
            level = get_degree_level(text)
            name  = clean_name(text)
            if not is_valid_program(name, level):
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            parent_a = tag.find('a', href=True)
            prog_url = urljoin(base_url, parent_a['href']) if parent_a else base_url
            programs.append({
                "name": name, "degree_level": level,
                "category": get_category(name),
                "program_url": prog_url,
                "is_featured": False, "top20_rank": None, "reputation_note": None,
            })

    return programs

# ── Knowledge base ────────────────────────────────────────────
def apply_knowledge(programs, uni_name):
    try:
        from university_knowledge import KNOWN_FEATURED
    except ImportError:
        KNOWN_FEATURED = {}
    uni_lower = uni_name.lower()
    for key, data in KNOWN_FEATURED.items():
        if key not in uni_lower:
            continue
        feat  = data["featured_name"]
        rep   = data.get("reputation_note","")
        purl  = data.get("program_url")
        top20 = data.get("top20", [feat])
        found = False
        for p in programs:
            if p["name"].lower() == feat.lower():
                p.update({"is_featured": True, "reputation_note": rep,
                          "program_url": purl or p.get("program_url")})
                found = True
                break
        if not found and programs:
            programs.insert(0, {
                "name": feat, "degree_level": "Undergraduate",
                "category": get_category(feat), "program_url": purl,
                "is_featured": True, "top20_rank": 1, "reputation_note": rep,
            })
        for rank, pname in enumerate(top20, 1):
            for p in programs:
                if p["name"].lower() == pname.lower():
                    p["top20_rank"] = rank
                    break
        break
    if programs and not any(p["is_featured"] for p in programs):
        programs[0]["is_featured"] = True
    return programs

# ── Database ──────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)

def get_universities(skip_done=False, min_programs=0):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.name, u.school_url,
                       COALESCE(u.catalog_url, u.school_url) AS catalog_url,
                       COUNT(p.id) AS prog_count
                FROM universities u
                LEFT JOIN university_programs p ON p.university_id = u.id
                GROUP BY u.id, u.name, u.school_url, u.catalog_url
                ORDER BY u.name
            """)
            rows = [dict(r) for r in cur.fetchall()]

    if min_programs > 0:
        rows = [r for r in rows if r['prog_count'] < min_programs]
    if skip_done:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT university_id FROM university_programs
                    WHERE generated_by = 'catalog'
                """)
                done_ids = {r['university_id'] for r in cur.fetchall()}
        rows = [r for r in rows if r['id'] not in done_ids]
    return rows

def save_programs(uid, programs, reset=False):
    if not programs:
        return 0, 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("DELETE FROM university_programs WHERE university_id=%s", (uid,))
            ins = skp = 0
            for p in programs:
                cur.execute("""
                    INSERT INTO university_programs
                        (university_id,name,category,degree_level,description,
                         is_featured,top20_rank,reputation_note,program_url,generated_by)
                    VALUES (%s,%s,%s,%s,NULL,%s,%s,%s,%s,'catalog')
                    ON CONFLICT DO NOTHING RETURNING id
                """, (uid, p["name"][:200], p["category"][:100],
                      p["degree_level"][:50], p["is_featured"],
                      p.get("top20_rank"), p.get("reputation_note"),
                      p.get("program_url")))
                if cur.fetchone(): ins += 1
                else: skp += 1
            conn.commit()
    return ins, skp

# ── CLI ───────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Scrape real programs from university catalog websites",
        epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all",           action="store_true")
    ap.add_argument("--id",            type=int)
    ap.add_argument("--name",          type=str)
    ap.add_argument("--reset",         action="store_true")
    ap.add_argument("--preview",       action="store_true")
    ap.add_argument("--skip-done",     action="store_true")
    ap.add_argument("--min-programs",  type=int, default=0)
    ap.add_argument("--delay",         type=float, default=1.5)
    args = ap.parse_args()

    if not any([args.all, args.id, args.name]):
        ap.print_help(); return

    def run_one(u, preview=False):
        programs = scrape_university(
            u["name"],
            u.get("school_url","") or "",
            u.get("catalog_url","") or ""
        )
        if preview:
            print(f"\n  {u['name']}")
            print(f"  Catalog URL: {u.get('catalog_url','none')}")
            print(f"  Programs found: {len(programs)}")
            if programs:
                by_level = {}
                for p in programs:
                    by_level.setdefault(p['degree_level'], []).append(p)
                for level in ["Undergraduate","Graduate","Doctoral","Certificate"]:
                    grp = by_level.get(level, [])
                    if not grp: continue
                    print(f"\n  {level} ({len(grp)}):")
                    for p in sorted(grp, key=lambda x: x['name'])[:20]:
                        f = "⭐" if p["is_featured"] else "  "
                        print(f"    {f} {p['name']}")
                    if len(grp) > 20:
                        print(f"       ... and {len(grp)-20} more")
            return len(programs), 0

        ins, skp = save_programs(u["id"], programs, reset=args.reset)
        return len(programs), ins

    # Single university
    if args.id or args.name:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if args.id:
                    cur.execute("""
                        SELECT id, name, school_url,
                               COALESCE(catalog_url, school_url) AS catalog_url
                        FROM universities WHERE id=%s
                    """, (args.id,))
                else:
                    cur.execute("""
                        SELECT id, name, school_url,
                               COALESCE(catalog_url, school_url) AS catalog_url
                        FROM universities WHERE name ILIKE %s
                    """, (f'%{args.name}%',))
                matches = [dict(r) for r in cur.fetchall()]

        if not matches:
            print("No university found."); return
        if len(matches) > 1 and not args.id:
            print("Multiple matches — use --id:")
            for u in matches: print(f"  {u['id']}: {u['name']}")
            return

        u = matches[0]
        cnt, ins = run_one(u, preview=args.preview)
        if not args.preview:
            print(f"\n✅ {u['name']}: {cnt} programs scraped, {ins} saved")
        return

    # All universities
    unis  = get_universities(skip_done=args.skip_done,
                              min_programs=args.min_programs)
    total = len(unis)
    done  = good = failed = ins_total = 0

    print(f"\n{'='*65}")
    print(f"  🌐 Catalog URL Scraper — Real Programs from Official Catalogs")
    print(f"  Universities: {total:,}  |  Delay: {args.delay}s")
    print(f"{'='*65}\n")

    log = []
    for u in unis:
        done += 1
        cnt, ins = run_one(u)
        ins_total += ins

        if cnt >= 10:   good  += 1; icon = "✅"
        elif cnt > 0:              icon = "⚠️ "
        else:           failed += 1; icon = "❌"

        print(f"  {icon} [{done}/{total}] {u['name'][:55]:<55} {cnt:>4} programs")
        log.append({"id":u["id"],"name":u["name"],
                    "catalog_url":u.get("catalog_url",""),"programs":cnt,"inserted":ins})
        time.sleep(args.delay)

    print(f"\n{'='*65}")
    print(f"  ✅ COMPLETE  |  Good: {good}  |  Partial: {done-good-failed}  |  Failed: {failed}")
    print(f"  Programs inserted: {ins_total:,}")
    print(f"{'='*65}")
    with open("catalog_scrape_log.json","w") as f:
        json.dump(log, f, indent=2)
    print(f"  Log: catalog_scrape_log.json\n")

if __name__ == "__main__":
    main()
