"""
scrape_from_csv.py — Scrape real programs from university catalog URLs
=======================================================================
Reads catalog URLs from your database (imported from CSV).
Visits each URL directly and extracts real programs.
No AI, no guessing — only what's on the actual catalog page.

USAGE:
  python scrape_from_csv.py --id 3087 --preview
  python scrape_from_csv.py --name "north texas" --preview
  python scrape_from_csv.py --all
  python scrape_from_csv.py --all --reset
  python scrape_from_csv.py --all --skip-done
  python scrape_from_csv.py --from-csv Uni_data_filled_universities_summary_.csv
"""

import os, re, csv, json, time, argparse
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
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 25

# ── Category detection ─────────────────────────────────────────
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
                           "behavior analysis","addiction","radiologic","veterinary"]),
    ("Business",          ["business","accounting","finance","marketing","management",
                           "economics","entrepreneurship","supply chain","hospitality",
                           "tourism","real estate","insurance","merchandising","retail",
                           "logistics","mba","commerce","banking","actuarial"]),
    ("Education",         ["education","teaching","curriculum","instruction","counseling",
                           "learning","early childhood","special education","educational",
                           "pedagogy","literacy","teacher"]),
    ("Law & Policy",      ["law","political science","criminal justice","public administration",
                           "international relations","public policy","emergency management",
                           "nonprofit","urban planning","government","legal","criminology"]),
    ("Arts & Humanities", ["music","art","design","dance","theatre","theater","film",
                           "journalism","media","communication","english","history",
                           "philosophy","religion","linguistics","language","creative writing",
                           "visual","studio art","graphic design","architecture","advertising",
                           "photography","animation","fashion","interior design","spanish",
                           "french","german","japanese","chinese"]),
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

def get_degree_level(raw):
    """Detect degree level from raw program name including abbreviations."""
    n = raw.lower()
    # Doctoral — check abbreviations like PhD, EdD, DMA, JD, MD, PharmD
    if re.search(r'\bph\.?d\.?\b|\bdoctor\b|\bd\.m\.a\.?\b|\bed\.d\.?\b|\bj\.d\.?\b|\bm\.d\.?\b|\bpharm\.d\.?\b|\bdpt\b|\bdnp\b', n):
        return "Doctoral"
    # Grad track options leading to PhD are doctoral
    if 'grad track' in n and ('phd' in n or 'ph.d' in n or ', phd' in n):
        return "Doctoral"
    # Graduate — check abbreviations MS, MA, MBA, MEd, MFA, MArch etc.
    if re.search(r'\bmaster\b', n):
        return "Graduate"
    if re.search(r',\s*(m\.s\.?|m\.a\.?|mba|m\.ed\.?|mfa|m\.arch\.?|m\.mus\.?|m\.p\.h\.?|m\.p\.a\.?|m\.eng\.?|m\.f\.a\.?|ms|ma|msw)\s*$', n):
        return "Graduate"
    if 'grad track' in n:
        return "Graduate"
    # Certificate
    if re.search(r'\bcertificate\b|\bcertification\b', n):
        return "Certificate"
    # Minor
    if re.search(r'\bminor\b', n):
        return "Minor"
    # Associate
    if re.search(r'\bassociate\b|,\s*a\.s\.?\b|,\s*a\.a\.?\b', n):
        return "Undergraduate"
    return "Undergraduate"

def clean_name(raw):
    """Clean program name for display - keep concentration info, remove degree codes."""
    # Remove trailing degree abbreviation after last comma
    name = re.sub(
        r',\s*(B[A-Z]{1,5}S?|M[A-Z]{1,5}|Ph\.?D\.?|Ed\.?D\.?|J\.D\.?|M\.D\.?|'
        r'D\.M\.A\.?|D\.N\.P\.?|D\.P\.T\.?|BAAS|BSW|BSET|BSBC|BSBIO|'
        r'BSCHM|BSMTH|BSMLS|BAS|BSECO|BSPHY|BSEET)\s*$',
        '', raw, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s*\(not currently accepting students\)\s*', '', name, flags=re.I)
    name = re.sub(r'\s*\(dual degree[^)]*\)\s*', '', name, flags=re.I)
    return re.sub(r'\s+', ' ', name).strip()

# ── HTTP ──────────────────────────────────────────────────────
def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and 'text/html' in r.headers.get('content-type', ''):
            return r.url, BeautifulSoup(r.text, 'html.parser'), r.text
    except Exception:
        pass
    return None, None, ""

# ── Acalog helpers ────────────────────────────────────────────
def is_acalog(html):
    return any(m in html for m in ['preview_program.php', 'catoid=', 'Modern Campus Catalog', 'Acalog ACMS'])

def extract_acalog_programs(soup, base_url):
    """Extract all program links from an Acalog page."""
    SKIP_ENDSWITH = {'degree requirements', 'general university requirements',
                     'university core curriculum'}
    SKIP_CONTAINS = ['honors courses that meet university core']

    programs = []
    seen = set()  # use raw name as key to preserve all variants

    for a in soup.find_all('a', href=re.compile(r'preview_program\.php')):
        raw = a.get_text(strip=True)
        if not raw or len(raw) < 4:
            continue

        raw_lower = raw.lower().strip()
        if raw_lower in SKIP_ENDSWITH:
            continue
        if any(raw_lower.endswith(s) for s in SKIP_ENDSWITH):
            continue
        if any(s in raw_lower for s in SKIP_CONTAINS):
            continue

        # Use raw as dedup key so "Accounting, BBA" and "Accounting, BS" are both kept
        if raw_lower in seen:
            continue
        seen.add(raw_lower)

        level = get_degree_level(raw)
        if level == "Minor":
            continue

        name = clean_name(raw)
        if not name or len(name) < 4:
            continue

        programs.append({
            "name":          name,
            "degree_level":  level,
            "category":      get_category(name),
            "program_url":   urljoin(base_url, a['href']),
            "is_featured":   False,
            "top20_rank":    None,
            "reputation_note": None,
        })
    return programs

def scrape_acalog_catalog(catalog_root, soup, html):
    """
    Scrape ALL programs from an Acalog catalog, including any separate
    graduate/doctoral catalogs found in the dropdown selector.
    Strategy: try every navoid link, pick the page with the most programs.
    """
    all_programs = []
    seen_names = set()

    def scrape_one_catalog(root_url, root_soup):
        """Find the best navoid page and extract programs from it."""
        # Collect all navoid links
        navoid_links = set()
        for a in root_soup.find_all('a', href=True):
            if 'navoid=' in a['href']:
                navoid_links.add(urljoin(root_url, a['href']))

        if not navoid_links:
            return extract_acalog_programs(root_soup, root_url)

        # Try every navoid page — pick the one with most programs
        best_progs = []
        for nav_url in navoid_links:
            _, try_soup, _ = fetch(nav_url)
            if not try_soup:
                continue
            progs = extract_acalog_programs(try_soup, nav_url)
            if len(progs) > len(best_progs):
                best_progs = progs
        return best_progs

    # Step 1: Scrape the main catalog
    main_progs = scrape_one_catalog(catalog_root, soup)
    for p in main_progs:
        key = p['name'].lower()
        if key not in seen_names:
            all_programs.append(p)
            seen_names.add(key)

    # Step 2: Find and scrape any OTHER catalogs (graduate, doctoral) from the dropdown
    for opt in soup.find_all('option'):
        val  = opt.get('value', '')
        text = opt.get_text(strip=True).lower()
        if not val:
            continue
        # Only process graduate/doctoral catalogs we haven't scraped
        if not any(kw in text for kw in ['graduate', 'doctoral', 'grad']):
            continue
        # Build URL for this catalog
        if val.isdigit():
            parsed = urlparse(catalog_root)
            other_url = f"{parsed.scheme}://{parsed.netloc}/index.php?catoid={val}"
        elif 'catoid=' in val:
            parsed = urlparse(catalog_root)
            other_url = f"{parsed.scheme}://{parsed.netloc}/{val.lstrip('/')}"
        else:
            continue

        # Skip if same as current
        cur_catoid = re.search(r'catoid=(\d+)', catalog_root)
        opt_catoid = re.search(r'catoid=(\d+)', other_url) or (val.isdigit() and type('',(),{'group':lambda s,x:val})())
        if cur_catoid and val.isdigit() and cur_catoid.group(1) == val:
            continue

        _, other_soup, other_html = fetch(other_url)
        if not other_soup or not is_acalog(other_html):
            continue

        other_progs = scrape_one_catalog(other_url, other_soup)
        added = 0
        for p in other_progs:
            key = p['name'].lower()
            if key not in seen_names:
                all_programs.append(p)
                seen_names.add(key)
                added += 1

    return all_programs

# ── SmartCatalogIQ ────────────────────────────────────────────
def scrape_smartcatalog(soup, base_url):
    programs, seen = [], set()
    DEGREE_PAT = re.compile(
        r'\b(bachelor|master|doctor|associate|certificate in|b\.s|b\.a|m\.s|m\.a|mba|ph\.d|juris|pharm\.d)\b', re.I)
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        if not text or len(text) < 8 or len(text) > 200:
            continue
        if not DEGREE_PAT.search(text):
            continue
        level = get_degree_level(text)
        if level == "Minor":
            continue
        name = clean_name(text)
        key = name.lower()
        if key in seen or len(name) < 8:
            continue
        seen.add(key)
        programs.append({
            "name": name, "degree_level": level, "category": get_category(name),
            "program_url": urljoin(base_url, a['href']),
            "is_featured": False, "top20_rank": None, "reputation_note": None,
        })
    return programs

# ── Generic HTML ──────────────────────────────────────────────
DEGREE_PAT_GEN = re.compile(
    r'\b(bachelor|master|doctor|associate|certificate in|b\.s\.|b\.a\.|m\.s\.|m\.a\.|'
    r'm\.b\.a|mba|b\.f\.a|m\.f\.a|ph\.d|phd|juris doctor|pharm\.d|d\.p\.t|d\.n\.p)\b', re.I)

def scrape_generic(soup, base_url):
    programs, seen = [], set()
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 200:
            continue
        if not DEGREE_PAT_GEN.search(text):
            continue
        level = get_degree_level(text)
        if level == "Minor":
            continue
        name = clean_name(text)
        key = name.lower()
        if key in seen or len(name) < 8:
            continue
        seen.add(key)
        href = a['href']
        prog_url = urljoin(base_url, href) if not href.startswith('http') else href
        programs.append({
            "name": name, "degree_level": level, "category": get_category(name),
            "program_url": prog_url,
            "is_featured": False, "top20_rank": None, "reputation_note": None,
        })
    if len(programs) < 5:
        for tag in soup.find_all(['li', 'td', 'h3', 'h4']):
            text = tag.get_text(strip=True)
            if not text or len(text) < 10 or len(text) > 200:
                continue
            if not DEGREE_PAT_GEN.search(text):
                continue
            level = get_degree_level(text)
            if level == "Minor":
                continue
            name = clean_name(text)
            key = name.lower()
            if key in seen or len(name) < 8:
                continue
            seen.add(key)
            parent_a = tag.find('a', href=True)
            prog_url = urljoin(base_url, parent_a['href']) if parent_a else base_url
            programs.append({
                "name": name, "degree_level": level, "category": get_category(name),
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
        rep   = data.get("reputation_note", "")
        purl  = data.get("program_url")
        top20 = data.get("top20", [feat])
        found = False
        for p in programs:
            if p["name"].lower() == feat.lower():
                p.update({"is_featured": True, "reputation_note": rep,
                           "program_url": purl or p.get("program_url")})
                found = True
                break
        if not found:
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

# ── Main dispatch ─────────────────────────────────────────────
def scrape_catalog_url(catalog_url, uni_name, school_url=""):
    if not catalog_url or catalog_url.lower().endswith('.pdf'):
        return []

    # Extract base domain from school_url or catalog_url
    raw = school_url or catalog_url or ""
    domain = re.sub(r'^https?://(www\.)?', '', raw)
    domain = re.sub(r'^www\.', '', domain.split('/')[0].rstrip('/').lower())

    # ALWAYS try catalog.domain and bulletin.domain FIRST
    # These are the most reliable sources regardless of what the CSV says
    final_url = soup = html = None
    for try_url in [f"https://catalog.{domain}", f"https://bulletin.{domain}"]:
        tu, ts, th = fetch(try_url)
        if ts and len(th) > 3000:
            final_url, soup, html = tu, ts, th
            break

    # If catalog subdomain not found, try the given CSV catalog URL
    if not soup and catalog_url and catalog_url.startswith('http'):
        final_url, soup, html = fetch(catalog_url)

    # Last resort: school homepage
    if not soup and school_url and school_url.startswith('http'):
        final_url, soup, html = fetch(school_url)

    if not soup:
        return []

    # 1. Acalog / Modern Campus
    if is_acalog(html):
        parsed  = urlparse(final_url)
        root    = f"{parsed.scheme}://{parsed.netloc}"
        # Fetch catalog root if we're on a subpage
        if final_url.rstrip('/') != root.rstrip('/'):
            ru, rs, rh = fetch(root)
            if rs and is_acalog(rh):
                soup, html, final_url = rs, rh, ru
        progs = scrape_acalog_catalog(final_url, soup, html)
        if progs:
            return apply_knowledge(progs, uni_name)

    # 2. SmartCatalogIQ
    if 'smartcatalogiq' in (final_url or '').lower() or 'smartcatalogiq' in html.lower():
        progs = scrape_smartcatalog(soup, final_url)
        if progs:
            return apply_knowledge(progs, uni_name)

    # 3. Generic HTML
    progs = scrape_generic(soup, final_url)
    if progs:
        return apply_knowledge(progs, uni_name)

    # 4. Try catalog subdomain even if given URL worked (might be a better source)
    domain = re.sub(r'^https?://(www\.)?', '', school_url or catalog_url)
    domain = re.sub(r'^www\.', '', domain.split('/')[0].rstrip('/'))
    for try_url in [f"https://catalog.{domain}", f"https://bulletin.{domain}"]:
        if try_url.replace('https://','') in (final_url or '').replace('https://',''):
            continue
        tu, ts, th = fetch(try_url)
        if not ts:
            continue
        if is_acalog(th):
            parsed = urlparse(tu)
            root   = f"{parsed.scheme}://{parsed.netloc}"
            progs  = scrape_acalog_catalog(root, ts, th)
        else:
            progs = scrape_generic(ts, tu)
        if progs:
            return apply_knowledge(progs, uni_name)

    return []

# ── Database ──────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)

def get_universities(skip_done=False, min_programs=0):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.name, u.school_url, u.catalog_url,
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
                cur.execute("SELECT DISTINCT university_id FROM university_programs WHERE generated_by='catalog'")
                done = {r['university_id'] for r in cur.fetchall()}
        rows = [r for r in rows if r['id'] not in done]
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
                """, (uid, p["name"][:200], p["category"][:100], p["degree_level"][:50],
                      p["is_featured"], p.get("top20_rank"), p.get("reputation_note"),
                      p.get("program_url")))
                if cur.fetchone(): ins += 1
                else: skp += 1
            conn.commit()
    return ins, skp

# ── CLI ───────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Scrape programs from catalog URLs",
                                  epilog=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all",           action="store_true")
    ap.add_argument("--id",            type=int)
    ap.add_argument("--name",          type=str)
    ap.add_argument("--from-csv",      type=str)
    ap.add_argument("--reset",         action="store_true")
    ap.add_argument("--preview",       action="store_true")
    ap.add_argument("--skip-done",     action="store_true")
    ap.add_argument("--min-programs",  type=int, default=0)
    ap.add_argument("--delay",         type=float, default=1.5)
    args = ap.parse_args()

    if not any([args.all, args.id, args.name, args.from_csv]):
        ap.print_help(); return

    def show_preview(name, catalog_url, programs):
        print(f"\n  {'='*60}")
        print(f"  University : {name}")
        print(f"  Catalog URL: {catalog_url or 'none'}")
        print(f"  Programs   : {len(programs)}")
        if programs:
            by_level = {}
            for p in programs:
                by_level.setdefault(p['degree_level'], []).append(p)
            for level in ["Undergraduate","Graduate","Doctoral","Certificate"]:
                grp = sorted(by_level.get(level, []), key=lambda x: x['name'])
                if not grp: continue
                print(f"\n  {level} ({len(grp)}):")
                for p in grp[:30]:
                    star = "⭐" if p["is_featured"] else "  "
                    print(f"    {star} {p['name']}")
                if len(grp) > 30:
                    print(f"       ... and {len(grp)-30} more")
        print()

    if args.id or args.name:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if args.id:
                    cur.execute("SELECT id,name,school_url,catalog_url FROM universities WHERE id=%s", (args.id,))
                else:
                    cur.execute("SELECT id,name,school_url,catalog_url FROM universities WHERE name ILIKE %s", (f'%{args.name}%',))
                matches = [dict(r) for r in cur.fetchall()]
        if not matches:
            print("No university found."); return
        if len(matches) > 1 and not args.id:
            print("Multiple matches — use --id:")
            for u in matches: print(f"  {u['id']}: {u['name']}")
            return
        u = matches[0]
        programs = scrape_catalog_url(u.get('catalog_url') or u.get('school_url',''),
                                       u['name'], u.get('school_url',''))
        if args.preview:
            show_preview(u['name'], u.get('catalog_url'), programs)
        else:
            ins, skp = save_programs(u['id'], programs, reset=args.reset)
            print(f"\n✅ {u['name']}: {len(programs)} programs, {ins} saved")
        return

    if args.from_csv:
        if not os.path.exists(args.from_csv):
            print(f"File not found: {args.from_csv}"); return
        with open(args.from_csv, encoding='latin-1') as f:
            csv_rows = list(csv.DictReader(f))
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id,name,school_url FROM universities")
                db_map = {r['name'].lower().strip(): dict(r) for r in cur.fetchall()}
        total = len(csv_rows)
        done = good = failed = ins_total = 0
        print(f"\n{'='*65}")
        print(f"  Scraping from {args.from_csv}: {total} universities")
        print(f"{'='*65}\n")
        log = []
        for row in csv_rows:
            name = row.get('name','').strip()
            cat_url = row.get('course catalog links','').strip()
            sch_url = row.get('school_url','').strip()
            if not cat_url.startswith('http'):
                done += 1
                print(f"  ⏭️  [{done}/{total}] {name[:55]:<55} no URL")
                continue
            db_u = db_map.get(name.lower())
            if not db_u:
                done += 1
                print(f"  ❓ [{done}/{total}] {name[:55]:<55} not in DB")
                continue
            programs = scrape_catalog_url(cat_url, name, sch_url)
            done += 1
            if not args.preview:
                ins, _ = save_programs(db_u['id'], programs, reset=args.reset)
                ins_total += ins
            cnt = len(programs)
            if cnt >= 10:   good += 1; icon = "✅"
            elif cnt > 0:               icon = "⚠️ "
            else:           failed += 1; icon = "❌"
            print(f"  {icon} [{done}/{total}] {name[:55]:<55} {cnt:>4} programs")
            log.append({"name": name, "catalog_url": cat_url, "programs": cnt})
            time.sleep(args.delay)
        print(f"\n{'='*65}")
        print(f"  COMPLETE | Good:{good} Partial:{done-good-failed} Failed:{failed} | Inserted:{ins_total}")
        print(f"{'='*65}")
        with open("scrape_log.json","w") as f: json.dump(log,f,indent=2)
        print("  Log: scrape_log.json\n")
        return

    unis  = get_universities(skip_done=args.skip_done, min_programs=args.min_programs)
    total = len(unis)
    done = good = failed = ins_total = 0
    print(f"\n{'='*65}")
    print(f"  Scraping {total} universities | delay={args.delay}s")
    print(f"{'='*65}\n")
    log = []
    for u in unis:
        programs = scrape_catalog_url(u.get('catalog_url',''), u['name'], u.get('school_url',''))
        done += 1
        ins, _ = save_programs(u['id'], programs, reset=args.reset)
        ins_total += ins
        cnt = len(programs)
        if cnt >= 10:   good += 1; icon = "✅"
        elif cnt > 0:               icon = "⚠️ "
        else:           failed += 1; icon = "❌"
        print(f"  {icon} [{done}/{total}] {u['name'][:55]:<55} {cnt:>4} programs")
        log.append({"id":u["id"],"name":u["name"],"programs":cnt,"inserted":ins})
        time.sleep(args.delay)
    print(f"\n{'='*65}")
    print(f"  COMPLETE | Good:{good} Partial:{done-good-failed} Failed:{failed} | Inserted:{ins_total}")
    print(f"{'='*65}")
    with open("scrape_log.json","w") as f: json.dump(log,f,indent=2)
    print("  Log: scrape_log.json\n")

if __name__ == "__main__":
    main()
