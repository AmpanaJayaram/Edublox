"""
scripts/scrapers/scraper_utils.py
==================================
Shared utilities for all UniSearch scrapers.

Previously copy-pasted across master_scraper.py, production_scraper.py,
and scrape_all.py. Single source of truth for:
  - DB config and connection
  - Category / degree-level classification
  - HTML fetch helpers
  - Acalog + generic catalog scrapers
  - Knowledge-base application
  - DB save helpers
"""

import os
import re
import psycopg2
import psycopg2.extras
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── DB config ─────────────────────────────────────────────────
# No password fallback — script will fail clearly if .env is missing.
DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME",  "unisearch"),
    "user":     os.environ.get("DB_USER",  "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "host":     os.environ.get("DB_HOST",  "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 20


# ── DB connection ─────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


# ── Category classification ───────────────────────────────────

def get_category(name: str) -> str:
    n = name.lower()
    if any(k in n for k in [
        "engineering", "mechanical", "electrical", "civil", "chemical",
        "biomedical", "aerospace", "materials science", "nuclear", "petroleum",
        "robotics", "mechatronics", "construction engineering",
    ]):
        return "Engineering"
    if any(k in n for k in [
        "nursing", "medicine", "pharmacy", "public health", "kinesiology",
        "physical therapy", "occupational therapy", "health science",
        "rehabilitation", "nutrition", "behavior analysis", "addiction studies",
        "audiology", "speech-language", "radiologic", "veterinary", "dental",
        "clinical health", "health administration", "health informatics", "surgical",
    ]):
        return "Health & Medicine"
    if any(k in n for k in [
        "business administration", "business analytics", "accounting", "finance",
        "marketing", "supply chain", "hospitality", "merchandising", "logistics",
        "mba", "banking", "actuarial", "real estate", "insurance", "tourism",
        "entrepreneurship", "human resource", "management information",
        "e-commerce", "sport entertainment",
    ]):
        return "Business"
    if re.search(r'\beconomics\b|\bmanagement\b', n) and not any(
        x in n for x in ["information management", "project design", "recreation",
                          "applied", "park", "arts and"]
    ):
        return "Business"
    if any(k in n for k in [
        "curriculum and instruction", "educational leadership",
        "educational technology", "educational psychology", "special education",
        "early childhood education", "elementary education", "higher education",
        "school counseling", "literacy", "teacher education",
        "instructional design", "music education", "art education",
        "science education",
    ]):
        return "Education"
    if re.search(r'\beducation\b', n) and "physical education" not in n and "health education" not in n:
        return "Education"
    if any(k in n for k in [
        "criminal justice", "public administration", "public policy",
        "emergency management", "nonprofit", "urban planning", "urban policy",
        "homeland security", "legal studies", "criminology", "forensic",
        "conflict resolution", "emergency administration",
    ]):
        return "Law & Policy"
    if re.search(r'\blaw\b|\bpolitical science\b', n):
        return "Law & Policy"
    if any(k in n for k in [
        "computer science", "cybersecurity", "information technology",
        "data science", "data analytics", "data engineering",
        "artificial intelligence", "machine learning", "mathematics",
        "statistics", "physics", "chemistry", "biology", "biochemistry",
        "bioinformatics", "neuroscience", "geology", "astronomy", "ecology",
        "marine biology", "information science", "information systems",
        "software", "computing", "geographic information", "computational",
        "applied arts and sciences", "applied sciences", "interdisciplinary studies",
        "general studies", "liberal arts and sciences", "natural science",
        "environmental science", "cognitive science", "behavioral science",
        "applied technology", "learning technologies",
    ]):
        return "STEM"
    if any(k in n for k in [
        "sociology", "psychology", "anthropology", "social work", "geography",
        "social science", "women's studies", "gender studies", "ethnic studies",
        "family studies", "international studies", "international relations",
        "urban studies", "human development", "child development",
        "rehabilitation counseling",
    ]):
        return "Social Sciences"
    if any(k in n for k in [
        "music performance", "music theory", "music education", "jazz studies",
        "musical arts", "composition", "commercial music", "studio art",
        "fine arts", "art history", "graphic design", "fashion design",
        "interior design", "communication design", "dance", "theatre arts",
        "theater", "film production", "cinematography", "photography",
        "creative writing", "english literature", "english language and literature",
        "journalism", "advertising", "public relations", "media arts",
        "digital media", "visual arts", "art education", "world language",
        "foreign language", "history", "philosophy", "religious studies",
        "theology", "linguistics", "spanish", "french", "german", "japanese",
        "chinese", "arabic", "architecture", "technical communication",
        "content strategy",
    ]):
        return "Arts & Humanities"
    if re.search(r'\bcommunication studies\b|\bmass communication\b', n):
        return "Arts & Humanities"
    return "STEM"


# ── Degree-level classification ───────────────────────────────

def get_level(raw: str) -> str:
    n = raw.lower()
    if re.search(
        r'\bph\.?d\.?\b|\bdoctor\b|\bd\.m\.a\b|\bed\.d\b|\bj\.d\b'
        r'|\bm\.d\b|\bpharm\.d\b|\bdpt\b|\bdnp\b', n
    ):
        return "Doctoral"
    if re.search(r'\bmaster\b', n):
        return "Graduate"
    if re.search(
        r',\s*(m\.s\.?|m\.a\.?|mba|m\.ed\.?|mfa|m\.arch\.?'
        r'|m\.mus\.?|m\.p\.h\.?|m\.p\.a\.?|ms|ma|msw|m\.eng\.?)\s*$', n
    ):
        return "Graduate"
    if "grad track" in n:
        return "Graduate"
    if "certificate" in n:
        return "Certificate"
    if "minor" in n:
        return "Minor"
    return "Undergraduate"


def clean(raw: str) -> str:
    name = re.sub(
        r',\s*(B[A-Z]{1,5}S?|M[A-Z]{1,5}|Ph\.?D\.?|Ed\.?D\.?|J\.D\.?'
        r'|M\.D\.?|D\.M\.A\.?|D\.N\.P\.?|D\.P\.T\.?'
        r'|BAAS|BSW|BSET|BSBC|BSBIO|BSCHM|BSMTH|BSMLS|BAS|BSECO|BSPHY|BSEET)\s*$',
        '', raw, flags=re.I,
    ).strip()
    name = re.sub(r'\s*\(not currently accepting students\)\s*', '', name, flags=re.I)
    name = re.sub(r'\s*\(dual degree[^)]*\)\s*', '', name, flags=re.I)
    return re.sub(r'\s+', ' ', name).strip()


def is_real_degree(raw: str) -> bool:
    n = raw.lower().strip()
    SKIP = [
        'grad track option', 'teacher certification', 'teacher cert', 'pre-major',
        'degree requirements', 'general university requirements',
        'university core curriculum', 'honors courses that meet', 'dual degree',
        'preprofessional', 'department of ', 'college of ', 'school of ', 'division of ',
    ]
    if any(p in n for p in SKIP):
        return False
    if re.search(r'\bminor\b', n):
        return False
    if not re.search(
        r'\b(bachelor|master|doctor|associate|certificate'
        r'|b\.?s\.?\b|b\.?a\.?\b|b\.?f\.?a\.?\b|b\.?m\.?\b|b\.?b\.?a\.?\b'
        r'|m\.?s\.?\b|m\.?a\.?\b|mba|m\.?ed\.?\b|mfa|m\.?arch\.?\b'
        r'|ph\.?d\.?\b|phd|ed\.?d\.?\b|j\.?d\.?\b|m\.?d\.?\b|pharm\.?d\.?\b'
        r'|baas|bsbc|bsbio|bschm|bsmth|bsmls|bseco|bsphy|bseet|bset|bsw|bas)\b',
        raw, re.I,
    ):
        return False
    return True


# ── HTTP helpers ──────────────────────────────────────────────

def fetch(url: str) -> tuple:
    """Return (final_url, soup, html) or (None, None, '') on failure."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
            return r.url, BeautifulSoup(r.text, "html.parser"), r.text
    except Exception:
        pass
    return None, None, ""


# ── Catalog detection ─────────────────────────────────────────

def is_acalog(html: str) -> bool:
    return any(m in html for m in [
        "preview_program.php", "catoid=", "Modern Campus Catalog", "Acalog ACMS",
    ])


def is_smartcatalog(html: str, url: str = "") -> bool:
    return (
        "smartcatalogiq" in html.lower()
        or "smartcatalogiq" in (url or "").lower()
    )


# ── Acalog scraping ───────────────────────────────────────────

SKIP_EXACT = {
    "degree requirements",
    "general university requirements",
    "university core curriculum",
    "honors courses that meet university core curriculum",
}
SKIP_ENDS = ["degree requirements"]


def extract_acalog(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Extract program links from an Acalog catalog page."""
    programs, seen = [], set()
    for a in soup.find_all("a", href=re.compile(r"preview_program\.php")):
        raw = a.get_text(strip=True)
        if not raw or len(raw) < 4:
            continue
        rl = raw.lower().strip()
        if rl in SKIP_EXACT:
            continue
        if any(rl.endswith(s) for s in SKIP_ENDS):
            continue
        if not is_real_degree(raw):
            continue
        if rl in seen:
            continue
        seen.add(rl)
        level = get_level(raw)
        if level == "Minor":
            continue
        name = clean(raw)
        if not name or len(name) < 4:
            continue
        programs.append({
            "name":            name,
            "degree_level":    level,
            "category":        get_category(name),
            "program_url":     urljoin(base_url, a["href"]),
            "is_featured":     False,
            "top20_rank":      None,
            "reputation_note": None,
        })
    return programs


def best_acalog_page(root_url: str, root_soup: BeautifulSoup) -> list[dict]:
    """
    Try every navoid link on the page and return programs from the page
    with the most results. Prioritises pages labelled 'academic unit' or
    'programs listed' — these are typically the master program listings.
    """
    navoids = {}
    for a in root_soup.find_all("a", href=True):
        if "navoid=" in a["href"]:
            url = urljoin(root_url, a["href"])
            if url not in navoids:
                navoids[url] = a.get_text(strip=True)

    def _rank(item):
        t = item[1].lower()
        if "academic unit" in t or "programs listed" in t:
            return 0
        if "program" in t or "major" in t or "degree" in t:
            return 1
        return 2

    best: list[dict] = []
    for url, _ in sorted(navoids.items(), key=_rank):
        _, s, _ = fetch(url)
        if not s:
            continue
        progs = extract_acalog(s, url)
        if len(progs) > len(best):
            best = progs
        if len(best) >= 150:  # found the main program listing page
            break
    return best


def scrape_acalog_full(start_url: str, start_soup: BeautifulSoup, start_html: str) -> list[dict]:
    """
    Scrape ALL current (non-archived) catalogs from an Acalog site.
    Handles being called on a subpage — navigates to the root first.
    Deduplicates by name across all catalogs found in the dropdown.
    """
    parsed = urlparse(start_url)
    root   = f"{parsed.scheme}://{parsed.netloc}"

    root_soup = start_soup
    if start_url.rstrip("/") != root.rstrip("/"):
        _, rs, rh = fetch(root)
        if rs and is_acalog(rh):
            root_soup = rs

    all_progs: list[dict] = []
    seen: set[str]        = set()

    # Collect current catalogs from the dropdown (skip archived ones)
    current_cats = []
    seen_catoids: set[str] = set()
    for opt in root_soup.find_all("option"):
        val  = opt.get("value", "")
        text = opt.get_text(strip=True)
        if not val or not val.isdigit() or val in seen_catoids:
            continue
        seen_catoids.add(val)
        if "ARCHIVED" in text.upper():
            continue
        if any(x in text.lower() for x in ["courses", "academic units", "other content", "entire catalog"]):
            continue
        current_cats.append((val, text))

    if not current_cats:
        return best_acalog_page(root, root_soup)

    for catoid, _ in current_cats:
        cat_url = f"{root}/index.php?catoid={catoid}"
        _, cs, ch = fetch(cat_url)
        if not cs or not is_acalog(ch):
            continue
        for p in best_acalog_page(cat_url, cs):
            key = p["name"].lower()
            if key not in seen:
                all_progs.append(p)
                seen.add(key)

    return all_progs


# ── Generic HTML scraper ──────────────────────────────────────

DEGREE_RE = re.compile(
    r"\b(bachelor|master|doctor|associate|certificate in|b\.s\.|b\.a\.|m\.s\.|m\.a\.|"
    r"mba|m\.b\.a|b\.f\.a|m\.f\.a|ph\.d|phd|juris|pharm\.d|d\.p\.t|d\.n\.p)\b",
    re.I,
)


def scrape_generic(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """
    Generic HTML scraper — looks for degree-pattern anchor text.
    Falls back to li/td/h3/h4 elements if fewer than 5 links matched.
    """
    programs, seen = [], set()

    def _add(text: str, purl: str):
        name = clean(text)
        key  = name.lower()
        if key in seen or len(name) < 8:
            return
        seen.add(key)
        level = get_level(text)
        if level == "Minor":
            return
        programs.append({
            "name":            name,
            "degree_level":    level,
            "category":        get_category(name),
            "program_url":     purl,
            "is_featured":     False,
            "top20_rank":      None,
            "reputation_note": None,
        })

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 200:
            continue
        if not DEGREE_RE.search(text):
            continue
        href = a["href"]
        purl = urljoin(base_url, href) if not href.startswith("http") else href
        _add(text, purl)

    # Fallback: structured elements
    if len(programs) < 5:
        for tag in soup.find_all(["li", "td", "h3", "h4"]):
            text = tag.get_text(strip=True)
            if not text or len(text) < 10 or len(text) > 200:
                continue
            if not DEGREE_RE.search(text):
                continue
            pa   = tag.find("a", href=True)
            purl = urljoin(base_url, pa["href"]) if pa else base_url
            _add(text, purl)

    return programs


# ── Knowledge-base application ────────────────────────────────

def apply_knowledge(programs: list[dict], uni_name: str) -> list[dict]:
    """
    Apply featured program and top-20 data from university_knowledge.py
    (if it is importable from the current sys.path).
    """
    try:
        from university_knowledge import KNOWN_FEATURED  # type: ignore[import]
    except ImportError:
        return programs

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
                p.update({
                    "is_featured":     True,
                    "reputation_note": rep,
                    "program_url":     purl or p.get("program_url"),
                })
                found = True
                break
        if not found:
            programs.insert(0, {
                "name":            feat,
                "degree_level":    "Undergraduate",
                "category":        get_category(feat),
                "program_url":     purl,
                "is_featured":     True,
                "top20_rank":      1,
                "reputation_note": rep,
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


# ── DB save helpers ───────────────────────────────────────────

def save_programs(
    uid: int,
    programs: list[dict],
    reset: bool = False,
    wipe_catalog: bool = False,
) -> tuple[int, int]:
    """
    Persist programs for a university.

    reset=True:        delete ALL existing programs first.
    wipe_catalog=True: delete only previously catalog-scraped programs first
                       (production_scraper behaviour — keeps manual entries).

    Returns (inserted, skipped).
    """
    if not programs:
        return 0, 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("DELETE FROM university_programs WHERE university_id = %s", (uid,))
            elif wipe_catalog:
                cur.execute(
                    "DELETE FROM university_programs "
                    "WHERE university_id = %s AND generated_by = 'catalog'",
                    (uid,),
                )
            inserted = skipped = 0
            for p in programs:
                cur.execute(
                    """
                    INSERT INTO university_programs
                        (university_id, name, category, degree_level, description,
                         is_featured, top20_rank, reputation_note, program_url, generated_by)
                    VALUES (%s, %s, %s, %s, NULL, %s, %s, %s, %s, 'catalog')
                    ON CONFLICT DO NOTHING RETURNING id
                    """,
                    (
                        uid,
                        p["name"][:200],
                        p["category"][:100],
                        p["degree_level"][:50],
                        p["is_featured"],
                        p.get("top20_rank"),
                        p.get("reputation_note"),
                        p.get("program_url"),
                    ),
                )
                if cur.fetchone():
                    inserted += 1
                else:
                    skipped += 1
            conn.commit()

    return inserted, skipped


def fix_categories() -> None:
    """Re-classify every program's category using the current get_category() rules."""
    print("Fixing categories on all programs...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM university_programs")
            rows  = cur.fetchall()
            fixed = 0
            for row in rows:
                new_cat = get_category(row["name"])
                cur.execute(
                    "UPDATE university_programs SET category = %s WHERE id = %s AND category != %s",
                    (new_cat, row["id"], new_cat),
                )
                if cur.rowcount:
                    fixed += 1
            conn.commit()
    print(f"  Fixed {fixed:,} categories")


def update_top20() -> None:
    """Apply top-20 rankings and featured flags from university_knowledge.py."""
    try:
        from university_knowledge import KNOWN_FEATURED  # type: ignore[import]
    except ImportError:
        print("university_knowledge.py not found")
        return

    print(f"Updating top 20 for {len(KNOWN_FEATURED)} universities...")
    updated = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for key, data in KNOWN_FEATURED.items():
                cur.execute(
                    "SELECT id FROM universities WHERE name ILIKE %s", (f"%{key}%",)
                )
                for u in cur.fetchall():
                    uid   = u["id"]
                    feat  = data["featured_name"]
                    rep   = data.get("reputation_note", "")
                    purl  = data.get("program_url")
                    top20 = data.get("top20", [feat])

                    cur.execute(
                        "UPDATE university_programs "
                        "SET is_featured = FALSE, top20_rank = NULL "
                        "WHERE university_id = %s",
                        (uid,),
                    )
                    cur.execute(
                        """
                        UPDATE university_programs
                        SET is_featured = TRUE, reputation_note = %s,
                            program_url = COALESCE(%s, program_url)
                        WHERE university_id = %s AND LOWER(name) ILIKE %s
                        """,
                        (rep, purl, uid, f"%{feat.lower()[:30]}%"),
                    )
                    if cur.rowcount == 0:
                        cur.execute(
                            """
                            INSERT INTO university_programs
                                (university_id, name, category, degree_level, description,
                                 is_featured, top20_rank, reputation_note, program_url, generated_by)
                            VALUES (%s, %s, %s, 'Undergraduate', NULL, TRUE, 1, %s, %s, 'knowledge_base')
                            ON CONFLICT DO NOTHING
                            """,
                            (uid, feat, get_category(feat), rep, purl),
                        )
                    for rank, pname in enumerate(top20, 1):
                        cur.execute(
                            "UPDATE university_programs SET top20_rank = %s "
                            "WHERE university_id = %s AND LOWER(name) ILIKE %s",
                            (rank, uid, f"%{pname.lower()[:30]}%"),
                        )
                    updated += 1
            conn.commit()
    print(f"  Updated {updated} universities")
