"""
fix_unt_clean.py - Scrape ONLY real degree programs from UNT
Filters out: grad track options, concentrations-as-separate-entries,
pre-majors, teacher certifications, dual degrees, requirements pages.
Run: python fix_unt_clean.py
"""
import os, re, requests, psycopg2, psycopg2.extras
from bs4 import BeautifulSoup
from urllib.parse import urljoin

DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME", "unisearch"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "2000"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
}
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser"), r.text
    except Exception as e:
        print(f"  ERROR: {e}")
    return None, ""

def get_category(name):
    n = name.lower()
    for cat, kws in [
        ("Engineering",      ["engineering","mechanical","electrical","civil","chemical","biomedical","aerospace","materials","nuclear","petroleum","robotics"]),
        ("STEM",             ["computer science","cybersecurity","information technology","data science","mathematics","physics","chemistry","biology","biochemistry","statistics","information science","computing","geology","astronomy","ecology","neuroscience","bioinformatics"]),
        ("Health & Medicine",["nursing","medicine","pharmacy","public health","kinesiology","physical therapy","occupational","health","rehabilitation","nutrition","behavior analysis","addiction","audiology","speech","radiologic","veterinary","dental","clinical"]),
        ("Business",         ["business","accounting","finance","marketing","management","economics","entrepreneurship","supply chain","hospitality","merchandising","logistics","mba","banking","actuarial","real estate","insurance","tourism"]),
        ("Education",        ["education","teaching","curriculum","counseling","early childhood","literacy","teacher","pedagogy","special education"]),
        ("Law & Policy",     ["law","political","criminal justice","public administration","emergency","nonprofit","urban planning","government","legal","criminology","forensic"]),
        ("Arts & Humanities",["music","art","design","dance","theatre","theater","film","journalism","media","communication","english","history","philosophy","religion","linguistics","language","creative writing","graphic","fashion","photography","spanish","french","german","japanese","chinese","arabic","architecture","advertising"]),
        ("Social Sciences",  ["sociology","psychology","anthropology","social work","geography","social science","gender","ethnic","family","international studies","urban studies"]),
    ]:
        if any(kw in n for kw in kws): return cat
    return "STEM"

def get_level(raw):
    n = raw.lower()
    if re.search(r'\bph\.?d\.?\b|\bdoctor\b|\bd\.m\.a\b|\bed\.d\b|\bj\.d\b|\bm\.d\b|\bpharm\.d\b|\bdpt\b|\bdnp\b', n):
        return "Doctoral"
    if re.search(r'\bmaster\b|,\s*(m\.s\.?|m\.a\.?|mba|m\.ed\.?|mfa|m\.arch\.?|m\.mus\.?|m\.p\.h\.?|ms|ma|msw)\s*$', n):
        return "Graduate"
    if 'certificate' in n:
        return "Certificate"
    if 'minor' in n:
        return "Minor"
    return "Undergraduate"

def clean(raw):
    name = re.sub(r',\s*(B[A-Z]{1,5}S?|M[A-Z]{1,5}|Ph\.?D\.?|Ed\.?D\.?|J\.D\.?|M\.D\.?|D\.M\.A\.?|D\.N\.P\.?|BAAS|BSW|BSET|BAS|BSPHY)\s*$', '', raw, flags=re.I).strip()
    name = re.sub(r'\s*\(not currently accepting students\)\s*', '', name, flags=re.I)
    name = re.sub(r'\s*\(dual degree[^)]*\)\s*', '', name, flags=re.I)
    return re.sub(r'\s+', ' ', name).strip()

def is_real_degree(raw):
    """Only keep actual standalone degree programs."""
    n = raw.lower().strip()

    # Skip these patterns
    SKIP_PHRASES = [
        'grad track option',
        'teacher certification',
        'teacher cert',
        'pre-major',
        'degree requirements',
        'general university requirements',
        'university core curriculum',
        'honors courses that meet',
        'dual degree',
        'preprofessional',
        'department of ',
        'college of ',
        'school of ',
        'division of ',
        'go to information',
    ]
    if any(p in n for p in SKIP_PHRASES):
        return False

    # Skip pure minors
    if re.search(r'\bminor\b', n):
        return False

    # Must contain a real degree indicator
    if not re.search(
        r'\b(bachelor|master|doctor|associate|certificate|'
        r'b\.s\.|b\.a\.|b\.f\.a\.|b\.m\.|b\.b\.a\.|'
        r'm\.s\.|m\.a\.|mba|m\.ed\.|mfa|m\.arch\.|'
        r'ph\.d|phd|ed\.d|j\.d|m\.d|pharm\.d|'
        r'baas|bsw|bset|bas)\b', raw, re.I):
        return False

    return True

SKIP_EXACT = {'degree requirements', 'general university requirements', 'university core curriculum'}

def extract_programs(soup, base_url):
    programs, seen = [], set()
    for a in soup.find_all('a', href=re.compile(r'preview_program\.php')):
        raw = a.get_text(strip=True)
        if not raw or len(raw) < 4: continue
        rl = raw.lower().strip()
        if rl in SKIP_EXACT: continue
        if any(rl.endswith(s) for s in ['degree requirements']): continue

        # Only keep real degree programs
        if not is_real_degree(raw):
            continue

        if rl in seen: continue
        seen.add(rl)

        level = get_level(raw)
        if level == "Minor": continue

        name = clean(raw)
        if not name or len(name) < 4: continue

        programs.append({
            "name": name,
            "degree_level": level,
            "category": get_category(name),
            "program_url": urljoin(base_url, a['href']),
            "is_featured": "jazz studies" in name.lower(),
            "top20_rank": 1 if "jazz studies" in name.lower() else None,
            "reputation_note": "UNT Jazz Studies is ranked top-5 nationally" if "jazz studies" in name.lower() else None,
        })
    return programs

def best_page(root_url, root_soup):
    navoids = {}
    for a in root_soup.find_all('a', href=True):
        if 'navoid=' in a['href']:
            url = urljoin(root_url, a['href'])
            if url not in navoids:
                navoids[url] = a.get_text(strip=True)

    def rank(item):
        t = item[1].lower()
        if 'academic unit' in t or 'programs listed' in t: return 0
        if 'program' in t or 'major' in t or 'degree' in t: return 1
        return 2

    best, best_url = [], ""
    for url, text in sorted(navoids.items(), key=rank):
        s, _ = fetch(url)
        if not s: continue
        progs = extract_programs(s, url)
        if len(progs) > len(best):
            best, best_url = progs, url
        # Stop once we've found the main programs page (has many more than any single college page)
        if len(best) >= 150:
            break
    return best, best_url

# ── Scrape all UNT catalogs ────────────────────────────────────
print("="*60)
print("Fetching catalog.unt.edu...")
soup, html = fetch("https://catalog.unt.edu")

# Find all catalogs in dropdown
all_catoids = []
for opt in soup.find_all('option'):
    val  = opt.get('value','')
    text = opt.get_text(strip=True)
    if val and val.isdigit():
        all_catoids.append((val, text))
        print(f"  catoid={val}: {text}")

print(f"\nFound {len(all_catoids)} catalogs")

all_programs = []
seen_names = set()

for catoid, cat_name in all_catoids:
    cat_url = f"https://catalog.unt.edu/index.php?catoid={catoid}"
    print(f"\nScraping: {cat_name}")
    s, h = fetch(cat_url)
    if not s: continue
    progs, best_url = best_page(cat_url, s)
    print(f"  Programs: {len(progs)}")
    for p in progs:
        key = p['name'].lower()
        if key not in seen_names:
            all_programs.append(p)
            seen_names.add(key)

print(f"\n{'='*60}")
print(f"TOTAL REAL DEGREE PROGRAMS: {len(all_programs)}")

by_level = {}
for p in all_programs: by_level.setdefault(p['degree_level'],[]).append(p)
for lvl in ["Undergraduate","Graduate","Doctoral","Certificate"]:
    grp = by_level.get(lvl,[])
    if grp:
        print(f"\n{lvl} ({len(grp)}):")
        for p in sorted(grp, key=lambda x: x['name'])[:15]:
            print(f"  {'⭐' if p['is_featured'] else '  '} {p['name']}")
        if len(grp)>15: print(f"  ... and {len(grp)-15} more")

# Save
print(f"\n{'='*60}")
conn = psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
cur = conn.cursor()
cur.execute("SELECT id FROM universities WHERE name='University of North Texas'")
uid = cur.fetchone()['id']
cur.execute("DELETE FROM university_programs WHERE university_id=%s", (uid,))
ins = 0
for p in all_programs:
    cur.execute("""
        INSERT INTO university_programs
            (university_id,name,category,degree_level,description,
             is_featured,top20_rank,reputation_note,program_url,generated_by)
        VALUES (%s,%s,%s,%s,NULL,%s,%s,%s,%s,'catalog')
        ON CONFLICT DO NOTHING RETURNING id
    """, (uid,p['name'][:200],p['category'][:100],p['degree_level'][:50],
          p['is_featured'],p.get('top20_rank'),p.get('reputation_note'),p.get('program_url')))
    if cur.fetchone(): ins += 1
conn.commit()
conn.close()
print(f"✅ {ins} programs saved for University of North Texas")
