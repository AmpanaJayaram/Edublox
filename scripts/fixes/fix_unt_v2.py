"""
fix_unt_v2.py — Clean UNT scrape: current catalogs only, correct degree levels
Run: python fix_unt_v2.py
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
    if any(k in n for k in ["engineering","mechanical","electrical","civil","chemical","biomedical","aerospace","materials science","nuclear","petroleum","robotics","mechatronics","construction engineering"]):
        return "Engineering"
    if any(k in n for k in ["nursing","medicine","pharmacy","public health","kinesiology","physical therapy","occupational therapy","health science","rehabilitation","nutrition","behavior analysis","addiction studies","audiology","speech-language","radiologic","veterinary","dental","clinical health","health administration","health informatics","surgical"]):
        return "Health & Medicine"
    if any(k in n for k in ["business administration","business analytics","accounting","finance","marketing","supply chain","hospitality","merchandising","logistics","mba","banking","actuarial","real estate","insurance","tourism","entrepreneurship","human resource","management information","e-commerce","sport entertainment"]):
        return "Business"
    if re.search(r'\beconomics\b|\bmanagement\b', n) and not any(x in n for x in ["information management","project design","recreation","applied","park","arts and"]):
        return "Business"
    if any(k in n for k in ["curriculum and instruction","educational leadership","educational technology","educational psychology","special education","early childhood education","elementary education","higher education","school counseling","literacy","teacher education","instructional design","music education","art education","science education","math"]):
        return "Education"
    if re.search(r'\beducation\b', n) and "physical education" not in n and "health education" not in n:
        return "Education"
    if any(k in n for k in ["criminal justice","public administration","public policy","emergency management","nonprofit","urban planning","urban policy","homeland security","legal studies","criminology","forensic","conflict resolution"]):
        return "Law & Policy"
    if re.search(r'\blaw\b|\bpolitical science\b', n):
        return "Law & Policy"
    if any(k in n for k in ["computer science","cybersecurity","information technology","data science","data analytics","data engineering","artificial intelligence","machine learning","mathematics","statistics","physics","chemistry","biology","biochemistry","bioinformatics","neuroscience","geology","astronomy","ecology","marine biology","information science","information systems","software","computing","geographic information","computational","applied arts and sciences","interdisciplinary studies","general studies","natural science","environmental science","cognitive science","behavioral science","applied technology","learning technologies"]):
        return "STEM"
    if any(k in n for k in ["sociology","psychology","anthropology","social work","geography","social science","women's studies","gender studies","ethnic studies","family studies","international studies","international relations","urban studies","human development","child development","rehabilitation counseling"]):
        return "Social Sciences"
    if any(k in n for k in ["music performance","music theory","jazz studies","musical arts","composition","commercial music","studio art","fine arts","art history","graphic design","fashion design","interior design","communication design","dance","theatre","theater","film production","cinematography","photography","creative writing","english literature","english language","journalism","advertising","public relations","media arts","digital media","visual arts","foreign language","world language","history","philosophy","religious studies","theology","linguistics","spanish","french","german","japanese","chinese","arabic","architecture","technical communication","content strategy"]):
        return "Arts & Humanities"
    if re.search(r'\bcommunication studies\b|\bmass communication\b', n):
        return "Arts & Humanities"
    return "STEM"

def get_level(raw):
    """Determine degree level from RAW name (with abbreviation)."""
    n = raw.lower()
    # Doctoral
    if re.search(r'\bph\.?d\.?\b|\bdoctor\b|\bd\.m\.a\.?\b|\bed\.d\.?\b|\bj\.d\.?\b|\bm\.d\.?\b|\bpharm\.d\.?\b|\bdpt\b|\bdnp\b', n):
        return "Doctoral"
    # Graduate abbreviations at end: ", MS" ", MA" ", MBA" etc.
    if re.search(r',\s*(m\.s\.?|m\.a\.?|mba|m\.ed\.?|mfa|m\.arch\.?|m\.mus\.?|m\.p\.h\.?|m\.p\.a\.?|ms|ma|msw|m\.eng\.?)\s*$', n):
        return "Graduate"
    if re.search(r'\bmaster\b', n):
        return "Graduate"
    # Certificate
    if 'certificate' in n:
        return "Certificate"
    # Minor
    if re.search(r'\bminor\b', n):
        return "Minor"
    # Undergraduate abbreviations: BS, BA, BBA, BM, BFA, BAAS etc.
    if re.search(r',\s*(b\.s\.?|b\.a\.?|b\.f\.a\.?|b\.m\.?|b\.b\.a\.?|baas|bsw|bset|bas|b\.arch\.?)\s*$', n):
        return "Undergraduate"
    if re.search(r'\bbachelor\b|\bassociate\b', n):
        return "Undergraduate"
    return "Undergraduate"

def clean(raw):
    """Remove degree abbreviation for display — keep concentration details."""
    name = re.sub(r',\s*(B[A-Z]{1,5}S?|M[A-Z]{1,5}|Ph\.?D\.?|Ed\.?D\.?|J\.D\.?|M\.D\.?|D\.M\.A\.?|D\.N\.P\.?|BAAS|BSW|BSET|BAS|BSPHY)\s*$', '', raw, flags=re.I).strip()
    name = re.sub(r'\s*\(not currently accepting students\)\s*', '', name, flags=re.I)
    name = re.sub(r'\s*\(dual degree[^)]*\)\s*', '', name, flags=re.I)
    return re.sub(r'\s+', ' ', name).strip()

def is_real_degree(raw):
    n = raw.lower().strip()
    SKIP = ['grad track option','teacher certification','teacher cert','pre-major',
            'degree requirements','general university requirements','university core curriculum',
            'honors courses that meet','dual degree','preprofessional',
            'department of ','college of ','school of ','division of ']
    if any(p in n for p in SKIP): return False
    if re.search(r'\bminor\b', n): return False
    # Handle abbreviations with OR without dots (BS, B.S., BBA, B.B.A., MS, M.S. etc.)
    if not re.search(
        r'\b(bachelor|master|doctor|associate|certificate'
        r'|b\.?s\.?\b|b\.?a\.?\b|b\.?f\.?a\.?\b|b\.?m\.?\b|b\.?b\.?a\.?\b'
        r'|m\.?s\.?\b|m\.?a\.?\b|mba|m\.?ed\.?\b|mfa|m\.?arch\.?\b'
        r'|ph\.?d\.?\b|phd|ed\.?d\.?\b|j\.?d\.?\b|m\.?d\.?\b|pharm\.?d\.?\b'
        r'|baas|bsbc|bsbio|bschm|bsmth|bsmls|bseco|bsphy|bseet|bset|bsw|bas)\b',
        raw, re.I):
        return False
    return True

SKIP_EXACT = {'degree requirements','general university requirements','university core curriculum'}

def extract_programs(soup, base_url):
    programs = []
    seen_raw = set()  # dedup by RAW name to preserve all variants

    for a in soup.find_all('a', href=re.compile(r'preview_program\.php')):
        raw = a.get_text(strip=True)
        if not raw or len(raw) < 4: continue

        rl = raw.lower().strip()
        if rl in SKIP_EXACT: continue
        if any(rl.endswith(s) for s in ['degree requirements']): continue
        if not is_real_degree(raw): continue
        if rl in seen_raw: continue
        seen_raw.add(rl)

        # Get level from RAW (before cleaning removes abbreviation)
        level = get_level(raw)
        if level == "Minor": continue

        # Clean name for display
        name = clean(raw)
        if not name or len(name) < 4: continue

        programs.append({
            "name":          name,
            "degree_level":  level,
            "category":      get_category(name),
            "program_url":   urljoin(base_url, a['href']),
            "is_featured":   "jazz studies" in name.lower(),
            "top20_rank":    1 if "jazz studies" in name.lower() else None,
            "reputation_note": "UNT Jazz Studies is ranked top-5 nationally" if "jazz studies" in name.lower() else None,
        })
    return programs

def best_page(root_url, root_soup):
    """Find the navoid page with the most programs."""
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
        if len(best) >= 80:
            print(f"    ✓ Found best page: {text[:50]} ({len(best)} programs)")
            break
    return best, best_url

# ── Main ───────────────────────────────────────────────────────
print("="*60)
print("Fetching catalog.unt.edu...")
soup, _ = fetch("https://catalog.unt.edu")

# Get ONLY current (non-archived) catalogs
current = []
seen_ids = set()
for opt in soup.find_all('option'):
    val  = opt.get('value','')
    text = opt.get_text(strip=True)
    if not val or not val.isdigit() or val in seen_ids: continue
    seen_ids.add(val)
    if 'ARCHIVED' in text.upper(): continue
    if any(x in text.lower() for x in ['courses','academic units','other content','entire catalog']): continue
    current.append((val, text))
    print(f"  catoid={val}: {text}")

print(f"\nScraping {len(current)} current catalogs...")

all_programs = []
seen_display = set()  # dedup by display name across catalogs

for catoid, cat_name in current:
    cat_url = f"https://catalog.unt.edu/index.php?catoid={catoid}"
    print(f"\n→ {cat_name}")
    s, _ = fetch(cat_url)
    if not s: continue
    progs, _ = best_page(cat_url, s)
    print(f"  Programs found: {len(progs)}")

    added = 0
    for p in progs:
        # Use name+level as dedup key so "Accounting" UG and "Accounting" Grad are both kept
        key = f"{p['name'].lower()}|{p['degree_level']}"
        if key not in seen_display:
            all_programs.append(p)
            seen_display.add(key)
            added += 1
    print(f"  New programs added: {added}")

# Summary
print(f"\n{'='*60}")
print(f"TOTAL: {len(all_programs)} programs")
by_level = {}
for p in all_programs: by_level.setdefault(p['degree_level'],[]).append(p)
for lvl in ["Undergraduate","Graduate","Doctoral","Certificate"]:
    grp = by_level.get(lvl,[])
    if grp:
        print(f"  {lvl}: {len(grp)}")
        for p in sorted(grp, key=lambda x:x['name'])[:5]:
            print(f"    {'⭐' if p['is_featured'] else '  '} {p['name']}")
        if len(grp)>5: print(f"    ... and {len(grp)-5} more")

# Save
print(f"\n{'='*60}")
print("Saving to database...")
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
print("Refresh your browser to see the updated programs.")
