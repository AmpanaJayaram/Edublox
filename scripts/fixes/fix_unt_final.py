"""
fix_unt_final.py - Scrape ONLY current UNT catalogs (no archives)
Run: python fix_unt_final.py
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
    # Engineering first
    if any(kw in n for kw in ["engineering","mechanical","electrical","civil","chemical","biomedical","aerospace","materials science","nuclear","petroleum","robotics","mechatronics","construction engineering"]):
        return "Engineering"
    # Health
    if any(kw in n for kw in ["nursing","medicine","pharmacy","public health","kinesiology","physical therapy","occupational therapy","health science","rehabilitation","nutrition","behavior analysis","addiction studies","audiology","speech-language","radiologic","veterinary","dental","clinical health","health administration","health informatics","surgical"]):
        return "Health & Medicine"
    # Business
    if any(kw in n for kw in ["business administration","business analytics","accounting","finance","marketing","supply chain","hospitality","merchandising","logistics","mba","banking","actuarial","real estate","insurance","tourism","entrepreneurship","human resource","management information","e-commerce","sport entertainment"]):
        return "Business"
    if re.search(r'\beconomics\b|\bmanagement\b', n) and not any(x in n for x in ["information management","project design","recreation","arts and","applied","park"]):
        return "Business"
    # Education
    if any(kw in n for kw in ["curriculum and instruction","educational leadership","educational technology","educational psychology","special education","early childhood education","elementary education","higher education","school counseling","literacy","teacher education","instructional design"]):
        return "Education"
    if re.search(r'\beducation\b', n) and "physical education" not in n and "health education" not in n:
        return "Education"
    # Law & Policy
    if any(kw in n for kw in ["criminal justice","public administration","public policy","emergency management","nonprofit","urban planning","urban policy","homeland security","legal studies","criminology","forensic","conflict resolution","emergency administration"]):
        return "Law & Policy"
    if re.search(r'\blaw\b|\bpolitical science\b', n):
        return "Law & Policy"
    # STEM — specific first to avoid "art" in "artificial intelligence"
    if any(kw in n for kw in ["computer science","cybersecurity","information technology","data science","data analytics","data engineering","artificial intelligence","machine learning","mathematics","statistics","physics","chemistry","biology","biochemistry","bioinformatics","neuroscience","geology","astronomy","ecology","marine biology","information science","information systems","software","computing","geographic information","computational","applied arts and sciences","applied sciences","interdisciplinary studies","general studies","liberal arts and sciences","natural science","environmental science","cognitive science","behavioral science","applied technology"]):
        return "STEM"
    # Social Sciences
    if any(kw in n for kw in ["sociology","psychology","anthropology","social work","geography","social science","women's studies","gender studies","ethnic studies","family studies","international studies","international relations","urban studies","human development","child development","rehabilitation counseling"]):
        return "Social Sciences"
    # Arts & Humanities — very specific phrases only
    if any(kw in n for kw in ["music performance","music theory","music education","jazz studies","musical arts","composition","commercial music","studio art","fine arts","art history","graphic design","fashion design","interior design","communication design","dance","theatre arts","theater","film production","cinematography","photography","creative writing","english literature","english language and literature","journalism","advertising","public relations","media arts","digital media","visual arts","art education","world language","foreign language","history","philosophy","religious studies","theology","linguistics","spanish","french","german","japanese","chinese","arabic","architecture","technical communication","content strategy"]):
        return "Arts & Humanities"
    if re.search(r'\bcommunication studies\b|\bmass communication\b', n):
        return "Arts & Humanities"
    return "STEM"


def get_level(raw):
    n = raw.lower()
    if re.search(r'\bph\.?d\.?\b|\bdoctor\b|\bd\.m\.a\b|\bed\.d\b|\bj\.d\b|\bm\.d\b|\bpharm\.d\b|\bdpt\b|\bdnp\b', n): return "Doctoral"
    if re.search(r'\bmaster\b|,\s*(m\.s\.?|m\.a\.?|mba|m\.ed\.?|mfa|m\.arch\.?|m\.mus\.?|m\.p\.h\.?|ms|ma|msw)\s*$', n): return "Graduate"
    if 'certificate' in n: return "Certificate"
    if 'minor' in n: return "Minor"
    return "Undergraduate"

def clean(raw):
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
    if not re.search(r'\b(bachelor|master|doctor|associate|certificate|b\.s\.|b\.a\.|b\.f\.a\.|b\.m\.|b\.b\.a\.|m\.s\.|m\.a\.|mba|m\.ed\.|mfa|m\.arch\.|ph\.d|phd|ed\.d|j\.d|m\.d|pharm\.d|baas|bsw|bset|bas)\b', raw, re.I): return False
    return True

SKIP_EXACT = {'degree requirements','general university requirements','university core curriculum'}

def extract_programs(soup, base_url):
    programs, seen = [], set()
    for a in soup.find_all('a', href=re.compile(r'preview_program\.php')):
        raw = a.get_text(strip=True)
        if not raw or len(raw) < 4: continue
        rl = raw.lower().strip()
        if rl in SKIP_EXACT: continue
        if any(rl.endswith(s) for s in ['degree requirements']): continue
        if not is_real_degree(raw): continue
        if rl in seen: continue
        seen.add(rl)
        level = get_level(raw)
        if level == "Minor": continue
        name = clean(raw)
        if not name or len(name) < 4: continue
        programs.append({
            "name": name, "degree_level": level, "category": get_category(name),
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
        if len(best) >= 80:
            break
    return best, best_url

# ── ONLY scrape the 2 current catalogs ────────────────────────
print("="*60)
print("Fetching catalog.unt.edu...")
soup, _ = fetch("https://catalog.unt.edu")

# Find only the CURRENT catalogs (not archived)
current_catalogs = []
for opt in soup.find_all('option'):
    val  = opt.get('value','')
    text = opt.get_text(strip=True)
    if not val or not val.isdigit(): continue
    # Skip archived catalogs and non-catalog entries
    if 'ARCHIVED' in text.upper(): continue
    if any(x in text.lower() for x in ['courses', 'academic units', 'other content', 'entire catalog']): continue
    current_catalogs.append((val, text))
    print(f"  catoid={val}: {text}")

print(f"\nScraping {len(current_catalogs)} current catalogs only")

all_programs = []
seen_names = set()

for catoid, cat_name in current_catalogs:
    cat_url = f"https://catalog.unt.edu/index.php?catoid={catoid}"
    print(f"\nScraping: {cat_name}...")
    s, _ = fetch(cat_url)
    if not s: continue
    progs, best_url = best_page(cat_url, s)
    print(f"  Found: {len(progs)} programs")
    for p in progs:
        key = p['name'].lower()
        if key not in seen_names:
            all_programs.append(p)
            seen_names.add(key)

print(f"\n{'='*60}")
print(f"TOTAL: {len(all_programs)} programs")

by_level = {}
for p in all_programs: by_level.setdefault(p['degree_level'],[]).append(p)
for lvl in ["Undergraduate","Graduate","Doctoral","Certificate"]:
    grp = by_level.get(lvl,[])
    if grp:
        print(f"  {lvl}: {len(grp)}")

# Save
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
print(f"\n✅ {ins} programs saved!")
