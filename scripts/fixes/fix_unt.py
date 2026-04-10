"""
fix_unt.py - Directly scrape UNT catalog and save to DB
Run: python fix_unt.py
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
    print(f"  Fetching: {url}")
    r = requests.get(url, headers=HEADERS, timeout=25)
    print(f"  Status: {r.status_code}, Size: {len(r.text)} chars")
    return BeautifulSoup(r.text, "html.parser"), r.text

def get_category(name):
    n = name.lower()
    cats = [
        ("Engineering", ["engineering","mechanical","electrical","civil","chemical","biomedical","aerospace","materials"]),
        ("STEM", ["computer","cybersecurity","data science","mathematics","physics","chemistry","biology","biochemistry","statistics","information"]),
        ("Health & Medicine", ["nursing","pharmacy","public health","kinesiology","physical therapy","occupational","health","rehabilitation","nutrition","behavior analysis","addiction","speech","audiology"]),
        ("Business", ["business","accounting","finance","marketing","management","economics","entrepreneurship","supply chain","hospitality","merchandising","logistics"]),
        ("Education", ["education","teaching","curriculum","counseling","early childhood","literacy","teacher"]),
        ("Law & Policy", ["law","political","criminal justice","public administration","emergency","nonprofit","urban planning","government"]),
        ("Arts & Humanities", ["music","art","design","dance","theatre","film","journalism","media","communication","english","history","philosophy","religion","linguistics","language","creative writing","graphic","fashion","photography","spanish","french","german","japanese"]),
        ("Social Sciences", ["sociology","psychology","anthropology","social work","geography","social science","gender","ethnic","family","international studies"]),
    ]
    for cat, kws in cats:
        if any(kw in n for kw in kws):
            return cat
    return "STEM"

def get_level(raw):
    n = raw.lower()
    if re.search(r'\bph\.?d\.?\b|\bdoctor\b|\bd\.m\.a\b|\bed\.d\b|\bj\.d\b|\bm\.d\b|\bpharm\.d\b', n):
        return "Doctoral"
    if re.search(r'\bmaster\b|,\s*(m\.s|m\.a|mba|m\.ed|mfa|m\.arch|ms|ma)\s*$', n):
        return "Graduate"
    if 'grad track' in n:
        return "Graduate"
    if 'certificate' in n:
        return "Certificate"
    if 'minor' in n:
        return "Minor"
    return "Undergraduate"

def clean(raw):
    name = re.sub(r',\s*(B[A-Z]{1,5}S?|M[A-Z]{1,5}|Ph\.?D\.?|Ed\.?D\.?|J\.D\.?|BAAS|BSW|BSET|BAS|BSPHY)\s*$', '', raw, flags=re.I).strip()
    name = re.sub(r'\s*\(not currently accepting students\)\s*', '', name, flags=re.I)
    name = re.sub(r'\s*\(dual degree[^)]*\)\s*', '', name, flags=re.I)
    return re.sub(r'\s+', ' ', name).strip()

# Step 1: Fetch catalog.unt.edu
print("\n" + "="*60)
print("Fetching catalog.unt.edu...")
soup, html = fetch("https://catalog.unt.edu")

# Step 2: Find all navoid links
navoids = []
seen = set()
for a in soup.find_all('a', href=True):
    if 'navoid=' in a['href']:
        url = urljoin("https://catalog.unt.edu", a['href'])
        if url not in seen:
            seen.add(url)
            navoids.append((a.get_text(strip=True), url))

print(f"\nFound {len(navoids)} navoid links")

# Step 3: Sort - programs pages first
def rank(item):
    t = item[0].lower()
    if 'academic unit' in t or 'programs listed' in t: return 0
    if 'program' in t or 'major' in t or 'degree' in t: return 1
    return 2
navoids.sort(key=rank)

# Step 4: Try each navoid - stop when we find 100+ programs
best_progs = []
best_url = ""
print("\nTrying navoid pages:")
for i, (text, url) in enumerate(navoids):
    print(f"  [{i+1}/{len(navoids)}] {text[:55]}", end=" ... ")
    try:
        psoup, _ = fetch(url)
        links = psoup.find_all('a', href=re.compile(r'preview_program\.php'))
        print(f"{len(links)} programs")
        if len(links) > len(best_progs):
            best_progs = links
            best_url = url
        if len(best_progs) >= 100:
            print(f"\n  *** Found {len(best_progs)} programs on this page - stopping ***")
            break
    except Exception as e:
        print(f"ERROR: {e}")

print(f"\nBest page: {best_url}")
print(f"Programs found: {len(best_progs)}")

# Step 5: Extract programs
programs = []
seen_raw = set()
SKIP = {'degree requirements', 'general university requirements', 'university core curriculum'}
SKIP_ENDS = ['degree requirements']

for a in best_progs:
    raw = a.get_text(strip=True)
    if not raw or len(raw) < 4: continue
    if raw.lower().strip() in SKIP: continue
    if any(raw.lower().endswith(s) for s in SKIP_ENDS): continue
    if raw.lower() in seen_raw: continue
    seen_raw.add(raw.lower())
    
    level = get_level(raw)
    if level == "Minor": continue
    
    name = clean(raw)
    if not name: continue
    
    programs.append({
        "name": name,
        "degree_level": level,
        "category": get_category(name),
        "program_url": urljoin("https://catalog.unt.edu", a['href']),
        "is_featured": "jazz" in name.lower(),
        "top20_rank": 1 if "jazz" in name.lower() else None,
        "reputation_note": "UNT Jazz Studies is ranked top-5 nationally" if "jazz" in name.lower() else None,
    })

print(f"\nExtracted {len(programs)} programs after filtering")

# Step 6: Show sample
by_level = {}
for p in programs:
    by_level.setdefault(p['degree_level'], []).append(p)
for level in ["Undergraduate", "Graduate", "Doctoral", "Certificate"]:
    grp = by_level.get(level, [])
    if grp:
        print(f"\n{level} ({len(grp)}):")
        for p in sorted(grp, key=lambda x: x['name'])[:10]:
            star = "⭐" if p['is_featured'] else "  "
            print(f"  {star} {p['name']}")
        if len(grp) > 10:
            print(f"  ... and {len(grp)-10} more")

# Step 7: Save to DB
print("\n" + "="*60)
print("Saving to database...")
conn = psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
cur = conn.cursor()

# Get UNT id
cur.execute("SELECT id FROM universities WHERE name = 'University of North Texas'")
row = cur.fetchone()
if not row:
    print("ERROR: UNT not found in database!")
    exit()
uid = row['id']
print(f"UNT database ID: {uid}")

# Clear old programs
cur.execute("DELETE FROM university_programs WHERE university_id = %s", (uid,))
print(f"Cleared old programs")

# Insert new ones
ins = 0
for p in programs:
    cur.execute("""
        INSERT INTO university_programs
            (university_id,name,category,degree_level,description,
             is_featured,top20_rank,reputation_note,program_url,generated_by)
        VALUES (%s,%s,%s,%s,NULL,%s,%s,%s,%s,'catalog')
        ON CONFLICT DO NOTHING RETURNING id
    """, (uid, p['name'][:200], p['category'][:100], p['degree_level'][:50],
          p['is_featured'], p.get('top20_rank'), p.get('reputation_note'), p['program_url']))
    if cur.fetchone(): ins += 1

conn.commit()
conn.close()
print(f"\n✅ Done! {ins} programs saved for University of North Texas")
print("Refresh the UNT page on your website to see them.")
