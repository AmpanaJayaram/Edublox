"""
UNT Catalog Importer
Scrapes the full program list from catalog.unt.edu and saves to DB.
Usage: python unt_catalog_import.py --preview
       python unt_catalog_import.py --save
"""
import re, argparse, requests, psycopg2, psycopg2.extras
from bs4 import BeautifulSoup

DB_CONFIG = {
    "dbname": "unisearch", "user": "postgres",
    "password": "2000", "host": "localhost", "port": 5432,
}
UNT_DB_ID  = 3087
CATALOG_URL = "https://catalog.unt.edu/content.php?catoid=37&navoid=4293"
BASE_URL    = "https://catalog.unt.edu"

# Section headers to skip (not real programs)
SKIP_SECTIONS = {
    "majors", "minors", "requirements", "undergraduate academic certificates",
    "grad track options", "dual degrees", "pre-majors", "secondary teacher certification",
    "all level teacher certification", "certificates", "preprofessional studies",
}

# Map degree suffix → degree_level + category
DEGREE_MAP = [
    # Doctoral
    (r"\bphd\b|\bph\.d\b",                         "Doctoral",      None),
    # Graduate
    (r"\bms\b|\bm\.s\b|\bma\b|\bm\.a\b|\bmba\b|\bm\.ed\b|\bmfa\b|\bm\.arch\b", "Graduate", None),
    # Undergraduate BFA/BM/BBA/BS/BA/BAAS/BSW/BSET etc
    (r"\bbfa\b|\bbm\b|\bbba\b|\bbs\b|\bba\b|\bbaas\b|\bbsw\b|\bbset\b|\bbsbc\b|\bbsbio\b|\bbschm\b|\bbsmth\b|\bbsmls\b|\bbas\b|\bbseco\b|\bbsphy\b",
                                                   "Undergraduate", None),
    # Certificate
    (r"\bcertificate\b|\bcert\b",                   "Certificate",   None),
    # Minor → skip (don't include minors as programs)
    (r"\bminor\b",                                  "SKIP",          None),
]

CIP_CATEGORY = {
    "music":"Arts & Humanities","art":"Arts & Humanities","design":"Arts & Humanities",
    "dance":"Arts & Humanities","theatre":"Arts & Humanities","film":"Arts & Humanities",
    "journalism":"Arts & Humanities","media":"Arts & Humanities","communication":"Arts & Humanities",
    "english":"Arts & Humanities","history":"Arts & Humanities","philosophy":"Arts & Humanities",
    "religion":"Arts & Humanities","french":"Arts & Humanities","spanish":"Arts & Humanities",
    "german":"Arts & Humanities","japanese":"Arts & Humanities","linguistics":"Arts & Humanities",
    "business":"Business","accounting":"Business","finance":"Business","marketing":"Business",
    "management":"Business","economics":"Business","entrepreneurship":"Business",
    "supply chain":"Business","merchandising":"Business","hospitality":"Business","tourism":"Business",
    "real estate":"Business","insurance":"Business","analytics":"Business",
    "engineering":"Engineering","mechanical":"Engineering","electrical":"Engineering",
    "computer engineering":"Engineering","biomedical":"Engineering","materials":"Engineering",
    "construction":"Engineering","electromechanical":"Engineering",
    "computer science":"STEM","cybersecurity":"STEM","information":"STEM",
    "data science":"STEM","mathematics":"STEM","physics":"STEM","chemistry":"STEM",
    "biology":"STEM","biochemistry":"STEM","geography":"STEM","ecology":"STEM",
    "geology":"STEM","neuroscience":"STEM","statistics":"STEM",
    "education":"Education","teaching":"Education","counseling":"Education","learning":"Education",
    "kinesiology":"Health & Medicine","public health":"Health & Medicine","nursing":"Health & Medicine",
    "rehabilitation":"Health & Medicine","health":"Health & Medicine","audiology":"Health & Medicine",
    "behavior analysis":"Health & Medicine","addiction":"Health & Medicine",
    "criminal justice":"Law & Policy","emergency":"Law & Policy","public administration":"Law & Policy",
    "political science":"Law & Policy","international studies":"Law & Policy","law":"Law & Policy",
    "social work":"Social Sciences","sociology":"Social Sciences","psychology":"Social Sciences",
    "anthropology":"Social Sciences","social science":"Social Sciences",
    "urban":"Social Sciences","nonprofit":"Social Sciences",
}

def get_category(name: str) -> str:
    n = name.lower()
    for kw, cat in CIP_CATEGORY.items():
        if kw in n:
            return cat
    return "STEM"

def get_degree_level(name: str) -> str:
    n = name.lower()
    for pattern, level, _ in DEGREE_MAP:
        if re.search(pattern, n):
            return level
    return "Undergraduate"

def clean_name(raw: str) -> str:
    """Convert catalog name like 'Jazz Studies (instrumental), BM' to clean display name."""
    # Remove trailing degree abbreviations
    name = re.sub(r',\s*(B[A-Z]{1,4}|M[A-Z]{1,4}|Ph\.?D\.?|Ed\.?D\.?)\s*$', '', raw, flags=re.IGNORECASE).strip()
    # Remove "(not currently accepting students)" etc
    name = re.sub(r'\(not currently accepting students\)', '', name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def scrape_unt_programs():
    print("Fetching UNT catalog...")
    r = requests.get(CATALOG_URL, timeout=30,
                     headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    programs = []
    seen_names = set()

    # Find all program links: <a href="preview_program.php?catoid=37&poid=XXXX">Name</a>
    for a in soup.find_all("a", href=re.compile(r"preview_program\.php")):
        raw_name = a.get_text(strip=True)
        href = a.get("href","")

        if not raw_name or not href:
            continue

        # Skip section headers and requirement links
        name_lower = raw_name.lower()
        if name_lower in SKIP_SECTIONS:
            continue
        if "degree requirements" in name_lower:
            continue
        if "general university requirements" in name_lower:
            continue
        if "university core curriculum" in name_lower:
            continue
        if "honors courses" in name_lower:
            continue

        # Build full URL
        prog_url = BASE_URL + "/" + href.lstrip("/")

        # Get degree level from name suffix
        degree_level = get_degree_level(raw_name)

        # Skip minors — they're not degree programs
        if degree_level == "SKIP":
            continue

        # Clean the display name
        display_name = clean_name(raw_name)
        if not display_name or len(display_name) < 5:
            continue

        # Skip duplicates
        if display_name.lower() in seen_names:
            continue
        seen_names.add(display_name.lower())

        category = get_category(display_name)

        programs.append({
            "name":          display_name,
            "degree_level":  degree_level,
            "category":      category,
            "program_url":   prog_url,
            "is_featured":   False,
            "top20_rank":    None,
            "reputation_note": None,
        })

    # Mark featured
    for p in programs:
        if "jazz studies" in p["name"].lower() and p["degree_level"] == "Undergraduate":
            p["is_featured"]     = True
            p["top20_rank"]      = 1
            p["reputation_note"] = "UNT's College of Music is one of the largest in the US. The Jazz Studies program is consistently ranked top-5 nationally."
            break

    return programs

def save_programs(programs: list, reset: bool = True):
    with psycopg2.connect(**DB_CONFIG,
                          cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("DELETE FROM university_programs WHERE university_id=%s", (UNT_DB_ID,))
                print(f"  Cleared existing programs for UNT")
            ins = skp = 0
            for p in programs:
                cur.execute("""
                    INSERT INTO university_programs
                        (university_id,name,category,degree_level,description,
                         is_featured,top20_rank,reputation_note,program_url,generated_by)
                    VALUES (%s,%s,%s,%s,NULL,%s,%s,%s,%s,'catalog')
                    ON CONFLICT DO NOTHING RETURNING id
                """, (UNT_DB_ID, p["name"][:200], p["category"][:100],
                      p["degree_level"][:50], p["is_featured"],
                      p.get("top20_rank"), p.get("reputation_note"),
                      p.get("program_url")))
                if cur.fetchone(): ins += 1
                else: skp += 1
            conn.commit()
    return ins, skp

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preview", action="store_true", help="Show without saving")
    ap.add_argument("--save",    action="store_true", help="Save to database")
    args = ap.parse_args()

    programs = scrape_unt_programs()
    print(f"\nFound {len(programs)} programs from UNT catalog\n")

    by_level = {}
    for p in programs:
        by_level.setdefault(p["degree_level"], []).append(p)

    for level in ["Undergraduate","Graduate","Doctoral","Certificate"]:
        group = by_level.get(level, [])
        if group:
            print(f"  {level} ({len(group)}):")
            for p in sorted(group, key=lambda x: x["name"]):
                f = "⭐" if p["is_featured"] else "  "
                print(f"    {f}  {p['name']}")
            print()

    if args.save:
        ins, skp = save_programs(programs, reset=True)
        print(f"\n✅ Saved: {ins} inserted, {skp} skipped")
    elif not args.preview:
        print("Use --preview to display or --save to save to database")

if __name__ == "__main__":
    main()
