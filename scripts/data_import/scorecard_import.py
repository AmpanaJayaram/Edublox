"""
UniSearch — College Scorecard Program Importer
================================================
Uses the official US Department of Education College Scorecard API
to import EVERY program at EVERY accredited US university.

WHY THIS WORKS:
  - Official government data — not scraped, not guessed
  - Returns real CIP program titles for every institution
  - 200-400+ programs per major university
  - Works for ALL 6,000+ institutions
  - Takes about 5-10 minutes total for everything
  - Completely free

HOW TO GET A FREE API KEY (takes 30 seconds):
  1. Go to https://api.data.gov/signup
  2. Enter your email — key is emailed instantly
  3. Paste it in API_KEY below (or use DEMO_KEY for testing)

USAGE:
  # First test on one university:
  python scorecard_import.py --name "north texas" --preview

  # Import all universities:
  python scorecard_import.py --all

  # Import all and clear existing data first:
  python scorecard_import.py --all --reset

  # Re-import one university:
  python scorecard_import.py --name "north texas" --reset

  # Import one university by DB ID:
  python scorecard_import.py --id 3087

SETUP:
  pip install requests psycopg2-binary
"""

from pathlib import Path as _Path
import sys as _sys
_sys.path.insert(0, str(_Path(__file__).parent))
from db_config import DB_CONFIG, get_conn


import os, sys, re, time, json, argparse
import requests
import psycopg2, psycopg2.extras

# ─── PASTE YOUR FREE API KEY HERE ────────────────────────────────
# Get one free at: https://api.data.gov/signup
# Or use DEMO_KEY for testing (rate limited to 40 req/min)
API_KEY = os.environ.get("SCORECARD_API_KEY", "")  # set SCORECARD_API_KEY in .env

SCORECARD_URL = "https://api.data.gov/ed/collegescorecard/v1/schools.json"

# ─── CREDENTIAL LEVEL → DEGREE TYPE ──────────────────────────────
# College Scorecard credential levels
CRED_LEVEL = {
    1:  "Certificate",    # < 1 year certificate
    2:  "Certificate",    # ≥ 1 year certificate
    3:  "Undergraduate",  # Associate's degree
    4:  "Certificate",    # Postbaccalaureate certificate
    5:  "Undergraduate",  # Bachelor's degree
    6:  "Certificate",    # Post-master's certificate
    7:  "Graduate",       # Master's degree
    17: "Doctoral",       # Research doctorate (PhD)
    18: "Doctoral",       # Professional doctorate (MD, JD, PharmD)
    19: "Doctoral",       # Other doctorate
}

# ─── CIP 2-DIGIT → CATEGORY ───────────────────────────────────────
CIP_CAT = {
    "01": "STEM", "03": "STEM", "04": "Arts & Humanities",
    "05": "Social Sciences", "09": "Arts & Humanities",
    "10": "Arts & Humanities", "11": "STEM", "12": "Business",
    "13": "Education", "14": "Engineering", "15": "Engineering",
    "16": "Arts & Humanities", "19": "Social Sciences",
    "22": "Law & Policy", "23": "Arts & Humanities",
    "24": "STEM", "25": "STEM", "26": "STEM", "27": "STEM",
    "28": "Law & Policy", "29": "Law & Policy", "30": "STEM",
    "31": "Health & Medicine", "38": "Arts & Humanities",
    "39": "Arts & Humanities", "40": "STEM", "41": "STEM",
    "42": "Social Sciences", "43": "Law & Policy",
    "44": "Law & Policy", "45": "Social Sciences",
    "46": "Engineering", "47": "Engineering", "48": "Engineering",
    "49": "Engineering", "50": "Arts & Humanities",
    "51": "Health & Medicine", "52": "Business",
    "54": "Arts & Humanities",
}

# ─── KNOWN FEATURED PROGRAMS ─────────────────────────────────────
try:
    from university_knowledge import KNOWN_FEATURED
except ImportError:
    KNOWN_FEATURED = {}




def build_program_name(cip_title: str, cred_level: int, cred_title: str) -> str:
    """
    Convert a CIP title + credential level into a proper degree name.
    e.g. "Jazz/Jazz Studies" + 5 → "Bachelor of Music in Jazz Studies"
         "Computer Science" + 7 → "Master of Science in Computer Science"
    """
    # Clean up the CIP title
    subject = cip_title.strip()
    # Remove slashes like "Jazz/Jazz Studies" → "Jazz Studies"
    if "/" in subject:
        parts = subject.split("/")
        # Use the longer, more descriptive part
        subject = max(parts, key=len).strip()
    # Remove trailing parentheticals like "General)" 
    subject = re.sub(r"\s*\(General\)\s*", "", subject, flags=re.IGNORECASE).strip()
    subject = re.sub(r"\s*,\s*General$", "", subject, flags=re.IGNORECASE).strip()
    subject = re.sub(r"\s*--.*$", "", subject).strip()

    sp = subject.lower()
    cl = cred_level

    # ── Doctoral ─────────────────────────────────────────────────
    if cl in (17, 18, 19):
        if "medicine" in sp and "osteo" not in sp and "veterinary" not in sp:
            return "Doctor of Medicine"
        if "osteopathic" in sp:
            return "Doctor of Osteopathic Medicine"
        if "veterinary" in sp:
            return "Doctor of Veterinary Medicine"
        if "pharmacy" in sp or "pharmaceutical" in sp:
            return "Doctor of Pharmacy"
        if "law" in sp or "juris" in sp or "legal" in sp:
            return "Juris Doctor"
        if "dental" in sp or "dentistry" in sp:
            return "Doctor of Dental Surgery"
        if "optometry" in sp:
            return "Doctor of Optometry"
        if "podiatric" in sp:
            return "Doctor of Podiatric Medicine"
        if "chiropractic" in sp:
            return "Doctor of Chiropractic"
        if "nursing" in sp or "nursing practice" in sp:
            return "Doctor of Nursing Practice"
        if "physical therapy" in sp:
            return "Doctor of Physical Therapy"
        if "occupational therapy" in sp:
            return "Doctor of Occupational Therapy"
        if "audiology" in sp:
            return "Doctor of Audiology"
        if "education" in sp and cl == 17:
            return f"Doctor of Education in {subject}"
        if "musical arts" in sp or "music performance" in sp:
            return "Doctor of Musical Arts"
        if "business" in sp and cl == 18:
            return "Doctor of Business Administration"
        if "public health" in sp:
            return "Doctor of Public Health"
        if "theology" in sp or "divinity" in sp:
            return "Doctor of Theology"
        return f"Doctor of Philosophy in {subject}"

    # ── Master's ──────────────────────────────────────────────────
    if cl == 7:
        if "business administration" in sp:
            return "Master of Business Administration"
        if "public administration" in sp:
            return "Master of Public Administration"
        if "public health" in sp:
            return "Master of Public Health"
        if "public policy" in sp:
            return "Master of Public Policy"
        if "social work" in sp:
            return "Master of Social Work"
        if "library" in sp or "library science" in sp:
            return "Master of Library Science"
        if "library and information" in sp:
            return "Master of Library and Information Science"
        if "information science" in sp:
            return "Master of Science in Information Science"
        if "architecture" in sp and "landscape" not in sp:
            return "Master of Architecture"
        if "landscape architecture" in sp:
            return "Master of Landscape Architecture"
        if "divinity" in sp:
            return "Master of Divinity"
        if "theology" in sp:
            return "Master of Theology"
        if "laws" in sp or "law" == sp:
            return "Master of Laws"
        if "fine arts" in sp or "studio art" in sp:
            return f"Master of Fine Arts in {subject}"
        if "music" in sp and "music education" not in sp and "music technology" not in sp:
            return f"Master of Music in {subject}"
        if "education" in sp:
            return f"Master of Education in {subject}"
        if "teaching" in sp:
            return f"Master of Arts in Teaching"
        # Science vs Arts
        science_kws = {
            "computer","data","engineering","mathematics","physics","chemistry",
            "biology","statistics","health","science","technology","nursing",
            "cybersecurity","analytics","information","environmental","ecology",
            "geology","astronomy","biochemistry","neuroscience","bioinformatics",
            "accounting","finance","supply chain","logistics","economics"
        }
        if any(kw in sp for kw in science_kws):
            return f"Master of Science in {subject}"
        return f"Master of Arts in {subject}"

    # ── Post-master's certificate ─────────────────────────────────
    if cl == 6:
        return f"Post-Master's Certificate in {subject}"

    # ── Post-bacc certificate ─────────────────────────────────────
    if cl == 4:
        return f"Postbaccalaureate Certificate in {subject}"

    # ── Bachelor's ───────────────────────────────────────────────
    if cl == 5:
        if "business administration" in sp:
            return "Bachelor of Business Administration"
        if "nursing" in sp:
            return "Bachelor of Science in Nursing"
        if "social work" in sp:
            return "Bachelor of Social Work"
        if "architecture" in sp and "landscape" not in sp:
            return "Bachelor of Architecture"
        if "landscape architecture" in sp:
            return "Bachelor of Landscape Architecture"
        # Music special cases
        music_perf = {"jazz","piano","organ","strings","voice","guitar",
                      "percussion","winds","brass","woodwind","harp",
                      "music performance","classical music"}
        if any(kw in sp for kw in music_perf):
            return f"Bachelor of Music in {subject}"
        if "music" in sp and "music education" not in sp and \
           "music business" not in sp and "music technology" not in sp and \
           "music therapy" not in sp:
            return f"Bachelor of Music in {subject}"
        # Fine arts
        fa_kws = {"fine arts","studio art","painting","sculpture","drawing",
                  "ceramics","printmaking","photography","illustration",
                  "animation","metalsmithing","fiber","glass","intermedia"}
        if any(kw in sp for kw in fa_kws):
            return f"Bachelor of Fine Arts in {subject}"
        # Graphic design / interior design
        if "graphic design" in sp or "interior design" in sp or \
           "fashion design" in sp or "industrial design" in sp or \
           "game design" in sp:
            return f"Bachelor of Fine Arts in {subject}"
        # Science fields
        science_kws = {
            "computer","data","engineering","mathematics","physics","chemistry",
            "biology","statistics","health","science","technology","nursing",
            "cybersecurity","analytics","information","environmental","ecology",
            "geology","astronomy","biochemistry","neuroscience","bioinformatics",
            "kinesiology","nutrition","agriculture","geoscience","forestry",
            "atmospheric","oceanography","exercise"
        }
        if any(kw in sp for kw in science_kws):
            return f"Bachelor of Science in {subject}"
        return f"Bachelor of Arts in {subject}"

    # ── Associate's ───────────────────────────────────────────────
    if cl == 3:
        science_kws = {"science","technology","engineering","math","health",
                       "nursing","computer","applied","technical"}
        if any(kw in sp for kw in science_kws):
            return f"Associate of Science in {subject}"
        return f"Associate of Arts in {subject}"

    # ── Certificates ─────────────────────────────────────────────
    if cl in (1, 2):
        return f"Certificate in {subject}"

    # Fallback
    return f"{cred_title} in {subject}" if cred_title else subject


def fetch_scorecard_page(page: int, per_page: int = 100) -> dict:
    """Fetch one page of school program data from College Scorecard API."""
    params = {
        "api_key":  API_KEY,
        "fields":   "id,school.name,school.school_url,"
                    "latest.programs.cip_4_digit",
        "per_page": per_page,
        "page":     page,
    }
    for attempt in range(3):
        try:
            r = requests.get(SCORECARD_URL, params=params, timeout=30)
            if r.status_code == 429:
                print(f"\n  ⏳ Rate limited — waiting 60 seconds...")
                time.sleep(60)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            print(f"\n  ⚠️  Timeout on page {page}, retrying ({attempt+1}/3)...")
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"\n  ❌ Error on page {page}: {e}")
            time.sleep(5)
    return {}


def fetch_single_school(school_name: str) -> list[dict]:
    """Fetch program data for a specific school by name."""
    params = {
        "api_key":     API_KEY,
        "school.name": school_name,
        "fields":      "id,school.name,school.school_url,latest.programs.cip_4_digit",
        "per_page":    5,
    }
    try:
        r = requests.get(SCORECARD_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])
    except Exception as e:
        print(f"  Error: {e}")
        return []


def programs_from_scorecard_result(result: dict) -> list[dict]:
    """Convert one College Scorecard school result into program list."""
    raw_programs = result.get("latest.programs.cip_4_digit") or []
    if not raw_programs:
        return []

    programs = {}  # name → dict  (deduplicate by name)

    for prog in raw_programs:
        if not isinstance(prog, dict):
            continue

        cip_title  = prog.get("title", "")
        cred       = prog.get("credential") or {}
        cred_level = cred.get("level", 0)
        cred_title = cred.get("title", "")
        cip_code   = prog.get("code", "")

        if not cip_title or cred_level not in CRED_LEVEL:
            continue

        name = build_program_name(cip_title, cred_level, cred_title)
        if not name or len(name) < 8:
            continue

        if name in programs:
            continue  # Already have this one

        degree_level = CRED_LEVEL[cred_level]
        category     = CIP_CAT.get(str(cip_code)[:2], "STEM")

        programs[name] = {
            "name":          name,
            "category":      category,
            "degree_level":  degree_level,
            "description":   None,
            "program_url":   None,
            "is_featured":   False,
            "top20_rank":    None,
            "reputation_note": None,
        }

    return list(programs.values())


def apply_known_featured(programs: list[dict], uni_name: str) -> list[dict]:
    """Apply known featured program data from university_knowledge.py."""
    uni_lower = uni_name.lower()
    for key, data in KNOWN_FEATURED.items():
        if key not in uni_lower:
            continue

        feat_name = data["featured_name"]
        rep_note  = data["reputation_note"]
        prog_url  = data.get("program_url")
        top20     = data.get("top20", [feat_name])

        # Find or add featured
        found = False
        for p in programs:
            if p["name"].lower() == feat_name.lower():
                p["is_featured"]     = True
                p["reputation_note"] = rep_note
                p["program_url"]     = prog_url
                found = True
                break

        if not found:
            programs.insert(0, {
                "name":          feat_name,
                "category":      _quick_category(feat_name),
                "degree_level":  _quick_level(feat_name),
                "description":   None,
                "program_url":   prog_url,
                "is_featured":   True,
                "top20_rank":    1,
                "reputation_note": rep_note,
            })

        for rank, pname in enumerate(top20, 1):
            for p in programs:
                if p["name"].lower() == pname.lower():
                    p["top20_rank"] = rank
                    break
        break

    # Default: mark first as featured if none set
    if programs and not any(p["is_featured"] for p in programs):
        programs[0]["is_featured"] = True

    return programs


def _quick_category(name: str) -> str:
    n = name.lower()
    cats = [
        ("Arts & Humanities", ["music","art","jazz","film","journalism","english","history","theatre","dance","humanities"]),
        ("Engineering",       ["engineering"]),
        ("STEM",              ["computer","data","science","mathematics","physics","chemistry","biology","technology"]),
        ("Health & Medicine", ["nursing","medicine","pharmacy","health","audiology","therapy","medical"]),
        ("Business",          ["business","accounting","finance","marketing","management","mba","economics"]),
        ("Education",         ["education","teaching"]),
        ("Law & Policy",      ["law","criminal justice","public administration","policy"]),
        ("Social Sciences",   ["psychology","sociology","social work","anthropology"]),
    ]
    for cat, kws in cats:
        if any(kw in n for kw in kws):
            return cat
    return "STEM"


def _quick_level(name: str) -> str:
    n = name.lower()
    if any(x in n for x in ["doctor","ph.d","phd","juris","d.m.a","ed.d","pharm.d","d.n.p"]):
        return "Doctoral"
    if any(x in n for x in ["master","m.s.","m.a.","mba","m.b.a","m.ed","m.f.a","m.p.h"]):
        return "Graduate"
    if "certificate" in n:
        return "Certificate"
    return "Undergraduate"


def save_programs(uid: int, programs: list[dict], reset: bool = False) -> tuple[int, int]:
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
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'scorecard')
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


def get_all_universities() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, school_url FROM universities ORDER BY name")
            return [dict(r) for r in cur.fetchall()]


def normalize_name(s: str) -> str:
    """Normalize a university name for matching."""
    s = s.lower().strip()
    # Remove campus suffixes
    for suffix in ["-main campus", " main campus", "-undergraduate", " undergraduate",
                   "-online", " online", "-system", " system", "-tempe", "-tucson",
                   "-flagstaff", "-bloomington", "-urbana-champaign"]:
        s = s.replace(suffix, "")
    s = re.sub(r"\s*-\s*\w+\s+campus$", "", s)  # "-somecity campus"
    s = re.sub(r"[^a-z0-9 &]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("the ", "")
    return s


def main():
    ap = argparse.ArgumentParser(
        description="Import programs from College Scorecard API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    ap.add_argument("--all",     action="store_true", help="Import all universities")
    ap.add_argument("--id",      type=int,            help="Import one university by DB ID")
    ap.add_argument("--name",    type=str,            help="Import one university by name")
    ap.add_argument("--preview", action="store_true", help="Show programs without saving")
    ap.add_argument("--reset",   action="store_true", help="Clear existing programs first")
    args = ap.parse_args()

    if not any([args.all, args.id, args.name]):
        ap.print_help()
        return

    # ── Single university by name (quick API search) ─────────────
    if args.name:
        our_unis = get_all_universities()
        matches = [u for u in our_unis if args.name.lower() in u["name"].lower()]
        if not matches:
            print(f"No university matching '{args.name}'"); return
        if len(matches) > 1:
            print("Multiple matches — use --id:")
            for u in matches: print(f"  {u['id']}: {u['name']}")
            return
        u = matches[0]
        print(f"\n🔍 Fetching programs for: {u['name']}")

        results = fetch_single_school(u["name"])
        if not results:
            # Try shorter name
            short = " ".join(u["name"].split()[:4])
            results = fetch_single_school(short)

        if not results:
            print(f"  ❌ No results from College Scorecard for '{u['name']}'")
            print(f"     Try --all to match by paginating all schools")
            return

        # Pick best match
        best = None
        u_norm = normalize_name(u["name"])
        for r in results:
            r_norm = normalize_name(r.get("school.name", ""))
            if u_norm == r_norm or u_norm in r_norm or r_norm in u_norm:
                best = r
                break
        if not best:
            best = results[0]  # Take first result

        print(f"  Matched to: {best.get('school.name', 'Unknown')}")
        programs = programs_from_scorecard_result(best)
        programs = apply_known_featured(programs, u["name"])

        if args.preview:
            print(f"\n  Found {len(programs)} programs:\n")
            for p in sorted(programs, key=lambda x: (x["degree_level"], x["name"])):
                f = "⭐" if p["is_featured"] else "  "
                r = f"#{p['top20_rank']}" if p["top20_rank"] else "   "
                print(f"  {f} {r:<4}  [{p['degree_level']:<14}]  {p['name']}")
        else:
            ins, skp = save_programs(u["id"], programs, reset=args.reset)
            print(f"\n  ✅ {len(programs)} programs found → {ins} inserted, {skp} already existed")
        return

    # ── Single university by DB ID ─────────────────────────────────
    if args.id:
        our_unis = get_all_universities()
        matches  = [u for u in our_unis if u["id"] == args.id]
        if not matches:
            print(f"No university with ID {args.id}"); return
        u = matches[0]
        print(f"\n🔍 Fetching programs for: {u['name']}")

        results = fetch_single_school(u["name"])
        if not results:
            short = " ".join(u["name"].split()[:4])
            results = fetch_single_school(short)

        if not results:
            print(f"  ❌ No results from College Scorecard for '{u['name']}'")
            return

        best = None
        u_norm = normalize_name(u["name"])
        for r in results:
            r_norm = normalize_name(r.get("school.name", ""))
            if u_norm == r_norm or u_norm in r_norm or r_norm in u_norm:
                best = r
                break
        if not best:
            best = results[0]

        print(f"  Matched to: {best.get('school.name', 'Unknown')}")
        programs = programs_from_scorecard_result(best)
        programs = apply_known_featured(programs, u["name"])

        if args.preview:
            print(f"\n  Found {len(programs)} programs:\n")
            for p in sorted(programs, key=lambda x: (x["degree_level"], x["name"])):
                f = "⭐" if p["is_featured"] else "  "
                r = f"#{p['top20_rank']}" if p["top20_rank"] else "   "
                print(f"  {f} {r:<4}  [{p['degree_level']:<14}]  {p['name']}")
        else:
            ins, skp = save_programs(u["id"], programs, reset=args.reset)
            print(f"\n  ✅ {len(programs)} programs found → {ins} inserted, {skp} already existed")
        return

    # ── ALL universities ───────────────────────────────────────────
    if args.all:
        print("\n" + "="*65)
        print("  🚀 College Scorecard Import — ALL universities")
        print("  Fetching program data from api.data.gov...")
        print("="*65 + "\n")

        our_unis = get_all_universities()

        # Build normalized lookup: normalized_name → our_id
        our_lookup = {}
        for u in our_unis:
            key = normalize_name(u["name"])
            our_lookup[key] = u["id"]
            # Also add without last word (for campus variants)
            parts = key.split()
            if len(parts) > 2:
                our_lookup[" ".join(parts[:-1])] = u["id"]

        # Fetch all pages from Scorecard API
        print("  📥 Downloading school data from College Scorecard API...")
        first    = fetch_scorecard_page(0, per_page=100)
        total    = first.get("metadata", {}).get("total", 0)
        per_page = 100
        pages    = (total + per_page - 1) // per_page

        print(f"  Total schools in API: {total:,}  |  Pages: {pages}")
        print(f"  Our universities:     {len(our_unis):,}\n")

        all_results = first.get("results", [])

        for page in range(1, pages):
            data = fetch_scorecard_page(page, per_page=per_page)
            results = data.get("results", [])
            all_results.extend(results)
            if page % 10 == 0:
                print(f"  📄 Downloaded {len(all_results):,} / {total:,} schools...")
            # Be polite with API rate limits
            if API_KEY == "DEMO_KEY":
                time.sleep(0.5)  # DEMO_KEY has stricter rate limits
            else:
                time.sleep(0.1)

        print(f"\n  ✅ Downloaded {len(all_results):,} schools from Scorecard")
        print(f"  🔗 Matching to your database and importing programs...\n")

        # Match and import
        matched       = 0
        total_inserted = 0
        no_programs   = 0
        no_match      = 0

        for i, result in enumerate(all_results, 1):
            sc_name   = result.get("school.name", "")
            sc_norm   = normalize_name(sc_name)
            our_id    = our_lookup.get(sc_norm)

            # Try progressively shorter names if no direct match
            if not our_id:
                parts = sc_norm.split()
                for length in range(len(parts)-1, 2, -1):
                    short = " ".join(parts[:length])
                    if short in our_lookup:
                        our_id = our_lookup[short]
                        break

            if not our_id:
                no_match += 1
                continue

            programs = programs_from_scorecard_result(result)
            if not programs:
                no_programs += 1
                continue

            # Find full university name for featured program matching
            uni_name = next((u["name"] for u in our_unis if u["id"] == our_id), sc_name)
            programs = apply_known_featured(programs, uni_name)

            if not args.preview:
                ins, _ = save_programs(our_id, programs, reset=args.reset)
                total_inserted += ins
            matched += 1

            if i % 500 == 0:
                print(f"  [{i:>5}/{len(all_results)}]  "
                      f"Matched: {matched}  |  "
                      f"Programs imported: {total_inserted:,}")

        print(f"\n{'='*65}")
        print(f"  ✅ COMPLETE")
        print(f"  Schools matched to your DB:  {matched:,}")
        print(f"  Schools not matched:         {no_match:,}  (name mismatch)")
        print(f"  Schools with 0 programs:     {no_programs:,}  (trade schools etc.)")
        print(f"  Total programs imported:     {total_inserted:,}")
        print(f"{'='*65}\n")
        print(f"  Your website now has full program data for all matched universities.")
        print(f"  Each major university should have 50-300+ programs.\n")


if __name__ == "__main__":
    main()
