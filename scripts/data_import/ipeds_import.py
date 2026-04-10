"""
UniSearch — IPEDS Program Importer (fixed column names)

USAGE:
  python ipeds_import.py --name "north texas" --preview
  python ipeds_import.py --id 3087 --preview
  python ipeds_import.py --all
  python ipeds_import.py --all --reset
  python ipeds_import.py --all --skip-existing
"""

from pathlib import Path as _Path
import sys as _sys
_sys.path.insert(0, str(_Path(__file__).parent))
from db_config import DB_CONFIG, get_conn


import os, re, csv, json, io, zipfile, argparse
import requests
import psycopg2, psycopg2.extras

IPEDS_COMPLETIONS_URL = "https://nces.ed.gov/ipeds/datacenter/data/C2023_A.zip"
IPEDS_DIRECTORY_URL   = "https://nces.ed.gov/ipeds/datacenter/data/HD2023.zip"
CACHE_COMPLETIONS     = "ipeds_completions.csv"
CACHE_DIRECTORY       = "ipeds_directory.csv"

# ── CIP 2-digit → Category ────────────────────────────────────────
CIP_TO_CATEGORY = {
    "01":"STEM","03":"STEM","04":"Arts & Humanities","05":"Social Sciences",
    "09":"Arts & Humanities","10":"Arts & Humanities","11":"STEM",
    "12":"Business","13":"Education","14":"Engineering","15":"Engineering",
    "16":"Arts & Humanities","19":"Social Sciences","22":"Law & Policy",
    "23":"Arts & Humanities","26":"STEM","27":"STEM","30":"Social Sciences",
    "38":"Arts & Humanities","39":"Arts & Humanities","40":"STEM","41":"STEM",
    "42":"Social Sciences","43":"Law & Policy","44":"Law & Policy",
    "45":"Social Sciences","50":"Arts & Humanities","51":"Health & Medicine",
    "52":"Business","54":"Arts & Humanities","60":"Health & Medicine",
}

# ── Award level → Degree type ─────────────────────────────────────
AWARD_LEVEL = {
    "1":"Certificate","2":"Certificate","3":"Undergraduate","4":"Certificate",
    "5":"Undergraduate","6":"Certificate","7":"Graduate","8":"Certificate",
    "17":"Doctoral","18":"Doctoral","19":"Doctoral",
}

# ── CIP 6-digit code → Human readable title ──────────────────────
# Official NCES CIP 2020 titles (abbreviated list of most common)
CIP_TITLES = {
    "11.0101":"Computer and Information Sciences","11.0201":"Computer Programming",
    "11.0401":"Information Science and Systems","11.0501":"Computer Systems Analysis",
    "11.0701":"Computer Science","11.0801":"Web and Digital Media Design",
    "11.0901":"Computer Networking and Telecommunications",
    "11.1001":"Network and System Administration",
    "11.1003":"Cybersecurity and Information Assurance",
    "11.1006":"Data Analytics","11.1099":"Computer and Information Sciences",
    "14.0101":"Engineering","14.0201":"Aerospace Engineering",
    "14.0401":"Architectural Engineering","14.0501":"Biomedical Engineering",
    "14.0701":"Chemical Engineering","14.0801":"Civil Engineering",
    "14.0901":"Computer Engineering","14.1001":"Electrical Engineering",
    "14.1101":"Engineering Mechanics","14.1201":"Engineering Physics",
    "14.1301":"Environmental Engineering","14.1401":"Industrial Engineering",
    "14.1501":"Materials Engineering","14.1601":"Mechanical Engineering",
    "14.1901":"Naval Architecture and Marine Engineering",
    "14.2001":"Nuclear Engineering","14.2101":"Petroleum Engineering",
    "14.2201":"Systems Engineering","14.3201":"Polymer Engineering",
    "14.3901":"Mechatronics and Robotics Engineering",
    "14.4001":"Biological and Agricultural Engineering",
    "15.0000":"Engineering Technology","15.0201":"Civil Engineering Technology",
    "15.0303":"Electrical Engineering Technology",
    "15.0805":"Mechanical Engineering Technology",
    "52.0101":"Business","52.0201":"Business Administration and Management",
    "52.0301":"Accounting","52.0302":"Accounting Technology",
    "52.0601":"Business Economics","52.0701":"Entrepreneurship",
    "52.0801":"Finance","52.0803":"Banking and Financial Services",
    "52.0901":"Hospitality Management","52.0903":"Tourism Management",
    "52.1001":"Human Resources Management",
    "52.1101":"International Business",
    "52.1201":"Management Information Systems",
    "52.1202":"Business Analytics","52.1301":"Management Science",
    "52.1401":"Marketing","52.1501":"Real Estate",
    "52.2001":"Construction Management",
    "51.0000":"Health Professions","51.0401":"Dentistry",
    "51.0501":"Dental Hygiene","51.0701":"Health Administration",
    "51.0904":"Emergency Medical Technology","51.0907":"Radiologic Technology",
    "51.0908":"Respiratory Therapy","51.1004":"Medical Laboratory Technology",
    "51.1201":"Medicine","51.2001":"Pharmacy",
    "51.2207":"Public Health","51.2301":"Athletic Training",
    "51.2302":"Physical Therapy","51.2304":"Occupational Therapy",
    "51.2306":"Kinesiology and Exercise Science",
    "51.2401":"Veterinary Medicine",
    "51.3801":"Nursing","51.3818":"Nursing Practice",
    "13.0101":"Education","13.0301":"Curriculum and Instruction",
    "13.0401":"Educational Leadership and Administration",
    "13.0601":"Educational Technology",
    "13.1001":"Special Education","13.1101":"School Counseling",
    "13.1204":"Early Childhood Education",
    "13.1205":"Elementary Education","13.1206":"Middle School Education",
    "13.1302":"Art Education","13.1305":"English Education",
    "13.1311":"Mathematics Education","13.1312":"Music Education",
    "13.1314":"Physical Education","13.1316":"Science Education",
    "13.1318":"Social Studies Education",
    "22.0101":"Law","22.0301":"Legal Studies","22.0302":"Paralegal Studies",
    "23.0101":"English Language and Literature",
    "23.0201":"Creative Writing",
    "26.0101":"Biology","26.0202":"Biochemistry",
    "26.0204":"Molecular Biology","26.0501":"Microbiology",
    "26.0801":"Genetics","26.0901":"Physiology",
    "26.1201":"Biotechnology","26.1301":"Ecology",
    "26.1302":"Marine Biology","26.1501":"Neuroscience",
    "27.0101":"Mathematics","27.0301":"Applied Mathematics",
    "27.0501":"Statistics",
    "40.0501":"Chemistry","40.0502":"Analytical Chemistry",
    "40.0504":"Organic Chemistry","40.0601":"Geology",
    "40.0801":"Physics",
    "42.0101":"Psychology","42.0201":"Clinical Psychology",
    "42.0204":"Counseling Psychology",
    "42.0207":"Industrial and Organizational Psychology",
    "42.0601":"Cognitive Science",
    "45.0101":"Social Sciences","45.0201":"Anthropology",
    "45.0401":"Criminology","45.0601":"Economics",
    "45.0701":"Geography","45.0801":"History",
    "45.0901":"International Relations",
    "45.1001":"Political Science","45.1101":"Sociology",
    "45.1201":"Urban Studies",
    "44.0101":"Public Administration","44.0401":"Public Policy",
    "44.0701":"Social Work",
    "50.0301":"Dance","50.0402":"Commercial Art and Advertising",
    "50.0404":"Industrial and Product Design",
    "50.0409":"Graphic Design","50.0411":"Game and Interactive Media Design",
    "50.0501":"Theatre Arts","50.0601":"Film and Cinema Studies",
    "50.0602":"Film and Video Production",
    "50.0605":"Photography","50.0702":"Fine Arts",
    "50.0703":"Art History","50.0901":"Music",
    "50.0903":"Music Performance","50.0904":"Music Theory and Composition",
    "50.0910":"Jazz Studies","50.0913":"Music Technology",
    "09.0101":"Communication","09.0401":"Journalism",
    "09.0702":"Digital Media","09.1001":"Advertising",
    "09.1002":"Public Relations",
    "16.0101":"Linguistics","16.0102":"Foreign Languages",
    "38.0101":"Philosophy","38.0201":"Religious Studies",
    "38.0206":"Theology",
    "54.0101":"History",
    "30.0000":"Interdisciplinary Studies",
    "30.0101":"Biological and Physical Sciences",
    "30.0801":"Mathematics and Computer Science",
    "30.1001":"Biopsychology",
    "30.1701":"Behavioral Sciences",
    "30.2301":"Intercultural and International Studies",
    "30.9999":"Interdisciplinary Studies",
    "01.0101":"Agricultural Business and Management",
    "01.0901":"Animal Sciences","01.1001":"Food Science",
    "01.1101":"Plant Sciences","01.1201":"Soil Sciences",
    "19.0101":"Family and Consumer Sciences",
    "19.0201":"Family Studies and Human Development",
    "19.0701":"Human Nutrition",
    "24.0101":"Liberal Arts and Sciences",
    "24.0102":"General Studies",
    "31.0101":"Parks, Recreation and Leisure Studies",
    "31.0301":"Parks, Recreation and Leisure Facilities Management",
    "31.0501":"Health and Physical Education",
}


def cip_title_lookup(cip_code: str) -> str:
    """Get title from our lookup, or generate a readable fallback from the code."""
    # Normalize: ensure format like "11.0701"
    c = cip_code.strip()
    if "." not in c:
        if len(c) == 6:
            c = f"{c[:2]}.{c[2:]}"
    title = CIP_TITLES.get(c)
    if title:
        return title
    # Try 4-digit prefix (e.g. "11.07" → look for "11.0701")
    prefix4 = c[:5] if len(c) >= 5 else c
    for key, val in CIP_TITLES.items():
        if key.startswith(prefix4):
            return val
    # Try 2-digit prefix
    prefix2 = c[:2]
    for key, val in CIP_TITLES.items():
        if key.startswith(prefix2 + ".01"):  # take the "general" version
            return val
    return ""


def build_program_name(cip_code: str, award_level: str) -> str:
    title = cip_title_lookup(cip_code)
    if not title:
        return ""
    title = re.sub(r",\s*(general|other|nos)$", "", title, flags=re.IGNORECASE).strip()
    level = str(award_level)
    cip2  = cip_code[:2] if len(cip_code) >= 2 else ""
    tl    = title.lower()

    # Doctoral
    if level in ("17","18","19"):
        if "law" in tl:                     return "Juris Doctor"
        if "medicine" in tl:                return "Doctor of Medicine"
        if "pharmacy" in tl:                return "Doctor of Pharmacy"
        if "dental" in tl:                  return "Doctor of Dental Surgery"
        if "veterinary" in tl:              return "Doctor of Veterinary Medicine"
        if "physical therapy" in tl:        return "Doctor of Physical Therapy"
        if "nursing practice" in tl:        return "Doctor of Nursing Practice"
        if "education" in tl:               return "Doctor of Education"
        if "public health" in tl:           return "Doctor of Public Health"
        if "optometry" in tl:               return "Doctor of Optometry"
        if "chiropractic" in tl:            return "Doctor of Chiropractic"
        if "podiatric" in tl:               return "Doctor of Podiatric Medicine"
        if "business administration" in tl: return "Doctor of Business Administration"
        return f"Doctor of Philosophy in {title}"

    # Master's
    if level == "7":
        if "business administration" in tl: return "Master of Business Administration"
        if "public health" in tl:           return "Master of Public Health"
        if "public administration" in tl:   return "Master of Public Administration"
        if "fine arts" in tl:               return "Master of Fine Arts"
        if "social work" in tl:             return "Master of Social Work"
        if "library" in tl:                 return "Master of Library Science"
        if "architecture" in tl:            return "Master of Architecture"
        if "music" in tl and "education" not in tl: return f"Master of Music in {title}"
        if "education" in tl:               return f"Master of Education in {title}"
        if "science" in tl and cip2 in ("09","16","23","38","42","45","54"):
            return f"Master of Arts in {title}"
        return f"Master of Science in {title}"

    # Associate's
    if level == "3":
        if cip2 in ("11","14","15","41","47","48","49","46"):
            return f"Associate of Applied Science in {title}"
        return f"Associate of Science in {title}"

    # Bachelor's
    if level == "5":
        humanities = {"04","05","09","10","16","19","22","23","38","39","42","45","50","54"}
        if "fine arts" in tl:     return f"Bachelor of Fine Arts in {title}"
        if cip2 == "50" and "music" in tl: return f"Bachelor of Music in {title}"
        if "architecture" in tl:  return "Bachelor of Architecture"
        if "social work" in tl:   return "Bachelor of Social Work"
        if "nursing" in tl:       return "Bachelor of Science in Nursing"
        if cip2 in humanities:    return f"Bachelor of Arts in {title}"
        return f"Bachelor of Science in {title}"

    # Certificate
    if level in ("1","2","4","6","8"):
        return f"Certificate in {title}"

    return f"Bachelor of Science in {title}"


# ── CSV download + cache ──────────────────────────────────────────

def download_and_cache(url: str, cache_file: str) -> str:
    if os.path.exists(cache_file):
        print(f"  Using cached {cache_file}")
        return cache_file
    print(f"  Downloading {url} ...")
    r = requests.get(url, timeout=120, stream=True)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    chunks, downloaded = [], 0
    for chunk in r.iter_content(65536):
        chunks.append(chunk)
        downloaded += len(chunk)
        if total:
            print(f"    {downloaded*100//total}%", end="\r")
    print()
    z = zipfile.ZipFile(io.BytesIO(b"".join(chunks)))
    csvs = sorted([n for n in z.namelist() if n.lower().endswith(".csv")
                   and "dict" not in n.lower() and "rv" not in n.lower()])
    if not csvs:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
    print(f"  Extracting {csvs[0]}")
    with open(cache_file, "wb") as f:
        f.write(z.read(csvs[0]))
    return cache_file


def fix_key(row: dict, *candidates) -> str:
    """Get value from dict trying multiple key variants (handles BOM, case)."""
    for k in candidates:
        if k in row:
            return row[k]
    # Try stripping BOM from actual keys
    for actual_key in row:
        stripped = actual_key.lstrip('\ufeff').lstrip('ï»¿')
        for k in candidates:
            if stripped.upper() == k.upper():
                return row[actual_key]
    return ""


def load_directory() -> dict:
    path = download_and_cache(IPEDS_DIRECTORY_URL, CACHE_DIRECTORY)
    d = {}
    with open(path, encoding="latin-1") as f:
        reader = csv.DictReader(f)
        # Strip BOM from first header key if present
        # Strip BOM — directory file uses latin-1 with UTF-8 BOM chars
        if reader.fieldnames:
            first = reader.fieldnames[0]
            if first.startswith('﻿') or first.startswith('ï»¿') or first.startswith('ï»¿'):
                reader.fieldnames[0] = "UNITID"
        for row in reader:
            uid_str = fix_key(row, "UNITID", "unitid")
            try:
                uid = int(uid_str)
            except (ValueError, TypeError):
                continue
            d[uid] = {
                "name":  fix_key(row, "INSTNM","instnm").strip(),
                "city":  fix_key(row, "CITY","city").strip(),
                "state": fix_key(row, "STABBR","stabbr").strip(),
                "url":   fix_key(row, "WEBADDR","webaddr").strip(),
            }
    print(f"  Directory: {len(d)} institutions loaded")
    return d


def load_completions() -> dict:
    path = download_and_cache(IPEDS_COMPLETIONS_URL, CACHE_COMPLETIONS)
    comp = {}
    with open(path, encoding="utf-8-sig") as f:  # utf-8-sig strips BOM automatically
        reader = csv.DictReader(f)
        for row in reader:
            uid_str  = fix_key(row, "UNITID","unitid")
            cip      = fix_key(row, "CIPCODE","cipcode").strip()
            awlev    = fix_key(row, "AWLEVEL","awlevel").strip()
            total    = fix_key(row, "CTOTALT","ctotalt").strip()
            majornum = fix_key(row, "MAJORNUM","majornum").strip()

            try:
                uid = int(uid_str)
            except (ValueError, TypeError):
                continue

            if not cip or not awlev:
                continue
            if cip.startswith("99"):
                continue
            # Skip double-majors (MAJORNUM=2) to avoid duplicates
            if majornum == "2":
                continue
            try:
                if int(total) <= 0:
                    continue
            except ValueError:
                pass

            if uid not in comp:
                comp[uid] = []
            comp[uid].append({"cip": cip, "awlevel": awlev})

    print(f"  Completions: {len(comp)} institutions loaded")
    return comp


# ── Name matching ─────────────────────────────────────────────────

def normalize(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[''`]", "", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\b(the|of|at|in|and|a|an)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def find_unitid(name: str, url: str, directory: dict, name_index: dict) -> int | None:
    # 1. Exact normalized match
    key = normalize(name)
    if key in name_index:
        return name_index[key]

    # 2. Strip common suffixes and retry
    cleaned = re.sub(
        r"\s*[-–]\s*(main campus|online|digital immersion|downtown|campus immersion|"
        r"polytechnic|west valley|health science center|at dallas|at houston|"
        r"yuma|yavapai|pima|tucson|mesa|havasu|gila|"
        r"washington d\.?c\.?|northeastern arizona|"
        r"west$|east$|north$|south$|central$)\s*$",
        "", name, flags=re.IGNORECASE).strip()
    if cleaned != name:
        key2 = normalize(cleaned)
        if key2 in name_index:
            return name_index[key2]

    # 3. Substring match — prefer longer match
    best_uid, best_len = None, 0
    for ikey, uid in name_index.items():
        if key in ikey or ikey in key:
            if len(ikey) > best_len:
                best_len, best_uid = len(ikey), uid
    if best_uid:
        return best_uid

    # 4. URL domain match
    if url:
        domain = re.sub(r"^https?://(www\.)?", "", url).split("/")[0].lower()
        if domain:
            for uid, info in directory.items():
                if domain in info.get("url","").lower():
                    return uid
    return None


# ── Build programs for one school ─────────────────────────────────

def build_programs(unitid: int, uni_name: str, completions: dict) -> list:
    raw  = completions.get(unitid, [])
    seen = set()
    progs = []
    for item in raw:
        name = build_program_name(item["cip"], item["awlevel"])
        if not name or name in seen or len(name) < 10:
            continue
        seen.add(name)
        cip2 = item["cip"][:2] if len(item["cip"]) >= 2 else "99"
        progs.append({
            "name":          name,
            "category":      CIP_TO_CATEGORY.get(cip2, "STEM"),
            "degree_level":  AWARD_LEVEL.get(str(item["awlevel"]), "Undergraduate"),
            "description":   None,
            "program_url":   None,
            "is_featured":   False,
            "top20_rank":    None,
            "reputation_note": None,
        })

    # Apply knowledge base featured/top20 data
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
        for p in progs:
            if p["name"].lower() == feat.lower():
                p.update({"is_featured": True, "reputation_note": rep, "program_url": purl})
                found = True
                break
        if not found:
            progs.insert(0, {
                "name": feat,
                "category": CIP_TO_CATEGORY.get("50","Arts & Humanities"),
                "degree_level": "Undergraduate", "description": None,
                "program_url": purl, "is_featured": True, "top20_rank": 1,
                "reputation_note": rep,
            })
        for rank, pname in enumerate(top20, 1):
            for p in progs:
                if p["name"].lower() == pname.lower():
                    p["top20_rank"] = rank
                    break
        break

    if progs and not any(p["is_featured"] for p in progs):
        progs[0]["is_featured"] = True

    return progs


# ── Database helpers ──────────────────────────────────────────────



def get_all_universities(skip_existing=False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            if skip_existing:
                cur.execute("""
                    SELECT u.id, u.name, u.school_url FROM universities u
                    WHERE NOT EXISTS (
                        SELECT 1 FROM university_programs p WHERE p.university_id=u.id
                    ) ORDER BY u.name
                """)
            else:
                cur.execute("""
                    SELECT u.id, u.name, u.school_url, COUNT(p.id) AS cnt
                    FROM universities u
                    LEFT JOIN university_programs p ON p.university_id=u.id
                    GROUP BY u.id, u.name, u.school_url ORDER BY u.name
                """)
            return [dict(r) for r in cur.fetchall()]


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
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'ipeds')
                    ON CONFLICT DO NOTHING RETURNING id
                """, (uid, p["name"][:200], p["category"][:100], p["degree_level"][:50],
                      None, p["is_featured"], p.get("top20_rank"),
                      p.get("reputation_note"), p.get("program_url")))
                if cur.fetchone(): ins += 1
                else: skp += 1
            conn.commit()
    return ins, skp


# ── Main logic ────────────────────────────────────────────────────

_DATA = {}

def load_data():
    if _DATA:
        return _DATA
    print("\n📥 Loading IPEDS data (cached after first run)...")
    directory   = load_directory()
    completions = load_completions()
    name_index  = {normalize(info["name"]): uid for uid, info in directory.items()}
    _DATA.update({"directory": directory, "completions": completions, "name_index": name_index})
    return _DATA


def run_one(u, data, reset=False, preview=False):
    unitid  = find_unitid(u["name"], u.get("school_url",""),
                          data["directory"], data["name_index"])
    if not unitid:
        return None, 0, 0

    matched = data["directory"].get(unitid,{}).get("name","?")
    progs   = build_programs(unitid, u["name"], data["completions"])

    if preview:
        print(f"\n  {u['name']}")
        print(f"  IPEDS match : {matched}  (unitid={unitid})")
        print(f"  Programs    : {len(progs)}\n")
        for p in sorted(progs, key=lambda x: (x["degree_level"], x["name"])):
            f = "⭐" if p["is_featured"] else "  "
            r = f"#{p['top20_rank']}" if p["top20_rank"] else "   "
            print(f"  {f} {r:<4}  [{p['degree_level']:<14}]  {p['name']}")
        return matched, len(progs), 0

    ins, skp = save_programs(u["id"], progs, reset=reset)
    return matched, len(progs), ins


def main():
    ap = argparse.ArgumentParser(description="UniSearch IPEDS Importer", epilog=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all",           action="store_true")
    ap.add_argument("--id",            type=int)
    ap.add_argument("--name",          type=str)
    ap.add_argument("--reset",         action="store_true")
    ap.add_argument("--preview",       action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    if not any([args.all, args.id, args.name]):
        ap.print_help(); return

    data = load_data()

    if args.id or args.name:
        unis = get_all_universities()
        matches = [u for u in unis if u["id"] == args.id] if args.id \
                  else [u for u in unis if args.name.lower() in u["name"].lower()]
        if not matches:
            print("No match found."); return
        if len(matches) > 1 and not args.id:
            print("Multiple matches — use --id:")
            for u in matches: print(f"  {u['id']}: {u['name']}")
            return
        u = matches[0]
        matched, total, ins = run_one(u, data, reset=args.reset, preview=args.preview)
        if not matched:
            print(f"\n  Could not match '{u['name']}' in IPEDS data")
        elif not args.preview:
            print(f"\n  Matched : {matched}")
            print(f"  Programs: {total} found, {ins} inserted")
        return

    if args.all:
        unis    = get_all_universities(skip_existing=args.skip_existing)
        total_u = len(unis)
        done = ins_total = not_found = 0
        print(f"\n{'='*65}")
        print(f"  IPEDS Import — {total_u} universities")
        print(f"{'='*65}\n")
        results = []
        for u in unis:
            done += 1
            matched, prog_cnt, ins = run_one(u, data, reset=args.reset)
            ins_total += ins
            if not matched:
                not_found += 1
                print(f"  ❓ [{done}/{total_u}] {u['name'][:60]}")
            else:
                icon = "✅" if prog_cnt > 5 else "⚠️ "
                print(f"  {icon} [{done}/{total_u}] {u['name'][:55]:<55} {prog_cnt:>4} programs")
            results.append({"name": u["name"], "matched": matched,
                            "programs": prog_cnt, "inserted": ins})

        print(f"\n{'='*65}")
        print(f"  ✅ COMPLETE  |  Inserted: {ins_total}  |  Not matched: {not_found}")
        print(f"{'='*65}")
        with open("ipeds_log.json","w") as f:
            json.dump(results, f, indent=2)
        print(f"  Log: ipeds_log.json\n")


if __name__ == "__main__":
    main()
