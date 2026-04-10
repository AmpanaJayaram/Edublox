"""
carnegie_fetcher.py (v5) — Full Carnegie data extraction
=========================================================
Extracts all available Carnegie data from the official 2021 Public Data File:
- Basic classification (R1/R2/etc.) → T1/T2/T3 tier
- Research spending (S&E + Non-S&E R&D in $thousands)
- Research doctorates awarded
- Institution control (Public/Private)
- Special designations (HBCU, HSI, Tribal, Land-grant)
- Faculty count, enrollment, size & setting

SETUP:
1. Download: https://carnegieclassifications.acenet.edu/wp-content/uploads/2023/03/CCIHE2021-PublicData.xlsx
2. Save as CCIHE2021-PublicData.xlsx in your uni folder
3. Run: python carnegie_fetcher.py reset

Usage:
    python carnegie_fetcher.py              # process unmatched only
    python carnegie_fetcher.py reset        # clear all and re-run
    python carnegie_fetcher.py test "University of North Texas" TX
"""

import sys, re, os, json, logging
from datetime import timezone, datetime
import psycopg2, psycopg2.extras, openpyxl

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "unisearch",
    "user":     "postgres",
    "password": "2000",   # ← change to your password
}

CARNEGIE_DATA_FILE = "CCIHE2021-PublicData.xlsx"
CARNEGIE_BASE_URL  = "https://carnegieclassifications.acenet.edu/institution/"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# Correct BASIC2021 codes from Carnegie Values sheet
BASIC_LABELS = {
    -2: "Not classified",
    1:  "Associate's Colleges: High Transfer-High Traditional",
    2:  "Associate's Colleges: High Transfer-Mixed Traditional/Nontraditional",
    3:  "Associate's Colleges: High Transfer-High Nontraditional",
    4:  "Associate's Colleges: Mixed Transfer/Career & Technical-High Traditional",
    5:  "Associate's Colleges: Mixed Transfer/Career & Technical-Mixed Traditional/Nontraditional",
    6:  "Associate's Colleges: Mixed Transfer/Career & Technical-High Nontraditional",
    7:  "Associate's Colleges: High Career & Technical-High Traditional",
    8:  "Associate's Colleges: High Career & Technical-Mixed Traditional/Nontraditional",
    9:  "Associate's Colleges: High Career & Technical-High Nontraditional",
    10: "Special Focus Two-Year: Health Professions",
    11: "Special Focus Two-Year: Technical Professions",
    12: "Special Focus Two-Year: Arts & Design",
    13: "Special Focus Two-Year: Other Fields",
    14: "Baccalaureate/Associate's Colleges: Associate's Dominant",
    15: "Doctoral Universities: Very High Research Activity",   # R1 → T1
    16: "Doctoral Universities: High Research Activity",        # R2 → T2
    17: "Doctoral/Professional Universities",
    18: "Master's Colleges & Universities: Larger Programs",
    19: "Master's Colleges & Universities: Medium Programs",
    20: "Master's Colleges & Universities: Small Programs",
    21: "Baccalaureate Colleges: Arts & Sciences Focus",
    22: "Baccalaureate Colleges: Diverse Fields",
    23: "Baccalaureate/Associate's Colleges: Mixed Baccalaureate/Associate's",
    24: "Special Focus Four-Year: Faith-Related Institutions",
    25: "Special Focus Four-Year: Medical Schools & Centers",
    26: "Special Focus Four-Year: Other Health Professions Schools",
    27: "Special Focus Four-Year: Research Institution",
    28: "Special Focus Four-Year: Engineering and Other Technology-Related Schools",
    29: "Special Focus Four-Year: Business & Management Schools",
    30: "Special Focus Four-Year: Arts, Music & Design Schools",
    31: "Special Focus Four-Year: Law Schools",
    32: "Special Focus Four-Year: Other Special Focus Institutions",
    33: "Tribal Colleges and Universities",
}

CONTROL_LABELS = {1: "Public", 2: "Private not-for-profit", 3: "Private for-profit"}

def assign_tier(basic_code) -> tuple:
    try:
        code = int(basic_code)
    except (TypeError, ValueError):
        return "T3", "Not classified as R1 or R2"
    if code == 15:
        return "T1", "Carnegie R1 — Doctoral Universities: Very High Research Activity"
    elif code == 16:
        return "T2", "Carnegie R2 — Doctoral Universities: High Research Activity"
    else:
        label = BASIC_LABELS.get(code, "Other")
        return "T3", f"Not R1/R2 — {label}"

def normalize(name: str) -> str:
    n = name.lower().strip()
    n = re.sub(r'[^\w\s]', ' ', n)
    return re.sub(r'\s+', ' ', n).strip()

def name_similarity(a: str, b: str) -> float:
    at = set(normalize(a).split()) - {'the', 'of', 'at', 'and', 'for', 'in', 'a'}
    bt = set(normalize(b).split()) - {'the', 'of', 'at', 'and', 'for', 'in', 'a'}
    if not at or not bt:
        return 0.0
    return len(at & bt) / len(at | bt)

def make_slug(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r'[^a-z0-9\s]', '', slug)
    return re.sub(r'\s+', '-', slug).strip('-')

def load_carnegie_data() -> list[dict]:
    if not os.path.exists(CARNEGIE_DATA_FILE):
        log.error(f"File not found: {CARNEGIE_DATA_FILE}")
        log.error("Download from: https://carnegieclassifications.acenet.edu/wp-content/uploads/2023/03/CCIHE2021-PublicData.xlsx")
        return []
    log.info(f"Loading {CARNEGIE_DATA_FILE}...")
    wb = openpyxl.load_workbook(CARNEGIE_DATA_FILE, read_only=True, data_only=True)
    ws = wb['Data']
    rows = list(ws.iter_rows(values_only=True))
    headers = [str(h).lower().strip() if h else "" for h in rows[0]]
    records = [{headers[i]: rows[j][i] for i in range(len(headers))} for j in range(1, len(rows))]
    wb.close()
    log.info(f"  Loaded {len(records):,} records")
    return records

def build_lookup(records):
    by_unitid = {}
    by_name   = []
    for rec in records:
        unitid  = str(rec.get('unitid', '') or '').strip()
        name    = str(rec.get('name',   '') or '').strip()
        state   = str(rec.get('stabbr', '') or '').strip()
        if not name:
            continue

        basic       = rec.get('basic2021')
        serd        = rec.get('serd')        # S&E R&D $thousands
        nonserd     = rec.get('nonserd')     # Non-S&E R&D $thousands
        docrsdeg    = rec.get('docrsdeg')    # Research doctorates
        control     = rec.get('control')
        hbcu        = rec.get('hbcu')
        hsi         = rec.get('hsi')
        tribal      = rec.get('tribal')
        landgrnt    = rec.get('landgrnt')
        facnum      = rec.get('facnum')
        fallenr20   = rec.get('fallenr20')
        rooms       = rec.get('rooms')
        cce2024     = rec.get('cce2024')     # Community Engagement
        medical     = rec.get('medical')

        # Total research spending in dollars
        total_research = None
        if serd is not None or nonserd is not None:
            total_research = ((serd or 0) + (nonserd or 0)) * 1000

        # Special designations
        designations = []
        if hbcu == 1:    designations.append("HBCU")
        if hsi  == 1:    designations.append("HSI")
        if tribal == 1:  designations.append("Tribal College")
        if landgrnt == 1: designations.append("Land-grant Institution")
        if medical == 1: designations.append("Medical Degree Granting")
        if cce2024 and cce2024 not in (0, None, ''): designations.append("Community Engaged (Carnegie)")

        entry = {
            "unitid":            unitid,
            "name":              name,
            "state":             state,
            "basic_code":        basic,
            "basic_label":       BASIC_LABELS.get(int(basic), str(basic)) if basic and str(basic).lstrip('-').isdigit() else str(basic or ""),
            "control":           CONTROL_LABELS.get(int(control), "") if control else "",
            "research_spending": total_research,
            "research_doctorates": int(docrsdeg) if docrsdeg else None,
            "faculty_count":     int(facnum) if facnum else None,
            "total_enrollment":  int(fallenr20) if fallenr20 else None,
            "dorm_capacity":     int(rooms) if rooms else None,
            "designations":      designations,
            "carnegie_page_url": f"{CARNEGIE_BASE_URL}{make_slug(name)}/",
        }
        by_unitid[unitid] = entry
        by_name.append(entry)
    log.info(f"  Lookup built: {len(by_name):,} institutions")
    return by_unitid, by_name

def match_university(name, state, by_unitid, by_name):
    best_score = 0.0
    best_match = None
    for rec in by_name:
        if state and rec["state"] and state.upper() != rec["state"].upper():
            continue
        score = name_similarity(name, rec["name"])
        if score > best_score:
            best_score, best_match = score, rec
    if best_score < 0.60:
        for rec in by_name:
            score = name_similarity(name, rec["name"])
            if score > best_score:
                best_score, best_match = score, rec
    return (best_match, best_score) if best_score >= 0.60 else (None, 0.0)

def ensure_columns(conn):
    cols = [
        ("tier_label",          "TEXT"),
        ("tier_reason",         "TEXT"),
        ("evidence_snippet",    "TEXT"),
        ("notes",               "TEXT"),
        ("research_spending",   "BIGINT"),
        ("research_doctorates", "INTEGER"),
        ("institution_control", "TEXT"),
        ("designations",        "TEXT"),
        ("faculty_count",       "INTEGER"),
        ("total_enrollment",    "INTEGER"),
        ("dorm_capacity",       "INTEGER"),
    ]
    with conn.cursor() as cur:
        for col, typ in cols:
            try:
                cur.execute(f"ALTER TABLE carnegie_classifications ADD COLUMN IF NOT EXISTS {col} {typ}")
                conn.commit()
            except Exception:
                conn.rollback()

def process_all(reset=False):
    records = load_carnegie_data()
    if not records:
        return
    by_unitid, by_name = build_lookup(records)

    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_columns(conn)
    NOW = datetime.now(timezone.utc)

    if reset:
        log.info("Clearing all Carnegie records...")
        with conn.cursor() as cur:
            cur.execute("DELETE FROM carnegie_classifications")
            conn.commit()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT u.id, u.name, u.state FROM universities u
            LEFT JOIN carnegie_classifications cc ON cc.university_id = u.id
            WHERE cc.id IS NULL ORDER BY u.name
        """)
        unis = list(cur.fetchall())

    log.info(f"Processing {len(unis)} universities...")
    done = found = t1 = t2 = 0

    for uni in unis:
        uni_id = uni["id"]
        name   = uni["name"]
        state  = uni["state"] or ""

        match, score = match_university(name, state, by_unitid, by_name)

        with conn.cursor() as cur:
            if match:
                tier_label, tier_reason = assign_tier(match["basic_code"])
                evidence = f"Carnegie 2021: {match['basic_label']}"
                if match["research_spending"]:
                    evidence += f" | Research spending: ${match['research_spending']:,.0f}"
                designations_str = ", ".join(match["designations"]) if match["designations"] else None
                try:
                    cur.execute("""
                        INSERT INTO carnegie_classifications
                            (university_id, institutional_classification, basic_classification,
                             carnegie_page_url, extracted_at, confidence, match_method,
                             tier_label, tier_reason, evidence_snippet,
                             research_spending, research_doctorates, institution_control,
                             designations, faculty_count, total_enrollment, dorm_capacity, notes)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
                        ON CONFLICT (university_id) DO UPDATE SET
                            institutional_classification = EXCLUDED.institutional_classification,
                            basic_classification         = EXCLUDED.basic_classification,
                            carnegie_page_url            = EXCLUDED.carnegie_page_url,
                            extracted_at                 = EXCLUDED.extracted_at,
                            confidence                   = EXCLUDED.confidence,
                            tier_label                   = EXCLUDED.tier_label,
                            tier_reason                  = EXCLUDED.tier_reason,
                            evidence_snippet             = EXCLUDED.evidence_snippet,
                            research_spending            = EXCLUDED.research_spending,
                            research_doctorates          = EXCLUDED.research_doctorates,
                            institution_control          = EXCLUDED.institution_control,
                            designations                 = EXCLUDED.designations,
                            faculty_count                = EXCLUDED.faculty_count,
                            total_enrollment             = EXCLUDED.total_enrollment,
                            dorm_capacity                = EXCLUDED.dorm_capacity,
                            notes                        = NULL
                    """, (
                        uni_id, match["basic_label"], match["basic_label"],
                        match["carnegie_page_url"], NOW, round(score, 2), "name_state_match",
                        tier_label, tier_reason, evidence,
                        match["research_spending"], match["research_doctorates"],
                        match["control"], designations_str,
                        match["faculty_count"], match["total_enrollment"], match["dorm_capacity"],
                    ))
                    conn.commit()
                    found += 1
                    if tier_label == "T1": t1 += 1
                    if tier_label == "T2": t2 += 1
                    if tier_label in ("T1", "T2"):
                        log.info(f"  [{done+1}] ✓ {tier_label} | {name} — {match['basic_label'][:50]}")
                except Exception as e:
                    log.warning(f"  DB error for {name}: {e}")
                    conn.rollback()
            else:
                try:
                    cur.execute("""
                        INSERT INTO carnegie_classifications
                            (university_id, extracted_at, confidence, match_method,
                             notes, tier_label, tier_reason)
                        VALUES (%s,%s,0.0,'not_found','No Carnegie match','T3','Not in Carnegie database')
                        ON CONFLICT (university_id) DO NOTHING
                    """, (uni_id, NOW))
                    conn.commit()
                except Exception:
                    conn.rollback()
        done += 1
        if done % 500 == 0:
            log.info(f"  ── {done}/{len(unis)} | {found} matched | T1:{t1} T2:{t2} ──")

    log.info(f"\n{'='*55}")
    log.info(f"DONE — {done} processed | {found} matched")
    log.info(f"  T1 (R1): {t1} | T2 (R2): {t2} | T3: {found-t1-t2}")
    conn.close()

if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "reset":
        process_all(reset=True)
    elif args and args[0] == "test":
        name  = args[1] if len(args) > 1 else "University of North Texas"
        state = args[2] if len(args) > 2 else "TX"
        records = load_carnegie_data()
        if records:
            by_unitid, by_name = build_lookup(records)
            match, score = match_university(name, state, by_unitid, by_name)
            if match:
                tier, reason = assign_tier(match["basic_code"])
                print(json.dumps({**match, "tier": tier, "reason": reason, "score": score}, indent=2))
            else:
                print(f"No match: {name}")
    else:
        process_all(reset=False)
