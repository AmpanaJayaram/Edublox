"""
award_verifier.py — Finds publisher verification links for ranking/award items
==============================================================================
For each award/ranking stored in university_rankings, tries to find:
  1) A publisher URL (usnews.com, forbes.com, etc.) — primary
  2) A university page URL that cites the award — secondary fallback

Usage:
    python award_verifier.py              # process all unverified awards
    python award_verifier.py 100          # first 100 (test)
"""

import sys, re, time, logging
from datetime import timezone, datetime
import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "unisearch",
    "user":     "postgres",
    "password": "2000",   # ← change to your password
}

DELAY = 1.0
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UniSearch/2.0; educational research)",
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Known publishers and their ranking URL patterns ────────────
# Maps keywords found in award text → publisher domain + search URL
PUBLISHERS = [
    {
        "keywords":  ["u.s. news", "us news", "usnews", "best college", "best national university",
                      "best value", "best liberal arts", "top public"],
        "domain":    "usnews.com",
        "name":      "U.S. News & World Report",
        "url":       "https://www.usnews.com/best-colleges",
    },
    {
        "keywords":  ["forbes", "america's top college", "america's best college"],
        "domain":    "forbes.com",
        "name":      "Forbes",
        "url":       "https://www.forbes.com/top-colleges/",
    },
    {
        "keywords":  ["princeton review", "best 389", "best 388", "best 387", "best 386",
                      "best 385", "best 384", "best 383", "best 382", "best 381", "best 380"],
        "domain":    "princetonreview.com",
        "name":      "Princeton Review",
        "url":       "https://www.princetonreview.com/college-rankings",
    },
    {
        "keywords":  ["washington monthly", "washington monthly ranking"],
        "domain":    "washingtonmonthly.com",
        "name":      "Washington Monthly",
        "url":       "https://washingtonmonthly.com/college-guide/",
    },
    {
        "keywords":  ["money magazine", "money's best", "money best college"],
        "domain":    "money.com",
        "name":      "Money Magazine",
        "url":       "https://money.com/best-colleges/",
    },
    {
        "keywords":  ["niche", "niche.com", "niche ranking"],
        "domain":    "niche.com",
        "name":      "Niche",
        "url":       "https://www.niche.com/colleges/rankings/",
    },
    {
        "keywords":  ["qs world", "qs ranking", "qs university ranking"],
        "domain":    "topuniversities.com",
        "name":      "QS World University Rankings",
        "url":       "https://www.topuniversities.com/university-rankings",
    },
    {
        "keywords":  ["times higher education", "the world university", "the ranking"],
        "domain":    "timeshighereducation.com",
        "name":      "Times Higher Education",
        "url":       "https://www.timeshighereducation.com/world-university-rankings",
    },
    {
        "keywords":  ["carnegie", "r1", "r2", "carnegie classification"],
        "domain":    "carnegieclassifications.acenet.edu",
        "name":      "Carnegie Classifications",
        "url":       "https://carnegieclassifications.acenet.edu/institution-search/",
    },
    {
        "keywords":  ["fulbright", "fulbright scholar", "fulbright producer"],
        "domain":    "iie.org",
        "name":      "Institute of International Education",
        "url":       "https://www.iie.org/programs/fulbright",
    },
    {
        "keywords":  ["military friendly", "military times"],
        "domain":    "militarytimes.com",
        "name":      "Military Times",
        "url":       "https://bestcolleges.militarytimes.com/",
    },
    {
        "keywords":  ["hispanic serving", "hsi", "hispanic-serving institution"],
        "domain":    "hacu.net",
        "name":      "Hispanic Association of Colleges and Universities",
        "url":       "https://www.hacu.net/hacu/HSI_Definition.asp",
    },
]


def match_publisher(text: str) -> dict | None:
    """Find matching publisher for an award/ranking text."""
    text_lower = text.lower()
    for pub in PUBLISHERS:
        if any(kw in text_lower for kw in pub["keywords"]):
            return pub
    return None


def extract_rank_number(text: str) -> str | None:
    """Extract ranking number from text like '#5 Best...' or 'No. 12...'"""
    m = re.search(r"(?:#|no\.?\s*)(\d+)", text, re.I)
    return m.group(1) if m else None


def build_publisher_url(award_text: str, uni_name: str, publisher: dict) -> dict:
    """
    Build the best available publisher URL for an award.
    Returns dict with url, confidence, and note.
    """
    base_url  = publisher["url"]
    rank_num  = extract_rank_number(award_text)

    # For US News, build a more specific URL if possible
    if "usnews.com" in publisher["domain"]:
        if "national university" in award_text.lower():
            return {"url": "https://www.usnews.com/best-colleges/rankings/national-universities",
                    "confidence": 0.75, "note": "US News National Universities ranking page"}
        elif "liberal arts" in award_text.lower():
            return {"url": "https://www.usnews.com/best-colleges/rankings/national-liberal-arts-colleges",
                    "confidence": 0.75, "note": "US News Liberal Arts ranking page"}
        elif "public" in award_text.lower():
            return {"url": "https://www.usnews.com/best-colleges/rankings/top-public-schools",
                    "confidence": 0.75, "note": "US News Top Public Schools ranking page"}
        elif "best value" in award_text.lower():
            return {"url": "https://www.usnews.com/best-colleges/rankings/best-value-schools",
                    "confidence": 0.75, "note": "US News Best Value Schools"}
        return {"url": base_url, "confidence": 0.65, "note": "US News Best Colleges"}

    if "forbes.com" in publisher["domain"]:
        return {"url": "https://www.forbes.com/top-colleges/",
                "confidence": 0.70, "note": "Forbes Top Colleges ranking"}

    if "washingtonmonthly.com" in publisher["domain"]:
        return {"url": "https://washingtonmonthly.com/college-guide/",
                "confidence": 0.70, "note": "Washington Monthly College Guide"}

    return {"url": base_url, "confidence": 0.65, "note": f"{publisher['name']} ranking page"}


def ensure_columns(conn):
    """Add source columns to university_rankings if not present."""
    cols = [
        ("source_url_publisher",  "TEXT"),
        ("source_url_university", "TEXT"),
        ("publisher_name",        "TEXT"),
        ("publisher_confidence",  "NUMERIC(3,2)"),
        ("evidence_snippet",      "TEXT"),
        ("verified_at",           "TIMESTAMPTZ"),
    ]
    with conn.cursor() as cur:
        for col, typ in cols:
            try:
                cur.execute(
                    f"ALTER TABLE university_rankings ADD COLUMN IF NOT EXISTS {col} {typ}")
                conn.commit()
            except Exception:
                conn.rollback()


def process_awards(limit: int = None):
    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_columns(conn)
    NOW = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.id, r.description, r.source_url, r.type,
                   u.name AS uni_name, u.school_url
            FROM university_rankings r
            JOIN universities u ON u.id = r.university_id
            WHERE r.source_url_publisher IS NULL
            ORDER BY r.id
            LIMIT %s
        """, (limit or 999_999,))
        awards = list(cur.fetchall())

    log.info(f"Verifying {len(awards)} award/ranking items...")
    done = matched = 0

    for item in awards:
        desc     = item["description"] or ""
        uni_name = item["uni_name"]    or ""
        uni_url  = item["school_url"]  or ""

        publisher = match_publisher(desc)
        pub_url_data = None

        if publisher:
            pub_url_data = build_publisher_url(desc, uni_name, publisher)

        with conn.cursor() as cur:
            try:
                cur.execute("""
                    UPDATE university_rankings SET
                        source_url_publisher  = %s,
                        source_url_university = %s,
                        publisher_name        = %s,
                        publisher_confidence  = %s,
                        evidence_snippet      = %s,
                        verified_at           = %s
                    WHERE id = %s
                """, (
                    pub_url_data["url"]        if pub_url_data else None,
                    uni_url                    if publisher    else None,
                    publisher["name"]          if publisher    else None,
                    pub_url_data["confidence"] if pub_url_data else None,
                    desc[:200],
                    NOW,
                    item["id"],
                ))
                conn.commit()
                if publisher:
                    matched += 1
            except Exception as e:
                log.warning(f"  DB error: {e}")
                conn.rollback()

        done += 1
        if done % 500 == 0:
            log.info(f"  {done}/{len(awards)} done | {matched} publisher links found")

    log.info(f"\nDONE — {done} awards processed | {matched} publisher links found")
    conn.close()


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    process_awards(limit=limit)
