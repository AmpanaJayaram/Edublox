"""
app/services/program_service.py
================================
Database logic and web-scraping for university programs.
"""

import re
from app.db.connection import get_conn


# ── Queries ───────────────────────────────────────────────────

def get_programs(uid: int) -> list[dict] | dict:
    """
    Return programs for a university, or a dict with no_programs=True
    and the catalog URL if none exist yet.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM universities WHERE id = %s", (uid,))
            if not cur.fetchone():
                return {"error": "University not found"}

            cur.execute("""
                SELECT id, name, category, degree_level, description,
                       is_featured, top20_rank, reputation_note, program_url
                FROM university_programs
                WHERE university_id = %s
                ORDER BY is_featured DESC, top20_rank NULLS LAST, category, name
            """, (uid,))
            existing = cur.fetchall()

    if existing:
        return [dict(r) for r in existing]

    # No programs yet — return catalog URL so the UI can point the user there
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT catalog_url, school_url FROM universities WHERE id = %s", (uid,)
            )
            r = cur.fetchone()
            catalog_url = (r["catalog_url"] or r["school_url"] or "") if r else ""

    return {
        "no_programs": True,
        "catalog_url": catalog_url,
        "message": "No programs in database yet. Visit the official catalog to browse programs.",
    }


def set_featured_program(uid: int, program_id: int, reputation_note: str = "") -> None:
    """Unset any existing featured program for uid, then feature program_id."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE university_programs SET is_featured = false WHERE university_id = %s",
                (uid,),
            )
            cur.execute(
                """
                UPDATE university_programs
                SET is_featured = true, reputation_note = %s
                WHERE id = %s AND university_id = %s
                """,
                (reputation_note or None, program_id, uid),
            )
            conn.commit()


def reset_programs(uid: int) -> None:
    """Delete all programs for a university."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM university_programs WHERE university_id = %s", (uid,)
            )
            conn.commit()


# ── URL scraper ───────────────────────────────────────────────

def update_program_urls(uid: int) -> dict:
    """
    Scrape the university's website for program pages and update
    program_url on matched university_programs rows.

    Returns a summary dict: {scraped_links, pages_visited, updated, total}.
    """
    import requests as _req
    from bs4 import BeautifulSoup

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, school_url FROM universities WHERE id = %s", (uid,))
            row = cur.fetchone()
            if not row:
                return {"error": "University not found"}
            school_url = row["school_url"] or ""

            cur.execute(
                "SELECT id, name FROM university_programs WHERE university_id = %s", (uid,)
            )
            programs = [dict(r) for r in cur.fetchall()]

    base_domain = re.sub(r"^https?://(www\.)?", "", school_url).rstrip("/")
    base_url_s  = f"https://www.{base_domain}" if base_domain else ""

    session = _req.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; UniSearch/1.0)"})

    scraped_links: dict[str, str] = {}
    visited:       set[str]       = set()

    def _scrape(url: str) -> dict[str, str]:
        if url in visited or len(visited) > 50:
            return {}
        visited.add(url)
        try:
            resp = session.get(url, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                return {}
            soup  = BeautifulSoup(resp.text, "html.parser")
            links = {}
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True)
                href = a["href"].split("?")[0].split("#")[0]
                if not text or len(text) < 4 or len(text) > 120:
                    continue
                if href.startswith("/"):
                    href = f"https://{base_domain}{href}"
                elif not href.startswith("http"):
                    continue
                if base_domain and base_domain not in href:
                    continue
                links[text.strip().lower()] = href
            return links
        except Exception:
            return {}

    # Crawl common program-listing paths
    for path in [
        "/academics/degrees-and-programs", "/academics/programs", "/programs",
        "/degrees", "/catalog", "/academics", "/majors", "/academics/graduate",
    ]:
        for base in [f"https://{base_domain}", base_url_s]:
            scraped_links.update(_scrape(f"{base}{path}"))
        if len(scraped_links) > 200:
            break

    for url in [
        f"https://catalog.{base_domain}",
        f"https://graduate.{base_domain}/programs",
    ]:
        scraped_links.update(_scrape(url))

    # Follow promising sub-pages
    sub_pages = [
        url for _, url in list(scraped_links.items())
        if any(kw in url for kw in ["/program", "/degree", "/catalog", "/academics/"])
        and url not in visited
    ]
    for url in sub_pages[:20]:
        scraped_links.update(_scrape(url))

    # Match programs to scraped links
    updated = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for prog in programs:
                norm_name = _normalize(prog["name"])
                best_url, best_score = None, 0.0

                for link_text, link_url in scraped_links.items():
                    norm_link = _normalize(link_text)
                    if norm_name == norm_link:
                        best_url = link_url
                        break
                    score = _overlap(norm_name, norm_link)
                    if score >= 0.65 and score > best_score:
                        best_score = score
                        best_url   = link_url

                if best_url:
                    cur.execute(
                        "UPDATE university_programs SET program_url = %s WHERE id = %s",
                        (best_url, prog["id"]),
                    )
                    updated += 1
            conn.commit()

    return {
        "scraped_links": len(scraped_links),
        "pages_visited": len(visited),
        "updated":       updated,
        "total":         len(programs),
    }


# ── Private text helpers ──────────────────────────────────────

_DEGREE_PREFIXES = (
    "bachelor of science in ", "bachelor of arts in ",
    "master of science in ",   "master of arts in ",
    "master of business administration", "master of business ",
    "doctor of philosophy in ", "doctor of education in ",
    "ph.d. in ", "phd in ", "m.s. in ", "m.a. in ",
    "ed.d. in ", "master of ", "bachelor of ",
    "certificate in ", "minor in ",
)

_STOP_WORDS = {"in", "of", "and", "the", "a", "an", "with", "for", "at", "to", "studies"}


def _normalize(s: str) -> str:
    s = s.lower().strip()
    for prefix in _DEGREE_PREFIXES:
        if s.startswith(prefix):
            return s[len(prefix):].strip()
    return s


def _overlap(a: str, b: str) -> float:
    wa = set(a.split()) - _STOP_WORDS
    wb = set(b.split()) - _STOP_WORDS
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))
