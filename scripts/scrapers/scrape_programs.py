"""
UniSearch — Universal Program Scraper
=======================================
Scrapes ALL programs from ALL universities in your database.
Supports 100+ universities with correct featured programs.

USAGE:
  Scrape ALL universities at once:
    python scrape_programs.py --all

  Skip universities that already have data:
    python scrape_programs.py --all --skip-existing

  Scrape one university by DB ID:
    python scrape_programs.py --id 3087

  Scrape one university by name:
    python scrape_programs.py --name "north texas"

  Preview without saving:
    python scrape_programs.py --id 3087 --preview

  Clear existing data and re-scrape:
    python scrape_programs.py --id 3087 --reset

  List all universities with program counts:
    python scrape_programs.py --list

  Add one program manually:
    python scrape_programs.py --add --id 3087
      --program "Bachelor of Music in Jazz Studies"
      --category "Arts & Humanities" --level Undergraduate
      --url "https://music.unt.edu" --rank 1 --featured

REQUIREMENTS:
  pip install requests beautifulsoup4 psycopg2-binary lxml
"""

import argparse, re, sys, os, time, json
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

# Import knowledge base
from university_knowledge import KNOWN_FEATURED

# ─────────────────────────────────────────────────────────────────
# DB CONFIG — edit or set environment variables
# ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME",     "unisearch"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
})

# ─────────────────────────────────────────────────────────────────
# CLASSIFIERS
# ─────────────────────────────────────────────────────────────────
CATEGORY_RULES = [
    ("Engineering",       ["engineering","electrical","mechanical","civil","chemical",
                           "biomedical","aerospace","industrial","materials science",
                           "computer engineering","petroleum","nuclear"]),
    ("STEM",              ["computer science","data science","mathematics","statistics",
                           "physics","chemistry","biology","geology","astronomy",
                           "neuroscience","biochemistry","information science",
                           "cybersecurity","information technology","software",
                           "environmental science","ecology","bioinformatics",
                           "applied mathematics","data analytics","machine learning"]),
    ("Health & Medicine", ["nursing","medicine","public health","pharmacy","audiology",
                           "speech","pathology","kinesiology","nutrition","dental",
                           "physical therapy","occupational therapy","radiologic",
                           "clinical","health services","health administration",
                           "medical","healthcare","health science","rehabilitation",
                           "athletic training","dietetics"]),
    ("Business",          ["business","accounting","finance","marketing","management",
                           "economics","entrepreneurship","supply chain","real estate",
                           "hospitality","merchandising","fashion","retail","logistics",
                           "human resources","operations","mba"]),
    ("Education",         ["education","teaching","curriculum","instruction","counseling",
                           "educational leadership","special education","literacy",
                           "early childhood","higher education","school psychology"]),
    ("Law & Policy",      ["law","political science","public administration","public policy",
                           "government","criminal justice","legal","emergency management",
                           "homeland security","international relations","diplomacy",
                           "urban planning","nonprofit","forensic"]),
    ("Arts & Humanities", ["music","art","dance","theatre","theater","film","journalism",
                           "english","history","philosophy","languages","literature",
                           "creative writing","visual arts","studio art","design",
                           "media","communication","radio","television","digital media",
                           "humanities","linguistics","jazz","performance","composition",
                           "graphic design","animation","photography","architecture",
                           "advertising"]),
    ("Social Sciences",   ["sociology","psychology","anthropology","social work",
                           "women's studies","gender","ethnic studies","cultural studies",
                           "international studies","child development","family studies",
                           "behavioral science","cognitive science"]),
]

DEGREE_PREFIXES = [
    ("Doctoral",      ["doctor of philosophy","ph.d.","phd in","doctor of education",
                       "ed.d.","doctor of musical arts","d.m.a.","juris doctor","j.d.",
                       "doctor of pharmacy","pharm.d.","doctor of nursing","dnp",
                       "doctor of medicine","m.d.","doctor of business","d.b.a.",
                       "doctor of veterinary"]),
    ("Graduate",      ["master of","master's","m.s. in","m.a. in","m.b.a.","m.ed.",
                       "m.f.a.","m.p.h.","m.p.a.","m.eng.","m.arch.","m.mus.",
                       "m.s.w.","graduate certificate","post-baccalaureate"]),
    ("Certificate",   ["certificate in","minor in","concentration in","endorsement in"]),
    ("Undergraduate", ["bachelor of","bachelor's","b.s. in","b.a. in","b.f.a.","b.mus.",
                       "b.b.a.","b.s.w.","b.arch.","associate of","associate's","a.s.","a.a."]),
]

DEGREE_INDICATORS = ["bachelor","master","doctor","ph.d","phd","b.s.","b.a.",
                     "m.s.","m.a.","mba","m.b.a","bfa","mfa","b.f.a","m.f.a",
                     "certificate","minor in","associate","d.m.a","ed.d","m.ed",
                     "dpt","dnp","pharm.d","j.d.","juris doctor","d.d.s"]

NOISE_WORDS = {"click here","learn more","apply now","contact us","home","about",
               "news","events","login","register","search","menu","back to",
               "return to","view all","see all","read more","next","previous",
               "download","print","share","visit","tour","explore","skip to"}


def classify_category(name: str) -> str:
    n = name.lower()
    for cat, kws in CATEGORY_RULES:
        if any(kw in n for kw in kws):
            return cat
    return "STEM"


def classify_degree(name: str) -> str:
    n = name.lower()
    for level, indicators in DEGREE_PREFIXES:
        if any(n.startswith(ind) or f" {ind}" in n for ind in indicators):
            return level
    return "Undergraduate"


def looks_like_program(text: str) -> bool:
    if not text or len(text) < 8 or len(text) > 200:
        return False
    t = text.lower()
    if not any(d in t for d in DEGREE_INDICATORS):
        return False
    if any(n in t for n in NOISE_WORDS):
        return False
    return True


def find_known_data(uni_name: str) -> dict | None:
    """Find matching entry in KNOWN_FEATURED for a given university name."""
    uni_lower = uni_name.lower()
    for key, data in KNOWN_FEATURED.items():
        if key in uni_lower:
            return data
    return None


# ─────────────────────────────────────────────────────────────────
# SCRAPER CLASS
# ─────────────────────────────────────────────────────────────────
class UniversityScraper:
    def __init__(self, uid: int, name: str, school_url: str, verbose: bool = True):
        self.uid         = uid
        self.uni_name    = name
        self.school_url  = school_url or ""
        self.verbose     = verbose
        self.base_domain = re.sub(r"^https?://(www\.)?", "", self.school_url).rstrip("/")
        self.visited     = set()
        self.programs    = []
        self.seen_names  = set()

    def log(self, msg):
        if self.verbose:
            print(msg)

    def fetch(self, url: str) -> BeautifulSoup | None:
        if url in self.visited or len(self.visited) >= 80:
            return None
        self.visited.add(url)
        time.sleep(0.4)
        try:
            r = SESSION.get(url, timeout=12, allow_redirects=True)
            if r.status_code == 200:
                self.log(f"    ✓ {url}")
                return BeautifulSoup(r.text, "lxml")
            self.log(f"    ✗ {url} [{r.status_code}]")
            return None
        except Exception as e:
            self.log(f"    ✗ {url} ({type(e).__name__})")
            return None

    def resolve(self, href: str, base: str) -> str | None:
        if not href:
            return None
        href = href.split("?")[0].split("#")[0].strip()
        if not href or href.startswith(("mailto:", "javascript:", "tel:")):
            return None
        full   = urljoin(base, href)
        parsed = urlparse(full)
        if self.base_domain and self.base_domain not in parsed.netloc:
            return None
        return full

    def add(self, name: str, url: str | None) -> bool:
        name = re.sub(r"\s+", " ", name.strip())
        name = re.sub(r"\s*[-–|].*$", "", name).strip()
        if not looks_like_program(name):
            return False
        if name.lower() in self.seen_names:
            return False
        self.seen_names.add(name.lower())
        self.programs.append({
            "name":          name,
            "category":      classify_category(name),
            "degree_level":  classify_degree(name),
            "description":   None,
            "program_url":   url,
            "is_featured":   False,
            "top20_rank":    None,
            "reputation_note": None,
        })
        self.log(f"      + [{classify_degree(name):<14}] {name}")
        return True

    def scrape_page(self, url: str) -> list[str]:
        soup = self.fetch(url)
        if not soup:
            return []
        sub_urls = []
        for a in soup.find_all("a", href=True):
            text     = a.get_text(separator=" ", strip=True)
            link_url = self.resolve(a["href"], url)
            if not link_url:
                continue
            if looks_like_program(text):
                self.add(text, link_url)
            elif any(kw in link_url.lower() for kw in
                     ["/program","/degree","/major","/catalog/",
                      "/academics/","/graduate/","/undergraduate/"]):
                sub_urls.append(link_url)
        # Parse structured elements
        for tag in soup.find_all(["h2","h3","h4","li","dt","td"]):
            text = tag.get_text(separator=" ", strip=True)
            a    = tag.find("a")
            link_url = self.resolve(a["href"], url) if a and a.get("href") else None
            if looks_like_program(text):
                self.add(text, link_url or url)
        return sub_urls

    def run(self):
        if not self.base_domain:
            self.log(f"  ⚠️  No URL for {self.uni_name}, skipping")
            return

        d    = self.base_domain
        www  = f"https://www.{d}"
        base = f"https://{d}"

        self.log(f"\n{'─'*65}")
        self.log(f"  {self.uni_name}")
        self.log(f"  {self.school_url}")
        self.log(f"{'─'*65}")

        # Tier 1 — standard program listing paths
        t1_paths = [
            "/academics/degrees-and-programs", "/academics/programs",
            "/academics/undergraduate-programs", "/academics/graduate-programs",
            "/programs-of-study", "/catalog/programs", "/catalog",
            "/majors", "/degrees", "/programs", "/academics",
            "/future-students/academics", "/admissions/academics",
            "/graduate/programs", "/undergraduate/programs",
        ]
        sub_urls = []
        for path in t1_paths:
            for root in [www, base]:
                subs = self.scrape_page(f"{root}{path}")
                sub_urls.extend(subs)
                if len(self.programs) > 60:
                    break
            if len(self.programs) > 60:
                break

        # Tier 2 — catalog & graduate subdomains
        for url in [f"https://catalog.{d}", f"https://catalog.{d}/programs",
                    f"https://catalog.{d}/index.php",
                    f"https://graduate.{d}", f"https://graduate.{d}/programs"]:
            sub_urls.extend(self.scrape_page(url))

        # Tier 3 — crawl promising sub-pages
        seen_sub = set()
        for sub_url in sub_urls:
            if sub_url not in seen_sub and sub_url not in self.visited:
                seen_sub.add(sub_url)
                self.scrape_page(sub_url)
            if len(self.programs) > 250:
                break

        # Tier 4 — department subdomains
        for slug in ["music","cs","cob","art","engineering","science","law",
                     "nursing","pharmacy","education","business","graduate"]:
            if len(self.visited) >= 75:
                break
            self.scrape_page(f"https://{slug}.{d}")

        self.log(f"\n  📋 {len(self.programs)} programs  |  {len(self.visited)} pages visited")
        self._apply_known_data()

    def _apply_known_data(self):
        """Apply correct featured program and rankings from knowledge base."""
        data = find_known_data(self.uni_name)
        if not data:
            # No known data — just pick the first scraped program as featured
            if self.programs:
                self.programs[0]["is_featured"] = True
                self.log(f"  ℹ️  No known rankings — defaulting first scraped program as featured")
            return

        feat_name = data["featured_name"]
        rep_note  = data["reputation_note"]
        prog_url  = data.get("program_url")
        top20     = data.get("top20", [feat_name])

        # Find or add featured program
        found = False
        for p in self.programs:
            if p["name"].lower() == feat_name.lower():
                p["is_featured"]     = True
                p["reputation_note"] = rep_note
                if prog_url and not p["program_url"]:
                    p["program_url"] = prog_url
                found = True
                break

        if not found:
            self.programs.insert(0, {
                "name":          feat_name,
                "category":      classify_category(feat_name),
                "degree_level":  classify_degree(feat_name),
                "description":   None,
                "program_url":   prog_url,
                "is_featured":   True,
                "top20_rank":    1,
                "reputation_note": rep_note,
            })
            self.seen_names.add(feat_name.lower())
            self.log(f"  ⭐ Pinned featured: {feat_name}")

        # Apply top-20 ranks
        for rank, prog_name in enumerate(top20, 1):
            for p in self.programs:
                if p["name"].lower() == prog_name.lower():
                    p["top20_rank"] = rank
                    break

        self.log(f"  ✅ Applied known rankings for {self.uni_name}")


# ─────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG,
                            cursor_factory=psycopg2.extras.RealDictCursor)


def get_all_universities() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.name, u.school_url,
                       COUNT(p.id) AS program_count
                FROM universities u
                LEFT JOIN university_programs p ON p.university_id = u.id
                GROUP BY u.id, u.name, u.school_url
                ORDER BY u.name
            """)
            return [dict(r) for r in cur.fetchall()]


def save_programs(uid: int, programs: list[dict],
                  reset: bool = False, preview: bool = False):
    if preview:
        print(f"\n  PREVIEW — {len(programs)} programs (not saved)")
        for p in programs:
            r = f"#{p['top20_rank']}" if p['top20_rank'] else "   "
            f = "⭐" if p['is_featured'] else "  "
            u = "🔗" if p['program_url'] else "  "
            print(f"  {f} {r:<5} {u} [{p['degree_level']:<14}] {p['name']}")
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
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'scraper')
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


def scrape_one(uid, name, school_url, reset=False, preview=False,
               skip_existing=False, verbose=True) -> dict:
    if skip_existing and not preview:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM university_programs "
                            "WHERE university_id=%s", (uid,))
                count = cur.fetchone()["c"]
                if count > 0:
                    print(f"  ⏭  Skipping {name} ({count} programs already)")
                    return {"uid": uid, "name": name, "status": "skipped", "existing": count}

    scraper = UniversityScraper(uid, name, school_url, verbose=verbose)
    scraper.run()
    inserted, skipped = save_programs(uid, scraper.programs, reset=reset, preview=preview)
    if not preview:
        print(f"  💾 Inserted: {inserted}  |  Skipped: {skipped}\n")
    return {"uid": uid, "name": name, "status": "ok",
            "scraped": len(scraper.programs), "inserted": inserted,
            "skipped": skipped, "pages": len(scraper.visited)}


def add_manual(uid, name, category, degree_level, url=None,
               featured=False, rank=None, note=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM universities WHERE id=%s", (uid,))
            row = cur.fetchone()
            if not row:
                print(f"ERROR: University {uid} not found"); return
            uni_name = row["name"]
            if featured:
                cur.execute("UPDATE university_programs SET is_featured=false "
                            "WHERE university_id=%s", (uid,))
            cur.execute("""
                INSERT INTO university_programs
                    (university_id, name, category, degree_level,
                     is_featured, top20_rank, reputation_note, program_url, generated_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'manual')
                ON CONFLICT DO NOTHING RETURNING id
            """, (uid, name, category, degree_level, featured, rank, note, url))
            row = cur.fetchone()
            conn.commit()
    print(f"{'✅ Added' if row else '⚠️  Already exists'} in {uni_name}: {name}")


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="UniSearch — Universal Program Scraper for ALL universities",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all",           action="store_true",  help="Scrape ALL universities")
    ap.add_argument("--id",            type=int,             help="Scrape one university by DB ID")
    ap.add_argument("--name",          type=str,             help="Scrape one university by name")
    ap.add_argument("--list",          action="store_true",  help="List all universities + program counts")
    ap.add_argument("--reset",         action="store_true",  help="Clear existing programs before scraping")
    ap.add_argument("--preview",       action="store_true",  help="Show programs without saving")
    ap.add_argument("--skip-existing", action="store_true",  help="Skip universities that have data")
    ap.add_argument("--quiet",         action="store_true",  help="Less verbose output")
    # Manual add
    ap.add_argument("--add",           action="store_true")
    ap.add_argument("--program",       type=str)
    ap.add_argument("--category",      type=str, default="STEM")
    ap.add_argument("--level",         type=str, default="Undergraduate", dest="degree_level")
    ap.add_argument("--url",           type=str)
    ap.add_argument("--featured",      action="store_true")
    ap.add_argument("--rank",          type=int)
    ap.add_argument("--note",          type=str)
    args = ap.parse_args()

    # ── List
    if args.list:
        unis = get_all_universities()
        has_knowledge = [u for u in unis if find_known_data(u["name"])]
        print(f"\n{'ID':<8} {'Programs':<12} {'KB':<5} {'Name':<55} URL")
        print("─" * 120)
        for u in unis:
            c  = u.get("program_count", 0)
            m  = "✅" if c > 10 else "⚠️ " if c > 0 else "❌"
            kb = "✓" if find_known_data(u["name"]) else " "
            print(f"{u['id']:<8} {m} {str(c):<9} {kb:<5} {u['name']:<55} {u['school_url'] or ''}")
        print(f"\n  Total: {len(unis)} universities")
        print(f"  In knowledge base: {len(has_knowledge)} universities (correct featured programs)")
        print(f"  Not in knowledge base: {len(unis)-len(has_knowledge)} (will use first scraped program)")
        return

    # ── Manual add
    if args.add:
        if not args.id or not args.program:
            print("ERROR: --add requires --id and --program"); return
        add_manual(args.id, args.program, args.category, args.degree_level,
                   args.url, args.featured, args.rank, args.note)
        return

    # ── Scrape all
    if args.all:
        unis = get_all_universities()
        print(f"\n🚀 Starting batch scrape of {len(unis)} universities...")
        print(f"   Knowledge base covers {sum(1 for u in unis if find_known_data(u['name']))} of them\n")
        results = []
        for i, u in enumerate(unis, 1):
            print(f"\n[{i}/{len(unis)}] {u['name']}")
            r = scrape_one(u["id"], u["name"], u.get("school_url",""),
                           reset=args.reset, preview=args.preview,
                           skip_existing=args.skip_existing,
                           verbose=not args.quiet)
            results.append(r)
            time.sleep(2)  # be polite between universities

        ok   = [r for r in results if r["status"] == "ok"]
        skp  = [r for r in results if r["status"] == "skipped"]
        tot  = sum(r.get("inserted", 0) for r in ok)
        zero = [r for r in ok if r.get("scraped", 0) == 0]

        print(f"\n{'='*65}")
        print(f"  ✅ BATCH COMPLETE")
        print(f"  Universities scraped: {len(ok)}  |  Skipped: {len(skp)}")
        print(f"  Total programs inserted: {tot}")
        if zero:
            print(f"\n  ⚠️  Got 0 programs for {len(zero)} universities:")
            print(f"  (Use --add or import_programs_csv.py for these)")
            for r in zero:
                print(f"       python scrape_programs.py --id {r['uid']} --reset  [{r['name']}]")
        print(f"{'='*65}\n")
        with open("scrape_log.json","w") as f:
            json.dump(results, f, indent=2)
        print(f"  📄 Full log saved to scrape_log.json")
        return

    # ── Scrape one by ID
    if args.id:
        unis = [u for u in get_all_universities() if u["id"] == args.id]
        if not unis:
            print(f"ERROR: University ID {args.id} not found"); return
        u = unis[0]
        scrape_one(u["id"], u["name"], u.get("school_url",""),
                   reset=args.reset, preview=args.preview, verbose=not args.quiet)
        return

    # ── Scrape one by name
    if args.name:
        unis = [u for u in get_all_universities()
                if args.name.lower() in u["name"].lower()]
        if not unis:
            print(f"ERROR: No university matching '{args.name}'"); return
        if len(unis) > 1:
            print(f"Multiple matches — use --id instead:")
            for u in unis: print(f"  {u['id']}: {u['name']}")
            return
        u = unis[0]
        scrape_one(u["id"], u["name"], u.get("school_url",""),
                   reset=args.reset, preview=args.preview, verbose=not args.quiet)
        return

    ap.print_help()


if __name__ == "__main__":
    main()
