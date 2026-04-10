# UniSearch Refactor — Progress Tracker

That covers everything. When you start a fresh session, paste the contents of that file and say "continue the UniSearch refactor" — no re-explaining needed. The next session picks up exactly at the 5 remaining scrapers.

## Project
Flask + PostgreSQL university search app called **UniSearch**.
Refactoring from a monolithic root-dump into industry-standard structure.
Stack: Python 3.12, Flask, psycopg2, Groq (llama-3.3-70b), uv for package management.

---

## Current file structure (what has been built)

```
unisearch/
├── app/                          ✅ DONE
│   ├── __init__.py               create_app() factory
│   ├── config.py                 DB_CONFIG, PAGE_SIZE, env helpers
│   ├── extensions.py             Groq client
│   ├── db/
│   │   ├── __init__.py
│   │   └── connection.py         get_conn()
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── pages.py              index, university, compare, match, map, state
│   │   ├── universities.py       all /api/universities* and /api/university* routes
│   │   └── ai.py                 /api/analyze, /api/match_ai
│   ├── services/
│   │   ├── __init__.py
│   │   ├── university_service.py DB query logic (search, detail, compare, stats)
│   │   ├── ai_service.py         Groq prompts (analyze + match)
│   │   └── program_service.py    program queries + URL scraper
│   └── utils/
│       ├── __init__.py
│       ├── constants.py          STATE_NAMES, SORT_COLS, FILTER_MAP, FACTS_KEY
│       └── text_utils.py         infer_control()
├── migrations/                   ✅ DONE
│   ├── __init__.py
│   ├── startup.py                idempotent schema additions (runs at app start)
│   └── migrate.py                one-time Excel+JSON → PostgreSQL migration
│                                 (fixed: was fully hardcoded, now reads from .env)
├── scripts/
│   ├── __init__.py
│   ├── scrapers/                 ✅ DONE
│   │   ├── __init__.py
│   │   ├── scraper_utils.py      ← NEW: shared utils extracted from 3 duplicate scrapers
│   │   │                           get_category, get_level, clean, is_real_degree,
│   │   │                           fetch, is_acalog, is_smartcatalog,
│   │   │                           extract_acalog, best_acalog_page, scrape_acalog_full,
│   │   │                           scrape_generic, apply_knowledge,
│   │   │                           save_programs, fix_categories, update_top20
│   │   ├── master_scraper.py     refactored (imports from scraper_utils)
│   │   ├── production_scraper.py refactored (imports from scraper_utils)
│   │   ├── scrape_all.py         refactored (imports from scraper_utils)
│   │   ├── scrape_fast.py        moved (async scraper, path setup added)
│   │   └── university_scraper.py moved (College Scorecard + rankings scraper)
│   └── data_import/              ✅ DONE
│       ├── __init__.py
│       ├── db_config.py          ← NEW: shared DB config for all import scripts
│       ├── add_missing_programs.py  fixed (was opening DB at module level!)
│       ├── import_csv_data.py    fixed (removed hardcoded password)
│       ├── import_programs_csv.py   fixed
│       ├── ipeds_import.py       fixed
│       ├── scorecard_import.py   fixed (removed hardcoded API key fallback)
│       └── patch_institution_type.py  fixed
├── run.py                        ✅ DONE — entry point, replaces app.py
└── pyproject.toml                ✅ DONE
```

---

## What was fixed / why it matters

### app.py → app/ package
- 986-line monolith split into 14 focused files
- Bug fixed: `api_compare` was calling `app.test_client()` internally — replaced with direct service call
- Circular import resolved: imports inside `create_app()` not at module level

### pyproject.toml
- Added missing deps: `beautifulsoup4`, `requests`, `gunicorn`
- Added `[dependency-groups] scrapers` for `aiohttp` and `openpyxl`
- Added `[tool.hatch.build.targets.wheel] packages = ["app", "migrations"]` — fixes hatchling build error
- Added `[tool.pytest.ini_options]`

### scraper_utils.py
- `get_category`, `get_level`, `clean`, `is_real_degree` were copy-pasted across
  master_scraper, production_scraper, scrape_all — now one source of truth
- Canonical versions are from scrape_all.py (most evolved)
- master_scraper: 374 → 231 lines | production_scraper: 385 → 278 | scrape_all: 370 → 205

### Hardcoded passwords removed
- Every script had `"password": os.environ.get("DB_PASSWORD", "2000")`
- All now read from .env with no fallback
- `migrate.py` had fully hardcoded config with zero env var usage — fixed
- `scorecard_import.py` had a real API key hardcoded as fallback — removed

---

## How to run

```bash
uv sync --group dev          # install all deps including dev
uv sync --group scrapers     # install scraper-only deps (aiohttp, openpyxl)
uv run run.py                # development server
uv run gunicorn "app:create_app()"   # production
uv run pytest                # tests (not written yet)
```

---

## What's NOT done yet (next session priority order)

### 1. scripts/scrapers/ — 5 remaining files to place
These were identified as scrapers (not data importers) but not yet refactored:
- `carnegie_fetcher.py` → `scripts/scrapers/` — reads CCIHE2021-PublicData.xlsx,
  has hardcoded DB_CONFIG `{"password": "2000"}` (no env vars at all)
- `catalog_url_scraper.py` → `scripts/scrapers/` — has duplicate get_category/get_level/etc,
  hardcoded password, should import from scraper_utils
- `scrape_from_csv.py` → `scripts/scrapers/` — same duplication issues
- `scrape_programs.py` → `scripts/scrapers/` — OOP class UniversityScraper,
  has its own CATEGORY_RULES/DEGREE_PREFIXES, no hardcoded password but no load_dotenv
- `unt_catalog_import.py` → `scripts/scrapers/` — UNT-specific, self-contained,
  has hardcoded DB_CONFIG (dbname/user/password all literal strings, no env vars)

### 2. scripts/fixes/ — archive of one-off scripts
These just need to be moved, no code changes:
- `fix_db_duplicates.py`, `fix_duplicates.py`, `fix_unt.py`, `fix_unt_clean.py`,
  `fix_unt_final.py`, `fix_unt_grad.py`, `fix_unt_v2.py`, `debug_ipeds.py`,
  `debug_unt.py`, `debug2.py`, `check_ipeds_columns.py`, `check_unt.py`,
  `award_verifier.py`

### 3. data/ folder — raw data files to organise
Move from root into:
```
data/
├── raw/        CCIHE2021-PublicData.xlsx, Uni_data.xlsx, Uni_data_filled.xlsx,
│               Uni_data_filled_universities_summary_.csv, programs.csv,
│               ipeds_completions.csv, ipeds_directory.csv
└── logs/       ipeds_log.json, master_scrape_log.json, production_scrape_log.json,
                scrape_log.json, scrape_output.txt, scrape.log, university_profiles.json
```
Also: `schema.sql` and `unisearch_dump.sql` → `data/db/`

### 4. Root cleanup — files to delete or move
- `main.py` — delete (uv stub: `def main(): print("Hello from unisearch!")`)
- `app.orig` — delete (old backup)
- `requirements.txt` — delete (superseded by pyproject.toml)
- `university-scraper-free/` — confirm what this is, likely old project folder
- `.bat` files (SETUP.bat, SETUP_AND_IMPORT.bat, START_WEBSITE.bat) — confirm
  if replaced by uv commands, then delete

### 5. tests/ — not started
Planned files:
- `tests/conftest.py` — fixtures (Flask test client, mocked DB, sample data)
- `tests/test_routes.py` — status codes, response shapes, 404s, missing params
- `tests/test_services.py` — unit tests for service layer logic in isolation

### 6. README.md — not started
Needs: setup instructions, env vars list, run commands, folder map

### 7. Docker — intentionally deferred
User requested this be done last.

---

## Key decisions made

- **No Docker yet** — user wants this last
- **scraper_utils canonical versions** — from scrape_all.py (most evolved)
- **data_import/db_config.py** — mirrors scrapers/scraper_utils.py pattern
- **university_knowledge.py** — stays in root for now (or moves to scripts/scrapers/
  once remaining scrapers are placed — all scrapers import it)
- **api_compare fix** — no longer calls test_client() internally, uses service directly
- **hatchling packages config** — `["app", "migrations"]` not `["unisearch"]`

---

## .env vars required

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=unisearch
DB_USER=postgres
DB_PASSWORD=your_password_here
GROQ_API_KEY=your_groq_key_here
SCORECARD_API_KEY=your_scorecard_key_here   # get free at api.data.gov/signup
FLASK_ENV=development
PORT=5000
```

---

## To start next session

Paste this file and say:
> "Here's my progress doc. Let's continue the UniSearch refactor.
>  Start with the 5 remaining scraper files that need to go into scripts/scrapers/."

Upload these files at the start:
- `carnegie_fetcher.py` (already uploaded previously — has hardcoded config)
- `catalog_url_scraper.py` (already uploaded)
- `scrape_from_csv.py` (already uploaded)
- `scrape_programs.py` (already uploaded)
- `unt_catalog_import.py` (already uploaded)
- `university_knowledge.py` (not yet uploaded — needed for scraper imports)
