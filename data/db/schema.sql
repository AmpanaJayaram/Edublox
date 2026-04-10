-- ============================================================
--  UniSearch — PostgreSQL Schema
--  Run with: psql -U postgres -d unisearch -f schema.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS unisearch;

-- Universities master table
CREATE TABLE IF NOT EXISTS universities (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    city            TEXT,
    state           TEXT,
    school_url      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_universities_name ON universities(name);
CREATE INDEX IF NOT EXISTS idx_universities_state ON universities(state);

-- ── Facts table (provenance model) ──────────────────────────
-- Every single data point stored here with full source info
CREATE TABLE IF NOT EXISTS university_facts (
    id              SERIAL PRIMARY KEY,
    university_id   INTEGER NOT NULL REFERENCES universities(id) ON DELETE CASCADE,
    section         TEXT NOT NULL,   -- 'tuition', 'admissions', 'outcomes', etc.
    label           TEXT NOT NULL,   -- 'In-state tuition', 'Acceptance Rate', etc.
    value           TEXT,            -- '12,532' or '72.3%' (always stored as text)
    value_numeric   NUMERIC,         -- numeric copy for filtering/sorting
    source_url      TEXT,            -- verification link
    extracted_at    TIMESTAMPTZ DEFAULT NOW(),
    extractor       TEXT,            -- 'scorecard_api_v1', 'web_scraper_v3', etc.
    confidence      NUMERIC(3,2),    -- 0.00 to 1.00
    notes           TEXT,
    UNIQUE(university_id, section, label)
);

CREATE INDEX IF NOT EXISTS idx_facts_university ON university_facts(university_id);
CREATE INDEX IF NOT EXISTS idx_facts_section    ON university_facts(section);

-- ── Rankings & Awards ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS university_rankings (
    id              SERIAL PRIMARY KEY,
    university_id   INTEGER NOT NULL REFERENCES universities(id) ON DELETE CASCADE,
    type            TEXT NOT NULL,   -- 'ranking' or 'award'
    description     TEXT NOT NULL,
    source_url      TEXT,
    extracted_at    TIMESTAMPTZ DEFAULT NOW(),
    extractor       TEXT
);

CREATE INDEX IF NOT EXISTS idx_rankings_university ON university_rankings(university_id);

-- ── Carnegie Classifications ─────────────────────────────────
CREATE TABLE IF NOT EXISTS carnegie_classifications (
    id                          SERIAL PRIMARY KEY,
    university_id               INTEGER NOT NULL REFERENCES universities(id) ON DELETE CASCADE,
    ipeds_unitid                TEXT,
    institutional_classification TEXT,
    student_access_earnings     TEXT,
    research_activity           TEXT,
    size_setting                TEXT,
    basic_classification        TEXT,
    carnegie_page_url           TEXT,   -- verification link
    extracted_at                TIMESTAMPTZ DEFAULT NOW(),
    confidence                  NUMERIC(3,2),
    match_method                TEXT,    -- 'unitid', 'name_state', 'needs_review'
    notes                       TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_carnegie_university ON carnegie_classifications(university_id);

-- ── Scrape log (for resume/checkpoint) ──────────────────────
CREATE TABLE IF NOT EXISTS scrape_log (
    id              SERIAL PRIMARY KEY,
    university_id   INTEGER NOT NULL REFERENCES universities(id) ON DELETE CASCADE,
    scraper         TEXT NOT NULL,   -- 'web_scraper', 'carnegie_fetcher', 'scorecard_api'
    status          TEXT NOT NULL,   -- 'success', 'failed', 'skipped'
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    facts_found     INTEGER DEFAULT 0,
    error_msg       TEXT
);

CREATE INDEX IF NOT EXISTS idx_scrape_log_university ON scrape_log(university_id);
CREATE INDEX IF NOT EXISTS idx_scrape_log_scraper    ON scrape_log(scraper, status);
