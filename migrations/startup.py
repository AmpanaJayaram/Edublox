"""
migrations/startup.py
=====================
Idempotent schema additions that run once at app startup.

These are ADD COLUMN IF NOT EXISTS and CREATE TABLE IF NOT EXISTS statements —
safe to run on every boot. For destructive or versioned migrations, add
numbered SQL files under migrations/versions/ and run them explicitly.
"""

import traceback
from app.db.connection import get_conn


def run_startup_migrations() -> None:
    """Add any missing columns and tables to bring the schema up to date."""

    _CARNEGIE_COLS = [
        ("tier_label",          "TEXT"),
        ("tier_reason",         "TEXT"),
        ("institution_control", "TEXT"),
        ("research_spending",   "BIGINT"),
        ("research_doctorates", "INTEGER"),
        ("faculty_count",       "INTEGER"),
        ("total_enrollment",    "INTEGER"),
        ("designations",        "TEXT"),
        ("evidence_snippet",    "TEXT"),
        ("dorm_capacity",       "INTEGER"),
    ]

    _PROGRAM_COLS = [
        ("degree_level", "TEXT"),
        ("top20_rank",   "INTEGER"),
        ("program_url",  "TEXT"),
    ]

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                # 1. Add missing columns to carnegie_classifications
                for col, typ in _CARNEGIE_COLS:
                    cur.execute(f"""
                        ALTER TABLE carnegie_classifications
                        ADD COLUMN IF NOT EXISTS {col} {typ}
                    """)
                conn.commit()

                # 2. Backfill tier_label where still NULL
                cur.execute("""
                    UPDATE carnegie_classifications
                    SET tier_label = CASE
                        WHEN basic_classification ILIKE '%doctoral%very high%'
                          OR basic_classification ILIKE '%R1%'
                          OR institutional_classification ILIKE '%R1%'
                          OR institutional_classification ILIKE '%doctoral%very high%' THEN 'T1'
                        WHEN basic_classification ILIKE '%doctoral%high%'
                          OR basic_classification ILIKE '%R2%'
                          OR institutional_classification ILIKE '%R2%'
                          OR institutional_classification ILIKE '%doctoral%high%' THEN 'T2'
                        ELSE 'T3'
                    END
                    WHERE tier_label IS NULL
                """)
                conn.commit()

                # 3. Add catalog_url to universities if missing
                cur.execute("""
                    ALTER TABLE universities
                    ADD COLUMN IF NOT EXISTS catalog_url TEXT
                """)
                conn.commit()

                # 4. Create university_programs table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS university_programs (
                        id               SERIAL PRIMARY KEY,
                        university_id    INTEGER NOT NULL
                                         REFERENCES universities(id) ON DELETE CASCADE,
                        name             TEXT NOT NULL,
                        category         TEXT,
                        degree_level     TEXT,
                        description      TEXT,
                        is_featured      BOOLEAN DEFAULT FALSE,
                        top20_rank       INTEGER,
                        reputation_note  TEXT,
                        program_url      TEXT,
                        generated_by     TEXT DEFAULT 'groq',
                        created_at       TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_programs_university
                    ON university_programs(university_id)
                """)

                # 5. Add any missing columns to university_programs
                for col, typ in _PROGRAM_COLS:
                    cur.execute(f"""
                        ALTER TABLE university_programs
                        ADD COLUMN IF NOT EXISTS {col} {typ}
                    """)
                conn.commit()

    except Exception:
        print("[startup migration warning]")
        traceback.print_exc()
