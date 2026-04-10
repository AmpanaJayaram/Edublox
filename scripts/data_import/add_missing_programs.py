"""
add_missing_programs.py
=======================
Adds specific programs missing from UNT that the scraper missed.

Run: python add_missing_programs.py
"""

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))
from db_config import get_conn

MISSING_PROGRAMS = [
    {
        "university": "University of North Texas",
        "programs": [
            {
                "name": "Doctor of Philosophy in Data Science",
                "degree_level": "Doctoral",
                "category": "STEM",
                "program_url": "https://datascience.unt.edu/programs/phdds/index.html",
                "reputation_note": (
                    "Offered by the Anuradha and Vikas Sinha Department of Data Science at UNT, "
                    "one of the first dedicated PhD programs in Data Science in Texas. "
                    "Launched January 2025."
                ),
            },
            {
                "name": "Bachelor of Science in Data Science",
                "degree_level": "Undergraduate",
                "category": "STEM",
                "program_url": "https://datascience.unt.edu/programs/bsds/index.html",
            },
            {
                "name": "Master of Science in Data Science",
                "degree_level": "Graduate",
                "category": "STEM",
                "program_url": "https://datascience.unt.edu/programs/msds/index.html",
                "reputation_note": (
                    "Ranked 5th best Master's in Data Science in the U.S. by Fortune Magazine."
                ),
            },
        ],
    }
]


def main():
    conn = get_conn()
    cur  = conn.cursor()

    for entry in MISSING_PROGRAMS:
        cur.execute("SELECT id FROM universities WHERE name = %s", (entry["university"],))
        row = cur.fetchone()
        if not row:
            print(f"University not found: {entry['university']}")
            continue
        uid = row["id"]

        for p in entry["programs"]:
            cur.execute(
                "SELECT id FROM university_programs WHERE university_id=%s AND name=%s",
                (uid, p["name"]),
            )
            if cur.fetchone():
                print(f"  Already exists: {p['name']}")
                continue

            cur.execute(
                """
                INSERT INTO university_programs
                    (university_id, name, category, degree_level, description,
                     is_featured, top20_rank, reputation_note, program_url, generated_by)
                VALUES (%s, %s, %s, %s, NULL, FALSE, NULL, %s, %s, 'manual')
                """,
                (
                    uid,
                    p["name"],
                    p["category"],
                    p["degree_level"],
                    p.get("reputation_note"),
                    p.get("program_url"),
                ),
            )
            print(f"  ✅ Added: {p['name']}")

    conn.commit()
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
