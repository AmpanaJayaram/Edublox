"""
UniSearch — CSV Program Importer
==================================
Import programs from a CSV file directly into the database.
Use this when scraping misses programs or for manual corrections.

USAGE:
  Generate a pre-filled template for UNT:
    python import_programs_csv.py --template --id 3087 --output unt_programs.csv

  Import a CSV:
    python import_programs_csv.py --id 3087 --file unt_programs.csv

  Import and clear existing first:
    python import_programs_csv.py --id 3087 --file unt_programs.csv --reset

CSV COLUMNS:
  name, category, degree_level, description, program_url,
  is_featured (true/false), top20_rank (1-20), reputation_note

REQUIREMENTS:
  pip install psycopg2-binary
"""

from pathlib import Path as _Path
import sys as _sys
_sys.path.insert(0, str(_Path(__file__).parent))
from db_config import DB_CONFIG, get_conn


import argparse, csv, os, sys
import psycopg2
import psycopg2.extras

HEADER = ["name","category","degree_level","description",
          "program_url","is_featured","top20_rank","reputation_note"]

# Pre-filled UNT program data with verified real URLs
UNT_PROGRAMS = [
    # name | category | level | description | url | featured | rank | reputation_note
    ["Bachelor of Music in Jazz Studies","Arts & Humanities","Undergraduate",
     "The first accredited jazz program in the US, offering performance, composition, and jazz education tracks.",
     "https://music.unt.edu/areas-of-study/jazz-studies","true","1",
     "Ranked #1 jazz program in the nation; first accredited jazz program in the US with alumni including Pat Metheny and Don Henley."],
    ["Master of Music in Jazz Studies","Arts & Humanities","Graduate",
     "Advanced graduate training in jazz performance, composition, and pedagogy.",
     "https://music.unt.edu/areas-of-study/jazz-studies","false","2",""],
    ["Doctor of Musical Arts","Arts & Humanities","Doctoral",
     "Terminal degree in music with concentrations in performance, composition, and conducting.",
     "https://music.unt.edu/academics/graduate/doctoral","false","3",""],
    ["Bachelor of Music","Arts & Humanities","Undergraduate",
     "Comprehensive undergraduate music program in one of the largest music schools in the US.",
     "https://music.unt.edu/degrees-programs","false","4",""],
    ["Master of Science in Data Science","STEM","Graduate",
     "Nationally ranked MS program focusing on machine learning, statistical modeling, and data visualization.",
     "https://datascience.unt.edu","false","5",""],
    ["Bachelor of Science in Data Science","STEM","Undergraduate",
     "Undergraduate program combining statistics, computing, and domain-specific data analysis.",
     "https://datascience.unt.edu","false","",""],
    ["Bachelor of Science in Computer Science","STEM","Undergraduate",
     "ABET-accredited CS program with specializations in AI, cybersecurity, and software engineering.",
     "https://cs.unt.edu","false","11",""],
    ["Master of Science in Computer Science","STEM","Graduate",
     "Graduate CS program with research focus in AI, data mining, and distributed systems.",
     "https://cs.unt.edu/graduate","false","15",""],
    ["Doctor of Philosophy in Computer Science","STEM","Doctoral",
     "PhD program with research concentrations in AI, cybersecurity, data science, and bioinformatics.",
     "https://cs.unt.edu/graduate/phd","false","",""],
    ["Bachelor of Science in Merchandising","Business","Undergraduate",
     "One of the top merchandising programs in the US focusing on fashion, retail, and supply chain management.",
     "https://www.unt.edu/academics/programs/merchandising","false","6",""],
    ["Master of Business Administration","Business","Graduate",
     "AACSB-accredited MBA with concentrations in finance, marketing, supply chain, and entrepreneurship.",
     "https://cob.unt.edu/graduate/mba","false","13",""],
    ["Bachelor of Business Administration in Accounting","Business","Undergraduate",
     "AACSB-accredited accounting program preparing students for CPA and advisory careers.",
     "https://cob.unt.edu/accounting","false","",""],
    ["Bachelor of Business Administration in Finance","Business","Undergraduate",
     "Finance program covering investment management, corporate finance, and financial analysis.",
     "https://cob.unt.edu/finance","false","",""],
    ["Bachelor of Business Administration in Marketing","Business","Undergraduate",
     "Marketing program with focus on digital marketing, consumer behavior, and brand management.",
     "https://cob.unt.edu/marketing","false","",""],
    ["Master of Science in Accounting","Business","Graduate",
     "Accelerated MS program preparing graduates for CPA licensure and accounting leadership.",
     "https://cob.unt.edu/graduate/ms-accounting","false","",""],
    ["Bachelor of Fine Arts in Studio Art","Arts & Humanities","Undergraduate",
     "Comprehensive studio art program with concentrations in painting, sculpture, ceramics, and printmaking.",
     "https://art.unt.edu","false","8",""],
    ["Master of Fine Arts in Studio Art","Arts & Humanities","Graduate",
     "Terminal studio art degree with concentrations in ceramics, drawing, painting, jewelry, photography, and sculpture.",
     "https://art.unt.edu/mfa","false","19",""],
    ["Bachelor of Arts in Radio, Television and Film","Arts & Humanities","Undergraduate",
     "Award-winning RTVF program with professional broadcast, film production, and digital media facilities.",
     "https://rtvf.unt.edu","false","10",""],
    ["Master of Arts in Digital Media Studies","Arts & Humanities","Graduate",
     "Interdisciplinary program exploring digital culture, media production, and communication theory.",
     "https://digital.unt.edu","false","13",""],
    ["Bachelor of Arts in Journalism","Arts & Humanities","Undergraduate",
     "Award-winning journalism program with convergence media training and student-run newsroom.",
     "https://journalism.unt.edu","false","17",""],
    ["Master of Arts in Journalism","Arts & Humanities","Graduate",
     "Advanced journalism degree focusing on investigative reporting, digital journalism, and media management.",
     "https://journalism.unt.edu","false","",""],
    ["Bachelor of Arts in English","Arts & Humanities","Undergraduate",
     "English program with concentrations in literature, creative writing, linguistics, and professional writing.",
     "https://english.unt.edu","false","",""],
    ["Bachelor of Arts in History","Arts & Humanities","Undergraduate",
     "History program covering US, world, and public history with research methodology training.",
     "https://history.unt.edu","false","",""],
    ["Bachelor of Arts in Art History","Arts & Humanities","Undergraduate",
     "Art history program covering global art movements with museum studies and curatorial training.",
     "https://art.unt.edu/art-history","false","",""],
    ["Master of Arts in Art History","Arts & Humanities","Graduate",
     "Graduate art history program with research focus on contemporary and historical art criticism.",
     "https://art.unt.edu/art-history/graduate","false","",""],
    ["Master of Music in Music Education","Arts & Humanities","Graduate",
     "Graduate music education program for K-12 music teachers and music education researchers.",
     "https://music.unt.edu/music-education","false","",""],
    ["Doctor of Philosophy in Audiology","Health & Medicine","Doctoral",
     "AuD/PhD program nationally recognized for clinical training and hearing science research.",
     "https://hhp.unt.edu/audiology","false","12",""],
    ["Master of Public Health","Health & Medicine","Graduate",
     "CEPH-accredited MPH program preparing public health professionals for community and global health.",
     "https://publichealth.unt.edu","false","9",""],
    ["Doctor of Philosophy in Public Health","Health & Medicine","Doctoral",
     "Advanced research degree in public health with concentrations in epidemiology and health behavior.",
     "https://publichealth.unt.edu/phd","false","",""],
    ["Bachelor of Science in Kinesiology","Health & Medicine","Undergraduate",
     "Kinesiology program focusing on exercise science, sports management, and pre-professional health training.",
     "https://hhp.unt.edu/kinesiology","false","18",""],
    ["Master of Science in Kinesiology","Health & Medicine","Graduate",
     "Advanced kinesiology program covering exercise physiology, biomechanics, and sport psychology.",
     "https://hhp.unt.edu/kinesiology/graduate","false","",""],
    ["Bachelor of Science in Nutrition","Health & Medicine","Undergraduate",
     "Nutrition and dietetics program with RD exam preparation and clinical practice experience.",
     "https://hhp.unt.edu/nutrition","false","",""],
    ["Master of Science in Speech-Language Pathology","Health & Medicine","Graduate",
     "ASHA-accredited SLP program with clinical training in communication disorders.",
     "https://hhp.unt.edu/speech","false","",""],
    ["Master of Science in Information Science","STEM","Graduate",
     "Nationally ranked iSchool program focused on data management, digital libraries, and knowledge organization.",
     "https://unt.edu/lis","false","16",""],
    ["Doctor of Philosophy in Information Science","STEM","Doctoral",
     "Research doctoral program in information science with specializations in data curation and human-information interaction.",
     "https://unt.edu/lis","false","",""],
    ["Bachelor of Science in Biology","STEM","Undergraduate",
     "Comprehensive biology program with strong research in ecology, molecular biology, and conservation.",
     "https://biol.unt.edu","false","14",""],
    ["Master of Science in Biology","STEM","Graduate",
     "Graduate biology program with research in environmental biology, genetics, and cell biology.",
     "https://biol.unt.edu/graduate","false","",""],
    ["Bachelor of Science in Chemistry","STEM","Undergraduate",
     "ACS-certified chemistry program with undergraduate research opportunities and professional preparation.",
     "https://chem.unt.edu","false","",""],
    ["Bachelor of Science in Environmental Science","STEM","Undergraduate",
     "Environmental science program integrating ecology, policy, GIS, and sustainability.",
     "https://ees.unt.edu","false","20",""],
    ["Bachelor of Science in Mathematics","STEM","Undergraduate",
     "Mathematics program with concentrations in applied mathematics, statistics, and mathematics education.",
     "https://math.unt.edu","false","",""],
    ["Doctor of Philosophy in Mathematics","STEM","Doctoral",
     "PhD program with research areas in algebra, analysis, combinatorics, and applied mathematics.",
     "https://math.unt.edu/graduate","false","",""],
    ["Bachelor of Science in Physics","STEM","Undergraduate",
     "Physics program with modern laboratory facilities and research opportunities in astrophysics and condensed matter.",
     "https://physics.unt.edu","false","",""],
    ["Bachelor of Engineering Technology","Engineering","Undergraduate",
     "Applied engineering program covering electrical, mechanical, and construction technologies.",
     "https://cot.unt.edu","false","",""],
    ["Bachelor of Science in Mechanical and Energy Engineering","Engineering","Undergraduate",
     "ABET-accredited engineering program with focus on energy systems and sustainable design.",
     "https://engineering.unt.edu","false","",""],
    ["Bachelor of Science in Electrical Engineering","Engineering","Undergraduate",
     "ABET-accredited electrical engineering program covering circuits, signal processing, and power systems.",
     "https://engineering.unt.edu","false","",""],
    ["Master of Science in Engineering Systems","Engineering","Graduate",
     "Interdisciplinary engineering graduate program covering systems design and optimization.",
     "https://engineering.unt.edu/graduate","false","",""],
    ["Bachelor of Education in Early Childhood Education","Education","Undergraduate",
     "State-certified early childhood education program preparing teachers for PreK-4th grade.",
     "https://coe.unt.edu","false","",""],
    ["Bachelor of Education in Elementary Education","Education","Undergraduate",
     "State-certified elementary education program with field placements across Dallas-Fort Worth.",
     "https://coe.unt.edu","false","",""],
    ["Master of Education in Educational Leadership","Education","Graduate",
     "Prepares school administrators and educational leaders for K-12 and higher education settings.",
     "https://coe.unt.edu/educational-leadership","false","16",""],
    ["Doctor of Education in Educational Leadership","Education","Doctoral",
     "Ed.D. program preparing educational leaders for superintendent, principal, and university administrator roles.",
     "https://coe.unt.edu/doctoral","false","",""],
    ["Master of Science in Counseling","Education","Graduate",
     "CACREP-accredited counseling program with tracks in school, clinical mental health, and rehabilitation counseling.",
     "https://coe.unt.edu/counseling","false","",""],
    ["Bachelor of Arts in Psychology","Social Sciences","Undergraduate",
     "Comprehensive psychology program with research and applied learning opportunities.",
     "https://psych.unt.edu","false","",""],
    ["Master of Science in Psychology","Social Sciences","Graduate",
     "Graduate psychology program with specializations in clinical, cognitive, and behavioral psychology.",
     "https://psych.unt.edu/graduate","false","",""],
    ["Doctor of Philosophy in Psychology","Social Sciences","Doctoral",
     "APA-accredited PhD program with research strengths in health psychology and behavioral neuroscience.",
     "https://psych.unt.edu/doctoral","false","",""],
    ["Bachelor of Arts in Sociology","Social Sciences","Undergraduate",
     "Sociology program covering social structures, inequality, criminology, and research methods.",
     "https://sociology.unt.edu","false","",""],
    ["Bachelor of Arts in Anthropology","Social Sciences","Undergraduate",
     "Anthropology program offering tracks in cultural, biological, linguistic, and applied anthropology.",
     "https://sociology.unt.edu/anthropology","false","",""],
    ["Bachelor of Social Work","Social Sciences","Undergraduate",
     "CSWE-accredited social work program with field placements across the Dallas-Fort Worth Metroplex.",
     "https://socialwork.unt.edu","false","",""],
    ["Master of Social Work","Social Sciences","Graduate",
     "Advanced social work practice with concentrations in mental health and community practice.",
     "https://socialwork.unt.edu/graduate","false","",""],
    ["Bachelor of Arts in Political Science","Law & Policy","Undergraduate",
     "Political science program covering American government, international relations, and public policy.",
     "https://polisci.unt.edu","false","",""],
    ["Master of Public Administration","Law & Policy","Graduate",
     "NASPAA-accredited MPA program for public sector and nonprofit management careers.",
     "https://pubadmin.unt.edu","false","",""],
    ["Master of Science in Emergency Management and Disaster Science","Law & Policy","Graduate",
     "Specialized program in emergency preparedness, disaster response, and homeland security.",
     "https://ems.unt.edu","false","",""],
    ["Bachelor of Arts in Criminal Justice","Law & Policy","Undergraduate",
     "Criminal justice program covering law enforcement, courts, corrections, and crime prevention.",
     "https://cj.unt.edu","false","",""],
    ["Master of Arts in International Studies","Social Sciences","Graduate",
     "Interdisciplinary graduate program in international relations, cultural studies, and global issues.",
     "https://interdisciplinary.unt.edu","false","",""],
]




def generate_template(uid: int, output_path: str):
    """Generate a CSV template. If uid given, use UNT data."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT name FROM universities WHERE id=%s", (uid,))
    row = cur.fetchone()
    cur.close(); conn.close()
    uni_name = row["name"] if row else f"University (ID {uid})"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(HEADER)
        # Use UNT data or generic examples
        rows = UNT_PROGRAMS if row and "north texas" in uni_name.lower() else [
            ["Bachelor of Science in Computer Science","STEM","Undergraduate",
             "Description here","https://cs.university.edu","false","",""],
            ["Master of Science in Data Science","STEM","Graduate",
             "Description here","https://ds.university.edu","false","",""],
        ]
        for r in rows:
            writer.writerow(r)

    print(f"\n✅ Template generated: {output_path}")
    print(f"   University: {uni_name}")
    print(f"   Programs:   {len(rows)} rows pre-filled")
    print(f"\n   Next steps:")
    print(f"   1. Open {output_path} in Excel")
    print(f"   2. Edit/add/remove rows as needed")
    print(f"   3. Run: python import_programs_csv.py --id {uid} --file {output_path}")


def import_csv(uid: int, filepath: str, reset: bool = False):
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}"); sys.exit(1)

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT name FROM universities WHERE id=%s", (uid,))
    row = cur.fetchone()
    if not row:
        print(f"ERROR: University ID {uid} not found"); sys.exit(1)
    uni_name = row["name"]
    print(f"\nImporting into: {uni_name} (ID: {uid})")

    if reset:
        cur.execute("DELETE FROM university_programs WHERE university_id=%s", (uid,))
        print("  🗑  Cleared existing programs")

    inserted = skipped = errors = 0
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 2):
            name       = (row.get("name") or "").strip()
            category   = (row.get("category") or "STEM").strip()
            deg_level  = (row.get("degree_level") or "Undergraduate").strip()
            desc       = (row.get("description") or "").strip() or None
            prog_url   = (row.get("program_url") or "").strip() or None
            is_feat    = str(row.get("is_featured","")).lower() in ("true","1","yes")
            top20_s    = (row.get("top20_rank") or "").strip()
            top20      = int(top20_s) if top20_s.isdigit() else None
            rep_note   = (row.get("reputation_note") or "").strip() or None
            if not name:
                continue
            try:
                cur.execute("""
                    INSERT INTO university_programs
                        (university_id, name, category, degree_level, description,
                         is_featured, top20_rank, reputation_note, program_url, generated_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'manual_csv')
                    ON CONFLICT DO NOTHING RETURNING id
                """, (uid, name[:200], category[:100], deg_level[:50],
                      desc, is_feat, top20, rep_note, prog_url))
                if cur.fetchone():
                    print(f"  ✅ [{deg_level:<14}] {name}")
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  ❌ Row {i}: {e}"); errors += 1

    conn.commit(); cur.close(); conn.close()
    print(f"\n{'='*60}")
    print(f"  ✅ Inserted: {inserted}  |  Skipped: {skipped}  |  Errors: {errors}")
    print(f"{'='*60}\n")


def main():
    ap = argparse.ArgumentParser(description="UniSearch CSV Importer")
    ap.add_argument("--id",       "-u", type=int, required=True)
    ap.add_argument("--file",     "-f", type=str)
    ap.add_argument("--template", action="store_true")
    ap.add_argument("--output",   "-o", type=str, default="programs_template.csv")
    ap.add_argument("--reset",    action="store_true")
    args = ap.parse_args()

    if args.template:
        generate_template(args.id, args.output)
    elif args.file:
        import_csv(args.id, args.file, reset=args.reset)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
