"""
Fix Duplicate Programs in PostgreSQL
=====================================
Database: unisearch
Table: university_programs
"""

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "unisearch",
    "user": "postgres",
    "password": "2000",
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)

def main():
    print("=" * 50)
    print("PostgreSQL Duplicate Program Remover")
    print("=" * 50)
    
    conn = get_conn()
    cur = conn.cursor()
    
    # 1. Show current counts
    cur.execute("SELECT COUNT(*) as total FROM university_programs")
    total_before = cur.fetchone()['total']
    print(f"\nTotal programs in database: {total_before}")
    
    # 2. Show UNT specifically
    print("\n--- UNT Program Counts ---")
    cur.execute("""
        SELECT u.name, COUNT(p.id) as program_count
        FROM universities u
        LEFT JOIN university_programs p ON u.id = p.university_id
        WHERE u.name ILIKE '%north texas%'
        GROUP BY u.id, u.name
        ORDER BY u.name
    """)
    for row in cur.fetchall():
        print(f"  {row['name']}: {row['program_count']} programs")
    
    # 3. Show duplicate examples
    print("\n--- Sample Duplicates (same name + degree_level per university) ---")
    cur.execute("""
        SELECT university_id, name, degree_level, COUNT(*) as copies
        FROM university_programs
        GROUP BY university_id, name, degree_level
        HAVING COUNT(*) > 1
        ORDER BY copies DESC
        LIMIT 10
    """)
    duplicates = cur.fetchall()
    
    if not duplicates:
        print("  No exact duplicates found!")
        
        # Check for duplicates by name only (different degree levels showing as same)
        print("\n--- Checking duplicates by name only ---")
        cur.execute("""
            SELECT university_id, name, COUNT(*) as copies
            FROM university_programs
            GROUP BY university_id, name
            HAVING COUNT(*) > 1
            ORDER BY copies DESC
            LIMIT 10
        """)
        name_dups = cur.fetchall()
        if name_dups:
            print("  Found programs with same name but different degree_level:")
            for row in name_dups:
                print(f"    '{row['name']}': {row['copies']} copies")
        
        conn.close()
        return
    
    for row in duplicates:
        print(f"  '{row['name']}' ({row['degree_level']}): {row['copies']} copies")
    
    # 4. Count total duplicates
    cur.execute("""
        SELECT COUNT(*) as dup_count FROM (
            SELECT university_id, name, degree_level
            FROM university_programs
            GROUP BY university_id, name, degree_level
            HAVING COUNT(*) > 1
        ) dups
    """)
    dup_types = cur.fetchone()['dup_count']
    print(f"\nTotal duplicate program types: {dup_types}")
    
    # 5. Ask to fix
    print("\n" + "=" * 50)
    response = input("Remove duplicates? Type 'yes' to confirm: ")
    
    if response.lower() != 'yes':
        print("Cancelled.")
        conn.close()
        return
    
    # 6. Delete duplicates - keep the one with lowest ID
    print("\nRemoving duplicates...")
    cur.execute("""
        DELETE FROM university_programs
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM university_programs
            GROUP BY university_id, name, degree_level
        )
    """)
    deleted = cur.rowcount
    conn.commit()
    
    # 7. Show results
    cur.execute("SELECT COUNT(*) as total FROM university_programs")
    total_after = cur.fetchone()['total']
    
    print(f"\n--- Results ---")
    print(f"Programs before: {total_before}")
    print(f"Programs after:  {total_after}")
    print(f"Deleted:         {deleted} duplicates")
    
    # 8. Show UNT after fix
    print("\n--- UNT After Fix ---")
    cur.execute("""
        SELECT u.name, COUNT(p.id) as program_count
        FROM universities u
        LEFT JOIN university_programs p ON u.id = p.university_id
        WHERE u.name ILIKE '%north texas%'
        GROUP BY u.id, u.name
        ORDER BY u.name
    """)
    for row in cur.fetchall():
        print(f"  {row['name']}: {row['program_count']} programs")
    
    conn.close()
    print("\nDone! Refresh your website to see the changes.")

if __name__ == "__main__":
    main()
