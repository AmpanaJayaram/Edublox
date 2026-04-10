"""Deeper diagnostic — paste output to Claude."""
import csv, os

for fname, enc in [("ipeds_directory.csv", "utf-8-sig"), 
                   ("ipeds_directory.csv", "latin-1"),
                   ("ipeds_completions.csv", "utf-8-sig"),
                   ("ipeds_completions.csv", "latin-1")]:
    print(f"\n=== {fname} with {enc} ===")
    try:
        size = os.path.getsize(fname)
        print(f"  File size: {size:,} bytes")
        with open(fname, encoding=enc, errors="replace") as f:
            # Read raw first line
            first_line = f.readline()
            print(f"  First 120 chars: {repr(first_line[:120])}")
            f.seek(0)
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            print(f"  Headers[0:6]: {headers[:6]}")
            # Try reading rows
            count = 0
            errors = 0
            for row in reader:
                try:
                    uid = row.get("UNITID") or row.get("unitid") or ""
                    if uid.strip().isdigit():
                        count += 1
                except Exception as e:
                    errors += 1
                    if errors == 1:
                        print(f"  First row error: {e}")
                if count + errors > 5:
                    break
            print(f"  Valid rows in first 6: {count}, errors: {errors}")
    except Exception as e:
        print(f"  FAILED: {e}")
