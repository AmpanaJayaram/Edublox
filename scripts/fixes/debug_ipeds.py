"""Run this to diagnose the IPEDS CSV files."""
import csv, os

print("=== Testing ipeds_completions.csv ===")
# Test with utf-8-sig (strips BOM automatically)
with open("ipeds_completions.csv", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    print("Headers:", list(reader.fieldnames)[:8])
    rows = [next(reader) for _ in range(3)]
    for row in rows:
        print("  UNITID:", row.get("UNITID","MISSING"))
        print("  CIPCODE:", row.get("CIPCODE","MISSING"))
        print("  AWLEVEL:", row.get("AWLEVEL","MISSING"))
        print("  CTOTALT:", row.get("CTOTALT","MISSING"))
        print()

print("=== Testing ipeds_directory.csv ===")
with open("ipeds_directory.csv", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    print("Headers:", list(reader.fieldnames)[:8])
    row = next(reader)
    print("  UNITID:", row.get("UNITID","MISSING"))
    print("  INSTNM:", row.get("INSTNM","MISSING"))
    print("  WEBADDR:", row.get("WEBADDR","MISSING"))

print("\n=== File sizes ===")
print(f"  completions: {os.path.getsize('ipeds_completions.csv')//1024//1024} MB")
print(f"  directory:   {os.path.getsize('ipeds_directory.csv')//1024} KB")
