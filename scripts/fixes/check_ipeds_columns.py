import csv
print("=== ipeds_directory.csv ===")
with open("ipeds_directory.csv", encoding="latin-1") as f:
    r = csv.DictReader(f)
    print("Headers:", list(r.fieldnames)[:20])
    row = next(r)
    for k,v in list(row.items())[:6]: print(f"  {repr(k)}: {repr(v)}")

print()
print("=== ipeds_completions.csv ===")
with open("ipeds_completions.csv", encoding="latin-1") as f:
    r = csv.DictReader(f)
    print("Headers:", list(r.fieldnames)[:20])
    row = next(r)
    for k,v in list(row.items())[:8]: print(f"  {repr(k)}: {repr(v)}")
