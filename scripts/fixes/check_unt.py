import json

# Check production_scrape_log.json
print("=== Checking production_scrape_log.json ===")
try:
    with open('production_scrape_log.json', 'r') as f:
        data = json.load(f)
    unt = [u for u in data if 'north texas' in u['name'].lower()]
    print(f"Found {len(unt)} UNT entries:")
    for u in unt:
        print(f"  {u['name']}: {u.get('programs', 0)} programs")
except FileNotFoundError:
    print("  File not found")

# Check master_scrape_log.json
print("\n=== Checking master_scrape_log.json ===")
try:
    with open('master_scrape_log.json', 'r') as f:
        data = json.load(f)
    unt = [u for u in data if 'north texas' in u['name'].lower()]
    print(f"Found {len(unt)} UNT entries:")
    for u in unt:
        print(f"  {u['name']}: {u.get('programs', 0)} programs")
except FileNotFoundError:
    print("  File not found")

# Check if there's a programs table or CSV
print("\n=== Looking for program data sources ===")
import os
for f in os.listdir('.'):
    if 'program' in f.lower() and (f.endswith('.json') or f.endswith('.csv')):
        print(f"  Found: {f}")
