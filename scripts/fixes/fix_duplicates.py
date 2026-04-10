import json
from pathlib import Path

# Find the most recent programs file
output_dir = Path("output")
programs_files = list(output_dir.glob("programs_*.json"))
if not programs_files:
    # Try university-scraper-free/output
    output_dir = Path("university-scraper-free/output")
    programs_files = list(output_dir.glob("programs_*.json"))

latest_file = max(programs_files, key=lambda f: f.stat().st_mtime)
print(f"Processing: {latest_file}")

with open(latest_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

total_before = 0
total_after = 0

for uni_name, uni_data in data.items():
    programs = uni_data.get('programs', [])
    total_before += len(programs)
    
    # Deduplicate by name + degree_type
    seen = set()
    unique = []
    
    for p in programs:
        name = p.get('name', '').strip().lower()
        degree = p.get('degree_type', 'Other').strip()
        
        # Create unique key
        key = f"{name}|{degree}"
        
        if key not in seen:
            seen.add(key)
            unique.append(p)
    
    uni_data['programs'] = unique
    uni_data['program_count'] = len(unique)
    total_after += len(unique)

# Save cleaned data
output_file = output_dir / "programs_cleaned.json"
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2)

print(f"Before: {total_before:,} programs")
print(f"After:  {total_after:,} programs")
print(f"Removed: {total_before - total_after:,} duplicates")
print(f"Saved to: {output_file}")
