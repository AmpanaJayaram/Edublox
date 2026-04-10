"""
debug_unt.py - Debug scraper for UNT
Run: python debug_unt.py
This will show EXACTLY what's happening step by step.
"""
import requests, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
}

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=25, allow_redirects=True)
        print(f"    GET {url[:80]} → {r.status_code} ({len(r.text)} chars)")
        if r.status_code == 200:
            return r.url, BeautifulSoup(r.text, 'html.parser'), r.text
    except Exception as e:
        print(f"    ERROR {url[:80]} → {e}")
    return None, None, ""

print("="*60)
print("STEP 1: Fetch catalog.unt.edu")
print("="*60)
final_url, soup, html = fetch("https://catalog.unt.edu")

if not soup:
    print("FAILED to fetch catalog.unt.edu")
    exit()

print(f"Final URL: {final_url}")
print(f"Is Acalog: {'preview_program.php' in html or 'catoid=' in html}")

# Count preview_program links on homepage
homepage_progs = len(soup.find_all('a', href=re.compile(r'preview_program\.php')))
print(f"preview_program.php links on homepage: {homepage_progs}")

print()
print("="*60)
print("STEP 2: All navoid links on catalog.unt.edu")
print("="*60)
navoid_links = []
for a in soup.find_all('a', href=True):
    if 'navoid=' in a['href']:
        full = urljoin("https://catalog.unt.edu", a['href'])
        text = a.get_text(strip=True)[:60]
        navoid_links.append((full, text))
        print(f"  {text:<50} → {full}")

print(f"\nTotal navoid links: {len(navoid_links)}")

print()
print("="*60)
print("STEP 3: Try each navoid link and count programs")
print("="*60)

results = []
seen = set()
for url, text in navoid_links:
    if url in seen:
        continue
    seen.add(url)
    _, try_soup, _ = fetch(url)
    if not try_soup:
        continue
    count = len(try_soup.find_all('a', href=re.compile(r'preview_program\.php')))
    results.append((count, url, text))
    print(f"  {count:>4} programs: [{text[:50]}]")

print()
results.sort(reverse=True)
if results:
    best_count, best_url, best_text = results[0]
    print(f"BEST PAGE: {best_url}")
    print(f"Programs: {best_count}")
    
    if best_count > 0:
        print()
        print("="*60)
        print("STEP 4: Extract programs from best page")
        print("="*60)
        _, best_soup, _ = fetch(best_url)
        for a in best_soup.find_all('a', href=re.compile(r'preview_program\.php'))[:30]:
            print(f"  {a.get_text(strip=True)}")
else:
    print("No navoid links worked!")
