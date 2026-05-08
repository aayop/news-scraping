"""
Main Scraper — runs all scrapers and saves raw data to the Bronze layer
Usage: python main_scraper.py
"""

import json
import os
import sys
from datetime import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from hespress_scraper   import run_scraper as scrape_hespress
from bbc_scraper        import run_scraper as scrape_bbc
from barlamane_scraper  import run_scraper as scrape_barlamane
from akhbarona_scraper  import run_scraper as scrape_akhbarona
from aljazeera_scraper  import run_scraper as scrape_aljazeera
from reuters_scraper    import run_scraper as scrape_reuters

BRONZE_DIR = "../data_lake/bronze"


def save_to_bronze(articles, source):
    os.makedirs(BRONZE_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{BRONZE_DIR}/{source}_{timestamp}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Saved {len(articles)} articles → {filename}")
    return filename


def main():
    print("=" * 60)
    print("🚀 Starting News Scraper Pipeline — 6 Sources")
    print(f"   Time: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    scrapers = [
        ("Hespress",   scrape_hespress),
        ("BBC News",   scrape_bbc),
        ("Barlamane",  scrape_barlamane),
        ("Akhbarona",  scrape_akhbarona),
        ("Al Jazeera", scrape_aljazeera),
        ("Reuters",    scrape_reuters),
    ]

    results = {}
    for i, (name, scraper_fn) in enumerate(scrapers, 1):
        print(f"\n📰 [{i}/{len(scrapers)}] Scraping {name}...")
        try:
            articles = scraper_fn(max_articles=50)
            save_to_bronze(articles, name.lower().replace(" ", "_"))
            results[name] = len(articles)
        except Exception as e:
            print(f"  ❌ Error scraping {name}: {e}")
            results[name] = 0

    total = sum(results.values())
    print("\n" + "=" * 60)
    print(f"✅ Pipeline complete! Total articles: {total}")
    for name, count in results.items():
        status = "✅" if count > 0 else "❌"
        print(f"   {status} {name}: {count} articles")
    print(f"\n   Bronze layer: {BRONZE_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()