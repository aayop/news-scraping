"""
Barlamane News Scraper
Collects articles from barlamane.com (Moroccan news site)
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,fr;q=0.9,en;q=0.8",
}

BASE_URL = "https://barlamane.com"

CATEGORIES = [
    f"{BASE_URL}/category/الأخبار/سياسة/",
    f"{BASE_URL}/category/الأخبار/مجتمع/",
    f"{BASE_URL}/category/الأخبار/اقتصاد/",
    f"{BASE_URL}/category/الأخبار/رياضة/",
    f"{BASE_URL}/category/الأخبار/خارجة-الحدود/",
]


def get_article_links(category_url, max_articles=20):
    links = []
    try:
        response = requests.get(category_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith("/"):
                href = BASE_URL + href
            if not href.startswith(BASE_URL):
                continue
            if any(skip in href for skip in ["/category/", "/page/", "/tag/", "/author/", "/fr", "/feed", "/contact", "/about"]):
                continue
            path = href[len(BASE_URL):].strip("/")
            if not path or path.count("/") > 1:
                continue
            if href not in links:
                links.append(href)
            if len(links) >= max_articles:
                break

    except requests.RequestException as e:
        logger.error(f"Failed to fetch {category_url}: {e}")

    return links


def scrape_article(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Title
        title = None
        for selector in ["h1.entry-title", "h1.post-title", "h1"]:
            tag = soup.select_one(selector)
            if tag:
                title = tag.get_text(strip=True)
                break

        # Author
        author = None
        author_tag = soup.find(class_=lambda c: c and "author" in c.lower())
        if author_tag:
            author = author_tag.get_text(strip=True)

        # Date
        pub_date = None
        date_tag = soup.find("time")
        if date_tag:
            pub_date = date_tag.get("datetime") or date_tag.get_text(strip=True)

        # Category
        category = None
        cat_tag = soup.find(class_=lambda c: c and "category" in c.lower())
        if cat_tag:
            category = cat_tag.get_text(strip=True)

        # Content
        content = ""
        for selector in ["div.entry-content", "div.post-content", "div.article-content", "article"]:
            content_div = soup.select_one(selector)
            if content_div:
                for tag in content_div.find_all(["script", "style", "nav", "aside"]):
                    tag.decompose()
                content = content_div.get_text(separator=" ", strip=True)
                break

        if not title or not content:
            logger.warning(f"Skipping (missing title or content): {url}")
            return None

        return {
            "title": title,
            "author": author,
            "publication_date": pub_date,
            "category": category,
            "content": content[:5000],
            "source": "Barlamane",
            "url": url,
            "language": "ar",
            "scraped_at": datetime.utcnow().isoformat(),
        }

    except requests.RequestException as e:
        logger.error(f"Failed to scrape {url}: {e}")
        return None


def run_scraper(max_articles=50):
    all_articles = []
    seen_urls = set()

    for category_url in CATEGORIES:
        if len(all_articles) >= max_articles:
            break

        logger.info(f"Fetching links from: {category_url}")
        links = get_article_links(category_url, max_articles=10)
        logger.info(f"Found {len(links)} links")

        for url in links:
            if url in seen_urls:
                continue
            seen_urls.add(url)

            logger.info(f"Scraping: {url}")
            article = scrape_article(url)
            if article:
                all_articles.append(article)
                logger.info(f"✓ {article['title'][:60]}")

            time.sleep(1)

            if len(all_articles) >= max_articles:
                break

    logger.info(f"Total articles scraped: {len(all_articles)}")
    return all_articles


if __name__ == "__main__":
    articles = run_scraper(max_articles=50)
    output_file = "barlamane_raw.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Done! Scraped {len(articles)} articles → saved to {output_file}")