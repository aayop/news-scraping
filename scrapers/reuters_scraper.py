"""
Reuters News Scraper
Collects articles from reuters.com using RSS feeds + HTML scraping
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import logging
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

BASE_URL = "https://www.reuters.com"

RSS_FEEDS = {
    "top_news":  "https://feeds.reuters.com/reuters/topNews",
    "world":     "https://feeds.reuters.com/Reuters/worldNews",
    "business":  "https://feeds.reuters.com/reuters/businessNews",
    "technology":"https://feeds.reuters.com/reuters/technologyNews",
}


def get_links_from_rss(feed_url, category):
    articles_meta = []
    try:
        response = requests.get(feed_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        channel = root.find("channel")
        if not channel:
            return []

        for item in channel.findall("item"):
            title_el = item.find("title")
            link_el  = item.find("link")
            date_el  = item.find("pubDate")
            desc_el  = item.find("description")

            title = title_el.text.strip() if title_el is not None else None
            link  = link_el.text.strip()  if link_el  is not None else None
            date  = date_el.text.strip()  if date_el  is not None else None
            desc  = desc_el.text.strip()  if desc_el  is not None else None

            if title and link:
                articles_meta.append({
                    "title": title,
                    "url": link,
                    "publication_date": date,
                    "description": desc,
                    "category": category,
                })

    except Exception as e:
        logger.error(f"Failed to parse RSS feed {feed_url}: {e}")

    return articles_meta


def scrape_article(meta):
    url = meta["url"]
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Author
        author = None
        author_tag = soup.find(class_=lambda c: c and "author" in c.lower())
        if author_tag:
            author = author_tag.get_text(strip=True)

        # Content — Reuters uses data-testid attributes
        content = ""
        for selector in [
            "[data-testid='paragraph-0']",
            "div.article-body__content",
            "div.StandardArticleBody_body",
            "article",
        ]:
            content_div = soup.select_one(selector)
            if content_div:
                for tag in content_div.find_all(["script", "style", "figure", "nav"]):
                    tag.decompose()
                paragraphs = content_div.find_all("p")
                if paragraphs:
                    content = " ".join(p.get_text(strip=True) for p in paragraphs)
                else:
                    content = content_div.get_text(separator=" ", strip=True)
                break

        if not content:
            # Fallback: use description from RSS
            content = meta.get("description", "")

        if not content:
            logger.warning(f"No content found for: {url}")
            return None

        return {
            "title": meta["title"],
            "author": author,
            "publication_date": meta.get("publication_date"),
            "category": meta.get("category"),
            "content": content[:5000],
            "source": "Reuters",
            "url": url,
            "language": "en",
            "scraped_at": datetime.utcnow().isoformat(),
        }

    except requests.RequestException as e:
        logger.error(f"Failed to scrape {url}: {e}")
        return None


def run_scraper(max_articles=50):
    all_articles = []
    seen_urls = set()

    for category, feed_url in RSS_FEEDS.items():
        if len(all_articles) >= max_articles:
            break

        logger.info(f"Fetching RSS feed: {category}")
        articles_meta = get_links_from_rss(feed_url, category)
        logger.info(f"Found {len(articles_meta)} items in RSS")

        for meta in articles_meta:
            url = meta["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)

            logger.info(f"Scraping: {url}")
            article = scrape_article(meta)

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
    output_file = "reuters_raw.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Done! Scraped {len(articles)} articles → saved to {output_file}")