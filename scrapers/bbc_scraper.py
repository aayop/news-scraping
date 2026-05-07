"""
BBC News Scraper
Collects articles from bbc.com/news using their RSS feeds + HTML scraping
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
    "Accept-Language": "en-GB,en;q=0.9",
}

RSS_FEEDS = {
    "top_news":   "https://feeds.bbci.co.uk/news/rss.xml",
    "world":      "https://feeds.bbci.co.uk/news/world/rss.xml",
    "technology": "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "business":   "https://feeds.bbci.co.uk/news/business/rss.xml",
    "science":    "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
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
            if title and link and "bbc.com" in link:
                articles_meta.append({
                    "title": title,
                    "url": link,
                    "publication_date": date,
                    "category": category,
                    "description": desc,
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

        author = None
        author_tag = soup.find(attrs={"data-testid": "byline-name"})
        if author_tag:
            author = author_tag.get_text(strip=True)

        content = ""
        article_tag = soup.find("article")
        if article_tag:
            for tag in article_tag.find_all(["script", "style", "figure", "nav"]):
                tag.decompose()
            text_blocks = article_tag.find_all("div", attrs={"data-component": "text-block"})
            if text_blocks:
                content = " ".join(b.get_text(separator=" ", strip=True) for b in text_blocks)
            else:
                content = " ".join(p.get_text(strip=True) for p in article_tag.find_all("p"))

        if not content:
            logger.warning(f"No content found for: {url}")
            return None

        return {
            "title": meta["title"],
            "author": author,
            "publication_date": meta.get("publication_date"),
            "category": meta.get("category"),
            "content": content[:5000],
            "source": "BBC News",
            "url": url,
            "language": "en",
            "scraped_at": datetime.utcnow().isoformat(),
        }

    except requests.RequestException as e:
        logger.error(f"Failed to scrape article {url}: {e}")
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
    output_file = "bbc_raw.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Done! Scraped {len(articles)} articles → saved to {output_file}")