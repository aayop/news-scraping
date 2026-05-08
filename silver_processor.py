"""
Silver Layer Processor
Reads raw JSON from Bronze layer, cleans and normalizes data, saves to Silver layer.

Transformations:
- Remove HTML tags
- Normalize text (whitespace, encoding)
- Detect language
- Validate required fields
- Deduplicate articles
"""

import json
import os
import re
import glob
import sys
import logging
from datetime import datetime
from html.parser import HTMLParser

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BRONZE_DIR = "data_lake/bronze"
SILVER_DIR = "data_lake/silver"


# ─────────────────────────────────────────────
# HTML Cleaner
# ─────────────────────────────────────────────

class HTMLStripper(HTMLParser):
    """Simple HTML tag remover without external libraries."""
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return " ".join(self.fed)


def strip_html(text: str) -> str:
    """Remove all HTML tags from a string."""
    if not text:
        return ""
    stripper = HTMLStripper()
    stripper.feed(text)
    return stripper.get_data()


# ─────────────────────────────────────────────
# Text Normalization
# ─────────────────────────────────────────────

def normalize_text(text: str) -> str:
    """Clean and normalize text content."""
    if not text:
        return ""

    # Remove HTML tags
    text = strip_html(text)

    # Remove URLs
    text = re.sub(r"http\S+|www\S+", "", text)

    # Remove excessive whitespace
    text = re.sub(r"\s+", " ", text)

    # Remove special characters but keep Arabic, French, English letters
    text = re.sub(r"[^\w\s\u0600-\u06FF\u00C0-\u024F.,!?;:'\"-]", " ", text)

    # Strip leading/trailing whitespace
    text = text.strip()

    return text


def normalize_date(date_str: str) -> str | None:
    """Try to parse and normalize publication date to ISO format."""
    if not date_str:
        return None

    # Common date formats
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",   # RSS format: Mon, 06 May 2026 12:00:00 +0000
        "%a, %d %b %Y %H:%M:%S GMT",
        "%Y-%m-%dT%H:%M:%S%z",         # ISO format
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue

    # Return original if can't parse
    return date_str


# ─────────────────────────────────────────────
# Language Detection
# ─────────────────────────────────────────────

def detect_language(text: str, source: str = "") -> str:
    """
    Detect language based on content and source.
    Uses simple character-based detection + source hints.
    """
    if not text:
        return "unknown"

    # Source-based hints
    moroccan_sources = ["hespress", "akhbarona", "barlamane", "lakom"]
    if any(s in source.lower() for s in moroccan_sources):
        # Check if mostly Arabic characters
        arabic_chars = len(re.findall(r"[\u0600-\u06FF]", text))
        latin_chars  = len(re.findall(r"[a-zA-Z]", text))
        if arabic_chars > latin_chars:
            return "ar"
        else:
            return "fr"  # Moroccan sites sometimes publish in French

    # For international sources
    english_sources = ["bbc", "reuters", "cnn"]
    if any(s in source.lower() for s in english_sources):
        return "en"

    arabic_sources = ["aljazeera", "al jazeera"]
    if any(s in source.lower() for s in arabic_sources):
        # Al Jazeera English
        arabic_chars = len(re.findall(r"[\u0600-\u06FF]", text))
        latin_chars  = len(re.findall(r"[a-zA-Z]", text))
        return "ar" if arabic_chars > latin_chars else "en"

    # Fallback: character analysis
    arabic_chars = len(re.findall(r"[\u0600-\u06FF]", text))
    latin_chars  = len(re.findall(r"[a-zA-Z]", text))

    if arabic_chars > latin_chars:
        return "ar"
    return "en"


# ─────────────────────────────────────────────
# Data Quality Checks
# ─────────────────────────────────────────────

def is_valid_article(article: dict) -> tuple[bool, str]:
    """
    Check if article passes quality checks.
    Returns (is_valid, reason_if_invalid)
    """
    # Check title
    if not article.get("title"):
        return False, "missing_title"

    if len(article.get("title", "")) < 5:
        return False, "title_too_short"

    # Check content
    if not article.get("content"):
        return False, "missing_content"

    if len(article.get("content", "")) < 50:
        return False, "content_too_short"

    # Check URL
    if not article.get("url"):
        return False, "missing_url"

    return True, "ok"


# ─────────────────────────────────────────────
# Main Processing
# ─────────────────────────────────────────────

def process_article(raw: dict) -> dict:
    """Transform a raw Bronze article into a clean Silver article."""
    title   = normalize_text(raw.get("title", ""))
    content = normalize_text(raw.get("content", ""))
    author  = normalize_text(raw.get("author", "")) or None
    source  = raw.get("source", "")

    language = raw.get("language") or detect_language(content, source)
    pub_date = normalize_date(raw.get("publication_date"))
    category = normalize_text(raw.get("category", "")) or None

    return {
        "title":            title,
        "author":           author,
        "publication_date": pub_date,
        "category":         category,
        "content":          content,
        "content_length":   len(content),
        "source":           source,
        "url":              raw.get("url", ""),
        "language":         language,
        "scraped_at":       raw.get("scraped_at"),
        "processed_at":     datetime.utcnow().isoformat(),
    }


def run_silver_processor():
    """Read all Bronze files, process them, save to Silver layer."""
    os.makedirs(SILVER_DIR, exist_ok=True)

    # Find all Bronze JSON files
    bronze_files = glob.glob(f"{BRONZE_DIR}/*.json")
    if not bronze_files:
        logger.warning(f"No JSON files found in {BRONZE_DIR}/")
        return []

    logger.info(f"Found {len(bronze_files)} Bronze files to process")

    all_silver = []
    seen_urls  = set()
    stats = {"total": 0, "valid": 0, "invalid": 0, "duplicate": 0}

    for bronze_file in bronze_files:
        logger.info(f"Processing: {bronze_file}")

        with open(bronze_file, "r", encoding="utf-8") as f:
            raw_articles = json.load(f)

        for raw in raw_articles:
            stats["total"] += 1

            # Deduplicate by URL
            url = raw.get("url", "")
            if url in seen_urls:
                stats["duplicate"] += 1
                continue
            seen_urls.add(url)

            # Process article
            silver = process_article(raw)

            # Validate
            valid, reason = is_valid_article(silver)
            if not valid:
                logger.warning(f"Invalid article ({reason}): {url}")
                stats["invalid"] += 1
                continue

            all_silver.append(silver)
            stats["valid"] += 1

    # Save Silver output
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = f"{SILVER_DIR}/articles_silver_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_silver, f, ensure_ascii=False, indent=2)

    # Print summary
    print("\n" + "=" * 60)
    print("[SUCCESS] Silver Layer Processing Complete!")
    print(f"   Total articles processed : {stats['total']}")
    print(f"   [OK] Valid articles       : {stats['valid']}")
    print(f"   [ERROR] Invalid (quality) : {stats['invalid']}")
    print(f"   [DUPE] Duplicates removed : {stats['duplicate']}")
    print(f"   [SAVE] Saved to           : {output_file}")
    print("=" * 60)

    return all_silver


if __name__ == "__main__":
    run_silver_processor()
