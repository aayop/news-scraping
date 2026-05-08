"""
Gold Layer Processor
Reads clean Silver data and builds analytics tables.

Analytics produced:
- articles_by_source
- articles_by_category
- articles_by_language
- articles_by_date
- top_keywords
- summary_stats
"""

import json
import re
import sys
from collections import Counter
from datetime import datetime

from storage import ensure_prefix, exists, list_json, modified_at, read_json, write_json

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')


# Common stop words to exclude from keyword analysis
STOP_WORDS = {
    # English
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "this", "that", "these", "those", "it", "its",
    "he", "she", "they", "we", "you", "i", "my", "his", "her", "their",
    "our", "your", "as", "if", "not", "no", "so", "up", "out", "about",
    "than", "then", "also", "more", "after", "said", "says", "new", "two",
    "can", "one", "over", "into", "who", "what", "when", "how", "which",
    # French
    "le", "la", "les", "un", "une", "des", "du", "de", "en", "et", "est",
    "au", "aux", "ce", "se", "sa", "son", "ses", "qui", "que", "dans",
    "sur", "par", "pour", "avec", "il", "elle", "ils", "elles", "nous",
    "vous", "je", "tu", "me", "te", "lui", "leur", "plus", "pas", "ne",
    # Common noise words
    "said", "says", "new", "also", "just", "like", "get", "got", "s", "t",
}


def load_silver_data() -> list[dict]:
    """Load all Silver JSON files."""
    latest_snapshot = "silver/articles_silver_latest.json"
    if exists(latest_snapshot):
        silver_files = [latest_snapshot]
    else:
        silver_files = list_json("silver/")
        if silver_files:
            silver_files = [max(silver_files, key=lambda key: modified_at(key) or datetime.min)]

    if not silver_files:
        print("❌ No Silver files found in the Silver layer")
        return []

    all_articles = []
    for key in silver_files:
        all_articles.extend(read_json(key))

    print(f"[OK] Loaded {len(all_articles)} articles from Silver layer")
    return all_articles


def articles_by_source(articles: list[dict]) -> list[dict]:
    """Count articles per source."""
    counter = Counter(a.get("source", "Unknown") for a in articles)
    return [
        {"source": source, "article_count": count}
        for source, count in counter.most_common()
    ]


def articles_by_category(articles: list[dict]) -> list[dict]:
    """Count articles per category."""
    counter = Counter(
        a.get("category", "Uncategorized") or "Uncategorized"
        for a in articles
    )
    return [
        {"category": cat, "article_count": count}
        for cat, count in counter.most_common()
    ]


def articles_by_language(articles: list[dict]) -> list[dict]:
    """Count articles per language."""
    counter = Counter(a.get("language", "unknown") for a in articles)
    lang_names = {"en": "English", "ar": "Arabic", "fr": "French", "unknown": "Unknown"}
    return [
        {"language_code": lang, "language": lang_names.get(lang, lang), "article_count": count}
        for lang, count in counter.most_common()
    ]


def articles_by_date(articles: list[dict]) -> list[dict]:
    """Count articles per publication date (by day)."""
    date_counter = Counter()
    for a in articles:
        pub_date = a.get("publication_date") or a.get("scraped_at", "")
        if pub_date:
            day = pub_date[:10]  # Extract YYYY-MM-DD
            date_counter[day] += 1

    return [
        {"date": date, "article_count": count}
        for date, count in sorted(date_counter.items(), reverse=True)
    ]


def top_keywords(articles: list[dict], top_n: int = 30) -> list[dict]:
    """Extract most frequent keywords across all articles."""
    word_counter = Counter()

    for article in articles:
        content = article.get("content", "") + " " + article.get("title", "")
        # Extract words (min 4 chars, no numbers)
        words = re.findall(r"\b[a-zA-Z\u0600-\u06FF]{4,}\b", content.lower())
        # Filter stop words
        words = [w for w in words if w not in STOP_WORDS]
        word_counter.update(words)

    return [
        {"keyword": word, "frequency": count}
        for word, count in word_counter.most_common(top_n)
    ]


def summary_stats(articles: list[dict]) -> dict:
    """Overall summary statistics."""
    sources    = set(a.get("source", "") for a in articles)
    languages  = set(a.get("language", "") for a in articles)
    avg_length = sum(a.get("content_length", 0) for a in articles) / len(articles) if articles else 0

    return {
        "total_articles":       len(articles),
        "total_sources":        len(sources),
        "sources":              list(sources),
        "languages":            list(languages),
        "avg_content_length":   round(avg_length, 1),
        "generated_at":         datetime.now().isoformat(),
    }


def save_gold_table(data, filename: str):
    """Save a Gold analytics table as JSON."""
    ensure_prefix("gold")
    storage_key = f"gold/{filename}"
    write_json(storage_key, data)
    print(f"   [SAVE] {storage_key}")


def save_articles_json(articles: list[dict]):
    """Save the full article list to the Gold layer for dashboard rendering."""
    ensure_prefix("gold")
    storage_key = "gold/articles.json"
    write_json(storage_key, articles)
    print(f"   [SAVE] {storage_key}")


def run_gold_processor():
    print("=" * 60)
    print("[GOLD] Building Gold Layer Analytics Tables")
    print("=" * 60)

    articles = load_silver_data()
    if not articles:
        return

    print("\n[ANALYTICS] Generating analytics tables...")

    # Build and save all Gold tables
    save_gold_table(articles_by_source(articles),   "articles_by_source.json")
    save_gold_table(articles_by_category(articles), "articles_by_category.json")
    save_gold_table(articles_by_language(articles), "articles_by_language.json")
    save_gold_table(articles_by_date(articles),     "articles_by_date.json")
    save_gold_table(top_keywords(articles),         "top_keywords.json")
    save_gold_table(summary_stats(articles),        "summary_stats.json")
    save_articles_json(articles)

    # Print preview
    print("\n[STATS] Articles by Source:")
    for row in articles_by_source(articles):
        print(f"   {row['source']:<20} {row['article_count']} articles")

    print("\n[LANGUAGE] Articles by Language:")
    for row in articles_by_language(articles):
        print(f"   {row['language']:<15} {row['article_count']} articles")

    print("\n[KEYWORDS] Top 10 Keywords:")
    for row in top_keywords(articles, top_n=10):
        print(f"   {row['keyword']:<20} {row['frequency']} times")

    stats = summary_stats(articles)
    print(f"\n[SUMMARY] Summary:")
    print(f"   Total articles     : {stats['total_articles']}")
    print(f"   Sources            : {stats['total_sources']}")
    print(f"   Avg content length : {stats['avg_content_length']} chars")

    print("\n" + "=" * 60)
    print("[SUCCESS] Gold layer saved to storage")
    print("=" * 60)


if __name__ == "__main__":

    run_gold_processor()
