"""
Simple streaming part for the project.

For this version, I simulate streaming by turning every cleaned article into
an article_published event. The events are saved in the Data Lake and also
inserted into MySQL when XAMPP is running.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from hashlib import sha256

from mysql.connector import Error

from storage import ensure_prefix, exists, read_json, storage_uri, write_json
from warehouse_loader import MYSQL_DATABASE, connect, parse_datetime, prepare_database

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def load_latest_articles() -> list[dict]:
    """Use the latest Silver file first, otherwise fallback to Gold articles."""
    latest_silver = "silver/articles_silver_latest.json"
    gold_articles = "gold/articles.json"

    if exists(latest_silver):
        return read_json(latest_silver)
    if exists(gold_articles):
        return read_json(gold_articles)
    return []


def build_event(article: dict, sequence: int, batch_id: str) -> dict:
    """Create one event from one article."""
    event_time = datetime.now(UTC).isoformat()
    url = article.get("url", "")
    event_id = sha256(f"{batch_id}:{sequence}:{url}".encode("utf-8")).hexdigest()

    return {
        "event_id": event_id,
        "event_type": "article_published",
        "event_time": event_time,
        "batch_id": batch_id,
        "source": article.get("source"),
        "url": url,
        "title": article.get("title"),
        "publication_date": article.get("publication_date"),
        "language": article.get("language"),
        "category": article.get("category"),
        "payload": article,
    }


def save_events_to_lake(events: list[dict], batch_id: str) -> str:
    """Save the event batch in the streaming folder."""
    ensure_prefix("streaming")
    event_key = f"streaming/article_events_{batch_id}.json"
    latest_key = "streaming/article_events_latest.json"
    write_json(event_key, events)
    write_json(latest_key, events)
    return event_key


def create_streaming_schema(cursor) -> None:
    """Create the MySQL table used to keep streaming events."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS streaming_article_events (
            event_id CHAR(64) PRIMARY KEY,
            event_type VARCHAR(100) NOT NULL,
            event_time DATETIME NOT NULL,
            batch_id VARCHAR(32) NOT NULL,
            source VARCHAR(255),
            url_hash CHAR(64),
            url TEXT,
            title TEXT,
            publication_date DATETIME NULL,
            language VARCHAR(20),
            category VARCHAR(255),
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def save_events_to_warehouse(events: list[dict]) -> bool:
    """Copy events to MySQL so they can be checked in phpMyAdmin."""
    prepare_database()
    loaded_at = datetime.now()

    with connect(MYSQL_DATABASE) as conn:
        cursor = conn.cursor()
        create_streaming_schema(cursor)
        cursor.executemany(
            """
            INSERT INTO streaming_article_events (
                event_id, event_type, event_time, batch_id, source, url_hash,
                url, title, publication_date, language, category, loaded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE loaded_at = VALUES(loaded_at)
            """,
            [
                (
                    event["event_id"],
                    event["event_type"],
                    parse_datetime(event["event_time"]),
                    event["batch_id"],
                    event.get("source"),
                    sha256(event.get("url", "").encode("utf-8")).hexdigest(),
                    event.get("url"),
                    event.get("title"),
                    parse_datetime(event.get("publication_date")),
                    event.get("language"),
                    event.get("category"),
                    loaded_at,
                )
                for event in events
                if event.get("url")
            ],
        )
        conn.commit()

    return True


def run_streaming_ingestion() -> dict:
    """Build events and save them in both targets."""
    articles = load_latest_articles()
    if not articles:
        raise RuntimeError("No Silver or Gold articles found to stream")

    batch_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    events = [build_event(article, index + 1, batch_id) for index, article in enumerate(articles)]
    event_key = save_events_to_lake(events, batch_id)

    warehouse_loaded = False
    try:
        warehouse_loaded = save_events_to_warehouse(events)
    except Error as exc:
        print(f"[WARN] Streaming events saved to Data Lake, but MySQL mirror failed: {exc}")

    return {
        "batch_id": batch_id,
        "event_count": len(events),
        "event_key": event_key,
        "warehouse_loaded": warehouse_loaded,
    }


def main() -> None:
    print("=" * 70)
    print("[STREAMING] Publishing article events")
    print("=" * 70)

    try:
        stats = run_streaming_ingestion()
    except RuntimeError as exc:
        print(f"[ERROR] Streaming ingestion failed: {exc}")
        sys.exit(1)

    print("[SUCCESS] Streaming events published")
    print(f"   Batch ID        : {stats['batch_id']}")
    print(f"   Events          : {stats['event_count']}")
    print(f"   Data Lake file  : {storage_uri(stats['event_key'])}")
    print(f"   MySQL mirror    : {'yes' if stats['warehouse_loaded'] else 'no'}")
    print("=" * 70)


if __name__ == "__main__":
    main()
