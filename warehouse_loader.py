"""
Loads the Gold layer into MySQL.

I used XAMPP for the warehouse part, so the default connection is the usual
local setup: root user, no password, port 3306.
"""

from __future__ import annotations

import os
import sys
from hashlib import sha256
from datetime import datetime

import mysql.connector
from mysql.connector import Error

from storage import exists, read_json

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "news_warehouse")


def connect(database: str | None = None):
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )


def load_gold_json(filename: str, default):
    """Read one Gold JSON file from the Data Lake."""
    key = f"gold/{filename}"
    if not exists(key):
        print(f"[WARN] Missing Gold file: {key}")
        return default
    return read_json(key)


def prepare_database() -> None:
    """Create the warehouse database if it does not exist yet."""
    with connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        conn.commit()


def create_schema(cursor) -> None:
    """Create the tables used for reporting."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_source (
            source VARCHAR(255) PRIMARY KEY,
            article_count INT NOT NULL,
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_category (
            category VARCHAR(255) PRIMARY KEY,
            article_count INT NOT NULL,
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_language (
            language_code VARCHAR(20) PRIMARY KEY,
            language VARCHAR(100) NOT NULL,
            article_count INT NOT NULL,
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_articles_by_day (
            publication_day DATE PRIMARY KEY,
            article_count INT NOT NULL,
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_keywords (
            keyword VARCHAR(255) PRIMARY KEY,
            frequency INT NOT NULL,
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_articles (
            url_hash CHAR(64) PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            author VARCHAR(255),
            publication_date DATETIME NULL,
            category VARCHAR(255),
            content MEDIUMTEXT,
            content_length INT,
            source VARCHAR(255),
            language VARCHAR(20),
            scraped_at DATETIME NULL,
            processed_at DATETIME NULL,
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_summary (
            id TINYINT PRIMARY KEY,
            total_articles INT NOT NULL,
            total_sources INT NOT NULL,
            avg_content_length DECIMAL(12,2) NOT NULL,
            generated_at DATETIME NULL,
            loaded_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def parse_datetime(value: str | None):
    """Convert ISO strings to datetime values accepted by MySQL."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def truncate_tables(cursor) -> None:
    """Reload analytical tables from scratch each time."""
    for table in [
        "dim_source",
        "dim_category",
        "dim_language",
        "fact_articles_by_day",
        "fact_keywords",
        "fact_articles",
        "warehouse_summary",
    ]:
        cursor.execute(f"TRUNCATE TABLE `{table}`")


def load_warehouse() -> dict:
    """Main load step: JSON Gold files to MySQL tables."""
    prepare_database()

    by_source = load_gold_json("articles_by_source.json", [])
    by_category = load_gold_json("articles_by_category.json", [])
    by_language = load_gold_json("articles_by_language.json", [])
    by_date = load_gold_json("articles_by_date.json", [])
    keywords = load_gold_json("top_keywords.json", [])
    articles = load_gold_json("articles.json", [])
    summary = load_gold_json("summary_stats.json", {})

    loaded_at = datetime.now()

    with connect(MYSQL_DATABASE) as conn:
        cursor = conn.cursor()
        create_schema(cursor)
        truncate_tables(cursor)

        cursor.executemany(
            "INSERT INTO dim_source (source, article_count, loaded_at) VALUES (%s, %s, %s)",
            [(row["source"], row["article_count"], loaded_at) for row in by_source],
        )
        cursor.executemany(
            "INSERT INTO dim_category (category, article_count, loaded_at) VALUES (%s, %s, %s)",
            [(row["category"], row["article_count"], loaded_at) for row in by_category],
        )
        cursor.executemany(
            "INSERT INTO dim_language (language_code, language, article_count, loaded_at) VALUES (%s, %s, %s, %s)",
            [
                (row["language_code"], row["language"], row["article_count"], loaded_at)
                for row in by_language
            ],
        )
        cursor.executemany(
            "INSERT INTO fact_articles_by_day (publication_day, article_count, loaded_at) VALUES (%s, %s, %s)",
            [(row["date"], row["article_count"], loaded_at) for row in by_date],
        )
        cursor.executemany(
            "INSERT INTO fact_keywords (keyword, frequency, loaded_at) VALUES (%s, %s, %s)",
            [(row["keyword"], row["frequency"], loaded_at) for row in keywords],
        )
        cursor.executemany(
            """
            INSERT INTO fact_articles (
                url_hash, url, title, author, publication_date, category, content,
                content_length, source, language, scraped_at, processed_at, loaded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    sha256(row.get("url", "").encode("utf-8")).hexdigest(),
                    row.get("url"),
                    row.get("title", ""),
                    row.get("author"),
                    parse_datetime(row.get("publication_date")),
                    row.get("category"),
                    row.get("content"),
                    row.get("content_length"),
                    row.get("source"),
                    row.get("language"),
                    parse_datetime(row.get("scraped_at")),
                    parse_datetime(row.get("processed_at")),
                    loaded_at,
                )
                for row in articles
                if row.get("url")
            ],
        )
        cursor.execute(
            """
            INSERT INTO warehouse_summary (
                id, total_articles, total_sources, avg_content_length, generated_at, loaded_at
            ) VALUES (1, %s, %s, %s, %s, %s)
            """,
            (
                summary.get("total_articles", 0),
                summary.get("total_sources", 0),
                summary.get("avg_content_length", 0),
                parse_datetime(summary.get("generated_at")),
                loaded_at,
            ),
        )

        conn.commit()

    return {
        "database": MYSQL_DATABASE,
        "articles": len(articles),
        "sources": len(by_source),
        "categories": len(by_category),
        "languages": len(by_language),
        "dates": len(by_date),
        "keywords": len(keywords),
    }


def main() -> None:
    print("=" * 70)
    print("[WAREHOUSE] Loading Gold tables into MySQL/MariaDB")
    print("=" * 70)
    print(f"Connection: {MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

    try:
        stats = load_warehouse()
    except Error as exc:
        print(f"[ERROR] Warehouse load failed: {exc}")
        print("Check that XAMPP MySQL is running and that MYSQL_* env vars are correct.")
        sys.exit(1)

    print("[SUCCESS] Warehouse loaded")
    print(f"   Database   : {stats['database']}")
    print(f"   Articles   : {stats['articles']}")
    print(f"   Sources    : {stats['sources']}")
    print(f"   Categories : {stats['categories']}")
    print(f"   Languages  : {stats['languages']}")
    print(f"   Dates      : {stats['dates']}")
    print(f"   Keywords   : {stats['keywords']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
