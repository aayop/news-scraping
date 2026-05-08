"""
Data Quality Checker
Validates data quality across all layers of the data lake.
Checks for completeness, consistency, accuracy, and timeliness.

Usage: python quality_checker.py
"""

import json
import os
import glob
import sys
from datetime import datetime, timedelta
from collections import Counter
import logging

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BRONZE_DIR = "data_lake/bronze"
SILVER_DIR = "data_lake/silver"
GOLD_DIR = "data_lake/gold"


class DataQualityChecker:
    """Comprehensive data quality validation across all data lake layers."""

    def __init__(self):
        self.issues = []
        self.warnings = []
        self.stats = {
            "bronze_files": 0,
            "silver_files": 0,
            "gold_files": 0,
            "total_articles": 0,
            "quality_score": 0.0
        }

    def log_issue(self, layer: str, issue_type: str, message: str, severity: str = "ERROR"):
        """Log a data quality issue."""
        issue = {
            "layer": layer,
            "type": issue_type,
            "message": message,
            "severity": severity,
            "timestamp": datetime.utcnow().isoformat()
        }

        if severity == "WARNING":
            self.warnings.append(issue)
            logger.warning(f"[{layer}] {message}")
        else:
            self.issues.append(issue)
            logger.error(f"[{layer}] {message}")

    def check_bronze_layer(self):
        """Validate Bronze layer data quality."""
        logger.info("🔍 Checking Bronze layer...")

        bronze_files = glob.glob(f"{BRONZE_DIR}/*.json")
        self.stats["bronze_files"] = len(bronze_files)

        if not bronze_files:
            self.log_issue("bronze", "missing_data", "No Bronze files found")
            return

        total_articles = 0
        sources = set()

        for file_path in bronze_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    articles = json.load(f)

                if not isinstance(articles, list):
                    self.log_issue("bronze", "format_error", f"Invalid format in {file_path}")
                    continue

                for article in articles:
                    total_articles += 1

                    # Check required fields
                    required_fields = ["title", "content", "url", "source"]
                    for field in required_fields:
                        if field not in article or not article[field]:
                            self.log_issue("bronze", "missing_field",
                                         f"Missing {field} in article from {file_path}", "WARNING")

                    # Check for basic content quality
                    if article.get("content", "") and len(article["content"]) < 100:
                        self.log_issue("bronze", "content_quality",
                                     f"Very short content in {file_path}", "WARNING")

                    # Track sources
                    if article.get("source"):
                        sources.add(article["source"])

            except Exception as e:
                self.log_issue("bronze", "file_error", f"Error reading {file_path}: {e}")

        self.stats["total_articles"] = total_articles

        # Check for data freshness (should have recent files)
        if bronze_files:
            latest_file = max(bronze_files, key=os.path.getctime)
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getctime(latest_file))
            if file_age > timedelta(hours=24):
                self.log_issue("bronze", "data_freshness",
                              f"Latest Bronze data is {file_age.days} days old", "WARNING")

        logger.info(f"✅ Bronze layer: {len(bronze_files)} files, {total_articles} articles, {len(sources)} sources")

    def check_silver_layer(self):
        """Validate Silver layer data quality."""
        logger.info("🔍 Checking Silver layer...")

        silver_files = glob.glob(f"{SILVER_DIR}/*.json")
        self.stats["silver_files"] = len(silver_files)

        if not silver_files:
            self.log_issue("silver", "missing_data", "No Silver files found")
            return

        processed_articles = 0
        languages = Counter()
        sources = Counter()

        for file_path in silver_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    articles = json.load(f)

                for article in articles:
                    processed_articles += 1

                    # Check required fields
                    required_fields = ["title", "content", "url", "source", "language", "processed_at"]
                    for field in required_fields:
                        if field not in article or not article[field]:
                            self.log_issue("silver", "missing_field",
                                         f"Missing {field} in processed article", "WARNING")

                    # Check content normalization
                    content = article.get("content", "")
                    if "<" in content or ">" in content:
                        self.log_issue("silver", "normalization_error",
                                     "HTML tags found in Silver content", "WARNING")

                    # Check language detection
                    lang = article.get("language", "")
                    if lang not in ["en", "ar", "fr"]:
                        self.log_issue("silver", "language_detection",
                                     f"Unknown language code: {lang}", "WARNING")
                    else:
                        languages[lang] += 1

                    # Check date formats
                    pub_date = article.get("publication_date")
                    if pub_date:
                        try:
                            datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                        except:
                            self.log_issue("silver", "date_format",
                                         f"Invalid date format: {pub_date}", "WARNING")

                    sources[article.get("source", "unknown")] += 1

            except Exception as e:
                self.log_issue("silver", "file_error", f"Error reading {file_path}: {e}")

        logger.info(f"✅ Silver layer: {len(silver_files)} files, {processed_articles} articles")
        logger.info(f"   Languages: {dict(languages)}")
        logger.info(f"   Sources: {dict(sources)}")

    def check_gold_layer(self):
        """Validate Gold layer analytics quality."""
        logger.info("🔍 Checking Gold layer...")

        expected_files = [
            "articles_by_source.json",
            "articles_by_category.json",
            "articles_by_language.json",
            "articles_by_date.json",
            "top_keywords.json",
            "summary_stats.json"
        ]

        gold_files = []
        for filename in expected_files:
            filepath = f"{GOLD_DIR}/{filename}"
            if os.path.exists(filepath):
                gold_files.append(filepath)
            else:
                self.log_issue("gold", "missing_file", f"Missing analytics file: {filename}")

        self.stats["gold_files"] = len(gold_files)

        # Validate analytics data
        for filepath in gold_files:
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)

                filename = os.path.basename(filepath)

                if filename == "summary_stats.json":
                    required_keys = ["total_articles", "total_sources", "generated_at"]
                    for key in required_keys:
                        if key not in data:
                            self.log_issue("gold", "missing_key",
                                         f"Missing {key} in {filename}", "WARNING")

                    # Check if stats make sense
                    if data.get("total_articles", 0) <= 0:
                        self.log_issue("gold", "invalid_stats",
                                     "Total articles should be > 0", "WARNING")

                elif filename.startswith("articles_by_"):
                    if not isinstance(data, list):
                        self.log_issue("gold", "format_error",
                                     f"Invalid format in {filename}", "WARNING")
                        continue

                    if not data:
                        self.log_issue("gold", "empty_data",
                                     f"No data in {filename}", "WARNING")
                        continue

                    # Check data structure
                    if filename == "articles_by_source.json":
                        for item in data:
                            if "source" not in item or "article_count" not in item:
                                self.log_issue("gold", "structure_error",
                                             f"Invalid structure in {filename}", "WARNING")

                    elif filename == "articles_by_language.json":
                        for item in data:
                            if "language" not in item or "article_count" not in item:
                                self.log_issue("gold", "structure_error",
                                             f"Invalid structure in {filename}", "WARNING")

                elif filename == "top_keywords.json":
                    if not isinstance(data, list) or not data:
                        self.log_issue("gold", "empty_data",
                                     f"No keywords in {filename}", "WARNING")
                    else:
                        # Check keyword quality
                        for item in data:
                            if "keyword" not in item or "frequency" not in item:
                                self.log_issue("gold", "structure_error",
                                             f"Invalid keyword structure", "WARNING")

            except Exception as e:
                self.log_issue("gold", "file_error", f"Error reading {filepath}: {e}")

        logger.info(f"✅ Gold layer: {len(gold_files)}/{len(expected_files)} analytics files")

    def check_data_consistency(self):
        """Check consistency between layers."""
        logger.info("🔍 Checking data consistency across layers...")

        # Compare Bronze vs Silver article counts
        bronze_count = self.stats["total_articles"]
        silver_files = glob.glob(f"{SILVER_DIR}/*.json")
        silver_count = 0

        for f in silver_files:
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    silver_count += len(json.load(fp))
            except:
                pass

        if silver_count < bronze_count * 0.8:  # Allow for some filtering
            self.log_issue("consistency", "article_count_mismatch",
                         f"Silver ({silver_count}) much less than Bronze ({bronze_count})", "WARNING")

        # Check if Gold summary matches Silver
        summary_file = f"{GOLD_DIR}/summary_stats.json"
        if os.path.exists(summary_file):
            try:
                with open(summary_file, "r", encoding="utf-8") as f:
                    summary = json.load(f)

                gold_total = summary.get("total_articles", 0)
                if abs(gold_total - silver_count) > 5:  # Small tolerance for processing differences
                    self.log_issue("consistency", "summary_mismatch",
                                 f"Gold summary ({gold_total}) != Silver count ({silver_count})", "WARNING")
            except:
                pass

    def calculate_quality_score(self):
        """Calculate overall data quality score (0-100)."""
        total_checks = len(self.issues) + len(self.warnings)

        if total_checks == 0:
            self.stats["quality_score"] = 100.0
        else:
            # Weight errors more heavily than warnings
            error_weight = 10
            warning_weight = 2
            total_weight = (len(self.issues) * error_weight) + (len(self.warnings) * warning_weight)
            max_possible_weight = 50  # Assume 5 errors + 20 warnings is very bad

            # Score = 100 - (weighted_issues / max_possible) * 100
            penalty = min(total_weight / max_possible_weight, 1.0) * 100
            self.stats["quality_score"] = max(0, 100 - penalty)

    def run_quality_checks(self):
        """Run all data quality checks."""
        print("=" * 70)
        print("[QUALITY] DATA QUALITY CHECKS")
        print("=" * 70)

        self.check_bronze_layer()
        self.check_silver_layer()
        self.check_gold_layer()
        self.check_data_consistency()
        self.calculate_quality_score()

        # Print summary
        print("\n" + "=" * 70)
        print("[SUMMARY] QUALITY CHECK SUMMARY")
        print("=" * 70)
        print(f"Files checked: Bronze({self.stats['bronze_files']}) Silver({self.stats['silver_files']}) Gold({self.stats['gold_files']})")
        print(f"Total articles: {self.stats['total_articles']}")
        print(f"Quality score: {self.stats['quality_score']:.1f}/100")

        if self.issues:
            print(f"\n❌ CRITICAL ISSUES ({len(self.issues)}):")
            for issue in self.issues[:5]:  # Show first 5
                print(f"   • {issue['layer'].upper()}: {issue['message']}")

        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings[:5]:  # Show first 5
                print(f"   • {warning['layer'].upper()}: {warning['message']}")

        if len(self.issues) > 5 or len(self.warnings) > 5:
            print(f"\n   ... and {len(self.issues) + len(self.warnings) - 10} more issues")

        # Save detailed report
        self.save_quality_report()

        print("\n" + "=" * 70)
        if self.stats["quality_score"] >= 90:
            print("[EXCELLENT] Data quality is very good!")
        elif self.stats["quality_score"] >= 75:
            print("[GOOD] Data quality is acceptable but could be improved")
        else:
            print("[POOR] Data quality needs attention")
        print("=" * 70)

        return self.stats["quality_score"] >= 75  # Return True if quality is acceptable

    def save_quality_report(self):
        """Save detailed quality report to file."""
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "stats": self.stats,
            "issues": self.issues,
            "warnings": self.warnings
        }

        os.makedirs("reports", exist_ok=True)
        report_file = f"reports/quality_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"📄 Detailed report saved to: {report_file}")


if __name__ == "__main__":
    checker = DataQualityChecker()
    checker.run_quality_checks()