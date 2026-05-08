"""
News Intelligence Pipeline
Orchestrates the complete data pipeline from scraping to analytics.

Pipeline Steps:
1. Web Scraping (Bronze Layer)
2. Data Cleaning (Silver Layer)
3. Streaming Events
4. Analytics (Gold Layer)
5. Data Warehouse Load
6. Quality Checks
7. Dashboard Update

Usage: python pipeline.py [--skip-scraping] [--force]
"""

import argparse
import sys
import time
from datetime import datetime
import subprocess
import os
import json
import logging

from storage import ensure_prefix, exists, list_json, read_json, storage_uri, get_backend_description

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')


class PipelineRunner:
    """Orchestrates the complete news intelligence pipeline."""

    def __init__(self, skip_scraping=False, force=False):
        self.skip_scraping = skip_scraping
        self.force = force
        self.start_time = None
        self.stats = {
            "scraping": {"status": "pending", "duration": 0, "articles": 0},
            "silver": {"status": "pending", "duration": 0, "articles": 0},
            "streaming": {"status": "pending", "duration": 0, "events": 0},
            "gold": {"status": "pending", "duration": 0, "tables": 0},
            "warehouse": {"status": "pending", "duration": 0, "tables": 0},
            "quality": {"status": "pending", "duration": 0, "score": 0.0}
        }

    def run_command(self, command: str, description: str) -> tuple[bool, str]:
        """Run a command and return success status and output."""
        logger.info(f"▶️  {description}")
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=300  # 5 minute timeout
            )
            success = result.returncode == 0
            output = result.stdout + result.stderr
            return success, output
        except subprocess.TimeoutExpired:
            return False, "Command timed out"
        except Exception as e:
            return False, str(e)

    def step_scraping(self) -> bool:
        """Step 1: Run web scraping to collect raw data."""
        if self.skip_scraping:
            logger.info("⏭️  Skipping scraping step")
            self.stats["scraping"]["status"] = "skipped"
            return True

        start_time = time.time()
        logger.info("📰 Starting web scraping...")

        success, output = self.run_command(
            f"cd scrapers && \"{sys.executable}\" main_scraper.py",
            "Running web scrapers"
        )

        duration = time.time() - start_time
        self.stats["scraping"]["duration"] = round(duration, 2)

        if success:
            self.stats["scraping"]["status"] = "completed"
            # Try to extract article count from output
            for line in output.split('\n'):
                if "Total articles:" in line:
                    try:
                        count = int(line.split("Total articles:")[1].strip())
                        self.stats["scraping"]["articles"] = count
                    except:
                        pass
            logger.info(f"✅ Scraping completed in {duration:.1f}s")
            return True
        else:
            self.stats["scraping"]["status"] = "failed"
            logger.error(f"❌ Scraping failed: {output}")
            return False

    def step_silver_processing(self) -> bool:
        """Step 2: Clean and normalize data (Silver layer)."""
        start_time = time.time()
        logger.info("🧹 Starting Silver layer processing...")

        success, output = self.run_command(
            f"\"{sys.executable}\" silver_processor.py",
            "Processing Silver layer"
        )

        duration = time.time() - start_time
        self.stats["silver"]["duration"] = round(duration, 2)

        if success:
            self.stats["silver"]["status"] = "completed"
            # Try to extract article count from output
            for line in output.split('\n'):
                if "Total articles processed :" in line:
                    try:
                        count = int(line.split(":")[1].strip())
                        self.stats["silver"]["articles"] = count
                    except:
                        pass
            logger.info(f"✅ Silver processing completed in {duration:.1f}s")
            return True
        else:
            self.stats["silver"]["status"] = "failed"
            logger.error(f"❌ Silver processing failed: {output}")
            return False

    def step_gold_processing(self) -> bool:
        """Step 3: Generate analytics (Gold layer)."""
        start_time = time.time()
        logger.info("🥇 Starting Gold layer analytics...")

        success, output = self.run_command(
            f"\"{sys.executable}\" gold_processor.py",
            "Processing Gold layer"
        )

        duration = time.time() - start_time
        self.stats["gold"]["duration"] = round(duration, 2)

        if success:
            self.stats["gold"]["status"] = "completed"
            gold_files = list_json("gold/")
            self.stats["gold"]["tables"] = len(gold_files)
            logger.info(f"✅ Gold processing completed in {duration:.1f}s")
            return True
        else:
            self.stats["gold"]["status"] = "failed"
            logger.error(f"❌ Gold processing failed: {output}")
            return False

    def step_streaming_ingestion(self) -> bool:
        """Step 3: Publish each article as a streaming event."""
        start_time = time.time()
        logger.info("Publishing article events to the streaming layer...")

        success, output = self.run_command(
            f"\"{sys.executable}\" streaming_ingestion.py",
            "Publishing streaming article events"
        )

        duration = time.time() - start_time
        self.stats["streaming"]["duration"] = round(duration, 2)

        if success:
            self.stats["streaming"]["status"] = "completed"
            for line in output.splitlines():
                if "Events" in line:
                    try:
                        self.stats["streaming"]["events"] = int(line.split(":")[1].strip())
                    except Exception:
                        pass
            logger.info(f"Streaming ingestion completed in {duration:.1f}s")
            return True

        self.stats["streaming"]["status"] = "failed"
        logger.error(f"Streaming ingestion failed: {output}")
        return False

    def step_quality_checks(self) -> bool:
        """Step 4: Run data quality validation."""
        start_time = time.time()
        logger.info("🔍 Running data quality checks...")

        success, output = self.run_command(
            f"\"{sys.executable}\" quality_checker.py",
            "Running quality checks"
        )

        duration = time.time() - start_time
        self.stats["quality"]["duration"] = round(duration, 2)

        if success:
            self.stats["quality"]["status"] = "completed"
            # Try to extract quality score from output
            for line in output.split('\n'):
                if "Quality score:" in line:
                    try:
                        score = float(line.split(":")[1].strip().split("/")[0])
                        self.stats["quality"]["score"] = score
                    except:
                        pass
            logger.info(f"✅ Quality checks completed in {duration:.1f}s")
            return True
        else:
            self.stats["quality"]["status"] = "failed"
            logger.error(f"❌ Quality checks failed: {output}")
            return False

    def step_warehouse_load(self) -> bool:
        """Step 4: Load Gold analytics into the MySQL data warehouse."""
        start_time = time.time()
        logger.info("Loading Gold tables into the data warehouse...")

        success, output = self.run_command(
            f"\"{sys.executable}\" warehouse_loader.py",
            "Loading MySQL warehouse"
        )

        duration = time.time() - start_time
        self.stats["warehouse"]["duration"] = round(duration, 2)

        if success:
            self.stats["warehouse"]["status"] = "completed"
            self.stats["warehouse"]["tables"] = sum(
                1
                for line in output.splitlines()
                if any(label in line for label in ["Sources", "Categories", "Languages", "Dates", "Keywords"])
            )
            logger.info(f"Warehouse load completed in {duration:.1f}s")
            return True

        self.stats["warehouse"]["status"] = "failed"
        logger.error(f"Warehouse load failed: {output}")
        return False

    def update_dashboard_data(self):
        """Update dashboard with latest data from Gold layer."""
        logger.info("📊 Updating dashboard data...")

        try:
            dashboard_data = {}

            summary_file = "gold/summary_stats.json"
            if exists(summary_file):
                dashboard_data["summary"] = read_json(summary_file)

            analytics_files = [
                ("bySource", "articles_by_source.json"),
                ("byLanguage", "articles_by_language.json"),
                ("byDate", "articles_by_date.json"),
                ("keywords", "top_keywords.json")
            ]

            for key, filename in analytics_files:
                storage_key = f"gold/{filename}"
                if exists(storage_key):
                    dashboard_data[key] = read_json(storage_key)

            silver_files = list_json("silver/")
            sample_articles = []
            for sf in silver_files[:2]:
                try:
                    articles = read_json(sf)
                    sample_articles.extend(articles[:10])
                except Exception:
                    pass

            dashboard_data["articles"] = sample_articles[:20]

            # Update dashboard HTML with new data
            self._update_dashboard_html(dashboard_data)

            logger.info("✅ Dashboard updated with latest data")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to update dashboard: {e}")
            return False

    def _update_dashboard_html(self, data):
        """Update the dashboard HTML with new data."""
        dashboard_file = "dashboard.html"

        if not os.path.exists(dashboard_file):
            logger.warning("Dashboard file not found, skipping update")
            return

        with open(dashboard_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Replace the DATA object in the HTML
        data_js = f"""
const DATA = {{
  summary: {json.dumps(data.get("summary", {}), indent=2)},
  bySource: {json.dumps(data.get("bySource", []), indent=2)},
  byLanguage: {json.dumps(data.get("byLanguage", []), indent=2)},
  byDate: {json.dumps(data.get("byDate", []), indent=2)},
  keywords: {json.dumps(data.get("keywords", []), indent=2)},
  articles: {json.dumps(data.get("articles", []), indent=2)}
}};"""

        # Find and replace the data section
        start_marker = "// ── PASTE YOUR GOLD DATA HERE ──────────────────────────────────────────────"
        end_marker = "// ── REPLACE DATA ABOVE WITH YOUR ACTUAL GOLD FILES ──────────────────────────"

        start_idx = content.find(start_marker)
        end_idx = content.find(end_marker)

        if start_idx != -1 and end_idx != -1:
            end_idx += len(end_marker)
            new_content = content[:start_idx] + start_marker + "\n" + data_js + "\n" + content[end_idx:]

            with open(dashboard_file, "w", encoding="utf-8") as f:
                f.write(new_content)

            logger.info("📄 Dashboard HTML updated with fresh data")
            return

        # Fallback: replace the initial DATA object block if markers are missing
        data_start = content.find("const DATA = {")
        if data_start != -1:
            data_end = content.find("};", data_start)
            if data_end != -1:
                data_end += 2
                new_content = content[:data_start] + data_js + content[data_end:]
                with open(dashboard_file, "w", encoding="utf-8") as f:
                    f.write(new_content)
                logger.info("📄 Dashboard HTML updated with fresh data (fallback)")
                return

        logger.warning("Dashboard HTML markers not found; inline data update skipped")

    def check_prerequisites(self) -> bool:
        """Check if all required files and dependencies exist."""
        logger.info("🔧 Checking prerequisites...")

        required_files = [
            "scrapers/main_scraper.py",
            "silver_processor.py",
            "streaming_ingestion.py",
            "gold_processor.py",
            "warehouse_loader.py",
            "quality_checker.py"
        ]

        missing_files = []
        for file in required_files:
            if not os.path.exists(file):
                missing_files.append(file)

        if missing_files:
            logger.error(f"❌ Missing required files: {missing_files}")
            return False

        # Ensure data lake prefixes exist
        for prefix in ["bronze", "silver", "gold"]:
            ensure_prefix(prefix)

        logger.info(f"✅ Prerequisites check passed ({get_backend_description()})")
        return True

    def print_summary(self):
        """Print pipeline execution summary."""
        total_duration = time.time() - self.start_time

        print("\n" + "=" * 80)
        print("🎯 PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"⏱️  Total execution time: {total_duration:.1f} seconds")
        print(f"📅 Executed at: {datetime.now().isoformat()}")
        print()

        # Step status
        steps = [
            ("Web Scraping", self.stats["scraping"]),
            ("Silver Processing", self.stats["silver"]),
            ("Streaming Events", self.stats["streaming"]),
            ("Gold Analytics", self.stats["gold"]),
            ("Warehouse Load", self.stats["warehouse"]),
            ("Quality Checks", self.stats["quality"])
        ]

        for name, stat in steps:
            status_icon = {
                "completed": "✅",
                "failed": "❌",
                "skipped": "⏭️",
                "pending": "⏳"
            }.get(stat["status"], "❓")

            duration = f" ({stat['duration']:.1f}s)" if stat["duration"] > 0 else ""
            extra_info = ""

            if name == "Web Scraping" and stat.get("articles"):
                extra_info = f" - {stat['articles']} articles"
            elif name == "Silver Processing" and stat.get("articles"):
                extra_info = f" - {stat['articles']} articles"
            elif name == "Streaming Events" and stat.get("events"):
                extra_info = f" - {stat['events']} events"
            elif name == "Gold Analytics" and stat.get("tables"):
                extra_info = f" - {stat['tables']} tables"
            elif name == "Warehouse Load" and stat.get("tables"):
                extra_info = f" - {stat['tables']} tables"
            elif name == "Quality Checks" and stat.get("score"):
                extra_info = f" - Score: {stat['score']:.1f}/100"

            print(f"{status_icon} {name}: {stat['status'].upper()}{duration}{extra_info}")

        print("\n" + "=" * 80)

        # Overall status
        key_mapping = {
            "Web Scraping": "scraping",
            "Silver Processing": "silver",
            "Streaming Events": "streaming",
            "Gold Analytics": "gold",
            "Warehouse Load": "warehouse",
            "Quality Checks": "quality"
        }
        failed_steps = [s for s in steps if self.stats[key_mapping[s[0]]]["status"] == "failed"]
        if failed_steps:
            print("❌ PIPELINE FAILED")
            print(f"Failed steps: {', '.join([s[0] for s in failed_steps])}")
        else:
            print("✅ PIPELINE COMPLETED SUCCESSFULLY")
            if self.stats["quality"]["score"] >= 90:
                print("🌟 Excellent data quality!")
            elif self.stats["quality"]["score"] >= 75:
                print("👍 Good data quality")
            else:
                print("⚠️  Data quality needs attention")

        print("=" * 80)

    def save_pipeline_report(self):
        """Save pipeline execution report."""
        report = {
            "execution_time": datetime.now().isoformat(),
            "total_duration": time.time() - self.start_time,
            "stats": self.stats,
            "success": all(s["status"] in ["completed", "skipped"] for s in self.stats.values())
        }

        os.makedirs("reports", exist_ok=True)
        report_file = f"reports/pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        logger.info(f"📄 Pipeline report saved to: {report_file}")

    def run_pipeline(self) -> bool:
        """Execute the complete pipeline."""
        self.start_time = time.time()

        print("=" * 80)
        print("🚀 NEWS INTELLIGENCE PIPELINE")
        print("=" * 80)
        print(f"Started at: {datetime.now().isoformat()}")
        print(f"Skip scraping: {self.skip_scraping}")
        print(f"Force execution: {self.force}")
        print("=" * 80)

        # Check prerequisites
        if not self.check_prerequisites():
            return False

        # Execute pipeline steps
        steps = [
            ("scraping", self.step_scraping),
            ("silver", self.step_silver_processing),
            ("streaming", self.step_streaming_ingestion),
            ("gold", self.step_gold_processing),
            ("warehouse", self.step_warehouse_load),
            ("quality", self.step_quality_checks)
        ]

        success = True
        for step_name, step_func in steps:
            try:
                if not step_func():
                    success = False
                    if not self.force:
                        logger.error(f"❌ Pipeline stopped at {step_name} step")
                        break
                    else:
                        logger.warning(f"⚠️  Continuing despite {step_name} failure (--force enabled)")
            except Exception as e:
                logger.error(f"❌ Unexpected error in {step_name}: {e}")
                success = False
                if not self.force:
                    break

        # Update dashboard if pipeline succeeded
        if success:
            self.update_dashboard_data()

        # Print summary and save report
        self.print_summary()
        self.save_pipeline_report()

        return success


def main():
    parser = argparse.ArgumentParser(description="News Intelligence Pipeline")
    parser.add_argument("--skip-scraping", action="store_true",
                       help="Skip the web scraping step")
    parser.add_argument("--force", action="store_true",
                       help="Continue pipeline even if steps fail")

    args = parser.parse_args()

    runner = PipelineRunner(skip_scraping=args.skip_scraping, force=args.force)
    success = runner.run_pipeline()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
