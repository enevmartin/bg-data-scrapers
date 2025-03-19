import os
import time
import json
import logging
import psutil
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ScraperMonitor:
    """Monitor for tracking scraper progress and resource usage"""

    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
        self.log_file = os.path.join(storage_dir, "monitor_log.json")

    def check_disk_usage(self, path=None):
        """Check disk usage for the storage directory"""
        if path is None:
            path = self.storage_dir

        disk = psutil.disk_usage(path)
        return {
            "total": disk.total,
            "used": disk.used,
            "free": disk.free,
            "percent": disk.percent
        }

    def check_stuck_scrapers(self, max_duration_hours=3):
        """Check for scrapers that might be stuck"""
        cutoff_time = datetime.now() - timedelta(hours=max_duration_hours)
        stuck_scrapers = []

        # Look for stats files in each institution directory
        for institution_dir in os.listdir(self.storage_dir):
            institution_path = os.path.join(self.storage_dir, institution_dir)

            if not os.path.isdir(institution_path):
                continue

            # Look for the latest date directory
            date_dirs = sorted([
                d for d in os.listdir(institution_path)
                if os.path.isdir(os.path.join(institution_path, d))
            ], reverse=True)

            if not date_dirs:
                continue

            latest_date_dir = os.path.join(institution_path, date_dirs[0])

            # Look for stats files
            for filename in os.listdir(latest_date_dir):
                if filename.startswith("stats_") and filename.endswith(".json"):
                    stats_path = os.path.join(latest_date_dir, filename)

                    try:
                        with open(stats_path, 'r') as f:
                            stats = json.load(f)

                        # Check if the scraper is completed
                        if not stats.get("end_time"):
                            # Check file modification time
                            mtime = datetime.fromtimestamp(os.path.getmtime(stats_path))

                            if mtime < cutoff_time:
                                stuck_scrapers.append({
                                    "institution": institution_dir,
                                    "stats_file": stats_path,
                                    "last_modified": mtime.isoformat()
                                })
                    except Exception as e:
                        logger.error(f"Error checking stats file {stats_path}: {str(e)}")

        return stuck_scrapers

    def generate_summary_report(self):
        """Generate a summary report of all scrapers"""
        summary = {
            "timestamp": datetime.now().isoformat(),
            "disk_usage": self.check_disk_usage(),
            "institutions": {},
            "stuck_scrapers": self.check_stuck_scrapers()
        }

        # Look for stats files in each institution directory
        for institution_dir in os.listdir(self.storage_dir):
            institution_path = os.path.join(self.storage_dir, institution_dir)

            if not os.path.isdir(institution_path):
                continue

            institution_stats = {
                "total_files": 0,
                "total_size": 0,
                "last_run": None,
                "runs": []
            }

            # Process each date directory
            for date_dir in sorted(os.listdir(institution_path), reverse=True):
                date_path = os.path.join(institution_path, date_dir)

                if not os.path.isdir(date_path):
                    continue

                # Look for stats files
                for filename in os.listdir(date_path):
                    if filename.startswith("stats_") and filename.endswith(".json"):
                        stats_path = os.path.join(date_path, filename)

                        try:
                            with open(stats_path, 'r') as f:
                                stats = json.load(f)

                            # Add run stats
                            run_stats = {
                                "date": date_dir,
                                "files_scraped": stats.get("files_scraped", 0),
                                "files_failed": stats.get("files_failed", 0),
                                "total_size": stats.get("total_size", 0),
                                "duration": stats.get("duration", 0),
                                "completed": bool(stats.get("end_time"))
                            }

                            institution_stats["runs"].append(run_stats)

                            # Update totals
                            institution_stats["total_files"] += run_stats["files_scraped"]
                            institution_stats["total_size"] += run_stats["total_size"]

                            # Update last run
                            if not institution_stats["last_run"] or date_dir > institution_stats["last_run"]:
                                institution_stats["last_run"] = date_dir
                        except Exception as e:
                            logger.error(f"Error reading stats file {stats_path}: {str(e)}")

            # Add institution stats to summary
            summary["institutions"][institution_dir] = institution_stats

        # Save summary report
        report_path = os.path.join(
            self.storage_dir,
            f"summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        with open(report_path, 'w') as f:
            json.dump(summary, f, indent=2)

        # Also update the latest report
        latest_report_path = os.path.join(self.storage_dir, "latest_summary.json")
        with open(latest_report_path, 'w') as f:
            json.dump(summary, f, indent=2)

        return summary

    def run_continuous_monitoring(self, interval_minutes=30):
        """Run continuous monitoring at specified intervals"""
        logger.info(f"Starting continuous monitoring with {interval_minutes} minute intervals")

        while True:
            try:
                summary = self.generate_summary_report()

                # Check for stuck scrapers
                stuck_scrapers = summary.get("stuck_scrapers", [])
                if stuck_scrapers:
                    logger.warning(f"Found {len(stuck_scrapers)} stuck scrapers")
                    # Add notification logic here if needed

                # Check disk usage
                disk_usage = summary.get("disk_usage", {})
                if disk_usage.get("percent", 0) > 80:
                    logger.warning(f"Disk usage is high: {disk_usage.get('percent')}%")
                    # Add notification logic here if needed

                # Sleep until next check
                logger.info(f"Monitoring completed. Next check in {interval_minutes} minutes")
                time.sleep(interval_minutes * 60)

            except Exception as e:
                logger.error(f"Error in monitoring: {str(e)}")
                time.sleep(60)  # Shorter interval on error