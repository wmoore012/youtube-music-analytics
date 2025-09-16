#!/usr/bin/env python3
"""
System Health Monitor - Production-Grade Monitoring
==================================================

Comprehensive monitoring and observability for the YouTube music analytics platform.
Demonstrates advanced data engineering practices for portfolio showcase.

Built by Grammy-nominated producer + M.S. Data Science student.
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class SystemHealthMonitor:
    """
    Production-grade system health monitoring and observability.

    Demonstrates advanced data engineering practices:
    - Comprehensive logging and monitoring
    - Performance metrics and alerting
    - Data quality validation
    - Automated diagnostics
    """

    def __init__(self):
        self.health_report = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "unknown",
            "components": {},
            "performance_metrics": {},
            "alerts": [],
            "recommendations": [],
        }

    def check_database_health(self) -> Dict[str, any]:
        """Check database connectivity and performance."""
        print("🗄️  Checking database health...")

        db_health = {
            "status": "unknown",
            "connection_time": 0.0,
            "record_counts": {},
            "data_freshness": {},
            "performance_metrics": {},
        }

        try:
            # Time database connection
            start_time = time.time()

            # Try to load data and measure performance
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    """
import sys
import time
sys.path.insert(0, '.')

try:
    from src.youtubeviz.data import load_youtube_data
    from web.etl_helpers import get_engine
    from sqlalchemy import text

    # Test database connection
    engine = get_engine()

    # Check table record counts
    with engine.connect() as conn:
        tables = ['youtube_videos', 'youtube_metrics', 'youtube_comments']
        for table in tables:
            try:
                result = conn.execute(text(f'SELECT COUNT(*) as count FROM {table}'))
                count = result.fetchone()[0]
                print(f'{table.upper()}_COUNT:{count}')
            except Exception as e:
                print(f'{table.upper()}_ERROR:{e}')

    # Test data loading performance
    start = time.time()
    df = load_youtube_data()
    load_time = time.time() - start

    print(f'DATA_LOAD_TIME:{load_time:.3f}')
    print(f'TOTAL_RECORDS:{len(df)}')

    # Check data freshness
    if 'published_at' in df.columns:
        latest_video = pd.to_datetime(df['published_at']).max()
        days_old = (pd.Timestamp.now() - latest_video).days
        print(f'LATEST_VIDEO_DAYS_OLD:{days_old}')

    print('DATABASE_STATUS:healthy')

except Exception as e:
    print(f'DATABASE_ERROR:{e}')
    print('DATABASE_STATUS:error')
""",
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )

            connection_time = time.time() - start_time
            db_health["connection_time"] = connection_time

            if result.returncode == 0:
                # Parse results
                for line in result.stdout.strip().split("\n"):
                    if ":" in line:
                        key, value = line.split(":", 1)
                        if key.endswith("_COUNT"):
                            table_name = key.replace("_COUNT", "").lower()
                            db_health["record_counts"][table_name] = int(value)
                        elif key == "DATA_LOAD_TIME":
                            db_health["performance_metrics"]["load_time"] = float(value)
                        elif key == "LATEST_VIDEO_DAYS_OLD":
                            db_health["data_freshness"]["latest_video_age_days"] = int(value)
                        elif key == "DATABASE_STATUS":
                            db_health["status"] = value

                if db_health["status"] == "healthy":
                    print(f"✅ Database healthy (connection: {connection_time:.2f}s)")
                else:
                    print(f"⚠️  Database issues detected")
            else:
                db_health["status"] = "error"
                print(f"❌ Database connection failed")

        except Exception as e:
            db_health["status"] = "error"
            print(f"❌ Database health check failed: {e}")

        return db_health

    def check_model_performance(self) -> Dict[str, any]:
        """Check ML model performance and availability."""
        print("🤖 Checking model performance...")

        model_health = {
            "sentiment_analysis": {"available": False, "performance": {}},
            "bot_detection": {"available": False, "performance": {}},
            "model_benchmarks": {},
        }

        # Test sentiment analysis
        try:
            from src.youtubeviz.music_sentiment import analyze_comment

            # Performance test
            test_comments = [
                "This song is absolutely fire!",
                "This is trash, worst song ever",
                "Pretty decent track, not bad",
            ]

            start_time = time.time()
            results = []
            for comment in test_comments:
                result = analyze_comment(comment)
                results.append(result)
            total_time = time.time() - start_time

            model_health["sentiment_analysis"]["available"] = True
            model_health["sentiment_analysis"]["performance"] = {
                "avg_time_ms": (total_time / len(test_comments)) * 1000,
                "throughput_per_sec": len(test_comments) / total_time,
            }

            print(f"✅ Sentiment analysis available ({total_time*1000/len(test_comments):.1f}ms avg)")

        except Exception as e:
            print(f"⚠️  Sentiment analysis not available: {e}")

        # Test bot detection
        try:
            from src.youtubeviz.bot_detection import is_likely_bot

            start_time = time.time()
            bot_results = []
            for comment in test_comments:
                result = is_likely_bot(comment)
                bot_results.append(result)
            total_time = time.time() - start_time

            model_health["bot_detection"]["available"] = True
            model_health["bot_detection"]["performance"] = {
                "avg_time_ms": (total_time / len(test_comments)) * 1000,
                "throughput_per_sec": len(test_comments) / total_time,
            }

            print(f"✅ Bot detection available ({total_time*1000/len(test_comments):.1f}ms avg)")

        except Exception as e:
            print(f"⚠️  Bot detection not available: {e}")

        return model_health

    def check_system_performance(self) -> Dict[str, any]:
        """Check overall system performance and resource usage."""
        print("⚡ Checking system performance...")

        performance = {
            "cpu_usage": 0.0,
            "memory_usage": 0.0,
            "disk_usage": 0.0,
            "response_times": {},
            "throughput_metrics": {},
        }

        try:
            import psutil

            # Get system metrics
            performance["cpu_usage"] = psutil.cpu_percent(interval=1)
            performance["memory_usage"] = psutil.virtual_memory().percent
            performance["disk_usage"] = psutil.disk_usage(".").percent

            print(
                f"✅ System resources: CPU {performance['cpu_usage']:.1f}%, "
                f"Memory {performance['memory_usage']:.1f}%, "
                f"Disk {performance['disk_usage']:.1f}%"
            )

        except ImportError:
            print("⚠️  psutil not available - install for detailed system metrics")
        except Exception as e:
            print(f"⚠️  Could not get system metrics: {e}")

        return performance

    def generate_health_report(self) -> Dict[str, any]:
        """Generate comprehensive health report."""
        print("\n📊 GENERATING SYSTEM HEALTH REPORT")
        print("=" * 60)

        # Check all components
        self.health_report["components"]["database"] = self.check_database_health()
        self.health_report["components"]["models"] = self.check_model_performance()
        self.health_report["components"]["system"] = self.check_system_performance()

        # Determine overall status
        component_statuses = []
        for component, health in self.health_report["components"].items():
            if isinstance(health, dict) and "status" in health:
                component_statuses.append(health["status"])

        if "error" in component_statuses:
            self.health_report["overall_status"] = "critical"
        elif "degraded" in component_statuses:
            self.health_report["overall_status"] = "degraded"
        else:
            self.health_report["overall_status"] = "healthy"

        # Generate recommendations
        self._generate_recommendations()

        return self.health_report

    def _generate_recommendations(self):
        """Generate actionable recommendations based on health check."""
        db_health = self.health_report["components"].get("database", {})
        model_health = self.health_report["components"].get("models", {})

        # Database recommendations
        if db_health.get("status") == "error":
            self.health_report["recommendations"].append(
                "Database connection failed - check credentials and connectivity"
            )

        # Model recommendations
        if not model_health.get("sentiment_analysis", {}).get("available", False):
            self.health_report["recommendations"].append("Sentiment analysis not available - run: make setup-sentiment")

        if not model_health.get("bot_detection", {}).get("available", False):
            self.health_report["recommendations"].append("Bot detection not available - check bot_detection.py imports")

        # Performance recommendations
        db_load_time = db_health.get("performance_metrics", {}).get("load_time", 0)
        if db_load_time > 2.0:
            self.health_report["recommendations"].append(
                f"Data loading slow ({db_load_time:.1f}s) - consider query optimization"
            )

    def save_report(self, filename: str = "system_health_report.json"):
        """Save health report to file."""
        with open(filename, "w") as f:
            json.dump(self.health_report, f, indent=2)
        print(f"✅ Health report saved to {filename}")

    def print_summary(self):
        """Print executive summary of system health."""
        print(f"\n🏥 SYSTEM HEALTH SUMMARY")
        print("=" * 60)

        status_emoji = {"healthy": "🟢", "degraded": "🟡", "critical": "🔴", "unknown": "⚪"}

        overall_status = self.health_report["overall_status"]
        print(f"Overall Status: {status_emoji.get(overall_status, '⚪')} {overall_status.upper()}")

        # Component status
        for component, health in self.health_report["components"].items():
            if isinstance(health, dict):
                comp_status = health.get("status", "unknown")
                print(f"{component.title()}: {status_emoji.get(comp_status, '⚪')} {comp_status}")

        # Key metrics
        db_health = self.health_report["components"].get("database", {})
        total_records = sum(db_health.get("record_counts", {}).values())
        if total_records > 0:
            print(f"\nKey Metrics:")
            print(f"  Total Records: {total_records:,}")
            print(f"  Connection Time: {db_health.get('connection_time', 0):.2f}s")

        # Recommendations
        if self.health_report["recommendations"]:
            print(f"\n💡 Recommendations:")
            for i, rec in enumerate(self.health_report["recommendations"], 1):
                print(f"  {i}. {rec}")


def main():
    """Run system health monitoring."""
    print("🏥 SYSTEM HEALTH MONITOR")
    print("YouTube Music Analytics Platform")
    print("Built by Grammy-nominated producer + M.S. Data Science student")
    print("=" * 70)

    monitor = SystemHealthMonitor()

    # Generate comprehensive health report
    health_report = monitor.generate_health_report()

    # Print summary
    monitor.print_summary()

    # Save detailed report
    monitor.save_report()

    # Exit with appropriate code
    if health_report["overall_status"] in ["critical", "error"]:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
