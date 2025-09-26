#!/usr/bin/env python3
"""
Demo: Enhanced Data Quality Manager

Demonstrates the professional data quality system with real YouTube data.
Shows automatic cleanup, bot detection, and educational reporting.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from src.youtubeviz.enhanced_data_quality import run_enhanced_data_quality_check
from web.etl_helpers import get_engine


def main():
    """Demonstrate the enhanced data quality system."""
    print("🎬 ENHANCED DATA QUALITY MANAGER DEMO")
    print("=" * 60)
    print("This demo shows how the professional data quality system:")
    print("• 🔍 Automatically detects data quality issues")
    print("• 🧹 Performs safe cleanup operations")
    print("• 🤖 Analyzes bot patterns with educational examples")
    print("• 📊 Provides professional reporting with emojis and statistics")
    print("• 💡 Generates actionable recommendations")
    print()

    try:
        # Get database connection
        engine = get_engine()

        # Run the complete enhanced data quality analysis
        print("🚀 Running Enhanced Data Quality Analysis...")
        print()

        report = run_enhanced_data_quality_check(engine)

        # Additional insights
        print("\n🎓 EDUCATIONAL INSIGHTS")
        print("-" * 40)
        print("💡 Why Data Quality Matters in Music Analytics:")
        print("   • Clean data ensures accurate artist performance metrics")
        print("   • Removing invalid records prevents skewed sentiment analysis")
        print("   • Bot detection helps identify authentic fan engagement")
        print("   • Quality scores help track data health over time")

        print("\n📈 Business Impact:")
        if report.total_records_cleaned > 0:
            print(f"   • Improved data accuracy by cleaning {report.total_records_cleaned:,} records")
        print(f"   • Achieved {report.quality_score:.1f}/100 data quality score")

        if report.bot_analysis_summary.get("high_risk", 0) > 0:
            bot_count = report.bot_analysis_summary["high_risk"]
            print(f"   • Identified {bot_count:,} high-risk bot comments for filtering")

        print("\n🔄 Next Steps:")
        print("   • Run this analysis regularly (daily/weekly)")
        print("   • Monitor quality score trends over time")
        print("   • Review bot detection results for pattern changes")
        print("   • Use clean data for accurate business decisions")

        print(f"\n✨ Demo completed successfully!")
        print(f"📊 Final Quality Score: {report.quality_score:.1f}/100")

        return 0

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print("\nTroubleshooting:")
        print("• Ensure database connection is configured in .env")
        print("• Check that YouTube data tables exist")
        print("• Verify bot detection dependencies are installed")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
