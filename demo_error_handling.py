#!/usr/bin/env python3
"""
Demo script showing the comprehensive error handling and data validation system
for storytelling notebooks.

This demonstrates how the system gracefully handles various data quality issues
while providing educational error messages and recovery suggestions.
"""

import sys

import pandas as pd

sys.path.insert(0, ".")

from src.youtubeviz.storytelling import (
    StorytellingDataError,
    create_confidence_indicator,
    create_error_recovery_suggestions,
    generate_data_quality_report,
    handle_missing_data_gracefully,
    validate_data_for_storytelling,
)


def demo_perfect_data():
    """Demonstrate validation with perfect data."""
    print("🟢 DEMO 1: Perfect Data Quality")
    print("=" * 50)

    # Create perfect data
    df = pd.DataFrame(
        {
            "artist_name": ["Taylor Swift", "Drake", "Bad Bunny", "Billie Eilish"],
            "view_count": [5000000, 4200000, 3800000, 3500000],
            "like_count": [250000, 210000, 190000, 175000],
            "comment_count": [15000, 12000, 11000, 10500],
            "published_at": pd.date_range("2024-01-01", periods=4, freq="W"),
        }
    )

    # Validate data
    result = validate_data_for_storytelling(
        df, required_columns=["artist_name", "view_count", "like_count"], analysis_type="artist_comparison", min_rows=3
    )

    print(f"✅ Validation Result: {result['is_valid']}")
    print(f"✅ Confidence Score: {result['confidence_score']:.1%}")
    print(f"✅ Errors: {len(result['errors'])}")
    print(f"✅ Warnings: {len(result['warnings'])}")

    # Generate confidence indicator
    confidence = create_confidence_indicator(
        result["confidence_score"], result["data_quality_issues"], "artist comparison"
    )
    print("\n" + confidence)
    print("\n")


def demo_missing_data():
    """Demonstrate handling of missing data."""
    print("🟡 DEMO 2: Missing Data Handling")
    print("=" * 50)

    # Create data with missing values
    df = pd.DataFrame(
        {
            "artist_name": ["Taylor Swift", "Drake", "Bad Bunny", "Billie Eilish"],
            "view_count": [5000000, None, 3800000, None],  # 50% missing
            "like_count": [250000, 210000, None, 175000],  # 25% missing
            "comment_count": [15000, 12000, 11000, 10500],
        }
    )

    print("Original data shape:", df.shape)
    print("Missing values per column:")
    print(df.isnull().sum())

    # Validate data
    result = validate_data_for_storytelling(
        df, required_columns=["artist_name", "view_count", "like_count"], analysis_type="artist_comparison"
    )

    print(f"\n📊 Validation Result: {result['is_valid']}")
    print(f"📊 Confidence Score: {result['confidence_score']:.1%}")
    print(f"📊 Warnings: {len(result['warnings'])}")

    if result["warnings"]:
        print("\n⚠️ Warnings:")
        for warning in result["warnings"]:
            print(warning[:100] + "..." if len(warning) > 100 else warning)

    # Demonstrate different missing data strategies
    print("\n🔧 MISSING DATA STRATEGIES:")

    # Strategy 1: Exclude missing rows
    cleaned_df1, explanation1 = handle_missing_data_gracefully(df, "view_count", "exclude", "artist comparison")
    print(f"\n1. Exclude Strategy: {len(cleaned_df1)} rows remaining")
    print(explanation1[:150] + "..." if len(explanation1) > 150 else explanation1)

    # Strategy 2: Fill with mean
    cleaned_df2, explanation2 = handle_missing_data_gracefully(df, "view_count", "fill_mean", "trend analysis")
    print(f"\n2. Fill Mean Strategy: {len(cleaned_df2)} rows, mean = {cleaned_df2['view_count'].mean():.0f}")

    # Generate confidence indicator
    confidence = create_confidence_indicator(
        result["confidence_score"], result["data_quality_issues"], "artist comparison"
    )
    print("\n" + confidence)
    print("\n")


def demo_critical_errors():
    """Demonstrate handling of critical data errors."""
    print("🔴 DEMO 3: Critical Data Errors")
    print("=" * 50)

    # Demo 1: Empty data
    print("📭 Testing empty data...")
    try:
        validate_data_for_storytelling(
            pd.DataFrame(), required_columns=["artist_name", "view_count"], analysis_type="trend_analysis"
        )
    except StorytellingDataError as e:
        print(f"❌ Caught expected error: {str(e)}")

        # Show recovery suggestions
        suggestions = create_error_recovery_suggestions(
            "no_data", {"date_range": "2024-01-01 to 2024-01-07", "artists_requested": 2}
        )
        print("\n💡 Recovery Suggestions:")
        print(suggestions[:300] + "..." if len(suggestions) > 300 else suggestions)

    # Demo 2: Missing required columns
    print("\n🔍 Testing missing columns...")
    df_bad_columns = pd.DataFrame(
        {"artist": ["Taylor Swift", "Drake"], "views": [1000000, 2000000]}  # Wrong column name  # Wrong column name
    )

    try:
        validate_data_for_storytelling(
            df_bad_columns,
            required_columns=["artist_name", "view_count", "like_count"],
            analysis_type="artist_comparison",
        )
    except StorytellingDataError as e:
        print(f"❌ Caught expected error: {str(e)}")

        # Show recovery suggestions
        suggestions = create_error_recovery_suggestions("data_quality")
        print("\n💡 Recovery Suggestions:")
        print(suggestions[:300] + "..." if len(suggestions) > 300 else suggestions)

    print("\n")


def demo_data_quality_report():
    """Demonstrate comprehensive data quality reporting."""
    print("📋 DEMO 4: Data Quality Report")
    print("=" * 50)

    # Create data with various quality issues
    df = pd.DataFrame(
        {
            "artist_name": ["Taylor Swift", "Drake", None, "Billie Eilish", "Bad Bunny"],
            "view_count": [5000000, None, 3800000, None, 4100000],
            "like_count": [250000, 210000, 190000, 175000, 205000],
            "engagement_rate": [5.2, None, 4.8, 5.1, 4.9],
            "published_at": ["2024-01-01", "2024-01-08", "invalid-date", "2024-01-22", "2024-01-29"],
            "category": ["Pop", "Hip-Hop", "Pop", "Pop", "Latin"],
        }
    )

    # Generate comprehensive quality report
    report = generate_data_quality_report(df, "artist_comparison")
    print(report)

    # Show confidence assessment
    validation_result = validate_data_for_storytelling(
        df, required_columns=["artist_name", "view_count", "like_count"], analysis_type="artist_comparison"
    )

    confidence = create_confidence_indicator(
        validation_result["confidence_score"], validation_result["data_quality_issues"], "artist comparison"
    )
    print(confidence)
    print("\n")


def demo_analysis_specific_validation():
    """Demonstrate analysis-specific validation scenarios."""
    print("🎯 DEMO 5: Analysis-Specific Validation")
    print("=" * 50)

    # Single artist for comparison analysis
    print("🎤 Testing single artist for comparison...")
    df_single = pd.DataFrame(
        {
            "artist_name": ["Taylor Swift"] * 5,
            "view_count": [1000000, 1200000, 1100000, 1300000, 1150000],
            "like_count": [50000, 60000, 55000, 65000, 57500],
        }
    )

    result = validate_data_for_storytelling(
        df_single, required_columns=["artist_name", "view_count"], analysis_type="artist_comparison"
    )

    print(f"📊 Single artist validation - Confidence: {result['confidence_score']:.1%}")
    if result["warnings"]:
        print("⚠️ Warning:", result["warnings"][0][:100] + "...")

    # Low comment count for sentiment analysis
    print("\n💬 Testing low comment count for sentiment...")
    df_low_comments = pd.DataFrame(
        {
            "artist_name": ["Taylor Swift", "Drake"],
            "comment_count": [25, 30],  # Total 55 < 100 threshold
            "view_count": [1000000, 1200000],
        }
    )

    result = validate_data_for_storytelling(
        df_low_comments, required_columns=["artist_name", "comment_count"], analysis_type="sentiment_analysis"
    )

    print(f"💬 Low comments validation - Confidence: {result['confidence_score']:.1%}")
    if result["warnings"]:
        print("⚠️ Warning:", result["warnings"][0][:100] + "...")

    # Short time range for trend analysis
    print("\n📅 Testing short time range for trends...")
    df_short_time = pd.DataFrame(
        {
            "artist_name": ["Taylor Swift", "Drake"],
            "view_count": [1000000, 1200000],
            "published_at": ["2024-01-01", "2024-01-02"],  # Only 1 day apart
        }
    )

    result = validate_data_for_storytelling(
        df_short_time, required_columns=["artist_name", "view_count", "published_at"], analysis_type="trend_analysis"
    )

    print(f"📅 Short timeframe validation - Confidence: {result['confidence_score']:.1%}")
    if result["warnings"]:
        print("⚠️ Warning:", result["warnings"][0][:100] + "...")

    print("\n")


def main():
    """Run all error handling demos."""
    print("🚀 STORYTELLING ERROR HANDLING & DATA VALIDATION DEMO")
    print("=" * 60)
    print("This demo shows how the storytelling system handles various")
    print("data quality issues with educational error messages.\n")

    demo_perfect_data()
    demo_missing_data()
    demo_critical_errors()
    demo_data_quality_report()
    demo_analysis_specific_validation()

    print("✅ DEMO COMPLETE!")
    print("The storytelling system now provides:")
    print("• Comprehensive data validation with educational messages")
    print("• Graceful degradation for missing data scenarios")
    print("• Confidence indicators for analysis reliability")
    print("• Detailed data quality reports")
    print("• Context-specific error recovery suggestions")
    print("• Analysis-type specific validation rules")


if __name__ == "__main__":
    main()
