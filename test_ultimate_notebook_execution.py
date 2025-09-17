#!/usr/bin/env python3
"""
Test the ultimate notebook execution to ensure all 15 charts work.
"""

import sys
import traceback


def test_notebook_imports():
    """Test that all required imports work."""
    print("🧪 Testing notebook imports...")

    try:
        # Test core imports
        import numpy as np
        import pandas as pd
        import plotly.graph_objects as go

        print("✅ Core data science stack imported")

        # Test statistical utils
        from youtubeviz.statistical_utils import (
            apply_bayesian_shrinkage,
            apply_loess_smoothing,
            calculate_wilson_intervals,
        )

        print("✅ Statistical utilities imported")

        # Test advanced charts (all 15 functions)
        from youtubeviz.advanced_charts import (
            ColorBrewerPalettes,
            create_diverging_sentiment_bars,
            create_negative_theme_lollipops,
            create_positive_theme_lollipops,
            create_sentiment_cluster_heatmap,
            create_standout_videos_scatter,
            enhance_chart_beauty,
        )

        print("✅ Advanced charts imported (6/15 functions available)")

        # Test core analytics
        from youtubeviz.config_validation import get_artists_from_env
        from youtubeviz.data import load_recent_window_days

        print("✅ Core analytics imported")

        return True

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        traceback.print_exc()
        return False


def test_chart_function_availability():
    """Test which chart functions are actually implemented."""
    print("\n🔍 Testing chart function availability...")

    chart_functions = [
        "create_diverging_sentiment_bars",
        "create_sentiment_cluster_heatmap",
        "create_positive_theme_lollipops",
        "create_negative_theme_lollipops",
        "create_standout_videos_scatter",
        "create_tour_compatibility_analysis",
        "create_upset_feature_intersections",
        "create_isrc_balance_bars",
        "create_content_length_dumbbells",
        "create_content_type_dots",
        "create_views_by_category_areas",
        "create_genre_context_heatmap",
        "create_roster_rank_bump_chart",
        "create_polarity_ridgelines",
        "create_ab_test_framework",
    ]

    available_functions = []
    missing_functions = []

    try:
        import youtubeviz.advanced_charts as charts_module

        for func_name in chart_functions:
            if hasattr(charts_module, func_name):
                available_functions.append(func_name)
                print(f"✅ {func_name}")
            else:
                missing_functions.append(func_name)
                print(f"❌ {func_name} - not implemented")

    except ImportError as e:
        print(f"❌ Could not import advanced_charts module: {e}")
        missing_functions = chart_functions

    print(f"\n📊 Chart Function Summary:")
    print(f"   ✅ Available: {len(available_functions)}/15")
    print(f"   ❌ Missing: {len(missing_functions)}/15")

    if missing_functions:
        print(f"\n🔧 Missing functions to implement:")
        for func in missing_functions:
            print(f"   • {func}")

    return len(available_functions), len(missing_functions)


def test_sample_chart_generation():
    """Test generating a sample chart with mock data."""
    print("\n🎨 Testing sample chart generation...")

    try:
        import numpy as np
        import pandas as pd

        from youtubeviz.advanced_charts import create_diverging_sentiment_bars, enhance_chart_beauty

        # Create mock data
        np.random.seed(42)
        mock_data = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B", "Artist C"] * 20,
                "sentiment_category": np.random.choice(["positive", "negative", "neutral"], 60),
                "daily_views": np.random.randint(100, 10000, 60),
                "engagement_rate": np.random.uniform(0.01, 0.1, 60),
            }
        )

        # Test chart generation
        fig = create_diverging_sentiment_bars(mock_data)
        fig = enhance_chart_beauty(fig, theme="professional")

        print("✅ Sample chart generated successfully")
        print(f"   📊 Chart has {len(fig.data)} traces")
        print(f"   📏 Chart height: {fig.layout.height or 500}px")

        return True

    except Exception as e:
        print(f"❌ Chart generation failed: {e}")
        traceback.print_exc()
        return False


def test_notebook_execution():
    """Test actual notebook execution and count chart outputs."""
    print("\n📓 Testing actual notebook execution...")

    try:
        import json
        import os
        import subprocess

        notebook_path = "notebooks/MusicScope™_Ultimate_Analytics_Dashboard.ipynb"

        if not os.path.exists(notebook_path):
            print(f"❌ Notebook not found: {notebook_path}")
            return False, 0

        # Execute notebook using nbconvert
        print("🔄 Executing notebook (this may take a while)...")

        result = subprocess.run(
            [
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--output",
                "MusicScope™_Ultimate_Analytics_Dashboard_executed.ipynb",
                "--output-dir",
                "notebooks/",
                notebook_path,
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )  # 5 minute timeout

        if result.returncode != 0:
            print(f"❌ Notebook execution failed:")
            print(f"   STDOUT: {result.stdout}")
            print(f"   STDERR: {result.stderr}")
            return False, 0

        # Read executed notebook and count charts
        executed_path = "notebooks/MusicScope™_Ultimate_Analytics_Dashboard_executed.ipynb"

        with open(executed_path, "r") as f:
            executed_notebook = json.load(f)

        # Count successful chart generations
        chart_count = 0
        error_count = 0

        for cell in executed_notebook["cells"]:
            if cell["cell_type"] == "code" and cell.get("outputs"):
                for output in cell["outputs"]:
                    # Check for successful chart generation
                    if output.get("output_type") == "display_data":
                        if "application/vnd.plotly.v1+json" in output.get("data", {}):
                            chart_count += 1

                    # Check for error outputs
                    elif output.get("output_type") == "error":
                        error_count += 1

                    # Check for successful completion messages in stdout
                    elif output.get("output_type") == "stream" and output.get("name") == "stdout":
                        text = output.get("text", "")
                        if "✅ Chart" in text and "complete" in text:
                            # This indicates successful chart generation
                            pass

        print(f"✅ Notebook executed successfully")
        print(f"   📊 Charts generated: {chart_count}")
        print(f"   ❌ Errors encountered: {error_count}")
        print(f"   📄 Executed notebook saved to: {executed_path}")

        return True, chart_count

    except subprocess.TimeoutExpired:
        print("❌ Notebook execution timed out (>5 minutes)")
        return False, 0
    except FileNotFoundError:
        print("❌ Jupyter not found - install with: pip install jupyter nbconvert")
        return False, 0
    except Exception as e:
        print(f"❌ Notebook execution error: {e}")
        traceback.print_exc()
        return False, 0


def test_notebook_cell_count():
    """Test that notebook has the expected number of cells."""
    print("\n📋 Testing notebook structure...")

    try:
        import json

        notebook_path = "notebooks/MusicScope™_Ultimate_Analytics_Dashboard.ipynb"

        with open(notebook_path, "r") as f:
            notebook = json.load(f)

        total_cells = len(notebook["cells"])
        code_cells = len([c for c in notebook["cells"] if c["cell_type"] == "code"])
        markdown_cells = len([c for c in notebook["cells"] if c["cell_type"] == "markdown"])

        # Count chart cells specifically
        chart_cells = 0
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                source = "".join(cell.get("source", []))
                if "fig_" in source and "create_" in source:
                    chart_cells += 1

        print(f"✅ Notebook structure analysis:")
        print(f"   📊 Total cells: {total_cells}")
        print(f"   💻 Code cells: {code_cells}")
        print(f"   📝 Markdown cells: {markdown_cells}")
        print(f"   📈 Chart generation cells: {chart_cells}")

        # Validate against design spec expectations
        meets_spec = total_cells >= 30 and chart_cells >= 15

        if meets_spec:
            print("✅ Notebook structure meets specification")
        else:
            print("⚠️  Notebook structure below specification")
            print(f"   Expected: ≥30 total cells, ≥15 chart cells")
            print(f"   Actual: {total_cells} total cells, {chart_cells} chart cells")

        return meets_spec, total_cells, chart_cells

    except Exception as e:
        print(f"❌ Notebook structure test failed: {e}")
        return False, 0, 0


def main():
    """Run all tests including notebook execution."""
    print("🧪 TESTING ULTIMATE NOTEBOOK EXECUTION")
    print("=" * 50)

    # Test 1: Imports
    imports_ok = test_notebook_imports()

    # Test 2: Function availability
    available_count, missing_count = test_chart_function_availability()

    # Test 3: Sample chart
    chart_ok = test_sample_chart_generation()

    # Test 4: Notebook structure (before execution)
    structure_ok, total_cells, chart_cells = test_notebook_cell_count()

    # Test 5: EXECUTE THE NOTEBOOK (the critical step!)
    print("\n" + "🚀" * 20)
    print("🚀 EXECUTING NOTEBOOK - THIS IS THE REAL TEST!")
    print("🚀" * 20)
    execution_ok, executed_charts = test_notebook_execution()

    # Final summary
    print("\n" + "=" * 50)
    print("🎯 COMPREHENSIVE TEST SUMMARY")
    print("=" * 50)

    if imports_ok:
        print("✅ Core imports: PASS")
    else:
        print("❌ Core imports: FAIL")

    print(f"📊 Chart functions: {available_count}/15 available")

    if chart_ok:
        print("✅ Chart generation: PASS")
    else:
        print("❌ Chart generation: FAIL")

    if structure_ok:
        print(f"✅ Notebook structure: PASS ({total_cells} cells, {chart_cells} chart cells)")
    else:
        print(f"⚠️  Notebook structure: NEEDS IMPROVEMENT ({total_cells} cells, {chart_cells} chart cells)")

    if execution_ok:
        print(f"✅ Notebook execution: PASS ({executed_charts} charts generated)")
    else:
        print("❌ Notebook execution: FAIL")

    # Critical success criteria
    print("\n🎯 SUCCESS CRITERIA EVALUATION:")
    print(f"   📊 Target: 15 charts | Actual: {executed_charts} charts")

    if executed_charts >= 15:
        print("🎉 ULTIMATE SUCCESS: All 15 charts generated in executed notebook!")
    elif executed_charts >= 10:
        print("🎊 STRONG SUCCESS: Most charts working, minor gaps to fill")
    elif executed_charts >= 5:
        print("⚠️  PARTIAL SUCCESS: Core system works, need more chart implementations")
    else:
        print("❌ NEEDS WORK: Major implementation gaps")

    # Overall assessment
    if execution_ok and executed_charts >= 15:
        print("\n🏆 NOTEBOOK FULLY READY FOR PRODUCTION!")
        print("💝 All 15 data-science-grade charts working with statistical rigor")
    elif execution_ok and executed_charts >= 5:
        print("\n🎯 NOTEBOOK READY FOR DEVELOPMENT!")
        print("💡 Core system works, implement remaining chart functions")
    else:
        print("\n🔧 NOTEBOOK NEEDS DEVELOPMENT")
        print("💡 Focus on core execution and chart implementation")

    print(f"\n📓 Notebook: notebooks/MusicScope™_Ultimate_Analytics_Dashboard.ipynb")
    print(f"📊 Expected charts: 15")
    print(f"🎨 Available functions: {available_count}")
    print(f"✅ Executed charts: {executed_charts}")

    return executed_charts >= 15


if __name__ == "__main__":
    main()
