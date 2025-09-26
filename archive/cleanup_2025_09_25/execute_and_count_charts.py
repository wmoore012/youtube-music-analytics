#!/usr/bin/env python3
"""
Execute the ultimate notebook and count the actual charts produced.
Uses real data from the database - no mocking.
"""

import json
import os
import subprocess
import sys


def execute_notebook_and_count_charts():
    """Execute notebook with real data and count chart outputs."""

    print("🚀 EXECUTING ULTIMATE NOTEBOOK WITH REAL DATA")
    print("=" * 60)

    notebook_path = "notebooks/MusicScope™_Ultimate_Analytics_Dashboard.ipynb"
    executed_path = "notebooks/MusicScope™_Ultimate_Analytics_Dashboard_executed.ipynb"

    if not os.path.exists(notebook_path):
        print(f"❌ Notebook not found: {notebook_path}")
        return False

    print("📊 Executing notebook (this will use real database data)...")
    print("⏱️  This may take several minutes...")

    try:
        # Execute the notebook
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
                "--ExecutePreprocessor.timeout=600",  # 10 minute timeout
                notebook_path,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print(f"❌ Notebook execution failed:")
            print(f"STDERR: {result.stderr}")
            if "No module named" in result.stderr:
                print("💡 Missing dependencies - run: pip install -e .")
            return False

        print("✅ Notebook executed successfully!")

        # Now count the actual charts in the executed notebook
        return count_charts_in_executed_notebook(executed_path)

    except FileNotFoundError:
        print("❌ Jupyter not found. Install with: pip install jupyter nbconvert")
        return False
    except Exception as e:
        print(f"❌ Execution error: {e}")
        return False


def count_charts_in_executed_notebook(executed_path):
    """Count actual chart outputs in the executed notebook."""

    print(f"\n📊 ANALYZING EXECUTED NOTEBOOK: {executed_path}")
    print("=" * 60)

    try:
        with open(executed_path, "r") as f:
            notebook = json.load(f)

        chart_count = 0
        error_count = 0
        success_messages = 0

        for i, cell in enumerate(notebook["cells"]):
            if cell["cell_type"] == "code" and cell.get("outputs"):

                cell_has_chart = False
                cell_has_error = False
                cell_has_success = False

                for output in cell["outputs"]:
                    # Count Plotly charts (the real test!)
                    if output.get("output_type") == "display_data":
                        data = output.get("data", {})
                        if "application/vnd.plotly.v1+json" in data:
                            chart_count += 1
                            cell_has_chart = True
                            print(f"✅ Chart found in cell {i}")

                    # Count errors
                    elif output.get("output_type") == "error":
                        error_count += 1
                        cell_has_error = True
                        error_name = output.get("ename", "Unknown")
                        error_value = output.get("evalue", "Unknown error")
                        print(f"❌ Error in cell {i}: {error_name}: {error_value}")

                    # Count success messages
                    elif output.get("output_type") == "stream":
                        text = output.get("text", "")
                        if "✅ Chart" in text and "complete" in text:
                            success_messages += 1
                            cell_has_success = True
                        elif "⚠️  Chart" in text and "issue" in text:
                            print(f"⚠️  Chart issue in cell {i}: {text.strip()}")

                # Report cell status
                if cell_has_chart:
                    pass  # Already reported above
                elif cell_has_error:
                    pass  # Already reported above
                elif cell_has_success:
                    print(f"✅ Success message in cell {i} (but no chart output)")
                elif "fig_" in "".join(cell.get("source", [])):
                    print(f"⚠️  Chart cell {i} ran but produced no output")

        # Final count
        print("\n" + "=" * 60)
        print("🎯 FINAL CHART COUNT RESULTS")
        print("=" * 60)
        print(f"📊 Plotly charts generated: {chart_count}")
        print(f"✅ Success messages: {success_messages}")
        print(f"❌ Errors encountered: {error_count}")
        print(f"📓 Total cells: {len(notebook['cells'])}")

        # Success criteria
        if chart_count >= 15:
            print(f"\n🎉 ULTIMATE SUCCESS! All 15+ charts generated!")
            print(f"🏆 The notebook is production-ready!")
        elif chart_count >= 10:
            print(f"\n🎊 STRONG SUCCESS! {chart_count}/15 charts working!")
            print(f"💪 Most functionality implemented!")
        elif chart_count >= 5:
            print(f"\n⚡ GOOD PROGRESS! {chart_count}/15 charts working!")
            print(f"🔧 Core system functional, need more implementations!")
        elif chart_count >= 1:
            print(f"\n🌱 BASIC SUCCESS! {chart_count}/15 charts working!")
            print(f"🚀 Foundation is solid, keep building!")
        else:
            print(f"\n🔧 NEEDS WORK! No charts generated!")
            print(f"💡 Focus on data loading and chart function implementation!")

        return chart_count >= 15

    except Exception as e:
        print(f"❌ Error analyzing executed notebook: {e}")
        return False


def main():
    """Main execution."""

    # Check if we have the notebook
    if not os.path.exists("notebooks/MusicScope™_Ultimate_Analytics_Dashboard.ipynb"):
        print("❌ Ultimate notebook not found!")
        print("💡 Run create_ultimate_notebook.py first")
        return

    # Execute and count
    success = execute_notebook_and_count_charts()

    if success:
        print(f"\n🎵 SUCCESS: Ultimate notebook is working with 15+ charts! 🎵")
    else:
        print(f"\n🔧 The notebook needs more work to reach 15 charts.")
        print(f"💡 Check the executed notebook for specific errors and missing functions.")


if __name__ == "__main__":
    main()
