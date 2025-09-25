#!/usr/bin/env python3
"""
Test that TDD validation system detects working charts.

Creates a minimal notebook with 1 working chart to prove the system works.
"""

import json
import os
import subprocess
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath("."))


def create_minimal_working_notebook():
    """Create minimal notebook with 1 working chart."""

    notebook = {
        "cells": [
            {"cell_type": "markdown", "metadata": {}, "source": "# 🎯 TDD Test - 1 Working Chart\n"},
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Load data and chart function\n",
                    "from youtubeviz.data import load_recent_window_days\n",
                    "from youtubeviz.advanced_charts import create_diverging_sentiment_bars\n",
                    "import plotly.graph_objects as go\n",
                    "\n",
                    "# Load real data\n",
                    "df = load_recent_window_days(days=30)\n",
                    "print(f'✅ Data loaded: {len(df)} rows')\n",
                ],
            },
            {"cell_type": "markdown", "metadata": {}, "source": "## Chart 1: Working Chart\n"},
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "print('🎨 Generating Chart 1: Working Chart...')\n",
                    "\n",
                    "try:\n",
                    "    if not df.empty:\n",
                    "        fig_1 = create_diverging_sentiment_bars(df)\n",
                    "        \n",
                    "        if fig_1 and hasattr(fig_1, 'data') and len(fig_1.data) > 0:\n",
                    "            fig_1.show()\n",
                    "            print('✅ Chart 1: Generated with REAL data!')\n",
                    "        else:\n",
                    "            print('❌ Chart 1: Function returned empty figure')\n",
                    "            fig_1 = None\n",
                    "    else:\n",
                    "        print('📋 Chart 1: No data available')\n",
                    "        fig_1 = None\n",
                    "        \n",
                    "except Exception as e:\n",
                    "    print(f'❌ Chart 1 error: {e}')\n",
                    "    fig_1 = None\n",
                ],
            },
            {"cell_type": "markdown", "metadata": {}, "source": "---\n\n# 🎯 VALIDATION SUMMARY\n"},
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Count working charts\n",
                    "real_charts = []\n",
                    "requirement_charts = []\n",
                    "error_charts = []\n",
                    "\n",
                    "# Check chart 1\n",
                    "if 'fig_1' in locals():\n",
                    "    fig = locals()['fig_1']\n",
                    "    if fig is not None and hasattr(fig, 'data') and len(fig.data) > 0:\n",
                    "        # Check if it has real data\n",
                    "        has_real_data = any(\n",
                    "            hasattr(trace, 'x') and len(getattr(trace, 'x', [])) > 1 \n",
                    "            for trace in fig.data\n",
                    "        )\n",
                    "        if has_real_data:\n",
                    "            real_charts.append(1)\n",
                    "        else:\n",
                    "            requirement_charts.append(1)\n",
                    "    else:\n",
                    "        error_charts.append(1)\n",
                    "else:\n",
                    "    error_charts.append(1)\n",
                    "\n",
                    "print('🎯 REAL DATA ANALYTICS SUMMARY')\n",
                    "print('=' * 50)\n",
                    "print(f'📊 Charts with REAL data: {len(real_charts)}/1')\n",
                    "print(f'📋 Charts showing data requirements: {len(requirement_charts)}/1')\n",
                    "print(f'❌ Charts with errors: {len(error_charts)}/1')\n",
                    "\n",
                    "if real_charts:\n",
                    "    print(f'\\n✅ Working with real data: {real_charts}')\n",
                    "if requirement_charts:\n",
                    "    print(f'📋 Need data columns: {requirement_charts}')\n",
                    "if error_charts:\n",
                    "    print(f'❌ Have errors: {error_charts}')\n",
                    "\n",
                    "# Success message\n",
                    "if len(real_charts) >= 1:\n",
                    "    print(f'\\n🎉 SUCCESS: {len(real_charts)} chart working with REAL data!')\n",
                    "    print('💝 No fake data used - authentic analytics only!')\n",
                    "else:\n",
                    "    print('\\n📋 All charts show data requirements - add real data to see analytics!')\n",
                    "\n",
                    "print('\\n🎵 MusicScope™ Real Data Analytics Complete! 🎵')\n",
                    "\n",
                    "# CI/CD validation\n",
                    "success_rate = len(real_charts) / 1 if 1 > 0 else 0\n",
                    "if success_rate >= 0.8:\n",
                    "    print('\\n✅ CI/CD: PASS - Excellent chart health')\n",
                    "elif success_rate >= 0.6:\n",
                    "    print('\\n⚠️  CI/CD: WARNING - Acceptable but needs improvement')\n",
                    "else:\n",
                    "    print('\\n❌ CI/CD: FAIL - Poor chart health')\n",
                ],
            },
        ],
        "metadata": {
            "kernelspec": {"display_name": "YouTube Analytics", "language": "python", "name": "youtubeviz"},
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.13.5",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }

    return notebook


def main():
    """Test TDD validation with working chart."""

    print("🎯 Testing TDD Validation with 1 Working Chart")
    print("=" * 50)

    # Create minimal notebook
    notebook = create_minimal_working_notebook()

    # Save notebook
    output_path = "notebooks/TDD_Test_Working_Chart.ipynb"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)

    print(f"✅ Created test notebook: {output_path}")

    # Execute notebook
    try:
        executed_filename = "TDD_Test_Working_Chart_executed.ipynb"
        cmd = ["jupyter", "nbconvert", "--to", "notebook", "--execute", "--output", executed_filename, output_path]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        if result.returncode == 0:
            print("✅ Notebook executed successfully")

            # Load executed notebook and check output
            executed_path = os.path.join("notebooks", executed_filename)
            with open(executed_path, "r", encoding="utf-8") as f:
                executed_notebook = json.load(f)

            # Find validation output
            for cell in executed_notebook["cells"]:
                if cell.get("cell_type") == "code":
                    outputs = cell.get("outputs", [])
                    for output in outputs:
                        if output.get("output_type") == "stream" and output.get("name") == "stdout":
                            text = "".join(output.get("text", []))
                            if "REAL DATA ANALYTICS SUMMARY" in text:
                                print("\n" + "=" * 50)
                                print("📊 TDD VALIDATION OUTPUT:")
                                print("=" * 50)
                                print(text)

                                # Check for success indicators
                                if "Charts with REAL data: 1/1" in text:
                                    print("\n🎉 SUCCESS: TDD system detected working chart!")
                                    print("✅ System correctly reports 1/1 charts working")
                                    return True
                                else:
                                    print("\n❌ FAILURE: TDD system did not detect working chart")
                                    return False

            print("❌ No validation output found in executed notebook")
            return False

        else:
            print(f"❌ Notebook execution failed: {result.stderr}")
            return False

    except Exception as e:
        print(f"❌ Error executing notebook: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎯 TDD VALIDATION SYSTEM: PROVEN TO WORK!")
        print("💝 System detects working charts with real data")
    else:
        print("\n💥 TDD validation test failed")

    exit(0 if success else 1)
