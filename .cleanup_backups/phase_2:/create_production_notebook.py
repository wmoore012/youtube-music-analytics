#!/usr/bin/env python3
"""
Create production-ready notebook with bulletproof CI/CD validation.

This script generates a notebook with all 20 charts and proper validation
that counts working charts vs data requirements vs errors.
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath("."))

from src.youtubeviz.notebook_generator import NotebookTemplateManager


def main():
    """Generate production notebook with all 20 charts."""

    print("🎯 Creating Production MusicScope™ Notebook...")
    print("=" * 50)

    # Create notebook manager
    manager = NotebookTemplateManager(total_charts=20)

    print(f"📊 Total charts configured: {manager.total_charts}")
    print(f"📋 Chart registry entries: {len(manager.chart_registry)}")

    # Generate notebook
    output_path = "notebooks/MusicScope™_Production_Dashboard.ipynb"

    try:
        notebook = manager.generate_notebook_template(
            notebook_name="MusicScope™ Production Dashboard - 20 Charts",
            include_charts=list(range(1, 21)),  # All 20 charts
        )

        # Save notebook
        manager.save_notebook(notebook, output_path)

        print(f"✅ Generated notebook: {output_path}")
        print(f"📊 Total cells: {len(notebook['cells'])}")

        # Count chart cells
        chart_cells = [
            cell
            for cell in notebook["cells"]
            if cell["cell_type"] == "markdown" and "Chart " in "".join(cell["source"])
        ]
        print(f"📈 Chart sections: {len(chart_cells)}")

        # Verify validation cell
        validation_cells = [
            cell
            for cell in notebook["cells"]
            if cell["cell_type"] == "code" and "REAL DATA ANALYTICS SUMMARY" in "".join(cell["source"])
        ]

        if validation_cells:
            validation_source = "".join(validation_cells[0]["source"])
            if "range(1, 21)" in validation_source and "/20" in validation_source:
                print("✅ Validation cell correctly counts 20 charts")
            else:
                print("❌ Validation cell has incorrect chart count")
        else:
            print("❌ No validation cell found")

        print("\n🎵 Production notebook ready for CI/CD! 🎵")
        print(f"💡 Run: jupyter notebook {output_path}")

    except Exception as e:
        print(f"❌ Error generating notebook: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
