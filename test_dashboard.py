#!/usr/bin/env python3
"""
Quick test script to check if MusicScope_YouTube_Dashboard.ipynb cells execute without errors.
"""
import json
import sys
from pathlib import Path


def check_notebook_structure(nb_path):
    """Check if notebook has valid structure and expected cells."""
    with open(nb_path, "r") as f:
        nb = json.load(f)

    cells = nb.get("cells", [])
    print(f"✅ Valid JSON with {len(cells)} cells")

    # Check for expected cell types
    code_cells = [c for c in cells if c["cell_type"] == "code"]
    markdown_cells = [c for c in cells if c["cell_type"] == "markdown"]

    print(f"   📝 Code cells: {len(code_cells)}")
    print(f"   📄 Markdown cells: {len(markdown_cells)}")

    # Check for key cells
    cell_sources = ["".join(c["source"]) for c in cells]

    checks = {
        "Config cell": any("THRESHOLDS" in s and "pre_breakout" in s for s in cell_sources),
        "Data loading": any("videos_df" in s and "comments_df" in s for s in cell_sources),
        "Momentum scoring": any("momentum_score" in s and "s_views" in s for s in cell_sources),
        "Episode detection": any("def _episodes" in s or "episodes =" in s for s in cell_sources),
        "KPI-22 chart": any("KPI 22" in s or "KPI-22" in s for s in cell_sources),
    }

    print("\n🔍 Key components:")
    for name, found in checks.items():
        status = "✅" if found else "❌"
        print(f"   {status} {name}")

    return all(checks.values())


def main():
    nb_path = Path("notebooks/MusicScope_YouTube_Dashboard.ipynb")

    if not nb_path.exists():
        print(f"❌ Notebook not found: {nb_path}")
        return 1

    print(f"📊 Testing: {nb_path}")
    print("=" * 60)

    try:
        all_good = check_notebook_structure(nb_path)

        if all_good:
            print("\n✅ All checks passed!")
            print("\n💡 Next steps:")
            print("   1. Open the notebook in Jupyter/VS Code")
            print("   2. Run all cells (Kernel → Restart & Run All)")
            print("   3. Check for any runtime errors")
            return 0
        else:
            print("\n⚠️  Some checks failed - review notebook structure")
            return 1

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
