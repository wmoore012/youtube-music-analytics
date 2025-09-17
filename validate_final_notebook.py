#!/usr/bin/env python3
"""
Comprehensive validation of the final notebook execution.
This test checks the exact cell output you specified.
"""

import json
import re
import subprocess


def execute_and_validate_notebook():
    """Execute notebook and validate the final results."""

    print("🧪 COMPREHENSIVE NOTEBOOK VALIDATION")
    print("=" * 60)

    notebook_path = "notebooks/MusicScope™_Real_Data_Dashboard.ipynb"
    executed_path = "notebooks/MusicScope™_Real_Data_Dashboard_executed.ipynb"

    # Execute the notebook
    print("🚀 Executing notebook...")
    result = subprocess.run(
        [
            "jupyter",
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            "--output",
            "MusicScope™_Real_Data_Dashboard_executed.ipynb",
            "--output-dir",
            "notebooks/",
            "--ExecutePreprocessor.kernel_name=youtubeviz",
            "--ExecutePreprocessor.timeout=300",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"❌ Notebook execution failed: {result.stderr}")
        return False

    print("✅ Notebook executed successfully")

    # Load and analyze the executed notebook
    with open(executed_path, "r") as f:
        notebook = json.load(f)

    # Find the final validation cell
    validation_cell = None
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code" and cell.get("outputs"):
            for output in cell["outputs"]:
                if output.get("output_type") == "stream":
                    text = output.get("text", "")
                    if isinstance(text, list):
                        text = "".join(text)
                    if "FINAL CHART COUNT RESULTS" in text:
                        validation_cell = text
                        break

    if not validation_cell:
        print("❌ Could not find final validation cell")
        return False

    print("✅ Found final validation cell")
    print("\n📊 VALIDATION CELL OUTPUT:")
    print("-" * 40)
    print(validation_cell)
    print("-" * 40)

    # Parse the results
    successful_match = re.search(r"✅ Successful charts: (\d+)/15", validation_cell)
    failed_match = re.search(r"❌ Failed charts: (\d+)/15", validation_cell)

    if not successful_match or not failed_match:
        print("❌ Could not parse chart counts")
        return False

    successful_count = int(successful_match.group(1))
    failed_count = int(failed_match.group(1))

    print(f"\n📊 PARSED RESULTS:")
    print(f"   ✅ Successful: {successful_count}/15")
    print(f"   ❌ Failed: {failed_count}/15")

    # Validate the counts
    if successful_count + failed_count != 15:
        print(f"❌ Chart counts don't add up: {successful_count} + {failed_count} ≠ 15")
        return False

    # Check for specific success criteria
    if successful_count >= 15:
        print("\n🎉 ULTIMATE SUCCESS: All 15 charts working!")
        success_level = "ULTIMATE"
    elif successful_count >= 13:
        print(f"\n🎊 STRONG SUCCESS: {successful_count}/15 charts working!")
        success_level = "STRONG"
    elif successful_count >= 10:
        print(f"\n⚡ GOOD SUCCESS: {successful_count}/15 charts working!")
        success_level = "GOOD"
    elif successful_count >= 5:
        print(f"\n🌱 BASIC SUCCESS: {successful_count}/15 charts working!")
        success_level = "BASIC"
    else:
        print(f"\n🔧 NEEDS WORK: Only {successful_count}/15 charts working!")
        success_level = "NEEDS_WORK"

    # Check for import success
    import_success = "All 15 chart functions imported successfully!" in validation_cell
    if import_success:
        print("✅ All 15 chart functions imported successfully")
    else:
        print("❌ Chart function import issues detected")

    # Check for data loading
    data_loading = "Real data loading failed" in validation_cell
    if data_loading:
        print("⚠️  Real data loading failed - using fallback")
    else:
        print("✅ Real data loaded successfully")

    # Final assessment
    print(f"\n🎯 FINAL ASSESSMENT:")
    print(f"   📊 Chart Success Rate: {successful_count/15*100:.1f}%")
    print(f"   🏆 Success Level: {success_level}")
    print(f"   📦 Import Status: {'✅' if import_success else '❌'}")
    print(f"   💾 Data Status: {'⚠️ Fallback' if data_loading else '✅ Real'}")

    # Return success if we have at least 13/15 charts working
    return successful_count >= 13


def main():
    """Main validation function."""

    success = execute_and_validate_notebook()

    if success:
        print(f"\n🎉 VALIDATION PASSED!")
        print(f"📊 The notebook meets the success criteria!")
        print(f"🎵 MusicScope™ is ready for production!")
    else:
        print(f"\n🔧 VALIDATION NEEDS IMPROVEMENT")
        print(f"💡 Check the specific chart failures and fix them")

    return success


if __name__ == "__main__":
    main()
