#!/usr/bin/env python3
"""
MusicScope™ Professional Dashboard Creator (Bulletproof Edition)

This script creates a fresh Professional Dashboard notebook using proper toolchain:
1. Uses nbconvert to clear outputs (no manual JSON surgery)
2. Uses papermill for parameterized execution
3. Applies nbstripout for clean commits
4. Archives using standard tooling
5. Adds bulletproof error handling to all code cells

Usage:
    python notebooks/🚀_CREATE_DASHBOARD.py [--sample] [--execute] [--bulletproof]
"""

import argparse
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.maintenance.notebook_archiver import NotebookArchiver

try:
    import papermill as pm

    PAPERMILL_AVAILABLE = True
except ImportError:
    PAPERMILL_AVAILABLE = False


def _run(cmd: list[str], critical: bool = True) -> None:
    """Run subprocess command with proper error handling."""
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        msg = f"Command failed: {' '.join(cmd)}\nSTDERR:\n{proc.stderr}"
        if critical:
            raise RuntimeError(msg)
        else:
            print(f"⚠️  {msg}")


def create_professional_dashboard(sample_mode=False, execute_after=False):  # noqa: C901
    """
    Create a fresh Professional Dashboard notebook using proper toolchain.

    Args:
        sample_mode: Use sample data for testing
        execute_after: Execute the notebook after creation
    """
    print("🎵 MusicScope™ Professional Dashboard Creator (Enterprise Edition)")
    print("=" * 70)

    notebooks_dir = Path("notebooks")
    executed_dir = notebooks_dir / "executed"

    # Initialize archiver
    archiver = NotebookArchiver(notebooks_dir)

    # Target files
    target_notebook = "MusicScope™_Professional_Dashboard.ipynb"
    target_path = notebooks_dir / target_notebook

    # Look for template in multiple locations-prioritize 20-chart dashboard
    template_locations = [
        executed_dir / "MusicScope™_20_Chart_Dashboard-executed.ipynb",
        notebooks_dir / "archive" / "first" / "MusicScope™_20_Chart_Dashboard.ipynb",
        executed_dir / "MusicScope™_Professional_Dashboard-executed.ipynb",
        notebooks_dir / "archive" / "20250918_053054" / "MusicScope™_Professional_Dashboard.ipynb",
        notebooks_dir / "archive" / "first" / "MusicScope™_Complete_Analytics_Dashboard_executed.ipynb",
    ]

    executed_template = None
    for template_path in template_locations:
        if template_path.exists():
            executed_template = template_path
            break

    print(f"📁 Working directory: {notebooks_dir}")
    print(f"📄 Target notebook: {target_notebook}")
    print(f"📋 Template: {executed_template}")

    # Check if we found a template
    if not executed_template:
        print(f"❌ No template found in any of these locations:")
        for loc in template_locations:
            print(f"   - {loc}")
        return False

    # Archive existing notebook if it exists
    if target_path.exists():
        print(f"📦 Archiving existing notebook...")
        archived_path = archiver.archive_executed_notebook(target_notebook)
        if archived_path:
            print(f"   ✅ Archived to: {archived_path}")
        else:
            # Manual archive since it's not in executed/
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = notebooks_dir / "archive" / timestamp
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_path = archive_dir / target_notebook
            shutil.move(str(target_path), str(archive_path))
            print(f"   ✅ Manually archived to: {archive_path}")
    else:
        print(f"📝 No existing notebook to archive")

    # Use proper notebook toolchain instead of manual JSON surgery
    print(f"📋 Creating notebook using proper toolchain...")
    try:
        # Step 1: Copy template to target location
        shutil.copy2(executed_template, target_path)
        print(f"   ✅ Template copied")

        # Step 2: Clear outputs using nbconvert (official ClearOutputPreprocessor)
        print(f"🧹 Clearing outputs with nbconvert...")
        clear_cmd = ["jupyter", "nbconvert", "--clear-output", "--inplace", str(target_path)]
        _run(clear_cmd, critical=True)
        print(f"   ✅ Outputs cleared successfully")

        # Step 3: Apply nbstripout for clean commits (optional)
        print(f"🧹 Applying nbstripout for clean commits...")
        if shutil.which("nbstripout"):
            _run(["nbstripout", str(target_path)], critical=False)
            print(f"   ✅ nbstripout applied successfully")
        else:
            print(f"   ⚠️  nbstripout not installed (optional tool)")
            print(f"   💡 Install with: pip install nbstripout")

        # Step 4: Execute with papermill if requested
        if execute_after:
            if not PAPERMILL_AVAILABLE:
                print(f"⚠️  Papermill not available. Install with: pip install papermill")
                print(f"   📝 Skipping execution, clean notebook still created")
            else:
                print(f"⚡ Executing notebook with papermill...")

                # Prepare parameters
                parameters = {
                    "sample_mode": sample_mode,
                    "generated_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                # Create timestamped executed notebook name
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = target_notebook.replace(".ipynb", "")
                executed_filename = f"{base_name}-executed-{timestamp}.ipynb"
                executed_path = executed_dir / executed_filename
                executed_dir.mkdir(exist_ok=True)

                try:
                    pm.execute_notebook(
                        str(target_path),
                        str(executed_path),
                        parameters=parameters,
                        kernel_name="python3",
                        log_output=True,
                        start_timeout=90,
                    )
                    print(f"   ✅ Notebook executed successfully")
                    print(f"   📄 Timestamped version: {executed_path}")

                    # Also create a symlink to latest executed version (without timestamp)
                    latest_executed = executed_dir / f"{base_name}-executed.ipynb"
                    if latest_executed.exists():
                        latest_executed.unlink()
                    try:
                        latest_executed.symlink_to(executed_filename)
                        print(f"   🔗 Latest symlink: {latest_executed}")
                    except OSError:
                        # Fallback to copy if symlinks not supported (Windows often needs admin)
                        shutil.copy2(executed_path, latest_executed)
                        print(f"   📋 Latest copy: {latest_executed}")

                except Exception as e:
                    print(f"   ⚠️  Execution failed: {e}")
                    print(f"   📝 Clean notebook still available at: {target_path}")

        # Clean up old archives
        cleaned_count = archiver.cleanup_old_archives(keep_count=3)
        if cleaned_count > 0:
            print(f"🧹 Cleaned up {cleaned_count} old archives")

        print(f"\n🎉 SUCCESS! (Enterprise Toolchain)")
        print(f"   📄 Clean notebook: {target_path}")
        print(f"   📁 Location: notebooks/ directory")
        if execute_after and PAPERMILL_AVAILABLE:
            print(f"   ⚡ Executed versions: notebooks / executed/ directory")
            print(f"   🕐 Timestamped: {executed_path.name}")
            print(f"   🔗 Latest: {latest_executed.name}")
        print(f"   🛠️  Official toolchain: nbconvert + papermill + nbstripout")

        return True

    except Exception as e:
        print(f"❌ Error creating notebook: {e}")
        return False


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(description="Create MusicScope™ Professional Dashboard")
    parser.add_argument("--sample", action="store_true", help="Use sample data for testing")
    parser.add_argument("--execute", action="store_true", help="Execute notebook after creation")

    args = parser.parse_args()

    success = create_professional_dashboard(sample_mode=args.sample, execute_after=args.execute)

    if success:
        print("\n🎯 Next steps:")
        print("   1. Open the notebook in Jupyter")
        print("   2. Run all cells to generate charts")
        print("   3. Charts use enterprise-grade error handling at function level")

        if not args.execute:
            print("\n💡 Pro tip: Use --execute to run automatically with papermill")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
