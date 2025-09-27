#!/usr / bin / env python3
"""
📁 Archive Historical Documentation

Moves historical documentation files from root directory to archive.
This completes the codebase cleanup by organizing development artifacts.
"""

from datetime import datetime
import os
from pathlib import Path
import shutil


def main():
    """Archive historical documentation files."""
    print("📁 Archiving Historical Documentation")
    print("=" * 50)

    # Define files to archive
    root_docs_to_archive = [
        "FINAL_WORKING_SYSTEM_SUMMARY.md",
        "WORKING_SYSTEM_DEMONSTRATION.md",
        "BENCHMARK_RESULTS_GUIDE.md",
        "BENCHMARK_SYSTEM_IMPLEMENTATION.md",
        "ANALYTICS_QUERIES_SCHEMA_ALIGNMENT_REPORT.md",
        "TASK_1_ML_DATA_COLLECTION_IMPLEMENTATION.md",
        "TASK_2_TRANSFORMER_PREPROCESSING_IMPLEMENTATION.md",
    ]

    misc_files_to_archive = ["yt_proj.sql", "diverging_sentiment_bars.html", "bulletproof_etl.log", "youtube_etl.log"]

    # Create archive directories
    archive_base = Path("archive / cleanup_2025_09_25")
    docs_archive = archive_base / "documentation"
    misc_archive = archive_base / "misc"

    docs_archive.mkdir(parents=True, exist_ok=True)
    misc_archive.mkdir(parents=True, exist_ok=True)

    # Archive root documentation files
    archived_docs = []
    for doc_file in root_docs_to_archive:
        if Path(doc_file).exists():
            print(f"📄 Archiving: {doc_file}")
            shutil.move(doc_file, docs_archive / doc_file)
            archived_docs.append(doc_file)
        else:
            print(f"⚠️  Not found: {doc_file}")

    # Archive misc files
    archived_misc = []
    for misc_file in misc_files_to_archive:
        if Path(misc_file).exists():
            print(f"📦 Archiving: {misc_file}")
            shutil.move(misc_file, misc_archive / misc_file)
            archived_misc.append(misc_file)
        else:
            print(f"⚠️  Not found: {misc_file}")

    # Check notebooks for archival candidates
    print("\n📓 Analyzing Notebooks...")
    notebooks_dir = Path("notebooks")

    # Current active notebooks (keep these)
    active_notebooks = [
        "MusicScope™_20_Chart_Dashboard.ipynb",  # Main dashboard
        "MusicScope™_Professional_Dashboard.ipynb",  # Professional version
        "🔧_CHECK_DEPENDENCIES.py",  # Utility
        "🚀_RUN_NOTEBOOK_CREATION.py",  # Utility
        "README.md",  # Documentation
        "run_dashboard.sh",  # Utility script
    ]

    # Potential archive candidates (executed versions, demos, etc.)
    archive_candidates = []
    if notebooks_dir.exists():
        for notebook in notebooks_dir.iterdir():
            if notebook.is_file() and notebook.name not in active_notebooks:
                if any(
                    keyword in notebook.name.lower()
                    for keyword in ["executed", "demo", "simple", "validated", "real_data"]
                ):
                    archive_candidates.append(notebook.name)

    print(f"\n✅ Active Notebooks (keeping {len(active_notebooks)}):")
    for nb in active_notebooks:
        if (notebooks_dir / nb).exists():
            print(f"   📊 {nb}")

    print(f"\n🤔 Archive Candidates ({len(archive_candidates)}):")
    for nb in archive_candidates:
        print(f"   📋 {nb}")

    # Create summary
    print(f"\n📋 Archive Summary")
    print(f"   📄 Documentation files archived: {len(archived_docs)}")
    print(f"   📦 Misc files archived: {len(archived_misc)}")
    print(f"   📓 Notebook candidates identified: {len(archive_candidates)}")

    if archived_docs or archived_misc:
        print(f"\n✅ Historical documentation successfully archived!")
        print(f"   Location: {archive_base}")
    else:
        print(f"\n💡 No files needed archiving - root directory already clean!")

    # Show current root directory status
    print(f"\n📊 Current Root Directory Status:")
    root_files = [f for f in Path(".").iterdir() if f.is_file() and not f.name.startswith(".")]
    essential_files = [
        "README.md",
        "LICENSE",
        "pyproject.toml",
        "requirements.txt",
        "Makefile",
        "setup.cfg",
        "pytest.ini",
    ]

    essential_count = sum(1 for f in root_files if f.name in essential_files)
    other_count = len(root_files) - essential_count

    print(f"   ✅ Essential files: {essential_count}")
    print(f"   📄 Other files: {other_count}")
    print(
        f"   🎯 Clutter level: {'Excellent' if other_count <= 3 else 'Good' if other_count <= 8 else 'Needs cleanup'}"
    )


if __name__ == "__main__":
    main()
