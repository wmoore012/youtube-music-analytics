#!/usr/bin/env python3
"""
Update All Systems to Use Unique Comments

This script updates all existing systems in the codebase to use the
UniqueCommentManager to ensure no comment overlap between systems.
"""

import os
from pathlib import Path
import re
import sys

sys.path.insert(0, "src")


def find_comment_sampling_code():
    """Find all files that sample comments from the database."""

    patterns_to_find = [
        r"youtube_comments.*SELECT",
        r"SELECT.*youtube_comments",
        r"comment_text.*LIMIT",
        r"RANDOM\(\).*LIMIT",
        r"ORDER BY.*like_count",
    ]

    files_to_check = []

    # Find Python files
    for root, dirs, files in os.walk("."):
        # Skip hidden directories and common non-source directories
        dirs[:] = [d for d in dirs if not d.startswith(".") and d not in ["__pycache__", "node_modules"]]

        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                files_to_check.append(file_path)

    print(f"🔍 Checking {len(files_to_check)} Python files for comment sampling...")

    files_with_sampling = []

    for file_path in files_to_check:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

                # Check if file contains comment sampling patterns
                for pattern in patterns_to_find:
                    if re.search(pattern, content, re.IGNORECASE):
                        files_with_sampling.append(file_path)
                        break
        except Exception as e:
            print(f"⚠️  Could not read {file_path}: {e}")

    return files_with_sampling


def analyze_comment_sampling_files(files):
    """Analyze files that sample comments."""

    print(f"\n📊 Found {len(files)} files with comment sampling:")
    print("=" * 60)

    for file_path in files:
        print(f"\n📁 {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Find lines with comment sampling
            sampling_lines = []
            for i, line in enumerate(lines, 1):
                if any(
                    pattern in line.lower()
                    for pattern in ["youtube_comments", "comment_text", "select.*from.*comment", "limit"]
                ):
                    if any(sql_word in line.lower() for sql_word in ["select", "from", "where", "order", "limit"]):
                        sampling_lines.append((i, line.strip()))

            if sampling_lines:
                print("   SQL queries found:")
                for line_num, line in sampling_lines[:3]:  # Show first 3
                    print(f"     Line {line_num}: {line[:80]}...")
                if len(sampling_lines) > 3:
                    print(f"     ... and {len(sampling_lines) - 3} more")

        except Exception as e:
            print(f"   ❌ Error analyzing: {e}")


def create_migration_guide():
    """Create a guide for migrating to unique comments."""

    guide = """
# MIGRATION GUIDE: Using Unique Comments

## Overview
All systems must use the UniqueCommentManager to ensure no comment overlap.

## Quick Migration Steps

### 1. Import the manager
```python
from youtubeviz.unique_comment_manager import (
    get_unique_comments_for_classification,
    get_unique_comments_for_training,
    get_unique_comments_for_testing,
    get_unique_comments_for_benchmark,
    get_unique_comments_for_evaluation
)
```

### 2. Replace direct database queries

#### OLD (DON'T USE):
```python
query = "SELECT comment_text FROM youtube_comments ORDER BY RANDOM() LIMIT 100"
```

#### NEW (USE THIS):
```python
# For classification
comments = get_unique_comments_for_classification(100)

# For ML training
comments = get_unique_comments_for_training(100)

# For testing
comments = get_unique_comments_for_testing(100)

# For benchmarking
comments_data = get_unique_comments_for_benchmark("system_name", 100)

# For evaluation
comments_data = get_unique_comments_for_evaluation("system_name", 100)
```

### 3. System-specific usage

#### Smart Classifier
```python
def get_sample_comments(self, count: int) -> List[str]:
    return get_unique_comments_for_classification(count)
```

#### Model Benchmark System
```python
def fetch_evaluation_comments(self, count: int) -> List[Dict]:
    return get_unique_comments_for_benchmark("model_benchmark", count)
```

#### VADER Evaluation
```python
def get_test_comments(self, count: int) -> List[Dict]:
    return get_unique_comments_for_evaluation("vader_evaluation", count)
```

## Benefits
- ✅ No data leakage between train/test/validation
- ✅ No duplicate comments across systems
- ✅ Proper ML evaluation methodology
- ✅ Trackable comment usage
- ✅ Reproducible results

## Usage Statistics
Check comment allocation with:
```python
from youtubeviz.unique_comment_manager import comment_manager
stats = comment_manager.get_usage_stats()
print(stats)
```

## Reset System Allocation
If you need to reset a system's comment allocation:
```python
comment_manager.reset_system_allocation("system_name")
```
"""

    with open("UNIQUE_COMMENTS_MIGRATION_GUIDE.md", "w") as f:
        f.write(guide)

    print("📖 Created UNIQUE_COMMENTS_MIGRATION_GUIDE.md")


def main():
    """Main migration analysis."""

    print("🔄 UNIQUE COMMENTS MIGRATION ANALYZER")
    print("=" * 60)
    print("This tool finds all comment sampling code that needs to be updated")
    print("to use the UniqueCommentManager for proper data isolation.")
    print()

    # Find files with comment sampling
    files_with_sampling = find_comment_sampling_code()

    if not files_with_sampling:
        print("✅ No comment sampling code found!")
        return

    # Analyze the files
    analyze_comment_sampling_files(files_with_sampling)

    # Create migration guide
    print(f"\n📋 MIGRATION SUMMARY")
    print("=" * 40)
    print(f"Files to update: {len(files_with_sampling)}")
    print(f"Priority files:")

    priority_files = [
        f
        for f in files_with_sampling
        if any(keyword in f.lower() for keyword in ["benchmark", "classifier", "evaluation", "test", "train"])
    ]

    for file_path in priority_files:
        print(f"  🔥 {file_path}")

    create_migration_guide()

    print(f"\n🎯 NEXT STEPS:")
    print("1. Review the files listed above")
    print("2. Replace direct SQL queries with UniqueCommentManager calls")
    print("3. Use the migration guide for specific patterns")
    print("4. Test each system to ensure unique comment usage")

    print(f"\n✅ Migration analysis complete!")


if __name__ == "__main__":
    main()
