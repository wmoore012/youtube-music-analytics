#!/usr/bin/env python3
"""
Unique Comment Integration System

Ensures all comment sampling across the codebase uses UNIQUE comments.
Integrates with existing data loading functions to prevent duplicate usage.
"""

import functools
from typing import Any, Callable, Dict, List

import pandas as pd

from .unique_comment_manager import UniqueCommentManager, comment_manager


class UniqueCommentEnforcer:
    """
    Enforces unique comment usage across all data loading functions.

    Wraps existing functions to ensure no comment is used multiple times
    across different analysis contexts.
    """

    def __init__(self, manager: UniqueCommentManager = None):
        self.manager = manager or comment_manager
        self._function_registry: Dict[str, str] = {}

    def register_function(self, func_name: str, usage_type: str = "analysis") -> Callable:
        """
        Decorator to register a function for unique comment enforcement.

        Args:
            func_name: Name of the function for tracking
            usage_type: Type of usage (analysis, training, testing, etc.)
        """

        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Execute the original function
                result = func(*args, **kwargs)

                # If result is a DataFrame with comments, track them
                if isinstance(result, pd.DataFrame) and not result.empty:
                    self._track_comment_usage(result, func_name, usage_type)

                return result

            # Register the function
            self._function_registry[func_name] = usage_type
            return wrapper

        return decorator

    def _track_comment_usage(self, df: pd.DataFrame, func_name: str, usage_type: str) -> None:
        """Track comment usage from a DataFrame result."""
        comment_columns = ["comment_text", "comment", "text"]

        for col in comment_columns:
            if col in df.columns:
                comments = df[col].dropna().astype(str).tolist()

                for comment in comments:
                    if comment and len(comment.strip()) > 0:
                        # Try to allocate the comment
                        allocated = self.manager.allocate_comment(
                            comment_text=comment,
                            usage_type=usage_type,
                            system_name=func_name,
                            notes=f"Used by {func_name} function",
                        )

                        if not allocated:
                            # Comment was already used-log warning
                            usage_info = self.manager.get_comment_usage(comment)
                            if usage_info:
                                print(f"⚠️  Comment reuse detected in {func_name}:")
                                print(f"   Previously used by: {usage_info['system_name']}")
                                print(f"   Usage type: {usage_info['usage_type']}")
                break

    def get_unique_comments_for_function(self, func_name: str, count: int, usage_type: str = "analysis") -> List[str]:
        """
        Get unique comments specifically for a function.

        Args:
            func_name: Name of the function requesting comments
            count: Number of unique comments needed
            usage_type: Type of usage for tracking

        Returns:
            List of unique comment texts
        """
        comments_data = self.manager.get_unique_comments_for_system(
            system_name=func_name, usage_type=usage_type, count=count
        )

        return [c["comment_text"] for c in comments_data]

    def validate_dataframe_uniqueness(self, df: pd.DataFrame, context: str = "unknown") -> pd.DataFrame:
        """
        Validate that a DataFrame contains only unique comments.

        Args:
            df: DataFrame to validate
            context: Context for logging

        Returns:
            DataFrame with only unique comments
        """
        if df.empty:
            return df

        comment_columns = ["comment_text", "comment", "text"]
        comment_col = None

        for col in comment_columns:
            if col in df.columns:
                comment_col = col
                break

        if not comment_col:
            return df  # No comment column found

        # Check for duplicates within the DataFrame
        original_count = len(df)
        df_unique = df.drop_duplicates(subset=[comment_col])

        if len(df_unique) < original_count:
            removed = original_count - len(df_unique)
            print(f"🔄 Removed {removed} duplicate comments in {context}")

        # Check against global usage tracking
        unique_comments = []
        for _, row in df_unique.iterrows():
            _comment_text_item = str(row[comment_col]).strip()

            if _comment_text_item and not self.manager.is_comment_used(_comment_text_item):
                unique_comments.append(row)
            else:
                usage_info = self.manager.get_comment_usage(_comment_text_item)
                if usage_info:
                    print(f"⚠️  Skipping previously used comment in {context}")
                    print(f"   Used by: {usage_info['system_name']} ({usage_info['usage_type']})")

        if unique_comments:
            result_df = pd.DataFrame(unique_comments)
            print(f"✅ Validated {len(result_df)} unique comments for {context}")
            return result_df
        else:
            print(f"⚠️  No unique comments available for {context}")
            return pd.DataFrame(columns=df.columns)


# Global enforcer instance
unique_enforcer = UniqueCommentEnforcer()


def ensure_unique_comments(func_name: str, usage_type: str = "analysis"):
    """
    Decorator to ensure a function uses only unique comments.

    Usage:
        @ensure_unique_comments("load_comment_examples", "analysis")
        def load_comment_examples(...):
            ...
    """
    return unique_enforcer.register_function(func_name, usage_type)


def get_unique_comments_for_analysis(count: int, context: str = "general_analysis") -> List[str]:
    """
    Get unique comments for general analysis purposes.

    Args:
        count: Number of unique comments needed
        context: Context description for tracking

    Returns:
        List of unique comment texts
    """
    return unique_enforcer.get_unique_comments_for_function(context, count, "analysis")


def validate_comment_dataframe(df: pd.DataFrame, context: str = "unknown") -> pd.DataFrame:
    """
    Validate that a DataFrame contains only unique, unused comments.

    Args:
        df: DataFrame to validate
        context: Context for logging

    Returns:
        DataFrame with only unique comments
    """
    return unique_enforcer.validate_dataframe_uniqueness(df, context)


def scan_for_fake_data(df: pd.DataFrame, context: str = "unknown") -> pd.DataFrame:
    """
    Scan DataFrame for fake or synthetic data and remove it.

    Args:
        df: DataFrame to scan
        context: Context for logging

    Returns:
        DataFrame with fake data removed
    """
    if df.empty:
        return df

    # Common fake data patterns
    fake_patterns = [
        "test comment",
        "sample comment",
        "fake comment",
        "dummy comment",
        "lorem ipsum",
        "placeholder",
        "example comment",
        "mock comment",
        "synthetic comment",
        "generated comment",
    ]

    comment_columns = ["comment_text", "comment", "text", "title", "description"]

    original_count = len(df)
    cleaned_df = df.copy()

    for col in comment_columns:
        if col in cleaned_df.columns:
            # Remove rows with fake data patterns
            for pattern in fake_patterns:
                mask = cleaned_df[col].astype(str).str.lower().str.contains(pattern, na=False)
                fake_count = mask.sum()

                if fake_count > 0:
                    print(f"🗑️  Removing {fake_count} fake '{pattern}' entries from {col} in {context}")
                    cleaned_df = cleaned_df[~mask]

    # Remove rows with obviously fake data (too short, repetitive, etc.)
    for col in comment_columns:
        if col in cleaned_df.columns:
            # Remove very short comments (likely fake)
            short_mask = cleaned_df[col].astype(str).str.len() < 3
            short_count = short_mask.sum()

            if short_count > 0:
                print(f"🗑️  Removing {short_count} suspiciously short entries from {col} in {context}")
                cleaned_df = cleaned_df[~short_mask]

            # Remove repetitive comments (likely fake)
            repetitive_mask = cleaned_df[col].astype(str).str.match(r"^(.)\1{10,}$", na=False)
            rep_count = repetitive_mask.sum()

            if rep_count > 0:
                print(f"🗑️  Removing {rep_count} repetitive entries from {col} in {context}")
                cleaned_df = cleaned_df[~repetitive_mask]

    removed_count = original_count - len(cleaned_df)
    if removed_count > 0:
        print(f"✅ Removed {removed_count} fake / suspicious entries from {context}")
    else:
        print(f"✅ No fake data detected in {context}")

    return cleaned_df


def enforce_real_data_only(df: pd.DataFrame, context: str = "unknown") -> pd.DataFrame:
    """
    Comprehensive function to ensure DataFrame contains only real, unique data.

    Args:
        df: DataFrame to clean
        context: Context for logging

    Returns:
        DataFrame with only real, unique data
    """
    print(f"🔍 Enforcing real data policy for {context}...")

    # Step 1: Remove fake data
    cleaned_df = scan_for_fake_data(df, context)

    # Step 2: Ensure uniqueness
    unique_df = validate_comment_dataframe(cleaned_df, context)

    # Step 3: Final validation
    if unique_df.empty:
        print(f"⚠️  No valid real data found for {context}")
    else:
        print(f"✅ Validated {len(unique_df)} real, unique records for {context}")

    return unique_df


def get_usage_statistics() -> Dict[str, Any]:
    """Get comprehensive usage statistics for unique comments."""
    stats = comment_manager.get_usage_stats()

    # Add function registry information
    stats["registered_functions"] = unique_enforcer._function_registry
    stats["total_functions"] = len(unique_enforcer._function_registry)

    return stats


def reset_function_usage(func_name: str) -> int:
    """Reset comment usage for a specific function."""
    return comment_manager.reset_system_allocation(func_name)


if __name__ == "__main__":
    # Demo the unique comment integration system
    print("🔍 UNIQUE COMMENT INTEGRATION DEMO")
    print("=" * 50)

    # Show current usage statistics
    stats = get_usage_statistics()
    print(f"📊 Current Usage Statistics:")
    print(f"   Total allocated: {stats['total_allocated']}")
    print(f"   Registered functions: {stats['total_functions']}")
    print(f"   By usage type: {stats['by_usage_type']}")

    # Demo fake data detection
    print(f"\n🧪 Testing fake data detection...")

    fake_data = pd.DataFrame(
        {
            "comment_text": [
                "This is a real comment about music",
                "test comment",
                "fake comment for testing",
                "I love this song so much!",
                "sample comment",
                "aaaaaaaaaaaaaaaa",  # repetitive
                "xx",  # too short
                "Another genuine music comment",
            ],
            "video_id": ["vid1", "vid2", "vid3", "vid4", "vid5", "vid6", "vid7", "vid8"],
        }
    )

    print(f"Original data: {len(fake_data)} records")
    cleaned_data = enforce_real_data_only(fake_data, "demo")
    print(f"Cleaned data: {len(cleaned_data)} records")

    print(f"\n✅ Demo completed successfully!")
