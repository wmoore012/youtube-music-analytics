#!/usr / bin / env python3
"""
Legacy Setup Tool Wrapper

This is a backward compatibility wrapper for the old setup.py tool.
It redirects to the new unified setup tool with appropriate warnings.

DEPRECATED: Use tools / core / unified_setup.py instead.
"""

from pathlib import Path
import sys
import warnings

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def main():
    """Main wrapper function with deprecation warning."""
    warnings.warn(
        "This setup tool is deprecated. Use 'python tools / core / unified_setup.py' instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    print("⚠️  DEPRECATION WARNING:")
    print("   This setup tool has been replaced by the unified setup tool.")
    print("   Please use: python tools / core / unified_setup.py")
    print("   Redirecting to new tool...\n")

    # Import and run the new unified setup tool
    try:
        from tools.core.unified_setup import main as unified_main

        return unified_main()
    except ImportError as e:
        print(f"❌ Failed to import unified setup tool: {e}")
        print("💡 Make sure you're running from the project root directory")
        return 1


if __name__ == "__main__":
    sys.exit(main())
