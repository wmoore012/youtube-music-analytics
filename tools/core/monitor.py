#!/usr / bin / env python3
"""
📊 YouTube Analytics Monitoring Tool - LEGACY WRAPPER

This file has been replaced by unified_monitor.py but is kept for backward compatibility.
All functionality has been moved to the new unified monitoring tool.

DEPRECATED: Use unified_monitor.py instead.

Usage:
    python tools / core / unified_monitor.py                 # Quick health check
    python tools / core / unified_monitor.py --data - quality  # Data quality report
    python tools / core / unified_monitor.py --etl - status    # ETL pipeline status
    python tools / core / unified_monitor.py --full - check    # Complete system check
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
        "This monitoring tool is deprecated. Use 'python tools / core / unified_monitor.py' instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    print("⚠️  DEPRECATION WARNING:")
    print("   This monitoring tool has been replaced by the unified monitoring tool.")
    print("   Please use: python tools / core / unified_monitor.py")
    print("   Redirecting to new tool...\n")

    # Map old arguments to new ones
    import sys

    new_args = []

    for arg in sys.argv[1:]:
        if arg == "--consistency":
            # Consistency check is part of quick health check now
            pass  # Will use default quick check
        elif arg == "--quality":
            new_args.append("--data - quality")
        elif arg == "--etl - status":
            new_args.append("--etl - status")
        elif arg == "--full - check":
            new_args.append("--full - check")
        else:
            new_args.append(arg)  # Pass through other args

    # Import and run the new unified monitoring tool
    try:
        # Temporarily replace sys.argv
        original_argv = sys.argv
        sys.argv = ["unified_monitor.py"] + new_args

        from tools.core.unified_monitor import main as unified_main

        return unified_main()
    except ImportError as e:
        print(f"❌ Failed to import unified monitoring tool: {e}")
        print("💡 Make sure you're running from the project root directory")
        return 1
    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    sys.exit(main())
