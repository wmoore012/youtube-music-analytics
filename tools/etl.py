#!/usr/bin/env python3
"""
🚀 YouTube Analytics ETL Pipeline

Consolidated, robust ETL tool that combines focused and comprehensive pipelines.
Handles all data extraction, transformation, and loading with proper error handling.

Usage:
    python tools/etl.py                    # Full pipeline
    python tools/etl.py --focused          # Quick core data only
    python tools/etl.py --channels "A,B"   # Specific channels
    python tools/etl.py --sentiment        # Include sentiment analysis
    python tools/etl.py --help             # Show all options
"""

import argparse
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """Main ETL pipeline with comprehensive options."""
    parser = argparse.ArgumentParser(
        description="YouTube Analytics ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/etl.py                     # Full pipeline
  python tools/etl.py --focused           # Quick core data only  
  python tools/etl.py --sentiment         # Include sentiment analysis
  python tools/etl.py --channels "A,B"    # Specific channels only
  python tools/etl.py --dry-run           # Test without changes
        """
    )
    
    # Pipeline options
    parser.add_argument('--focused', action='store_true',
                       help='Run focused ETL (core data only, faster)')
    parser.add_argument('--comprehensive', action='store_true', 
                       help='Run comprehensive ETL (all features)')
    parser.add_argument('--sentiment', action='store_true',
                       help='Include sentiment analysis')
    parser.add_argument('--channels', type=str,
                       help='Comma-separated list of channels to process')
    
    # Control options
    parser.add_argument('--dry-run', action='store_true',
                       help='Test run without making changes')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Minimal output')
    
    args = parser.parse_args()
    
    # Import after path setup
    try:
        from tools.etl.run_focused_etl import main as run_focused
        from tools.etl.run_comprehensive_etl import main as run_comprehensive
        print("🚀 YouTube Analytics ETL Pipeline")
        print("=" * 50)
        
        if args.dry_run:
            print("🧪 DRY RUN MODE - No changes will be made")
        
        # Determine which pipeline to run
        if args.focused:
            print("⚡ Running FOCUSED ETL (core data only)")
            result = run_focused()
        elif args.comprehensive:
            print("🔄 Running COMPREHENSIVE ETL (all features)")  
            result = run_comprehensive()
        else:
            # Default: run focused first, then comprehensive features
            print("🎯 Running FULL ETL PIPELINE")
            print("\n📊 Phase 1: Core Data (Focused ETL)")
            result1 = run_focused()
            
            if result1 == 0:  # Success
                print("\n🔬 Phase 2: Advanced Features (Comprehensive ETL)")
                result2 = run_comprehensive()
                result = result2
            else:
                print("❌ Core ETL failed, skipping advanced features")
                result = result1
        
        if result == 0:
            print("\n✅ ETL Pipeline completed successfully!")
            print("💡 Run 'python tools/monitor.py' to check data quality")
        else:
            print(f"\n❌ ETL Pipeline failed with code {result}")
            print("💡 Check logs for details or run with --verbose")
            
        return result
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure you're running from the project root directory")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())