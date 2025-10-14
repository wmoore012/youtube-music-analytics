#!/usr/bin/env python3
"""
Quick ETL Update Script
Fetches updated statistics for all artist channels to refresh momentum data.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from web.etl_entrypoints import run_channel_etl

# Artist channel URLs from .env
CHANNELS = [
    "https://youtube.com/@BicFizzle",
    "https://www.youtube.com/@COBRAH",
    "https://youtube.com/channel/UCrlh7pS6M6ywPw76fbAkgrQ",  # Corook
    "https://youtube.com/channel/UCPvxMHz48A5qFB68Cp42uqw",  # Raiche
    "https://www.youtube.com/@re6ce",
    "https://www.youtube.com/@FlyanaBoss",
]

def main():
    print("=" * 80)
    print("🚀 RUNNING ETL UPDATE FOR ALL ARTISTS")
    print("=" * 80)
    print()
    print("This will fetch updated view counts, likes, and comments for all videos")
    print("to create new weekly snapshots for momentum tracking.")
    print()
    
    total_videos = 0
    total_errors = 0
    
    for i, channel_url in enumerate(CHANNELS, 1):
        print(f"\n{'=' * 80}")
        print(f"📺 [{i}/{len(CHANNELS)}] Processing: {channel_url}")
        print(f"{'=' * 80}")
        
        try:
            # Run ETL for this channel (no limit = fetch all videos)
            summary = run_channel_etl(channel_url, limit=None)
            
            print(f"\n✅ Channel ETL Complete:")
            print(f"   Channel ID: {summary.channel_id}")
            print(f"   Videos seen: {summary.videos_seen}")
            print(f"   Raw upserts: {summary.raw_upserts}")
            print(f"   Metrics upserts: {summary.metrics_upserts}")
            
            if summary.errors:
                print(f"   ⚠️  Errors: {len(summary.errors)}")
                for error in summary.errors[:3]:  # Show first 3 errors
                    print(f"      - {error}")
                total_errors += len(summary.errors)
            
            total_videos += summary.videos_seen
            
        except Exception as e:
            print(f"\n❌ Error processing channel: {e}")
            total_errors += 1
            continue
    
    print(f"\n{'=' * 80}")
    print("📊 ETL UPDATE SUMMARY")
    print(f"{'=' * 80}")
    print(f"   Total channels processed: {len(CHANNELS)}")
    print(f"   Total videos updated: {total_videos}")
    print(f"   Total errors: {total_errors}")
    print()
    
    if total_errors == 0:
        print("✅ ETL update completed successfully!")
        print("   → Momentum charts should now show updated data")
        print("   → Re-run the dashboard notebook to see the latest metrics")
    else:
        print(f"⚠️  ETL completed with {total_errors} errors")
        print("   → Check the output above for details")
    
    print()
    print("=" * 80)

if __name__ == "__main__":
    main()

