#!/bin/bash
# 🚀 MusicScope™ Dashboard - PLAY BUTTON (Shell Script)
#
# Usage: ./run_dashboard.sh
# Or: bash run_dashboard.sh

echo "🚀 Starting MusicScope™ Dashboard Creation..."
echo ""

# Make sure we're in the right directory
cd "$(dirname "$0")"

# Run the Python play button
python create_dashboard.py

echo ""
echo "🎯 Dashboard creation complete!"
