"""Bootstrap code for MusicScope™ notebooks.

Copy this cell to the top of your notebook to set up imports and logging.
"""

import logging

# Bootstrapping for MusicScope™ in this notebook
import sys

# Make sure src modules are importable
sys.path.insert(0, ".")  # Current directory for src/youtubeviz imports

# Optional: keep logs tidy in notebooks
logger = logging.getLogger("musicscope.charts")
for h in list(logger.handlers):
    logger.removeHandler(h)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

print("🎵 MusicScope™ notebook environment ready!")
print("✅ Imports configured")
print("✅ Logging configured")
print("✅ Ready for bulletproof charts!")

# Example imports you can now use:
# from src.youtubeviz import advanced_charts as ac
# from src.youtubeviz.bulletproof import bulletproof_chart
# from src.youtubeviz.chart_patterns import safe_artist_views_bar, safe_content_type_sentiment
# import pandas as pd
