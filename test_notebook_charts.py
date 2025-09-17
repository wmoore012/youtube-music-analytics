#!/usr/bin/env python3
"""
Test script to verify Plotly charts work in notebook environment
"""

import pandas as pd
import plotly.express as px


def test_chart_display():
    """Test if charts display properly"""

    # Create test data
    df = pd.DataFrame({"artist": ["Artist A", "Artist B", "Artist C"], "views": [1000, 1500, 800]})

    # Create chart
    fig = px.bar(df, x="artist", y="views", title="Test Chart")

    # This should display the chart in Jupyter
    fig.show()

    print("✅ Chart created and displayed!")
    print("If you're running this in Jupyter notebook, you should see an interactive chart above.")
    print("If you're running this as a script, the chart will open in your browser.")


if __name__ == "__main__":
    test_chart_display()
