"""
Momentum Bar Race Chart - Animated visualization of artist momentum rankings over time.

This module creates an animated bar chart showing how artist momentum rankings
change from 2017 to present, highlighting breakout periods (momentum ≥75).

COLOR LOGIC:
- Grey (#CCCCCC): Normal periods (momentum <75)
- Red (#FF6B6B): Breakout periods (momentum ≥75)
- Does NOT use global artist color palette
"""

import pandas as pd
import plotly.graph_objects as go
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Color constants for this chart only
BREAKOUT_COLOR = "#FF6B6B"  # Bright red for breakout periods
NORMAL_COLOR = "#CCCCCC"    # Grey for normal periods


def create_momentum_bar_race(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    breakout_threshold: float = 75.0,
    animation_duration: int = 800,  # milliseconds per frame
) -> go.Figure:
    """
    Create an animated bar race chart showing momentum rankings over time.
    
    Highlights historical breakout periods that were missed opportunities for
    increased marketing spend.
    
    Args:
        df: DataFrame with video metrics (must have published_at or metrics_date)
        artist_col: Column name for artist names
        breakout_threshold: Momentum score threshold for breakout status (default: 75)
        animation_duration: Duration of each frame in milliseconds (default: 800)
    
    Returns:
        Plotly figure with animated bar race chart
    """
    from youtubeviz.advanced_charts import calculate_momentum_index
    
    if df.empty:
        return go.Figure().add_annotation(
            text="No data available for momentum bar race",
            x=0.5, y=0.5,
            showarrow=False
        )
    
    # Calculate momentum index
    momentum_df = calculate_momentum_index(df, artist_col=artist_col)
    
    if momentum_df.empty:
        return go.Figure().add_annotation(
            text="Insufficient data to calculate momentum",
            x=0.5, y=0.5,
            showarrow=False
        )
    
    # Group by week and artist, get average momentum per week
    weekly_momentum = (
        momentum_df
        .groupby(['week_start', artist_col])['momentum_index']
        .mean()
        .reset_index()
    )

    # Ensure every frame includes all artists (fill missing with 0)
    artists = sorted(weekly_momentum[artist_col].unique())
    weeks = sorted(weekly_momentum['week_start'].unique())

    if artists and weeks:
        full_index = (
            pd.MultiIndex.from_product([weeks, artists], names=['week_start', artist_col])
            .to_frame(index=False)
        )
        weekly_momentum = (
            full_index
            .merge(weekly_momentum, on=['week_start', artist_col], how='left')
            .sort_values(['week_start', 'momentum_index'], ascending=[True, True])
        )
        weekly_momentum['momentum_index'] = weekly_momentum['momentum_index'].fillna(0.0)
    
    # Create frames for animation (one per week)
    frames = []
    weeks = sorted(weekly_momentum['week_start'].unique())

    # Track breakout periods for title
    breakout_periods = []

    for week in weeks:
        week_data = weekly_momentum[weekly_momentum['week_start'] == week].copy()
        week_data = week_data.sort_values('momentum_index', ascending=True)  # Bottom to top

        # Determine bar colors (grey for normal, red for breakout)
        bar_colors = []
        for _, row in week_data.iterrows():
            artist = row[artist_col]
            momentum = row['momentum_index']

            if momentum >= breakout_threshold:
                # Breakout: use bright red
                bar_colors.append(BREAKOUT_COLOR)

                # Track breakout period
                period_key = (artist, week.strftime('%b %Y'))
                if period_key not in breakout_periods:
                    breakout_periods.append((artist, week, momentum))
            else:
                # Normal: use grey
                bar_colors.append(NORMAL_COLOR)
        
        # Create frame
        frame = go.Frame(
            data=[go.Bar(
                y=week_data[artist_col],
                x=week_data['momentum_index'],
                orientation='h',
                marker=dict(color=bar_colors),
                text=week_data['momentum_index'].apply(lambda x: f'{x:.0f}'),
                textposition='outside',
                hovertemplate='%{y}<br>Momentum: %{x:.1f}/100<extra></extra>'
            )],
            name=week.strftime('%Y-%m-%d'),
            layout=go.Layout(
                title_text=f"Artist Momentum Rankings - {week.strftime('%b %d, %Y')}"
            )
        )
        frames.append(frame)
    
    # Create initial frame (first week)
    first_week_data = weekly_momentum[weekly_momentum['week_start'] == weeks[0]].copy()
    first_week_data = first_week_data.sort_values('momentum_index', ascending=True)

    initial_colors = []
    for _, row in first_week_data.iterrows():
        momentum = row['momentum_index']
        if momentum >= breakout_threshold:
            initial_colors.append(BREAKOUT_COLOR)
        else:
            initial_colors.append(NORMAL_COLOR)
    
    fig = go.Figure(
        data=[go.Bar(
            y=first_week_data[artist_col],
            x=first_week_data['momentum_index'],
            orientation='h',
            marker=dict(color=initial_colors),
            text=first_week_data['momentum_index'].apply(lambda x: f'{x:.0f}'),
            textposition='outside',
            hovertemplate='%{y}<br>Momentum: %{x:.1f}/100<extra></extra>'
        )],
        frames=frames
    )
    
    # Find top 3 breakout periods for title
    breakout_periods_sorted = sorted(breakout_periods, key=lambda x: x[2], reverse=True)[:3]
    
    if breakout_periods_sorted:
        breakout_summary = ", ".join([
            f"{artist} ({date.strftime('%b %Y')})"
            for artist, date, _ in breakout_periods_sorted
        ])
        title_text = (
            f"We Missed {len(set(bp[0] for bp in breakout_periods))} Breakout Windows: {breakout_summary}<br>"
            f"<sub>Historical momentum trajectory 2017-2025 · Red bars = breakout periods (≥{breakout_threshold}) · "
            f"Missed opportunities for increased marketing spend</sub>"
        )
    else:
        title_text = (
            f"Artist Momentum Rankings Over Time (2017-2025)<br>"
            f"<sub>No breakout periods (≥{breakout_threshold}) detected in historical data</sub>"
        )
    
    # Add animation controls
    fig.update_layout(
        title=title_text,
        xaxis=dict(
            title="Momentum Index (0-100)",
            range=[0, 105]
        ),
        yaxis=dict(
            title="Artist",
            categoryorder='total ascending'
        ),
        height=600,
        template="plotly_white",
        updatemenus=[{
            'type': 'buttons',
            'showactive': False,
            'buttons': [
                {
                    'label': '▶ Play',
                    'method': 'animate',
                    'args': [None, {
                        'frame': {'duration': animation_duration, 'redraw': True},
                        'fromcurrent': True,
                        'mode': 'immediate',
                        'transition': {'duration': animation_duration // 2}
                    }]
                },
                {
                    'label': '⏸ Pause',
                    'method': 'animate',
                    'args': [[None], {
                        'frame': {'duration': 0, 'redraw': False},
                        'mode': 'immediate',
                        'transition': {'duration': 0}
                    }]
                }
            ],
            'x': 0.1,
            'y': 1.15,
            'xanchor': 'left',
            'yanchor': 'top'
        }],
        sliders=[{
            'active': 0,
            'steps': [
                {
                    'args': [[frame.name], {
                        'frame': {'duration': animation_duration, 'redraw': True},
                        'mode': 'immediate',
                        'transition': {'duration': animation_duration // 2}
                    }],
                    'label': pd.to_datetime(frame.name).strftime('%b %Y'),
                    'method': 'animate'
                }
                for frame in frames
            ],
            'x': 0.1,
            'y': 0,
            'len': 0.9,
            'xanchor': 'left',
            'yanchor': 'top'
        }]
    )
    
    # Add breakout threshold line
    fig.add_vline(
        x=breakout_threshold,
        line_dash="dash",
        line_color="red",
        line_width=2
    )
    
    logger.info(f"Created momentum bar race with {len(frames)} frames covering {len(weeks)} weeks")
    
    return fig

