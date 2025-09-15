from __future__ import annotations

from typing import Iterable, Optional, Sequence


def _join_bullets(lines: Sequence[str]) -> str:
    items = [f"<li>{l}</li>" for l in lines if str(l).strip()]
    return "\n".join(items)


def story_block(
    fig,
    title: str,
    bullets: Iterable[str],
    caption: Optional[str] = None,
    width_left: str = "58%",
    theme: str = "light",
):
    """Display a chart with a human narrative beside it in notebooks.

    - Places an interactive Plotly/Altair figure on the left
    - Places concise, humanized bullets on the right
    - Falls back to returning a tuple if IPython is not available

    Args:
        fig: Plotly/Altair figure (or any object with ``to_html`` or rich repr)
        title: Short, engaging headline
        bullets: Key points that explain the chart and the story
        caption: Optional one-line caption (tone, takeaway)
        width_left: CSS width for chart column
        theme: "light" or "dark" (affects text colors)
    """
    try:
        from IPython.display import HTML, display
    except Exception:
        # Outside IPython: just return the content so callers can handle
        return {"figure": fig, "title": title, "bullets": list(bullets), "caption": caption}

    # Render figure HTML if supported, else rely on notebook renderer
    fig_html: str
    try:
        # Plotly
        fig_html = fig.to_html(include_plotlyjs="cdn", full_html=False)  # type: ignore[attr-defined]
    except Exception:
        try:
            # Altair
            fig_html = fig.to_html()  # type: ignore[attr-defined]
        except Exception:
            # Fallback: rely on IPython repr
            fig_html = f"<div class='figure-fallback'>{fig}</div>"

    text_color = "#111" if theme == "light" else "#fafafa"
    sub_color = "#444" if theme == "light" else "#ddd"

    html = f"""
    <div style="display:flex; gap:18px; align-items:flex-start; width:100%;">
      <div style="flex:0 0 {width_left}; max-width:{width_left};">{fig_html}</div>
      <div style="flex:1; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; color:{text_color};">
        <h3 style="margin:0 0 6px 0; font-weight:700;">{title}</h3>
        <ul style="margin:6px 0 8px 18px; padding:0; line-height:1.4;">{_join_bullets(list(bullets))}</ul>
        {f"<div style='font-size:12px; color:{sub_color}; margin-top:4px;'>{caption}</div>" if caption else ''}
      </div>
    </div>
    """
    display(HTML(html))
    return None


def quick_takeaways(
    artist: str,
    last_7d_change_pct: Optional[float] = None,
    engagement_rate: Optional[float] = None,
    standout_video: Optional[str] = None,
) -> list[str]:
    """Generate friendly, executive-style bullets for a single artist.

    This is intentionally simple and safe; callers can pass real metrics.
    """
    out: list[str] = []
    if last_7d_change_pct is not None:
        arrow = "⤴️" if last_7d_change_pct >= 0 else "⤵️"
        out.append(f"Momentum {arrow} last 7 days: {last_7d_change_pct:+.1f}%")
    if engagement_rate is not None:
        out.append(f"Fan engagement: {engagement_rate:.1f}% — community leaning in 🎧")
    if standout_video:
        out.append(f"Breakout track: “{standout_video}” — consider boosting promo 💸")
    if not out:
        out.append("Steady performance — watch for emerging spikes and collabs 🤝")
    return out
