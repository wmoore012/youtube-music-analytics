import re

from src.youtubeviz.storytelling import quick_takeaways, story_block


class DummyFig:
    def to_html(self, include_plotlyjs="cdn", full_html=False):  # pragma: no cover - trivial
        return "<div id='dummy-fig'>FIG</div>"


def test_quick_takeaways_various():
    bullets = quick_takeaways(
        artist="X",
        last_7d_change_pct=12.34,
        engagement_rate=4.2,
        standout_video="Hit Song",
    )
    text = " ".join(bullets)
    assert "12.3%" in text or "+12.3%" in text
    assert "4.2%" in text
    assert "Hit Song" in text


def test_story_block_returns_html_for_testing():
    fig = DummyFig()
    html = story_block(
        fig,
        title="🚀 Momentum rising",
        bullets=["Point A", "Point B"],
        caption="Recommendation: boost budget",
        return_html=True,
    )
    assert isinstance(html, str)
    assert "Momentum rising" in html
    # Ensure bullets rendered as <li>
    assert len(re.findall(r"<li>.*?</li>", html)) >= 2
    # Figure HTML embedded
    assert "dummy-fig" in html
