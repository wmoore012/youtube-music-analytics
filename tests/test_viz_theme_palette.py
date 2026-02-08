from __future__ import annotations

from youtubeviz.viz_theme import build_color_discrete_map, get_artist_color


def test_get_artist_color_uses_palette_when_present() -> None:
    palette = {"BiC Fizzle": "#A3262A"}
    assert get_artist_color("BiC Fizzle", palette=palette) == "#A3262A"


def test_get_artist_color_fallback_is_deterministic_and_not_gray() -> None:
    color_first = get_artist_color("New Artist Name", palette={})
    color_second = get_artist_color("New Artist Name", palette={})
    assert color_first == color_second
    assert color_first != "#999999"


def test_build_color_discrete_map_assigns_unknown_artists_without_gray() -> None:
    color_map = build_color_discrete_map(["Known", "Unknown"], palette={"Known": "#7A1F2B"})
    assert color_map["Known"] == "#7A1F2B"
    assert color_map["Unknown"] != "#999999"
    assert color_map["Unknown"].startswith("#")
