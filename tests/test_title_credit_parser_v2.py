# tests/icatalog_public/oss/test_title_credit_parser_v2.py

import pytest
from src.icatalog_public.oss.title_credit_parser_v2 import (  # helper that splits "A & B - Title"
    parse_title_and_credits,
    split_artists_from_title,
)


def test_basic_title_no_features_no_version():
    title = "My Awesome Song"
    expected = {
        "artist": "",
        "title": "My Awesome Song",
        "features": [],
        "version": "Original",
    }
    assert parse_title_and_credits(title) == expected


def test_title_with_feat_parentheses():
    title = "Song Title (feat. Featured Artist)"
    expected = {
        "artist": "",
        "title": "Song Title",
        "features": ["Featured Artist"],
        "version": "Original",
    }
    assert parse_title_and_credits(title) == expected


def test_title_with_ft_parentheses():
    title = "Another Song (ft. Another Artist)"
    expected = {
        "artist": "",
        "title": "Another Song",
        "features": ["Another Artist"],
        "version": "Original",
    }
    assert parse_title_and_credits(title) == expected


def test_title_with_featuring_parentheses():
    title = "Third Song (featuring Third Artist)"
    expected = {
        "artist": "",
        "title": "Third Song",
        "features": ["Third Artist"],
        "version": "Original",
    }
    assert parse_title_and_credits(title) == expected


def test_title_with_remix_parentheses():
    title = "Dance Track (Remix)"
    expected = {
        "artist": "",
        "title": "Dance Track",
        "features": [],
        "version": "Remix",
    }
    assert parse_title_and_credits(title) == expected


def test_title_with_live_parentheses():
    title = "Concert Jam (Live)"
    expected = {"artist": "", "title": "Concert Jam", "features": [], "version": "Live"}
    assert parse_title_and_credits(title) == expected


def test_title_with_multiple_features():
    title = "Collaboration (feat. Artist A & Artist B, Artist C)"
    expected = {
        "artist": "",
        "title": "Collaboration",
        "features": ["Artist A", "Artist B", "Artist C"],
        "version": "Original",
    }
    assert parse_title_and_credits(title) == expected


def test_title_with_feature_and_version():
    title = "Epic Tune (feat. Guest) (Live Version)"
    expected = {
        "artist": "",
        "title": "Epic Tune",
        "features": ["Guest"],
        "version": "Live Version",  # This might need refinement in the regex
    }
    assert parse_title_and_credits(title) == expected


def test_title_with_acoustic_version():
    title = "Quiet Moment (Acoustic)"
    expected = {
        "artist": "",
        "title": "Quiet Moment",
        "features": [],
        "version": "Acoustic",
    }
    assert parse_title_and_credits(title) == expected


def test_youtube_noise_off_by_default():
    t = "Title (Official Video)"
    got = parse_title_and_credits(t)
    assert got["title"] == "Title"
    # No version keyword present -> defaults Original, even if noise is present
    assert got["version"] == "Original"


def test_youtube_noise_removed_when_toggle_on():
    t = "Title (Official Audio)"
    got = parse_title_and_credits(t, normalize_youtube_noise=True)
    assert got["title"] == "Title"
    assert got["version"] == "Original"


def test_slowed_reverb_canonicalization_variants():
    for variant in [
        "Luther's Freestyle (SlowedxReverb)",
        "Luther's Freestyle (Slowed + Reverb)",
        "Luther's Freestyle (slowed & reverb)",
        "Luther's Freestyle (Slowed Reverb)",
        "Luther's Freestyle (slowed x reverb)",
    ]:
        got = parse_title_and_credits(variant, normalize_youtube_noise=True)
        assert got["title"] == "Luther's Freestyle"
        assert got["version"] == "Slowed and Reverbed"


def test_feat_and_version_and_noise():
    t = "Epic Tune (feat. Guest) (Live Version) (Official Video)"
    got = parse_title_and_credits(t, normalize_youtube_noise=True)
    assert got["title"] == "Epic Tune"
    assert got["features"] == ["Guest"]
    assert got["version"] == "Live Version"


def test_multi_main_artists_in_title_string_is_left_intact():
    # This parser does not split main artists from title; it only cleans the title/credits.
    t = "Artist A & Artist B - Shared Song (feat. C, D) (Visualizer)"
    got = parse_title_and_credits(t, normalize_youtube_noise=True)
    # The "artist" key is intentionally blank (consistent with your existing tests)
    assert got["artist"] == ""
    assert got["title"] == "Artist A & Artist B - Shared Song"
    assert got["features"] == ["C", "D"]
    assert got["version"] == "Original"


@pytest.mark.parametrize(
    "raw,expected_version",
    [
        ("Luther's Freestyle (SlowedxReverb)", "Slowed and Reverbed"),
        ("Luther's Freestyle (Slowed + Reverb)", "Slowed and Reverbed"),
        ("Luther's Freestyle (Slowed & Reverb)", "Slowed and Reverbed"),
        ("Luther's Freestyle (Slowed Reverb)", "Slowed and Reverbed"),
        ("Luther's Freestyle (Slowed and Reverb)", "Slowed and Reverbed"),
        ("Luther's Freestyle (Reverbed + Slowed)", "Slowed and Reverbed"),
    ],
)
def test_slowed_reverb_variants(raw, expected_version):
    got = parse_title_and_credits(raw)
    assert got["title"] == "Luther's Freestyle"
    assert got["features"] == []
    assert got["version"] == expected_version


def test_multi_artists_and_features_and_version():
    full = "Lute & JID - Luther's Freestyle (feat. EarthGang) (Remix)"
    artists, core_title = split_artists_from_title(full)
    assert artists == ["Lute", "JID"]

    got = parse_title_and_credits(core_title)
    assert got["title"] == "Luther's Freestyle"
    assert got["features"] == ["EarthGang"]
    assert got["version"] == "Remix"


def test_youtube_noise_off_by_default():
    t = "Title (Official Video)"
    got = parse_title_and_credits(t)
    # Noise in parens should not become a version and must not remain in the title.
    assert got["title"] == "Title"
    assert got["version"] == "Original"


def test_youtube_noise_removed_when_toggle_on():
    t = "Title (Official Audio)"
    got = parse_title_and_credits(t, normalize_youtube_noise=True)
    assert got["title"] == "Title"
    assert got["version"] == "Original"


def test_slowed_reverb_canonicalization_variants():
    for variant in [
        "Luther's Freestyle (SlowedxReverb)",
        "Luther's Freestyle (Slowed + Reverb)",
        "Luther's Freestyle (slowed & reverb)",
        "Luther's Freestyle (Slowed Reverb)",
        "Luther's Freestyle (slowed x reverb)",
    ]:
        got = parse_title_and_credits(variant, normalize_youtube_noise=True)
        assert got["title"] == "Luther's Freestyle"
        assert got["version"] == "Slowed and Reverbed"


def test_feat_and_version_and_noise():
    t = "Epic Tune (feat. Guest) (Live Version) (Official Video)"
    got = parse_title_and_credits(t, normalize_youtube_noise=True)
    assert got["title"] == "Epic Tune"
    assert got["features"] == ["Guest"]
    assert got["version"] == "Live Version"


def test_multi_main_artists_in_title_string_is_left_intact():
    # This parser does not split main artists from title; it only cleans the title/credits.
    t = "Artist A & Artist B - Shared Song (feat. C, D) (Visualizer)"
    got = parse_title_and_credits(t, normalize_youtube_noise=True)
    # The "artist" key is intentionally blank (consistent with existing behavior)
    assert got["artist"] == ""
    assert got["title"] == "Artist A & Artist B - Shared Song"
    assert got["features"] == ["C", "D"]
    assert got["version"] == "Original"


def test_multiple_primary_artists_no_features():
    full = "Artist One & Artist Two - Shared Hit"
    artists, core_title = split_artists_from_title(full)
    assert artists == ["Artist One", "Artist Two"]

    got = parse_title_and_credits(core_title)
    assert got["title"] == "Shared Hit"
    assert got["features"] == []
    assert got["version"] == "Original"


def test_multiple_primary_artists_with_features_and_version():
    full = "Artist One, Artist Two - Anthem (feat. Guest A & Guest B) (Live Version)"
    artists, core_title = split_artists_from_title(full)
    assert artists == ["Artist One", "Artist Two"]

    got = parse_title_and_credits(core_title)
    assert got["title"] == "Anthem"
    assert got["features"] == ["Guest A", "Guest B"]
    assert got["version"] == "Live Version"
