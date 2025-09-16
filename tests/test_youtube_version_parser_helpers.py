"""Regression tests for YouTube title parsing helpers."""

from web.youtube_version_parser import (
    _extract_collaboration_clauses,
    parse_youtube_title,
)


def test_extract_collaboration_clauses_handles_multiple_patterns():
    """The helper should remove both feat/with clauses and keep collaborator order."""

    title, featured = _extract_collaboration_clauses("Dream Line feat. Guest One with Crew Two")

    assert title == "Dream Line"
    assert featured == ["Guest One", "Crew Two"]


def test_parse_possessive_title_uses_collaboration_helper():
    """Possessive-form titles should parse featured artists only once."""

    result = parse_youtube_title("Singer One's song Bright Lights feat. Guest One", "")

    assert result["primary"] == ["Singer One"]
    assert result["featured"] == ["Guest One"]


def test_parse_artist_list_title_deduplicates_collaborators():
    """Repeated collaborator names should be reported once in the featured list."""

    result = parse_youtube_title(
        "Singer One & Singer Two - Bright Lights feat. Guest One feat. Guest One",
        "",
    )

    assert result["featured"] == ["Guest One"]
    assert "feat" not in result["title"].lower()
