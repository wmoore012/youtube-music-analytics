#!/usr/bin/env python3
import pytest
from src.icatalog_public.oss.etl_helpers_oss import extract_isrc_from_text, run_with_timeout


def test_extract_isrc_hyphenated():
    text = "Great song ISRC US-UM7-20-12345 appears here"
    assert extract_isrc_from_text(text) == "USUM72012345"


def test_extract_isrc_compact():
    text = "isrc=USUM72012345"
    assert extract_isrc_from_text(text) == "USUM72012345"


def test_extract_isrc_none():
    assert extract_isrc_from_text("") is None
    assert extract_isrc_from_text(None) is None


def test_run_with_timeout_success():
    def f(x):
        return x + 1

    assert run_with_timeout(f, 0.5, 1) == 2


def test_run_with_timeout_timeout():
    import time

    def slow():
        time.sleep(0.2)
        return 1

    with pytest.raises(TimeoutError):
        run_with_timeout(slow, 0.05)
