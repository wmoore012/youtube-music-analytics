import pandas as pd

from src.youtubeviz.utils import ensure_cols


def test_ensure_cols_supports_fill_mapping():
    df = pd.DataFrame({"existing": [1]})

    result = ensure_cols(df, ["existing", "missing_a", "missing_b"], fill={"missing_a": 0, "missing_b": 2})

    assert list(result.columns) == ["existing", "missing_a", "missing_b"]
    assert result.loc[0, "missing_a"] == 0
    assert result.loc[0, "missing_b"] == 2
    assert "missing_a" not in df.columns
    assert "missing_b" not in df.columns
