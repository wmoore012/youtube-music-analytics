from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd


def ensure_cols(df: pd.DataFrame, cols: Iterable[str], fill: Optional[object] = None) -> pd.DataFrame:
    """Ensure dataframe has the specified columns; add them filled with `fill` if missing."""
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = fill
    return df


def safe_head(df: pd.DataFrame, cols: Optional[Iterable[str]] = None, n: int = 5) -> pd.DataFrame:
    """Return a safe head view of the dataframe selecting `cols` when possible."""
    if cols:
        cols = [c for c in cols if c in df.columns]
        if not cols:
            # fallback to returning head of full df
            return df.head(n)
        return df.loc[:, cols].head(n)
    return df.head(n)


def filter_artists(df: pd.DataFrame, col: str, artists: Iterable[str]) -> pd.DataFrame:
    """Return rows where `col` is in the provided `artists` list."""
    artists = list(artists) if artists is not None else []
    if not artists:
        return df.copy()
    return df.loc[df[col].isin(artists)].copy()


@dataclass
class ArtistFilter:
    col: str
    artists: List[str]

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return filter_artists(df, self.col, self.artists)


__all__ = ["ensure_cols", "safe_head", "filter_artists", "ArtistFilter"]
