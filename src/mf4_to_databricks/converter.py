"""Convert MF4 measurement data to Parquet format for Databricks upload."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from asammdf import MDF


def mf4_to_dataframe(
    path: str | Path,
    channels: list[str] | None = None,
    group_indices: list[int] | None = None,
    raster: float | None = None,
) -> pd.DataFrame:
    """Extract signals from an MF4 file into a single DataFrame.

    Parameters
    ----------
    path : path to the .mf4 file
    channels : optional list of channel names to extract (None = all)
    group_indices : optional list of group indices to include (None = all)
    raster : optional resampling interval in seconds (None = keep original)

    Returns
    -------
    pd.DataFrame with a 'timestamps' column and one column per signal.
    """
    mdf = MDF(str(path))

    if group_indices is not None:
        # Build channel list from selected groups
        group_channels: list[str] = []
        for idx in group_indices:
            group_channels.extend(ch.name for ch in mdf.groups[idx].channels)
        # Intersect with explicit channel filter if given
        if channels is not None:
            group_channels = [c for c in group_channels if c in set(channels)]
        channels = group_channels or None

    if channels:
        df = mdf.to_dataframe(channels=channels, raster=raster, time_from_zero=True)
    else:
        df = mdf.to_dataframe(raster=raster, time_from_zero=True)

    df.index.name = "timestamps"
    df = df.reset_index()
    return df


def dataframe_to_parquet(df: pd.DataFrame, output_path: str | Path) -> Path:
    """Write a DataFrame to Parquet.

    Parameters
    ----------
    df : the data to write
    output_path : destination .parquet file path

    Returns
    -------
    Path to the written file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    return output_path


def mf4_to_parquet(
    mf4_path: str | Path,
    output_path: str | Path | None = None,
    channels: list[str] | None = None,
    group_indices: list[int] | None = None,
    raster: float | None = None,
) -> Path:
    """One-step conversion: MF4 → Parquet.

    If *output_path* is None, the parquet file is written next to the MF4 file
    with the same stem and a .parquet extension.
    """
    mf4_path = Path(mf4_path)
    if output_path is None:
        output_path = mf4_path.with_suffix(".parquet")

    df = mf4_to_dataframe(mf4_path, channels=channels, group_indices=group_indices, raster=raster)
    return dataframe_to_parquet(df, output_path)
