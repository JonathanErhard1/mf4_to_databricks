"""Convert MF4 measurement data to Parquet format for Databricks upload."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from asammdf import MDF


def mf4_to_dataframe(
    path_or_mdf: str | Path | MDF,
    channels: list[str] | None = None,
    group_indices: list[int] | None = None,
    raster: float | None = None,
) -> pd.DataFrame:
    """Extract signals from an MF4 file into a single DataFrame.

    Parameters
    ----------
    path_or_mdf : path to the .mf4 file, or an already-opened MDF object
    channels : optional list of channel names to extract (None = all)
    group_indices : optional list of group indices to include (None = all)
    raster : optional resampling interval in seconds (None = keep original)

    Returns
    -------
    pd.DataFrame with a 'timestamps' column and one column per signal.
    """
    if isinstance(path_or_mdf, MDF):
        mdf = path_or_mdf
    else:
        mdf = MDF(str(path_or_mdf))

    # Determine which channels to extract
    target_channels: list[str] | None = None

    if group_indices is not None:
        group_channels: list[str] = []
        for idx in group_indices:
            group_channels.extend(ch.name for ch in mdf.groups[idx].channels)
        if channels is not None:
            ch_set = set(channels)
            group_channels = [c for c in group_channels if c in ch_set]
        target_channels = group_channels or None
    elif channels is not None:
        target_channels = channels

    # Build DataFrame signal-by-signal for robustness
    if target_channels is None:
        target_channels = sorted(mdf.channels_db.keys())

    # Skip internal timestamp channels
    skip = {"time", "timestamps", "t"}
    target_channels = [c for c in target_channels if c.lower() not in skip]

    series_dict: dict[str, pd.Series] = {}
    timestamps_arr: np.ndarray | None = None

    for ch_name in target_channels:
        try:
            sig = mdf.get(ch_name, raw=False)
        except Exception:
            continue

        samples = sig.samples

        # Skip non-numeric signals (byte arrays, strings, structured arrays)
        if samples.dtype.kind in ("U", "S", "O", "V"):
            continue
        # Skip multi-dimensional samples
        if samples.ndim != 1:
            continue

        ts = sig.timestamps

        # Use raster resampling if requested
        if raster is not None:
            try:
                sig = sig.interp(np.arange(ts[0], ts[-1], raster)) if len(ts) > 1 else sig
                ts = sig.timestamps
                samples = sig.samples
            except Exception:
                continue

        # Deduplicate column names
        col_name = ch_name
        counter = 1
        while col_name in series_dict:
            col_name = f"{ch_name}_{counter}"
            counter += 1

        # Store first valid timestamp array as reference
        if timestamps_arr is None or len(ts) > len(timestamps_arr):
            timestamps_arr = ts

        series_dict[col_name] = pd.Series(samples, index=ts, dtype=np.float64, name=col_name)

    if not series_dict:
        raise ValueError("Keine numerischen Kanäle gefunden, die exportiert werden können.")

    # Align all series to a common time axis
    if raster is not None and timestamps_arr is not None:
        common_ts = np.arange(timestamps_arr[0], timestamps_arr[-1], raster)
    else:
        assert timestamps_arr is not None
        common_ts = timestamps_arr

    df = pd.DataFrame({"timestamps": common_ts})
    for col_name, s in series_dict.items():
        # Reindex to common timestamps, forward-fill gaps
        aligned = s.reindex(s.index.union(common_ts)).interpolate(method="index").reindex(common_ts)
        df[col_name] = aligned.values

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
