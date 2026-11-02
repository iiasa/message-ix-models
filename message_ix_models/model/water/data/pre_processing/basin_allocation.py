"""Shared basin-allocation helpers for water input refreshes.

The committed water data uses MESSAGE basin labels such as ``120|MEA``. Some
refresh sources are country-level or region-level, so pre-processing scripts
need an explicit, reusable convention for distributing those values across
basins.
"""

# TODO: move this module's docstring content to DOCS when the docs update lands.

from __future__ import annotations

from typing import Literal

import pandas as pd

from message_ix_models.util import package_data_path

Region = Literal["R11", "R12", "ZMB"]


def load_country_basin_overlap(region: Region) -> pd.DataFrame:
    """Load country-basin overlap areas for ``region``.

    Rows with missing ISO code ``-99`` are excluded. The returned frame has the
    normalized columns ``country``, ``BCU_name``, ``model_region``, and
    ``area_km2``.
    """
    path = package_data_path("water", "delineation", f"basins_country_{region}.csv")
    overlap = pd.read_csv(path)
    required = {"ISO", "BCU_name", "REGION", "area_km2"}
    missing = required - set(overlap.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {sorted(missing)}")

    return (
        overlap.loc[overlap["ISO"].ne("-99"), ["ISO", "BCU_name", "REGION", "area_km2"]]
        .rename(columns={"ISO": "country", "REGION": "model_region"})
        .assign(area_km2=lambda df: df["area_km2"].astype(float))
    )


def country_to_region_map(region: Region) -> dict[str, str]:
    """Map each country to its dominant MESSAGE water region by overlap area."""
    overlap = load_country_basin_overlap(region)
    dominant = (
        overlap.groupby(["country", "model_region"], as_index=False)["area_km2"]
        .sum()
        .sort_values("area_km2", ascending=False)
        .drop_duplicates("country", keep="first")
    )
    return dict(zip(dominant["country"], dominant["model_region"]))


def distribute_by_shares(
    totals: pd.DataFrame,
    shares: pd.DataFrame,
    *,
    on: list[str],
    value_col: str,
    output_col: str,
) -> pd.DataFrame:
    """Distribute grouped totals to basins using a share table."""
    required_totals = {*on, value_col}
    required_shares = {*on, "BCU_name", "share"}
    if missing := required_totals - set(totals.columns):
        raise ValueError(f"totals missing columns {sorted(missing)}")
    if missing := required_shares - set(shares.columns):
        raise ValueError(f"shares missing columns {sorted(missing)}")

    joined = totals.merge(shares, on=on, how="inner")
    return joined.assign(**{output_col: joined[value_col] * joined["share"]})
