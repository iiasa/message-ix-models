"""Generate R12 desalination input data from external country/region sources.

The generator writes both committed R12 desalination files:

- ``projected_desalination_potential_km3_year_R12.csv``
- ``historical_capacity_desalination_km3_year_R12.csv``

Source data live on pdrive at
``/mnt/p/watxene/Wat-Data/desalination/marina_refresh_2026``. Country-level
``total.desal`` supplies the projected and observed capacity levels; regional
technology cumulative columns supply historical technology shares. Basin
allocation uses the fixed R12 desalination template in
``data/water/infrastructure/desalination_basin_allocation_template_R12.csv``.

Run with ``uv run --no-sync python -m`` and the module path
``message_ix_models.model.water.data.pre_processing.generate_desalination``.
"""

# TODO: move this module's docstring content to DOCS when the docs update lands.

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from message_ix_models.model.water.data.pre_processing.basin_allocation import (
    country_to_region_map,
    distribute_by_shares,
)
from message_ix_models.model.water.utils import M3DAY_TO_KM3YR
from message_ix_models.util import package_data_path

SOURCE_DESAL = Path("/mnt/p/watxene/Wat-Data/desalination/marina_refresh_2026")

INFRA = package_data_path("water", "infrastructure")

PROJECTED_OUTPUT = INFRA / "projected_desalination_potential_km3_year_R12.csv"
HISTORICAL_OUTPUT = INFRA / "historical_capacity_desalination_km3_year_R12.csv"
ALLOCATION_TEMPLATE = INFRA / "desalination_basin_allocation_template_R12.csv"
SOURCE_SSPS: tuple[str, ...] = ("SSP1", "SSP3", "SSP5")
# SSPs not modelled by Marina inherit the nearest source SSP by assignment.
SSP_ASSIGNMENT: dict[str, str] = {
    "SSP1": "SSP1",
    "SSP2": "SSP1",
    "SSP3": "SSP3",
    "SSP4": "SSP3",
    "SSP5": "SSP5",
}
HISTORICAL_YEARS: tuple[int, ...] = (1995, 2000, 2005, 2010, 2015, 2020)
CARRY_FORWARD_YEAR = 2025
SOURCE_REGION_TO_R12: dict[str, list[str]] = {
    "A. Pacific": ["PAO"],
    "Africa": ["AFR"],
    "E. Asia": ["CHN", "RCPA"],
    "Eurasia": ["FSU"],
    "Europe": ["WEU", "EEU"],
    "Latin Am.": ["LAM"],
    "Middle E.": ["MEA"],
    "N. America": ["NAM"],
    "S. Asia": ["SAS"],
    "S.E. Asia": ["PAS"],
}
R12_TO_SOURCE_REGION: dict[str, str] = {
    r12: source for source, r12s in SOURCE_REGION_TO_R12.items() for r12 in r12s
}
TECH_TO_TEC_TYPE = {
    "Distillation-based": "distillation",
    "Membrane-based": "membrane",
    "Electrochemical": "membrane",
}


def _load_country_capacity() -> pd.DataFrame:
    """Load country-level desalination capacity in km3/yr."""
    df = pd.read_csv(SOURCE_DESAL / "cntry_level_desal.csv")
    return df.assign(cap_km3_year=df["total.desal"] * M3DAY_TO_KM3YR)[
        ["countrycode", "year", "scenario", "cap_km3_year"]
    ]


def _country_to_r12_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate country rows to R12 by dominant country-region overlap."""
    c2r = country_to_region_map("R12")
    return (
        df.assign(model_region=df["countrycode"].map(c2r))
        .dropna(subset=["model_region"])
        .groupby(["model_region", "year", "scenario"], as_index=False)["cap_km3_year"]
        .sum()
    )


def _allocation_template(allocation: str) -> pd.DataFrame:
    """Load the fixed R12 desalination basin allocation template."""
    template = pd.read_csv(ALLOCATION_TEMPLATE, comment="#")
    required = {"allocation", "model_region", "tec_type", "BCU_name", "share"}
    if missing := required - set(template.columns):
        raise ValueError(
            f"{ALLOCATION_TEMPLATE.name}: missing columns {sorted(missing)}"
        )

    selected = template[template["allocation"] == allocation].copy()
    if selected.empty:
        raise ValueError(f"{ALLOCATION_TEMPLATE.name}: no rows for {allocation!r}")
    return selected


def build_projected() -> pd.DataFrame:
    """Build projected desalination potential for R12, keyed by SSP."""
    projected = _country_to_r12_totals(_load_country_capacity())
    projected = projected[projected["scenario"].isin(SOURCE_SSPS)].rename(
        columns={"scenario": "ssp"}
    )
    projected = distribute_by_shares(
        projected,
        _allocation_template("projected").drop(columns=["allocation", "tec_type"]),
        on=["model_region"],
        value_col="cap_km3_year",
        output_col="basin_cap_km3_year",
    )
    projected = (
        projected.drop(columns=["model_region", "cap_km3_year", "share"])
        .rename(columns={"basin_cap_km3_year": "cap_km3_year"})
        .groupby(["BCU_name", "ssp", "year"], as_index=False)["cap_km3_year"]
        .sum()
    )

    expanded = [
        projected[projected["ssp"] == source].assign(ssp=target)
        for target, source in SSP_ASSIGNMENT.items()
        if target != source
    ]
    return (
        pd.concat([projected, *expanded], ignore_index=True)
        .sort_values(["BCU_name", "ssp", "year"])
        .reset_index(drop=True)
    )


def _country_vintage_flow() -> pd.DataFrame:
    """Observed country cumulative capacity differenced to 5-year additions."""
    observed = _load_country_capacity()
    observed = observed[observed["scenario"] == "Observed"]
    wide = (
        observed.pivot(index="countrycode", columns="year", values="cap_km3_year")
        .fillna(0.0)
        .sort_index(axis=1)
    )
    records = []
    for year in HISTORICAL_YEARS:
        previous = year - 5
        if previous not in wide.columns or year not in wide.columns:
            raise ValueError(f"observed desalination data missing {previous} or {year}")
        flow = (wide[year] - wide[previous]).clip(lower=0)
        records.append(
            pd.DataFrame(
                {
                    "countrycode": flow.index,
                    "year": year,
                    "cap_km3_year": flow.to_numpy(),
                }
            )
        )
    return pd.concat(records, ignore_index=True)


def _r12_vintage_totals() -> pd.DataFrame:
    """Observed country vintage additions aggregated to R12."""
    flows = _country_vintage_flow().assign(scenario="Observed")
    return (
        _country_to_r12_totals(flows)
        .drop(columns="scenario")
        .rename(columns={"cap_km3_year": "r12_vintage_km3"})
    )


def _source_region_for_model_region() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"model_region": model_region, "source_region": source_region}
            for model_region, source_region in R12_TO_SOURCE_REGION.items()
        ]
    )


def _vintage_tech_share() -> pd.DataFrame:
    """Technology shares for historical vintage additions by source region."""
    energy = pd.read_csv(SOURCE_DESAL / "reg_desal_energy.csv")
    energy = energy[energy["scenario"] == "Historical"]
    energy = energy.assign(tec_type=energy["technology"].map(TECH_TO_TEC_TYPE))
    energy = energy.dropna(subset=["tec_type"])

    tech = energy.groupby(["reg.code", "year", "tec_type"], as_index=False)[
        "reg.tech.cumsum"
    ].sum()
    totals = energy.groupby(["reg.code", "year"], as_index=False)[
        "reg.total.cumsum"
    ].first()

    records = []
    for source_region in tech["reg.code"].unique():
        sub = tech[tech["reg.code"] == source_region]
        total = totals[totals["reg.code"] == source_region].set_index("year")[
            "reg.total.cumsum"
        ]
        for year in HISTORICAL_YEARS:
            previous = year - 5
            if year not in total.index or previous not in total.index:
                continue
            total_flow = total.loc[year] - total.loc[previous]
            if total_flow <= 0:
                continue
            for tec_type in ("membrane", "distillation"):
                current = sub[(sub["year"] == year) & (sub["tec_type"] == tec_type)]
                previous_rows = sub[
                    (sub["year"] == previous) & (sub["tec_type"] == tec_type)
                ]
                share = max(
                    0.0,
                    (
                        float(current["reg.tech.cumsum"].sum())
                        - float(previous_rows["reg.tech.cumsum"].sum())
                    )
                    / total_flow,
                )
                records.append(
                    {
                        "source_region": source_region,
                        "year": year,
                        "tec_type": tec_type,
                        "share": share,
                    }
                )

    shares = pd.DataFrame(records)
    total = shares.groupby(["source_region", "year"])["share"].transform("sum")
    return shares.assign(share=np.where(total > 0, shares["share"] / total, 0.0))


def build_historical() -> pd.DataFrame:
    """Build historical desalination vintage additions for R12."""
    r12_flow = _r12_vintage_totals().merge(
        _source_region_for_model_region(), on="model_region", how="left"
    )
    if missing := sorted(
        r12_flow.loc[r12_flow["source_region"].isna(), "model_region"].unique()
    ):
        raise KeyError(f"R12 regions with no source-region mapping: {missing}")

    historical = r12_flow.merge(
        _vintage_tech_share(), on=["source_region", "year"], how="inner"
    )
    historical = historical.assign(
        cap_km3_year=historical["r12_vintage_km3"] * historical["share"]
    ).drop(columns="share")
    historical = distribute_by_shares(
        historical,
        _allocation_template("historical").drop(columns="allocation"),
        on=["model_region", "tec_type"],
        value_col="cap_km3_year",
        output_col="basin_cap_km3_year",
    )
    historical = (
        historical.drop(columns=["r12_vintage_km3", "source_region", "share"])
        .drop(columns="cap_km3_year")
        .rename(columns={"basin_cap_km3_year": "cap_km3_year"})
        .groupby(["BCU_name", "year", "tec_type"], as_index=False)["cap_km3_year"]
        .sum()
        .loc[lambda df: df["cap_km3_year"] > 0]
    )

    carry_forward = historical[historical["year"] == 2020].assign(
        year=CARRY_FORWARD_YEAR
    )
    return (
        pd.concat([historical, carry_forward], ignore_index=True)
        .sort_values(["BCU_name", "year", "tec_type"])
        .reset_index(drop=True)
    )


def write_outputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build and write projected and historical desalination files."""
    projected = build_projected()
    historical = build_historical()
    projected.to_csv(PROJECTED_OUTPUT, index=False)
    historical.to_csv(HISTORICAL_OUTPUT, index=False)
    return projected, historical


def _print_summary(projected: pd.DataFrame, historical: pd.DataFrame) -> None:
    print(f"Wrote {PROJECTED_OUTPUT.name}: {len(projected)} rows")
    print(f"  basins: {projected['BCU_name'].nunique()}")
    print(f"  ssps: {sorted(projected['ssp'].unique())}")
    print(f"  years: {[int(y) for y in sorted(projected['year'].unique())]}")
    print(f"Wrote {HISTORICAL_OUTPUT.name}: {len(historical)} rows")
    print(f"  basins: {historical['BCU_name'].nunique()}")
    print(f"  years: {[int(y) for y in sorted(historical['year'].unique())]}")


def main() -> None:
    projected, historical = write_outputs()
    _print_summary(projected, historical)


if __name__ == "__main__":
    main()
