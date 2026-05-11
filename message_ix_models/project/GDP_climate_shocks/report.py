"""Climate damage cost reporting for GDP–climate impact scenarios."""

import logging

import pandas as pd
import pyam

from message_ix_models.util import private_data_path

from .util import regional_gdp_impacts

log = logging.getLogger(__name__)

DAMAGE_VARIABLE = "Damage Cost|Gross"  # Economic Climate Damages without adaptation
DAMAGE_UNIT = "billion US$2010/yr"
DAMAGE_REL_VARIABLE = "Damage Cost|Gross|Relative"
DAMAGE_REL_UNIT = "%"
BASE_YEAR = 2025

# Mapping from RIME model node codes to IXMP/pyam region names used in the
# scenario timeseries. Keeps the merged result in timeseries format so it can
# be uploaded back via add_timeseries() without further renaming.
_NODE_TO_REGION_R12 = {
    "R12_CHN": "China (R12)",
    "R12_EEU": "Eastern Europe (R12)",
    "R12_FSU": "Former Soviet Union (R12)",
    "R12_LAM": "Latin America (R12)",
    "R12_MEA": "Middle East and Africa (R12)",
    "R12_NAM": "North America (R12)",
    "R12_PAS": "Pacific Asia (R12)",
    "R12_PAO": "Pacific OECD (R12)",
    "R12_RCPA": "Rest of Centrally planned Asia (R12)",
    "R12_SAS": "South Asia (R12)",
    "R12_AFR": "Subsaharan Africa (R12)",
    "R12_WEU": "Western Europe (R12)",
}


def report_damages(
    scs,
    sc_str: str,
    damage_model: str,
    pp: int,
    it: int,
    ssp: str,
    regions: str,
    discount_rate: float = 0.015,
) -> None:
    """Calculate discounted climate damage timeseries and append to the scenario.

    Multiplies RIME regional GDP loss (%) by GDP|MER from the converged
    scenario timeseries and applies a discount factor from BASE_YEAR (2025).
    Appends the result as a new variable to the scenario timeseries and
    updates the reporting Excel output file on disk.

    Parameters
    ----------
    scs : message_ix.Scenario
        Converged scenario whose timeseries already contains GDP|MER —
        i.e. ``run_legacy_reporting`` must be called before this function.
    sc_str : str
        Scenario string used to locate RIME output files, typically
        ``"{model_name_clone}_{scenario_name}_GDP_CI"``.
    damage_model : str
        Damage function model name (e.g. "Burke", "Waidelich").
    pp : int
        Percentile of climate damages (e.g. 50).
    it : int
        Final converged iteration number. The RIME file written during this
        iteration is loaded (``regional_gdp_impacts`` is called with ``it + 1``
        internally so that it reads the file indexed at ``it``).
    ssp : str
        Shared Socioeconomic Pathway identifier (e.g. "SSP2").
    regions : str
        Region mapping YAML file stem passed to ``regional_gdp_impacts``
        (e.g. "R12").
    discount_rate : float, optional
        Annual discount rate applied from BASE_YEAR, by default 0.015 (1.5 %).
    """

    log.info(
        "Calculating damage timeseries for %s (discount_rate=%.3f)",
        scs.scenario,
        discount_rate,
    )

    # Step 1: regional % GDP loss from the final RIME iteration.
    # regional_gdp_impacts reads the file indexed at (it_arg - 1), so pass
    # it + 1 to load the RIME output written during the last loop iteration.
    gdp_change_df = regional_gdp_impacts(sc_str, damage_model, it + 1, ssp, regions, pp)

    # Step 2: GDP|MER from scenario timeseries (populated by run_legacy_reporting).
    idf = pyam.IamDataFrame(scs.timeseries())
    gdp_data = idf.filter(variable="GDP|MER").filter(region="World", keep=False).data

    if gdp_data.empty:
        raise ValueError(
            f"GDP|MER not found in timeseries of {scs.model} {scs.scenario}. "
            "Ensure run_legacy_reporting has completed before calling report_damages."
        )

    # Step 3: merge RIME % loss with GDP|MER and compute discounted damages.
    gdp_df = gdp_data[["region", "year", "value"]].rename(
        columns={"region": "node", "value": "gdp_mer"}
    )
    # Translate RIME node codes to timeseries region names before merging.
    gdp_change_df = (
        gdp_change_df.assign(node=gdp_change_df["node"].map(_NODE_TO_REGION_R12))
        .dropna(subset=["node"])
        .drop_duplicates(subset=["node", "year"])
    )
    merged = gdp_df.merge(gdp_change_df, on=["node", "year"], how="inner")

    if merged.empty:
        raise ValueError(
            "No overlapping (region, year) pairs between GDP|MER timeseries and "
            f"RIME output for {scs.scenario}. Check region naming conventions."
        )

    # perc_change_sum is negative (GDP loss) → negate to get positive damage cost.
    merged["damage"] = -merged["gdp_mer"] * merged["perc_change_sum"] / 100
    discount_factor = (1 + discount_rate) ** (merged["year"] - BASE_YEAR)
    merged["value"] = merged["damage"] / discount_factor
    # relative damage: raw RIME loss negated to positive — discount-rate agnostic
    merged["value_rel"] = -merged["perc_change_sum"]

    # Step 4: add World aggregate (sum of regional damages / sum of regional GDP).
    world_abs = merged.groupby("year", as_index=False)["value"].sum()
    world_abs["node"] = "World"
    # World relative: GDP-weighted average of regional loss percentages
    world_gdp = merged.groupby("year")["gdp_mer"].sum()
    world_rel_val = (
        merged.groupby("year")["damage"].sum() / world_gdp * 100
    ).reset_index()
    world_rel_val.columns = ["year", "value_rel"]
    world_rel_val["node"] = "World"

    result_abs = pd.concat(
        [merged[["node", "year", "value"]], world_abs[["node", "year", "value"]]],
        ignore_index=True,
    )
    result_rel = pd.concat(
        [
            merged[["node", "year", "value_rel"]].rename(
                columns={"value_rel": "value"}
            ),
            world_rel_val[["node", "year", "value_rel"]].rename(
                columns={"value_rel": "value"}
            ),
        ],
        ignore_index=True,
    )

    def _to_idf(df, variable, unit):
        out = df.copy()
        out["model"] = scs.model
        out["scenario"] = scs.scenario
        out["variable"] = variable
        out["unit"] = unit
        return out.rename(columns={"node": "region"})

    result_idf = pyam.IamDataFrame(
        pd.concat(
            [
                _to_idf(result_abs, DAMAGE_VARIABLE, DAMAGE_UNIT),
                _to_idf(result_rel, DAMAGE_REL_VARIABLE, DAMAGE_REL_UNIT),
            ],
            ignore_index=True,
        )
    )

    # Step 5: append to scenario timeseries in the database.
    # timeseries() returns wide format with MultiIndex (model, scenario, region,
    # variable, unit); drop model/scenario levels so add_timeseries gets the
    # expected (region, variable, unit) index.
    result_ts = (
        result_idf.timeseries()
        .reset_index(level=["model", "scenario"], drop=True)
        .reset_index()
    )

    scs.check_out(timeseries_only=True)
    scs.add_timeseries(result_ts)
    scs.commit(f"Add {DAMAGE_VARIABLE} and {DAMAGE_REL_VARIABLE}")
    log.info("Damage timeseries committed to scenario %s", scs.scenario)

    # Step 6: replace reporting Excel file with version that includes damage rows.
    rep_path = private_data_path().parent / "reporting_output"
    report_file = rep_path / f"{scs.model}_{scs.scenario}.xlsx"

    if report_file.exists():
        existing = pyam.IamDataFrame(str(report_file))
        updated = existing.append(result_idf)
        updated.to_excel(str(report_file))
        log.info("Reporting file updated: %s", report_file.name)
    else:
        log.warning(
            "Reporting file not found at %s. Writing damages separately.",
            report_file,
        )
        damage_file = rep_path / f"{scs.model}_{scs.scenario}_damages.xlsx"
        result_idf.to_excel(str(damage_file))
        log.info("Damage file written: %s", damage_file.name)
