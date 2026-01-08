"""ALPS project reporting module.

Genno-based reporting for water-nexus and cooling module results.
All value computations via genno (aggregations, selections, unit assignment).
Python handles only final packaging (metadata, concat, file output).

Usage
-----
CLI:
    mix-models alps report --model MODEL --scenario SCENARIO

Python API:
    from message_ix_models.project.alps.report import (
        prepare_water_reporter,
        report_water_nexus,
    )

    rep, _ = prepare_water_reporter(scenario)
    df = report_water_nexus(scenario, output_dir="./output")
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import yaml
from message_ix import Reporter, Scenario

from message_ix_models.util import package_data_path


def _load_elec_consuming_techs() -> dict[str, list[str]]:
    """Load techs from technology.yaml that consume electricity.

    Returns
    -------
    dict with keys:
      - 'cooling_parasitic': techs ending in __cl_fresh or __air
      - 'water_infra': nexus techs (urban_recycle, membrane, etc.)
    """
    tech_yaml = package_data_path("water", "technology.yaml")
    with open(tech_yaml) as f:
        data = yaml.safe_load(f)

    cooling_elec = []
    water_elec = []

    # Cooling section: check input for electr
    for tech, spec in data.get("cooling", {}).items():
        if spec is None:
            continue
        inp = spec.get("input", {})
        if inp and "electr" in str(inp):
            cooling_elec.append(tech)

    # Nexus section: check input for electr
    for tech, spec in data.get("nexus", {}).items():
        if spec is None:
            continue
        inp = spec.get("input", {})
        if inp and "electr" in str(inp):
            water_elec.append(tech)

    return {"cooling_parasitic": cooling_elec, "water_infra": water_elec}

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger(__name__)

__all__ = [
    "prepare_water_reporter",
    "report_water_nexus",
    "water_report_callback",
]

# Aggregated technology groups (must match water_cooling.yaml)
COOLING_TYPES = [
    "Cooling|Once-through Fresh",
    "Cooling|Closed-loop Fresh",
    "Cooling|Air",
    "Cooling|Once-through Saline",
]

EXTRACTION_TYPES = [
    "Extraction|Surface Water",
    "Extraction|Groundwater",
    "Extraction|Fossil Groundwater",
]

DESALINATION_TYPES = [
    "Desalination|Membrane",
    "Desalination|Distillation",
]

# Config for standard report extractions: (key, genno_key, prefix, unit, time_col, filter_types)
STANDARD_REPORT_CONFIGS = (
    ("cooling_cap", "CAP:nl-t-ya:cool_cap", "Capacity|Electricity|Cooling", "GW", None, COOLING_TYPES),
    ("cooling_act", "ACT:nl-t-ya-h:cool_act", "Activity|Electricity|Cooling", "GWa", "h", COOLING_TYPES),
    ("water_cap", "water_extract_cap:nl-t-ya:water_cap", "Capacity|Water|Extraction", "MCM", None, EXTRACTION_TYPES),
    ("water_act", "water_extract_act:nl-t-ya-h:water_act", "Activity|Water|Extraction", "MCM", "h", EXTRACTION_TYPES),
    ("desal_cap", "desal_cap:nl-t-ya:desal_cap", "Capacity|Water|Desalination", "MCM", None, DESALINATION_TYPES),
    ("desal_act", "desal_act:nl-t-ya-h:desal_act", "Activity|Water|Desalination", "MCM", "h", DESALINATION_TYPES),
)


def _get_config_path() -> Path:
    """Return path to water_cooling.yaml config."""
    return package_data_path("alps", "report", "water_cooling.yaml")


def _detect_temporal_resolution(scenario: Scenario) -> str:
    """Detect if scenario uses seasonal (h1/h2) or annual resolution."""
    time_set = scenario.set("time")
    if len(time_set) > 1 or (len(time_set) == 1 and time_set.iloc[0] != "year"):
        return "seasonal"
    return "annual"


def prepare_water_reporter(
    scenario: Scenario,
    reporter: Optional[Reporter] = None,
) -> tuple[Reporter, str]:
    """Prepare a Reporter with ALPS water/cooling computations.

    Parameters
    ----------
    scenario : Scenario
        Solved MESSAGE scenario with water-nexus/cooling module.
    reporter : Reporter, optional
        Existing reporter to extend. If None, creates new from scenario.

    Returns
    -------
    tuple[Reporter, str]
        Reporter configured with water/cooling computations, and temporal resolution.
    """
    if reporter is None:
        rep = Reporter.from_scenario(scenario)
    else:
        rep = reporter

    config_path = _get_config_path()
    if config_path.exists():
        log.info(f"Loading ALPS water/cooling report config from {config_path}")
        rep.configure(path=config_path)
    else:
        log.warning(f"Config not found at {config_path}, using defaults")

    temporal_res = _detect_temporal_resolution(scenario)
    log.info(f"Detected temporal resolution: {temporal_res}")

    # Add programmatic electricity consumption keys (tech lists from technology.yaml)
    elec_techs = _load_elec_consuming_techs()
    log.info(
        f"Loaded {len(elec_techs['cooling_parasitic'])} cooling + "
        f"{len(elec_techs['water_infra'])} water infra electricity-consuming techs"
    )

    from genno import Key

    # Full input key with all dimensions
    in_key = Key("in:nl-t-yv-ya-m-no-c-l-h-ho")

    # Cooling parasitic electricity (regional)
    rep.add(
        Key("cooling_elec:nl-t-yv-ya-m-c-l-h"),
        "select",
        in_key,
        indexers={"c": ["electr"], "t": elec_techs["cooling_parasitic"]},
    )

    # Water infrastructure electricity (basin-level)
    rep.add(
        Key("water_infra_elec:nl-t-yv-ya-m-c-l-h"),
        "select",
        in_key,
        indexers={"c": ["electr"], "t": elec_techs["water_infra"]},
    )

    return rep, temporal_res


def water_report_callback(rep: Reporter, context) -> None:
    """Callback for registering water reporting with main reporter."""
    config_path = _get_config_path()
    if config_path.exists():
        rep.configure(path=config_path)
        log.info("Registered ALPS water/cooling reporting")


def _package_genno_result(
    result,
    variable_prefix: str,
    unit: str,
    region_col: str = "nl",
    year_col: str = "ya",
    time_col: Optional[str] = None,
    type_col: str = "t",
    value_col: Optional[str] = None,
    filter_types: Optional[list] = None,
) -> "pd.DataFrame":
    """Convert genno AttrSeries to DataFrame with standard columns.

    This is pure packaging - no value computation.
    """
    import pandas as pd

    df = result.to_dataframe().reset_index()

    # Auto-detect value column if not specified
    if value_col is None:
        # Last column that's not an index is typically the value
        possible_vals = ["CAP", "ACT", "demand", "capacity_factor"]
        value_col = next((c for c in possible_vals if c in df.columns), df.columns[-1])

    # Filter to aggregated types if specified
    if filter_types and type_col in df.columns:
        df = df[df[type_col].isin(filter_types)]

    # Build variable name
    if type_col in df.columns:
        df["variable"] = variable_prefix + "|" + df[type_col].astype(str)
    else:
        df["variable"] = variable_prefix

    # Rename columns
    rename_map = {region_col: "region", year_col: "year", value_col: "value"}
    if time_col and time_col in df.columns:
        rename_map[time_col] = "subannual"
    df = df.rename(columns=rename_map)

    df["unit"] = unit

    # Select output columns
    cols = ["region", "variable", "year"]
    if "subannual" in df.columns:
        cols.append("subannual")
    cols.extend(["value", "unit"])

    return df[[c for c in cols if c in df.columns]]


def report_water_nexus(
    scenario: Scenario,
    output_dir: Optional[Union[str, Path]] = None,
    keys: Optional[list[str]] = None,
    format: str = "csv",
) -> "pd.DataFrame":
    """Run water/nexus report and optionally save results.

    All value computation via genno. Python handles only packaging.

    Parameters
    ----------
    scenario : Scenario
        Solved MESSAGE scenario with water-nexus/cooling module.
    output_dir : Path, optional
        Directory to save output files. If None, returns DataFrame only.
    keys : list[str], optional
        Specific report keys to compute. Options:
        - "cooling_cap": Cooling capacity by type
        - "cooling_act": Cooling activity by type (seasonal)
        - "cooling_elec": Cooling parasitic electricity consumption
        - "water_cap": Water extraction capacity
        - "water_act": Water extraction activity (seasonal)
        - "water_avail": Water availability by basin
        - "water_infra_elec": Water infrastructure electricity consumption
        - "water_demand": Final water demands by sector
        - "irrigation": Irrigation water demands by crop type
        If None, computes all keys.
    format : str
        Output format: "csv", "xlsx", or "parquet" (default: "csv").

    Returns
    -------
    pd.DataFrame
        Report data with columns: model, scenario, region, variable,
        year, [subannual], value, unit
    """
    import pandas as pd

    rep, temporal_res = prepare_water_reporter(scenario)

    results = []
    report_keys = keys or [
        "cooling_cap", "cooling_act", "cooling_elec",
        "water_cap", "water_act", "desal_cap", "desal_act",
        "water_avail", "water_infra_elec", "water_demand", "irrigation",
    ]

    # Standard report extractions (cooling, water extraction, desalination)
    for key, genno_key, prefix, unit, time_col, filter_types in STANDARD_REPORT_CONFIGS:
        if key in report_keys:
            log.info(f"Extracting {key}...")
            try:
                result = rep.get(genno_key)
                df = _package_genno_result(
                    result,
                    variable_prefix=prefix,
                    unit=unit,
                    time_col=time_col,
                    filter_types=filter_types,
                )
                results.append(df)
            except Exception as e:
                log.warning(f"Could not extract {key}: {e}")

    # Water availability (basin-level)
    if "water_avail" in report_keys:
        log.info("Extracting water availability...")
        try:
            result = rep.get("water_avail:n-c-l-y-h")
            df = result.to_dataframe().reset_index()
            # Custom packaging for water availability
            df["variable"] = "Water Availability|" + df["c"].str.replace("_basin", "")
            df = df.rename(columns={"n": "region", "y": "year", "h": "subannual", "demand": "value"})
            df["value"] = -df["value"]  # Flip sign: negative demand = positive supply
            df["unit"] = "MCM"
            df = df[["region", "variable", "year", "subannual", "value", "unit"]]
            results.append(df)
        except Exception as e:
            log.warning(f"Could not extract water availability: {e}")

    # Cooling parasitic electricity (regional)
    if "cooling_elec" in report_keys:
        log.info("Extracting cooling parasitic electricity...")
        try:
            result = rep.get("cooling_elec:nl-t-yv-ya-m-c-l-h")
            df = result.to_dataframe().reset_index()
            # Sum over vintage years and modes, group by tech
            df = df.groupby(["nl", "t", "ya", "h"])["value"].sum().reset_index()
            df["variable"] = "Electricity|Cooling|" + df["t"].astype(str)
            df = df.rename(columns={"nl": "region", "ya": "year", "h": "subannual"})
            df["unit"] = "GWa"
            df = df[["region", "variable", "year", "subannual", "value", "unit"]]
            results.append(df)
        except Exception as e:
            log.warning(f"Could not extract cooling electricity: {e}")

    # Water infrastructure electricity (basin-level)
    if "water_infra_elec" in report_keys:
        log.info("Extracting water infrastructure electricity...")
        try:
            result = rep.get("water_infra_elec:nl-t-yv-ya-m-c-l-h")
            df = result.to_dataframe().reset_index()
            # Sum over vintage years and modes, group by tech
            df = df.groupby(["nl", "t", "ya", "h"])["value"].sum().reset_index()
            df["variable"] = "Electricity|Water|" + df["t"].astype(str)
            df = df.rename(columns={"nl": "region", "ya": "year", "h": "subannual"})
            df["unit"] = "GWa"
            df = df[["region", "variable", "year", "subannual", "value", "unit"]]
            results.append(df)
        except Exception as e:
            log.warning(f"Could not extract water infrastructure electricity: {e}")

    # Final water demands (basin-level)
    if "water_demand" in report_keys:
        log.info("Extracting final water demands...")
        try:
            result = rep.get("water_demand_final:n-c-l-y-h")
            df = result.to_dataframe().reset_index()
            df["variable"] = "Demand|Water|" + df["c"].astype(str)
            df = df.rename(columns={"n": "region", "y": "year", "h": "subannual", "demand": "value"})
            df["unit"] = "MCM"
            df = df[["region", "variable", "year", "subannual", "value", "unit"]]
            results.append(df)
        except Exception as e:
            log.warning(f"Could not extract water demands: {e}")

    # Irrigation water demands (from land_input parameter)
    if "irrigation" in report_keys:
        log.info("Extracting irrigation demands...")
        try:
            result = rep.get("irrigation_demand:n-c-l-y")
            df = result.to_dataframe().reset_index()
            # Map level to crop type for variable naming
            level_map = {
                "irr_cereal": "Cereals",
                "irr_oilcrops": "Oilcrops",
                "irr_sugarcrops": "Sugarcrops",
            }
            df["variable"] = "Demand|Water|Irrigation|" + df["l"].map(level_map)
            df = df.rename(columns={"n": "region", "y": "year", "land_input": "value"})
            df["unit"] = "MCM"
            df = df[["region", "variable", "year", "value", "unit"]]
            results.append(df)
        except Exception as e:
            log.warning(f"Could not extract irrigation demands: {e}")

    if not results:
        log.warning("No data extracted")
        return pd.DataFrame()

    # Combine results (pandas concat - packaging only)
    df = pd.concat(results, ignore_index=True)

    # Add model/scenario metadata
    df["model"] = scenario.model
    df["scenario"] = scenario.scenario

    # Reorder columns
    cols = ["model", "scenario", "region", "variable", "unit", "year"]
    if "subannual" in df.columns:
        cols.append("subannual")
    cols.append("value")
    df = df[[c for c in cols if c in df.columns]]

    # Save if output_dir provided
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        filename = f"{scenario.model}_{scenario.scenario}_water_nexus"

        if format == "csv":
            outfile = output_path / f"{filename}.csv"
            df.to_csv(outfile, index=False)
        elif format == "xlsx":
            outfile = output_path / f"{filename}.xlsx"
            df.to_excel(outfile, index=False)
        elif format == "parquet":
            outfile = output_path / f"{filename}.parquet"
            df.to_parquet(outfile, index=False)
        else:
            raise ValueError(f"Unknown format: {format}")

        log.info(f"Saved report to {outfile}")

    return df


# Legacy function name for backwards compatibility
report_water_cooling = report_water_nexus
