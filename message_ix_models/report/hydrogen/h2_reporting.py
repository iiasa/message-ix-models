from typing import List

import message_ix
import pandas as pd
import pyam
from iam_units import registry
from message_ix.report import Reporter

from message_ix_models.util import broadcast

from .config import Config

LOG = logging.getLogger(__name__)


def ensure_historical_keys(rep: Reporter) -> None:
    """Ensure historical reporter keys exist.

    Creates 'out_hist' and 'emi_hist' keys if they don't already exist.

    - out_hist = output × historical_activity (for historical production data)
    - emi_hist = emission_factor × historical_activity (for historical emissions)
      Note: emi (model emissions) = emission_factor × ACT
            emi_hist (historical emissions) = emission_factor × historical_activity

    Only creates keys if the base keys are available in the reporter.
    """
    # Check what keys are available - need to check for keys with any dimension combo
    # Keys in reporter often have dimensions like "historical_activity:nl-t-ya-m"
    # So we need to check if the base key name exists (before the colon)
    has_output = any("output" == str(k).split(":")[0] for k in rep.keys())
    has_historical_activity = any(
        "historical_activity" == str(k).split(":")[0] for k in rep.keys()
    )
    has_emission_factor = any(
        "emission_factor" == str(k).split(":")[0] for k in rep.keys()
    )

    # Check if historical keys already exist
    has_out_hist = any("out_hist" == str(k).split(":")[0] for k in rep.keys())
    has_emi_hist = any("emi_hist" == str(k).split(":")[0] for k in rep.keys())

    # Check if base keys exist with dimensions
    try:
        if not has_out_hist and has_output and has_historical_activity:
            rep.add("out_hist", "mul", "output", "historical_activity")
        elif not has_out_hist:
            LOG.warning(
                "Cannot create out_hist: output=%s historical_activity=%s",
                has_output,
                has_historical_activity,
            )
    except Exception as exc:
        LOG.error("Error creating out_hist key: %s", exc)

    try:
        if not has_emi_hist and has_emission_factor and has_historical_activity:
            rep.add("emi_hist", "mul", "emission_factor", "historical_activity")
        elif not has_emi_hist:
            LOG.warning(
                "Cannot create emi_hist: emission_factor=%s historical_activity=%s",
                has_emission_factor,
                has_historical_activity,
            )
    except Exception as exc:
        LOG.error("Error creating emi_hist key: %s", exc)


# Cache attribute name for storing first model year on Reporter
_FIRST_MODEL_YEAR_ATTR = "_h2_first_model_year"


def get_first_model_year(rep: Reporter) -> Optional[int]:
    """Return the first model year defined in cat_year."""
    cached = getattr(rep, _FIRST_MODEL_YEAR_ATTR, None)
    if cached is not None:
        return cached
    try:
        df = rep.get("cat_year")
        fm = df.loc[df["type_year"] == "firstmodelyear", "year"].astype(int).min()
        setattr(rep, _FIRST_MODEL_YEAR_ATTR, fm)
        return fm
    except Exception:
        LOG.warning("Could not determine first model year from cat_year")
        setattr(rep, _FIRST_MODEL_YEAR_ATTR, None)
        return None


def pyam_df_from_rep(
    rep: message_ix.Reporter, reporter_var: str, mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """Queries data from Reporter and maps to IAMC variable names.

    Parameters
    ----------
    rep
        message_ix.Reporter to query
    reporter_var
        Registered key of Reporter to query, e.g. "out", "in", "ACT", "emi", "CAP"
    mapping_df
        DataFrame mapping Reporter dimension values to IAMC variable names
    """
    filters_dict = {
        col: list(mapping_df.index.get_level_values(col).unique())
        for col in mapping_df.index.names
    }
    rep.set_filters(**filters_dict)
    df_var = pd.DataFrame(rep.get(f"{reporter_var}:nl-t-ya-m-c-l-e"))

    # Use join to merge data - this allows partial index matching
    # (e.g. emissions only need t,m but output needs t,m,c,l)
    df = (
        df_var.join(
            mapping_df[["iamc_name", "unit", "original_unit", "stoichiometric_factor"]]
        )
        .dropna()
        .groupby(["nl", "ya", "iamc_name", "original_unit", "stoichiometric_factor"])
        .sum(numeric_only=True)
    )
    rep.set_filters()
    return df


def _load_unit_conversions() -> dict:
    """Load unit conversion factors from YAML file.

    Returns
    -------
    dict
        Dictionary mapping (source_unit, target_unit) tuples to conversion factors
    """
    import yaml
    from message_ix_models.util import package_data_path

    try:
        path = package_data_path("hydrogen", "reporting", "unit_conversions.yaml")
        with open(path) as f:
            data = yaml.safe_load(f)

        # Convert the YAML format to the expected dictionary format
        conversions = {}
        for key, factor in data.get("conversions", {}).items():
            # Parse keys like "GWa_to_EJ/yr" into ("GWa", "EJ/yr")
            if "_to_" in key:
                source_unit, target_unit = key.split("_to_", 1)
                conversions[(source_unit, target_unit)] = factor

        return conversions

    except Exception as e:
        import logging

        log = logging.getLogger(__name__)
        log.warning(f"Could not load unit conversions from YAML: {e}")
        return {}


def convert_units_from_mapping(df: pd.DataFrame, target_unit: str) -> pd.DataFrame:
    """Convert units in DataFrame using iam_units.registry based on original_unit column.

    Also applies stoichiometric factors if present in the index, which are used to convert
    the output commodity (e.g., ammonia) to hydrogen content after unit conversion.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'original_unit' and optionally 'stoichiometric_factor' in index and 'value' column
    target_unit : str
        Target unit to convert to

    Returns
    -------
    pd.DataFrame
        DataFrame with converted values
    """
    if "original_unit" not in df.index.names:
        # No unit conversion needed if original_unit is not in the index
        return df

    # Create a copy to avoid modifying the original
    df_converted = df.copy()

    # Get unique original units from the index
    original_units = df.reset_index()["original_unit"].unique()

    # Load conversion factors from YAML file
    yaml_conversions = _load_unit_conversions()

    for orig_unit in original_units:
        if orig_unit == target_unit:
            # No conversion needed
            continue

        # Create mask for rows with this original unit
        mask = df.reset_index()["original_unit"] == orig_unit
        indices = df.reset_index()[mask].set_index(df.index.names).index

        # Get the values to convert
        values_to_convert = df.loc[indices, "value"].values

        # Check if we have a YAML-defined conversion first
        conversion_key = (orig_unit, target_unit)
        if conversion_key in yaml_conversions:
            factor = yaml_conversions[conversion_key]
            df_converted.loc[indices, "value"] = values_to_convert * factor
            continue

        # Try using iam_units.registry for conversion
        try:
            converted_quantity = registry.Quantity(values_to_convert, orig_unit).to(
                target_unit
            )

            # Update the values in the DataFrame
            df_converted.loc[indices, "value"] = converted_quantity.magnitude

        except Exception as e:
            # Log the error but continue processing
            import logging

            log = logging.getLogger(__name__)
            log.warning(f"Could not convert from {orig_unit} to {target_unit}: {e}")
            # Keep original values if conversion fails
            continue

    # Apply stoichiometric factors if present (after unit conversion)
    # This converts the output commodity (e.g., ammonia in EJ) to hydrogen content (EJ H2)
    if "stoichiometric_factor" in df_converted.index.names:
        df_reset = df_converted.reset_index()
        # Apply stoichiometric factor to each row
        df_reset["value"] = df_reset["value"] * df_reset["stoichiometric_factor"]
        df_converted = df_reset.set_index(df_converted.index.names)

    return df_converted


def format_reporting_df(
    df: pd.DataFrame,
    variable_prefix: str,
    model_name: str,
    scenario_name: str,
    unit: str,
    mappings,
) -> pyam.IamDataFrame:
    """Formats a DataFrame created with :func:pyam_df_from_rep to pyam.IamDataFrame."""
    df.columns = ["value"]

    # Apply unit conversions using iam_units.registry
    df = convert_units_from_mapping(df, unit)

    # Prepare list of columns to drop
    cols_to_drop = ["original_unit"]
    if "stoichiometric_factor" in df.index.names:
        cols_to_drop.append("stoichiometric_factor")

    df = (
        df.reset_index()
        .rename(columns={"iamc_name": "variable", "nl": "region", "ya": "Year"})
        .assign(
            variable=lambda x: variable_prefix + x["variable"],
            region=lambda x: x["region"].str.replace(
                "R12_", "", regex=False
            ),  # Remove R12_ prefix
            Model=model_name,
            Scenario=scenario_name,
            Unit=unit,  # Set target unit
        )
        .drop(
            columns=cols_to_drop
        )  # Remove original_unit and stoichiometric_factor columns
        .groupby(
            ["Model", "Scenario", "region", "variable", "Year", "Unit"], dropna=False
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    py_df = pyam.IamDataFrame(df)

    if py_df.empty:
        return py_df
    missing = [
        variable_prefix + i
        for i in mappings.iamc_name.unique().tolist()
        if i not in [i.replace(variable_prefix, "") for i in py_df.variable]
    ]
    if missing:
        zero_ts = pyam.IamDataFrame(
            pd.DataFrame()
            .assign(
                variable=missing,
                region=None,
                unit=unit,
                value=0,
                scenario=scenario_name,
                model=model_name,
                year=None,
            )
            .pipe(broadcast, region=py_df.region, year=py_df.year)
        )
        py_df = pyam.concat([py_df, zero_ts])
    return py_df


def compute_aggregates_from_iamc(
    df: pyam.IamDataFrame, aggregates: dict, iamc_prefix: str, short_to_iamc: dict
) -> pyam.IamDataFrame:
    """Compute aggregate variables by summing already-processed IAMC variables.

    This function aggregates variables at the IAMC level (after unit conversion and
    stoichiometric factor application), rather than re-querying raw MESSAGE data.
    This ensures that variables with different stoichiometric factors (e.g., methanol
    from different feedstocks) are correctly summed.

    Parameters
    ----------
    df : pyam.IamDataFrame
        DataFrame with computed leaf variables (after unit conversion and
        stoichiometric factor application)
    aggregates : dict
        Aggregate definitions from Config.get_aggregate_definitions()
        Structure: {level: {iamc_name: {"short": str, "components": list}}}
    iamc_prefix : str
        IAMC variable prefix (e.g., "Production|" or "Production|Hydrogen|")
    short_to_iamc : dict
        Mapping from short_name to IAMC variable name (fragment after prefix)

    Returns
    -------
    pyam.IamDataFrame
        Combined DataFrame with both leaf variables and computed aggregates
    """
    if not aggregates:
        return df

    # Convert to pandas for easier manipulation
    df_work = df.as_pandas().copy()

    # Build reverse mapping: short_name -> full variable name
    short_to_full_var = {
        short: iamc_prefix + iamc_name for short, iamc_name in short_to_iamc.items()
    }

    # Process aggregates level by level to handle hierarchical aggregation
    for level_key in sorted(
        aggregates.keys()
    ):  # Process in order: level_1, level_2, etc.
        level_aggregates = aggregates[level_key]
        level_rows = []

        for iamc_name, agg_def in level_aggregates.items():
            components = agg_def["components"]
            short_name = agg_def["short"]

            # Find component variables in the DataFrame
            component_vars = []
            for comp_short in components:
                if comp_short in short_to_full_var:
                    component_vars.append(short_to_full_var[comp_short])

            if not component_vars:
                # No components found - skip this aggregate
                continue

            # Filter dataframe for these component variables
            df_components = df_work[df_work["variable"].isin(component_vars)]

            if df_components.empty:
                continue

            # Sum the components grouped by model, scenario, region, year, unit
            df_agg = (
                df_components.groupby(["model", "scenario", "region", "year", "unit"])
                .agg({"value": "sum"})
                .reset_index()
            )

            # Assign the aggregate variable name
            full_var_name = iamc_prefix + iamc_name
            df_agg["variable"] = full_var_name

            # Add to this level's collection
            level_rows.append(df_agg)

            # Register this aggregate for use in higher-level aggregates
            short_to_full_var[short_name] = full_var_name

        # Add this level's aggregates to df_work so they're available for next level
        if level_rows:
            df_work = pd.concat([df_work] + level_rows, ignore_index=True)

    # Return the combined dataframe with all leaves and aggregates
    return pyam.IamDataFrame(df_work)


def load_config(name: str) -> "Config":
    """Load a config for a given reporting variable category from the YAML files.

    This is a thin wrapper around :meth:`.Config.from_files`.
    """
    return Config.from_files(name)


def run_h2_fgt_reporting(
    rep: Reporter, model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for industry hydrogen fugitive emissions."""
    var = "h2_fgt_emi"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    return py_df


def run_lh2_fgt_reporting(
    rep: Reporter, model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for industry liquefied hydrogen fugitive emissions."""
    var = "lh2_fgt_emi"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    return py_df


def run_lh2_prod_reporting(
    rep: Reporter, model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for liquefied hydrogen production."""
    var = "lh2_prod"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    return py_df


def run_h2_prod_reporting(
    rep: Reporter, model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for hydrogen production."""
    var = "h2_prod"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    return py_df


def run_reporting(
    var: str, rep: Reporter, model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for any given variable.

    This function now computes leaf variables first (applying unit conversion and
    stoichiometric factors), then aggregates them at the IAMC level to handle
    cases where different components have different stoichiometric factors.
    """
    config = load_config(var)

    # Get leaf variables only (config.mapping only contains leaves now)
    df = pyam_df_from_rep(rep, config.var, config.mapping)

    # Format and convert units/factors for leaf variables
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )

    # Build mapping from short_name to iamc_name for aggregation
    short_to_iamc = (
        config.mapping.reset_index()[["short_name", "iamc_name"]]
        .drop_duplicates()
        .set_index("short_name")["iamc_name"]
        .to_dict()
    )

    # Compute aggregates from processed IAMC variables
    aggregates = config.get_aggregate_definitions()
    if aggregates:
        py_df = compute_aggregates_from_iamc(
            py_df, aggregates, config.iamc_prefix, short_to_iamc
        )

    return py_df


def fetch_variables() -> List[str]:
    """Fetch all variables from the data/hydrogen/reporting directory."""
    import os
    import glob
    from message_ix_models.util import package_data_path

    path = package_data_path("hydrogen", "reporting")
    variables = [f.stem for f in path.glob("*.yaml") if f.stem != "unit_conversions"]
    # we need to remove the _aggregates files
    variables = [var for var in variables if not var.endswith("_aggregates")]
    return variables


def run_h2_reporting(
    rep: Reporter, model_name: str, scen_name: str, add_world: bool = True
) -> pyam.IamDataFrame:
    """Generate all hydrogen reporting variables for a given scenario.

    This includes:
    - Hydrogen production by technology and fuel type
    - H2 fugitive emissions across the supply chain
    - LH2 fugitive emissions

    All variables include aggregated totals as defined in the reporting configuration files.

    Parameters
    ----------
    rep
        message_ix.Reporter to query
    model_name
        Name of the model
    scen_name
        Name of the scenario
    add_world
        If True, add World region as sum of all regions (default: True)

    Returns
    -------
    pyam.IamDataFrame
        Combined dataframe with all hydrogen reporting variables
    """
    variables = fetch_variables()
    print(f"Fetched variables: {variables}")
    dfs = [run_reporting(var, rep, model_name, scen_name) for var in variables]

    # Concatenate all dataframes
    py_df = pyam.concat(dfs)

    # Add World region as sum of all other regions
    if add_world:
        py_df.aggregate_region(py_df.variable, region="World", append=True)

    return py_df
