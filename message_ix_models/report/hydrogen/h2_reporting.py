from typing import List

import message_ix
import pandas as pd
import pyam
from iam_units import registry
from message_ix.report import Reporter

from message_ix_models.util import broadcast

from .config import Config


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
    df_var = pd.DataFrame(rep.get(f"{reporter_var}:nl-t-ya-m-c-l"))

    # Use join to merge data - this allows partial index matching
    # (e.g. emissions only need t,m but output needs t,m,c,l)
    df = (
        df_var.join(mapping_df[["iamc_name", "unit", "original_unit"]])
        .dropna()
        .groupby(["nl", "ya", "iamc_name", "original_unit"])
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

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'original_unit' in index and 'value' column
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

    df = (
        df.reset_index()
        .rename(columns={"iamc_name": "variable", "nl": "region", "ya": "Year"})
        .assign(
            variable=lambda x: variable_prefix + x["variable"],
            Model=model_name,
            Scenario=scenario_name,
            Unit=unit,  # Set target unit
        )
        .drop(columns=["original_unit"])  # Remove original_unit column
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
    """Generate reporting for any given variable."""
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
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
