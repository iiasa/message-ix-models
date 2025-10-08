from typing import List

import message_ix
import pandas as pd
import pyam
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
        df_var.join(mapping_df[["iamc_name", "unit"]])
        .dropna()
        .groupby(["nl", "ya", "iamc_name"])
        .sum(numeric_only=True)
    )
    rep.set_filters()
    return df


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
    df = (
        df.reset_index()
        .rename(columns={"iamc_name": "variable", "nl": "region", "ya": "Year"})
        .assign(
            variable=lambda x: variable_prefix + x["variable"],
            Model=model_name,
            Scenario=scenario_name,
            Unit=unit,
        )
    )
    py_df = pyam.IamDataFrame(df)
    if unit == "EJ/yr":
        py_df.convert_unit("", to="GWa", factor=1, inplace=True)
        py_df.convert_unit("GWa", to="EJ/yr", factor=0.03154, inplace=True)

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
    dfs = [
        run_h2_prod_reporting(rep, model_name, scen_name),
        run_h2_fgt_reporting(rep, model_name, scen_name),
        run_lh2_fgt_reporting(rep, model_name, scen_name),
    ]

    # Concatenate all dataframes
    py_df = pyam.concat(dfs)

    # Add World region as sum of all other regions
    if add_world:
        py_df.aggregate_region(py_df.variable, region="World", append=True)

    return py_df
