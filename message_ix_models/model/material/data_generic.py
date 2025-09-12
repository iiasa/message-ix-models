"""
Data and parameter generation for generic furnace and boiler technologies in
MESSAGEix-Materials model.

This module provides functions to read, process, and generate parameter data for generic
furnace and boiler technologies, including emission coefficients and generic relations.
"""

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, List

import pandas as pd
from message_ix import Scenario, make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

from .data_util import read_timeseries
from .util import read_config

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import ParameterData


def read_data_generic(scenario: "Scenario") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read and clean data from the generic furnace and boiler techno-economic file.

    Parameters
    ----------
    scenario : Scenario
        Scenario instance to build generic technologies on.

    Returns
    -------
    tuple of pd.DataFrame
        - DataFrame with static techno-economic data.
        - DataFrame with time-series data.
    """
    # Read the file
    data_generic = pd.read_excel(
        package_data_path(
            "material", "other", "generic_furnace_boiler_techno_economic.xlsx"
        ),
        sheet_name="generic",
    )

    # Clean the data
    # Drop columns that don't contain useful information
    data_generic = data_generic.drop(["Region", "Source", "Description"], axis=1)
    data_generic_ts = read_timeseries(
        scenario, "other", None, "generic_furnace_boiler_techno_economic.xlsx"
    )

    # Unit conversion
    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_generic, data_generic_ts


def add_non_co2_emission_coefficients(
    scen: "Scenario", df_input: pd.DataFrame, method="from_disk"
):
    """Derive non-CO2 emission coefficients for furnaces.

    Parameters
    ----------
    scen : Scenario
        Scenario instance.
    df_input : pd.DataFrame
        DataFrame of furnace input parameters.
    method : str, optional
        Method to obtain emission coefficients ("from_disk" or "from_scenario").

    Returns
    -------
    pd.DataFrame
        DataFrame with non-CO2 emission coefficients for furnaces.
    """
    if method == "from_disk":
        df_emi = pd.read_csv(
            package_data_path(
                "material", "other", "industry_thermal_emi_coefficients.csv"
            )
        )
    else:
        df_emi = get_thermal_industry_emi_coefficients(scen)
    df_input = df_input[df_input["year_act"].ge(scen.firstmodelyear)]
    df_input = df_input.set_index(["node_loc", "year_act", "commodity", "technology"])
    df_furnace_emi = calculate_furnace_non_co2_emi_coeff(df_input, df_emi)
    return df_furnace_emi


def add_ind_therm_link_relations(tecs: List[str], years: List[str], nodes: List[str]):
    """Generate industrial thermal demand link relation values for given technologies.

    Parameters
    ----------
    tecs
        List of technology names.
    years
        List of years for relation activity.
    nodes
        List of model nodes.

    Returns
    -------
    pd.DataFrame
        DataFrame with relation_activity for industrial thermal demand links.
    """
    col_val_dict = {
        "relation": "IndThermDemLink",
        "mode": ["high_temp", "low_temp"],
        "unit": "???",
        "value": 1,
    }
    df = (
        make_df("relation_activity", **col_val_dict)
        .pipe(broadcast, node_loc=nodes)
        .pipe(broadcast, year_rel=years)
        .pipe(same_node)
        .pipe(broadcast, technology=tecs)
    )
    df["year_act"] = df["year_rel"]
    df = df[
        ~(
            (
                (df["technology"].str.startswith("solar"))
                | (df["technology"].str.startswith("fc_h2_"))
                | (df["technology"].str.startswith("furnace_h2"))
                | (df["technology"].str.startswith("dheat"))
            )
            & (df["year_act"].isin([2020, 2025]))
        )
    ]
    return df


def gen_data_generic(scenario: "Scenario", dry_run: bool = False) -> "ParameterData":
    """Generate all parameter data for sectoral furnace and boiler technologies.

    Parameters
    ----------
    scenario
        Scenario instance to build generic technologies on.
    dry_run
        If True, do not perform any file writing or scenario modification.

    Returns
    -------
    dict[str, pd.DataFrame]
        Dictionary with MESSAGEix parameters as keys and parametrization as values.
    """
    # Load configuration

    config = read_config()["material"]["generic"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_generic, data_generic_ts = read_data_generic(scenario)

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    modelyears = s_info.Y  # s_info.Y is only for modeling years
    yv_ya = s_info.yv_ya

    # Do not parametrize GLB region the same way
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]
    for t in config["technology"]["add"]:
        t = t.id

        # Select the subset of data associated with this technology
        # TODO Use `for data_generic.groupby("technology")` above
        group_df = data_generic.query("technology == @t")
        if group_df.empty:
            log.warning(f"No data for {t!r}")
            continue

        # years = s_info.Y
        params = group_df["parameter"].values.tolist()

        # Availability year of the technology
        av = group_df["availability"].values[0]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[yv_ya.year_vtg >= av,]

        # Iterate over parameters
        for par in params:
            split = par.split("|")
            param_name = split[0]

            val = group_df.query("parameter == @par")["value"].values[0]

            # Common parameters for all input and output tables
            # year_act is none at the moment
            # node_dest and node_origin are the same as node_loc

            common = dict(
                year_vtg=yva.year_vtg,
                year_act=yva.year_act,
                time="year",
                time_origin="year",
                time_dest="year",
            )

            if len(split) > 1:
                if (param_name == "input") | (param_name == "output"):
                    com = split[1]
                    lev = split[2]
                    mod = split[3]

                    df = (
                        make_df(
                            param_name,
                            technology=t,
                            commodity=com,
                            level=lev,
                            mode=mod,
                            value=val,
                            unit="t",
                            **common,
                        )
                        .pipe(broadcast, node_loc=nodes)
                        .pipe(same_node)
                    )

                    results[param_name].append(df)

                elif param_name == "emission_factor":
                    emi = split[1]

                    # TODO: Now tentatively fixed to one mode.
                    #  Have values for the other mode too
                    df_low = make_df(
                        param_name,
                        technology=t,
                        value=val,
                        emission=emi,
                        mode="low_temp",
                        unit="t",
                        **common,
                    ).pipe(broadcast, node_loc=nodes)

                    df_high = make_df(
                        param_name,
                        technology=t,
                        value=val,
                        emission=emi,
                        mode="high_temp",
                        unit="t",
                        **common,
                    ).pipe(broadcast, node_loc=nodes)

                    results[param_name].append(df_low)
                    results[param_name].append(df_high)

            # Rest of the parameters apart from input, output and emission_factor

            else:
                df = make_df(
                    param_name, technology=t, value=val, unit="t", **common
                ).pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    gen_data_generic_ts(data_generic_ts, results, nodes, global_region)

    results["relation_activity"].append(
        add_non_co2_emission_coefficients(scenario, pd.concat(results["input"]))
    )
    results["relation_activity"].append(
        add_ind_therm_link_relations(
            [t.id for t in config["technology"]["add"]],
            yv_ya["year_act"].unique(),
            nodes,
        )
    )
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}
    reduced_pdict = {}
    for k, v in results.items():
        if {"year_act", "year_vtg"}.issubset(v.columns):
            v = v[(v["year_act"] - v["year_vtg"]) <= 25]
        reduced_pdict[k] = v.drop_duplicates().copy(deep=True)
    return reduced_pdict


def gen_data_generic_ts(
    data_generic_ts: pd.DataFrame, results, nodes: List[str], global_region: str
) -> None:
    tec_ts = set(data_generic_ts.technology)  # set of tecs in timeseries sheet

    for t in tec_ts:
        common = dict(
            time="year",
            time_origin="year",
            time_dest="year",
        )

        param_name = data_generic_ts.loc[
            (data_generic_ts["technology"] == t), "parameter"
        ]

        for p in set(param_name):
            val = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "value",
            ]
            regions = data_generic_ts.loc[
                (
                    (data_generic_ts["technology"] == t)
                    & (data_generic_ts["parameter"] == p)
                ),
                "region",
            ]
            # units = data_generic_ts.loc[
            #     (data_generic_ts["technology"] == t)
            #     & (data_generic_ts["parameter"] == p),
            #     "units",
            # ].values[0]
            mod = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "mode",
            ]
            yr = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "year",
            ]

            if p == "var_cost":
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    **common,
                ).pipe(broadcast, node_loc=nodes)
            else:
                rg = data_generic_ts.loc[
                    (data_generic_ts["technology"] == t)
                    & (data_generic_ts["parameter"] == p),
                    "region",
                ]
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    node_loc=rg,
                    **common,
                )

            # Copy parameters to all regions
            if (
                (len(set(regions)) == 1)
                and len(set(df["node_loc"])) == 1
                and list(set(df["node_loc"]))[0] != global_region
            ):
                df["node_loc"] = None
                df = df.pipe(broadcast, node_loc=nodes)

            results[p].append(df)


def get_thermal_industry_emi_coefficients(scen: "Scenario") -> pd.DataFrame:
    """Retrieve and normalize non-CO2 emission coefficients of ``i_therm`` technologies.

    Parameters
    ----------
    scen
        Scenario instance to retrieve emission coefficients from.

    Returns
    -------
    pd.DataFrame
        Normalized emission coefficients indexed by node, year, and technology.
    """
    ind_th_tecs = ["biomass_i", "coal_i", "eth_i", "foil_i", "gas_i", "loil_i"]
    relations = [
        "BCA_Emission",
        "CH4_Emission",
        "CO_Emission",
        "N2O_Emission",
        "NH3_Emission",
        "NOx_Emission",
        "OCA_Emission",
        "SO2_Emission",
        "VOC_Emission",
    ]
    first_year = 2020
    common_index = ["node_loc", "year_act", "technology"]
    df_rel = scen.par(
        "relation_activity", filters={"technology": ind_th_tecs, "relation": relations}
    )
    df_rel = df_rel[df_rel["year_act"].ge(first_year)]
    df_rel = df_rel.set_index(common_index)

    df_in = scen.par("input", filters={"technology": ind_th_tecs})
    df_in = df_in[df_in["year_act"].ge(first_year)]
    df_in = df_in.set_index(common_index)

    df_joined = df_rel.join(
        df_in[["value", "commodity"]], rsuffix="_in"
    ).drop_duplicates()
    df_joined["emi_factor"] = df_joined["value"] / df_joined["value_in"]
    return df_joined


def get_furnace_inputs(scen: "Scenario", first_year: int) -> pd.DataFrame:
    """Query input coefficients for furnace technologies from a Scenario instance.

    Parameters
    ----------
    scen
        Scenario instance to pull input parameter from.
    first_year
        Earliest year for which furnace input parameter should be retrieved.

    Returns
    -------
    pd.DataFrame
        Furnace input parameters indexed by node, year, commodity, and technology.
    """
    furn_tecs = "furnace"
    df_furn = scen.par("input")
    df_furn = df_furn[
        (df_furn["technology"].str.contains(furn_tecs))
        & (df_furn["year_act"].ge(first_year))
    ]
    df_furn = df_furn.set_index(["node_loc", "year_act", "commodity", "technology"])
    return df_furn


def calculate_furnace_non_co2_emi_coeff(
    df_furn: pd.DataFrame, df_emi: pd.DataFrame
) -> pd.DataFrame:
    """Scale emission factors of furnace technologies with input coefficients.

    ``input`` and ``CO2_ind`` ``relation_activity`` DataFrames are used to scale non-CO2
    emission factors for furnaces.

    Parameters
    ----------
    df_furn
        DataFrame containing input parametrization of furnaces.
    df_emi
        DataFrame containing input-normalized non-CO2 emission coefficients.

    Returns
    -------
    pd.DataFrame
        DataFrame with relation_activity for non-CO2 emissions from furnaces.
    """
    df_final = (
        pd.DataFrame(
            df_emi.reset_index().set_index(["node_loc", "year_act", "commodity"])[
                ["relation", "emi_factor"]
            ]
        )
        .join(
            df_furn["value"]
            .reset_index()
            .drop_duplicates()
            .set_index(["node_loc", "year_act", "commodity", "technology"])
        )
        .reset_index()
        .drop_duplicates()
    )
    df_final["value"] = df_final["value"] * df_final["emi_factor"]
    df_final_new = (
        make_df("relation_activity", **df_final)
        .pipe(same_node)
        .pipe(broadcast, mode=["high_temp", "low_temp"])
    )
    df_final_new["year_rel"] = df_final_new["year_act"]
    df_final_new["unit"] = "???"
    return df_final_new
