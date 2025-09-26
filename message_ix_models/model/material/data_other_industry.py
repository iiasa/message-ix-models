"""
Data and parameter generation for other industry sectors in MESSAGEix models.

This module provides functions to read, process, and generate parameter data
for other industry technologies, demand, and related constraints, including
historical activity and demand adjustments based on IEA data.
"""

import os
from typing import TYPE_CHECKING, List

import pandas as pd
from message_ix.util import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import (
    map_iea_db_to_msg_regs,
    read_iea_tec_map,
)
from message_ix_models.model.material.util import get_ssp_from_context, read_config
from message_ix_models.util import merge_data, package_data_path

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import ParameterData


def get_hist_act_data(
    map_fname: str, iea_data_path: str, years: List[int] | None = None
) -> pd.DataFrame:
    """Reads IEA DB, maps and aggregates variables to MESSAGE technologies.

    Parameters
    ----------
    map_fname
        Name of MESSAGEix-technology-to-IEA-flow/product mapping file.
    years
        Years for which historical activity should be calculated and returned.
    iea_data_path
        Path to IEA EWEB parquet file.

    Returns
    -------
    pd.DataFrame
        Aggregated historical activity data mapped to MESSAGE technologies.
    """
    path = os.path.join(iea_data_path)
    iea_enb_df = pd.read_parquet(path, engine="fastparquet")
    if years:
        iea_enb_df = iea_enb_df[iea_enb_df["TIME"].isin(years)]

    # map IEA countries to MESSAGE region definition
    iea_enb_df = map_iea_db_to_msg_regs(iea_enb_df)

    # read file for IEA product/flow - MESSAGE technologies map
    MAP = read_iea_tec_map(map_fname)

    # map IEA flows to MESSAGE technologies and aggregate
    df_final = iea_enb_df.set_index(["PRODUCT", "FLOW"]).join(
        MAP.set_index(["PRODUCT", "FLOW"])
    )

    # multiply with efficiency and sector coverage ratios

    df_final = (
        df_final.drop_duplicates()
        .groupby(["REGION", "technology", "TIME"])
        .sum(numeric_only=True)
    )
    return df_final


def get_2020_industry_activity(years: List[int], iea_data_path: str) -> pd.DataFrame:
    """Calculate 2020 industry activity for MESSAGE technologies using IEA data.

    Parameters
    ----------
    years
        List of years for which activity should be calculated.
    iea_data_path
        Path to IEA EWEB parquet file.

    Returns
    -------
    pd.DataFrame
        DataFrame of industry activity for MESSAGE technologies.
    """
    df_mat = get_hist_act_data("industry.csv", iea_data_path=iea_data_path, years=years)
    df_chem = get_hist_act_data(
        "chemicals.csv", iea_data_path=iea_data_path, years=years
    )

    # scale chemical activity to deduct explicitly
    # represented activities of MESSAGEix-Materials
    # (67% are covered by NH3, HVCs and methanol)
    df_chem = df_chem.mul(0.67)
    df_mat = df_mat.sub(df_chem, fill_value=0)

    # calculate share of residual activity not covered
    # by industry sector explicit technologies
    df = df_mat.dropna().sort_values("Value", ascending=False)
    # manually set elec_i to 0 since all of it is covered by iron/steel sector
    df.loc[:, "elec_i", :] = 0
    df[df.le(0)] *= -1

    df = df.round(5)
    df.index.set_names(["node_loc", "technology", "year_act"], inplace=True)
    df["Value"] *= 1000 / 3600 / 8760  # convert from TJ to GWa
    df.fillna(0)

    df = df[(df.index.get_level_values(0).str.startswith("R12"))]
    return df


def get_hist_act(
    scen: "Scenario",
    years: List[int],
    iea_file_path: str = "",
    use_cached: bool = False,
) -> dict:
    """Derive activity calibration data for industrial technologies.

    Parameters
    ----------
    scen
        Scenario instance.
    years
        List of years for which activity should be retrieved.
    iea_file_path
        Path to EWEB parquet file.
    use_cached
        If True, use cached CSV files instead of recomputation.

    Returns
    -------
    dict
        Dictionary with DataFrames for ``bound_activity_up``, ``bound_activity_lo`` and
        ``historical_activity``.
    """
    s_info = ScenarioInfo(scen)
    fmy = s_info.y0
    if use_cached:
        df = pd.DataFrame()
        for type_tec in ["i_spec", "i_therm"]:
            df = pd.concat(
                [
                    df,
                    pd.read_csv(
                        package_data_path(
                            "material",
                            "other",
                            "activity",
                            f"{type_tec}_tecs_hist_act.csv",
                        )
                    ),
                ]
            )
    else:
        df = get_2020_industry_activity(years, iea_file_path)
        ind_tecs = [
            i for i in scen.set("technology") if (i.endswith("_i") or i.endswith("_I"))
        ]
        inp = scen.par(
            "input",
            filters={"technology": ind_tecs, "year_act": years, "year_vtg": years},
        )
        inp = inp[inp["year_act"] == inp["year_vtg"]]
        inp = inp.set_index(["node_loc", "technology", "year_act"]).rename(
            columns={"value": "efficiency"}
        )
        df = df.div(inp["efficiency"], axis=0).dropna()

        df = df.reset_index().rename(columns={"Value": "value"})
    df_rt = scen.par(
        "bound_activity_up", filters={"technology": "sp_el_I_RT"}
    ).set_index(["node_loc", "year_act"])["value"]
    df_sp_el = (
        df[df["technology"] == "sp_el_I"]
        .set_index(["node_loc", "year_act"])["value"]
        .sub(df_rt, fill_value=0)
        .reset_index()
    )
    df_sp_el = df_sp_el.assign(technology="sp_el_I")
    df = df[df["technology"] != "sp_el_I"]
    df = pd.concat([df, df_sp_el])
    df["mode"] = "M1"
    df["unit"] = "GWa"
    df["time"] = "year"
    df = make_df("historical_activity", **df)
    # common = {
    #     "mode": "M1",
    #     "unit": "GWa",
    #     "time": "year",
    #     "year_act": 2020,
    #     "technology": ind_tecs,
    #     "value": 0,
    # }
    # df_zero = message_ix.util.make_df("historical_activity", **common).pipe(
    #     broadcast, node_loc=nodes_ex_world(s_info.N)
    # )
    # df_join = df_zero[["node_loc", "technology", "year_act", "value"]].merge(
    #     df[["node_loc", "technology", "year_act", "value"]],
    #     on=["node_loc", "technology", "year_act"],
    #     how="left",
    # )
    # df_missing = df_join[df_join.value_y.isna()]
    return {
        "bound_activity_up": df[df["year_act"].ge(fmy)].assign(
            value=lambda x: x["value"] * 1.005, axis=1
        ),
        "bound_activity_lo": df[df["year_act"].ge(fmy)].assign(
            value=lambda x: x["value"] * 0.95, axis=1
        ),
        "historical_activity": df[df["year_act"].lt(fmy)],
    }


def gen_other_ind_demands(ssp: str) -> dict[str, pd.DataFrame]:
    """Generate demand parameter data for other industry sector (i_therm, i_spec).

    Parameters
    ----------
    ssp
        Shared Socioeconomic Pathway (SSP) code.

    Returns
    -------
    dict
        Dictionary with demand DataFrames for ``i_therm`` and ``i_spec`` commodity.
    """
    demands = {}
    for comm in ["i_therm", "i_spec"]:
        df_fixed = pd.read_csv(
            package_data_path(
                "material", "other", "activity", f"{comm}_tecs_hist_act.csv"
            )
        ).rename(columns={"year_act": "year", "node_loc": "node"})
        df_fixed = (
            df_fixed[df_fixed["year"].isin([2020, 2025])]
            .groupby(["node", "year"])
            .sum(numeric_only=True)
            .round(3)
            .reset_index()
        ).assign(level="useful", commodity=comm, time="year", unit="GWa")
        df = pd.read_csv(
            package_data_path("material", "other", "demand", f"{comm}_{ssp}.csv")
        )
        df = pd.concat([df[df["year"].ge(2030)], df_fixed]).sort_values(
            ["node", "year"]
        )
        demands[comm] = df.copy(deep=True)
    return demands


def gen_data_other(scenario) -> "ParameterData":
    context = read_config()
    par_data = {}
    demands = pd.concat(
        v[v["year"].isin(scenario.vintage_and_active_years()["year_act"].unique())]
        for v in gen_other_ind_demands(get_ssp_from_context(context)).values()
    )
    par_data["demand"] = demands
    # overwrite non-Materials industry technology calibration
    calib_data = get_hist_act(
        scenario, [1990, 1995, 2000, 2010, 2015, 2020], use_cached=True
    )
    merge_data(par_data, calib_data)
    return par_data
