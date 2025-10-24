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
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from message_ix import Scenario


def modify_demand_and_hist_activity(scen: "Scenario") -> None:
    """Takes care of demand changes due to the introduction of material parents.

    Shed industrial energy demand properly. Also need take care of remove dynamic
    constraints for certain energy carriers. Adjust the historical activity of the
    related industry technologies that provide output to different categories of
    industrial demand (e.g. i_therm, i_spec, i_feed). The historical activity is reduced
    the same % as the industrial demand is reduced.

    Parameters
    ----------
    scen
        Scenario where industry demand should be adjusted.
    """
    # NOTE Temporarily modifying industrial energy demand
    # From IEA database (dumped to an excel)

    s_info = ScenarioInfo(scen)
    fname = "MESSAGEix-Materials_final_energy_industry.xlsx"

    if "R12_CHN" in s_info.N:
        sheet_n = "R12"
        region_type = "R12_"
        region_name_CPA = "RCPA"
        region_name_CHN = "CHN"
    else:
        sheet_n = "R11"
        region_type = "R11_"
        region_name_CPA = "CPA"
        region_name_CHN = ""

    df = pd.read_excel(
        package_data_path("material", "other", fname), sheet_name=sheet_n, usecols="A:F"
    )

    # Filter the necessary variables
    df = df[
        (df["SECTOR"] == "feedstock (petrochemical industry)")
        | (df["SECTOR"] == "feedstock (total)")
        | (df["SECTOR"] == "industry (chemicals)")
        | (df["SECTOR"] == "industry (iron and steel)")
        | (df["SECTOR"] == "industry (non-ferrous metals)")
        | (df["SECTOR"] == "industry (non-metallic minerals)")
        | (df["SECTOR"] == "industry (total)")
    ]
    df = df[df["RYEAR"] == 2015]

    # NOTE: Total cehmical industry energy: 27% thermal, 8% electricity, 65% feedstock
    # SOURCE: IEA Sankey 2020: https://www.iea.org/sankey/#?c=World&s=Final%20consumption
    # 67% of total chemicals energy is used for primary chemicals (ammonia,methnol,HVCs)
    # SOURCE: https://www.iea.org/data-and-statistics/charts/primary-chemical-production-in-the-sustainable-development-scenario-2000-2030

    # Retreive data for i_spec
    # 67% of total chemcials electricity demand comes from primary chemicals (IEA)
    # (Excludes petrochemicals as the share is negligable)
    # Aluminum, cement and steel included.
    # NOTE: Steel has high shares (previously it was not inlcuded in i_spec)

    df_spec = df[
        (df["FUEL"] == "electricity")
        & (df["SECTOR"] != "industry (total)")
        & (df["SECTOR"] != "feedstock (petrochemical industry)")
        & (df["SECTOR"] != "feedstock (total)")
    ]
    df_spec_total = df[
        (df["SECTOR"] == "industry (total)") & (df["FUEL"] == "electricity")
    ]

    df_spec_new = pd.DataFrame(
        columns=["REGION", "SECTOR", "FUEL", "RYEAR", "UNIT_OUT", "RESULT"]
    )
    for r in df_spec["REGION"].unique():
        df_spec_temp = df_spec.loc[df_spec["REGION"] == r]
        df_spec_total_temp = df_spec_total.loc[df_spec_total["REGION"] == r]
        df_spec_temp.loc[:, "i_spec"] = (
            df_spec_temp.loc[:, "RESULT"]
            / df_spec_total_temp.loc[:, "RESULT"].values[0]
        )
        df_spec_new = pd.concat([df_spec_temp, df_spec_new], ignore_index=True)

    df_spec_new.drop(["FUEL", "RYEAR", "UNIT_OUT", "RESULT"], axis=1, inplace=True)
    df_spec_new.loc[df_spec_new["SECTOR"] == "industry (chemicals)", "i_spec"] = (
        df_spec_new.loc[df_spec_new["SECTOR"] == "industry (chemicals)", "i_spec"]
        * 0.67
    )

    df_spec_new = df_spec_new.groupby(["REGION"]).sum().reset_index()

    # Already set to zero: ammonia, methanol, HVCs cover most of the feedstock

    df_feed = df[
        (df["SECTOR"] == "feedstock (petrochemical industry)") & (df["FUEL"] == "total")
    ]
    # df_feed_total =
    # df[(df["SECTOR"] == "feedstock (total)") & (df["FUEL"] == "total")]
    df_feed_temp = pd.DataFrame(columns=["REGION", "i_feed"])
    df_feed_new = pd.DataFrame(columns=["REGION", "i_feed"])

    for r in df_feed["REGION"].unique():
        i = 0
        df_feed_temp.at[i, "REGION"] = r
        df_feed_temp.at[i, "i_feed"] = 1
        i = i + 1
        df_feed_new = pd.concat([df_feed_temp, df_feed_new], ignore_index=True)

    # Retreive data for i_therm
    # 67% of chemical thermal energy chemicals comes from primary chemicals. (IEA)
    # NOTE: Aluminum is excluded since refining process is not explicitly represented
    # NOTE: CPA has a 3% share while it used to be 30% previosuly ??

    df_therm = df[
        (df["FUEL"] != "electricity")
        & (df["FUEL"] != "total")
        & (df["SECTOR"] != "industry (total)")
        & (df["SECTOR"] != "feedstock (petrochemical industry)")
        & (df["SECTOR"] != "feedstock (total)")
        & (df["SECTOR"] != "industry (non-ferrous metals)")
    ]
    df_therm_total = df[
        (df["SECTOR"] == "industry (total)")
        & (df["FUEL"] != "total")
        & (df["FUEL"] != "electricity")
    ]
    df_therm_total = (
        df_therm_total.groupby(by="REGION").sum().drop(["RYEAR"], axis=1).reset_index()
    )
    df_therm = (
        df_therm.groupby(by=["REGION", "SECTOR"])
        .sum()
        .drop(["RYEAR"], axis=1)
        .reset_index()
    )
    df_therm_new = pd.DataFrame(
        columns=["REGION", "SECTOR", "FUEL", "RYEAR", "UNIT_OUT", "RESULT"]
    )

    for r in df_therm["REGION"].unique():
        df_therm_temp = df_therm.loc[df_therm["REGION"] == r]
        df_therm_total_temp = df_therm_total.loc[df_therm_total["REGION"] == r]
        df_therm_temp.loc[:, "i_therm"] = (
            df_therm_temp.loc[:, "RESULT"]
            / df_therm_total_temp.loc[:, "RESULT"].values[0]
        )
        df_therm_new = pd.concat([df_therm_temp, df_therm_new], ignore_index=True)
        df_therm_new = df_therm_new.drop(["RESULT"], axis=1)

    df_therm_new.drop(["FUEL", "RYEAR", "UNIT_OUT"], axis=1, inplace=True)
    df_therm_new.loc[df_therm_new["SECTOR"] == "industry (chemicals)", "i_therm"] = (
        df_therm_new.loc[df_therm_new["SECTOR"] == "industry (chemicals)", "i_therm"]
        * 0.67
    )

    # Modify CPA based on https://www.iea.org/sankey/#?c=Japan&s=Final%20consumption.
    # Since the value did not allign with the one in the IEA website.
    index = (df_therm_new["SECTOR"] == "industry (iron and steel)") & (
        (df_therm_new["REGION"] == region_name_CPA)
        | (df_therm_new["REGION"] == region_name_CHN)
    )

    df_therm_new.loc[index, "i_therm"] = 0.2

    df_therm_new = df_therm_new.groupby(["REGION"]).sum(numeric_only=True).reset_index()

    # TODO: Useful technology efficiencies will also be included

    # Add the modified demand and historical activity to the scenario

    # Relted technologies that have outputs to useful industry level.
    # Historical activity of theese will be adjusted
    tec_therm = [
        "biomass_i",
        "coal_i",
        "elec_i",
        "eth_i",
        "foil_i",
        "gas_i",
        "h2_i",
        "heat_i",
        "hp_el_i",
        "hp_gas_i",
        "loil_i",
        "meth_i",
        "solar_i",
    ]
    tec_fs = [
        "coal_fs",
        "ethanol_fs",
        "foil_fs",
        "gas_fs",
        "loil_fs",
        "methanol_fs",
    ]
    tec_sp = ["sp_coal_I", "sp_el_I", "sp_eth_I", "sp_liq_I", "sp_meth_I", "h2_fc_I"]

    thermal_df_hist = scen.par("historical_activity", filters={"technology": tec_therm})
    spec_df_hist = scen.par("historical_activity", filters={"technology": tec_sp})
    feed_df_hist = scen.par("historical_activity", filters={"technology": tec_fs})
    useful_thermal = scen.par("demand", filters={"commodity": "i_therm"})
    useful_spec = scen.par("demand", filters={"commodity": "i_spec"})
    useful_feed = scen.par("demand", filters={"commodity": "i_feed"})

    for r in df_therm_new["REGION"]:
        r_MESSAGE = region_type + r

        useful_thermal.loc[useful_thermal["node"] == r_MESSAGE, "value"] = (
            useful_thermal.loc[useful_thermal["node"] == r_MESSAGE, "value"]
            * (1 - df_therm_new.loc[df_therm_new["REGION"] == r, "i_therm"].values[0])
        )

        thermal_df_hist.loc[thermal_df_hist["node_loc"] == r_MESSAGE, "value"] = (
            thermal_df_hist.loc[thermal_df_hist["node_loc"] == r_MESSAGE, "value"]
            * (1 - df_therm_new.loc[df_therm_new["REGION"] == r, "i_therm"].values[0])
        )

    for r in df_spec_new["REGION"]:
        r_MESSAGE = region_type + r

        useful_spec.loc[useful_spec["node"] == r_MESSAGE, "value"] = useful_spec.loc[
            useful_spec["node"] == r_MESSAGE, "value"
        ] * (1 - df_spec_new.loc[df_spec_new["REGION"] == r, "i_spec"].values[0])

        spec_df_hist.loc[spec_df_hist["node_loc"] == r_MESSAGE, "value"] = (
            spec_df_hist.loc[spec_df_hist["node_loc"] == r_MESSAGE, "value"]
            * (1 - df_spec_new.loc[df_spec_new["REGION"] == r, "i_spec"].values[0])
        )

    for r in df_feed_new["REGION"]:
        r_MESSAGE = region_type + r

        useful_feed.loc[useful_feed["node"] == r_MESSAGE, "value"] = useful_feed.loc[
            useful_feed["node"] == r_MESSAGE, "value"
        ] * (1 - df_feed_new.loc[df_feed_new["REGION"] == r, "i_feed"].values[0])

        feed_df_hist.loc[feed_df_hist["node_loc"] == r_MESSAGE, "value"] = (
            feed_df_hist.loc[feed_df_hist["node_loc"] == r_MESSAGE, "value"]
            * (1 - df_feed_new.loc[df_feed_new["REGION"] == r, "i_feed"].values[0])
        )

    scen.check_out()
    scen.add_par("demand", useful_thermal)
    scen.add_par("demand", useful_spec)
    scen.add_par("demand", useful_feed)
    scen.commit("Demand values adjusted")

    scen.check_out()
    scen.add_par("historical_activity", thermal_df_hist)
    scen.add_par("historical_activity", spec_df_hist)
    scen.add_par("historical_activity", feed_df_hist)
    scen.commit(
        comment="historical activity for useful level industry \
    technologies adjusted"
    )

    # For aluminum there is no significant deduction required
    # (refining process not included and thermal energy required from
    # recycling is not a significant share.)
    # For petro: based on 13.1 GJ/tonne of ethylene and the demand in the model

    # df = scen.par(demand, filters={commodity:i_therm})
    # df.value = df.value * 0.38 #(30% steel, 25% cement, 7% petro)
    #
    # scen.check_out()
    # scen.add_par(demand, df)
    # scen.commit(comment = modify i_therm demand)

    # Adjust the i_spec.
    # Electricity usage seems negligable in the production of HVCs.
    # Aluminum: based on IAI China data 20%.

    # df = scen.par(demand, filters={commodity:i_spec})
    # df.value = df.value * 0.80  #(20% aluminum)
    #
    # scen.check_out()
    # scen.add_par(demand, df)
    # scen.commit(comment = modify i_spec demand)

    # Adjust the i_feedstock.
    # 45 GJ/tonne of ethylene or propylene or BTX
    # 2020 demand of one of these: 35.7 Mt
    # Makes up around 30% of total feedstock demand.

    # df = scen.par(demand, filters={commodity:i_feed})
    # df.value = df.value * 0.7  #(30% HVCs)
    #
    # scen.check_out()
    # scen.add_par(demand, df)
    # scen.commit(comment = modify i_feed demand)

    # NOTE Aggregate industrial coal demand need to adjust to
    #      the sudden intro of steel setor in the first model year

    t_i = ["coal_i", "elec_i", "gas_i", "heat_i", "loil_i", "solar_i"]

    for t in t_i:
        df = scen.par("growth_activity_lo", filters={"technology": t, "year_act": 2020})

        scen.check_out()
        scen.remove_par("growth_activity_lo", df)
        scen.commit(comment="remove growth_lo constraints")

    scen.check_out()
    for substr in ["up", "lo"]:
        df = scen.par(f"bound_activity_{substr}")
        scen.remove_par(
            f"bound_activity_{substr}",
            df[(df["technology"].str.endswith("_fs")) & (df["year_act"] == 2020)],
        )
        scen.remove_par(
            f"bound_activity_{substr}",
            df[(df["technology"].str.endswith("_i")) & (df["year_act"] == 2020)],
        )
        scen.remove_par(
            f"bound_activity_{substr}",
            df[(df["technology"].str.endswith("_I")) & (df["year_act"] == 2020)],
        )
    scen.commit(comment="remove bounds")


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
