import os
from typing import TYPE_CHECKING

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
    """Take care of demand changes due to the introduction of material parents
    Shed industrial energy demand properly.
    Also need take care of remove dynamic constraints for certain energy carriers.
    Adjust the historical activity of the related industry technologies
    that provide output to different categories of industrial demand (e.g.
    i_therm, i_spec, i_feed). The historical activity is reduced the same %
    as the industrial demand is reduced.

    Parameters
    ----------
    scen: .Scenario
        scenario where industry demand should be reduced
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

    # df = scen.par('demand', filters={'commodity':'i_therm'})
    # df.value = df.value * 0.38 #(30% steel, 25% cement, 7% petro)
    #
    # scen.check_out()
    # scen.add_par('demand', df)
    # scen.commit(comment = 'modify i_therm demand')

    # Adjust the i_spec.
    # Electricity usage seems negligable in the production of HVCs.
    # Aluminum: based on IAI China data 20%.

    # df = scen.par('demand', filters={'commodity':'i_spec'})
    # df.value = df.value * 0.80  #(20% aluminum)
    #
    # scen.check_out()
    # scen.add_par('demand', df)
    # scen.commit(comment = 'modify i_spec demand')

    # Adjust the i_feedstock.
    # 45 GJ/tonne of ethylene or propylene or BTX
    # 2020 demand of one of these: 35.7 Mt
    # Makes up around 30% of total feedstock demand.

    # df = scen.par('demand', filters={'commodity':'i_feed'})
    # df.value = df.value * 0.7  #(30% HVCs)
    #
    # scen.check_out()
    # scen.add_par('demand', df)
    # scen.commit(comment = 'modify i_feed demand')

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
    map_fname: str, years: list or None = None, iea_data_path: str = None
) -> pd.DataFrame:
    """
    Reads IEA DB, maps and aggregates variables to MESSAGE technologies

    Parameters
    ----------
    map_fname
        name of MESSAGEix-technology-to-IEA-flow/product mapping file
    years
        specifies timesteps for whom historical activity should
        be calculated and returned
    iea_data_path: str
        path to IEA EWEB parquet file

    Returns
    -------
    pd.DataFrame

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


def get_2020_industry_activity(years: list, iea_data_path: str) -> pd.DataFrame:
    df_mat = get_hist_act_data("industry.csv", years=years, iea_data_path=iea_data_path)
    df_chem = get_hist_act_data(
        "chemicals.csv", years=years, iea_data_path=iea_data_path
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


def calc_hist_activity(scen: "Scenario", years: list, iea_data_path) -> pd.DataFrame:
    df_orig = get_hist_act_data(
        "all_technologies.csv", years=years, iea_data_path=iea_data_path
    )
    df_mat = get_hist_act_data("industry.csv", years=years, iea_data_path=iea_data_path)
    df_chem = get_hist_act_data(
        "chemicals.csv", years=years, iea_data_path=iea_data_path
    )

    # RFE: move hardcoded assumptions (chemicals and iron and steel)
    #  to external data files

    # scale chemical activity to deduct explicitly
    # represented activities of MESSAGEix-Materials
    # (67% are covered by NH3, HVCs and methanol)
    df_chem = df_chem.mul(0.67)
    df_mat = df_mat.sub(df_chem, fill_value=0)

    # calculate share of residual activity not covered
    # by industry sector explicit technologies
    df = df_mat.div(df_orig).dropna().sort_values("Value", ascending=False)
    # manually set elec_i to 0 since all of it is covered by iron/steel sector
    df.loc[:, "elec_i", :] = 0

    df = df.round(5)
    df.index.set_names(["node_loc", "technology", "year_act"], inplace=True)

    tecs = df.index.get_level_values("technology").unique()
    df_hist_act = scen.par(
        "historical_activity", filters={"technology": tecs, "year_act": years}
    )

    df_hist_act_scaled = (
        df_hist_act.set_index([i for i in df_hist_act.columns if i != "value"])
        .mul(df.rename({"Value": "value"}, axis=1))
        .dropna()
    )

    return df_hist_act_scaled.reset_index()


def add_new_ind_hist_act(scen: "Scenario", years: list, iea_data_path: str) -> None:
    df_act = calc_hist_activity(scen, years, iea_data_path)
    scen.check_out()
    scen.add_par("historical_activity", df_act)
    scen.commit("adjust historical activity of industrial end use tecs")


def calc_demand_shares(iea_db_df: pd.DataFrame, base_year: int) -> pd.DataFrame:
    # RFE: refactor to use external mapping file (analogue to calc_hist_activity())
    i_spec_material_flows = ["NONMET", "NONFERR"]  # "CHEMICAL"
    i_therm_material_flows = ["NONMET", "CHEMICAL", "IRONSTL"]
    i_flow = ["TOTIND"]
    i_spec_prods = ["ELECTR", "NONBIODIES", "BIOGASOL"]
    year = base_year

    df_i_spec = iea_db_df[
        (iea_db_df["FLOW"].isin(i_flow))
        & (iea_db_df["PRODUCT"].isin(i_spec_prods))
        & ~((iea_db_df["PRODUCT"] == ("ELECTR")) & (iea_db_df["FLOW"] == "IRONSTL"))
        & (iea_db_df["TIME"] == year)
    ]
    df_i_spec = df_i_spec.groupby("REGION").sum(numeric_only=True)

    df_i_spec_materials = iea_db_df[
        (iea_db_df["FLOW"].isin(i_spec_material_flows))
        & (iea_db_df["PRODUCT"].isin(i_spec_prods))
        & (iea_db_df["TIME"] == year)
    ]
    df_i_spec_materials = df_i_spec_materials.groupby("REGION").sum(numeric_only=True)

    df_i_spec_resid_shr = (
        df_i_spec_materials.div(df_i_spec, fill_value=0).sub(1).mul(-1)
    )
    df_i_spec_resid_shr["commodity"] = "i_spec"

    df_elec_i = iea_db_df[
        ((iea_db_df["PRODUCT"] == ("ELECTR")) & (iea_db_df["FLOW"] == "IRONSTL"))
        & (iea_db_df["TIME"] == year)
    ]
    df_elec_i = df_elec_i.groupby("REGION").sum(numeric_only=True)

    agg_prods = ["MRENEW", "TOTAL"]
    df_i_therm = iea_db_df[
        (iea_db_df["FLOW"].isin(i_flow))
        & ~(iea_db_df["PRODUCT"].isin(i_spec_prods))
        & ~(iea_db_df["PRODUCT"].isin(agg_prods))
        & (iea_db_df["TIME"] == year)
    ]
    df_i_therm = df_i_therm.groupby("REGION").sum(numeric_only=True)
    df_i_therm = df_i_therm.add(df_elec_i, fill_value=0)

    agg_prods = ["MRENEW", "TOTAL"]
    df_i_therm_materials = iea_db_df[
        (iea_db_df["FLOW"].isin(i_therm_material_flows))
        & ~(iea_db_df["PRODUCT"].isin(i_spec_prods))
        & ~(iea_db_df["PRODUCT"].isin(agg_prods))
        & (iea_db_df["TIME"] == year)
    ]
    df_i_therm_materials = df_i_therm_materials.groupby(["REGION", "FLOW"]).sum(
        numeric_only=True
    )
    # only two thirds of chemical consumption is represented
    # by Materials module currently
    df_i_therm_materials.loc[
        df_i_therm_materials.index.get_level_values(1) == "CHEMICAL", "Value"
    ] *= 0.67

    # only covering cement at the moment
    # quick fix assuming 67% of NONMET is used for cement in each region
    # needs regional differentiation once data is collected
    df_i_therm_materials.loc[
        df_i_therm_materials.index.get_level_values(1) == "NONMET", "Value"
    ] *= 0.67

    df_i_therm_materials = df_i_therm_materials.groupby("REGION").sum(numeric_only=True)
    df_i_therm_materials = df_i_therm_materials.add(df_elec_i, fill_value=0)

    df_i_therm_resid_shr = df_i_therm_materials.div(df_i_therm).sub(1).mul(-1)
    df_i_therm_resid_shr["commodity"] = "i_therm"

    return (
        pd.concat([df_i_spec_resid_shr, df_i_therm_resid_shr])
        .set_index("commodity", append=True)
        .drop("TIME", axis=1)
    )


def calc_resid_ind_demand(
    scen: "Scenario", baseyear: int, iea_data_path: str
) -> pd.DataFrame:
    comms = ["i_spec", "i_therm"]
    path = os.path.join(iea_data_path)
    Inp = pd.read_parquet(path, engine="fastparquet")
    Inp = map_iea_db_to_msg_regs(Inp)
    demand_shrs_new = calc_demand_shares(pd.DataFrame(Inp), baseyear)
    df_demands = scen.par("demand", filters={"commodity": comms}).set_index(
        ["node", "commodity", "year"]
    )
    demand_shrs_new.index.set_names(["node", "commodity"], inplace=True)
    df_demands["value"] = df_demands["value"] * demand_shrs_new["Value"]
    return df_demands.reset_index()


def modify_industry_demand(scen: "Scenario", baseyear: int, iea_data_path: str) -> None:
    df_demands_new = calc_resid_ind_demand(scen, baseyear, iea_data_path)
    scen.check_out()
    scen.add_par("demand", df_demands_new)

    # RFE: calculate deductions from IEA data instead
    #  of assuming full coverage by MESSAGE-Materials (chemicals)
    # remove i_spec demand separately since we assume 100% coverage by MESSAGE-Materials
    df_i_feed = scen.par("demand", filters={"commodity": "i_feed"})
    scen.remove_par("demand", df_i_feed)
    scen.commit("adjust residual industry demands")


def add_elec_lowerbound_2020(scen: "Scenario") -> None:
    """
    DEPRECATED
    Adds a bound_activity_lo to the technology "sp_el_I"
    Parameters
    ----------
    scen: .Scenario
        scenario to apply the lower bound to
    """
    # To avoid zero i_spec prices only for R12_CHN, add the below section.
    # read input parameters for relevant technology/commodity combinations for
    # converting betwen final and useful energy

    input_residual_electricity = scen.par(
        "input",
        filters={"technology": "sp_el_I", "year_vtg": "2020", "year_act": "2020"},
    )

    # read processed final energy data from IEA extended energy balances
    # that is aggregated to MESSAGEix regions, fuels and (industry) sectors

    final = pd.read_csv(
        package_data_path("material", "other", "residual_industry_2019.csv")
    )

    # downselect needed fuels and sectors
    final_residual_electricity = final.query(
        'MESSAGE_fuel=="electr" & MESSAGE_sector=="industry_residual"'
    )

    # join final energy data from IEA energy balances and input coefficients
    # from final-to-useful technologies from MESSAGEix
    bound_residual_electricity = pd.merge(
        input_residual_electricity,
        final_residual_electricity,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    # derive useful energy values by dividing final energy by
    # input coefficient from final-to-useful technologies
    bound_residual_electricity["value"] = (
        bound_residual_electricity["Value"] / bound_residual_electricity["value"]
    )

    # downselect dataframe columns for MESSAGEix parameters
    bound_residual_electricity = bound_residual_electricity.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )
    # rename columns if necessary
    bound_residual_electricity.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    # Decrease 20% to aviod zero prices (the issue continiues otherwise)
    bound_residual_electricity["value"] = bound_residual_electricity["value"] * 0.8
    bound_residual_electricity = bound_residual_electricity[
        bound_residual_electricity["node_loc"] == "R12_CHN"
    ]

    scen.check_out()

    # add parameter dataframes to ixmp
    scen.add_par("bound_activity_lo", bound_residual_electricity)

    # Remove the previous bounds
    remove_par_lo = scen.par(
        "growth_activity_lo",
        filters={"technology": "sp_el_I", "year_act": 2020, "node_loc": "R12_CHN"},
    )
    scen.remove_par("growth_activity_lo", remove_par_lo)

    scen.commit("added lower bound for activity of residual electricity technologies")


def add_coal_lowerbound_2020(scen: "Scenario") -> None:
    """Set lower bounds for coal_i and sp_el_I technology as a calibration for 2020"""

    final_resid = pd.read_csv(
        package_data_path("material", "other", "residual_industry_2019.csv")
    )

    # read input parameters for relevant technology/commodity combinations
    # for converting betwen final and useful energy
    input_residual_coal = scen.par(
        "input",
        filters={"technology": "coal_i", "year_vtg": "2020", "year_act": "2020"},
    )
    input_residual_electricity = scen.par(
        "input",
        filters={"technology": "sp_el_I", "year_vtg": "2020", "year_act": "2020"},
    )

    # downselect needed fuels and sectors
    final_residual_coal = final_resid.query(
        'MESSAGE_fuel=="coal" & MESSAGE_sector=="industry_residual"'
    )
    final_residual_electricity = final_resid.query(
        'MESSAGE_fuel=="electr" & MESSAGE_sector=="industry_residual"'
    )

    # join final energy data from IEA energy balances and input
    # coefficients from final-to-useful technologies from MESSAGEix
    bound_coal = pd.merge(
        input_residual_coal,
        final_residual_coal,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )
    bound_residual_electricity = pd.merge(
        input_residual_electricity,
        final_residual_electricity,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    # derive useful energy values by dividing final energy
    # by input coefficient from final-to-useful technologies
    bound_coal["value"] = bound_coal["Value"] / bound_coal["value"]
    bound_residual_electricity["value"] = (
        bound_residual_electricity["Value"] / bound_residual_electricity["value"]
    )

    # downselect dataframe columns for MESSAGEix parameters
    bound_coal = bound_coal.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )
    bound_residual_electricity = bound_residual_electricity.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )

    # rename columns if necessary
    bound_coal.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]
    bound_residual_electricity.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    # (Artificially) lower bounds when i_spec act is too close
    # to the bounds (avoid 0-price for macro calibration)
    more = ["R12_MEA", "R12_EEU", "R12_SAS", "R12_PAS"]
    # import pdb; pdb.set_trace()
    bound_residual_electricity.loc[
        bound_residual_electricity.node_loc.isin(["R12_PAO"]), "value"
    ] *= 0.80

    bound_residual_electricity.loc[
        bound_residual_electricity.node_loc.isin(more), "value"
    ] *= 0.85

    scen.check_out()
    # add parameter dataframes to ixmp
    scen.add_par("bound_activity_lo", bound_coal)
    # sc.add_par("bound_activity_lo", bound_cement_coal)
    scen.add_par("bound_activity_lo", bound_residual_electricity)

    # commit scenario to ixmp backend
    scen.commit(
        "added lower bound for activity of residual industrial coal"
        "and cement coal furnace technologies and "
        "adjusted 2020 residual industrial electricity demand"
    )


def add_coal_lowerbound_2020_cement(scen: "Scenario") -> None:
    """
    DEPRECATED
    Adds a bound_activity_lo to the technology "furnace_coal_cement"
    Parameters
    ----------
    scen: .Scenario
        scenario to apply the lower bound to
    """
    final_resid = pd.read_csv(
        package_data_path("material", "other", "residual_industry_2019.csv")
    )
    input_cement_coal = scen.par(
        "input",
        filters={
            "technology": "furnace_coal_cement",
            "year_vtg": "2020",
            "year_act": "2020",
            "mode": "high_temp",
        },
    )
    final_cement_coal = final_resid.query(
        'MESSAGE_fuel=="coal" & MESSAGE_sector=="cement"'
    )
    bound_cement_coal = pd.merge(
        input_cement_coal,
        final_cement_coal,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )
    bound_cement_coal["value"] = bound_cement_coal["Value"] / bound_cement_coal["value"]
    bound_cement_coal = bound_cement_coal.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )
    bound_cement_coal.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]


def get_hist_act(scen, years, data_file_path=None, use_cached=False):
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
        df = get_2020_industry_activity(years, data_file_path)
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


def gen_other_ind_demands(ssp):
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
            .sum(numeric_only=True).round(3)
            .reset_index()
        )
        df = pd.read_csv(
            package_data_path("material", "other", "demand", f"{comm}_{ssp}.csv")
        ).rename(columns={"year_act": "year", "0": "value", "node_loc": "node"})
        df = pd.concat([df[df["year"].ge(2030)], df_fixed]).sort_values(["node","year"])
        df["commodity"] = comm
        df["time"] = "year"
        df["unit"] = "GWa"
        df["level"] = "useful"
        demands[comm] = df.copy(deep=True)
    return demands


if __name__ == "__main__":
    get_hist_act(None, [2020], use_cached=True)
    gen_other_ind_demands("SSP3")
    import ixmp
    import message_ix

    mp = ixmp.Platform("ixmp_dev")
    scen = message_ix.Scenario(
        mp, "SSP_dev_SSP2_v0.1_Blv0.18", "baseline_prep_lu_bkp_solved_materials_W34"
    )

    dfs = get_hist_act(
        scen,
        [2015, 2020],
        r"P:/ene.model/IEA_database/Florian/S3_IEA_REV2024_FiltISO_TJ_BIO.parquet",
        use_cached=True,
    )
    print()
