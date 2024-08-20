import pandas as pd
import pyam
from message_ix.report import Reporter

from message_ix_models.model.water.reporting import report
from message_ix_models.util import package_data_path

try:
    from message_data.tools.post_processing.iamc_report_hackathon import (
        report as old_reporting,
    )
except ImportError:  # message_data not installed
    old_reporting = None


def reg_index(region):
    temp = []
    for i, c in enumerate(region):
        if c == "|":
            temp.append(i)
    return temp


def remove_duplicate(data):
    final_list = []
    indexes = data[data["Variable"].str.contains("basin_to_reg")].index
    for i in data["Region"].index:
        strr = data["Region"][i]
        oprlist = reg_index(strr)
        # cover the case of repeated region name (e.g. Zambia|Zambia)
        if ("|" in strr) and (strr.split("|")[0] == strr.split("|")[1]):
            final_list.append(strr.split("|")[0])
        else:
            if i in indexes:
                if len(oprlist) > 1:
                    final_list.append(strr[oprlist[0] + 1 :])
                elif len(oprlist) == 1 and oprlist[0] > 6:
                    final_list.append(strr[: oprlist[0]])
                else:
                    final_list.append(strr)
            else:
                if len(oprlist) > 1:
                    final_list.append(strr[: oprlist[1]])
                elif len(oprlist) == 1 and oprlist[0] > 6:
                    final_list.append(strr[: oprlist[0]])
                else:
                    final_list.append(strr)
    return final_list


# df = pd.DataFrame(
#     {
#         "var1": ["A", "A", "A", "B", "B", "C"],
#         "var2": [1, 1, 1, 1, 1, 2],
#         "subannual": ["year", "1", "2", "year", "1", "year"],
#         "value": [10, 20, 30, 3, 2, 99],
#     }
# )

# hierarchy = {"year": ["1", "2"]}


# def sum_time_hierarchy(df, hierarchy):
#     # group by variables that are not "subannual" or "value"
#     group_cols = [col for col in df.columns if col not in ["subannual", "value"]]
#     df_grouped = df.groupby(group_cols).apply(lambda x: x.copy())

#     # iterate over the hierarchy and sum the values for each level
#     lower_hierarchy = hierarchy.get("year")
#     if lower_hierarchy:
#         df_lower = df[df["subannual"].isin(lower_hierarchy)]
#         df_year = df[df["subannual"] == "year"]

#         df_summed = df_lower.groupby(group_cols, as_index=False).agg({"value": "sum"})
#         df_summed["subannual"] = "year"
#         df_summed = df_summed[df.columns]
#         # add the summed parts to other with year
#         df_year2 = pd.concat([df_year, df_summed], ignore_index=True)
#         # aggregate those at year
#         df_year2 = df_year2.groupby(group_cols + ["subannual"], as_index=False).agg(
#             {"value": "sum"}
#         )

#         df_out = pd.concat([df_year2, df_lower])
#     return df_out


# sum_time_hierarchy(df, hierarchy)
# to check if for variables like price it makes sense. no we would need to make a mean


def report_country(sc):
    rep = Reporter.from_scenario(sc)
    report = rep.get(
        "message::default"
    )  # works also with suannual, but aggregates months
    # Create a timeseries dataframe
    report_df = report.timeseries()
    report_df.reset_index(inplace=True)
    report_df.columns = report_df.columns.astype(str)
    report_df.columns = report_df.columns.str.title()
    # Removing duplicate region names
    report_df["Region"] = remove_duplicate(report_df)

    vars_dic = [
        "in:nl-t-ya-m-h-no-c-l",
        "out:nl-t-ya-m-h-nd-c-l",
    ]

    # other variables do not ahve sub-annual dimension, we just take
    # annual values from report_df
    vars_from_annual = ["CAP_NEW", "inv cost", "total om cost", "emis"]
    # get annual variables
    report_df1 = report_df[
        report_df["Variable"].str.contains("|".join(vars_from_annual))
    ]
    report_df1["subannual"] = "year"
    # Convert to pyam dataframe
    report_iam = pyam.IamDataFrame(report_df1)

    report_df2 = pd.DataFrame()
    for vs in vars_dic:
        qty = rep.get(vs)
        df = qty.to_dataframe()
        df.reset_index(inplace=True)
        df["model"] = sc.model
        df["scenario"] = sc.scenario
        df["variable"] = (
            vs.split(":")[0]
            + "|"
            + df["l"]
            + "|"
            + df["c"]
            + "|"
            + df["t"]
            + "|"
            + df["m"]
        )

        df.rename(
            columns={
                "no": "reg2",  # needed to avoid dulicates
                "nd": "reg2",
                "nl": "reg1",
                "ya": "year",
                "h": "subannual",
            },
            inplace=True,
        )
        # take the right node column in case nl and no/nd are different
        df = (
            df.groupby(["model", "scenario", "variable", "subannual", "year"])
            .apply(
                lambda x: x.assign(
                    region=x["reg2"]
                    if len(x["reg2"].unique()) > len(x["reg1"].unique())
                    else x["reg1"]
                )
            )
            .reset_index(drop=True)
        )
        # case of
        exeption = "in|water_supply_basin|freshwater_basin|basin_to_reg"
        df["region"] = df.apply(
            lambda row: row["reg2"] if exeption in row["variable"] else row["region"],
            axis=1,
        )
        df = df[
            ["model", "scenario", "region", "variable", "subannual", "year", "value"]
        ]
        report_df2 = pd.concat([report_df2, df])

    report_df2["unit"] = ""
    report_df2.columns = report_df2.columns.astype(str)
    report_df2.columns = report_df2.columns.str.title()
    report_df2.reset_index(drop=True, inplace=True)
    report_df2["Region"] = remove_duplicate(report_df2)
    report_df2.columns = map(str.lower, report_df2.columns)
    # make iamc dataframe
    report_iam2 = pyam.IamDataFrame(report_df2)
    report_iam = report_iam.append(report_iam2)

    # demand part
    rep_dm2 = rep.get("demand:n-c-l-y-h")
    df_dmd = rep_dm2.to_dataframe()
    df_dmd.reset_index(inplace=True)

    df_dmd["model"] = sc.model
    df_dmd["scenario"] = sc.scenario
    df_dmd["variable"] = df_dmd["c"] + "|" + df_dmd["l"]
    df_dmd.rename(
        columns={"n": "region", "y": "year", "demand": "value", "h": "subannual"},
        inplace=True,
    )
    df_dmd["unit"] = "GWa"
    df_dmd = df_dmd[
        [
            "model",
            "scenario",
            "region",
            "variable",
            "subannual",
            "year",
            "unit",
            "value",
        ]
    ]
    df_dmd["value"] = df_dmd["value"].abs()
    report_dem = pyam.IamDataFrame(df_dmd)
    report_iam = report_iam.append(report_dem)

    # define mapping to aggregate by subregion and subtime
    map_node = sc.set("map_node")
    map_node = map_node[map_node["node_parent"] != map_node["node"]]
    map_node = map_node[map_node["node_parent"] != "World"]
    map_node_dict = map_node.groupby("node_parent")["node"].apply(list).to_dict()
    # similar for time
    map_time = sc.set("map_time")
    map_time = map_time[map_time["time_parent"] != map_time["time"]]
    map_time_dict = map_time.groupby("time_parent")["time"].apply(list).to_dict()

    # energy demand, Useful energy GWa/y or month
    ind_dem_c = [
        "i_feed|useful",
        "i_therm|useful",
    ]
    ind_dem_b = ["ind_man_urb|useful", "ind_man_rur|useful"]
    non_comm_dem = [
        "non-comm|useful",
    ]
    rc_dem_c = [
        "rc_therm|useful",
    ]
    rc_dem_b = ["res_com_urb|useful", "res_com_rur|useful"]
    trs_dem = [
        "transport|useful",
    ]
    crop_en_dem = ["crop_rur|useful"]
    # irrigation demand
    all_tecs = sc.set("technology")
    irr_tecs = list(all_tecs[all_tecs.str.contains("irr_")])
    irr_FE = ["in|final_rur|electr|" + x + "|M1" for x in irr_tecs]
    # water withdrawals from irrigation
    irr_string = "in|water_supply_basin|freshwater_basin|"
    irr_ww = [irr_string + x + "|M1" for x in irr_tecs]

    ### Secondary Energy, electricity ###
    # se_ec_iam = report_iam.filter(variable="out|offgrid_final_urb|electr|*").variable
    sec_en_string = "out|secondary|electr|"
    # nuclear
    electr_nuc = [sec_en_string + x + "|M1" for x in ["nuc_fbr", "nuc_hc", "nuc_lc"]]
    # oil
    electr_oil = [
        sec_en_string + x + "|M1"
        for x in ["foil_ppl", "loil_cc", "loil_ppl", "oil_ppl", "SO2_scrub_ppl"]
    ]
    # gas
    electr_gas = [
        sec_en_string + x + "|M1"
        for x in ["gas_cc", "gas_cc_ccs", "gas_ct", "gas_htfc", "gas_ppl"]
    ]
    # coal
    electr_coal = [
        sec_en_string + x + "|M1"
        for x in [
            "coal_adv",
            "coal_adv_ccs",
            "coal_ppl",
            "coal_ppl_u",
            "igcc",
            "igcc_ccs",
        ]
    ]
    # biomass
    electr_biomass = [
        sec_en_string + x + "|M1" for x in ["bio_istig", "bio_istig_ccs", "bio_ppl"]
    ]
    # geothermal
    electr_geot = [sec_en_string + x + "|M1" for x in ["geo_ppl"]]
    # hydro
    electr_hydro = [sec_en_string + x + "|M1" for x in ["hydro_hc", "hydro_lc"]]
    # solar
    solar_tec = [
        "solar_res1",
        "solar_res2",
        "solar_res3",
        "solar_res4",
        "solar_res5",
        "solar_res6",
        "solar_res7",
        "solar_res8",
        "csp_sm1_res",
        "csp_sm1_res1",
        "csp_sm1_res2",
        "csp_sm1_res3",
        "csp_sm1_res4",
        "csp_sm1_res5",
        "csp_sm1_res6",
        "csp_sm1_res7",
        "csp_sm3_res",
        "csp_sm3_res1",
        "csp_sm3_res2",
        "csp_sm3_res3",
        "csp_sm3_res4",
        "csp_sm3_res5",
        "csp_sm3_res6",
        "csp_sm3_res7",
        "solar_res_hist_2020",
    ]
    electr_solar = [sec_en_string + x + "|M1" for x in solar_tec]
    # wind
    wind_tecs = [
        "wind_res_hist_2015",
        "wind_res1",
        "wind_res2",
        "wind_res3",
        "wind_res4",
        "wind_ref1",
        "wind_ref2",
        "wind_ref3",
        "wind_ref4",
        "wind_ref5",
    ]
    electr_wind = [sec_en_string + x + "|M1" for x in wind_tecs]
    # offgrid technologies
    electr_offgrid_urb = ["out|offgrid_final_urb|electr|offgrid_urb|M1"]
    electr_offgrid_rur = [
        "out|offgrid_final_rur|electr|offgrid_rur|M1",
    ]
    electr_offgrid = electr_offgrid_urb + electr_offgrid_rur

    # electr_grid = (
    #     electr_nuc
    #     + electr_oil
    #     + electr_gas
    #     + electr_coal
    #     + electr_biomass
    #     + electr_geot
    #     + electr_hydro
    #     + electr_solar
    #     + electr_wind
    # )

    ### investment ###
    # inv_iam = report_iam.filter(variable="inv cost|*").variable
    inv_en_string = "inv cost|"
    # nuclear
    inv_el_nuc = [inv_en_string + x for x in ["nuc_fbr", "nuc_hc", "nuc_lc"]]
    # oil
    inv_el_oil = [
        inv_en_string + x
        for x in ["foil_ppl", "loil_cc", "loil_ppl", "oil_ppl", "SO2_scrub_ppl"]
    ]
    # gas
    inv_el_gas = [
        inv_en_string + x
        for x in [
            "gas_cc",
            "gas_cc_ccs",
            "gas_ct",
            "g_ppl_co2scr",
            "gfc_co2scr",
            "gas_htfc",
            "gas_ppl",
        ]
    ]
    # coal
    inv_el_coal = [
        inv_en_string + x
        for x in [
            "coal_adv",
            "coal_adv_ccs",
            "c_ppl_co2scr",
            "cfc_co2scr",
            "coal_ppl",
            "coal_ppl_u",
            "igcc",
            "igcc_ccs",
        ]
    ]
    # biomass
    inv_el_biomass = [
        inv_en_string + x
        for x in ["bio_istig", "bio_istig_ccs", "bio_ppl_co2scr", "bio_ppl"]
    ]
    # geothermal
    inv_el_geot = [inv_en_string + x for x in ["geo_ppl"]]
    # hydro
    inv_el_hydro = [inv_en_string + x for x in ["hydro_hc", "hydro_lc"]]
    # solar
    inv_el_solar = [inv_en_string + x for x in solar_tec]
    # wind
    inv_el_wind = [inv_en_string + x for x in wind_tecs]
    # offgrid technologies
    inv_el_offgrid_urb = ["inv cost|offgrid_urb"]
    inv_el_offgrid_rur = ["inv cost|offgrid_rur"]
    inv_el_offgrid = inv_el_offgrid_urb + inv_el_offgrid_rur

    # inv_el_grid = (
    #     inv_el_nuc
    #     + inv_el_oil
    #     + inv_el_gas
    #     + inv_el_coal
    #     + inv_el_biomass
    #     + inv_el_geot
    #     + inv_el_hydro
    #     + inv_el_solar
    #     + inv_el_wind
    # )
    # transm and distribution
    t_d_tec = [
        "elec_t_d",
        "elec_exp",
        "elec_imp",
    ]
    inv_t_d = [inv_en_string + x for x in t_d_tec]
    # non electricity cathegories aggregated
    # gases
    gas_tec = ["gas_bio", "coal_gas"]
    inv_gas = [inv_en_string + x for x in gas_tec]
    hydrog_tec = [
        "h2_bio_ccs",
        "h2_bio",
        "h2_coal_ccs",
        "h2_coal",
        "h2_elec",
        "h2_smr_ccs",
        "h2_smr",
    ]
    inv_hydrog = [inv_en_string + x for x in hydrog_tec]
    liq_tecs = [
        "eth_bio_ccs",
        "liq_bio_ccs",
        "eth_bio",
        "liq_bio",
        "syn_liq_ccs",
        "meth_coal",
        "syn_liq",
        "meth_ng_ccs",
        "meth_ng",
        "ref_lol",
        "ref_hil",
    ]
    inv_liq = [inv_en_string + x for x in liq_tecs]
    # extraction fossil fue
    extr_coal = ["coal_extr_ch4", "coal_extr", "lignite_extr"]
    inv_extr_coal = [inv_en_string + x for x in extr_coal]
    extr_gas = [
        "gas_extr_1",
        "gas_extr_2",
        "gas_extr_3",
        "gas_extr_4",
        "gas_extr_5",
        "gas_extr_6",
        "gas_extr_7",
        "gas_extr_8",
    ]
    inv_extr_gas = [inv_en_string + x for x in extr_gas]
    extr_oil = [
        "oil_extr_1",
        "oil_extr_2",
        "oil_extr_3",
        "oil_extr_1_ch4",
        "oil_extr_2_ch4",
        "oil_extr_3_ch4",
        "oil_extr_4",
        "oil_extr_4_ch4",
        "oil_extr_5",
        "oil_extr_6",
        "oil_extr_7",
        "oil_extr_8",
    ]
    inv_extr_oil = [inv_en_string + x for x in extr_oil]
    inv_extr = inv_extr_coal + inv_extr_gas + inv_extr_oil
    # irrigation
    inv_irr = [inv_en_string + x for x in irr_tecs]

    # fix and variable costs"
    # tom_iam = report_iam.filter(variable="total om cost|*").variable
    tom_en_string = "total om cost|"
    # nuclear
    tom_el_nuc = [tom_en_string + x for x in ["nuc_fbr", "nuc_hc", "nuc_lc"]]
    # oil
    tom_el_oil = [
        tom_en_string + x
        for x in ["foil_ppl", "loil_cc", "loil_ppl", "oil_ppl", "SO2_scrub_ppl"]
    ]
    # gas
    tom_el_gas = [
        tom_en_string + x
        for x in [
            "gas_cc",
            "gas_cc_ccs",
            "gas_ct",
            "g_ppl_co2scr",
            "gfc_co2scr",
            "gas_htfc",
            "gas_ppl",
        ]
    ]
    # coal
    tom_el_coal = [
        tom_en_string + x
        for x in [
            "coal_adv",
            "coal_adv_ccs",
            "c_ppl_co2scr",
            "cfc_co2scr",
            "coal_ppl",
            "coal_ppl_u",
            "igcc",
            "igcc_ccs",
        ]
    ]
    # biomass
    tom_el_biomass = [
        tom_en_string + x
        for x in ["bio_istig", "bio_istig_ccs", "bio_ppl_co2scr", "bio_ppl"]
    ]
    # geothermal
    tom_el_geot = [tom_en_string + x for x in ["geo_ppl"]]
    # hydro
    tom_el_hydro = [tom_en_string + x for x in ["hydro_hc", "hydro_lc"]]
    # solar
    tom_el_solar = [tom_en_string + x for x in solar_tec]
    # wind
    tom_el_wind = [tom_en_string + x for x in wind_tecs]
    # offgrid technologies
    tom_el_offgrid_urb = ["total om cost|offgrid_urb"]
    tom_el_offgrid_rur = ["total om cost|offgrid_rur"]
    tom_el_offgrid = tom_el_offgrid_urb + tom_el_offgrid_rur

    # tom_el_grid = (
    #     tom_el_nuc
    #     + tom_el_oil
    #     + tom_el_gas
    #     + tom_el_coal
    #     + tom_el_biomass
    #     + tom_el_geot
    #     + tom_el_hydro
    #     + tom_el_solar
    #     + tom_el_wind
    # )
    # transmission & distribtion
    tom_t_d = [tom_en_string + x for x in t_d_tec]
    # non electricity cathegories aggregated
    # gases
    tom_gas = [tom_en_string + x for x in gas_tec]
    tom_hydrog = [tom_en_string + x for x in hydrog_tec]
    tom_liq = [tom_en_string + x for x in liq_tecs]
    # extraction fossil fue
    tom_extr_coal = [tom_en_string + x for x in extr_coal]
    tom_extr_gas = [tom_en_string + x for x in extr_gas]
    tom_extr_oil = [tom_en_string + x for x in extr_oil]
    tom_extr = tom_extr_coal + tom_extr_gas + tom_extr_oil
    # irriation
    tom_irr = [tom_en_string + x for x in irr_tecs]

    # need to convert the unit
    map_agg_pd = pd.DataFrame(
        [
            ["Water Withdrawal|Irrigation", irr_ww, "km3/yr"],
            # useful energy
            ["Useful Energy|Industrial|Country", ind_dem_c, "TWh/yr"],
            ["Useful Energy|Industrial|Basin", ind_dem_b, "TWh/yr"],
            ["Useful Energy|Non Commercial", non_comm_dem, "TWh/yr"],
            ["Useful Energy|Residential & Commercial|Country", rc_dem_c, "TWh/yr"],
            ["Useful Energy|Residential & Commercial|Basin", rc_dem_b, "TWh/yr"],
            ["Useful Energy|Transport", trs_dem, "TWh/yr"],
            ["Useful Energy|Crop Processing", crop_en_dem, "TWh/yr"],
            # final energy irrigation
            ["Final Energy|Commercial|Water|Irrigation", irr_FE, "TWh/yr"],
            # secondary energy
            ["Secondary Energy|Electricity|Coal", electr_coal, "TWh/yr"],
            ["Secondary Energy|Electricity|Biomass", electr_biomass, "TWh/yr"],
            # ["Secondary Energy|Electricity|Fossil", electr_coal + electr_oil + electr_gas, "TWh/yr"],
            ["Secondary Energy|Electricity|Gas", electr_gas, "TWh/yr"],
            ["Secondary Energy|Electricity|Hydro", electr_hydro, "TWh/yr"],
            # ["Secondary Energy|Electricity|Non-Biomass Renewables", electr_solar + electr_wind + electr_hydro, "TWh/yr"],
            ["Secondary Energy|Electricity|Nuclear", electr_nuc, "TWh/yr"],
            ["Secondary Energy|Electricity|Geothermal", electr_nuc, "TWh/yr"],
            ["Secondary Energy|Electricity|Oil", electr_oil, "TWh/yr"],
            ["Secondary Energy|Electricity|Solar", electr_solar, "TWh/yr"],
            ["Secondary Energy|Electricity|Wind", electr_wind, "TWh/yr"],
            # ["Secondary Energy|Electricity|Generation Grid", electr_grid, "TWh/yr"],
            ["Secondary Energy|Electricity|Off-Grid", electr_offgrid, "TWh/yr"],
            # investments
            ["Investment|Energy Supply|Electricity|Coal", inv_el_coal, "Million USD"],
            [
                "Investment|Energy Supply|Electricity|Biomass",
                inv_el_biomass,
                "Million USD",
            ],
            # ["Investment|Energy Supply|Electricity|Fossil", inv_el_coal + inv_el_oil + inv_el_gas, "Million USD"],
            ["Investment|Energy Supply|Electricity|Gas", inv_el_gas, "Million USD"],
            ["Investment|Energy Supply|Electricity|Hydro", inv_el_hydro, "Million USD"],
            # ["Investment|Energy Supply|Electricity|Non-Biomass Renewables", inv_el_solar + inv_el_wind + inv_el_hydro, "Million USD"],
            ["Investment|Energy Supply|Electricity|Nuclear", inv_el_nuc, "Million USD"],
            [
                "Investment|Energy Supply|Electricity|Geothermal",
                inv_el_nuc,
                "Million USD",
            ],
            ["Investment|Energy Supply|Electricity|Oil", inv_el_oil, "Million USD"],
            ["Investment|Energy Supply|Electricity|Solar", inv_el_solar, "Million USD"],
            ["Investment|Energy Supply|Electricity|Wind", inv_el_wind, "Million USD"],
            # ["Investment|Energy Supply|Electricity|Generation Grid", inv_el_grid, "Million USD"],
            [
                "Investment|Energy Supply|Electricity|Off-Grid",
                inv_el_offgrid,
                "Million USD",
            ],
            [
                "Investment|Energy Supply|Electricity|Transmission and Distribution",
                inv_t_d,
                "Million USD",
            ],
            ["Investment|Energy Supply|Gas", inv_gas, "Million USD"],
            ["Investment|Energy Supply|Hydrogen", inv_hydrog, "Million USD"],
            ["Investment|Energy Supply|Liquids", inv_liq, "Million USD"],
            ["Investment|Energy Supply|Extraction|Coal", inv_extr_coal, "Million USD"],
            ["Investment|Energy Supply|Extraction|Gas", inv_extr_gas, "Million USD"],
            ["Investment|Energy Supply|Extraction|Oil", inv_extr_oil, "Million USD"],
            ["Investment|Energy Supply|Extraction", inv_extr, "Million USD"],
            # irrigation
            ["Investment|Infrastructure|Water|Irrigation", inv_irr, "Million USD"],
            # O & M costs
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Coal",
                tom_el_coal,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Biomass",
                tom_el_biomass,
                "Million USD",
            ],
            # ["Total Operation Management Cost|Energy Supply|Electricity|Fossil", tom_el_coal + tom_el_oil + tom_el_gas, "Million USD"],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Gas",
                tom_el_gas,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Hydro",
                tom_el_hydro,
                "Million USD",
            ],
            # ["Total Operation Management Cost|Energy Supply|Electricity|Non-Biomass Renewables", tom_el_solar + tom_el_wind + tom_el_hydro, "Million USD"],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Nuclear",
                tom_el_nuc,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Geothermal",
                tom_el_nuc,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Oil",
                tom_el_oil,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Solar",
                tom_el_solar,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Wind",
                tom_el_wind,
                "Million USD",
            ],
            # ["Total Operation Management Cost|Energy Supply|Electricity|Generation Grid", tom_el_grid, "Million USD"],
            [
                "Total Operation Management Cost|Energy Supply|Electricity|Off-Grid",
                tom_el_offgrid,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|ElectricityTransmission and Distribution",
                tom_t_d,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Gas",
                tom_gas,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Hydrogen",
                tom_hydrog,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Liquids",
                tom_liq,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Extraction|Coal",
                tom_extr_coal,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Extraction|Gas",
                tom_extr_gas,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Extraction|Oil",
                tom_extr_oil,
                "Million USD",
            ],
            [
                "Total Operation Management Cost|Energy Supply|Extraction",
                tom_extr,
                "Million USD",
            ],
            # irrigation
            [
                "Total Operation Management Cost|Infrastructure|Water|Irrigation",
                tom_irr,
                "Million USD",
            ],
        ],
        columns=["names", "list_cat", "unit"],
    )
    map_unit = pd.DataFrame(
        [
            ["Secondary Energy\\|Electricity", 0.03154, "EJ/yr"],  # from GWa to TWh
            ["Investment", 0.001, "billion US$2010/yr"],
            ["Total Operation Management Cost", 0.001, "billion US$2010/yr"],
            ["Useful Energy", 0.03154, "EJ/yr"],
            ["Final Energy", 0.03154, "EJ/yr"],
            ["Water Withdrawal", 1, "km3/yr"],
        ],
        columns=["names", "conv", "unit"],
    )
    rep_iamSE = report_iam.copy()
    for index, row in map_agg_pd.iterrows():
        print(row["names"])
        # Aggregates variables. no higher level in the pipes
        rep_tmp = rep_iamSE.aggregate(row["names"], components=row["list_cat"])
        rep_iamSE = rep_iamSE.append(rep_tmp)
        # aggregate regionally all different variables
        for rr in map_node_dict:
            rep_iamSE.aggregate_region(
                row["names"], region=rr, subregions=map_node_dict[rr], append=True
            )

    # filter only useful variables
    rep_iamSE = rep_iamSE.filter(variable=map_agg_pd.names)
    # filter only subannual
    rep_iamSE_sub = rep_iamSE.filter(subannual="year", keep=False)
    # aggregate subannual time for different variables
    sub_iam = rep_iamSE_sub
    for vv in rep_iamSE_sub.variable:
        print(vv)
        temp_iam = rep_iamSE_sub.aggregate_time(vv, column="subannual", value="year")
        sub_iam = sub_iam.append(temp_iam)

    rep_iamSE_sub = sub_iam.filter(subannual="year")
    # merge new summed-up timeseries
    rep_iamSE = rep_iamSE.append(rep_iamSE_sub)

    # 3) then aggregate the group categories
    agg_list = [
        "Secondary Energy|Electricity",
        "Investment|Energy Supply|Electricity",
        "Investment|Energy Supply",
        "Total Operation Management Cost|Energy Supply|Electricity",
        "Total Operation Management Cost|Energy Supply",
        "Useful Energy|Industrial",
        "Useful Energy|Residential & Commercial",
        "Useful Energy",
    ]
    for vv in agg_list:
        print(vv)
        rep_iamSE.aggregate(variable=vv, recursive=True, append=True)

    # add units TODO
    report_pd = rep_iamSE.as_pandas()
    # report_pd = report_pd.drop(columns=["exclude"])
    # TEMP FIX
    # Check if the "value" column exists in the DataFrame
    if "value" not in report_pd.columns:
        # Replace the name of the last column with "value"
        last_column_name = report_pd.columns[-1]
        report_pd.rename(columns={last_column_name: "value"}, inplace=True)

    for index, row in map_unit.iterrows():
        print(row["conv"])
        condition = report_pd["variable"].str.contains(row["names"])
        print(report_pd.loc[condition, "variable"].unique())
        report_pd.loc[condition, "unit"] = row["unit"]
        report_pd.loc[condition, "value"] = (
            report_pd.loc[condition, "value"] * row["conv"]
        )

    out_path = package_data_path().parents[0] / "reporting_output/"
    if not out_path.exists():
        out_path.mkdir()
    out_file = out_path / f"{sc.model}_{sc.scenario}_en.csv"
    report_pd.to_csv(out_file, index=False)

    return report_pd


def prep_submission_leapre(ts1, ts2):
    """Re-agregates variables that come from separated reporting flows,
    namely nexus and leap-re-country"""

    if ts1.empty or ts2.empty:
        print("One of the input dataframes is empty. The function cannot be run.")
        return

    varstoagg = [
        "Investment|Infrastructure|Water",
        "Investment|Infrastructure",
        "Investment",
        "Total Operation Management Cost|Infrastructure|Water",
        "Total Operation Management Cost|Infrastructure",
        "Total Operation Management Cost",
        "Final Energy|Commercial|Water",
        "Final Energy|Commercial",
        "Water Withdrawal",
        # 'Water Withdrawal'
    ]
    ts1 = ts1[ts2.columns]
    df_m_all = pd.DataFrame()
    for v in varstoagg:
        print("Aggregate: ", v)
        ts1 = ts1[ts1["variable"] != v]
        ts2 = ts2[ts2["variable"] != v]
        ts_in = pd.concat([ts1, ts2])
        df_p = pyam.IamDataFrame(ts_in)
        df_p.aggregate(variable=v, recursive="skip-validate", append=True)
        df_p = df_p.filter(variable=v)
        df_m = df_p.as_pandas()
        # df_m = df_m.drop(columns=["exclude"])
        df_m.reset_index(inplace=True, drop=True)
        df_m_all = pd.concat([df_m_all, df_m])

    ts_out = pd.concat([ts_in, df_m_all])

    return ts_out


# "skip-validate"


def report_all_leapre(sc, reg, sdgs):
    report(sc, reg, sdgs)

    ts = sc.timeseries()
    # load new part of country reporting
    ts_c = report_country(sc)
    # vars1 = ts.variable.unique()
    # vars2 = ts2.variable.unique()
    # [x for x in vars1 if x in vars2]
    # ts1 = ts.copy()
    # ts2 = ts_c.copy()
    ts_out = prep_submission_leapre(ts, ts_c)

    out_path = package_data_path().parents[0] / "reporting_output/"
    if not out_path.exists():
        out_path.mkdir()
    out_file = out_path / f"{sc.model}_{sc.scenario}_leap-re.csv"
    ts_out.to_csv(out_file, index=False)
    print("Leap-re reporting is completed")
