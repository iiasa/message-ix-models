import logging

import numpy as np
import pandas as pd
import pyam
from message_ix.reporting import Reporter

from message_ix_models.util import private_data_path

try:
    from message_data.tools.post_processing.iamc_report_hackathon import (
        report as old_reporting,
    )
except ImportError:  # message_data not installed
    old_reporting = None

log = logging.getLogger(__name__)


def run_old_reporting(sc=False):
    mp2 = sc.platform

    log.info(
        " Start reporting of the global energy system (old reporting scheme)"
        f"for the scenario {sc.model}.{sc.scenario}"
    )
    old_reporting(
        mp=mp2,
        scen=sc,
        merge_hist=True,
        merge_ts=False,
    )


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


def report(sc=False, sdgs=False):
    """Report nexus module results"""

    # Generating reporter
    rep = Reporter.from_scenario(sc)
    report = rep.get("message::default")
    # Create a timeseries dataframe
    report_df = report.timeseries()
    report_df.reset_index(inplace=True)
    report_df.columns = report_df.columns.astype(str)
    report_df.columns = report_df.columns.str.title()

    # Removing duplicate region names
    report_df["Region"] = remove_duplicate(report_df)

    # Adding Water availability as resource in demands
    # This is not automatically reported using message:default
    rep_dm = rep
    rep_dm.set_filters(l="water_avail_basin")

    def collapse_callback(df):
        """Callback function to populate the IAMC 'variable' column."""
        df["variable"] = "Water Resource|" + df["c"]
        return df.drop(["c"], axis=1)

    # Mapping from dimension IDs to column names
    rename = dict(n="region", y="year")

    key = rep_dm.convert_pyam("demand", rename=rename, collapse=collapse_callback)
    # Making a dataframe for demands
    df_dmd = rep.get(key).as_pandas()
    df_dmd["value"] = df_dmd["value"].abs()
    df_dmd["variable"].replace(
        "Water Resource|groundwater_basin", "Water Resource|Groundwater", inplace=True
    )
    df_dmd["variable"].replace(
        "Water Resource|surfacewater_basin",
        "Water Resource|Surface Water",
        inplace=True,
    )
    df_dmd = df_dmd.drop(columns=["exclude"])
    df_dmd["unit"] = "km3"
    df_dmd1 = pyam.IamDataFrame(df_dmd).timeseries()

    # Convert to pyam dataframe
    report_iam = pyam.IamDataFrame(report_df)
    # Merge both dataframes in pyam
    report_iam = report_iam.append(df_dmd1)

    # mapping model outputs for aggregation
    urban_infrastructure = [
        "CAP_NEW|new capacity|rural_recycle",
        "CAP_NEW|new capacity|rural_sewerage",
        "CAP_NEW|new capacity|rural_t_d",
        "CAP_NEW|new capacity|rural_treatment",
        "CAP_NEW|new capacity|rural_unconnected",
        "CAP_NEW|new capacity|rural_untreated",
    ]

    rural_infrastructure = [
        "CAP_NEW|new capacity|rural_recycle",
        "CAP_NEW|new capacity|rural_sewerage",
        "CAP_NEW|new capacity|rural_t_d",
        "CAP_NEW|new capacity|rural_treatment",
        "CAP_NEW|new capacity|rural_unconnected",
        "CAP_NEW|new capacity|rural_untreated",
    ]

    urban_treatment_recycling = [
        "CAP_NEW|new capacity|urban_recycle",
        "CAP_NEW|new capacity|urban_sewerage",
        "CAP_NEW|new capacity|urban_treatment",
    ]

    rural_treatment_recycling = [
        "CAP_NEW|new capacity|rural_recycle",
        "CAP_NEW|new capacity|rural_sewerage",
        "CAP_NEW|new capacity|rural_treatment",
    ]

    rural_dist = ["CAP_NEW|new capacity|rural_t_d"]
    urban_dist = ["CAP_NEW|new capacity|urban_t_d"]

    rural_unconnected = [
        "CAP_NEW|new capacity|rural_unconnected",
        "CAP_NEW|new capacity|rural_untreated",
    ]

    urban_unconnected = [
        "CAP_NEW|new capacity|urban_unconnected",
        "CAP_NEW|new capacity|urban_untreated",
    ]

    industry_unconnected = [
        "CAP_NEW|new capacity|industry_unconnected",
        "CAP_NEW|new capacity|industry_untreated",
    ]

    extrt_sw_cap = ["CAP_NEW|new capacity|extract_surfacewater"]
    extrt_gw_cap = ["CAP_NEW|new capacity|extract_groundwater"]
    extrt_fgw_cap = ["CAP_NEW|new capacity|extract_gw_fossil"]

    extrt_sw_inv = ["inv cost|extract_surfacewater"]
    extrt_gw_inv = ["inv cost|extract_groundwater"]
    # Calculating fossil groundwater invwatments
    # 163.56 million USD/km3 x 2 times the reneewable gw costs

    report_iam = report_iam.append(
        report_iam.multiply(
            "CAP_NEW|new capacity|extract_gw_fossil",
            163.56,
            "Fossil GW inv",
            ignore_units=True,
        )
    )
    extrt_fgw_inv = report_iam.filter(variable="Fossil GW inv").variable

    rural_infrastructure_inv = [
        "inv cost|rural_recycle",
        "inv cost|rural_sewerage",
        "inv cost|rural_t_d",
        "inv cost|rural_treatment",
        "inv cost|rural_unconnected",
        "inv cost|rural_untreated",
    ]

    urban_infrastructure_inv = [
        "inv cost|urban_recycle",
        "inv cost|urban_sewerage",
        "inv cost|urban_t_d",
        "inv cost|urban_treatment",
        "inv cost|urban_unconnected",
        "inv cost|urban_untreated",
    ]

    urban_treatment_recycling_inv = [
        "inv cost|urban_recycle",
        "inv cost|urban_sewerage",
        "inv cost|urban_treatment",
    ]

    rural_treatment_recycling_inv = [
        "inv cost|rural_recycle",
        "inv cost|rural_sewerage",
        "inv cost|rural_treatment",
    ]

    rural_dist_inv = ["inv cost|rural_t_d"]
    urban_dist_inv = ["inv cost|urban_t_d"]

    rural_unconnected_inv = [
        "inv cost|rural_unconnected",
        "inv cost|rural_untreated",
    ]

    urban_unconnected_inv = [
        "inv cost|urban_unconnected",
        "inv cost|urban_untreated",
    ]

    industry_unconnected_inv = [
        "inv cost|industry_unconnected",
        "inv cost|industry_untreated",
    ]

    saline_inv = [
        "inv cost|membrane",
        "inv cost|distillation",
    ]
    saline_totalom = [
        "total om cost|membrane",
        "total om cost|distillation",
    ]

    extrt_fgw_om = ["total om cost|extract_gw_fossil"]

    urban_infrastructure_totalom = [
        "total om cost|urban_recycle",
        "total om cost|urban_sewerage",
        "total om cost|urban_t_d",
        "total om cost|urban_treatment",
        "total om cost|urban_unconnected",
        "total om cost|urban_untreated",
    ]

    rural_infrastructure_totalom = [
        "total om cost|rural_recycle",
        "total om cost|rural_sewerage",
        "total om cost|rural_t_d",
        "total om cost|rural_treatment",
        "total om cost|rural_unconnected",
        "total om cost|rural_untreated",
    ]

    rural_treatment_recycling_totalom = [
        "total om cost|rural_recycle",
        "total om cost|rural_sewerage",
        "total om cost|rural_treatment",
    ]

    urban_treatment_recycling_totalom = [
        "total om cost|urban_recycle",
        "total om cost|urban_sewerage",
        "total om cost|urban_treatment",
    ]

    rural_dist_totalom = ["total om cost|rural_t_d"]
    urban_dist_totalom = ["total om cost|urban_t_d"]

    rural_unconnected_totalom = [
        "total om cost|rural_unconnected",
        "total om cost|rural_untreated",
    ]

    urban_unconnected_totalom = [
        "total om cost|urban_unconnected",
        "total om cost|urban_untreated",
    ]

    industry_unconnected_totalom = [
        "total om cost|industry_unconnected",
        "total om cost|industry_untreated",
    ]

    extract_sw = ["in|water_avail_basin|surfacewater_basin|extract_surfacewater|M1"]

    extract_gw = ["in|water_avail_basin|groundwater_basin|extract_groundwater|M1"]
    extract_fgw = ["out|water_supply_basin|freshwater_basin|extract_gw_fossil|M1"]

    desal_membrane = ["out|water_supply_basin|freshwater_basin|membrane|M1"]
    desal_distill = ["out|water_supply_basin|freshwater_basin|distillation|M1"]
    env_flow = ["in|water_avail_basin|surfacewater_basin|return_flow|M1"]
    gw_recharge = ["in|water_avail_basin|groundwater_basin|gw_recharge|M1"]

    rural_mwdem_unconnected = ["out|final|rural_disconnected|rural_unconnected|M1"]
    rural_mwdem_unconnected_eff = ["out|final|rural_disconnected|rural_unconnected|Mf"]
    rural_mwdem_connected = ["out|final|rural_mw|rural_t_d|M1"]
    rural_mwdem_connected_eff = ["out|final|rural_mw|rural_t_d|Mf"]
    urban_mwdem_unconnected = ["out|final|urban_disconnected|urban_unconnected|M1"]
    urban_mwdem_unconnected_eff = ["out|final|urban_disconnected|urban_unconnected|Mf"]
    urban_mwdem_connected = ["out|final|urban_mw|urban_t_d|M1"]
    urban_mwdem_connected_eff = ["out|final|urban_mw|urban_t_d|Mf"]
    industry_mwdem_unconnected = ["out|final|industry_mw|industry_unconnected|M1"]

    electr_gw = ["in|final|electr|extract_groundwater|M1"]
    electr_fgw = ["in|final|electr|extract_gw_fossil|M1"]
    electr_sw = ["in|final|electr|extract_surfacewater|M1"]
    extract_saline_region = ["out|saline_supply|saline_ppl|extract_salinewater|M1"]
    extract_saline_basin = [
        "out|water_avail_basin|salinewater_basin|extract_salinewater_basin|M1"
    ]
    electr_rural_trt = ["in|final|electr|rural_sewerage|M1"]
    electr_urban_trt = ["in|final|electr|urban_sewerage|M1"]
    electr_urban_recycle = ["in|final|electr|urban_recycle|M1"]
    electr_rural_recycle = ["in|final|electr|rural_recycle|M1"]
    electr_saline = [
        "in|final|electr|distillation|M1",
        "in|final|electr|distillation|M1",
    ]

    electr_urban_t_d = ["in|final|electr|urban_t_d|M1"]
    electr_urban_t_d_eff = ["in|final|electr|urban_t_d|Mf"]
    electr_rural_t_d = ["in|final|electr|rural_t_d|M1"]
    electr_rural_t_d_eff = ["in|final|electr|rural_t_d|Mf"]

    electr_irr = [
        "in|final|electr|irrigation_cereal|M1",
        "in|final|electr|irrigation_oilcrops|M1",
        "in|final|electr|irrigation_sugarcrops|M1",
    ]

    urban_collctd_wstwtr = ["in|final|urban_collected_wst|urban_sewerage|M1"]
    rural_collctd_wstwtr = ["in|final|rural_collected_wst|rural_sewerage|M1"]

    urban_treated_wstwtr = ["in|water_treat|urban_collected_wst|urban_recycle|M1"]
    rural_treated_wstwtr = ["in|water_treat|rural_collected_wst|rural_recycle|M1"]

    urban_wstwtr_recycle = ["out|water_supply_basin|freshwater_basin|urban_recycle|M1"]
    rural_wstwtr_recycle = ["out|water_supply_basin|freshwater_basin|rural_recycle|M1"]

    urban_transfer = ["in|water_supply_basin|freshwater_basin|urban_t_d|M1"]
    urban_transfer_eff = ["in|water_supply_basin|freshwater_basin|urban_t_d|Mf"]
    rural_transfer = ["in|water_supply_basin|freshwater_basin|rural_t_d|M1"]
    rural_transfer_eff = ["in|water_supply_basin|freshwater_basin|rural_t_d|Mf"]

    # irr_water = ["out|water_irr|freshwater|irrigation|M1"]

    irr_c = ["in|water_supply|freshwater|irrigation_cereal|M1"]
    irr_o = ["in|water_supply|freshwater|irrigation_oilcrops|M1"]
    irr_s = ["in|water_supply|freshwater|irrigation_sugarcrops|M1"]

    region_withdr = report_iam.filter(
        variable="in|water_supply_basin|freshwater_basin|basin_to_reg|*"
    ).variable

    cooling_saline_inv = report_iam.filter(variable="inv cost|*saline").variable
    cooling_air_inv = report_iam.filter(variable="inv cost|*air").variable
    cooling_ot_fresh = report_iam.filter(variable="inv cost|*ot_fresh").variable
    cooling_cl_fresh = report_iam.filter(variable="inv cost|*cl_fresh").variable

    elec_hydro_var = report_iam.filter(variable="out|secondary|electr|hydro*").variable

    for var in elec_hydro_var:
        if "hydro_1" in var or "hydro_hc" in var:
            report_iam = report_iam.append(
                # Multiply electricity output of hydro to get withdrawals
                # this is an ex-post model calculation and the values are taken from
                # data/water/ppl_cooling_tech/tech_water_performance_ssp_msg.csv
                # for hydr_n water_withdrawal_mid_m3_per output is converted by
                # multiplying with   60 * 60* 24 * 365 * 1e-9 to convert it
                # into km3/output
                report_iam.multiply(
                    f"{var}", 0.161, f"Water Withdrawal|Electricity|Hydro|{var[21:28]}"
                )
            )
        else:
            report_iam = report_iam.append(
                report_iam.multiply(
                    f"{var}", 0.323, f"Water Withdrawal|Electricity|Hydro|{var[21:28]}"
                )
            )

    water_hydro_var = report_iam.filter(
        variable="Water Withdrawal|Electricity|Hydro|*"
    ).variable

    # mapping for aggregation
    map_agg_pd = pd.DataFrame(
        [
            ["Water Extraction", extract_gw + extract_fgw + extract_sw, "km3/yr"],
            ["Water Extraction|Groundwater", extract_gw, "km3/yr"],
            ["Water Extraction|Brackish Water", extract_fgw, "km3/yr"],
            ["Water Extraction|Surface Water", extract_sw, "km3/yr"],
            [
                "Water Extraction|Seawater",
                extract_saline_basin + extract_saline_region,
                "km3/yr",
            ],
            ["Water Extraction|Seawater|Desalination", extract_saline_basin, "km3/yr"],
            ["Water Extraction|Seawater|Cooling", extract_saline_region, "km3/yr"],
            ["Water Desalination", desal_membrane + desal_distill, "km3/yr"],
            ["Water Desalination|Membrane", desal_membrane, "km3/yr"],
            ["Water Desalination|Distillation", desal_distill, "km3/yr"],
            [
                "Water Transfer",
                urban_transfer
                + rural_transfer
                + urban_transfer_eff
                + rural_transfer_eff,
                "km3/yr",
            ],
            ["Water Transfer|Urban", urban_transfer + urban_transfer_eff, "km3/yr"],
            ["Water Transfer|Rural", rural_transfer + rural_transfer_eff, "km3/yr"],
            [
                "Water Withdrawal",
                region_withdr
                + rural_mwdem_unconnected
                + rural_mwdem_unconnected_eff
                + rural_mwdem_connected
                + rural_mwdem_connected_eff
                + urban_mwdem_connected
                + urban_mwdem_connected_eff
                + urban_mwdem_unconnected
                + urban_mwdem_unconnected_eff
                + industry_mwdem_unconnected,
                "km3/yr",
            ],
            ["Water Withdrawal|Energy techs & Irrigation", region_withdr, "km3/yr"],
            # ["Water Withdrawal|Irrigation", irr_c + irr_o + irr_s, "km3/yr"],
            ["Water Withdrawal|Irrigation|Cereal", irr_c, "km3/yr"],
            ["Water Withdrawal|Irrigation|Oil Crops", irr_o, "km3/yr"],
            ["Water Withdrawal|Irrigation|Sugar Crops", irr_s, "km3/yr"],
            ["Water Withdrawal|Electricity|Hydro", water_hydro_var, "km3/yr"],
            [
                "Capacity Additions|Infrastructure|Water",
                rural_infrastructure
                + urban_infrastructure
                + urban_treatment_recycling
                + rural_treatment_recycling
                + urban_dist
                + rural_dist
                + rural_unconnected
                + urban_unconnected
                + industry_unconnected,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Extraction",
                extrt_sw_cap + extrt_gw_cap + extrt_fgw_cap,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Extraction|Surface Water",
                extrt_sw_cap,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Extraction|Groundwater",
                extrt_gw_cap + extrt_fgw_cap,
                "km3/yr",
            ],
            [
                (
                    "Capacity"
                    " Additions|Infrastructure|Water|Extraction|Groundwater|Renewable"
                ),
                extrt_gw_cap,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Extraction|Groundwater|Fossil",
                extrt_fgw_cap,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Rural",
                rural_infrastructure,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Urban",
                urban_infrastructure,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Industrial",
                industry_unconnected,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Treatment & Recycling|Urban",
                urban_treatment_recycling,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Treatment & Recycling|Rural",
                rural_treatment_recycling,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Distribution|Rural",
                rural_dist,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Distribution|Urban",
                urban_dist,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Unconnected|Rural",
                rural_unconnected,
                "km3/yr",
            ],
            [
                "Capacity Additions|Infrastructure|Water|Unconnected|Urban",
                urban_unconnected,
                "km3/yr",
            ],
            ["Freshwater|Environmental Flow", env_flow, "km3/yr"],
            ["Groundwater Recharge", gw_recharge, "km3/yr"],
            [
                "Water Withdrawal|Municipal Water",
                rural_mwdem_unconnected
                + rural_mwdem_unconnected_eff
                + rural_mwdem_connected
                + rural_mwdem_connected_eff
                + urban_mwdem_unconnected
                + urban_mwdem_unconnected_eff
                + urban_mwdem_connected
                + urban_mwdem_connected_eff,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Unconnected|Rural",
                rural_mwdem_unconnected,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Unconnected|Rural Eff",
                rural_mwdem_unconnected_eff,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Connected|Rural",
                rural_mwdem_connected,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Connected|Rural Eff",
                rural_mwdem_connected_eff,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Unconnected|Urban",
                urban_mwdem_unconnected,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Unconnected|Urban Eff",
                urban_mwdem_unconnected_eff,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Connected|Urban",
                urban_mwdem_connected,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Municipal Water|Connected|Urban Eff",
                urban_mwdem_connected_eff,
                "km3/yr",
            ],
            [
                "Water Withdrawal|Industrial Water|Unconnected",
                industry_mwdem_unconnected,
                "km3/yr",
            ],
            # ["Water Withdrawal|Irrigation", irr_water, "km3/yr"],
            [
                "Final Energy|Commercial",
                electr_saline
                + electr_gw
                + electr_fgw
                + electr_sw
                + electr_rural_trt
                + electr_urban_trt
                + electr_urban_recycle
                + electr_rural_recycle
                + electr_urban_t_d
                + electr_urban_t_d_eff
                + electr_rural_t_d
                + electr_rural_t_d_eff
                + electr_irr,
                "GWa",
            ],
            [
                "Final Energy|Commercial|Water",
                electr_saline
                + electr_gw
                + electr_fgw
                + electr_sw
                + electr_rural_trt
                + electr_urban_trt
                + electr_urban_recycle
                + electr_rural_recycle
                + electr_urban_t_d
                + electr_urban_t_d_eff
                + electr_rural_t_d
                + electr_rural_t_d_eff
                + electr_irr,
                "GWa",
            ],
            ["Final Energy|Commercial|Water|Desalination", electr_saline, "GWa"],
            [
                "Final Energy|Commercial|Water|Groundwater Extraction",
                electr_gw + electr_fgw,
                "GWa",
            ],
            [
                "Final Energy|Commercial|Water|Surface Water Extraction",
                electr_sw,
                "GWa",
            ],
            ["Final Energy|Commercial|Water|Irrigation", electr_irr, "GWa"],
            ["Final Energy|Commercial|Water|Treatment", electr_rural_trt, "GWa"],
            [
                "Final Energy|Commercial|Water|Treatment|Rural",
                electr_urban_trt + electr_rural_trt,
                "GWa",
            ],
            ["Final Energy|Commercial|Water|Treatment|Urban", electr_urban_trt, "GWa"],
            ["Final Energy|Commercial|Water|Reuse", electr_urban_recycle, "GWa"],
            [
                "Final Energy|Commercial|Water|Transfer",
                electr_urban_t_d
                + electr_urban_t_d_eff
                + electr_rural_t_d
                + electr_rural_t_d_eff,
                "GWa",
            ],
            [
                "Final Energy|Commercial|Water|Transfer|Urban",
                electr_urban_t_d + electr_urban_t_d_eff,
                "GWa",
            ],
            [
                "Final Energy|Commercial|Water|Transfer|Rural",
                electr_rural_t_d + electr_urban_t_d_eff,
                "GWa",
            ],
            [
                "Water Waste|Collected",
                urban_collctd_wstwtr + rural_collctd_wstwtr,
                "km3/yr",
            ],
            ["Water Waste|Collected|Urban", urban_collctd_wstwtr, "km3/yr"],
            ["Water Waste|Collected|Rural", rural_collctd_wstwtr, "km3/yr"],
            [
                "Water Waste|Treated",
                urban_treated_wstwtr + rural_treated_wstwtr,
                "km3/yr",
            ],
            ["Water Waste|Treated|Urban", urban_treated_wstwtr, "km3/yr"],
            ["Water Waste|Treated|Rural", rural_treated_wstwtr, "km3/yr"],
            [
                "Water Waste|Reuse",
                urban_wstwtr_recycle + rural_wstwtr_recycle,
                "km3/yr",
            ],
            ["Water Waste|Reuse|Urban", urban_wstwtr_recycle, "km3/yr"],
            ["Water Waste|Reuse|Rural", rural_wstwtr_recycle, "km3/yr"],
            [
                "Investment|Infrastructure|Water",
                rural_infrastructure_inv
                + urban_infrastructure_inv
                + extrt_sw_inv
                + extrt_gw_inv
                + extrt_fgw_inv
                + saline_inv
                + cooling_ot_fresh
                + cooling_cl_fresh
                + cooling_saline_inv
                + cooling_air_inv,
                +industry_unconnected_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Extraction",
                extrt_sw_inv + extrt_gw_inv + extrt_fgw_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Other",
                extrt_sw_inv + extrt_gw_inv + extrt_fgw_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Extraction|Surface",
                extrt_sw_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Extraction|Groundwater",
                extrt_gw_inv + extrt_fgw_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Extraction|Groundwater|Fossil",
                extrt_fgw_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Extraction|Groundwater|Renewable",
                extrt_gw_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Desalination",
                saline_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Cooling",
                cooling_ot_fresh
                + cooling_cl_fresh
                + cooling_saline_inv
                + cooling_air_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Cooling|Once through freshwater",
                cooling_ot_fresh,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Cooling|Closed loop freshwater",
                cooling_cl_fresh,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Cooling|Once through saline",
                cooling_saline_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Cooling|Air cooled",
                cooling_air_inv,
                "million US$2010/yr",
            ],
            # [
            #     "Investment|Infrastructure|Water",
            #     rural_infrastructure_inv + urban_infrastructure_inv,
            #     "million US$2010/yr",
            # ],
            [
                "Investment|Infrastructure|Water|Rural",
                rural_infrastructure_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Urban",
                urban_infrastructure_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Industrial",
                industry_unconnected_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Treatment & Recycling",
                urban_treatment_recycling_inv + rural_treatment_recycling_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Treatment & Recycling|Urban",
                urban_treatment_recycling_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Treatment & Recycling|Rural",
                rural_treatment_recycling_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Distribution",
                rural_dist_inv + urban_dist_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Distribution|Rural",
                rural_dist_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Distribution|Urban",
                urban_dist_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Unconnected",
                rural_unconnected_inv
                + urban_unconnected_inv
                + industry_unconnected_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Unconnected|Rural",
                rural_unconnected_inv,
                "million US$2010/yr",
            ],
            [
                "Investment|Infrastructure|Water|Unconnected|Urban",
                urban_unconnected_inv,
                "million US$2010/yr",
            ],
            [
                "Total Operation Management Cost|Infrastructure|Water|Desalination",
                saline_totalom,
                "million US$2010/yr",
            ],
            [
                "Total Operation Management Cost|Infrastructure|Water|Extraction",
                extrt_fgw_om,
                "million US$2010/yr",
            ],
            [
                "Total Operation Management Cost|Infrastructure|Water|Rural",
                rural_infrastructure_totalom,
                "million US$2010/yr",
            ],
            [
                "Total Operation Management Cost|Infrastructure|Water|Urban",
                urban_infrastructure_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management Cost|Infrastructure|Water|Treatment &"
                    " Recycling"
                ),
                urban_treatment_recycling_totalom + rural_treatment_recycling_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management Cost|Infrastructure|Water|Treatment &"
                    " Recycling|Urban"
                ),
                urban_treatment_recycling_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management Cost|Infrastructure|Water|Treatment &"
                    " Recycling|Rural"
                ),
                rural_treatment_recycling_totalom,
                "million US$2010/yr",
            ],
            [
                "Total Operation Management Cost|Infrastructure|Water| Distribution",
                rural_dist_totalom + rural_dist_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management"
                    " Cost|Infrastructure|Water|Distribution|Rural"
                ),
                rural_dist_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management"
                    " Cost|Infrastructure|Water|Distribution|Urban"
                ),
                urban_dist_totalom,
                "million US$2010/yr",
            ],
            [
                "Total Operation Management Cost|Infrastructure|Water|Unconnected",
                rural_unconnected_totalom
                + urban_unconnected_totalom
                + industry_unconnected_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management"
                    " Cost|Infrastructure|Water|Unconnected|Rural"
                ),
                rural_unconnected_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management"
                    " Cost|Infrastructure|Water|Unconnected|Urban"
                ),
                urban_unconnected_totalom,
                "million US$2010/yr",
            ],
            [
                (
                    "Total Operation Management"
                    " Cost|Infrastructure|Water|Unconnected|Industry"
                ),
                industry_unconnected_totalom,
                "million US$2010/yr",
            ],
        ],
        columns=["names", "list_cat", "unit"],
    )

    # add population with sanitation or drinking water access
    mp2 = sc.platform
    map_node = sc.set("map_node")
    # this might not be the best way to get the region, better from context
    if "R11" in map_node.node.to_list()[1]:
        reg = "R11"
    elif "R12" in map_node.node.to_list()[1]:
        reg = "R12"
    else:
        print("Check the region of the model is consistent with R11,R12")

    # load data on water and sanitation access
    load_path = private_data_path("water", "demands", "harmonized", reg)
    all_rates = pd.read_csv(load_path / "all_rates_SSP2.csv")

    pop_check = sc.timeseries(variable="Population")
    pop_check = pop_check[pop_check.year >= 2020]
    if pop_check.empty:
        print("The Population data does not exist or timeseries() has no future values")
    else:
        pop_drink_tot = pd.DataFrame()
        pop_sani_tot = pd.DataFrame()
        pop_sdg6 = pd.DataFrame()
        for ur in ["urban", "rural"]:
            # CHANGE TO URBAN AND RURAL POP
            pop_tot = sc.timeseries(variable=("Population|" + ur.capitalize()))
            pop_tot = pop_tot[-(pop_tot.region == "GLB region (R11)")]
            pop_reg = np.unique(pop_tot["region"])
            # need to change names
            reg_map = mp2.regions()
            reg_map = reg_map[reg_map.mapped_to.isin(pop_reg)].drop(
                columns=["parent", "hierarchy"]
            )
            reg_map["region"] = [x.split("_")[1] for x in reg_map.region]

            df_rate = all_rates[all_rates.variable.str.contains(ur)]

            df_rate = df_rate[
                df_rate.variable.str.contains("sdg" if sdgs else "baseline")
            ]

            df_rate["region"] = [x.split("|")[1] for x in df_rate.node]
            df_rate = df_rate.drop(columns=["node"])
            # make region mean (no weighted average)
            df_rate = (
                df_rate.groupby(["year", "variable", "region"])["value"]
                .mean()
                .reset_index()
            )
            # convert region name
            df_rate = df_rate.merge(reg_map, how="left")
            df_rate = df_rate.drop(columns=["region"])
            df_rate = df_rate.rename(
                columns={"mapped_to": "region", "variable": "new_var", "value": "rate"}
            )

            # Population|Drinking Water Access
            df_drink = df_rate[df_rate.new_var.str.contains("connection")]
            pop_drink = pop_tot.merge(df_drink, how="left")
            pop_drink["variable"] = (
                "Population|Drinking Water Access|" + ur.capitalize()
            )
            pop_drink["value"] = pop_drink.value * pop_drink.rate
            cols = pop_tot.columns
            pop_drink = pop_drink[cols]
            pop_drink_tot = pop_drink_tot.append(pop_drink)
            pop_sdg6 = pop_sdg6.append(pop_drink)

            # Population|Sanitation Acces
            df_sani = df_rate[df_rate.new_var.str.contains("treatment")]
            pop_sani = pop_tot.merge(df_sani, how="left")
            pop_sani["variable"] = "Population|Sanitation Access|" + ur.capitalize()
            pop_sani["value"] = pop_sani.value * pop_sani.rate
            pop_sani = pop_sani[cols]
            pop_sani_tot = pop_sani_tot.append(pop_drink)
            pop_sdg6 = pop_sdg6.append(pop_sani)

        # total values
        pop_drink_tot = (
            pop_drink_tot.groupby(["region", "unit", "year", "model", "scenario"])[
                "value"
            ]
            .sum()
            .reset_index()
        )
        pop_drink_tot["variable"] = "Population|Drinking Water Access"
        pop_drink_tot = pop_drink_tot[cols]
        pop_sani_tot = (
            pop_sani_tot.groupby(["region", "unit", "year", "model", "scenario"])[
                "value"
            ]
            .sum()
            .reset_index()
        )
        pop_sani_tot["variable"] = "Population|Sanitation Access"
        pop_sani_tot = pop_sani_tot[cols]
        # global values
        pop_sdg6 = pop_sdg6.append(pop_drink_tot).append(pop_sani_tot)
        pop_sdg6_glb = (
            pop_sdg6.groupby(["variable", "unit", "year", "model", "scenario"])["value"]
            .sum()
            .reset_index()
        )
        pop_sdg6_glb["region"] = "World"
        pop_sdg6_glb = pop_sdg6_glb[cols]

        pop_sdg6 = pop_sdg6.append(pop_sdg6_glb)
        print("Population|Drinking Water Access")

    # Add water prices, ad-hoc procedure
    wp = sc.var(
        "PRICE_COMMODITY", {"commodity": ["urban_mw", "rural_mw", "freshwater"]}
    )
    wp["value"] = wp["lvl"] / 1000
    wp["unit"] = "US$2010/m3"
    wp = wp.rename(columns={"node": "region"})
    # get withdrawals for weighted mean
    ww = report_iam.as_pandas()
    ww = ww[
        ww.variable.isin(
            ["out|final|rural_mw|rural_t_d|M1", "out|final|urban_mw|urban_t_d|M1"]
        )
    ]
    ww["commodity"] = np.where(
        ww.variable.str.contains("urban_mw"), "urban_mw", "rural_mw"
    )
    ww["wdr"] = ww["value"]
    ww = ww[["region", "year", "commodity", "wdr"]]
    # irrigation water, at regional level
    wp_irr = wp[wp.level == "water_irr"]
    wp_irr["variable"] = "Price|Irrigation Water"
    wp_irr = wp_irr.drop(columns={"level", "lvl", "mrg"})
    # driking water
    wr_dri = wp[wp.commodity.isin(["urban_mw", "rural_mw"])]
    wr_dri = wr_dri.drop(columns={"level", "lvl", "mrg"})
    wr_dri = wr_dri.merge(ww, how="left")
    wr_dri["variable"] = np.where(
        wr_dri.commodity == "urban_mw",
        "Price|Drinking Water|Urban",
        "Price|Drinking Water|Rural",
    )
    wr_dri_m = (
        wr_dri.groupby(["region", "unit", "year"])
        .apply(lambda x: np.average(x.value, weights=x.wdr))
        .reset_index()
    )
    wr_dri_m["value"] = wr_dri_m[0]
    wr_dri_m = wr_dri_m.drop(columns={0})
    wr_dri_m["variable"] = "Price|Drinking Water"

    wp = wp_irr.append(wr_dri).append(wr_dri_m)

    wp["model"] = sc.model
    wp["scenario"] = sc.scenario
    wp = wp[["model", "scenario", "region", "variable", "unit", "year", "value"]]

    wp_iam = pyam.IamDataFrame(wp)
    # Merge both dataframes in pyam
    report_iam = report_iam.append(wp_iam)

    # Fetching nodes from the scenario to aggregate to MESSAGE energy region definition
    map_node = sc.set("map_node")
    map_node = map_node[map_node["node_parent"] != map_node["node"]]
    map_node_dict = map_node.groupby("node_parent")["node"].apply(list).to_dict()

    for index, row in map_agg_pd.iterrows():
        print(row["names"])
        # Aggregates variables as per standard reporting
        report_iam.aggregate(row["names"], components=row["list_cat"], append=True)

        if row["names"] in (
            "Water Extraction|Seawater|Cooling",
            "Investment|Infrastructure|Water",
            "Water Extraction|Seawater",
        ):
            report_iam.aggregate_region(row["names"], append=True)
        else:
            for rr in map_node_dict:
                report_iam.aggregate_region(
                    row["names"], region=rr, subregions=map_node_dict[rr], append=True
                )
    # Aggregates variables separately that are not included map_agg_pd
    for rr in map_node_dict:
        report_iam.aggregate_region(
            "Water Resource|*", region=rr, subregions=map_node_dict[rr], append=True
        )
        report_iam.aggregate_region(
            "Price|*",
            method="mean",
            region=rr,
            subregions=map_node_dict[rr],
            append=True,
        )

    # Remove duplicate variables
    varsexclude = [
        "Investment|Infrastructure|Water",
        "Investment|Infrastructure|Water|Extraction",
        "Investment|Infrastructure|Water|Other",
        "Investment|Infrastructure|Water|Extraction|Groundwater",
    ]
    report_iam.filter(variable=varsexclude, unit="unknown", keep=False, inplace=True)
    # prepare data for loading timeserie
    report_pd = report_iam.as_pandas()
    report_pd = report_pd.drop(columns=["exclude"])
    # all initial variables form Reporte will be filtered out
    d = report_df.Variable.unique()
    d1 = pd.DataFrame({"variable": d})
    d1[["to_keep"]] = "No"
    # filter out initial variables
    report_pd = report_pd.merge(d1, how="left")
    report_pd = report_pd[report_pd["to_keep"] != "No"]
    report_pd = report_pd.drop(columns=["to_keep"])

    # ecluded other intermediate variables added later to report_iam
    report_pd = report_pd[-report_pd.variable.isin(water_hydro_var)]

    # add water population
    report_pd = report_pd.append(pop_sdg6)
    # add units
    for index, row in map_agg_pd.iterrows():
        report_pd.loc[(report_pd.variable == row["names"]), "unit"] = row["unit"]

    df_unit = pyam.IamDataFrame(report_pd)
    df_unit.convert_unit("GWa", to="EJ", inplace=True)
    df_unit_inv = df_unit.filter(variable="Investment*")
    df_unit_inv.convert_unit(
        "million US$2010/yr", to="billion US$2010/yr", factor=0.001, inplace=True
    )

    df_unit = df_unit.as_pandas()
    df_unit = df_unit[~df_unit["variable"].str.contains("Investment")]
    df_unit_inv = df_unit_inv.as_pandas()
    report_pd = pd.concat([df_unit, df_unit_inv])
    report_pd = report_pd.drop(columns=["exclude"])
    report_pd["unit"].replace("EJ", "EJ/yr", inplace=True)

    sc.check_out(timeseries_only=True)
    print("Starting to upload timeseries")
    print(report_pd.head())
    sc.add_timeseries(report_pd)
    print("Finished uploading timeseries")
    sc.commit("Reporting uploaded as timeseries")


def report_full(sc=False, sdgs=False):
    """Combine old and new reporting workflows"""
    a = sc.timeseries()
    # keep historical part, if present
    a = a[a.year >= 2020]

    sc.check_out(timeseries_only=True)
    print("Remove any previous timeseries")

    sc.remove_timeseries(a)
    print("Finished removing timeseries, now commit..")
    sc.commit("Remove existing timeseries")

    run_old_reporting(sc)
    print("First part of reporting completed, now procede with the water variables")

    report(sc, sdgs)
    print("overall NAVIGATE reporting completed")

    # add ad-hoc caplculated variables with a function
    ts = sc.timeseries()

    out_path = private_data_path().parents[0] / "reporting_output/NAVIGATE"

    if not out_path.exists():
        out_path.mkdir()

    out_file = out_path / f"{sc.model}_{sc.scenario}.csv"

    # Convert to pyam dataframe
    ts_long = pyam.IamDataFrame(ts)

    ts_long.to_csv(out_file)
    print(f"Saving csv to {out_file}")
