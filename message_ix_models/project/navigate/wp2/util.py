import logging
from itertools import product

import pandas as pd
from message_ix import make_df
from message_ix_models.util import broadcast, private_data_path

from message_data.tools.utilities import get_nodes, get_optimization_years

log = logging.getLogger(__name__)


def add_CCS_constraint(scen, maximum_value, type_rel):
    log.info("Add CCS limits")

    # Maximum value should be in GtCO2/yr.

    # Add a new global relation to limit CCS activity. Obtain positive
    # coefficients for ccs technologies to put a limit to the
    # overall CCS activity.

    relation_name = "global_co2_trans"

    scen.check_out()
    scen.add_set("relation", relation_name)
    df = scen.par(
        "relation_activity", filters={"relation": ["bco2_trans_disp", "co2_trans_disp"]}
    )
    df["node_rel"] = "R12_GLB"
    df["value"] *= -1  # Values were negative
    df["relation"] = relation_name
    scen.add_par("relation_activity", df)

    # Remove the technologies that aggregate the CCS activity.
    # Not needed in this relation.

    df = scen.par(
        "relation_activity",
        filters={
            "technology": ["bco2_tr_dis", "co2_tr_dis"],
            "relation": ["bco2_trans_disp", "co2_trans_disp"],
        },
    )
    df["node_rel"] = "R12_GLB"
    df["relation"] = relation_name
    scen.remove_par("relation_activity", df)
    scen.commit("Global CCS relation set up.")

    # The maximum allowed value should be converted to MtC.
    # (assuming this is the activity of the CCS technologies)
    # The unit MtC does not exist in the database!

    maximum_value_converted = maximum_value * (10**3) * (12 / 44)

    years = get_optimization_years(scen)
    years = [x for x in years if x >= 2030]

    df = pd.DataFrame(
        {
            "node_rel": "R12_GLB",
            "relation": relation_name,
            "year_rel": years,
            "value": maximum_value_converted,
            "unit": "tC",
        }
    )

    scen.check_out()
    scen.add_par(f"relation_{type_rel}", df)
    scen.commit(
        "added upper limit of zero for CO2 emissions"
        + f"accounted for in the relation {relation_name}"
    )


def add_electrification_share(scen):
    log.info("Add share constraints for electrification in industry")

    node_list = get_nodes(scen)

    # There are no processes at the moment that use low temperature heat
    # This implementation includes Other Industry, Cement, Aluminum,
    # High Value Chemicals and Resin production.
    # Iron and Steel not included since implementation is more complicated.
    # Depends on scrap availability etc.

    # Group 1: Final Energy Industry technologies that produce ht_heat

    nonelec_ind_tecs_ht = [
        # Cement industry
        "furnace_foil_cement",
        "furnace_loil_cement",
        "furnace_biomass_cement",
        "furnace_ethanol_cement",
        "furnace_methanol_cement",
        "furnace_gas_cement",
        "furnace_coal_cement",
        "furnace_h2_cement",
        # Aluminum indutry
        "furnace_coal_aluminum",
        "furnace_foil_aluminum",
        "furnace_loil_aluminum",
        "furnace_ethanol_aluminum",
        "furnace_biomass_aluminum",
        "furnace_methanol_aluminum",
        "furnace_gas_aluminum",
        "furnace_h2_aluminum",
        # High Value Chemicals
        "furnace_coke_petro",
        "furnace_coal_petro",
        "furnace_foil_petro",
        "furnace_loil_petro",
        "furnace_ethanol_petro",
        "furnace_biomass_petro",
        "furnace_methanol_petro",
        "furnace_gas_petro",
        "furnace_h2_petro",
        # Resins
        "furnace_coal_resins",
        "furnace_foil_resins",
        "furnace_loil_resins",
        "furnace_ethanol_resins",
        "furnace_biomass_resins",
        "furnace_methanol_resins",
        "furnace_gas_resins",
        "furnace_h2_resins",
    ]

    # Group 2: Other Industry (produces i_therm)
    non_elec_ind_tec_oth = [
        "foil_i",
        "loil_i",
        "biomass_i",
        "eth_i",
        "gas_i",
        "coal_i",
        "h2_i",
    ]

    # Final Energy Industry Electricity Technologies

    # Group 1:
    elec_ind_tecs_ht = [
        "furnace_elec_cement",
        "furnace_elec_aluminum",
        "furnace_elec_petro",
        "furnace_elec_resins",
    ]

    # Gruop 2:
    elec_ind_tecs_oth = ["elec_i"]

    all_ind_tecs = (
        nonelec_ind_tecs_ht
        + elec_ind_tecs_ht
        + non_elec_ind_tec_oth
        + elec_ind_tecs_oth
    )

    elec_ind_tecs = elec_ind_tecs_ht + elec_ind_tecs_oth

    shr_const = "share_low_lim_elec_ind"
    type_tec_shr = "elec_ind"
    type_tec_tot = "all_ind"

    scen.check_out()
    scen.add_set("shares", shr_const)
    scen.add_cat("technology", type_tec_shr, elec_ind_tecs)
    scen.commit("sets added")

    node_list.remove("R12_GLB")
    scen.check_out()

    # Group 1:
    levels = ["useful_cement", "useful_aluminum", "useful_petro", "useful_resins"]
    for n, level in product(node_list, levels):
        df = pd.DataFrame(
            {
                "shares": [shr_const],
                "node_share": n,
                "node": n,
                "type_tec": type_tec_shr,
                "mode": "high_temp",
                "commodity": "ht_heat",
                "level": level,
            }
        )

        scen.add_set("map_shares_commodity_share", df)

    # Group 2:
    for n in node_list:
        df = pd.DataFrame(
            {
                "shares": [shr_const],
                "node_share": n,
                "node": n,
                "type_tec": type_tec_shr,
                "mode": "M1",
                "commodity": "i_therm",
                "level": "useful",
            }
        )
        scen.add_set("map_shares_commodity_share", df)

    scen.commit("Share mapping added.")

    # Add all technologies which make up the "Total"
    scen.check_out()
    scen.add_cat("technology", type_tec_tot, all_ind_tecs)
    scen.commit("Total technology category added.")

    # Group 1:
    scen.check_out()
    for n, level in product(node_list, levels):
        df = pd.DataFrame(
            {
                "shares": [shr_const],
                "node_share": n,
                "node": n,
                "type_tec": type_tec_tot,
                "mode": "high_temp",
                "commodity": "ht_heat",
                "level": level,
            }
        )
        scen.add_set("map_shares_commodity_total", df)

    # Group 2:
    for n in node_list:
        df = pd.DataFrame(
            {
                "shares": [shr_const],
                "node_share": n,
                "node": n,
                "type_tec": type_tec_tot,
                "mode": "M1",
                "commodity": "i_therm",
                "level": "useful",
            }
        )
        scen.add_set("map_shares_commodity_total", df)

        # Add lower bound share constraint for end of the century

        for n in node_list:
            df = pd.DataFrame(
                {
                    "shares": shr_const,
                    "node_share": n,
                    "year_act": [
                        2030,
                        2035,
                        2040,
                        2045,
                        2050,
                        2055,
                        2060,
                        2070,
                        2080,
                        2090,
                        2100,
                        2110,
                    ],
                    "time": "year",
                    "value": [
                        0.4,
                        0.5,
                        0.6,
                        0.7,
                        0.8,
                        0.8,
                        0.8,
                        0.8,
                        0.8,
                        0.8,
                        0.8,
                        0.8,
                    ],
                    "unit": "%",
                }
            )

            scen.add_par("share_commodity_lo", df)

    scen.commit("Add industry electricity share constraints")

    # Remove the existing UE bounds if there are infeasibilities
    # Possible relations that can cause problem:
    # UE_industry_th
    # UE_industry_th_electric
    # FE_industry


def add_LED_setup(scen):
    log.info("Add LED setup to the scenario")

    # This function is adjusted based on:
    # https://github.com/volker-krey/message_data/blob/LED_update_materials/
    # message_data/projects/led/LED_low_energy_demand_setup.R#L73
    # Only relevant adjustments are chosen:
    # - Cost adjustments for VREs
    # - Greater contribution of intermittent solar and wind to total electricity
    #   generation
    # - Adjust wind and solar PV operating reserve requirements
    # - Adjust wind and solar PV reserve margin requirements
    # - Increase the initial starting point value for activity growth bounds on the
    #   solar PV technology (centralized generation)

    period_list = get_optimization_years(scen)
    [i for i in period_list if i >= 2025]
    node_list = get_nodes(scen)
    scen.check_out()

    # Adjsut the investment costs, fixed O&M costs and variable O&M costs for
    # the following technologies: solar_pv_ppl, stor_ppl, h2_elec, h2_fc_trp,
    # solar_i, h2_fc_I, h2_fc_RC.

    # Read technology investment costs from xlsx file and add to the scenario

    data_path = private_data_path("alps")
    path_costs = data_path.joinpath(
        "granular-techs_cost_comparison_20170831_revAG_SDS_5year.xlsx",
    )
    inv_costs = pd.read_excel(path_costs, sheet_name="NewCosts_fixed")
    year_columns = inv_costs._get_numeric_data().columns.values.tolist()
    inv_costs = pd.melt(
        inv_costs,
        id_vars=["TECHNOLOGY", "REGION"],
        value_vars=year_columns,
        var_name="YEAR",
        value_name="VALUE",
    )

    inv_costs = inv_costs.dropna()
    inv_costs.rename(
        columns={
            "TECHNOLOGY": "technology",
            "REGION": "node_loc",
            "YEAR": "year_vtg",
            "VALUE": "value",
        },
        inplace=True,
    )
    inv_costs["unit"] = "USD/kWa"
    scen.add_par("inv_cost", inv_costs)

    # Read technology fixed O&M costs from xlsx file and add to the scenario

    fom_costs = pd.read_excel(path_costs, sheet_name="NewFOMCosts_fixed")
    year_columns = fom_costs._get_numeric_data().columns.values.tolist()
    fom_costs = pd.melt(
        fom_costs,
        id_vars=["TECHNOLOGY", "REGION"],
        value_vars=year_columns,
        var_name="YEAR",
        value_name="VALUE",
    )
    fom_costs = fom_costs.dropna()

    for node, year_vtg, technology in product(
        node_list, period_list, fom_costs["TECHNOLOGY"].unique()
    ):
        try:
            years_tec_active = scen.years_active(node, technology, year_vtg)
        except Exception:
            continue

        fom_costs_temp = fom_costs[
            (fom_costs["TECHNOLOGY"] == technology)
            & (fom_costs["REGION"] == node)
            & (fom_costs["YEAR"] == year_vtg)
        ]
        df = pd.DataFrame(
            {
                "technology": technology,
                "node_loc": node,
                "year_vtg": year_vtg,
                "value": fom_costs_temp["VALUE"].values[0],
                "unit": "USD/kWa",
                "year_act": years_tec_active,
            }
        )
        scen.add_par("fix_cost", df)

    # Read technology variable O&M costs from xlsx file and add to the scenario
    vom_costs = pd.read_excel(path_costs, sheet_name="NewVOMCosts_fixed")
    year_columns = vom_costs._get_numeric_data().columns.values.tolist()
    vom_costs = pd.melt(
        vom_costs,
        id_vars=["TECHNOLOGY", "REGION"],
        value_vars=year_columns,
        var_name="YEAR",
        value_name="VALUE",
    )

    for node, year_vtg_technology in product(
        node_list, period_list, vom_costs["TECHNOLOGY"].unique()
    ):
        try:
            years_tec_active = scen.years_active(node, technology, year_vtg)
        except Exception:
            continue

        vom_costs_temp = vom_costs[
            (vom_costs["TECHNOLOGY"] == technology)
            & (vom_costs["REGION"] == node)
            & (vom_costs["YEAR"] == year_vtg)
        ]
        df = pd.DataFrame(
            {
                "technology": technology,
                "node_loc": node,
                "year_vtg": year_vtg,
                "value": vom_costs_temp["VALUE"].values[0],
                "unit": "USD/kWa",
                "year_act": years_tec_active,
                "mode": "M1",
                "time": "year",
            }
        )
        scen.add_par("var_cost", df)

    # Changing the renewable energy assumptions (steps) for the following technologies:
    # elec_t_d, h2_elec, relations: solar_step, solar_step2, solar_step3, wind_step,
    # wind_step2, wind_step3

    # Read solar and wind intermittency assumptions from xlsx file

    path_renew = data_path.joinpath(
        "solar_wind_intermittency_20170831_5year.xlsx",
    )
    renew_steps = pd.read_excel(path_renew, sheet_name="steps_NEW")
    year_columns = renew_steps._get_numeric_data().columns.values.tolist()
    renew_steps = pd.melt(
        renew_steps,
        id_vars=["TECHNOLOGY", "REGION", "RELATION"],
        value_vars=year_columns,
        var_name="YEAR",
        value_name="VALUE",
    )

    # Adjust wind and solar PV resource steps (contribution to total electricity
    # generation). These changes allow for greater contribution of intermittent solar
    # and wind to total electricity generation.

    renew_steps = renew_steps.dropna()
    renew_steps.rename(
        columns={
            "RELATION": "relation",
            "TECHNOLOGY": "technology",
            "REGION": "node_loc",
            "YEAR": "year_act",
            "VALUE": "value",
        },
        inplace=True,
    )
    renew_steps["unit"] = "???"
    renew_steps["node_rel"] = renew_steps["node_loc"]
    renew_steps["year_rel"] = renew_steps["year_act"]
    renew_steps["mode"] = "M1"
    scen.add_par("relation_activity", renew_steps)

    # Adjust relation: oper_res, technologies: wind_cv1, windcv2, windcv3, windcv4,
    # solar_cv1, solar_cv2, solar_cv3, solar_cv4, elec_trp

    # Read solar and wind intermittency assumptions from xlsx file
    renew_oper = pd.read_excel(path_renew, sheet_name="oper_NEW")
    year_columns = renew_oper._get_numeric_data().columns.values.tolist()
    renew_oper = pd.melt(
        renew_oper,
        id_vars=["TECHNOLOGY", "REGION", "RELATION"],
        value_vars=year_columns,
        var_name="YEAR",
        value_name="VALUE",
    )
    renew_oper = renew_oper.dropna()

    # Adjust wind and solar PV operating reserve requirements (amount of flexible
    # generation that needs to be run for every unit of intermittent solar and wind
    # => variable renewables <0, non-dispatchable thermal 0, flexible >0);

    # Also adjust the contribution of electric transport technologies to the operating
    # reserves, increasing the amount they can contribute (vehicle-to-grid). These
    # changes reduce the effective cost of building and running intermittent solar and
    # wind plants, since the amount of back-up capacity built is less than before.

    renew_oper.rename(
        columns={
            "RELATION": "relation",
            "TECHNOLOGY": "technology",
            "REGION": "node_loc",
            "YEAR": "year_act",
            "VALUE": "value",
        },
        inplace=True,
    )
    renew_oper["unit"] = "???"
    renew_oper["node_rel"] = renew_oper["node_loc"]
    renew_oper["year_rel"] = renew_oper["year_act"]
    renew_oper["mode"] = "M1"
    scen.add_par("relation_activity", renew_oper)

    # Relation: res_marg, Technology:  wind_cv1, windcv2, windcv3, windcv4,
    # solar_cv1, solar_cv2, solar_cv3, solar_cv4

    # Read solar and wind intermittency assumptions from xlsx file

    renew_resm = pd.read_excel(path_renew, sheet_name="resm_NEW")
    year_columns = renew_resm._get_numeric_data().columns.values.tolist()
    renew_resm = pd.melt(
        renew_resm,
        id_vars=["TECHNOLOGY", "REGION", "RELATION"],
        value_vars=year_columns,
        var_name="YEAR",
        value_name="VALUE",
    )
    renew_resm = renew_resm.dropna()

    # Adjust wind and solar PV reserve margin requirements (amount of firm capacity
    # that needs to be run to meet peak load and contingencies; intermittent
    # solar and wind do not contribute a full 100% to the reserve margin)
    # These changes allow for greater contribution of intermittent solar and
    # wind to total electricity generation

    renew_resm.rename(
        columns={
            "RELATION": "relation",
            "TECHNOLOGY": "technology",
            "REGION": "node_loc",
            "YEAR": "year_act",
            "VALUE": "value",
        },
        inplace=True,
    )
    renew_resm["unit"] = "???"
    renew_resm["node_rel"] = renew_resm["node_loc"]
    renew_resm["year_rel"] = renew_resm["year_act"]
    renew_resm["mode"] = "M1"
    scen.add_par("relation_activity", renew_resm)

    # Increase the initial starting point value for activity growth bounds on
    # the solar PV technology (centralized generation)
    # Only do this for a subset of the regions for which there are currently
    # "growth_activity_up" (formerly "mpa up") values defined.
    # We don't want to specify an "initial_activity_up" for a technology that
    # does not have a "growth_activity_up".
    years_subset = [2025, 2030, 2035, 2040, 2045, 2050]
    technology = "solar_pv_ppl"
    growth_activity_up = scen.par(
        "growth_activity_up", filters={"technology": technology}
    )
    node_subset = growth_activity_up["node_loc"]
    # node_subset = c("R11_CPA","R11_FSU","R11_LAM","R11_MEA","R11_NAM","R11_PAS")

    for node, year in product(node_subset, years_subset):
        df = pd.DataFrame(
            {
                "node_loc": node,
                "technology": technology,
                "year_act": year,
                "time": "year",
                "value": 90,
                "unit": "GWa",
            },
            index=[0],
        )
        scen.add_par("initial_activity_up", df)

    # Increase the initial starting point value for capacity growth bounds
    # on the solar PV technology (centralized generation)

    years_subset = [2025, 2030, 2035, 2040, 2045, 2050]
    technology = "solar_pv_ppl"

    for node, year in product(node_subset, years_subset):
        df = pd.DataFrame(
            {
                "node_loc": node,
                "technology": technology,
                "year_vtg": year,
                "time": "year",
                "value": 10,
                "unit": "GW",
            },
            index=[0],
        )
        scen.add_par("initial_new_capacity_up", df)

    # Read useful level fuel potential contribution assumptions from xlsx file
    # Adjust limits to potential fuel-specific contributions at useful energy
    # level (in each end-use sector separately)

    # path_useful_fuel = data_path.joinpath(
    #     "useful_level_fuel_potential_contribution_20170907_5year.xlsx",
    # )
    # useful_fuel = pd.read_excel(path_useful_fuel, sheet_name = "UE_constraints_NEW")
    # year_columns = useful_fuel._get_numeric_data().columns.values.tolist()
    # useful_fuel = pd.melt(useful_fuel, id_vars=['TECHNOLOGY','REGION','RELATION'],
    # value_vars= year_columns,var_name='YEAR', value_name='VALUE')

    # relation_list_useful_fuel = useful_fuel['RELATION'].unique()
    # node_list = useful_fuel['REGION'].unique()
    # useful_fuel = useful_fuel.dropna()

    # useful_fuel.rename(
    #     columns={
    #         "RELATION": "relation",
    #         "TECHNOLOGY": "technology",
    #         "REGION": "node_loc",
    #         "YEAR": "year_act",
    #         "VALUE": "value",
    #     },
    #     inplace=True,
    # )
    # useful_fuel['unit'] = '???'
    # useful_fuel['node_rel'] = useful_fuel['node_loc']
    # useful_fuel['year_rel'] = useful_fuel['year_act']
    # useful_fuel['mode'] = 'M1'
    # scen.add_par('relation_activity',useful_fuel)

    scen.commit("LED changes implemented.")


def limit_h2(scen, type="green"):
    log.info("Add h2 limit")
    node_list = get_nodes(scen)
    period_list = get_optimization_years(scen)
    scen.check_out()

    # Exclude grey hydrogen options
    # In Blue hydrogen: h2_bio_ccs, h2_coal_ccs, h2_elec, h2_smr_ccs allowed.

    technology_list = ["h2_coal", "h2_smr", "h2_bio"]

    if type == "green":
        # Exclude blue hydrogen options as well. h2_bio_ccs, h2_elec are allowed.
        technology_list.extend(["h2_smr_ccs", "h2_coal_ccs"])
    else:
        raise ValueError(f"No such type {type!r} is defined.")

    par = "bound_activity_up"
    common = dict(mode="M1", time="year", value=0, unit="GWa")

    df_h2 = make_df(par, **common).pipe(
        broadcast, node_loc=node_list, technology=technology_list, year=period_list
    )
    scen.add_par(par, df_h2)

    scen.commit("Hydrogen limits added.")
