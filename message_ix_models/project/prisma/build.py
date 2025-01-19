import message_ix

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_power_sector import gen_data_power_sector
from message_ix_models.model.material.data_steel import (
    gen_2020_calibration_relation,
    gen_dri_act_bound,
    gen_dri_cap_calibration,
    gen_dri_coal_model,
)
from message_ix_models.model.material.util import combine_df_dictionaries


def add_dri_update(scenario: message_ix.Scenario, dry_run=False):
    # remove deprecated calibration parameters
    if not dry_run:
        with scenario.transact():
            for par in [
                "historical_activity",
                "historical_new_capacity",
                "technical_lifetime",
            ]:
                df_tmp = scenario.par(par, filters={"technology": "dri_steel"})
                scenario.remove_par(par, df_tmp)

            scenario.add_set("technology", ["dri_coal_steel"])

    par_dict = {}
    s_info = ScenarioInfo(scenario)
    par_dict = combine_df_dictionaries(
        par_dict,
        gen_dri_act_bound(),
        gen_dri_cap_calibration(),
        gen_dri_coal_model(s_info),
    )
    if dry_run:
        return par_dict
    else:
        with scenario.transact():
            for k, v in par_dict.items():
                scenario.add_par(k, v)


def add_eaf_bof_calibration(scenario, dry_run=False):
    par_dict = gen_2020_calibration_relation(scenario, "eaf")
    par_dict = combine_df_dictionaries(
        par_dict, gen_2020_calibration_relation(scenario, "bof")
    )

    df_gro_up_old = scenario.par(
        "growth_activity_up",
        filters={"technology": ["bof_steel", "eaf_steel"], "year_act": 2020},
    )
    df_gro_up_old.value = 0.5

    par_dict = combine_df_dictionaries(par_dict, {"growth_activity_up": df_gro_up_old})

    if dry_run:
        return par_dict
    else:
        with scenario.transact():
            scenario.add_set("relation", ["eaf_bound_2020", "bof_bound_2020"])
            for k, v in par_dict.items():
                print(k)
                scenario.add_par(k, v)


def add_power_sector(scenario):
    power_dict = gen_data_power_sector(scenario)
    with scenario.transact():
        for k, v in power_dict.items():
            scenario.add_par(k, v)

    # lower aluminum demand by about 0.1 Mt to make the model feasible with
    # power sector module
    with scenario.transact():
        df = scenario.par(
            "demand",
            filters={"node": "R12_RCPA", "commodity": "aluminum", "year": 2020},
        )
        df.value = 1.88
        scenario.add_par("demand", df)


def resolve_infeasibilities(scenario):
    # remove water input req for steel technologies, which has led to infeasibilities
    # cause the water supply is not properly parametrized
    df_in_water = scenario.par(
        "input", filters={"commodity": ["water"], "level": "water_supply"}
    )
    with scenario.transact():
        scenario.remove_par("input", df_in_water)

    # remove balance equality for steel commodities to give the model room to adjust
    bal_eq_original = scenario.set("balance_equality")
    bal_eq_steel = bal_eq_original[bal_eq_original["commodity"] == "steel"]
    with scenario.transact():
        scenario.remove_set("balance_equality", bal_eq_steel)

    # relax growth constraint for AFR region to make 2020 bound feasible, since
    # historical EAF activity is not properly parametrized
    df = scenario.par(
        "initial_activity_up",
        filters={"technology": "eaf_steel", "node_loc": "R12_AFR"},
    )
    df.value = 3
    with scenario.transact():
        scenario.add_par("initial_activity_up", df)

    # relax growth constraint for MEA region to make 2020 bound feasible, since
    # historical EAF activity is not properly parametrized
    df = scenario.par(
        "initial_activity_up",
        filters={"technology": "eaf_steel", "node_loc": "R12_MEA"},
    )
    df.value = 10
    with scenario.transact():
        scenario.add_par("initial_activity_up", df)

    # fix EAF M1 and M2 historical activity for China, where the scrap based EAF
    # activity was not correctly parametrized
    df_hist_act = scenario.par(
        "historical_activity",
        filters={"technology": ["eaf_steel"], "year_act": 2015, "node_loc": "R12_CHN"},
    )
    df_hist_act.value = [35, 35]
    with scenario.transact():
        scenario.add_par("historical_activity", df_hist_act)
