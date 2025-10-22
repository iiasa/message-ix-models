from typing import TYPE_CHECKING

import pandas as pd

from message_ix_models.project.digsy.supply import (
    gen_res_marg_demand_tec_data,
    remove_elec_t_d_res_marg,
)

if TYPE_CHECKING:
    from message_ix import Scenario


def adjust_electrification_constraint(scenario: "Scenario"):
    df = scenario.par(
        "growth_activity_up",
        filters={"technology": ["elec_trp", "hp_el_rc", "elec_rc"]},
    )
    df.value += 0.01
    with scenario.transact():
        scenario.add_par("growth_activity_up", df)
    return


def lower_unconv_cost(scenario: "Scenario"):
    for par in ["var_cost", "inv_cost"]:
        df = scenario.par(
            par,
            filters={
                "technology": [
                    "gas_extr_5",
                    "gas_extr_6",
                    "gas_extr_7",
                    "oil_extr_5",
                    "oil_extr_6",
                    "oil_extr_7",
                ]
            },
        )
        df.value *= 0.9
        with scenario.transact():
            scenario.add_par(par, df)


def lower_cv_cost(scenario: "Scenario", scaler: float):
    wind_tecs = ["wind_cv1", "wind_cv2", "wind_cv3", "wind_cv4"]
    sol_tecs = ["solar_cv1", "solar_cv2", "solar_cv3", "solar_cv4"]
    df = scenario.par(
        "var_cost",
        filters={"technology": sol_tecs + wind_tecs},
    )
    df.value *= scaler
    with scenario.transact():
        scenario.add_par("var_cost", df)


def expand_reserve(scenario: "Scenario"):
    par = "resource_volume"
    for comm in ["gas", "crude"]:
        reserve1 = scenario.par(par, filters={"commodity": f"{comm}_1"})
        resource1 = scenario.par(par, filters={"commodity": f"{comm}_2"})
        reserve2 = scenario.par(par, filters={"commodity": f"{comm}_3"})
        resource2 = scenario.par(par, filters={"commodity": f"{comm}_4"})
        reserve_unconv = scenario.par(par, filters={"commodity": f"{comm}_5"})
        resource_unconv = scenario.par(par, filters={"commodity": f"{comm}_6"})

        reserve1_new = reserve1.copy(deep=True)
        reserve1_new.value *= 1.03
        df_sub = reserve1.copy(deep=True)
        df_sub.value *= 0.03
        df_sub.commodity = f"{comm}_2"
        resource1.set_index(
            [i for i in resource1.columns if i != "value"], inplace=True
        )
        resource1_new = resource1.sub(
            df_sub.set_index([i for i in df_sub.columns if i != "value"]), fill_value=0
        )

        reserve2_new = reserve2.copy(deep=True)
        reserve2_new.value *= 1.03
        df_sub = reserve2.copy(deep=True)
        df_sub.value *= 0.03
        df_sub.commodity = f"{comm}_4"
        resource2.set_index(
            [i for i in resource2.columns if i != "value"], inplace=True
        )
        resource2_new = resource2.sub(
            df_sub.set_index([i for i in df_sub.columns if i != "value"]), fill_value=0
        )

        reserve_unconv_new = reserve_unconv.copy(deep=True)
        reserve_unconv_new.value *= 1.15
        df_sub = reserve_unconv.copy(deep=True)
        df_sub.value *= 0.15
        df_sub.commodity = f"{comm}_6"
        resource_unconv.set_index(
            [i for i in resource_unconv.columns if i != "value"], inplace=True
        )
        resource_unconv_new = resource_unconv.sub(
            df_sub.set_index([i for i in df_sub.columns if i != "value"]), fill_value=0
        )
        par_new = pd.concat(
            [
                reserve1_new,
                resource1_new.reset_index(),
                reserve2_new,
                resource2_new.reset_index(),
                reserve_unconv_new,
                resource_unconv_new.reset_index(),
            ]
        )
        with scenario.transact():
            scenario.add_par(par, par_new)


def adjust_rooftop_constraint(scenario: "Scenario", new_value: float = 0.5):
    df = scenario.par(
        "share_commodity_up",
        filters={"shares": ["UE_RT_elec_share_RC_max"]},
    )
    df.value = new_value
    with scenario.transact():
        scenario.add_par("share_commodity_up", df)


def get_integration_constraints(scen: "Scenario"):
    df = scen.par(
        "relation_activity",
        filters={
            "relation": [
                "res_marg",
                "oper_res",
                "solar_step",
                "solar_step2",
                "solar_step3",
                "wind_step",
                "wind_step2",
                "wind_step3",
            ],
            "technology": [
                "solar_cv1",
                "solar_cv2",
                "solar_cv3",
                "solar_cv4",
                "elec_trp",
                "wind_cv1",
                "wind_cv2",
                "wind_cv3",
                "wind_cv4",
                "elec_t_d",
                "h2_elec",
            ],
        },
    )
    return df


def query_ssp_pars(scens: list[str]):
    df_list = {}
    for scen in scens:
        df_list[scen] = get_integration_constraints(
            message_ix.Scenario(mp, f"SSP_{scen}_v6.2", "baseline")
        )
    return df_list


def create_sensitivity_scenarios(scens: list[tuple], suffix: str, modifier):
    for model, scen in scens:
        scenario = message_ix.Scenario(mp, model, scen)
        clone = scenario.clone(
            scenario.model, scenario.scenario + suffix, keep_solution=False
        )
        modifier(clone)
    return


def change_res_marg(scen: "Scenario") -> None:
    remove_elec_t_d_res_marg(scen)
    rel = gen_res_marg_demand_tec_data(scen, 0.15)
    with scen.transact():
        scen.add_par("relation_activity", rel)


if __name__ == "__main__":
    import ixmp
    import message_ix

    sensitivity_map = {
        "relaxed electrification growth constraint": {
            "suffix": "_elec",
            "modifier": adjust_electrification_constraint,
        },
        "lower shale cost": {
            "suffix": "_shale_cost",
            "modifier": lower_unconv_cost,
        },
        "relaxed rooftop share constraint": {
            "suffix": "_RT_50",
            "modifier": adjust_rooftop_constraint,
        },
        "bigger reserve": {
            "suffix": "_expanded_reserve",
            "modifier": expand_reserve,
        },
        "reserve margin": {
            "suffix": "_res_marg_new",
            "modifier": change_res_marg,
        },
    }

    mp = ixmp.Platform("ixmp_dev")
    # create_sensitivity_scenarios(
    #     [(f"DIGSY_SSP2{i}", "baseline") for i in ["", "_BEST", "_WORST"]],
    #     **sensitivity_map["lower shale cost"],
    # )
    # create_sensitivity_scenarios(
    #     [(f"DIGSY_SSP2{i}", "baseline") for i in [""]],
    #     **sensitivity_map["bigger reserve"],
    # )
    create_sensitivity_scenarios(
        [(f"DIGSY_SSP2{i}", "baseline_DEFAULT") for i in [""]],
        **sensitivity_map["reserve margin"],
    )
    # scen = message_ix.Scenario(mp, "DIGSY_SSP2", "baseline")
    # pars = query_ssp_pars(["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"])
    # with pd.ExcelWriter("output.xlsx") as writer:
    #     for k,v in pars.items():
    #         v.to_excel(writer, sheet_name=k)
    # print()
