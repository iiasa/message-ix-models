from typing import TYPE_CHECKING

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


def adjust_rooftop_constraint(scenario: "Scenario", new_value: float = 0.4):
    df = scenario.par(
        "share_commodity_up",
        filters={"shares": ["UE_RT_elec_share_RC_max"]},
    )
    df.value = new_value
    with scenario.transact():
        scenario.add_par("share_commodity_up", df)
    raise NotImplementedError


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
    }

    mp = ixmp.Platform("ixmp_dev")
    # create_sensitivity_scenarios(
    #     [(f"DIGSY_SSP2{i}", "baseline") for i in ["", "_BEST", "_WORST"]],
    #     **sensitivity_map["lower shale cost"],
    # )
    create_sensitivity_scenarios(
        [(f"DIGSY_SSP2{i}", "baseline_1000f") for i in [""]],
        **sensitivity_map["relaxed electrification growth constraint"],
    )
    # scen = message_ix.Scenario(mp, "DIGSY_SSP2", "baseline")
    # pars = query_ssp_pars(["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"])
    # with pd.ExcelWriter("output.xlsx") as writer:
    #     for k,v in pars.items():
    #         v.to_excel(writer, sheet_name=k)
    # print()
