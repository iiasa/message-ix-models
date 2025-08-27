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


def adjust_fossil_fuels():
    raise NotImplementedError


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


if __name__ == "__main__":
    import ixmp
    import message_ix
    import pandas as pd

    mp = ixmp.Platform("ixmp_dev")
    scen = message_ix.Scenario(mp, "DIGSY_SSP2", "baseline_elec")
    for suffix in ["", "_BEST", "_WORST"]:
        scen = message_ix.Scenario(mp, f"DIGSY_SSP2{suffix}", "baseline")
        # scen = message_ix.Scenario(mp, "DIGSY_SSP2", "base_model_WORST")
        clone = scen.clone(scen.model, scen.scenario + "_elec", keep_solution=False)
        adjust_electrification_constraint(clone)
    # pars = query_ssp_pars(["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"])
    # with pd.ExcelWriter("output.xlsx") as writer:
    #     for k,v in pars.items():
    #         v.to_excel(writer, sheet_name=k)
    # print()
