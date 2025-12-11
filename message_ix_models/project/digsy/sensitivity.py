from typing import TYPE_CHECKING

import message_ix
import pandas as pd
from message_ix.util import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import ParameterData


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


def adjust_rooftop_constraint(
    digsy_scen,
    s_info: "ScenarioInfo",
) -> "ParameterData":
    val_map = {
        "BEST": 0.4,
        "BESTEST": 0.45,
    }
    df = (
        make_df(
            "share_commodity_up",
            shares="UE_RT_elec_share_RC_max",
            unit="???",
            time="year",
            value=val_map.get(digsy_scen),
        )
        .pipe(broadcast, year_act=[i for i in s_info.Y if i > 2025])
        .pipe(broadcast, node_share=[i for i in s_info.Y if i > 2025])
    )
    return {"share_commodity_up": df}


def adjust_electrification_constraint(scenario: "Scenario"):
    df = scenario.par(
        "growth_activity_up",
        filters={"technology": ["elec_trp", "hp_el_rc", "elec_rc"]},
    )
    df.value += 0.01
    return {"growth_activity_up": df}


def create_sensitivity_scenarios(scens: list[tuple], suffix: str, modifier):
    import ixmp

    mp = ixmp.Platform("ixmp_dev")
    for model, scen in scens:
        scenario = message_ix.Scenario(mp, model, scen)
        clone = scenario.clone(
            scenario.model, scenario.scenario + suffix, keep_solution=False
        )
        modifier(clone)
    return
