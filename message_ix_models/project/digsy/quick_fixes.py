from typing import TYPE_CHECKING

import ixmp
import message_ix
from message_ix import make_df

from message_ix_models.project.digsy.sensitivity import (
    adjust_electrification_constraint,
    adjust_rooftop_constraint,
)
from message_ix_models.util import broadcast

if TYPE_CHECKING:
    from message_ix import Scenario


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


def relax_steel_constraints(scen) -> None:
    bof_dims = {
        "node_loc": ["R12_AFR", "R12_EEU", "R12_LAM", "R12_MEA", "R12_NAM"],
        "technology": "bof_steel",
        "year_act": 2100,
        "time": "year",
        "unit": "???",
    }
    bf_bio_dims = {
        "node_loc": [
            "R12_CHN",
            "R12_FSU",
            "R12_PAO",
            "R12_PAS",
            "R12_RCPA",
            "R12_SAS",
            "R12_WEU",
        ],
        "technology": "bf_biomass_steel",
        "time": "year",
        "unit": "???",
    }
    gro = make_df("growth_activity_up", **bof_dims, value=0.1)
    ini = make_df("initial_activity_up", **bof_dims, value=0.5)
    gro_bio = make_df("growth_activity_up", **bf_bio_dims, value=0.1).pipe(
        broadcast, year_act=[2080, 2090, 2100]
    )
    with scen.transact():
        scen.add_par("growth_activity_up", gro)
        scen.add_par("growth_activity_up", gro_bio)
        scen.add_par("initial_activity_up", ini)


def fix_best_scenario():
    mp = ixmp.Platform("ixmp_dev")
    scen = message_ix.Scenario(
        mp, "DIGSY_SSP2_BEST", "INDC2030i_weak_SSP2 - Low Emissions_b"
    ).clone(
        "DIGSY_SSP2_BEST", "INDC2030i_weak_SSP2 - Low Emissions_b", keep_solution=False
    )
    adjust_electrification_constraint(scen)
    adjust_rooftop_constraint(scen, 0.4)
    scen.solve("MESSAGE-MACRO")
    scen.set_as_default()


def fix_worstest():
    mp = ixmp.Platform("ixmp_dev")
    scen = message_ix.Scenario(
        mp, "DIGSY_SSP2_WORSTEST", "INDC2030i_weak_SSP2 - Low Emissions_b"
    ).clone(
        "DIGSY_SSP2_WORSTEST",
        "INDC2030i_weak_SSP2 - Low Emissions_b",
        keep_solution=False,
    )
    adjust_electrification_constraint(scen)
    adjust_rooftop_constraint(scen, 0.5)
    lower_cv_cost(scen, 1.25)
    scen.solve("MESSAGE-MACRO")


def fix_bestest():
    mp = ixmp.Platform("ixmp_dev")
    scen = message_ix.Scenario(
        mp, "DIGSY_SSP2_BESTEST", "INDC2030i_weak_SSP2 - Low Emissions_b"
    ).clone(
        "DIGSY_SSP2_BESTEST",
        "INDC2030i_weak_SSP2 - Low Emissions_b",
        keep_solution=False,
    )
    adjust_electrification_constraint(scen)
    adjust_rooftop_constraint(scen, 0.45)
    lower_cv_cost(scen, 0.666)
    scen.solve("MESSAGE-MACRO")
    scen.set_as_default()
