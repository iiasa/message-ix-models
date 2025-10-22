import ixmp
import message_ix

from message_ix_models.project.digsy.sensitivity import (
    adjust_electrification_constraint,
    adjust_rooftop_constraint,
    lower_cv_cost,
)

if __name__ == "__main__":
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
