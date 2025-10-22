import ixmp
import message_ix

from message_ix_models.project.digsy.sensitivity import (
    adjust_electrification_constraint,
    adjust_rooftop_constraint,
)

if __name__ == "__main__":
    mp = ixmp.Platform("ixmp_dev")
    scen = message_ix.Scenario(
        mp, "DIGSY_SSP2_BEST", "INDC2030i_weak_SSP2 - Low Emissions_b"
    ).clone(
        "DIGSY_SSP2_BEST", "INDC2030i_weak_SSP2 - Low Emissions_b", keep_solution=False
    )
    adjust_electrification_constraint(scen)
    adjust_rooftop_constraint(scen, 0.4)
    scen.solve("MESSAGE-MACRO")
