import ixmp
import message_ix

from message_ix_models.project.digsy.sensitivity import (
    adjust_electrification_constraint,
    adjust_rooftop_constraint,
    lower_cv_cost,
)

if __name__ == "__main__":
    mp = ixmp.Platform("ixmp_dev")
    scen = message_ix.Scenario(mp, "DIGSY_SSP2_BEST", "NPiREF").clone(
        "DIGSY_SSP2_BEST", "NPiREF", keep_solution=False
    )
    adjust_electrification_constraint(scen)
    adjust_rooftop_constraint(scen, 0.4)
    scen.solve("MESSAGE-MACRO")
    del scen
    scen = message_ix.Scenario(mp, "DIGSY_SSP2_BESTEST", "NPiREF").clone(
        "DIGSY_SSP2_BESTEST",
        "NPiREF",
        keep_solution=False,
    )
    adjust_electrification_constraint(scen)
    adjust_rooftop_constraint(scen, 0.45)
    lower_cv_cost(scen, 0.666)
    scen.solve("MESSAGE-MACRO")
    del scen
    scen = message_ix.Scenario(mp, "DIGSY_SSP2_WORSTEST", "NPiREF", version=1).clone(
        "DIGSY_SSP2_WORSTEST",
        "NPiREF",
        keep_solution=False,
    )
    lower_cv_cost(scen, 1.25)
    scen.solve("MESSAGE-MACRO")
    scen.set_as_default()
