from typing import TYPE_CHECKING

import pytest
from message_ix import make_df

from message_ix_models import testing
from message_ix_models.project.navigate.wp2.util import (
    TECHS,
    add_CCS_constraint,
    add_electrification_share,
    add_LED_setup,
    limit_h2,
)
from message_ix_models.util import ScenarioInfo, broadcast

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from message_ix_models import Context

# The functions are only tested for this combination of settings
BARE_RES = dict(
    regions="R12",
    years="B",
)


def test_add_CCS_constraint(request: "FixtureRequest", test_context: "Context") -> None:
    ctx = test_context
    ctx.update(**BARE_RES)
    scenario = testing.bare_res(request, ctx)

    # Add scenario structure expected by the function
    # FIXME this should be handled either by the contents of testing.bare_res() (i.e.
    #       message_ix_models.model.bare and associated code lists), or by a function
    #       like .material.testing.built_material() that sets up a minimal
    #       MESSAGEix-Materials structure for testing
    # TODO remove once those are created
    with scenario.transact():
        scenario.add_set("node", f"{BARE_RES['regions']}_GLB")

    add_CCS_constraint(scenario, 2.0, "upper")


@pytest.mark.parametrize("kind", ["lo", "up"])
def test_add_electrification_share(
    request: "FixtureRequest", test_context: "Context", kind: str
) -> None:
    ctx = test_context
    ctx.update(**BARE_RES)
    scenario = testing.bare_res(request, ctx)

    # Add scenario structure expected by the function
    # FIXME this should be handled either by the contents of testing.bare_res() (i.e.
    #       message_ix_models.model.bare and associated code lists), or by a function
    #       like .material.testing.built_material() that sets up a minimal
    #       MESSAGEix-Materials structure for testing
    # TODO remove once those are created
    with scenario.transact():
        scenario.add_set("commodity", ["ht_heat", "i_therm"])
        scenario.add_set(
            "level",
            ["useful_aluminum", "useful_cement", "useful_petro", "useful_resins"],
        )
        scenario.add_set("mode", ["high_temp", "M1"])
        scenario.add_set("technology", TECHS["elec"] + TECHS["non-elec"])
        scenario.add_set("type_tec", "all_ind")

    add_electrification_share(scenario, kind)


def test_add_LED_setup(request: "FixtureRequest", test_context: "Context") -> None:
    ctx = test_context
    ctx.update(**BARE_RES)
    scenario = testing.bare_res(request, ctx)

    info = ScenarioInfo(scenario)

    # Add scenario structure expected by the function
    # FIXME this should be handled either by the contents of testing.bare_res() (i.e.
    #       message_ix_models.model.bare and associated code lists), or by a function
    #       like .material.testing.built_material() that sets up a minimal
    #       MESSAGEix-Materials structure for testing
    # TODO remove once those are created
    scenario.platform.add_unit("USD/KWa")
    with scenario.transact():
        scenario.add_set("mode", "M1")
        scenario.add_set(
            "relation",
            [
                "res_marg",
                "solar_step",
                "solar_step2",
                "solar_step3",
                "wind_step",
                "wind_step2",
                "wind_step3",
            ],
        )
        scenario.add_set("technology", "elec_t_d")

        # Add scenario data expected by the function
        par = "growth_activity_up"
        scenario.add_par(
            par,
            make_df(
                par,
                node_loc=["R12_CHN", "R12_FSU", "R12_LAM", "R12_MEA", "R12_PAS"],
                technology="solar_pv_ppl",
                value="-1",
                time="year",
                year_act=info.Y[0],
                unit="kg",
            ),
        )

        par = "technical_lifetime"
        df = make_df(par, value=30.0, unit="y").pipe(
            broadcast,
            node_loc=info.N,
            technology=[
                "solar_i",
                "h2_fc_I",
                "h2_fc_RC",
                "solar_pv_ppl",
                "stor_ppl",
                "h2_elec",
            ],
            year_vtg=info.Y,
        )
        scenario.add_par(par, df)

    add_LED_setup(scenario)

    # Ensure units column was not dropped during data operations
    df = scenario.par("inv_cost", filters={"technology": "solar_pv_ppl"})
    assert "???" not in df.unit.unique()


def test_limit_h2(request: "FixtureRequest", test_context: "Context") -> None:
    ctx = test_context
    ctx.update(**BARE_RES)
    scenario = testing.bare_res(request, ctx)

    # Add scenario structure expected by the function
    # FIXME this should be handled either by the contents of testing.bare_res() (i.e.
    #       message_ix_models.model.bare and associated code lists), or by a function
    #       like .material.testing.built_material() that sets up a minimal
    #       MESSAGEix-Materials structure for testing
    # TODO remove once those are created
    with scenario.transact():
        scenario.add_set("mode", "M1")

    limit_h2(scenario, "green")
