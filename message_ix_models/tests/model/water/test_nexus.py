import pandas as pd
import pytest
from message_ix_models.model import bare

from message_data.model.water import build
from message_data.model.water.data.demands import (
    add_irrigation_demand,
    add_sectoral_demands,
    add_water_availability,
)
from message_data.model.water.data.infrastructure import (
    add_desalination,
    add_infrastructure_techs,
)
from message_data.model.water.data.irrigation import add_irr_structure
from message_data.model.water.data.water_for_ppl import cool_tech, non_cooling_tec
from message_data.model.water.data.water_supply import add_e_flow, add_water_supply

pytestmark = pytest.mark.xfail(reason="The tests will be updated in the preceeding PR")


def configure_build(context, regions, years):
    context.update(regions=regions, years=years)

    # Information about the corresponding base model
    info = bare.get_spec(context)["add"]
    context["water build info"] = info
    context["water spec"] = build.get_spec(context)

    return info


# def test_build(request, test_context):
#     scenario = testing.bare_res(request, test_context)

#     # Code runs on the bare RES
#     build(scenario)

#     # New set elements were added
#     assert "extract_surfacewater" in scenario.set("technology").tolist()


# def test_get_spec(session_context):
#     # Code runs
#     spec = get_spec()

#     # Expected return type
#     assert isinstance(spec, dict) and len(spec) == 3

#     # Contents are read correctly
#     assert "water_supply" in spec["require"].set["level"]


# def test_read_config(session_context):
#     # read_config() returns a reference to the current context
#     context = read_config()
#     assert context is session_context

#     # 'add' elements have been converted to Code objects
#     assert isinstance(context["water"]["set"]["technology"]["add"][0], Code)


def test_read_data(test_context, regions, years):

    context = test_context
    # info = configure_build(context, regions, years)

    data_irr = add_irr_structure(context)

    # Returns a mapping
    assert {"input", "output"} == set(data_irr.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_irr.values()))

    data_water_ppl = cool_tech(context)

    # Returns a mapping
    assert {
        "growth_activity_lo",
        "growth_activity_up",
        "input",
        "output",
        "capacity_factor",
        "addon_conversion",
        "addon_lo",
        "inv_cost",
        "historical_new_capacity",
        "emission_factor",
        "historical_activity",
    } == set(data_water_ppl.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_water_ppl.values()))

    data_non_cool = non_cooling_tec(context)

    # Returns a mapping
    assert {"input"} == set(data_non_cool.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_non_cool.values()))

    data_infrastructure = add_infrastructure_techs(context)

    # Returns a mapping
    assert {
        "input",
        "output",
        "capacity_factor",
        "technical_lifetime",
        "inv_cost",
        "fix_cost",
        "construction_time",
    } == set(data_non_cool.keys())
    assert all(
        map(lambda df: isinstance(df, pd.DataFrame), data_infrastructure.values())
    )

    data_desal = add_desalination(context)
    # Returns a mapping
    assert {
        "input",
        "output",
        "bound_total_capacity_up",
        "historical_new_capacity",
        "inv_cost",
        "var_cost",
        "bound_activity_lo",
        "construction_time",
        "bound_total_capacity_up",
    } == set(data_desal.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_desal.values()))

    data_demands = add_sectoral_demands(context)
    # Returns a mapping
    assert {"demand", "historical_new_capacity", "share_commodity_lo"} == set(
        data_demands.keys()
    )
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_demands.values()))

    data_supply = add_water_supply(context)
    # Returns a mapping
    assert {
        "input",
        "output",
        "historical_new_capacity",
        "var_cost",
        "share_mode_up",
        "technical_lifetime",
        "inv_cost",
        "fix_cost",
        "",
    } == set(data_supply.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_supply.values()))

    data_eflow = add_e_flow(context)
    # Returns a mapping
    assert {"bound_activity_lo"} == set(data_eflow.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_eflow.values()))

    data_irr = add_irrigation_demand(context)
    # Returns a mapping
    assert {"land_input"} == set(data_irr.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_irr.values()))

    data_irr = add_irrigation_demand(context)
    # Returns a mapping
    assert {"land_input"} == set(data_irr.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_irr.values()))

    data_water_avail = add_water_availability(context)
    # Returns a mapping
    assert {"share_commodity_lo", "demand"} == set(data_water_avail.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data_water_avail.values()))
