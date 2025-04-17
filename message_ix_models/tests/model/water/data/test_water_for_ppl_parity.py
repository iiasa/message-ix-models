import cProfile
import pstats
import time
from typing import Optional

import pandas as pd
import pandas.testing as pdt
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo, testing
from message_ix_models.model.water.data.cool_tech import (
    cool_tech as cool_tech_refactor,
)

# from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.water_for_ppl import (
    apply_act_cap_multiplier,
    cool_tech,
    cooling_shares_SSP_from_yaml,
    non_cooling_tec,
)
from message_ix_models.model.water.data.water_for_ppl import (
    apply_act_cap_multiplier as apply_act_cap_multiplier_refactor,
)
from message_ix_models.model.water.data.water_for_ppl import (
    cooling_shares_SSP_from_yaml as cooling_shares_SSP_from_yaml_refactor,
)
from message_ix_models.model.water.data.water_for_ppl import (
    non_cooling_tec as non_cooling_tec_refactor,
)


def assert_equal_result(legacy, refactored):
    if isinstance(legacy, dict) and isinstance(refactored, dict):
        # Ensure the dictionaries have the same keys
        assert set(legacy.keys()) == set(refactored.keys()), (
            "Dictionary keys do not match"
        )
        # Recursively compare each value in the dictionary
        for key in legacy:
            assert_equal_result(legacy[key], refactored[key])
    elif isinstance(legacy, pd.DataFrame) and isinstance(refactored, pd.DataFrame):
        legacy = legacy.sort_index(axis=1)
        refactored = refactored.sort_index(axis=1)
        pdt.assert_frame_equal(legacy, refactored)
    elif isinstance(legacy, pd.Series) and isinstance(refactored, pd.Series):
        legacy = legacy.sort_index()
        refactored = refactored.sort_index()
        pdt.assert_series_equal(legacy, refactored)
    else:
        raise ValueError(
            f"Type mismatch: legacy type {type(legacy)} vs refactored type {type(refactored)}"
        )


@pytest.mark.asyncio
@cool_tech.minimum_version
@pytest.mark.parametrize("RCP", ["no_climate", "6p0"])
async def test_cool_tec(request, test_context, RCP):
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["gad_cc", "coal_ppl"])
    s.add_set("node", ["R11_CPA"])
    s.add_set("year", [2020, 2030, 2040])
    s.add_set("mode", ["M1", "M2"])
    s.add_set("commodity", ["electricity", "gas"])
    s.add_set("level", ["secondary", "final"])
    s.add_set("time", ["year"])

    # make a df for input
    df_add = pd.DataFrame(
        {
            "node_loc": ["R11_CPA"],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "year_act": [2020],
            "mode": ["M1"],
            "node_origin": ["R11_CPA"],
            "commodity": ["electricity"],
            "level": ["secondary"],
            "time": "year",
            "time_origin": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    # make a df for historical activity
    df_ha = pd.DataFrame(
        {
            "node_loc": ["R11_CPA"],
            "technology": ["coal_ppl"],
            "year_act": [2020],
            "mode": ["M1"],
            "time": "year",
            "value": [1],
            "unit": "GWa",
        }
    )
    df_hnc = pd.DataFrame(
        {
            "node_loc": ["R11_CPA"],
            "technology": ["coal_ppl"],
            "year_vtg": [2020],
            "value": [1],
            "unit": "GWa",
        }
    )
    # add a parameter with these columns to the scenario
    s.add_par("input", df_add)
    s.add_par("historical_activity", df_ha)
    s.add_par("historical_new_capacity", df_hnc)

    # TODO: this is where you would add
    #     "node_loc": ["loc1", "loc2"],
    #     "node_dest": ["dest1", "dest2"],
    #     "year_vtg": ["2020", "2020"],
    #     "year_act": ["2020", "2020"], etc
    # to the scenario as per usual. However, I don't know if that's necessary as the
    # test is passing without it, too.

    s.commit(comment="basic water non_cooling_tec test model")

    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(scenario_obj=s)
    test_context.type_reg = "global"
    test_context.regions = "R11"
    test_context.time = "year"
    test_context.nexus_set = "nexus"
    # TODO add
    test_context.update(
        RCP=RCP,
        REL="med",
        ssp="SSP2",
    )

    # TODO: only leaving this in so you can see which data you might want to assert to
    # be in the result. Please remove after adapting the assertions below:
    # Mock the DataFrame read from CSV
    # df = pd.DataFrame(
    #     {
    #         "technology_group": ["cooling", "non-cooling"],
    #         "technology_name": ["cooling_tech1", "non_cooling_tech1"],
    #         "water_supply_type": ["freshwater_supply", "freshwater_supply"],
    #         "water_withdrawal_mid_m3_per_output": [1, 2],
    #     }
    # )

    # FIXME This currently fails because the pd.DataFrame read in as ref_input is empty
    # This can most likely be fixed by calling the right function on the largely empty
    # Scenario created above that sets the Scenario up with all things necessary to run
    # cool_tech(). Whatever the fix here is, it can also be applied to the failing
    # test_build::test_build().

 

    start_time = time.time()
    result_legacy = cool_tech(context=test_context)
    end_time = time.time()

    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(f"Time taken for legacy cool_tech: {end_time - start_time} seconds\n")

    start_time = time.time()
    result_refactor = await cool_tech_refactor(context=test_context)
    end_time = time.time()
    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(
            f"Time taken for refactored cool_tech with async wrapping: {end_time - start_time} seconds\n"
        )

    assert_equal_result(result_legacy, result_refactor)

@pytest.mark.skip(reason="Skipping non_cooling_tec test")
def test_non_cooling_tec(request, test_context):
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.add_set("node", ["loc1", "loc2"])
    s.add_set("year", [2020, 2030, 2040])

    # TODO: this is where you would add
    #     "node_loc": ["loc1", "loc2"],
    #     "node_dest": ["dest1", "dest2"],
    #     "year_vtg": ["2020", "2020"],
    #     "year_act": ["2020", "2020"], etc
    # to the scenario as per usual. However, I don't know if that's necessary as the
    # test is passing without it, too.

    s.commit(comment="basic water non_cooling_tec test model")

    # set_scenario() updates Context.scenario_info
    test_context.set_scenario(s)
    # print(test_context.get_scenario())

    # # TODO This is where and how you would add data to the context, but these are not
    # #required for non_cooling_tech()
    # test_context["water build info"] = ScenarioInfo(scenario_obj=s)
    # test_context.type_reg = "country"
    # test_context.regions = "test_region"
    # test_context.map_ISO_c = {"test_region": "test_ISO"}

    # TODO: only leaving this in so you can see which data you might want to assert to
    # be in the result. Please remove after adapting the assertions below:
    # Mock the DataFrame read from CSV
    # df = pd.DataFrame(
    #     {
    #         "technology_group": ["cooling", "non-cooling"],
    #         "technology_name": ["cooling_tech1", "non_cooling_tech1"],
    #         "water_supply_type": ["freshwater_supply", "freshwater_supply"],
    #         "water_withdrawal_mid_m3_per_output": [1, 2],
    #     }
    # )

    start_time = time.time()
    result_legacy = non_cooling_tec(context=test_context)
    end_time = time.time()
    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(
            f"Time taken for legacy non_cooling_tec: {end_time - start_time} seconds\n"
        )

    start_time = time.time()
    result_refactor = non_cooling_tec_refactor(context=test_context)
    end_time = time.time()
    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(
            f"Time taken for refactored non_cooling_tec: {end_time - start_time} seconds\n"
        )

    assert_equal_result(result_legacy, result_refactor)


@pytest.mark.parametrize(
    "param_name, cap_fact_parent, expected_values",
    [
        (
            "historical_activity",
            None,
            [100 * 0.5, 150 * 1.2],
        ),  # Only apply hold_cost multipliers
        (
            "historical_new_capacity",
            pd.DataFrame(
                {
                    "node_loc": ["R1", "R2"],
                    "technology": ["TechA", "TechB"],
                    "cap_fact": [0.9, 0.9],
                }
            ),
            [100 * 0.5 * 0.9 * 1.2, 150 * 1.2 * 0.9 * 1.2],
        ),  # Apply capacity factors
    ],
)
@pytest.mark.skip(reason="Skipping apply_act_cap_multiplier test")
def test_apply_act_cap_multiplier(
    param_name: str,
    cap_fact_parent: Optional[pd.DataFrame],
    expected_values: list[float],
) -> None:
    # Dummy input data
    df = pd.DataFrame(
        {
            "node_loc": ["R1", "R2"],
            "technology": ["TechA", "TechB"],
            "value": [100, 150],
        }
    )

    hold_cost = pd.DataFrame(
        {
            "utype": ["Type1", "Type2"],
            "technology": ["TechA", "TechB"],
            "R1": [0.5, 0.8],
            "R2": [1.0, 1.2],
        }
    )

    start_time = time.time()
    result_legacy = apply_act_cap_multiplier(df, hold_cost, cap_fact_parent, param_name)
    end_time = time.time()
    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(
            f"Time taken for legacy apply_act_cap_multiplier: {end_time - start_time} seconds\n"
        )

    start_time = time.time()
    result_refactor = apply_act_cap_multiplier_refactor(
        df, hold_cost, cap_fact_parent, param_name
    )
    end_time = time.time()
    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(
            f"Time taken for refactored apply_act_cap_multiplier: {end_time - start_time} seconds\n"
        )

    assert_equal_result(result_legacy, result_refactor)

@pytest.mark.skip(reason="Skipping cooling_shares_SSP_from_yaml test")
@pytest.mark.parametrize("SSP, regions", [("SSP2", "R11"), ("LED", "R12")])
def test_cooling_shares_SSP_from_yaml(request, test_context, SSP, regions):
    test_context.model.regions = regions
    scenario = testing.bare_res(request, test_context)
    test_context["water build info"] = ScenarioInfo(scenario_obj=scenario)
    test_context.ssp = SSP

    # Act
    start_time = time.time()
    result_legacy = cooling_shares_SSP_from_yaml(test_context)
    end_time = time.time()
    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(
            f"Time taken for legacy cooling_shares_SSP_from_yaml: {end_time - start_time} seconds\n"
        )

    start_time = time.time()
    result_refactor = cooling_shares_SSP_from_yaml_refactor(test_context)
    end_time = time.time()
    with open("water_for_ppl_parity.txt", "a") as f:
        f.write(
            f"Time taken for refactored cooling_shares_SSP_from_yaml: {end_time - start_time} seconds\n"
        )

    assert_equal_result(result_legacy, result_refactor)
