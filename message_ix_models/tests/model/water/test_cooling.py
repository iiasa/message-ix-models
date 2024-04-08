from message_ix import Scenario

# from message_ix_models import ScenarioInfo
# from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.water_for_ppl import non_cooling_tec

# TODO Please add a similar test for cool_tech() :)


def test_non_cooling_tec(request, test_context):
    context = test_context
    mp = context.get_platform()
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

    s.commit(comment="basic water test model")

    # set_scenario() updates Context.scenario_info
    context.set_scenario(s)
    # print(context.get_scenario())

    # # TODO This is where and how you would add data to the context, but these are not
    # #required for non_cooling_tech()
    # context["water build info"] = ScenarioInfo(scenario_obj=s)
    # context.type_reg = "country"
    # context.regions = "test_region"
    # context.map_ISO_c = {"test_region": "test_ISO"}

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

    result = non_cooling_tec(context)

    # Assert the results
    assert isinstance(result, dict)
    assert "input" in result
    assert all(
        col in result["input"].columns
        for col in [
            "technology",
            "value",
            "unit",
            "level",
            "commodity",
            "mode",
            "time",
            "time_origin",
            "node_origin",
            "node_loc",
            "year_vtg",
            "year_act",
        ]
    )
