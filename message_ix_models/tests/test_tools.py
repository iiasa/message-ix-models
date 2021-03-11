import pandas.testing as pdt
from message_ix import make_df

from message_data.testing import bare_res
from message_data.tools.utilities import add_budget


def test_add_budget(request, test_context):
    # Create a empty Scenario (no data) with the RES structure for R14 (using test
    # utilities)
    test_context.regions = "R14"
    scen = bare_res(request, test_context, solved=False)

    # Add minimal structure for add_budget() to work
    scen.check_out()
    scen.add_set("type_emission", "TCE")
    scen.add_set("year", 2000)
    scen.add_set("cat_year", ("cumulative", 2000))
    scen.commit("Test prep")

    # Call the function on the prepared scenario
    add_budget(scen, 1000.0)

    # commented: for debugging, show the state of the scenario after the call
    # print(scen.par("bound_emission"))

    # The call above results in the expected contents in bound_emission
    expected = make_df(
        "bound_emission",
        node="World",
        type_emission="TCE",
        type_tec="all",
        type_year="cumulative",
        value=1000.0,
        unit="tC",
    )
    pdt.assert_frame_equal(expected, scen.par("bound_emission"))

    # Call again with adjust_cumulative=True

    assert 1 == len(
        scen.set("cat_year").query("type_year == 'cumulative' and year == 2000")
    )
    add_budget(scen, 1000.0, adjust_cumulative=True)

    # The year "2000" has been removed from the "cumulative" category of years because
    # it is before the firstmodelyear (2010 via bare_res())
    assert 0 == len(
        scen.set("cat_year").query("type_year == 'cumulative' and year == 2000")
    )
