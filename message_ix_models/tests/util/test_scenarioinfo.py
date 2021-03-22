import pandas as pd
from message_ix.testing import make_dantzig
from pandas.testing import assert_frame_equal
from sdmx.model import Code

from message_ix_models import ScenarioInfo


class TestScenarioInfo:
    def test_empty(self):
        """ScenarioInfo created from scratch."""
        info = ScenarioInfo()

        # Set values directly
        info.set["node"] = [Code(id="AT", name="Austria")]
        info.set["year"] = [1000, 1010, 1020, 1030]
        info.y0 = 1010

        # Shorthand properties

        # `yv_ya` is generated
        assert_frame_equal(
            pd.DataFrame(
                [
                    [1010, 1010],
                    [1010, 1020],
                    [1010, 1030],
                    [1020, 1020],
                    [1020, 1030],
                    [1030, 1030],
                ],
                columns=["year_vtg", "year_act"],
            ),
            info.yv_ya,
        )

        # List of Codes is converted to list of strings
        assert ["AT"] == info.N

        # Only years >= y0
        assert [1010, 1020, 1030] == info.Y

    def test_from_scenario(self, test_context):
        """ScenarioInfo initialized from an existing Scenario."""
        mp = test_context.get_platform()
        scenario = make_dantzig(mp, multi_year=True)

        # ScenarioInfo can be initialized from the scenario
        info = ScenarioInfo(scenario)

        # Shorthand properties
        assert_frame_equal(
            pd.DataFrame(
                [
                    [1962, 1963],
                    [1962, 1964],
                    [1962, 1965],
                    [1963, 1963],
                    [1963, 1964],
                    [1963, 1965],
                    [1964, 1964],
                    [1964, 1965],
                    [1965, 1965],
                ],
                columns=["year_vtg", "year_act"],
            ),
            info.yv_ya,
        )
        assert [
            "World",
            "seattle",
            "san-diego",
            "new-york",
            "chicago",
            "topeka",
        ] == info.N
        assert 1963 == info.y0
        assert [1963, 1964, 1965] == info.Y
