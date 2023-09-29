import logging

import pandas as pd
import pytest
from message_ix import make_df
from message_ix.testing import make_dantzig
from pandas.testing import assert_frame_equal
from sdmx.model.v21 import Code

from message_ix_models import ScenarioInfo, Spec
from message_ix_models.model.structure import get_codes, process_technology_codes
from message_ix_models.util import as_codes


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

    def test_units(self, caplog):
        """Test both :meth:`.io_units` and :meth:`.units_for`."""
        # Prepare ScenarioInfo with some commodities and technologies
        info = ScenarioInfo()
        info.set["commodity"] = get_codes("commodity")
        # NB create a technology with units annotation, since technology.yaml lacks
        #    units as of 2022-07-20
        t = as_codes({"example tech": {"units": "coulomb"}})
        process_technology_codes(t)
        info.set["technology"].extend(t)

        # units_for() runs, produces energy units
        c_units = info.units_for("commodity", "electr")
        assert {"[length]": 2, "[mass]": 1, "[time]": -2} == c_units.dimensionality

        # units_for() runs, produces expected units
        t_units = info.units_for("technology", "example tech")
        assert {"[current]": 1, "[time]": 1} == t_units.dimensionality

        # ValueError is raised for invalid input
        with pytest.raises(ValueError):
            info.units_for("commodity", "not a commodity")

        # io_units() runs, produces a ratio of commodity / technology units
        with caplog.at_level(logging.DEBUG, "message_ix_models"):
            assert (c_units / t_units) == info.io_units(
                "example tech", "electr", level="useful"
            )
        # level= keyword argument → logged warning
        assert "level = 'useful' ignored" == caplog.messages[-1]

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

    def test_repr(self):
        si = ScenarioInfo()
        si.set["foo"] = [1, 2, 3]
        assert "<ScenarioInfo: 3 code(s) in 1 set(s)>" == repr(si)

    def test_update(self):
        si = ScenarioInfo()
        si.par["demand"] = make_df("demand")

        # update() fails
        with pytest.raises(NotImplementedError):
            ScenarioInfo().update(si)

    @pytest.mark.parametrize(
        "codelist, y0, N_all, N_Y, y_m1, dp_checks",
        [
            (
                "A",
                2020,
                16,
                10,
                2110,
                ((1990, 10), (2010, 10), (2020, 10), (2050, 10), (2110, 10)),
            ),
            (
                "B",
                2020,
                28,
                14,
                2110,
                ((1990, 5), (2010, 5), (2020, 5), (2055, 5), (2110, 10)),
            ),
        ],
    )
    def test_year_from_codes(self, caplog, codelist, y0, N_all, N_Y, y_m1, dp_checks):
        caplog.set_level(logging.DEBUG, logger="message_ix_models")

        info = ScenarioInfo()
        codes = get_codes(f"year/{codelist}")
        info.year_from_codes(codes)

        # First model period
        assert y0 == info.y0
        assert ("firstmodelyear", y0) in info.set["cat_year"]

        # Total number of periods
        assert N_all == len(info.set["year"])

        # Number of model periods
        assert N_Y == len(info.Y)

        # Final period
        assert y_m1 == info.Y[-1]

        # Convert the data frame to a series
        dp = info.par["duration_period"].set_index("year")["value"]

        # duration_period entries are as expected
        for key, expected in dp_checks:
            assert expected == dp[key]

        # Test logging
        assert 0 == len(caplog.messages)

        info.year_from_codes(codes)

        assert 3 == len(caplog.messages)
        assert all(msg.startswith("Discard existing") for msg in caplog.messages)


class TestSpec:
    def test_getitem(self):
        s = Spec()
        with pytest.raises(KeyError):
            s["foo"]

    def test_setitem(self):
        s = Spec()

        # Setting a valid key via attribute access syntax works
        s.add = ScenarioInfo()
        # Setting a valid key via item access syntax works
        s["add"] = ScenarioInfo()

        # Setting an invalid key raises KeyError
        with pytest.raises(KeyError):
            s["foo"] = ScenarioInfo()

    def test_merge(self):
        s1 = Spec()
        s1.add.set["technology"] = ["t1", "t3", "t5"]
        s2 = Spec()
        s2.add.set["technology"] = ["t2", "t4", "t6"]

        s3 = Spec.merge(s1, s2)
        assert 6 == len(s3.add.set["technology"])
