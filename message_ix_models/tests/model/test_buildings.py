from collections import namedtuple

import numpy as np
import pandas as pd
import pytest

from message_data.model.buildings import Config, _mpd, sturm
from message_data.model.buildings.build import get_spec, get_tech_groups, get_techs
from message_data.model.buildings.report import (
    configure_legacy_reporting,
    report2,
    report3,
)


@pytest.mark.parametrize("commodity", [None, "gas"])
def test_get_techs(test_context, commodity):
    test_context.regions = "R12"
    spec = get_spec(test_context)
    result = get_techs(spec, commodity)

    # Generated technologies with buildings sector and end-use
    assert "gas_resid_cook" in result

    # Generated technologies for residuals of corresponding *_rc in the base model spec
    assert "gas_afofi" in result


@pytest.mark.parametrize(
    "args, present, absent",
    (
        # Default values of arguments, i.e. include="commodity enduse", legacy=False
        (dict(), {"rc", "comm hydrogen", "resid hotwater"}, set()),
        # As used e.g. in buildings reporting
        (dict(include="enduse"), {"resid hotwater", "rc"}, {"comm coal"}),
        # As used e.g. in legacy reporting. Assert that names like "h2" are used instead
        # of "hydrogen", and that end-use groups are not included.
        (
            dict(include="commodity", legacy=True),
            {"afofi", "comm h2", "resid heat"},
            {"comm other_uses"},
        ),
    ),
)
def test_get_tech_groups(test_context, args, present, absent):
    test_context.buildings = Config(sturm_scenario="")
    test_context.regions = "R12"

    spec = get_spec(test_context)

    # Function runs
    result = get_tech_groups(spec, **args)

    # # For debugging
    # for k in sorted(result.keys()):
    #     print(f"{k}:")
    #     print("  " + "\n  ".join(sorted(result[k])))

    # Certain keys are present
    assert set() == present - set(result)

    # Certain keys are absent
    assert set() == absent & set(result)


def test_configure_legacy_reporting(test_context):
    config = dict()

    configure_legacy_reporting(config)

    # Generated technology names are added to the appropriate sets
    assert ["meth_afofi"] == config["rc meth"]
    assert "h2_fc_AFOFI" in config["rc h2"]


def test_mpd():
    columns = ["node", "commodity", "year", "value"]

    # Function runs
    a = pd.DataFrame([["n1", "c1", "y1", 1.0]], columns=columns)
    b = pd.DataFrame([["n1", "c1", "y1", 1.1]], columns=columns)
    assert np.isclose(0.1 / (0.5 * 2.1), _mpd(a, b, "value"))

    # Returns NaN for various empty data frames
    c = pd.DataFrame()
    d = pd.DataFrame(columns=columns)
    assert np.isnan(_mpd(a, c, "value"))
    assert np.isnan(_mpd(a, d, "value"))
    assert np.isnan(_mpd(c, c, "value"))


def test_report3(test_data_path):
    # Mock contents of the Reporter
    s = namedtuple("Scenario", "scenario")("baseline")
    config = {"sturm output path": test_data_path.joinpath("buildings", "sturm")}

    sturm_rep = report2(s, config)
    result = report3(s, sturm_rep)

    # TODO add assertions
    del result


@pytest.mark.skip(reason="Slow")
@pytest.mark.parametrize("sturm_method", ["rpy2", "Rscript"])
def test_sturm_run(tmp_path, test_context, test_data_path, sturm_method):
    """Test that STURM can be run by either method."""
    test_context.model.regions = "R12"
    test_context.buildings = Config(
        sturm_method=sturm_method,
        sturm_scenario="NAV_Dem-NPi-ref",
        _output_path=tmp_path,
    )

    prices = pd.read_csv(
        test_data_path.joinpath("buildings", "prices.csv"), comment="#"
    )

    sturm.run(test_context, prices, True)


@pytest.mark.parametrize(
    "expected, input",
    [
        ("SSP2", "baseline"),
        ("NAV_Dem-NPi-act", "NAV_Dem-NPi-act"),
        ("NAV_Dem-NPi-act", "NAV_Dem-20C-act_u"),
        ("NAV_Dem-NPi-act", "NAV_Dem-20C-act_u + ENGAGE step 2"),
        # Without "NAV_Dem-"
        ("NAV_Dem-NPi-act", "20C-act_u + ENGAGE step 2"),
        # New naming as of 2022-11-22
        ("NAV_Dem-NPi-ref", "NPi-ref_EN1_1000_Gt"),
        ("NAV_Dem-NPi-all", "NAV_Dem-NPi-all"),
        ("NAV_Dem-NPi-ele", "NAV_Dem-NPi-ele"),
        ("NAV_Dem-NPi-ref", "NAV_Dem-NPi-ref"),
        ("NAV_Dem-NPi-ref", "NAV_Dem-NPi-ref"),
        ("NAV_Dem-NPi-ref", "NPi-ref_ENGAGE_20C_step-3+B"),
        ("NAV_Dem-NPi-ref", "Ctax-ref+B"),
    ],
)
def test_sturm_scenario_name(input, expected):
    assert expected == sturm.scenario_name(input)
