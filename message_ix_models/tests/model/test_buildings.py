from collections import namedtuple

import pandas as pd
import pytest

from message_data.model.buildings import Config, sturm
from message_data.model.buildings.build import get_spec, get_techs
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


def test_configure_legacy_reporting(test_context):
    config = dict()

    configure_legacy_reporting(config)

    # Generated technology names are added to the appropriate sets
    assert ["meth_afofi"] == config["rc meth"]
    assert "h2_fc_AFOFI" in config["rc h2"]


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
