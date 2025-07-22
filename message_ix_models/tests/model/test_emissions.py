import genno
import numpy as np
import pytest
from genno import Key
from message_ix import make_df
from message_ix.models import MACRO

from message_ix_models import ScenarioInfo, testing
from message_ix_models.model.emissions import (
    PRICE_EMISSION,
    add_tax_emission,
    get_emission_factors,
)
from message_ix_models.testing import bare_res
from message_ix_models.tools.exo_data import prepare_computer
from message_ix_models.util import package_data_path


class TestPRICE_EMISSION:
    @pytest.mark.parametrize(
        "source_kw, shape",
        (
            pytest.param(
                dict(scenario_info=ScenarioInfo()),
                (),
                marks=pytest.mark.xfail(raises=ValueError),
            ),
            (
                dict(
                    scenario_info=ScenarioInfo.from_url(
                        "SSP_LED_v5.3.1/baseline_1000f#1"
                    )
                ),
                (2, 2, 2, 12),
            ),
        ),
    )
    @pytest.mark.parametrize("method", ("apply", "prepare_computer"))
    def test_add_tasks(
        self, request, test_context, source_kw, shape, method, regions="R12"
    ) -> None:
        test_context.model.regions = regions

        source_kw.update(
            base_path=package_data_path("transport", regions, "price-emission")
        )

        c = genno.Computer()

        if method == "apply":
            # Current method for adding tasks
            keys = c.apply(PRICE_EMISSION.add_tasks, context=test_context, **source_kw)
        elif method == "prepare_computer":
            # Old method for adding tasks
            source = "message_ix_models.model.emissions.PRICE_EMISSION"
            keys = prepare_computer(test_context, c, source, source_kw)

        # Tasks are added to the graph
        assert isinstance(keys, tuple) and len(keys)

        # Key has expected dimensions
        exp = Key("PRICE_EMISSION:n-type_emission-type_tec-y:exo")
        assert exp == keys[0]

        # Preparation of data runs successfully
        result = c.get(keys[0])

        assert exp.dims == result.dims  # Result has expected dimensions
        assert shape == result.shape  # Result has expected shape

        # Result can be converted to MESSAGE data frame and added to a scenario
        c.require_compat("message_ix.report.operator")
        dims = {d: d for d in exp.dims} | {"node": "n", "type_year": "y"}
        kw = dict(name="tax_emission", dims=dims, common={})
        c.add("tmp", "as_message_df", keys[0], **kw)
        scenario = bare_res(request, test_context)
        c.add("store", "add_par_data", scenario, "tmp")

        with scenario.transact(""):
            # Add necessary set elements for data
            # TODO Transfer these entries to technology.yaml/emission.yaml
            scenario.add_set("node", "R12_GLB")
            scenario.add_set("type_emission", ["CO2_shipping_IMO", "TCE"])
            scenario.add_set("type_tec", ["bunkers"])

            result = c.get("store")


def add_test_data(scenario):
    scenario.platform.add_unit("")
    with scenario.transact():
        scenario.add_set("node", "foo")
        scenario.add_set("type_emission", "TCE")

        name = "interestrate"
        df = make_df(name, year=scenario.set("year"), value=0.05, unit="")
        scenario.add_par(name, df)

        # Initialize drate
        MACRO.initialize(scenario)
        name = "drate"
        df = scenario.add_par(
            name, make_df(name, node=["World", "foo"], value=[0.05, 0.03], unit="")
        )


def test_add_tax_emission(request, caplog, test_context):
    test_context.regions = "R12"
    s = testing.bare_res(request, test_context, solved=False)
    add_test_data(s)

    value = 1.1

    add_tax_emission(s, value, drate_parameter="interestrate")

    # Retrieve the added data
    data = s.par("tax_emission").set_index("type_year")
    y0 = min(data.index)
    y_max = max(data.index)

    # First model period value is converted from [money] / t CO₂ to [money] / t C
    v0 = value * 44.0 / 12
    assert np.isclose(v0, data.loc[y0, "value"])

    # Final period value is the same, inflated by the number of intervening years
    assert np.isclose(v0 * 1.05 ** (int(y_max) - int(y0)), data.loc[y_max, "value"])

    # Same using drate
    add_tax_emission(s, value)
    # Warning is logged about multiple drates
    assert (
        "Using the first of multiple discount rates: drate=[0.05 0.03]"
        == caplog.messages[-1]
    )


@pytest.mark.parametrize(
    "units, exp_coal",
    (
        (None, 25.8),
        # Unit expressions and values appearing in the message_doc table
        ("tC / TJ", 25.8),
        ("t CO2 / TJ", 94.6),
        ("t C / kWa", 0.8142),
    ),
)
def test_get_emission_factors(units, exp_coal):
    # Data are loaded
    result = get_emission_factors(units=units)
    assert 8 == result.size

    # Expected values are obtained
    assert np.isclose(exp_coal, result.sel(c="coal").item(), rtol=1e-4)
