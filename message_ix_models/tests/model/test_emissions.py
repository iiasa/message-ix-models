import numpy as np
from message_ix import make_df
from message_ix.models import MACRO

from message_ix_models import testing
from message_ix_models.model.emissions import add_tax_emission


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
