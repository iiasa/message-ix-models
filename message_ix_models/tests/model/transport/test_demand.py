from iam_units import registry
import message_ix
import pytest

from message_data.model.bare import get_spec
from message_data.model.transport import demand, plot


def test_demand_dummy(test_context):
    """Consumer-group-specific commodities are generated."""
    test_context.regions = "R11"
    info = get_spec(test_context)["add"]

    assert any(demand.dummy(info)["commodity"] == "transport pax URLMM")


def test_from_external_data(test_context, tmp_path):
    test_context.regions = "R11"
    info = get_spec(test_context)["add"]

    rep = demand.from_external_data(info, context=test_context)

    # These units are implied by the test of "transport pdt:*:mode":
    # "GDP PPP:n-y" → "MUSD / year"
    # "GDP PPP per capita:n-y" → "kUSD / passenger / year"
    # "transport pdt:n-y") → "Mm / year"

    # Total demand by mode
    key = "transport pdt:n-y-t:mode"

    # Graph structure can be visualized
    import dask
    from dask.optimization import cull

    dsk, deps = cull(rep.graph, key)
    dask.visualize(dsk, filename=str(tmp_path / "demand-graph.pdf"))

    # Can be computed
    result = rep.get(key)
    # Has correct units: km / year / capita
    assert (
        registry.Quantity(1, result.attrs["_unit"]) == registry("1 km / year")
    )

    # Can be plotted
    key = "transport pdt plot"
    p = plot.ModeShare2
    rep.add(key, tuple([p(), "config"] + p.inputs))
    rep.get(key)


@pytest.mark.skip(reason="Requires user's context")
def test_from_scenario(user_context):
    url = "ixmp://reporting/CD_Links_SSP2_v2.1_clean/baseline"
    scenario, mp = message_ix.Scenario.from_url(url)

    demand.from_scenario(scenario)
