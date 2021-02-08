from iam_units import registry
import message_ix
import pytest
from pytest import param

from message_data import testing
from message_data.model.bare import get_spec
from message_data.model.transport import demand, plot


def test_demand_dummy(transport_context):
    """Consumer-group-specific commodities are generated."""
    info = get_spec(transport_context)["add"]

    assert any(demand.dummy(info)["commodity"] == "transport pax URLMM")


@pytest.mark.parametrize(
    "regions", ["R11", param("R14", marks=testing.NIE), param("ISR", marks=testing.NIE)]
)
def test_from_external_data(transport_context_f, tmp_path, regions):
    ctx = transport_context_f
    ctx.regions = regions
    ctx.output_path = tmp_path

    info = get_spec(ctx)["add"]
    rep = demand.from_external_data(info, context=ctx)

    # These units are implied by the test of "transport pdt:*":
    # "GDP PPP:n-y" → "MUSD / year"
    # "GDP PPP per capita:n-y" → "kUSD / passenger / year"
    # "transport pdt:n-y:total") → "Mm / year"

    # Share weight
    print(rep.describe("share weight"))
    rep.get("share weight")

    # Total demand by mode
    key = "transport pdt:n-y-t"

    # Graph structure can be visualized
    import dask
    from dask.optimization import cull

    dsk, deps = cull(rep.graph, key)
    dask.visualize(dsk, filename=str(tmp_path / "demand-graph.pdf"))

    # Can be computed
    result = rep.get(key)
    # Has correct units: km / year / capita
    assert registry.Quantity(1, result.attrs["_unit"]) == registry("1 km / year")

    # Can be plotted
    key = "transport pdt plot"
    p = plot.DemandExo
    rep.add(key, tuple([p(), "config"] + p.inputs))
    rep.get(key)


@pytest.mark.skip(reason="Requires user's context")
def test_from_scenario(user_context):
    url = "ixmp://reporting/CD_Links_SSP2_v2.1_clean/baseline"
    scenario, mp = message_ix.Scenario.from_url(url)

    demand.from_scenario(scenario)
