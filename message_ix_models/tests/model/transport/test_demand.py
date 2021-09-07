import logging
from itertools import dropwhile

import message_ix
import pytest
from message_ix.reporting import Key
from message_ix_models import testing
from message_ix_models.model.bare import get_spec
from message_ix_models.model.structure import get_codes
from message_ix_models.util import eval_anno
from pytest import param

from message_data.model.transport import demand, read_config
from message_data.model.transport.demand import assert_units

log = logging.getLogger(__name__)


@pytest.mark.parametrize("regions", ["R11", "R14", param("ISR", marks=testing.NIE)])
@pytest.mark.parametrize("years", ["A", "B"])
def test_demand_dummy(test_context, regions, years):
    """Consumer-group-specific commodities are generated."""
    ctx = test_context
    ctx.regions = regions
    ctx.years = years

    read_config(ctx)

    info = get_spec(test_context)["add"]

    assert any(demand.dummy(info)["commodity"] == "transport pax URLMM")


@pytest.mark.parametrize(
    "regions,source,years", [("R11", "GEA mix", "A"), ("R14", "SSP2", "B")]
)
def test_population(regions, source, years):
    # Inputs to the function: list of model nodes
    nodes = get_codes(f"node/{regions}")
    nodes = nodes[nodes.index("World")].child

    # Get the list of model periods
    # TODO move upstream to message_ix_models
    periods = get_codes(f"year/{years}")
    periods = list(
        map(
            lambda c: int(str(c)),
            dropwhile(lambda c: not eval_anno(c, "firstmodelyear"), periods),
        )
    )

    # Configuration (only the expected key)
    config = {"transport": {"data source": {"population": source}}}

    # Function runs
    result = demand.population(nodes, periods, config)

    # Data have expected dimensions, units, and coords
    assert ("n", "y") == result.dims
    assert_units(result, "Mpassenger")
    assert set(nodes) == set(result.coords["n"].values)
    assert set(periods) <= set(result.coords["y"].values)


@pytest.mark.parametrize(
    "regions,years,N_node",
    [
        ("R11", "A", 11),
        ("R11", "B", 11),
        ("R14", "B", 14),
        param("ISR", "A", 1, marks=testing.NIE),
    ],
)
def test_from_external_data(test_context, tmp_path, regions, years, N_node):
    """Exogenous demand calculation succeeds."""
    ctx = test_context
    ctx.regions = regions
    ctx.years = years
    ctx.output_path = tmp_path

    read_config(ctx)

    spec = get_spec(ctx)

    rep = message_ix.Reporter()
    demand.prepare_reporter(rep, context=ctx, exogenous_data=True, info=spec["add"])
    rep.configure(output_dir=tmp_path)

    for key, unit in (
        ("population:n-y", "Mpassenger"),
        ("GDP:n-y:PPP+capita", "kUSD / passenger / year"),
        ("votm:n-y", ""),
        ("PRICE_COMMODITY:n-c-y:transport+smooth", "USD / km"),
        ("cost:n-y-c-t", "USD / km"),
        ("transport pdt:n-y-t", "passenger km / year"),
        # These units are implied by the test of "transport pdt:*":
        # "transport pdt:n-y:total" [=] Mm / year
    ):
        try:
            # Quantity can be computed
            qty = rep.get(key)

            # Quantity has the expected units
            demand.assert_units(qty, unit)

            # Quantity has the expected size on the n/node dimension
            assert N_node == len(qty.coords["n"])
        except AssertionError:
            # Something else
            print(f"\n\n-- {key} --\n\n")
            print(rep.describe(key))
            print(qty, qty.attrs)
            raise

    # Total demand by mode
    key = Key("transport pdt", "nyt")

    # Graph structure can be visualized
    import dask
    from dask.optimization import cull

    dsk, deps = cull(rep.graph, key)
    path = tmp_path / "demand-graph.pdf"
    log.info(f"Visualize compute graph at {path}")
    dask.visualize(dsk, filename=str(path))

    # Plots can be generated
    rep.add("demand plots", ["plot demand-exo", "plot demand-exo-capita"])
    rep.get("demand plots")


@pytest.mark.skip(reason="Requires user's context")
def test_from_scenario(user_context):
    url = "ixmp://reporting/CD_Links_SSP2_v2.1_clean/baseline"
    scenario, mp = message_ix.Scenario.from_url(url)

    demand.from_scenario(scenario)
