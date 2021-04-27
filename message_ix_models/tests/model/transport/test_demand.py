import logging

import message_ix
import pytest
from message_ix_models.model.bare import get_spec
from pytest import param

from message_data import testing
from message_data.model.transport import demand, report

log = logging.getLogger(__name__)


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

    spec = get_spec(ctx)

    rep = message_ix.Reporter()
    demand.prepare_reporter(rep, context=ctx, exogenous_data=True, info=spec["add"])
    rep.configure(output_dir=tmp_path)

    for key, unit in (
        ("GDP:n-y:PPP+percapita", "kUSD / passenger / year"),
        ("votm:n-y", ""),
        ("PRICE_COMMODITY:n-c-y:transport+smooth", "USD / km"),
        ("cost:n-y-c-t", "USD / km"),
        ("transport pdt:n-y-t", "km / year"),
    ):
        try:
            qty = rep.get(key)
            demand.assert_units(qty, unit)
        except AssertionError:
            print(f"\n\n-- {key} --\n\n")
            print(rep.describe(key))
            print(qty, qty.attrs)
            raise

    # These units are implied by the test of "transport pdt:*":
    # "transport pdt:n-y:total") → "Mm / year"

    # Total demand by mode
    key = "transport pdt:n-y-t"

    # Graph structure can be visualized
    import dask
    from dask.optimization import cull

    dsk, deps = cull(rep.graph, key)
    path = tmp_path / "demand-graph.pdf"
    log.info(f"Visualize output at {path}")
    dask.visualize(dsk, filename=str(path))

    # Plots can be generated
    log.info(f"Plot demand data at {rep.get('plot demand-exo')}")


@pytest.mark.skip(reason="Requires user's context")
def test_from_scenario(user_context):
    url = "ixmp://reporting/CD_Links_SSP2_v2.1_clean/baseline"
    scenario, mp = message_ix.Scenario.from_url(url)

    demand.from_scenario(scenario)
