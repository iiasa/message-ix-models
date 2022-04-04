import logging

import message_ix
import pytest
from genno.testing import assert_units
from message_ix.reporting import Key
from message_ix_models.model import bare
from message_ix_models.model.structure import get_codes
from message_ix_models.testing import NIE
from pytest import param

from message_data.model.transport import build, demand, configure

from . import MARK

log = logging.getLogger(__name__)


@pytest.mark.parametrize("regions", ["R11", "R14", "ISR"])
@pytest.mark.parametrize("years", ["A", "B"])
def test_demand_dummy(test_context, regions, years):
    """Consumer-group-specific commodities are generated."""
    ctx = test_context
    ctx.regions = regions
    ctx.years = years

    configure(ctx)

    spec = build.get_spec(ctx)

    args = (
        spec["add"].set["commodity"],
        spec["require"].set["node"],
        get_codes(f"year/{years}"),  # FIXME should be present in the spec
        ctx["transport config"],
    )

    # Returns empty dict without config flag set
    ctx["transport config"]["data source"]["demand dummy"] = False
    assert dict() == demand.dummy(*args)

    ctx["transport config"]["data source"]["demand dummy"] = True
    data = demand.dummy(*args)

    assert any(data["demand"]["commodity"] == "transport pax URLMM")


@pytest.mark.parametrize(
    "regions,years,N_node,mode_shares",
    [
        ("R11", "A", 11, None),
        ("R11", "B", 11, None),
        ("R11", "B", 11, "debug"),
        ("R11", "B", 11, "A---"),
        ("R12", "B", 12, None),
        param("R14", "B", 14, None, marks=MARK[0]),
        param("ISR", "A", 1, None, marks=NIE),
    ],
)
def test_exo(test_context, tmp_path, regions, years, N_node, mode_shares):
    """Exogenous demand calculation succeeds."""
    rep, info = demand_computer(
        test_context,
        tmp_path,
        regions,
        years,
        options={"futures-scenario": mode_shares}
        if mode_shares is not None
        else dict(),
    )

    for key, unit in (
        ("population:n-y", "Mpassenger"),
        ("GDP:n-y:PPP+capita", "kUSD / passenger / year"),
        ("votm:n-y", ""),
        ("PRICE_COMMODITY:n-c-y:transport+smooth", "USD / km"),
        ("cost:n-y-c-t", "USD / km"),
        # These units are implied by the test of "transport pdt:*":
        # "transport pdt:n-y:total" [=] Mm / year
        ("transport pdt:n-y-t", "passenger km / year"),
    ):
        try:
            # Quantity can be computed
            qty = rep.get(key)

            # Quantity has the expected units
            assert_units(qty, unit)

            # Quantity has the expected size on the n/node dimension
            assert N_node == len(qty.coords["n"])
        except AssertionError:
            # Something else
            print(f"\n\n-- {key} --\n\n")
            print(rep.describe(key))
            print(qty, qty.attrs, qty.dims, qty.coords)
            raise

    # Demand is expressed for the expected quantities
    data = rep.get("demand:ixmp")

    # Returns a dict with a single key/DataFrame
    df = data.pop("demand")
    assert 0 == len(data)

    # Both LDV and non-LDV commodities are demanded
    assert {"transport pax RUEMF", "transport pax air"} < set(df["commodity"])

    # Demand covers the model horizon
    assert set(info.Y) == set(df["year"].unique()), (
        "`demand` does not cover the model horizon",
        df,
    )


def test_exo_report(test_context, tmp_path):
    """Exogenous demand results can be plotted.

    Separated from the above because the plotting step is slow.
    """
    rep, info = demand_computer(
        test_context,
        tmp_path,
        regions="R12",
        years="B",
        options={"futures-scenario": "debug"},
    )

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


def demand_computer(test_context, tmp_path, regions, years, options):
    # TODO convert to a fixture
    ctx = test_context
    ctx.update(regions=regions, years=years, output_path=tmp_path)
    configure(ctx, options=options)

    spec = bare.get_spec(ctx)

    rep = message_ix.Reporter()
    demand.prepare_reporter(rep, context=ctx, exogenous_data=True, info=spec["add"])
    rep.configure(output_dir=tmp_path)

    return rep, spec["add"]


@pytest.mark.parametrize(
    "regions", ["R11", "R12", param("R14", marks=MARK[0]), param("ISR", marks=NIE)]
)
@pytest.mark.parametrize("years", ["B"])
@pytest.mark.parametrize("pop_scen", ["SSP2"])
def test_cg_shares(test_context, tmp_path, regions, years, pop_scen):
    c, info = demand_computer(
        test_context,
        tmp_path,
        regions,
        years,
        options={"data source": {"population": pop_scen}},
    )

    key = Key("cg share", "n y cg".split())
    result = c.get(key)

    # Data have the correct size
    exp = dict(n=len(info.set["node"]) - 1, y=len(info.Y), cg=27)

    # NB as of genno 1.3.0, can't use .sizes on AttrSeries:
    # assert result.sizes == exp
    obs = {dim: len(result.coords[dim]) for dim in exp.keys()}
    assert exp == obs, result.coords

    # Data sum to 1 across the consumer_group dimension, i.e. constitute a discrete
    # distribution
    assert (result.sum("cg") - 1.0 < 1e-08).all()


@pytest.mark.parametrize(
    "regions,years,pop_scen",
    [
        ("R11", "A", "GEA mix"),
        ("R11", "A", "GEA supply"),
        ("R11", "A", "GEA eff"),
        # Different years
        ("R11", "B", "GEA mix"),
        # Different regions & years
        param("R14", "B", "SSP1", marks=MARK[0]),
        param("R14", "B", "SSP2", marks=MARK[0]),
        param("R14", "B", "SSP3", marks=MARK[0]),
        param("ISR", "B", "SSP2", marks=NIE),
    ],
)
def test_urban_rural_shares(test_context, tmp_path, regions, years, pop_scen):
    c, info = demand_computer(
        test_context,
        tmp_path,
        regions,
        years,
        options={"data source": {"population": pop_scen}},
    )

    # Shares can be retrieved
    key = Key("population", "n y area_type".split())
    result = c.get(key)

    assert key.dims == result.dims
    assert set(info.N[1:]) == set(result.coords["n"].values)
    assert set(info.Y) <= set(result.coords["y"].values)
    assert set(["UR+SU", "RU"]) == set(result.coords["area_type"].values)


@pytest.mark.skip(reason="Requires user's context")
def test_from_scenario(user_context):
    url = "ixmp://reporting/CD_Links_SSP2_v2.1_clean/baseline"
    scenario, mp = message_ix.Scenario.from_url(url)

    demand.from_scenario(scenario)


@pytest.mark.parametrize(
    "nodes, data_source",
    [
        ("R11", "GEA mix"),
        ("R12", "SSP2"),
        param("R14", "SSP2", marks=MARK[0]),
        ("R11", "SHAPE innovation"),
    ],
)
def test_cli(tmp_path, mix_models_cli, nodes, data_source):
    assert 0 == len(list(tmp_path.glob("*.csv")))

    result = mix_models_cli.invoke(
        [
            "transport",
            "gen-demand",
            f"--nodes={nodes}",
            "--years=B",
            data_source,
            str(tmp_path),
        ]
    )
    if result.exit_code != 0:
        print(result.output)
        raise result.exception

    # 1 file created in the temporary path
    assert 1 == len(list(tmp_path.glob("*.csv")))
