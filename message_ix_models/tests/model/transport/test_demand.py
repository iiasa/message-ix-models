import logging

import pytest
from genno.testing import assert_units
from iam_units import registry
from message_ix.reporting import Key
from message_ix_models.model.structure import get_codes
from message_ix_models.testing import NIE
from pytest import param

from message_data.model.transport import Config, build, demand, testing
from message_data.model.transport.testing import MARK

log = logging.getLogger(__name__)


@pytest.mark.parametrize("regions", ["R11", "R14", "ISR"])
@pytest.mark.parametrize("years", ["A", "B"])
def test_demand_dummy(test_context, regions, years):
    """Consumer-group-specific commodities are generated."""
    ctx = test_context
    ctx.model.regions = regions
    ctx.model.years = years

    Config.from_context(ctx)

    spec = build.get_spec(ctx)

    args = (
        spec["add"].set["commodity"],
        spec["require"].set["node"],
        get_codes(f"year/{years}"),  # FIXME should be present in the spec
        {"transport": ctx.transport},  # Minimal config object
    )

    # Returns empty dict without config flag set
    ctx.transport.data_source.dummy_demand = False
    assert dict() == demand.dummy(*args)

    ctx.transport.data_source.dummy_demand = True
    data = demand.dummy(*args)

    assert any(data["demand"]["commodity"] == "transport pax URLMM")


@pytest.mark.parametrize(
    "regions, years, N_node, options",
    [
        param("R11", "A", 11, dict(), marks=MARK[1]),
        param("R11", "B", 11, dict(), marks=MARK[1]),
        param("R11", "B", 11, dict(futures_scenario="debug"), marks=MARK[1]),
        param("R11", "B", 11, dict(futures_scenario="A---"), marks=MARK[1]),
        ("R12", "B", 12, dict()),
        ("R12", "B", 12, dict(navigate_scenario="act+ele+tec")),
        param("R14", "B", 14, dict(), marks=NIE),
        param("ISR", "A", 1, dict(), marks=NIE),
    ],
)
def test_exo(test_context, tmp_path, regions, years, N_node, options):
    """Exogenous demand calculation succeeds."""
    c, info = testing.configure_build(
        test_context, tmp_path=tmp_path, regions=regions, years=years, options=options
    )

    # Check that some keys (a) can be computed without error and (b) have correct units
    for key, unit in (
        ("population:n-y", "Mpassenger"),
        ("cg share:n-y-cg", ""),
        ("GDP:n-y:PPP+capita", "kUSD / passenger / year"),
        ("GDP:n-y:PPP+capita+index", ""),
        ("votm:n-y", ""),
        ("PRICE_COMMODITY:n-c-y:transport+smooth", "USD / km"),
        ("cost:n-y-c-t", "USD / km"),
        # These units are implied by the test of "transport pdt:*":
        # "transport pdt:n-y:total" [=] Mm / year
        ("pdt:n-y-t", "passenger km / year"),
        ("ldv pdt:n-y:total", "Gp km / a"),
        # ("transport ldv pdt:n-y-cg", {"[length]": 1, "[passenger]": 1, "[time]": -1}),
        ("ldv pdt:n-y-cg", "Gp km / a"),
        ("pdt factor:n-y-t", ""),
        ("fv factor:n-y", ""),
        ("fv:n-y", "Gt km"),
    ):
        try:
            # Quantity can be computed
            qty = c.get(key)

            # Quantity has the expected units
            assert_units(qty, unit)

            # Quantity has the expected size on the n/node dimension
            assert N_node == len(qty.coords["n"]), qty.coords["n"].data

            if "factor" in key:
                fn = f"{key.replace(' ', '-')}-{hash(tuple(options.items()))}"
                dump = tmp_path.joinpath(fn).with_suffix(".csv")
                print(f"Dumped to {dump}")
                qty.to_series().to_csv(dump)
        except Exception:
            # Something else
            print(f"\n\n-- {key} --\n\n")
            print(c.describe(key))
            print(qty, qty.attrs, qty.dims, qty.coords)
            raise

    # Freight demand is available
    data = c.get("transport demand freight::ixmp")
    assert {"demand"} == set(data.keys())
    assert not data["demand"].isna().any().any()

    # Demand is expressed for the expected quantities
    data = c.get("transport demand passenger::ixmp")

    units = data["demand"]["unit"].unique()
    assert 1 == len(units)
    assert registry.Unit("Gp km / a") == registry.Unit(units[0])

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


@pytest.mark.skip(reason="Temporary, for #440; crashes pytest-xdist worker")
def test_exo_report(test_context, tmp_path):
    """Exogenous demand results can be plotted.

    Separated from the above because the plotting step is slow.
    """
    c, info = testing.configure_build(
        test_context,
        tmp_path=tmp_path,
        regions="R12",
        years="B",
        options=dict(futures_scenario="debug"),
    )

    # Total demand by mode
    key = Key("pdt", "nyt")

    # Graph structure can be visualized
    import dask
    from dask.optimization import cull

    dsk, deps = cull(c.graph, key)
    path = tmp_path / "demand-graph.pdf"
    log.info(f"Visualize compute graph at {path}")
    dask.visualize(dsk, filename=str(path))

    # Plots can be generated
    c.add("demand plots", ["plot demand-exo", "plot demand-exo-capita"])
    c.get("demand plots")


@pytest.mark.parametrize(
    "regions",
    [
        param("ISR", marks=NIE),
        "R11",
        "R12",
        param("R14", marks=NIE),
    ],
)
@pytest.mark.parametrize("years", ["B"])
@pytest.mark.parametrize("pop_scen", ["SSP2"])
def test_cg_shares(test_context, tmp_path, regions, years, pop_scen):
    c, info = testing.configure_build(
        test_context,
        tmp_path=tmp_path,
        regions=regions,
        years=years,
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
        ("R14", "B", "SSP1"),
        ("R14", "B", "SSP2"),
        ("R14", "B", "SSP3"),
        ("ISR", "B", "SSP2"),
    ],
)
def test_urban_rural_shares(test_context, tmp_path, regions, years, pop_scen):
    c, info = testing.configure_build(
        test_context,
        tmp_path=tmp_path,
        regions=regions,
        years=years,
        options={"data source": {"population": pop_scen}},
    )

    # Shares can be retrieved
    key = Key("population", "n y area_type".split())
    result = c.get(key)

    assert set(key.dims) == set(result.dims)
    assert set(info.N[1:]) == set(result.coords["n"].values)
    assert set(info.Y) <= set(result.coords["y"].values)
    assert set(["UR+SU", "RU"]) == set(result.coords["area_type"].values)


@pytest.mark.parametrize(
    "nodes, data_source",
    [
        ("R11", "--source=GEA mix"),
        ("R12", "--source=SSP2"),
        ("R12", "--ssp-update=2"),
        param("R14", "--source=SSP2", marks=MARK[2](ValueError)),
        ("R11", "--source=SHAPE innovation"),
    ],
)
def test_cli(tmp_path, mix_models_cli, nodes, data_source):
    assert 0 == len(list(tmp_path.glob("*.csv")))

    result = mix_models_cli.invoke(
        [
            "transport",
            "gen-activity",
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
