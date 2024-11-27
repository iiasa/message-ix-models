import logging
import re
from pathlib import Path

import genno
import pytest
from genno import ComputationError, Key
from genno.testing import assert_units
from pytest import param

from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import Config, build, demand, testing
from message_ix_models.model.transport.testing import MARK
from message_ix_models.project.ssp import SSP_2017, SSP_2024

log = logging.getLogger(__name__)


@pytest.mark.parametrize("regions", ["R11", "R14", "ISR"])
@pytest.mark.parametrize("years", ["A", "B"])
def test_demand_dummy(test_context, regions, years):
    """Consumer-group-specific commodities are generated."""
    ctx = test_context
    ctx.model.regions = regions
    ctx.model.years = years

    config = Config.from_context(ctx)

    spec = config.spec

    args = (
        spec.add.set["commodity"],
        spec.require.set["node"],
        get_codes(f"year/{years}"),  # FIXME should be present in the spec
        {"transport": ctx.transport},  # Minimal config object
    )

    # Returns empty dict without config flag set
    config.dummy_demand = False
    assert dict() == demand.dummy(*args)

    config.dummy_demand = True
    data = demand.dummy(*args)

    assert any(data["demand"]["commodity"] == "transport pax URLMM")


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions, years, N_node, options",
    [
        param("R11", "A", 11, dict(), marks=MARK[1]),
        param("R11", "B", 11, dict(), marks=MARK[1]),
        param("R11", "B", 11, dict(futures_scenario="debug"), marks=MARK[1]),
        param("R11", "B", 11, dict(futures_scenario="A---"), marks=MARK[1]),
        ("R12", "B", 12, dict()),
        ("R12", "B", 12, dict(navigate_scenario="act+ele+tec")),
        param("R14", "B", 14, dict(), marks=MARK[2](genno.ComputationError)),
        param("ISR", "A", 1, dict(), marks=MARK[3]),
    ],
)
def test_exo(test_context, tmp_path, regions, years, N_node, options):
    """Exogenous demand calculation succeeds."""
    c, info = testing.configure_build(
        test_context, tmp_path=tmp_path, regions=regions, years=years, options=options
    )

    # Check that some keys (a) can be computed without error and (b) have correct units
    # commented: these are slow because they repeat some calculations many times.
    # Uncommented as needed for debugging
    for key, unit in (
        # ("mode share:n-t-y:base", ""),
        # ("mode share:n-t-y", ""),
        # ("population:n-y", "Mpassenger"),
        # ("cg share:n-y-cg", ""),
        # ("GDP:n-y:PPP+capita", "kUSD / passenger / year"),
        # ("GDP:n-y:PPP+capita+index", ""),
        # ("votm:n-y", ""),
        # ("PRICE_COMMODITY:n-c-y:transport+smooth", "USD / km"),
        # ("cost:n-y-c-t", "USD / km"),
        # # These units are implied by the test of "transport pdt:*":
        # # "transport pdt:n-y:total" [=] Mm / year
        # (demand.pdt_nyt + "1", "passenger km / year"),
        # (demand.ldv_ny + "total", "Gp km / a"),
        # (demand.ldv_nycg, {"[length]": 1, "[passenger]": 1, "[time]": -1}),
        # ("pdt factor:n-y-t", ""),
        # ("fv factor:n-y", ""),
        # ("fv:n:advance", ""),
        # (demand.fv_cny, "Gt km"),
    ):
        try:
            # Quantity can be computed
            qty = c.get(key)

            # Quantity has the expected units
            assert_units(qty, unit)

            # Quantity has the expected size on the n/node dimension
            assert N_node == len(qty.coords["n"]), qty.coords["n"].data

            # commented: dump to a temporary path for inspection
            # fn = f"{key.replace(' ', '-')}-{hash(tuple(options.items()))}"
            # dump = tmp_path.joinpath(fn).with_suffix(".csv")
            # print(f"Dumped to {dump}")
            # qty.to_series().to_csv(dump)
        except Exception:
            # Something else
            print(f"\n\n-- {key} --\n\n")
            print(c.describe(key))
            print(qty.to_series().to_string(), qty.attrs, qty.dims, qty.coords)
            raise

    # Demand can be computed
    data = c.get("transport demand::ixmp")

    # Data is returned for the demand parameter only
    assert {"demand"} == set(data.keys())

    # Certain labels are specifically excluded/dropped in the calculation
    assert not {"transport pax ldv", "transport F WATER"} & set(
        data["demand"]["commodity"].unique()
    )
    assert {"useful"} == set(data["demand"]["level"].unique())

    # No missing data
    assert not data["demand"].isna().any().any()

    # No negative values
    check = data["demand"]["value"] < 0
    if check.any():  # pragma: no cover
        print(data["demand"][check].to_string())
        assert False, "Negative values in demand"


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "ssp",
    [
        pytest.param(SSP_2017["2"], marks=MARK[2](genno.ComputationError)),
        SSP_2024["1"],
        SSP_2024["2"],
        SSP_2024["3"],
        SSP_2024["4"],
        SSP_2024["5"],
    ],
)
def test_exo_pdt(test_context, ssp, regions="R12", years="B"):
    from message_ix_models.model.transport.testing import assert_units

    c, info = testing.configure_build(
        test_context, regions=regions, years=years, options=dict(ssp=ssp)
    )

    data = c.get("transport demand::ixmp")
    # data = c.get("t demand pax non-ldv::ixmp")

    # Returns a dict with a single key/data frame
    assert {"demand"} == set(data.keys())

    # Data have common, expected units
    for _, group_df in data["demand"].groupby("unit"):
        try:
            assert_units(group_df, {"[passenger]": 1, "[length]": 1, "[time]": -1})
        except AssertionError:
            continue
        else:
            df = group_df
            break

    # Passenger distance travelled is positive
    negative = df[df.value < 0]
    assert 0 == len(negative), f"Negative values in PDT:\n{negative.to_string()}"

    # Both LDV and non-LDV commodities are demanded
    assert {"transport pax RUEMF", "transport pax air"} < set(df["commodity"].unique())

    # Demand covers the model horizon
    assert set(info.Y) == set(df["year"].unique()), (
        "`demand` does not cover the model horizon",
        df,
    )


@MARK[7]
@build.get_computer.minimum_version
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


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions",
    [
        param("ISR", marks=MARK[3]),
        "R11",
        "R12",
        param("R14", marks=MARK[2]((AttributeError, ComputationError))),
    ],
)
@pytest.mark.parametrize("years", ["B"])
@pytest.mark.parametrize("pop_scen", [SSP_2017["2"], SSP_2024["2"]])
def test_cg_shares(test_context, tmp_path, regions, years, pop_scen):
    options = dict(ssp=pop_scen)
    c, info = testing.configure_build(
        test_context, tmp_path=tmp_path, regions=regions, years=years, options=options
    )

    key = Key("cg share", "n y cg".split())
    result = c.get(key)

    # Data have the correct size
    exp = dict(n=len(info.set["node"]) - 1, y=len(info.set["year"]), cg=27)

    # NB as of genno 1.3.0, can't use .sizes on AttrSeries:
    # assert result.sizes == exp
    obs = {dim: len(result.coords[dim]) for dim in exp.keys()}
    assert exp == obs, result.coords

    # Data sum to 1 across the consumer_group dimension, i.e. constitute a discrete
    # distribution
    assert (result.sum("cg") - 1.0 < 1e-08).all()


DATA = """
n        y     value
R11_AFR  2020  0
R11_AFR  2050  100
R11_AFR  2100  200
R11_WEU  2020  100
R11_WEU  2050  200
R11_WEU  2100  300
"""


@build.get_computer.minimum_version
def test_pdt_per_capita(
    tmp_path, test_context, regions="R12", years="B", options=dict()
):
    """Test calculation of PDT per capita, as configured by :func:`.pdt_per_capita`.

    Moved from :mod:`.test_operator`.
    """
    # TODO After #551, this is largely similar to test_exo and test_pdt; merge
    from message_ix_models.model.transport.key import pdt_cap

    c, info = testing.configure_build(
        test_context, tmp_path=tmp_path, regions=regions, years=years, options=options
    )

    result = c.get(pdt_cap)

    # Data have the expected dimensions and shape
    assert {"n", "y"} == set(result.dims)
    assert (12, 29) == result.shape
    # Data have the expected units
    assert_units(result, "km / year")


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions,years,pop_scen",
    [
        pytest.param("R11", "A", "GEA mix", marks=MARK[4]),
        pytest.param("R11", "A", "GEA supply", marks=MARK[4]),
        pytest.param("R11", "A", "GEA eff", marks=MARK[4]),
        # Different years
        pytest.param("R11", "B", "GEA mix", marks=MARK[4]),
        # Different regions & years
        ("R12", "B", SSP_2024["2"]),
        ("R14", "B", SSP_2017["1"]),
        ("R14", "B", SSP_2017["2"]),
        ("R14", "B", SSP_2017["3"]),
        pytest.param("ISR", "B", SSP_2024["2"], marks=MARK[3]),
    ],
)
def test_urban_rural_shares(test_context, tmp_path, regions, years, pop_scen):
    options = dict(ssp=pop_scen)
    c, info = testing.configure_build(
        test_context, tmp_path=tmp_path, regions=regions, years=years, options=options
    )

    # Shares can be retrieved
    key = Key("population", "n y area_type".split())
    result = c.get(key)

    assert set(key.dims) == set(result.dims)
    assert set(info.N[1:]) == set(result.coords["n"].values)
    assert set(info.Y) <= set(result.coords["y"].values)
    assert set(["UR+SU", "RU"]) == set(result.coords["area_type"].values)


@MARK[7]
@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "nodes, target",
    [
        pytest.param("R11", "GEA mix", marks=MARK[4]),
        ("R12", "SSP2"),
        ("R12", "SSP5"),
        ("R14", "SSP2"),
        pytest.param("R14", "SSP5", marks=MARK[2](RuntimeError)),
        pytest.param("R11", "SHAPE innovation", marks=MARK[4]),
    ],
)
def test_cli(tmp_path, mix_models_cli, test_context, nodes, target):
    """Transport CLI can be used to generate build-phase debug outputs."""
    # NB test_context is necessary so that the temporary, in-memory platform established
    #    by .transport.workflow.generate() does not carry to other tests
    cmd = ["transport", "run", f"--nodes={nodes}", f"{target} debug build", "--go"]
    result = mix_models_cli.assert_exit_0(cmd)

    # Identify the path containing the outputs
    expr = re.compile(r"Save to (.*)\.pdf$", flags=re.MULTILINE)
    output_dir = Path(expr.search(result.output).group(1)).parent

    # Files created in the temporary path
    assert 6 <= len(list(output_dir.glob("*.csv")))
    assert 3 <= len(list(output_dir.glob("*.pdf")))
