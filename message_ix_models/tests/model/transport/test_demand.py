import logging
import re
from pathlib import Path

from genno import ComputationError, Key
from genno.testing import assert_units
from pytest import mark, param

from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import Config, build, demand, testing, workflow
from message_ix_models.model.transport import plot as plots
from message_ix_models.model.workflow import STAGE
from message_ix_models.project.ssp import SSP_2017, SSP_2024
from message_ix_models.report import plot

log = logging.getLogger(__name__)


pytestmark = mark.transport_build_data


@mark.parametrize("regions", ["R11", "R14", "ISR"])
@mark.parametrize("years", ["A", "B"])
def test_demand_dummy(test_context: Context, regions: str, years: str) -> None:
    """Consumer-group-specific commodities are generated."""
    ctx = test_context
    ctx.model.regions = regions
    ctx.model.years = years

    config = Config.from_context(ctx)

    spec = config.spec

    args: tuple[list, list, list, dict] = (
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
@mark.parametrize(
    "ssp",
    [
        SSP_2017["2"],
        SSP_2024["1"],
        SSP_2024["2"],
        SSP_2024["3"],
        SSP_2024["4"],
        SSP_2024["5"],
    ],
)
def test_exo_pdt(
    test_context: Context, ssp: SSP_2017 | SSP_2024, regions="R12", years="B"
) -> None:
    from message_ix_models.model.transport.testing import assert_units

    c, info = testing.configure_build(
        test_context, regions=regions, years=years, options=dict(ssp=ssp)
    )

    data = c.get("transport demand::ixmp")
    # data = c.get("demand::P+ixmp")

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


@build.get_computer.minimum_version
def test_exo_report(test_context: Context, tmp_path: Path) -> None:
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
    path = tmp_path / "demand-graph.pdf"
    log.info(f"Visualize compute graph at {path}")
    c.visualize(path, key)

    # Add all BUILD/single plots and necessary keys, e.g. 'scenario'
    plot.prepare_computer(c, plots, "demand plots", stage=STAGE.BUILD)

    # Select only a couple
    c.add("demand plots", ["plot demand-exo", "plot demand-exo-capita"])

    # Plots can be generated
    c.get("demand plots")


@build.get_computer.minimum_version
@mark.parametrize(
    "regions",
    [
        param("ISR", marks=mark.ISR_no_data),
        "R11",
        "R12",
        param(
            "R14", marks=mark.no_data("node=R14", (AttributeError, ComputationError))
        ),
    ],
)
@mark.parametrize("years", ["B"])
@mark.parametrize("pop_scen", [SSP_2017["2"], SSP_2024["2"]])
def test_cg_shares(
    test_context: Context,
    tmp_path: Path,
    regions: str,
    years: str,
    pop_scen: SSP_2017 | SSP_2024,
) -> None:
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
    tmp_path: Path, test_context: Context, regions="R12", years="B", options=dict()
) -> None:
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
    assert (12, 28) == result.shape
    # Data have the expected units
    assert_units(result, "km / year")


@build.get_computer.minimum_version
@mark.parametrize(
    "regions,years,pop_scen",
    [
        param("R11", "A", "GEA mix", marks=mark.GEA_not_implemented),
        param("R11", "A", "GEA supply", marks=mark.GEA_not_implemented),
        param("R11", "A", "GEA eff", marks=mark.GEA_not_implemented),
        # Different years
        param("R11", "B", "GEA mix", marks=mark.GEA_not_implemented),
        # Different regions & years
        ("R12", "B", SSP_2024["2"]),
        ("R14", "B", SSP_2017["1"]),
        ("R14", "B", SSP_2017["2"]),
        ("R14", "B", SSP_2017["3"]),
        param("ISR", "B", SSP_2024["2"], marks=mark.ISR_no_data),
    ],
)
def test_urban_rural_shares(
    test_context: Context, tmp_path: Path, regions: str, years: str, pop_scen: str
) -> None:
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


@mark.gh_471
@mark.gh_375
@build.get_computer.minimum_version
@workflow.generate.minimum_version
@mark.parametrize(
    "nodes, target",
    [
        param("R11", "GEA mix", marks=mark.GEA_not_implemented),
        ("R12", "SSP2"),
        ("R12", "SSP5"),
        ("R14", "SSP2"),
        param("R14", "SSP5", marks=mark.no_data("node=R14, ssp=SSP5", RuntimeError)),
        param("R11", "SHAPE innovation", marks=mark.SHAPE_not_implemented),
    ],
)
def test_cli(
    tmp_path: Path, mix_models_cli, test_context: Context, nodes: str, target: str
) -> None:
    """Transport CLI can be used to generate build-phase debug outputs."""
    # NB test_context is necessary so that the temporary, in-memory platform established
    #    by .transport.workflow.generate() does not carry to other tests
    cmd = ["transport", "run", f"--nodes={nodes}", f"{target} T debug build", "--go"]
    result = mix_models_cli.assert_exit_0(cmd)

    # Identify the path containing the outputs
    expr = re.compile(r"Save to (.*)\.pdf$", flags=re.MULTILINE)
    match = expr.search(result.output)
    assert match
    output_dir = Path(match.group(1)).parent

    # Files created in the temporary path
    assert 6 <= len(list(output_dir.glob("*.csv")))
    assert 3 <= len(list(output_dir.glob("*.pdf")))
