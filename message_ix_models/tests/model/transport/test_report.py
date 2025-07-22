import logging
from contextlib import nullcontext
from copy import deepcopy
from importlib.metadata import version
from typing import TYPE_CHECKING

import pytest
from packaging.version import Version as V
from pytest import mark, param

from message_ix_models import ScenarioInfo
from message_ix_models.model.transport import Config, build, key
from message_ix_models.model.transport.report import configure_legacy_reporting
from message_ix_models.model.transport.testing import (
    MARK,
    built_transport,
    make_mark,
    simulated_solution,
)
from message_ix_models.report import prepare_reporter
from message_ix_models.testing import GHA
from message_ix_models.util._logging import silence_log

if TYPE_CHECKING:
    import message_ix
    from genno.types import KeyLike

    from message_ix_models import Context

log = logging.getLogger(__name__)


@pytest.fixture
def quiet_genno(caplog):
    """Quiet some log messages from genno via by :func:`.reporting.prepare_reporter`."""
    caplog.set_level(logging.WARNING, logger="genno.config")
    caplog.set_level(logging.WARNING, logger="genno.compat.pyam")


@pytest.mark.xfail(
    reason="Requires variables in .report.legacy.default_tables that have not been "
    "migrated from message_data"
)
def test_configure_legacy():
    from message_ix_models.report.legacy.default_tables import TECHS

    config = deepcopy(TECHS)

    configure_legacy_reporting(config)

    # Number of technologies in data/transport/technology.yaml using the given commodity
    # as input, thus expected to be added to the respective legacy reporting sets
    expected = {
        "trp back": 0,
        "trp coal": 1,
        "trp elec": 7,
        "trp eth": 5,
        "trp foil": 1,
        "trp gas": 7,
        "trp h2": 4,
        "trp loil": 17,
        "trp meth": 5,
    }

    # Resulting lists have the expected length, or are unaltered
    for k, v in config.items():
        assert expected.get(k, 0) + len(TECHS[k]) == len(v), k


@pytest.mark.ece_db
@pytest.mark.parametrize(
    "url, key, verbosity",
    (
        (
            "ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-T-R12 ci nightly/SSP_2024.2 baseline#726",  # noqa: E501
            "base model data",
            1,
        ),
    ),
)
def test_debug(
    request, tmp_path, test_context, url, verbosity: int, key: "KeyLike"
):  # pragma: no cover
    """Test for debugging reporting of specific MESSAGEix-Transport scenarios.

    Similar to :func:`.transport.test_build.test_debug`.

    This **should** be invoked using the :program:`pytest … --ixmp-user-config` option.

    Parameters
    ----------
    key :
       Key to be reported. This should *not* be the default/general key, as that would
       result in updated time series being stored on the scenario at `url`.
    """
    # Populate test_context.transport
    Config.from_context(test_context)

    test_context.core.handle_cli_args(url=url, verbose=bool(verbosity))
    test_context.report.key = key
    test_context.report.register("model.transport")

    with nullcontext() if verbosity > 1 else silence_log("genno message_ix_models"):
        rep, _key = prepare_reporter(test_context)

    assert key == _key

    # Show what will be computed
    # verbosity = True  # DEBUG Force printing the description even if verbosity == 0
    if verbosity:
        print(rep.describe(key))

    # return  # DEBUG Exit before doing any computation

    # Reporting `key` succeeds
    tmp = rep.get(key)

    # DEBUG Handle a subset of the result for inspection
    # print(tmp)

    del tmp


@MARK[10]
@MARK[7]
@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions, years",
    (
        param("R11", "A", marks=make_mark[2](ValueError)),
        ("R12", "B"),
        param("R14", "A", marks=MARK[9]),
        param("ISR", "A", marks=MARK[3]),
    ),
)
def test_bare(request, test_context, tmp_path, regions, years):
    """Run MESSAGEix-Transport–specific reporting."""
    from message_ix_models.model.transport.report import callback
    from message_ix_models.report import Config

    # Update configuration
    # key = "transport all"  # All including plots, etc.
    key = "transport::iamc+all"  # IAMC-structured data stored and written to file
    test_context.update(
        regions=regions,
        years=years,
        report=Config("global.yaml", key=key, output_dir=tmp_path),
    )
    test_context.report.register(callback)

    # Built and (optionally) solved scenario. dummy supply data is necessary for the
    # scenario to be feasible without any other contents.
    scenario = built_transport(
        request, test_context, options=dict(dummy_supply=True), solved=True
    )

    # commented: for debugging
    # dump_path = tmp_path / "scenario.xlsx"
    # log.info(f"Dump contents to {dump_path}")
    # scenario.to_excel(dump_path)

    rep, key = prepare_reporter(test_context, scenario)

    # Reporting `key` succeeds
    rep.get(key)


@build.get_computer.minimum_version
@MARK[10]
@MARK[7]
@mark.usefixtures("quiet_genno")
@mark.parametrize(
    "build",
    (
        True,  # Run .transport.build.main()
        False,  # Use data from an Excel export
    ),
)
def test_simulated(
    request, test_context: "Context", build: bool, regions="R12", years="B"
) -> None:
    """:func:`message_ix_models.report.prepare_reporter` works on the simulated data."""
    test_context.update(regions=regions, years=years)
    rep = simulated_solution(request, test_context, build)

    # A quantity for a MESSAGEix variable was added and can be retrieved
    k = rep.full_key("ACT")
    rep.get(k)

    # A quantity for MESSAGEix can be computed
    k = rep.full_key("out")
    rep.get(k)

    # A quantity for message_ix_models.model.transport can be computed
    k = "transport stock::iamc"
    result = rep.get(k)
    assert 0 < len(result)

    # SDMX data for message_ix_models.project.edits can be computed
    result = rep.get(key.report.sdmx)

    # The task returns the directory in which output is written
    p = result
    # Expected files are generated
    assert p.joinpath("structure.xml").exists()
    assert p.joinpath("DF_POPULATION_IN.csv").exists()
    assert p.joinpath("DF_POPULATION_IN.xml").exists()


@pytest.mark.skipif(
    GHA and (V("3.8") < V(version("ixmp")) < V("3.11")),
    reason="Fails on GHA with ixmp/message_ix v3.9 and v3.10 or their dependencies",
)
@build.get_computer.minimum_version
@MARK[10]
def test_simulated_iamc(
    request, tmp_path_factory, test_context, regions="R12", years="B"
) -> None:
    test_context.update(regions=regions, years=years)
    test_context.report.output_dir = test_context.get_local_path()

    rep = simulated_solution(request, test_context, build=True)

    # Key collecting both file output/scenario update
    # NB the trailing colons are necessary because of how genno handles report.yaml
    rep.add(
        "test",
        [
            "transport::iamc+file",
            "transport::iamc+store",
            # DEBUG Other keys:
            # "emi:nl-t-yv-ya-m-e-h:transport",
        ],
    )

    # print(rep.describe("test"))  # DEBUG
    result = rep.get("test")
    # print(result[-1])  # DEBUG

    s: "message_ix.Scenario" = rep.get("scenario")

    # File with output was created
    path = tmp_path_factory.getbasetemp().joinpath(
        "data0", ScenarioInfo(s).path, "transport.csv"
    )
    assert path.exists(), path

    # Retrieve time series data stored on the scenario object
    ts = s.timeseries()
    # print(ts, ts["variable"].unique(), sep="\n")  # DEBUG

    # The reported data was stored on the scenario, and has expected variable names
    # print("\n".join(sorted(ts["variable"].unique())))  # DEBUG
    assert {
        "Energy Service|Transportation|Domestic Aviation",
        "Final Energy|Transportation|Bus",
        "Transport|Stock|Road|Passenger|LDV|BEV",
    } <= set(ts["variable"].unique())

    del result


@build.get_computer.minimum_version
@MARK[10]
@mark.usefixtures("quiet_genno")
@pytest.mark.parametrize(
    "plot_name",
    # # All plots
    # list(PLOTS.keys()),
    # Only a subset
    [
        # "energy-by-cmdty",
        "stock-ldv",
        # "stock-non-ldv",
    ],
)
def test_simulated_plot(request, test_context, plot_name, regions="R12", years="B"):
    """Plots are generated correctly using simulated data."""
    test_context.update(regions=regions, years=years)
    log.debug(f"test_plot_simulated: {test_context.regions = }")
    rep = simulated_solution(request, test_context, build=True)

    # print(rep.describe(f"plot {plot_name}"))  # DEBUG

    # Succeeds
    rep.get(f"plot {plot_name}")
