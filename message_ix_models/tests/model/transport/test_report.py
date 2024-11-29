import logging
from copy import deepcopy
from typing import TYPE_CHECKING

import genno
import pytest
from pytest import mark, param

from message_ix_models import ScenarioInfo
from message_ix_models.model.transport import build
from message_ix_models.model.transport.report import configure_legacy_reporting
from message_ix_models.model.transport.testing import (
    MARK,
    built_transport,
    simulated_solution,
)
from message_ix_models.report import prepare_reporter, sim
from message_ix_models.testing import GHA

if TYPE_CHECKING:
    import message_ix

log = logging.getLogger(__name__)


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


@MARK[7]
@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions, years",
    (
        param("R11", "A", marks=MARK[2](ValueError)),
        ("R12", "A"),
        param("R14", "A", marks=MARK[2](genno.ComputationError)),
        param("ISR", "A", marks=MARK[3]),
    ),
)
def test_report_bare_solved(request, test_context, tmp_path, regions, years):
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


@pytest.fixture
def quiet_genno(caplog):
    """Quiet some log messages from genno via by :func:`.reporting.prepare_reporter`."""
    caplog.set_level(logging.WARNING, logger="genno.config")
    caplog.set_level(logging.WARNING, logger="genno.compat.pyam")


@MARK[7]
@build.get_computer.minimum_version
@mark.usefixtures("quiet_genno")
def test_simulated_solution(request, test_context, regions="R12", years="B"):
    """:func:`message_ix_models.report.prepare_reporter` works on the simulated data."""
    test_context.update(regions=regions, years=years)
    rep = simulated_solution(request, test_context)

    # A quantity for a MESSAGEix variable was added and can be retrieved
    k = rep.full_key("ACT")
    rep.get(k)

    # A quantity for MESSAGEix can be computed
    k = rep.full_key("out")
    rep.get(k)

    # A quantity for message_data.model.transport can be computed
    k = "transport stock::iamc"
    result = rep.get(k)
    assert 0 < len(result)


@build.get_computer.minimum_version
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
def test_plot_simulated(request, test_context, plot_name, regions="R12", years="B"):
    """Plots are generated correctly using simulated data."""
    test_context.update(regions=regions, years=years)
    rep = simulated_solution(request, test_context)

    # print(rep.describe(f"plot {plot_name}"))  # DEBUG

    # Succeeds
    rep.get(f"plot {plot_name}")


@pytest.mark.xfail(condition=GHA, reason="Temporary, for #213; fails on GitHub Actions")
@sim.to_simulate.minimum_version
def test_iamc_simulated(
    request, tmp_path_factory, test_context, regions="R12", years="B"
) -> None:
    test_context.update(regions=regions, years=years)
    test_context.report.output_dir = test_context.get_local_path()

    rep = simulated_solution(request, test_context)

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
