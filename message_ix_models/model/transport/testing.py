"""Utilities for testing :mod:`~message_ix_models.model.transport`."""

import logging
import platform
from collections.abc import Mapping
from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import pytest
from genno import Computer
from message_ix import Reporter, Scenario

import message_ix_models.report
from message_ix_models import Context, ScenarioInfo, testing
from message_ix_models.report.sim import add_simulated_solution
from message_ix_models.util import silence_log
from message_ix_models.util.graphviz import HAS_GRAPHVIZ

from . import Config, build

if TYPE_CHECKING:
    import pandas
    import pint

log = logging.getLogger(__name__)

# Common marks for transport code. Do not reuse keys that are less than the highest key
# appearing in the dict.
MARK: dict[int, pytest.MarkDecorator] = {
    0: pytest.mark.xfail(
        reason="Missing R14 input data/assumptions", raises=FileNotFoundError
    ),
    1: pytest.mark.skip(
        reason="Currently only possible with regions=R12 input data/assumptions",
    ),
    2: lambda t: pytest.mark.xfail(
        reason="Missing input data/assumptions for this node codelist", raises=t
    ),
    3: pytest.mark.xfail(raises=ValueError, reason="Missing ISR/mer-to-ppp.csv"),
    4: pytest.mark.xfail(reason="Currently unsupported"),
    # Tests that fail with data that cannot be migrated from message_data
    5: lambda f: pytest.mark.xfail(
        raises=FileNotFoundError, reason=f"Requires non-public data ({f})"
    ),
    7: pytest.mark.xfail(
        condition=testing.GHA and platform.system() == "Darwin" and not HAS_GRAPHVIZ,
        reason="Graphviz missing on macos-13 GitHub Actions runners",
    ),
}


def assert_units(
    df: "pandas.DataFrame", expected: Union[str, dict, "pint.Unit", "pint.Quantity"]
):
    """Assert that `df` has the unique, `expected` units."""
    import pint
    from iam_units import registry

    all_units = df["unit"].unique()
    assert 1 == len(all_units), f"Non-unique {all_units = }"

    # Convert the unique value to the same class as `expected`
    if isinstance(expected, pint.Quantity):
        assert expected == expected.__class__(1.0, all_units[0])
    elif isinstance(expected, Mapping):
        # Compare dimensionality of the units, rather than exact match
        assert expected == registry.Quantity(all_units[0] or "0").dimensionality
    else:
        assert expected == expected.__class__(all_units[0])


def configure_build(
    test_context: Context,
    *,
    regions: str,
    years: str,
    tmp_path: Optional[Path] = None,
    options=None,
) -> tuple[Computer, ScenarioInfo]:
    test_context.update(regions=regions, years=years, output_path=tmp_path)

    # By default, omit plots while testing
    options = options or {}
    options.setdefault("extra_modules", [])
    options["extra_modules"].append("-plot")

    c = build.get_computer(test_context, visualize=False, options=options)

    return c, test_context.transport.base_model_info


def built_transport(
    request,
    context: Context,
    options: Optional[dict] = None,
    solved: bool = False,
    quiet: bool = True,
) -> Scenario:
    """Analogous to :func:`.testing.bare_res`, with transport detail added."""
    options = options or dict()

    # Retrieve (maybe generate) the bare RES with the same settings
    res = testing.bare_res(request, context, solved)

    # Derive the name for the transport scenario
    model_name = res.model.replace("-GLOBIOM", "-Transport")

    try:
        scenario = Scenario(context.get_platform(), model_name, "baseline")
    except ValueError:
        log.info(f"Create '{model_name}/baseline' for testing")

        # Optionally silence logs for code used via build.main()
        log_cm = (
            silence_log("genno message_ix_models.model.transport message_ix_models")
            if quiet
            else nullcontext()
        )

        with log_cm:
            scenario = res.clone(model=model_name)
            build.main(context, scenario, options, fast=True)
    else:
        # Loaded existing Scenario; ensure config files are loaded on `context`
        Config.from_context(context, options=options)

    if solved and not scenario.has_solution():
        log.info(f"Solve '{scenario.model}/{scenario.scenario}'")
        scenario.solve(solve_options=dict(lpmethod=4))

    log.info(f"Clone to '{model_name}/{request.node.name}'")
    return scenario.clone(scenario=request.node.name, keep_solution=solved)


def simulated_solution(request, context) -> Reporter:
    """Return a :class:`.Reporter` with a simulated model solution.

    The contents allow for fast testing of reporting code, without solving an actual
    :class:`.Scenario`.
    """
    from .report import callback

    # Build the base model
    scenario = built_transport(request, context, solved=False)

    # Info about the built model
    info = ScenarioInfo(scenario)

    config: "Config" = context.transport
    technologies = config.spec.add.set["technology"]

    # Create a reporter
    rep = Reporter.from_scenario(scenario)

    # Add simulated solution data
    # TODO expand
    data = dict(
        ACT=dict(
            nl=info.N[-1],
            t=technologies,
            yv=2020,
            ya=2020,
            m="all",
            h="year",
            value=1.0,
        ),
        CAP=dict(
            nl=[info.N[-1]] * 2,
            t=["ELC_100", "ELC_100"],
            yv=[2020, 2020],
            ya=[2020, 2025],
            value=[1.0, 1.1],
        ),
    )
    add_simulated_solution(rep, info, data)

    # Register the callback to set up transport reporting
    context.report.register(callback)

    # Prepare the reporter
    with silence_log("genno", logging.CRITICAL):
        message_ix_models.report.prepare_reporter(context, reporter=rep)

    return rep
