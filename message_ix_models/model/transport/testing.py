"""Utilities for testing :mod:`~message_ix_models.model.transport`."""

import logging
import platform
from collections import ChainMap
from collections.abc import Callable, Hashable, Mapping
from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import pytest
from message_ix import Reporter, Scenario

import message_ix_models.report
from message_ix_models import ScenarioInfo, testing
from message_ix_models.report.sim import add_simulated_solution
from message_ix_models.testing import GHA, SOLVE_OPTIONS, bare_res
from message_ix_models.util import silence_log
from message_ix_models.util.graphviz import HAS_GRAPHVIZ

from . import Config, build

if TYPE_CHECKING:
    import pandas
    import pint
    from genno import Computer

    from message_ix_models import Context

log = logging.getLogger(__name__)

# Common marks for transport code. Do not reuse keys that are less than the highest key
# appearing in the dict.
MARK: Mapping[Hashable, pytest.MarkDecorator] = ChainMap(
    {
        0: pytest.mark.xfail(
            reason="Missing R14 input data/config", raises=FileNotFoundError
        ),
        1: pytest.mark.skip(
            reason="Currently only possible with regions=R12 input data/config",
        ),
        3: pytest.mark.xfail(
            raises=NotImplementedError,
            reason="Missing ISR/mer-to-ppp.csv + not supported by MaybeAdaptR11Source",
        ),
        4: pytest.mark.xfail(reason="Currently unsupported"),
        7: pytest.mark.xfail(
            condition=GHA and platform.system() == "Darwin" and not HAS_GRAPHVIZ,
            reason="Graphviz missing on macos-13 GitHub Actions runners",
        ),
        9: pytest.mark.xfail(reason="Missing R14 input data/config"),
        10: pytest.mark.usefixtures(
            "advance_test_data", "iea_eweb_test_data", "ssp_user_data"
        ),
    },
    testing.MARK,
)

make_mark: dict[Hashable, Callable[..., pytest.MarkDecorator]] = {
    2: lambda t: pytest.mark.xfail(
        reason="Missing input data/assumptions for this node codelist", raises=t
    ),
    5: lambda f: pytest.mark.xfail(
        raises=FileNotFoundError, reason=f"Requires non-public data ({f})"
    ),
    "gh": lambda f: pytest.mark.xfail(
        condition=GHA,
        reason=f"Temporary, for https://github.com/iiasa/message-ix-models/pull/{f}:"
        " fails on GHA, but not locally",
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
        other: Any = expected.__class__(1.0, all_units[0])
    elif isinstance(expected, Mapping):
        # Compare dimensionality of the units, rather than exact match
        other = registry.Quantity(all_units[0] or "0").dimensionality
    else:
        other = expected.__class__(all_units[0])

    assert expected == other, f"{expected!r} != {other!r}"


def configure_build(
    test_context: "Context",
    *,
    regions: str,
    years: str,
    tmp_path: Optional[Path] = None,
    options=None,
) -> tuple["Computer", ScenarioInfo]:
    test_context.update(regions=regions, years=years, output_path=tmp_path)

    # By default, omit plots while testing
    options = options or {}
    options.setdefault("extra_modules", [])
    options["extra_modules"].append("-plot")

    c = build.get_computer(test_context, visualize=False, options=options)

    return c, test_context.transport.base_model_info


def built_transport(
    request: "pytest.FixtureRequest",
    context: "Context",
    options: Optional[dict] = None,
    solved: bool = False,
    quiet: bool = True,
) -> Scenario:
    """Analogous to :func:`.testing.bare_res`, with transport detail added."""
    # Retrieve (generate if necessary) a bare scenario with the same settings
    res = bare_res(request, context, solved)

    # Derive the name for the transport scenario
    model_name = res.model.replace("-GLOBIOM", "-Transport")

    try:
        # Load an existing transport scenario
        scenario = Scenario(res.platform, model_name, "baseline")

        # NB Unclear why, but on GitHub Actions scenarios like
        #    'MESSAGEix-Transport R12 YB 3594a/baseline#1' are sometimes returned with
        #    contents resembling make_dantzig(). Don't allow these to be used.
        assert set(res.set("node")) == set(scenario.set("node"))
    except (AssertionError, ValueError, RuntimeError) as e:
        if isinstance(e, AssertionError):
            log.warning(
                f"Loaded {scenario.url!r} has nodes {sorted(scenario.set('node'))} != "
                f"{sorted(res.set('node'))} → discard"
            )

        log.info(f"Create '{model_name}/baseline' for testing")

        # Optionally silence logs for code used via build.main()
        log_cm = (
            silence_log("genno message_ix_models.model.transport message_ix_models")
            if quiet
            else nullcontext()
        )

        with log_cm:
            scenario = res.clone(model=model_name)
            build.main(context, scenario, options or {})
    else:
        # Loaded existing Scenario; ensure context.transport has a full/matching Config
        Config.from_context(context, options=options or {})

    # Fresh clone with a name matching `request`
    log.info(f"Clone to '{model_name}/{request.node.name}'")
    result = scenario.clone(scenario=request.node.name, keep_solution=solved)

    if solved and not result.has_solution():
        log.info(f"Solve {result.url}")
        result.solve(**SOLVE_OPTIONS)

    # DEBUG Dump the scenario to a temporary path
    # si = ScenarioInfo(scenario)
    # tmp_path = request.getfixturevalue("tmp_path")
    # dump_path = tmp_path.joinpath(f"{si.path}.xlsx")
    # log.info(f"Dump to {dump_path}")
    # result.to_excel(dump_path)

    return result


def simulated_solution(
    request: "pytest.FixtureRequest", context: "Context", build: bool
) -> Reporter:
    """Return a :class:`.Reporter` with a simulated model solution.

    The contents allow for performant testing of reporting code, without solving an
    actual :class:`.Scenario`.

    Parameters
    ----------
    build
        If :any:`False`, do not run :func:`.transport.build.main`; load data for the
        built scenario from a file like
        :file:`message_ix_models/data/test/transport/MESSAGEix-Transport R12 YB a1b2c3_baseline.xlsx`.
    """  # noqa: E501
    from message_ix_models.model import bare
    from message_ix_models.report.sim import reporter_from_excel
    from message_ix_models.util import package_data_path

    from .report import callback

    if build:
        # Use some default options to force built_transport → build.main() →
        # build.get_computer() to generate config
        options = dict(dummy_LDV=False)

        # Build the base model
        scenario = built_transport(request, context, options=options, solved=False)

        # Config object generated by built_transport()
        config: "Config" = context.transport

        # Info about the built model
        info = ScenarioInfo(scenario)

        # Create a reporter
        rep = Reporter.from_scenario(scenario)
    else:
        # Create a Reporter with the contents of a file
        model_name = bare.name(context, unique=True).replace("-GLOBIOM", "-Transport")
        path = package_data_path("test", "transport", f"{model_name}_baseline.xlsx")
        rep = reporter_from_excel(path)

        # Ensure a Config object
        config = Config.from_context(context)

        # Retrieve the ScenarioInfo generated in reporter_from_excel()
        info = rep.graph["scenario info"]

    technologies = config.spec.add.set["technology"]

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
