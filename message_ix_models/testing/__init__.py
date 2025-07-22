import logging
import os
import shutil

try:
    from base64 import b32hexencode as b32encode
except ImportError:
    from base64 import b32encode
import platform
from collections.abc import Generator, Hashable
from copy import deepcopy
from pathlib import Path
from random import randbytes
from tempfile import TemporaryDirectory

import message_ix
import pandas as pd
import pytest
import sdmx.exceptions
from ixmp import config as ixmp_config

from message_ix_models import util
from message_ix_models.model import snapshot
from message_ix_models.util._logging import mark_time
from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

#: Items with names that match (partially or fully) these names are omitted by
#: :func:`export_test_data`.
EXPORT_OMIT = [
    "aeei",
    "cost_MESSAGE",
    "demand_MESSAGE",
    "demand",
    "depr",
    "esub",
    "gdp_calibrate",
    "grow",
    "historical_gdp",
    "kgdp",
    "kpvs",
    "lakl",
    "land",
    "lotol",
    "mapping_macro_sector",
    "MERtoPPP",
    "prfconst",
    "price_MESSAGE",
    "ref_",
    "sector",
]

#: :data:`True` if tests occur on GitHub Actions.
GHA = "GITHUB_ACTIONS" in os.environ

#: Common marks for tests. Use either short, informative :class:`str` keys or
#: :class:`int`; in the latter case, do not reuse keys lower than the highest key in the
#: collection.
MARK: dict[Hashable, pytest.MarkDecorator] = {
    0: pytest.mark.xfail(
        condition=GHA,
        reason="GitHub-hosted runner has no access to IIASA-internal databases",
    ),
    1: pytest.mark.xfail(
        condition=GHA
        and platform.system() == "Darwin"
        and not bool(shutil.which("dot")),
        reason="Graphviz not available for GitHub Actions jobs on some macOS runners",
    ),
    2: pytest.mark.xfail(
        condition=not util.HAS_MESSAGE_DATA,
        reason="Not yet migrated from message_data",
    ),
    "#375": pytest.mark.flaky(
        reruns=3,
        rerun_delay=2,
        condition=GHA,
        reason="https://github.com/iiasa/message-ix-models/issues/375",
    ),
    "sdmx#230": pytest.mark.xfail(
        condition=GHA,
        reason="https://github.com/khaeru/sdmx/issues/230",
        raises=sdmx.exceptions.XMLParseError,
    ),
}

#: Shorthand for marking a parametrized test case that is expected to fail because it is
#: not implemented.
NIE = pytest.mark.xfail(raises=NotImplementedError)

#: Keys for :py:`pytestconfig.stash`.
KEY = {
    "cache-path": pytest.StashKey[Path](),
    "user-local-data": pytest.StashKey[Path](),
}


SOLVE_OPTIONS = dict(solve_options=dict(iis=1, lpmethod=4), quiet=True)

# pytest hooks


def pytest_addoption(parser):
    """Add two command-line options to pytest:

    ``--local-cache``
       Use existing, local cache files in tests. This option can speed up tests that
       *use* the results of slow data loading/parsing. However, if cached values are not
       up to date with the current code, unexpected failure may occur.

    ``--jvmargs``
       Additional arguments to give for the Java Virtual Machine used by :mod:`ixmp`'s
       :class:`.JDBCBackend`. Used by :func:`session_context`.
    """
    parser.addoption(
        "--local-cache",
        action="store_true",
        help="Use existing local cache files in tests",
    )
    parser.addoption(
        "--jvmargs",
        action="store",
        default="",
        help="Arguments for Java VM used by ixmp JDBCBackend",
    )


def pytest_sessionstart(session: pytest.Session) -> None:
    """Identify the path to be used for cached local data.

    If :program:`pytest --local-cache` was given, pick up the existing setting from the
    user's environment. Otherwise, use a pytest-managed cache directory that persists
    across test sessions.
    """
    config = Context.only().core

    session.config.stash[KEY["cache-path"]] = (
        config.cache_path
        if session.config.option.local_cache
        else Path(session.config.cache.mkdir("cache"))
    )

    # Store current .Config.local_data value from the user's configuration
    session.config.stash[KEY["user-local-data"]] = config.local_data


def pytest_report_header(config, start_path) -> str:
    """Add the ixmp configuration to the pytest report header."""
    return f"message-ix-models cache path: {config.stash[KEY['cache-path']]}"


# Fixtures


@pytest.fixture(scope="session")
def session_context(pytestconfig, tmp_env):
    """A :class:`.Context` connected to a temporary, in-memory database.

    This Context is suitable for modifying and running test code that does not affect
    the user/developer's filesystem and configured :mod:`ixmp` databases.

    Uses the :func:`.tmp_env` fixture from ixmp. This fixture also sets:

    - :attr:`.Config.cache_path`, depending on whether the :program:`--local-cache` CLI
      option was given:

      - If not given: pytest's :doc:`standard cache directory <pytest:how-to/cache>`.
      - If given: the :file:`/cache/` directory under the user's "message local data"
        directory.

    - the "message local data" config key to a temporary directory :file:`/data/` under
      the :ref:`pytest tmp_path directory <pytest:tmp_path>`.

    """
    ctx = Context.only()

    # Temporary, empty local directory for local data
    session_tmp_dir = Path(pytestconfig._tmp_path_factory.mktemp("data"))

    # Apply the cache path determined in pytest_sessionstart(), above
    ctx.core.cache_path = pytestconfig.stash[KEY["cache-path"]]

    # Other local data in the temporary directory for this session only
    ctx.core.local_data = session_tmp_dir

    # Also set the "message local data" key in the ixmp config
    ixmp_config.set("message local data", session_tmp_dir)

    # If message_data is not installed, use a temporary path for private_data_path()
    message_data_path = util.MESSAGE_DATA_PATH
    if util.MESSAGE_DATA_PATH is None:
        util.MESSAGE_DATA_PATH = session_tmp_dir.joinpath("message_data")

        # Create some subdirectories
        util.MESSAGE_DATA_PATH.joinpath("data", "tests").mkdir(parents=True)

    # Add a platform connected to an in-memory database
    platform_name = "message-ix-models"
    ixmp_config.add_platform(
        platform_name,
        "jdbc",
        "hsqldb",
        url=f"jdbc:hsqldb:mem://{platform_name}",
        jvmargs=pytestconfig.option.jvmargs,
    )
    ixmp_config.save()

    ctx.platform_info["name"] = platform_name

    try:
        yield ctx
    finally:
        ctx.close_db()
        ixmp_config.remove_platform(platform_name)

        # Restore prior value
        util.MESSAGE_DATA_PATH = message_data_path


@pytest.fixture(scope="function")
def test_context(request, session_context):
    """A copy of :func:`session_context` scoped to one test function."""
    try:
        ctx = deepcopy(session_context)
    except Exception:
        print(repr(session_context._values))
        raise

    yield ctx

    ctx.delete()


@pytest.fixture(scope="function")
def user_context(request):  # pragma: no cover
    """Context which can access user's configuration, e.g. platform names."""
    # Disabled; this is bad practice
    raise NotImplementedError


@pytest.fixture
def mix_models_cli(session_context, tmp_env):
    """A :class:`.CliRunner` object that invokes the :program:`mix-models` CLI.

    NB this requires:

    - The :mod:`ixmp` :func:`.tmp_env` fixture. This sets ``IXMP_DATA`` to a temporary
      directory managed by :mod:`pytest`.
    - The :func:`session_context` fixture. This (a) sets :attr:`.Config.local_data` to
      a temporary directory within ``IXMP_DATA`` and (b) ensures changes to
      :class:`.Context` made by invoked commands do not reach other tests.
    """
    from message_ix_models import cli
    from message_ix_models.util.click import CliRunner

    yield CliRunner(cli.main, cli.__name__, env=tmp_env)


@pytest.fixture
def advance_test_data(monkeypatch) -> None:
    """Temporarily allow :func:`path_fallback` to find test data."""
    from message_ix_models.project.advance.data import ADVANCE

    monkeypatch.setattr(ADVANCE, "use_test_data", True)


@pytest.fixture
def iea_eei_user_data(pytestconfig, monkeypatch) -> None:
    """Temporarily allow :class:`.IEA_EEI` to find user data."""
    from message_ix_models.tools.iea.eei import IEA_EEI

    monkeypatch.setattr(
        IEA_EEI, "where", IEA_EEI.where + [pytestconfig.stash[KEY["user-local-data"]]]
    )


@pytest.fixture
def iea_eweb_test_data(monkeypatch, session_context) -> None:
    """Temporarily allow :func:`path_fallback` to find test data.

    `session_context` is required in order to set the cache path in the test
    environment.
    """
    from message_ix_models.tools.iea.web import IEA_EWEB

    monkeypatch.setattr(IEA_EWEB, "use_test_data", True)


@pytest.fixture
def iea_eweb_user_data(pytestconfig, monkeypatch) -> None:  # pragma: no cover
    """Temporarily allow :class:`.IEA_EWEB` to find user data."""
    from message_ix_models.tools.iea.web import IEA_EWEB

    monkeypatch.setattr(
        IEA_EWEB,
        "where",
        IEA_EWEB.where + [pytestconfig.stash[KEY["user-local-data"]]],
    )


@pytest.fixture
def ssp_test_data(monkeypatch) -> None:
    """Temporarily allow :func:`path_fallback` to find test data."""
    from message_ix_models.project.ssp.data import SSPOriginal, SSPUpdate

    for cls in SSPOriginal, SSPUpdate:
        monkeypatch.setattr(cls, "use_test_data", True)


@pytest.fixture
def ssp_user_data(pytestconfig, monkeypatch) -> None:
    """Temporarily allow :class:`.SSPOriginal`/:class:`.SSPUpdate` to find user data."""
    from message_ix_models.project.ssp.data import SSPOriginal, SSPUpdate

    for cls in SSPOriginal, SSPUpdate:
        monkeypatch.setattr(
            cls, "where", cls.where + [pytestconfig.stash[KEY["user-local-data"]]]
        )


# Testing utility functions


def bare_res(
    request: "pytest.FixtureRequest", context: "Context", solved: bool = False
) -> message_ix.Scenario:
    """Return or create a :class:`.Scenario` containing the bare RES for use in testing.

    The Scenario has a model name like "MESSAGEix-GLOBIOM [regions] Y[years]", for
    instance "MESSAGEix-GLOBIOM R14 YB" (see :func:`.bare.name`) and a scenario name
    either from :py:`request.node.name` or "baseline" plus a random string of 5
    characters.

    This function should:

    - only be called from within test code, i.e. in :mod:`message_data.tests`.
    - be called once for each test function, so that each test receives a fresh copy of
      the RES scenario.

    Parameters
    ----------
    request : .FixtureRequest or None
        The pytest :fixture:`pytest:request` fixture. If provided the pytest test node
        name is used for the scenario name of the returned Scenario.
    context : .Context
        Passed to :func:`.testing.bare_res`.
    solved : bool, optional
        Return a solved Scenario.

    Returns
    -------
    Scenario
        The scenario is a fresh clone, so can be modified freely without disturbing
        other tests.
    """
    from message_ix_models.model import bare

    # Model name: standard "MESSAGEix-GLOBIOM R12 YB" plus a suffix
    model_name = bare.name(context, unique=True)

    try:
        base = message_ix.Scenario(context.get_platform(), model_name, "baseline")
    except ValueError:
        log.info(f"Create '{model_name}/baseline' for testing")
        context.scenario_info.update(model=model_name, scenario="baseline")
        base = bare.create_res(context)

    log.info(f"Clone to '{model_name}/{request.node.name}'")
    scenario = base.clone(scenario=request.node.name, keep_solution=solved)

    if solved and not scenario.has_solution():
        log.info(f"Solve {scenario.url}")
        scenario.solve(**SOLVE_OPTIONS)

    return scenario


def export_test_data(context: Context):
    """Export a subset of data from a scenario, for use in tests.

    The context settings ``export_nodes`` (default: "R11_AFR" and "R11_CPA") and
    ``export_techs`` (default: "coal_ppl") are used to filter the data exported.
    In addition, any item (set, parameter, variable, or equation) with a name matching
    :data:`EXPORT_OMIT` *or* the context setting ``export_exclude`` is discarded.

    The output is stored at :file:`data/tests/{model name}_{scenario name}_{techs}.xlsx`
    in :mod:`message_data`.

    See also
    --------
    :ref:`export-test-data`
    """
    from message_ix_models.util import private_data_path

    # Load the scenario to be exported
    scen = context.get_scenario()

    # Retrieve the context settings giving the nodes and technologies to export
    nodes = context.get("export_nodes", ["R11_AFR", "R11_CPA"])
    technology = context.get("export_techs", ["coal_ppl"])

    # Construct the destination file name
    dest_file = private_data_path(
        "tests", f"{scen.model}_{scen.scenario}_{'_'.join(technology)}.xlsx"
    )
    # Temporary file name
    td = TemporaryDirectory()
    tmp_file = Path(td.name).joinpath("export_test_data.xlsx")

    # Ensure the target directory exists
    dest_file.parent.mkdir(exist_ok=True)

    # Dump data to temporary Excel file
    log.info(f"Export test data to {dest_file}")
    scen.to_excel(
        tmp_file,
        filters={  # type: ignore [arg-type]
            "technology": technology,
            "node": nodes,
            "node_dest": nodes,
            "node_loc": nodes,
            "node_origin": nodes,
            "node_parent": nodes,
            "node_rel": nodes,
            "node_share": nodes,
        },
    )

    mark_time()

    log.info("Reduce test data")

    # Read from temporary file and write to final file, omitting unnecessary sheets
    reader = pd.ExcelFile(tmp_file)
    writer = pd.ExcelWriter(dest_file)

    # Retrieve the type mapping first, to be modified as sheets are discarded
    ix_type_mapping = reader.parse("ix_type_mapping").set_index("item")

    for name in map(str, reader.sheet_names):
        # Check if this sheet is to be included
        if name == "ix_type_mapping":
            # Already handled
            continue
        elif any(i in name for i in (EXPORT_OMIT + context.get("export_exclude", []))):
            log.info(f"Discard sheet '{name}'")

            # Remove from the mapping
            ix_type_mapping.drop(name, inplace=True)

            continue

        # Copy the sheet from temporary to final file
        reader.parse(name).to_excel(writer, sheet_name=name, index=False)

    # Close the temporary file
    reader.close()

    # Write the mapping
    ix_type_mapping.reset_index().to_excel(
        writer, sheet_name="ix_type_mapping", index=False
    )

    # Close the final file
    writer.close()

    mark_time()


def not_ci(reason=None, action="skip"):
    """Mark a test as xfail or skipif if on CI infrastructure.

    Checks the ``GITHUB_ACTIONS`` environment variable; returns a pytest mark.
    """
    action = "skipif" if action == "skip" else action
    return getattr(pytest.mark, action)(condition=GHA, reason=reason)


def unpack_snapshot_data(context: Context, snapshot_id: int):
    """Already-unpacked data for a snapshot.

    This copies the .csv.gz files from message_ix_models/data/test/… to the directory
    where they *would* be unpacked by .model.snapshot._unpack. This causes the code to
    skip unpacking them, which can be very slow.
    """
    if snapshot_id not in (0, 1):
        log.info(f"No unpacked data for snapshot {snapshot_id}")
        return

    parts = (f"snapshot-{snapshot_id}", "MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline")
    dest = context.get_cache_path(*parts)
    log.debug(f"{dest = }")

    snapshot_data_path = util.package_data_path("test", *parts)
    log.debug(f"{snapshot_data_path = }")

    shutil.copytree(snapshot_data_path, dest, dirs_exist_ok=True)


@pytest.fixture(
    scope="session",
    params=[
        int(k.split("-")[1]) for k in util.pooch.SOURCE if k.startswith("snapshot")
    ],
)
def loaded_snapshot(
    request, session_context, solved: bool = False
) -> Generator[message_ix.Scenario, None, None]:
    snapshot_id: int = request.param
    assert snapshot_id is not None
    unpack_snapshot_data(context=session_context, snapshot_id=snapshot_id)
    model_name = "MESSAGEix-GLOBIOM_1.1_R11_no-policy"
    scenario_name = f"baseline_v{snapshot_id}"
    mp = session_context.get_platform()

    # The following code roughly parallels bare_res()
    try:
        base = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
    except ValueError:
        log.info(f"Create '{model_name}/{scenario_name}' for testing")
        session_context.scenario_info.update(model=model_name, scenario=scenario_name)
        base = message_ix.Scenario(
            mp, model=model_name, scenario=scenario_name, version="new"
        )
        snapshot.load(
            scenario=base,
            snapshot_id=snapshot_id,
            extra_cache_path=f"snapshot-{snapshot_id}",
        )

    if solved and not base.has_solution():
        log.info("Solve")
        base.solve(solve_options=dict(lpmethod=4), quiet=True)

    try:
        new_name = request.node.name
    except AttributeError:
        # Generate a new scenario name with a random part
        new_name = f"baseline {b32encode(randbytes(3)).decode().rstrip('=').lower()}"

    log.info(f"Clone to '{model_name}/{new_name}'")
    yield base.clone(scenario=new_name, keep_solution=solved)
