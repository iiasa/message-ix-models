import logging
import os
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import click.testing
import message_ix
import pandas as pd
import pytest
from ixmp import Platform
from ixmp import config as ixmp_config

from message_ix_models import cli, util
from message_ix_models.util._logging import mark_time, preserve_log_level
from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

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


def pytest_sessionstart():
    # Quiet logs for some upstream packages
    for name in ("pycountry.db", "matplotlib.backends", "matplotlib.font_manager"):
        logging.getLogger(name).setLevel(logging.DEBUG + 1)


# Fixtures


@pytest.fixture(scope="session")
def session_context(pytestconfig, tmp_env):
    """A :class:`.Context` connected to a temporary, in-memory database.

    This Context is suitable for modifying and running test code that does not affect
    the user/developer's filesystem and configured :mod:`ixmp` databases.

    Uses the :func:`.tmp_env` fixture from ixmp. This fixture also sets:

    - :attr:`.Context.cache_path`, depending on whether the :program:`--local-cache` CLI
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

    # Set the cache path according to whether pytest --local-cache was given. If True,
    # pick up the existing setting from the user environment. If False, use a pytest-
    # managed cache directory that persists across test sessions.
    ctx.cache_path = (
        ctx.local_data.joinpath("cache")
        if pytestconfig.option.local_cache
        # TODO use pytestconfig.cache.mkdir() when pytest >= 6.3 is available
        else Path(pytestconfig.cache.makedir("cache"))
    )

    # Other local data in the temporary directory for this session only
    ctx.local_data = session_tmp_dir

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

    # Launch Platform and connect to testdb (reconnect if closed)
    mp = Platform(name=platform_name)
    mp.open_db()

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
    ctx = deepcopy(session_context)

    # Ensure there is a report key
    ctx.setdefault("report", dict())

    yield ctx

    ctx.delete()


@pytest.fixture(scope="function")
def user_context(request):  # pragma: no cover
    """Context which can access user's configuration, e.g. platform names."""
    # Disabled; this is bad practice
    raise NotImplementedError


class CliRunner(click.testing.CliRunner):
    """Subclass of :class:`click.testing.CliRunner` with extra features."""

    # NB decorator ensures any changes that the CLI makes to the logger level are
    #    restored
    @preserve_log_level()
    def invoke(self, *args, **kwargs):
        """Invoke the :program:`mix-models` CLI."""
        result = super().invoke(cli.main, *args, **kwargs)

        # Store the result to be used by assert_exit_0()
        self.last_result = result

        return result

    def assert_exit_0(self, *args, **kwargs):
        """Assert a result has exit_code 0, or print its traceback.

        If any `args` or `kwargs` are given, :meth:`.invoke` is first called. Otherwise,
        the result from the last call of :meth:`.invoke` is used.

        Raises
        ------
        AssertionError
            if the result exit code is not 0. The exception contains the traceback from
            within the CLI.

        Returns
        -------
        click.testing.Result
        """
        __tracebackhide__ = True

        if len(args) + len(kwargs):
            self.invoke(*args, **kwargs)

        if self.last_result.exit_code != 0:
            # Re-raise the exception triggered within the CLI invocation
            raise (
                self.last_result.exc_info[1].__context__ or self.last_result.exc_info[1]
            )

        return self.last_result


@pytest.fixture(scope="session")
def mix_models_cli(request, session_context, tmp_env):
    """A :class:`.CliRunner` object that invokes the :program:`mix-models` CLI."""
    # Require the `session_context` fixture in order to set Context.local_data
    yield CliRunner(env=tmp_env)


@cli.main.group("_test", hidden=True)
def cli_test_group():
    """Hidden group of CLI commands.

    Other code which needs to test CLI behaviour **may** attach temporary/throw-away
    commands to this group and then invoke them using :func:`mix_models_cli`. This
    avoids the need to expose additional commands for testing purposes only.
    """


# Testing utility functions


def bare_res(request, context: Context, solved: bool = False) -> message_ix.Scenario:
    """Return or create a |Scenario| containing the bare RES, for use in testing.

    The Scenario has a model name like "MESSAGEix-GLOBIOM [regions]
    [start]:[duration]:[end]", e.g. "MESSAGEix-GLOBIOM R14 2020:10:2110" (see
    :func:`.bare.name`) and the scenario name "baseline".

    This function should:

    - only be called from within test code, i.e. in :mod:`message_data.tests`.
    - be called once for each test function, so that each test receives a fresh copy of
      the RES scenario.

    Parameters
    ----------
    request : .Request or None
        The pytest :fixture:`pytest:request` fixture. If provided the pytest test node
        name is used for the scenario name of the returned Scenario.
    context : Context
        Passed to :func:`.testing.bare_res`.
    solved : bool, *optional*
        Return a solved Scenario.

    Returns
    -------
    Scenario
        The scenario is a fresh clone, so can be modified freely without disturbing
        other tests.
    """
    from message_ix_models.model import bare

    name = bare.name(context)
    mp = context.get_platform()

    try:
        base = message_ix.Scenario(mp, name, "baseline")
    except ValueError:
        log.info(f"Create '{name}/baseline' for testing")
        context.scenario_info.update(model=name, scenario="baseline")
        base = bare.create_res(context)

    if solved and not base.has_solution():
        log.info("Solve")
        base.solve(solve_options=dict(lpmethod=4), quiet=True)

    try:
        new_name = request.node.name
    except AttributeError:
        new_name = "baseline"

    log.info(f"Clone to '{name}/{new_name}'")
    return base.clone(scenario=new_name, keep_solution=solved)


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
        filters={
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

    for name in reader.sheet_names:
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


#: Shorthand for marking a parametrized test case that is expected to fail because it is
#: not implemented.
NIE = pytest.mark.xfail(raises=NotImplementedError)

#: :data:`True` if tests occur on GitHub Actions.
GHA = "GITHUB_ACTIONS" in os.environ


def not_ci(reason=None, action="skip"):
    """Mark a test as xfail or skipif if on CI infrastructure.

    Checks the ``GITHUB_ACTIONS`` environment variable; returns a pytest mark.
    """
    action = "skipif" if action == "skip" else action
    return getattr(pytest.mark, action)(condition=GHA, reason=reason)
