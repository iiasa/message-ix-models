import logging
from copy import deepcopy
from pathlib import Path

import click.testing
import message_ix
import pytest
from ixmp import Platform
from ixmp import config as ixmp_config

from message_ix_models import cli
from message_ix_models.util._logging import preserve_log_level
from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

# pytest hooks


def pytest_addoption(parser):
    """Add the ``--local-cache`` command-line option to pytest."""
    parser.addoption(
        "--local-cache",
        action="store_true",
        help="Use existing local cache files in tests",
    )


# Fixtures


@pytest.fixture(scope="session")
def session_context(request, tmp_env):
    """A Context connected to a temporary, in-memory database.

    Uses the :func:`.tmp_env` fixture from ixmp.
    """
    ctx = Context.only()

    # Temporary, empty local directory for local data
    session_tmp_dir = Path(request.config._tmp_path_factory.mktemp("data"))

    # Set the cache path according to whether pytest --local-cache was given. If True,
    # pick up the existing setting from the user environment.
    ctx.cache_path = (
        ctx.local_data if request.config.option.local_cache else session_tmp_dir
    ).joinpath("cache")

    # Other local data in the temporary directory
    ctx.local_data = session_tmp_dir

    platform_name = "message-ix-models"

    # Add a platform connected to an in-memory database
    # NB cannot call Config.add_platform() here because it does not support supplying a
    #    URL for a HyperSQL database.
    # TODO add that feature upstream.
    ixmp_config.values["platform"][platform_name] = {
        "class": "jdbc",
        "driver": "hsqldb",
        "url": f"jdbc:hsqldb:mem://{platform_name}",
    }

    # Launch Platform and connect to testdb (reconnect if closed)
    mp = Platform(name=platform_name)
    mp.open_db()

    ctx.platform_info["name"] = platform_name

    yield ctx

    ctx.close_db()
    ixmp_config.remove_platform(platform_name)


@pytest.fixture(scope="function")
def test_context(request, session_context):
    """A copy of :func:`session_context` scoped to one test function."""
    ctx = deepcopy(session_context)

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
            raise self.last_result.exc_info[1].__context__

        return self.last_result


@pytest.fixture(scope="session")
def mix_models_cli(request, session_context, tmp_env):
    """A :class:`.CliRunner` object that invokes the :program:`mix-models` CLI."""
    # Require the `session_context` fixture in order to set Context.local_data
    yield CliRunner(env=tmp_env)


# Testing utility functions


def bare_res(request, context: Context, solved: bool = False) -> message_ix.Scenario:
    """Return or create a Scenario containing the bare RES, for use in testing.

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
    context : .Context
        Passed to :func:`.testing.bare_res`.
    solved : bool, optional
        Return a solved Scenario.

    Returns
    -------
    .Scenario
        The scenario is a fresh clone, so can be modified freely without disturbing
        other tests.
    """
    from message_ix_models.model import bare

    context.use_defaults(bare.SETTINGS)

    name = bare.name(context)
    mp = context.get_platform()

    try:
        base = message_ix.Scenario(mp, name, "baseline")
    except ValueError:
        log.info(f"Create '{name}/baseline' for testing")
        context.scenario_info.update(dict(model=name, scenario="baseline"))
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
