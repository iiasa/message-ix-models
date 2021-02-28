from copy import deepcopy
from pathlib import Path

import pytest
from ixmp import Platform
from ixmp import config as ixmp_config

from message_ix_models.util.context import Context

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

    platform_name = "message_data"

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
