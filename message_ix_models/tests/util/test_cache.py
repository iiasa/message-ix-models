import logging
from copy import deepcopy

import pytest
import sdmx.model.v21 as sdmx_model
import xarray as xr
from genno.caching import hash_args
from ixmp.testing import assert_logs

from message_ix_models import ScenarioInfo
from message_ix_models.util import cache, cached

log = logging.getLogger(__name__)


class TestEncoder:
    def test_sdmx(self):
        """:mod:`message_ix_models` configures :class:`.Encoder` for :class:`.Code`."""
        codes0 = [sdmx_model.Code(id=f"FOO{i}", name="foo") for i in range(5)]
        codes1 = [f"FOO{i}" for i in range(5)]

        # List of codes hashes the same as a list of their string IDs
        expected = "40a0735385448dcbe745904ebfec7255995ca451"
        assert expected == hash_args(codes0, bar="baz") == hash_args(codes1, bar="baz")


def test_cache_skip(test_context) -> None:
    """:attr:`.Config.cache_skip` updates :data:`.cache.COMPUTER`."""
    pre = cache.COMPUTER.graph["config"].get("cache_skip")

    try:
        test_context.core.cache_skip = True

        assert cache.COMPUTER.graph["config"]["cache_skip"] is True

        test_context.core.cache_skip = False

        assert cache.COMPUTER.graph["config"]["cache_skip"] is False
    finally:
        if pre is None:
            cache.COMPUTER.graph["config"].pop("cache_skip")
        else:
            cache.COMPUTER.graph["config"]["cache_skip"] = pre


def test_cached(caplog, test_context, tmp_path) -> None:
    """:func:`.cached` works as expected."""
    # Store in the temporary directory for this session, to avoid collisions across
    # sessions
    test_context.cache_path = tmp_path.joinpath("cache")

    # A dummy path to be hashed as an argument
    path_foo = tmp_path.joinpath("foo", "bar")

    with caplog.at_level(logging.DEBUG, logger="message_ix_models"):

        @cached
        def func0(ctx, a, path, b=3):
            """A test function."""
            log.info("func0 runs")
            return f"{id(ctx)}, {a + b}"

    # Docstring is modified
    assert "Data returned by this function is cached" in (func0.__doc__ or "")

    @cached
    def func1(x=1, y=2, **kwargs):
        # Function with defaults for all arguments
        log.info("func1 runs")
        return x + y

    caplog.clear()

    # pathlib.Path argument is serialized to JSON as part of the argument hash;
    # function runs, messages logged
    with assert_logs(caplog, "func0 runs"):
        result0 = func0(test_context, 1, path_foo)

    caplog.clear()
    result1 = func0(test_context, 1, path_foo)
    # Function does not run
    assert "func0 runs" not in caplog.messages
    assert caplog.messages[0].startswith("Cache hit for func0")
    # Results identical
    assert result0 == result1

    # Different context object with identical contents hashes equal
    ctx2 = deepcopy(test_context)
    assert id(test_context) != id(ctx2)

    result2 = func0(ctx2, 1, path_foo)
    # Function does not run
    assert "func0 runs" not in caplog.messages
    # Results are identical, i.e. including the old ID
    assert result0 == result2

    ctx2.delete()
    caplog.clear()

    # Hash of no arguments is the same, function only runs once
    assert 3 == func1() == func1()
    assert 1 == sum(m == "func1 runs" for m in caplog.messages)

    # Warnings logged for unhashables; ScenarioInfo is hashed as dict
    caplog.clear()
    with assert_logs(
        caplog,
        [
            "ignores <class 'xarray.core.dataset.Dataset'>",
            "ignores <class 'ixmp.core.platform.Platform'>",
        ],
    ):
        func1(ds=xr.Dataset(), mp=test_context.get_platform(), si=ScenarioInfo())

    # Unserializable type raises an exception
    with pytest.raises(
        TypeError, match="Object of type slice is not JSON serializable"
    ):
        func1(arg=slice(None))
