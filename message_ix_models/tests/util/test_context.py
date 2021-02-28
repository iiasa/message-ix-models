import logging
import re
from copy import deepcopy
from pathlib import Path

import click
import ixmp
import pytest

from message_ix_models import Context
from message_ix_models.util import MESSAGE_DATA_PATH


class TestContext:
    def test_get_instance(self, session_context):
        c = Context()
        assert c is Context.get_instance(-1)

    def test_only(self):
        with pytest.raises(IndexError, match="ambiguous: 2 Context instances"):
            Context.only()

    def test_default_value(self, test_context):
        # Setting is missing
        with pytest.raises(AttributeError):
            test_context.foo

        # setdefault() returns the new value
        assert test_context.setdefault("foo", 23) == 23

        # setdefault() returns the existing value
        assert test_context.setdefault("foo", 45) == 23

        # Attribute access works
        assert test_context.foo == 23

    def test_deepcopy(self, session_context):
        """Paths are preserved through deepcopy()."""
        ld = session_context.local_data

        c = deepcopy(session_context)

        assert ld == c.local_data

    def test_get_cache_path(self, test_context):
        """cache_path() returns the expected output."""
        base = test_context.local_data

        assert base.joinpath(
            "cache", "pytest", "bar.pkl"
        ) == test_context.get_cache_path("pytest", "bar.pkl")

    def test_get_local_path(self, tmp_path_factory, session_context):
        assert str(tmp_path_factory.mktemp("data").joinpath("foo", "bar")).replace(
            "data1", "data0"
        ) == str(session_context.get_local_path("foo", "bar"))

    def test_get_platform(self, session_context):
        assert isinstance(session_context.get_platform(), ixmp.Platform)
        assert isinstance(session_context.get_platform(reload=True), ixmp.Platform)

    def test_get_scenario(self, test_context):
        test_context.scenario_info = dict(model="model name", scenario="scenario name")
        with pytest.raises(ValueError):
            test_context.get_scenario()

    def test_handle_cli_args(self):
        p = "platform name"
        m = "model name"
        s = "scenario name"
        v = "42"

        args1 = dict(
            local_data=Path("foo", "bar"),
            platform=p,
            model_name=m,
            scenario_name=s,
            version=v,
        )

        expected = dict(
            local_data=args1["local_data"],
            platform_info=dict(name=p),
            scenario_info=dict(model=m, scenario=s, version=v),
        )

        ctx = Context()
        ctx.handle_cli_args(**args1)
        assert all(ctx[k] == v for k, v in expected.items())

        url = f"ixmp://{p}/{m}/{s}#{v}"
        args2 = args1.copy()
        args2["url"] = url

        with pytest.raises(click.BadOptionUsage, match="redundant with --url"):
            ctx.handle_cli_args(**args2)

        # New instance
        ctx = Context()

        # Platform and scenario info are empty
        assert 0 == len(ctx["platform_info"]) == len(ctx["scenario_info"])

        ctx.handle_cli_args(url=url, local_data=args1["local_data"])

        # ixmp parse_url() converts the version number to an int
        expected["scenario_info"]["version"] = int(v)
        # url is also stored
        expected["url"] = url

        assert all(ctx[k] == v for k, v in expected.items()), ctx

    def test_use_defaults(self, caplog):
        caplog.set_level(logging.INFO)

        c = Context()

        defaults = dict(foo=["foo2", "foo1", "foo3"], bar=["bar1", "bar3"])

        c.foo = "foo1"

        c.use_defaults(defaults)
        assert ["Use default bar=bar1"] == caplog.messages

        c.bar = "bar2"

        with pytest.raises(
            ValueError, match=re.escape("bar must be in ['bar1', 'bar3']; got bar2")
        ):
            c.use_defaults(defaults)

    # Deprecated methods and attributes

    def test_get_config_file(self, test_context):
        with pytest.deprecated_call():
            assert (
                "message_ix_models",
                "data",
                "level.yaml",
            ) == test_context.get_config_file("level").parts[-3:]

    @pytest.mark.xfail(
        condition=MESSAGE_DATA_PATH is None,
        reason="Requires message_data to be installed.",
    )
    def test_get_path(self, test_context):
        with pytest.deprecated_call():
            assert MESSAGE_DATA_PATH.joinpath(
                "data", "foo", "bar"
            ) == test_context.get_path("foo", "bar")

    def test_load_config(self, test_context):
        # Calling this method is deprecated
        with pytest.deprecated_call():
            # Config files can be loaded and are parsed from YAML into Python objects
            assert isinstance(test_context.load_config("level"), dict)

        # The loaded file is stored and can be reused
        assert isinstance(test_context["level"], dict)

    def test_units(self, test_context):
        """Context.units can be used to parse units that are not standard in pint.

        i.e. message_data unit definitions are used.
        """
        with pytest.deprecated_call():
            assert test_context.units("15 USD_2005 / year").dimensionality == {
                "[currency]": 1,
                "[time]": -1,
            }
