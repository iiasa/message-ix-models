import logging
import os
import platform
import re
from copy import deepcopy
from pathlib import Path

import click
import ixmp
import pytest
from message_ix import Scenario

from message_ix_models import Context


class TestContext:
    def test_get_instance(self, session_context):
        c = Context()
        assert c is Context.get_instance(-1)
        c.delete()

    def test_only(self):
        # Ensure at least 2 instances exist
        c2 = Context()

        with pytest.raises(IndexError, match=r"ambiguous: \d+ Context instances"):
            Context.only()

        c2.delete()

    def test_clone_to_dest(self, caplog, test_context):
        ctx = test_context

        platform_name = ctx.platform_info["name"]
        model_name = "foo model"
        scenario_name = "bar scenario"

        # Works with direct settings, no URL
        c = deepcopy(ctx)

        # Force the base scenario info to be empty
        c.scenario_info.clear()
        c.dest_scenario = dict(model=model_name, scenario=scenario_name)

        # Fails with create=False
        with pytest.raises(
            TypeError, match="missing 1 required positional argument: 'model'"
        ):
            c.clone_to_dest(create=False)

        # Succeeds with default create=True
        s = c.clone_to_dest()

        # Base scenario was created
        assert "Base scenario not given or found" in caplog.messages

        # Works with a URL to parse and no base scenario
        url = f"ixmp://{platform_name}/{model_name}/{scenario_name}"

        c2 = deepcopy(ctx)
        c2.scenario_info.clear()
        c2.dest = url
        s = c2.clone_to_dest()
        assert model_name == s.model and scenario_name == s.scenario

        del s

        # Works with a base scenario
        c2.handle_cli_args(url=url)
        c2.dest_scenario = dict(model="baz model", scenario="baz scenario")
        c2.dest_platform.clear()
        s = c2.clone_to_dest()

        assert s.model.startswith("baz") and s.scenario.startswith("baz")

    def test_dealias(self, caplog):
        """Aliasing works with :meth:`.Context.__init__`, :meth:`.Context.update`."""
        c = Context()
        c.update(regions="R99")
        assert [] == caplog.messages  # No log warnings for core Config, .model.Config
        assert "R99" == c.model.regions

        c = Context(regions="R98")
        assert [
            "Create a Config instance instead of passing ['regions'] to Context()"
        ] == caplog.messages
        caplog.clear()
        assert "R98" == c.model.regions == c.regions
        assert [] == caplog.messages  # No log warnings for access

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

    def test_get_cache_path(self, pytestconfig, test_context):
        """cache_path() returns the expected output."""
        # One of two values depending on whether the user has given --local-cache
        assert (
            test_context.get_cache_path("pytest", "bar.pkl")
            in (
                test_context.local_data.joinpath("cache", "pytest", "bar.pkl"),
                Path(pytestconfig.cache.makedir("cache")).joinpath("pytest", "bar.pkl"),
            )
            or pytestconfig.option.local_cache
        )

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

    @pytest.mark.flaky(
        reruns=5,
        rerun_delay=2,
        condition="GITHUB_ACTIONS" in os.environ and platform.system() == "Darwin",
        reason="Flaky; see iiasa/message-ix-models#112",
    )
    def test_set_scenario(self, test_context):
        mp = test_context.get_platform()
        s = Scenario(mp, "foo", "bar", version="new")

        # set_scenario() updates Context.scenario_info
        test_context.scenario_info = dict()
        test_context.set_scenario(s)
        assert (
            dict(model="foo", scenario="bar", version=0) == test_context.scenario_info
        )

    def test_write_debug_archive(self, mix_models_cli):
        """:meth:`.write_debug_archive` works."""
        # Create a CLI command attached to the hidden "_test" group

        from message_ix_models.testing import cli_test_group

        @cli_test_group.command("write-debug-archive")
        @click.pass_obj
        def _(context):
            # Register one file to be archived
            p = context.core.local_data.joinpath("foo.txt")
            context.core.debug_paths.append(p)

            # Write some text to this file
            p.write_text("Here is some debug output in a file.")

            # Register a non-existent path
            context.core.debug_paths.append(p.with_name("bar.txt"))

            # Write the archive
            context.write_debug_archive()

        # Invoke the command; I/O occurs in a temporary directory
        result = mix_models_cli.invoke(["_test", "write-debug-archive"])

        # Output path is constructed as expected; file exists
        match = re.search(
            r"Write to: (.*main-_test-write-debug-archive-[\dabcdefT\-]+.zip)",
            result.output,
        )
        assert Path(match.group(1)).exists()

        # Log output is generated for the non-existent path in Context.debug_paths
        assert re.search(r"Not found: .*bar.txt", result.output)

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
        ctx.delete()
        ctx = Context()

        # Platform and scenario info are empty
        assert 0 == len(ctx["platform_info"]) == len(ctx["scenario_info"])

        ctx.handle_cli_args(url=url, local_data=args1["local_data"])

        # ixmp parse_url() converts the version number to an int
        expected["scenario_info"]["version"] = int(v)
        # url is also stored
        expected["url"] = url

        assert all(ctx[k] == v for k, v in expected.items()), ctx

        ctx.delete()

    def test_repr(self):
        c = Context()
        assert re.fullmatch("<Context object at [^ ]+ with 2 keys>", repr(c))

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

        c.delete()
