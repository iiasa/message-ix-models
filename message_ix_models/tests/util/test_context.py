from copy import deepcopy

import pytest


class TestContext:
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

    def test_load_config(self, test_context):
        # Config files can be loaded and are parsed from YAML into Python objects
        assert isinstance(test_context.load_config("sources"), dict)

        # The loaded file is stored and can be reused
        assert isinstance(test_context["sources"], dict)

    def test_deepcopy(self, session_context):
        """Paths are preserved through deepcopy()."""
        mdp = session_context.message_data_path
        ldp = session_context.local_data_path

        c = deepcopy(session_context)

        assert mdp == c.message_data_path
        assert ldp == c.local_data_path

    def test_get_cache_path(self, pytestconfig, test_context):
        """cache_path() returns the expected output."""
        base = (
            test_context.metadata_path
            if pytestconfig.option.local_cache
            else test_context.local_data_path
        )
        assert base.joinpath(
            "cache", "pytest", "bar.pkl"
        ) == test_context.get_cache_path("pytest", "bar.pkl")

    def test_units(self, test_context):
        """Context.units can be used to parse units that are not standard in pint.

        i.e. message_data unit definitions are used.
        """
        assert test_context.units("15 USD_2005 / year").dimensionality == {
            "[currency]": 1,
            "[time]": -1,
        }
