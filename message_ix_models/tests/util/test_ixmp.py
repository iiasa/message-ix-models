"""Tests of :mod:`.util.ixmp`."""

import pytest

from message_ix_models.util.ixmp import Platforms


class TestPlatforms:
    @pytest.fixture(scope="function")
    def platforms(self) -> Platforms:
        """Fixture: an instance of :class:`Platforms`."""
        return Platforms()

    def test_pop(self, platforms: Platforms) -> None:
        """:meth:`.Platforms.pop` of non-existent Platform does not raise exception."""
        platforms.pop(name="foo")

    def test_popitem(self, platforms: Platforms) -> None:
        """:meth:`.Platforms.popitem` with 0 Platforms does not raise exception."""
        assert 0 == len(platforms._platforms)
        platforms.popitem()
        platforms.popitem()
