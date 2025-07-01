import os
from typing import TYPE_CHECKING

from message_ix_models.testing import bare_res, not_ci

if TYPE_CHECKING:
    import pytest

    from message_ix_models import Context
    from message_ix_models.util.click import CliRunner


def test_bare_res_solved(
    request: "pytest.FixtureRequest", test_context: "Context"
) -> None:
    """:func:`.bare_res` works with `solve` = :obj:`True`.

    This test can be removed once this feature of the test function is used by another
    test.
    """
    bare_res(request, test_context, solved=True)


def test_cli_runner(mix_models_cli: "CliRunner") -> None:
    result = mix_models_cli.invoke(["foo", "bar"])
    assert "No such command 'foo'" in result.output


@not_ci(reason="foo", action="skip")
def test_not_ci_skip() -> None:
    """Test not_ci(action="skip")."""


@not_ci(reason="foo", action="xfail")
def test_not_ci_xfail() -> None:
    """Test not_ci(action="skip")."""
    assert "GITHUB_ACTIONS" not in os.environ
