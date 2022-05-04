import os

import click
import pytest

from message_ix_models.testing import bare_res, not_ci


def test_bare_res_no_request(test_context):
    """:func:`bare_res` works with `request` = :obj:`None`."""
    bare_res(None, test_context, solved=False)


def test_bare_res_solved(request, test_context):
    """:func:`bare_res` works with `solve` = :obj:`True`.

    This test can be removed once this feature of the test function is used by another
    test.
    """
    bare_res(request, test_context, solved=True)


def test_cli_runner(mix_models_cli):
    with pytest.raises(click.exceptions.UsageError, match="No such command 'foo'"):
        mix_models_cli.assert_exit_0(["foo", "bar"])


@not_ci(reason="foo", action="skip")
def test_not_ci_skip():
    """Test not_ci(action="skip")."""


@not_ci(reason="foo", action="xfail")
def test_not_ci_xfail():
    """Test not_ci(action="skip")."""
    assert "GITHUB_ACTIONS" not in os.environ
