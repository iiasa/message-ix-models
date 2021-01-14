from copy import deepcopy

import pytest

from message_data.model.transport.utils import read_config


@pytest.fixture(scope="session")
def transport_context(session_context):
    """A context with the MESSAGEix-Transport configuration loaded."""
    read_config()
    yield session_context


@pytest.fixture(scope="function")
def transport_context_f(transport_context):
    """A modifiable copy of :func:`transport_context`, scoped to one test function."""
    ctx = deepcopy(transport_context)
    yield ctx
    ctx.delete()
