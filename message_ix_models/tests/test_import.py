from importlib import import_module

import pytest

MODULES_WITHOUT_TESTS = [
    None,
    "model.water",
    "model.water.build",
    "model.water.data",
    "model.water.reporting",
]


@pytest.mark.parametrize("name", MODULES_WITHOUT_TESTS)
def test_import(name):
    """Check that modules can be imported.

    Modules **should** have specific tests that actually run code. Where those tests are
    not yet written, this test checks that the modules can at least be imported without
    error.

    Once tests are written for each module, remove it from the above list.
    """
    full_name = ".".join(filter(None, ["message_ix_models", name]))
    import_module(full_name)
