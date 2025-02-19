from importlib.metadata import version

import pytest

if version("genno") < "1.28.0":
    pytest.skip(
        reason="""message_ix/ixmp v3.7.0 are tested with genno < 1.25, but these tests
need ≥ 1.28.0:

- .model.transport.key imports genno.Keys
- .tests.model.transport.test_base imports genno.operator.random_qty()""",
        allow_module_level=True,
    )
