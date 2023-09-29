import pytest
from genno import Computer

from message_ix_models.tools.exo_data import (
    DemoSource,
    ExoDataSource,
    prepare_computer,
    register_source,
)


class TestExoDataSource:
    def test_abstract(self):
        with pytest.raises(TypeError, match="Can't instantiate"):
            ExoDataSource()

    def test_register_source(self):
        with pytest.raises(ValueError, match="already registered for"):
            register_source(DemoSource)


@pytest.mark.parametrize("regions, N_n", [("R12", 12), ("R14", 14)])
def test_prepare_computer(test_context, regions, N_n):
    """:func:`.exo_data.prepare_computer` works as intended."""
    test_context.model.regions = regions

    c = Computer()

    # Function runs successfully `c`
    keys = prepare_computer(test_context, c, "test s1", dict(measure="POP"))

    # print(c.describe(keys[-1]))

    # Computation of data runs successfully
    result = c.get(keys[-1])

    # Data has the expected dimensions
    assert ("n", "y") == result.dims

    # Data is complete
    assert N_n == len(result.coords["n"])
    assert 14 == len(result.coords["y"])


def test_prepare_computer_exc(test_context):
    c = Computer()

    with pytest.raises(ValueError, match="must be one of"):
        prepare_computer(test_context, c, "test s1", dict(measure="FOO"))

    with pytest.raises(ValueError, match="No source found that can handle"):
        prepare_computer(test_context, c, "not a source")


@pytest.mark.parametrize("regions, N_n", [("R12", 12), ("R14", 14)])
def test_operator(test_context, regions, N_n):
    """Exogenous data calculations can be set up through :meth:`.Computer.add`."""
    test_context.model.regions = regions

    c = Computer()
    c.require_compat("message_ix_models.report.computations")

    # Function runs successfully `c`
    keys = c.add(
        "exogenous_data",
        context=test_context,
        source="test s1",
        source_kw=dict(measure="POP"),
    )

    # print(c.describe(keys[-1]))

    # Computation of data runs successfully
    result = c.get(keys[-1])

    # Data has the expected dimensions
    assert ("n", "y") == result.dims

    # Data is complete
    assert N_n == len(result.coords["n"])
    assert 14 == len(result.coords["y"])
