import genno
import pytest

from message_ix_models.project.shape.data import SHAPE  # noqa: F401
from message_ix_models.tools.exo_data import prepare_computer


class TestSHAPE:
    @pytest.mark.parametrize(
        "source_kw, dimensionality",
        (
            (dict(measure="gdp", scenario="society"), {}),
            (dict(measure="gini", scenario="society"), {}),
            (dict(measure="population", scenario="all_SHAPE_SDPs"), {}),
            (dict(measure="urbanisation", scenario="urb_tech"), {}),
            pytest.param(
                dict(measure="foo", scenario="urb_tech"),
                {},
                marks=pytest.mark.xfail(raises=ValueError),
            ),
        ),
    )
    @pytest.mark.parametrize(
        "regions, aggregate, N_n",
        (
            ("R12", True, 12),
            ("R12", False, 183),
        ),
    )
    def test_prepare_computer(
        self, test_context, source_kw, dimensionality, regions, aggregate, N_n
    ):
        test_context.model.regions = regions

        source = "SHAPE"
        source_kw.update(aggregate=aggregate)

        c = genno.Computer()

        keys = prepare_computer(test_context, c, source, source_kw)

        # Key has an informative name
        assert source_kw["measure"] == keys[0].name

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have the expected dimensions and size
        assert {"n", "y"} == set(result.dims)
        assert N_n <= len(result.coords["n"])
        assert N_n * 14 <= result.size


@pytest.mark.parametrize(
    "args, size",
    [
        (("gdp", None), 23904),
        (("gdp", "1.2"), 23904),
        (("gdp", "1.1"), 14193),
        (("gdp", "1.0"), 14193),
        (("gini", None), 9333),
        (("gini", "1.1"), 9333),
        pytest.param(
            ("gini", "1.0"),
            9333,
            marks=pytest.mark.xfail(
                raises=genno.ComputationError,
                reason="File format differs, lacks 'tgt.achieved' column",
            ),
        ),
        (("population", None), 7968),
        (("population", "1.2"), 7968),
        (("population", "1.1"), 4731),
        (("population", "1.0"), 4731),
        (("urbanisation", None), 11001),
    ],
)
def test_get_shape_data(test_context, args, size):
    test_context.model.regions = "R12"

    source = "SHAPE"
    source_kw = dict(measure=args[0], aggregate=False, interpolate=False)
    if args[1]:
        source_kw.update(version=args[1])

    c = genno.Computer()

    keys = prepare_computer(test_context, c, source, source_kw)

    # Preparation of data runs successfully
    result = c.get(keys[0])

    # Data have the expected size
    assert size == result.size
    assert {"n", "y", "SCENARIO"} == set(result.dims)
