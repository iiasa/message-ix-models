import pandas as pd
import pandas.testing as pdt
import pytest
from sdmx.model import Annotation, Code

from message_ix_models.model.macro import generate, load
from message_ix_models.util import package_data_path


@pytest.mark.parametrize(
    "parameter, value",
    [
        ("aeei", 1.0),
        ("config", None),
        ("depr", 1.0),
        ("drate", 1.0),
        ("lotol", 1.0),
        pytest.param("foo", 1.0, marks=pytest.mark.xfail(raises=NotImplementedError)),
    ],
)
def test_generate0(test_context, parameter, value):
    result = generate(parameter, test_context, value=value)

    assert not result.isna().any(axis=None)


def test_generate1(test_context):
    commodities = [
        Code(id="foo", annotations=[Annotation(id="macro-sector", text="BAR")]),
        Code(id="baz", annotations=[Annotation(id="macro-sector", text="QUX")]),
    ]

    result = generate("config", test_context, commodities)

    assert {"foo", "baz"} == set(result["commodity"].unique())

    # Only the identified sectors appear
    assert {"BAR", "QUX"} == set(result["sector"].unique())

    # Only 2 unique (commodity, sector) combinations appear
    assert 2 == len(result[["commodity", "sector"]].drop_duplicates())


def test_load(test_context):
    result = load(package_data_path("test", "macro"))
    assert {"kgdp"} == set(result.keys())
    pdt.assert_index_equal(pd.Index(["node", "value", "unit"]), result["kgdp"].columns)
    assert not result["kgdp"].isna().any(axis=None)
