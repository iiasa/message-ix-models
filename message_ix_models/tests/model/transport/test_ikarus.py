from copy import deepcopy
from typing import TYPE_CHECKING

import numpy as np
import pytest
from iam_units import registry

from message_ix_models.model.transport import build, testing
from message_ix_models.model.transport.ikarus import TARGET
from message_ix_models.model.transport.passenger import UNITS
from message_ix_models.project.navigate import T35_POLICY
from message_ix_models.testing.check import (
    Check,
    ContainsDataForParameters,
    HasCoords,
    HasUnits,
    NoneMissing,
    Size,
    insert_checks,
    verbose_check,
)

if TYPE_CHECKING:
    from message_ix_models.types import KeyLike

PARAMETERS = "fix_cost input inv_cost output technical_lifetime var_cost".split()


class C1(Check):
    """A particular value in ``inv_cost``."""

    types = (dict,)

    def run(self, obj):
        row = (
            obj["inv_cost"]
            .query("technology == 'rail_pub' and year_vtg == 2020")
            .iloc[0, :]
        )
        return np.allclose(
            23.689086, row["value"]
        ), "inv_cost(t=rail_pub, yv=2020) has expected value"


class C2(Check):
    """Particular values in ``technical_lifetime``."""

    types = (dict,)

    def run(self, obj):
        name = "technical_lifetime"

        df = obj[name]
        n, yv = sorted(df["node_loc"].unique())[-1], (2010, 2050)  # noqa: F841
        q = "node_loc == @n and technology == 'ICG_bus' and year_vtg in @yv"
        msg = f"{name} values of 14.7 are rounded to 15.0"

        return np.allclose(df.query(q).value, 15.0), msg


CHECKS: dict["KeyLike", list[Check]] = {
    TARGET: [
        ContainsDataForParameters(set(PARAMETERS)),
        HasCoords({"technology": ["con_ar"]}),
        C1(),
        C2(),
    ]
}


@build.get_computer.minimum_version
@testing.MARK[10]
@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions, N_node",
    [
        pytest.param("ISR", 1, marks=testing.MARK[3]),
        ("R11", 11),
        ("R12", 12),
        ("R14", 14),
    ],
)
@pytest.mark.parametrize("options", [{}, dict(navigate_scenario=T35_POLICY.TEC)])
def test_get_ikarus_data(
    tmp_path,
    test_context,
    regions,
    N_node,
    years,
    options,
    verbosity: int = 1,
):
    """Test genno-based IKARUS data prep.

    .. todo:: Roll in to :func:`.transport.test_build.test_debug`.
    """
    ctx = test_context
    c, info = testing.configure_build(
        ctx, regions=regions, years=years, options=options
    )

    # Extend `CHECKS`
    checks = deepcopy(CHECKS)

    # Data cover the model time horizon
    checks[TARGET].append(HasCoords({"year_vtg": info.Y}))

    # Data have the expected units for the respective parameter
    for par_name in PARAMETERS:
        checks[f"transport nonldv {par_name}::ixmp"] = [
            HasUnits(registry(UNITS[par_name])),
        ]

    result = insert_checks(
        c,
        "test_get_ikarus_data",
        checks,
        # Construct a list of common checks
        [Size({"n": N_node}), NoneMissing()] + verbose_check(verbosity, tmp_path),
    )

    # Show and print a different key
    k = TARGET

    # Show what will be computed
    # verbosity = True  # DEBUG Force printing the description
    if verbosity:
        print(c.describe(k))

    # return  # DEBUG Exit before doing any computation

    # Compute the test key; all calculations complete without error
    tmp = c.get(k)

    assert result, "1 or more checks failed"
    del tmp
