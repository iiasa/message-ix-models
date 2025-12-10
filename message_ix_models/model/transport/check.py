"""Checks and conditions for :mod:`.transport.build`."""

from typing import TYPE_CHECKING

from message_ix_models.model.transport import (
    Config,
    constraint,
    disutility,
    freight,
    key,
    ldv,
    other,
    passenger,
    policy,
)
from message_ix_models.model.transport.testing import assert_units
from message_ix_models.testing.check import (
    Check,
    ContainsDataForParameters,
    HasCoords,
    HasUnits,
    NoDuplicates,
    NoneMissing,
    NonNegative,
    Size,
    insert_checks,
    verbose_check,
)

if TYPE_CHECKING:
    from pathlib import Path

    from genno import Computer
    from genno.types import KeyLike

    from message_ix_models import Context, ScenarioInfo
    from message_ix_models.testing.check import CheckResult


class LDV_PHEV_input(Check):
    """Check magnitudes of input energy intensities of LDV PHEV technologies.

    There are three conditions:

    1. Electricity input to the PHEV is less than electricity input to BEV.
    2. Light oil input to the PHEV is less than light oil input to an ICEV.
    3. Total energy input to the PHEV is between the BEV and ICEV.
    """

    # Technologies to check
    _t = ("ELC_100", "PHEV_ptrp", "ICE_conv")

    types = (dict,)

    def run(self, obj):
        def _join_levels(df):
            """Join multi-index labels to a single str."""
            return df.set_axis(
                df.columns.to_series().apply(lambda v: " ".join(map(str, v))), axis=1
            )

        t0, t1, t2 = self._t
        tmp = (
            obj["input"]
            .query(f"technology in {self._t!r}")
            .set_index(["node_loc", "year_vtg", "year_act"])
            .pivot(columns=["technology", "commodity"], values="value")
            .pipe(_join_levels)
            .eval(f"`{t1}` = `{t1} electr` + `{t1} lightoil`")
            .eval(f"c1 = `{t1} electr` <= `{t0} electr`")
            .eval(f"c2 = `{t1} lightoil` <= `{t2} lightoil`")
            .eval(f"c3 = `{t0} electr` < `{t1}` < `{t2} lightoil`")
            .eval("cond = c1 & c2 & c3")
        )

        if not tmp.cond.all():  # pragma: no cover
            fail = tmp[~tmp.cond]
            return (
                False,
                f"LDV input does not satisfy conditions in {len(fail)} cases:\n"
                f"{fail.to_string()}",
            )
        else:
            return True, "LDV input satisfies conditions"


class Passenger(Check):
    """Check data from :mod:`.transport.passenger`.

    These assertions were formerly in a test at
    :py:`.tests.transport.test_data.test_get_non_ldv_data()`.
    """

    types = (dict,)

    def run(self, obj):
        from iam_units import registry

        # Input data have expected units
        df_input = obj["input"]
        mask0 = df_input["technology"].str.endswith(" usage")
        mask1 = df_input["technology"].str.startswith("transport other")

        assert_units(df_input[mask0], registry("Gv km"))
        if mask1.any():
            assert_units(df_input[mask1], registry("GWa"))
        assert_units(df_input[~(mask0 | mask1)], registry("1.0 GWa / (Gv km)"))

        # Output data exist for all non-LDV modes
        df_output = obj["output"]
        modes = list(filter(lambda m: m != "LDV", Config().demand_modes))
        obs = set(df_output["commodity"].unique())
        assert len(modes) * 2 == len(obs)

        # Output data have expected units
        mask = df_output["technology"].str.endswith(" usage")
        assert_units(df_output[~mask], {"[vehicle]": 1, "[length]": 1})
        assert_units(df_output[mask], {"[passenger]": 1, "[length]": 1})

        return True, "Passenger non-LDV input and output satisfy conditions"


#: Inline checks for :func:`.test_debug`.
CHECKS: dict["KeyLike", tuple[Check, ...]] = {
    # .build.add_structure()
    "broadcast:t-c-l:transport+input": (HasUnits("dimensionless"),),
    "broadcast:t-c-l:transport+output": (
        HasUnits("dimensionless"),
        HasCoords({"commodity": ["transport F RAIL vehicle"]}),
    ),
    #
    # From .constraint
    # NB Cannot use NoDuplicates here yet due to:
    # - initial_new_capacity_up: 336 duplicated keys
    # - growth_activity_lo: 336 duplicated keys
    # - growth_new_capacity_up: 336 duplicated keys
    constraint.TARGET: (
        ContainsDataForParameters(
            {
                "bound_new_capacity_up",
                "growth_activity_lo",
                "growth_activity_up",
                "growth_new_capacity_lo",
                "growth_new_capacity_up",
                "initial_activity_lo",
                "initial_activity_up",
                "initial_new_capacity_lo",
                "initial_new_capacity_up",
            }
        ),
    ),
    #
    # From .freight
    # The following replicates a deleted .transport.test_data.test_get_freight_data()
    freight.TARGET: (
        ContainsDataForParameters(
            {
                "demand",
                "capacity_factor",
                "input",
                "output",
                "technical_lifetime",
            }
        ),
        # HasCoords({"technology": ["f rail electr"]}),
    ),
    "output::F+ixmp": (
        HasCoords(
            {"commodity": ["transport F RAIL vehicle", "transport F ROAD vehicle"]}
        ),
    ),
    # .freight.other()
    "other::F+ixmp": (HasCoords({"technology": ["f rail electr"]}),),
    #
    # The following are intermediate checks formerly in .test_demand.test_exo
    "mode share:n-t-y:base": (HasUnits(""),),
    "mode share:n-t-y": (HasUnits(""),),
    key.pop: (HasUnits("Mpassenger"),),
    "cg share:n-y-cg": (HasUnits(""),),
    "GDP:n-y:PPP+capita": (HasUnits("kUSD / passenger / year"),),
    "votm:n-y": (HasUnits(""),),
    key.price: (HasUnits("USD / km"),),
    "cost:n-y-c-t": (HasUnits("USD / km"),),
    key.pdt_nyt[0]: (HasUnits("passenger km / year"),),
    key.pdt_nyt[1]: (HasUnits("passenger km / year"),),
    key.ldv_ny + "total": (HasUnits("Gp km / a"),),
    # FIXME Handle dimensionality instead of exact units
    # demand.ldv_nycg: (HasUnits({"[length]": 1, "[passenger]": 1, "[time]": -1}),),
    "pdt factor:n-y-t": (HasUnits(""),),
    # "fv factor:n-y": (HasUnits(""),),  # Fails: this key no longer exists
    # "fv:n:advance": (HasUnits(""),),  # Fails: only fuzzed data in message-ix-models
    key.fv_cny: (HasUnits("Gt km"),),
    #
    # Exogenous demand calculation succeeds
    "transport demand::ixmp": (
        # Data is returned for the demand parameter only
        ContainsDataForParameters({"demand"}),
        HasCoords({"level": ["useful"]}),
        # Certain labels are specifically excluded/dropped in the calculation
        HasCoords(
            {"commodity": ["transport pax ldv", "transport F WATER"]}, inverse=True
        ),
        # No negative values
        NonNegative(),
        # …plus default NoneMissing
    ),
    f"input{ldv.Li}": (LDV_PHEV_input(),),
    # .disutility.prepare_computer()
    "disutility:n-cg-t-y": (Size(dict(cg=27 * 12)),),
    disutility.TARGET: (ContainsDataForParameters({"input"}),),
    #
    "historical_new_capacity::LDV+ixmp": (HasUnits("million * v / a"),),
    # The following partly replicates .test_ldv.test_get_ldv_data()
    # NB Cannot use NoDuplicates here yet due to:
    # - inv_cost: 50076 duplicated keys
    # - emission_factor: 2280 duplicated keys
    # - capacity_factor: 54432 duplicated keys
    ldv.TARGET: (
        ContainsDataForParameters(
            {
                "bound_new_capacity_lo",
                "bound_new_capacity_up",
                "capacity_factor",
                "emission_factor",
                "fix_cost",
                "historical_new_capacity",
                "input",
                "inv_cost",
                "output",
                "relation_activity",
                "technical_lifetime",
                "var_cost",
            }
        ),
    ),
    other.TARGET: (
        ContainsDataForParameters({"bound_activity_lo", "bound_activity_up", "input"}),
    ),
    # NB Cannot use NoDuplicates here yet due to:
    # - output: 1260 duplicated keys
    passenger.TARGET: (
        ContainsDataForParameters(
            {
                "bound_activity_lo",  # From .passenger.other(). For R11 this is empty.
                "bound_activity_up",  # act-non_ldv.csv via .passenger.bound_activity()
                "capacity_factor",
                # "emission_factor",
                "fix_cost",
                "input",
                "inv_cost",
                "output",
                # "relation_activity",
                "technical_lifetime",
                "var_cost",
            }
        ),
        Passenger(),
    ),
    policy.TARGET: (
        HasCoords({"type_emission": ["TCE"]}),
        # No structure in base scenarios to accommodate these values → discard
        HasCoords({"type_emission": ["CO2_shipping_IMO"]}, inverse=True),
    ),
}


def insert(c: "Computer", N_node: int, verbosity: int, path: "Path") -> "CheckResult":
    """Insert :data:`CHECKS` into `c`."""
    context: "Context" = c.graph["context"]
    info: "ScenarioInfo" = c.get("info")

    # Modify CHECKS according to the settings
    checks = CHECKS.copy()
    if context.model.regions == "R12":
        checks["transport::O+ixmp"] = (
            ContainsDataForParameters(
                {"bound_activity_lo", "bound_activity_up", "input"}
            ),
        )

    # Has exactly the periods (y) in the model horizon
    k = "disutility:n-cg-t-y"
    checks[k] = checks[k] + (HasCoords({"y": info.Y}),)

    # Construct a list of common checks to be appended to every value in `checks`
    common = [Size({"n": N_node}), NoneMissing()] + verbose_check(verbosity, path)

    # Insert key-specific and common checks
    k = "test_debug"
    return insert_checks(c, k, checks, common)
