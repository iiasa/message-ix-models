"""Policies."""

from __future__ import annotations

import logging
from abc import ABC
from collections.abc import Collection
from typing import TYPE_CHECKING, cast

import message_ix
import pandas as pd
import yaml
from message_ix import make_df

from message_ix_models import ScenarioInfo

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import TypeVar

    from message_ix_models.util.context import Context

    T = TypeVar("T", bound="Policy")

log = logging.getLogger(__name__)


class Policy(ABC):
    """Base class for policies.

    This class has no attributes or public methods. Other modules in
    :mod:`message_ix_models`:

    - **should** subclass Policy to represent different kinds of policy.
    - **may** add attributes, methods, etc. to aid with the *implementation* of those
      policies in concrete scenarios.
    - in contrast, **may** use minimal subclasses as mere flags to be interpreted by
      other code.

    The default implementation of :func:`hash` returns a value the same for every
    instance of a subclass. This means that two instances of the same subclass hash
    equal. See :attr:`.Config.policy`.
    """

    def __hash__(self) -> int:
        return hash(type(self))


def single_policy_of_type(collection: Collection[Policy], cls: type["T"]) -> "T | None":
    """Return a single member of `collection` of type `cls`."""
    if matches := list(filter(lambda p: isinstance(p, cls), collection)):
        if len(matches) > 1:
            raise ValueError(f"Ambiguous: {len(matches)} instance of {cls}")
        return cast("T", matches[0])

    return None


def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE"
) -> message_ix.Scenario:
    """Plain solve."""
    solve_options = {
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    }

    scenario.solve(model, solve_options=solve_options)
    scenario.set_as_default()

    return scenario


def make_scenario_runner(
    context,
):
    """Create and initialize a ScenarioRunner for policy scenario generation."""
    from message_data.model.scenario_runner import ScenarioRunner

    from message_ix_models.util import private_data_path

    biomass_trade = getattr(context, "biomass_trade", False)

    config_path = (
        private_data_path(*context.policy_config_path)
        if isinstance(context.policy_config_path, tuple)
        else private_data_path(context.policy_config_path)
    )
    with open(config_path) as f:
        config = yaml.safe_load(f)

    model_name = context.dest_scenario["model"]
    model_config = config[model_name]
    # print(f"DEBUG model_config[{model_name!r}] = {model_config}")

    slack_data = model_config["policy_slacks"][model_config["slack_scn"]][context.ssp]

    sr = ScenarioRunner(
        context,
        slack_data=slack_data,
        biomass_trade=biomass_trade,
    )

    # Pre-populate baseline scenario(s) if they do not exist.
    # Use baseline_DEFAULT to match the workflow target
    # (e.g., base cloned -> baseline_DEFAULT).
    # policy_baseline is used by the runner's internal logic; baseline_DEFAULT is the
    # prerequisite name passed to sr.add(..., start_scen="baseline_DEFAULT")
    # by add_glasgow, etc.
    if "policy_baseline" not in sr.scen:
        base_scenario = message_ix.Scenario(
            mp=sr.mp,
            model=sr.model_name,
            scenario="baseline_DEFAULT",
            cache=False,
        )
        sr.scen["policy_baseline"] = base_scenario
        sr.scen["baseline_DEFAULT"] = base_scenario

    return sr


def add_NPi2030(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Add NPi2030 to the scenario."""

    sr = make_scenario_runner(context)
    sr.add(
        "NPi2030",
        "baseline_DEFAULT",
        # must start with this scenario name (hard-coded in the general scenario runner)
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        run_reporting=False,
        solve_typ="MESSAGE-MACRO",
    )

    # sr.add(
    #     "npi_low_dem_scen",
    #     "NPi2030",
    #     slice_year=2025,
    #     tax_emission=150,
    #     run_reporting = False,
    #     solve_typ="MESSAGE-MACRO",
    # )

    sr.run_all()

    # return sr.scen["npi_low_dem_scen"]
    return sr.scen["NPi2030"]


def add_NDC2030(context, scenario):
    """Add NDC policies to the scenario."""
    sr = make_scenario_runner(context)

    sr.add(
        "INDC2030i_weak",
        "baseline_DEFAULT",
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        copy_demands="baseline_low_dem_scen",
        # or other low demand scenario
        # e.g., "npi_low_dem_scen"
        run_reporting=False,
        solve_typ="MESSAGE-MACRO",
    )

    sr.run_all()

    return sr.scen["INDC2030i_weak"]


def add_glasgow(context, scenario, level, start_scen, target_scen, slice_yr):
    """Add Glasgow policies to the scenario.

    Examples
    --------
    In a :class:`~message_ix_models.workflow.Workflow`, use this function as the step
    action and pass the same keyword arguments that
    :meth:`~message_ix_models.workflow.Workflow.add_step` forwards to the
    callable::

        wf.add_step(
            "glasgow_partial_2030 solved",
            "base reported",
            add_glasgow,
            target=f"{model_name}/glasgow_partial_2030",
            target_scen="glasgow_partial_2030",
            slice_yr=2025,
            start_scen="baseline_DEFAULT",
            level="Partial",
        )

        wf.add_step(
            "glasgow_full_2030 solved",
            "base reported",
            add_glasgow,
            target=f"{model_name}/glasgow_full_2030",
            start_scen="baseline_DEFAULT",
            target_scen="glasgow_full_2030",
            slice_yr=2025,
            level="Full",
        )
    """
    sr = make_scenario_runner(context)

    # Prepare add() arguments
    add_kwargs = {
        "mk_INDC": True,
        "slice_year": slice_yr,
        "run_reporting": False,
        "solve_typ": "MESSAGE-MACRO",
    }
    if level.lower() == "full":
        add_kwargs["copy_demands"] = "baseline_low_dem_scen"
        # or other low demand scenario
        # e.g., "npi_low_dem_scen"

    sr.add(target_scen, start_scen, **add_kwargs)

    sr.run_all()

    # Return the target scenario that was created
    return sr.scen[target_scen]


def add_forever_constant(
    context: Context,
    scenario: message_ix.Scenario,
    specified_price: Mapping[str, float] | None = None,
    solve_type: str = "MESSAGE-MACRO",
) -> message_ix.Scenario:
    """Apply constant carbon prices from first model year to 2110.

    - If `specified_price` is given, use those node-level constant values.
    - Otherwise, use model-derived values from ``PRICE_EMISSION`` at
      ``scenario.firstmodelyear`` and extend them as constants through 2110.

    Example
    -------
    `specified_price` can be::

        {
            "R12_AFR": 7.33,
            "R12_CHN": 7.33,
            "R12_EEU": 91.667,...
        }
    or copied from a lookup run.
    """
    fmy = int(scenario.firstmodelyear)

    info = ScenarioInfo(scenario)
    model_years = [y for y in info.Y if fmy <= y <= 2110]

    if specified_price:
        base_prices = pd.DataFrame(
            {"node": list(specified_price), "lvl": list(specified_price.values())}
        )
    else:
        base_prices = scenario.var("PRICE_EMISSION").loc[
            lambda df: df.year == fmy, ["node", "lvl"]
        ]
        missing = (set(info.N) - {"World", "R12_GLB"}) - set(base_prices.node)
        if missing:
            base_prices = pd.concat(
                [base_prices, pd.DataFrame({"node": list(missing), "lvl": 0})],
                ignore_index=True,
            )

    price_long = pd.concat(
        [base_prices.assign(year=year) for year in model_years], ignore_index=True
    )
    df = make_df(
        "tax_emission",
        node=price_long["node"],
        type_emission="TCE",
        type_tec="all",
        type_year=price_long["year"],
        unit="USD/tC",
        value=price_long["lvl"],
    )

    with scenario.transact("applying constant cprice"):
        bound_df = scenario.par("bound_emission")
        if not bound_df.empty:
            scenario.remove_par("bound_emission", bound_df)
        scenario.add_par("tax_emission", df)

    if specified_price:
        detail = ", ".join(
            f"{node}={price}" for node, price in sorted(specified_price.items())
        )
        log.info(
            f"Added specified constant carbon prices ({fmy}-2110) to "
            f"{scenario.model}/{scenario.scenario}: {detail}"
        )
    else:
        log.info(
            f"Added constant carbon prices ({fmy}-2110) to "
            f"{scenario.model}/{scenario.scenario}"
        )
    solve(context, scenario, model=solve_type)
    scenario.set_as_default()
    return scenario


def add_forever_interpolate(
    context: Context,
    scenario: message_ix.Scenario,
    price_2100: float = 200,
    solve_type: str = "MESSAGE-MACRO",
) -> message_ix.Scenario:
    """Apply interpolated carbon prices from first model year to 2110.

    - Base values are taken from ``PRICE_EMISSION`` at ``scenario.firstmodelyear``.
    - Values are interpolated to reach `price_2100` in years 2100 and 2110.
    """
    fmy = int(scenario.firstmodelyear)
    info = ScenarioInfo(scenario)
    regions = set(info.N) - {"World", "R12_GLB"}
    years = [y for y in info.Y if fmy <= y <= 2110]

    base = scenario.var("PRICE_EMISSION").loc[
        lambda df: df.year == fmy, ["node", "lvl"]
    ]
    missing = regions - set(base.node)
    if missing:
        base = pd.concat(
            [base, pd.DataFrame({"node": list(missing), "lvl": 0})], ignore_index=True
        )
    base = base.assign(year=fmy)

    long = (
        pd.concat(
            [
                base,
                pd.DataFrame({"node": list(regions), "lvl": price_2100, "year": 2100}),
                pd.DataFrame({"node": list(regions), "lvl": price_2100, "year": 2110}),
            ],
            ignore_index=True,
        )
        .pivot_table(values="lvl", index="year", columns="node")
        .reindex(sorted(set(years) | {fmy, 2100, 2110}))
        .sort_index()
        .interpolate(method="index")
        .loc[years]
        .reset_index()
        .melt(id_vars="year", var_name="node", value_name="lvl")
    )
    df = make_df(
        "tax_emission",
        node=long["node"],
        type_emission="TCE",
        type_tec="all",
        type_year=long["year"],
        unit="USD/tC",
        value=long["lvl"],
    )

    with scenario.transact("applying interpolated cprice"):
        bound_df = scenario.par("bound_emission")
        if not bound_df.empty:
            scenario.remove_par("bound_emission", bound_df)
        scenario.add_par("tax_emission", df)

    log.info(
        f"Added interpolated carbon prices ({fmy}-2110, {price_2100} at 2100/2110) to "
        f"{scenario.model}/{scenario.scenario}"
    )
    solve(context, scenario, model=solve_type)
    scenario.set_as_default()
    return scenario
