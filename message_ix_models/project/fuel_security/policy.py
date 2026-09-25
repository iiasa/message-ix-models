"""Policy-scenario helpers specific to the fuel security project."""

import logging

import message_ix
import yaml

from message_ix_models import Context
from message_ix_models.util import private_data_path

log = logging.getLogger(__name__)


def make_scenario_runner(context: Context):
    """Create and initialize a ScenarioRunner for fuel security policy scenarios.

    Args:
        context: Context with `policy_config_path`, `dest_scenario`, and `ssp` set
    Returns:
        sr: Initialized ScenarioRunner, with "baseline_DEFAULT" pre-registered
    """
    from message_data.model.scenario_runner import ScenarioRunner

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

    slack_data = model_config["policy_slacks"][model_config["slack_scn"]][context.ssp]

    sr = ScenarioRunner(
        context,
        slack_data=slack_data,
        biomass_trade=biomass_trade,
    )

    # Pre-populate baseline scenario(s) if they do not exist.
    # Use baseline_DEFAULT to match the workflow target
    # (e.g., "Base cloned" -> baseline_DEFAULT).
    if "policy_baseline" not in sr.scen:
        base_scenario = message_ix.Scenario(
            mp=sr.mp,
            model=sr.model_name,
            scenario="baseline_DEFAULT",
            cache=False,
        )
        sr.scen["policy_baseline"] = base_scenario
        sr.scen["baseline_DEFAULT"] = base_scenario
        sr.scen["baseline"] = base_scenario

    return sr


def add_NPi2030(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Add NPi2030 to the scenario.

    Args:
        context: Context with `policy_config_path`, `dest_scenario`, and `ssp` set
        scenario: Base scenario (unused directly; the ScenarioRunner clones from
            "baseline_DEFAULT" on the platform identified by `context`)
    Returns:
        scenario: The NPi2030 scenario produced by the ScenarioRunner
    """
    sr = make_scenario_runner(context)
    sr.add(
        "NPi2030",
        "baseline_DEFAULT",
        # must start with this scenario name (hard-coded in the general scenario
        # runner)
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        run_reporting=False,
        solve_typ="MESSAGE",
    )

    sr.run_all()

    # ixmp's Scenario.clone() (used internally by ScenarioRunner) does not mark
    # the new version as default - see _set_default() in workflow.py for the same
    # issue on "Base cloned". Without this, downstream code loading
    # "fuel_security/NPi2030" by name only would silently resolve to a stale
    # default version instead of the one just produced by this run.
    sr.scen["NPi2030"].set_as_default()

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
        run_reporting=False,
        solve_typ="MESSAGE",
    )

    sr.run_all()

    sr.scen["INDC2030i_weak"].set_as_default()

    return sr.scen["INDC2030i_weak"]


def add_gdp_price_growth(
    context: Context,
    scenario: message_ix.Scenario,
    base_year: int = 2030,
    price_cap: float = 300 * 44 / 12,
    solve_scenario: bool = True,
) -> message_ix.Scenario:
    """Grow the extrapolated NDC carbon price with GDP, subject to a ceiling.

    The SSP_SSP2_v6.6 "_forever" scenarios hold each region's `base_year` carbon
    price (matched against the pricelookup_baselineS_* scenarios) constant through
    2110. This rescales it as in message_data's ScenarioRunner._post_target, i.e.
    price(y) = price(base_year) * gdp_calibrate(y) / gdp_calibrate(base_year),
    then clips it at `price_cap`.

    Args:
        context: Workflow context (unused)
        scenario: A "_forever" scenario with a flat post-`base_year` tax_emission
        base_year: Year whose price is grown forward
        price_cap: Ceiling in the tax_emission unit, USD/tC (default: 300
            USD/tCO2, the top of the v6.6 price lookup)
        solve_scenario: Whether to solve the new scenario with MESSAGE
    Returns:
        target_scenario: Clone of `scenario` named "<scenario>_gdp"
    """
    target_scenario = scenario.clone(
        "fuel_security", f"{scenario.scenario}_gdp", keep_solution=False
    )
    target_scenario.set_as_default()

    tax = target_scenario.par("tax_emission")
    year = tax["type_year"].astype(int)
    base = tax[year == base_year]
    if base["node"].duplicated().any():
        raise ValueError(f"Expected one {base_year} tax_emission row per node")
    base_price = tax["node"].map(base.set_index("node")["value"])

    gdp = target_scenario.par("gdp_calibrate").pivot_table(
        index="node", columns="year", values="value"
    )
    growth = [gdp.at[n, y] / gdp.at[n, base_year] for n, y in zip(tax["node"], year)]

    new_tax = tax.assign(value=(base_price * growth).clip(upper=price_cap))[
        year > base_year
    ]
    if new_tax["value"].isna().any():
        raise ValueError(f"Nodes without a {base_year} tax_emission value")

    with target_scenario.transact("Grow carbon price with GDP, capped"):
        target_scenario.add_par("tax_emission", new_tax)

    log.info(
        "tax_emission (USD/tC) after GDP growth and cap:\n%s",
        new_tax.pivot_table(index="node", columns="type_year", values="value"),
    )

    if solve_scenario:
        target_scenario.solve(
            quiet=False, model="MESSAGE", solve_options={"scaind": "-1"}
        )

    return target_scenario