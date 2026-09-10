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