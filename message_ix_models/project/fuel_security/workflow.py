"""
Workflow for developing baseline scenarios and bilateralizing them for fuel security project
"""
import logging
import os
from ixmp import Platform
import message_ix

# Import tools
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.fuel_security.liquefaction_calibration import *
from message_ix_models.project.fuel_security.adjust_reexports import *
from message_ix_models.project.fuel_security.bilateralize_scenario import *
from message_ix_models.project.fuel_security.FSU_restriction import *
from message_ix_models.project.fuel_security.MEA_conflict import run_mea_conflict_scenario
from message_ix_models.project.fuel_security.NAM_boost import run_nam_boost_scenario

from message_ix_models import Context
from message_ix_models.util import private_data_path
from message_ix_models.workflow import Workflow

from message_ix_models.project.fuel_security.policy import (
    add_NPi2030,
    add_NDC2030,
    add_gdp_price_growth,
)

log = logging.getLogger(__name__)


def _set_default(context, scenario):
    """Mark `scenario` as the default version for its (model, scenario) name.

    ixmp's Scenario.clone() does not do this automatically for a clone into a new
    (model, scenario) name pair (see ixmp.core.scenario.Scenario.clone docstring).
    Without it, downstream code that loads "baseline_DEFAULT" by name only (e.g.
    message_data.model.scenario_runner.make_scenario_runner) silently resolves to
    a stale default version instead of the scenario produced by this workflow run.
    """
    scenario.set_as_default()
    return scenario

def _bilateralize(context, scenario):
    """Bilateralize trade technologies on the scenario produced upstream.

    Policy scenarios with a first model year after 2030 take their 2030 trade history
    from fuel_security/baseline_bilateral, so the "Baseline bilateralized" step must
    have been run (and solved) first.
    """
    history_scenario = None
    if scenario.firstmodelyear > 2030:
        history_scenario = message_ix.Scenario(
            scenario.platform, model="fuel_security", scenario="baseline_bilateral"
        )
    return bilateralize_scenario(
        project_name="fuel_security",
        config_name="config.yaml",
        scenario=scenario,
        target_scenario=f"{scenario.scenario}_bilateral",
        history_scenario=history_scenario,
        last_model_year=2060,
        lt_contracts=True,  # long-term gas contracts as bound_activity_lo
        contract_share=0.9,
    )

def _FSU_restriction(context, scenario, friction_endyear,
                     relax_lng_growth=None, relax_lng_initial=None,
                     relax_lng_decline=None):
    """Run FSU restriction scenario."""
    return run_friction_scenario(
        base_scenario=scenario,
        friction_endyear=friction_endyear,
        relax_lng_growth=relax_lng_growth,
        relax_lng_initial=relax_lng_initial,
        relax_lng_decline=relax_lng_decline,
    )

def _mea_conflict(context, scenario, conf_level):
    """Apply an MEA conflict shock to the scenario produced upstream."""
    return run_mea_conflict_scenario(
        base_scenario=scenario,
        conf_level=conf_level
    )

def _nam_boost(context, scenario, bound_level_ej):
    """Apply a NAM export boost to the scenario produced upstream."""
    return run_nam_boost_scenario(
        base_scenario=scenario,
        bound_level_ej=bound_level_ej
    )

# Generate workflow
def generate(context: Context) -> Workflow:
    """
    Generate workflow for fuel security project
    """
    wf = Workflow(context)

    # Context attributes
    context.ssp = "SSP2"
    context.model.regions = "R12"
    
    context.run_reporting_only = False
    context.policy_data_file = "fuel_security_policy_data.xlsx"
    context.policy_config_path = ("projects", "fuel_security", "config.yaml")
    context.region_id = "R12"
    
    # Workflow steps
    wf.add_step(
        "Base",
        None,
        target = "ixmp://ixmp-dev/SSP_SSP2_v6.6/baseline" 
    ) # Load baseline scenario from SSP_SSP2_v6.6

    wf.add_step(
        "INDC2030i_forever",
        None,
        target = "ixmp://ixmp-dev/SSP_SSP2_v6.6/INDC2030i_forever",
    ) # Load INDC2030i_forever scenario from SSP_SSP2_v6.6

    wf.add_step(
        "Base cloned",
        "Base",
        _set_default,
        target = "ixmp://localdb/fuel_security/baseline_DEFAULT", # This has to be named baseline_DEFAULT to match policy tool requirement
        clone = dict(keep_solution = True)
    ) # Clone baseline scenario to fuel_security/baseline_DEFAULT

    wf.add_step(
        "Clone INDC2030i_forever",
        "INDC2030i_forever",
        _set_default,
        target = "ixmp://localdb/fuel_security/INDC2030i_forever",
        clone = dict(keep_solution = True)
    ) # Clone INDC2030i_forever to fuel_security/INDC2030i_forever

    wf.add_step(
        "INDC2030i_forever_gdp",
        "Clone INDC2030i_forever",
        add_gdp_price_growth,
        target = "ixmp://localdb/fuel_security/INDC2030i_forever_gdp",
    ) # Grow INDC2030i_forever's flat carbon price with GDP, capped at 300 USD/tCO2

    wf.add_step(
        "Add and solve NPi2030",
        "Base cloned",
        add_NPi2030,
        target = "ixmp://localdb/fuel_security/NPi2030"
    ) # Add and solve NPi2030 onto baseline_DEFAULT to create fuel_security/NPi2030

    # Load the (solved) base scenarios already on localdb. Bilateralize steps start from
    # these instead of from the clone/policy steps above, so a bilateralize run only
    # opens localdb: it does not connect to ixmp-dev or re-clone/re-solve the base. This
    # also means the JVM is started by localdb's platform config, so its "jvmargs" (Java
    # heap) apply when reading the bilateral solution back into the database.
    # Run the step that creates each base (e.g. "Base cloned") once beforehand.
    for name, scen in (
        ("Base local", "baseline_DEFAULT"),
        ("NPi2030 local", "NPi2030"),
        ("INDC2030i_forever local", "INDC2030i_forever"),
        ("INDC2030i_forever_gdp local", "INDC2030i_forever_gdp"),
    ):
        wf.add_step(name, None, target=f"ixmp://localdb/fuel_security/{scen}")

    wf.add_step(
        "Baseline bilateralized",
        "Base local",
        _bilateralize,
        target="ixmp://localdb/fuel_security/baseline_bilateral"
    ) # Bilateralize fuel_security/baseline_DEFAULT to create fuel_security/baseline_bilateral

    wf.add_step(
        "NPi2030 bilateralized",
        "NPi2030 local",
        _bilateralize,
        target="ixmp://localdb/fuel_security/NPi2030_bilateral"
    ) # Bilateralize NPi2030 to create fuel_security/NPi2030_bilateral

    wf.add_step(
        "INDC2030i_forever bilateralized",
        "INDC2030i_forever local",
        _bilateralize,
        target="ixmp://localdb/fuel_security/INDC2030i_forever_bilateral"
    ) # Bilateralize INDC2030i_forever to create fuel_security/INDC2030i_forever_bilateral

    wf.add_step(
        "INDC2030i_forever_gdp bilateralized",
        "INDC2030i_forever_gdp local",
        _bilateralize,
        target="ixmp://localdb/fuel_security/INDC2030i_forever_gdp_bilateral"
    ) # Bilateralize INDC2030i_forever_gdp to create fuel_security/INDC2030i_forever_gdp_bilateral
        
    # FSU restriction sensitivities on each bilateralized base (which already carries
    # the long-term contract bounds; the restriction supersedes them where it applies).
    # run_friction_scenario names its output <name>_FSU<year>.
    bilat_bases = [
        ("Baseline bilateralized", "Baseline", "baseline"),
        ("NPi2030 bilateralized", "NPi2030", "NPi2030"),
        ("INDC2030i_forever bilateralized", "INDC2030i_forever", "INDC2030i_forever"),
        ("INDC2030i_forever_gdp bilateralized", "INDC2030i_forever_gdp", "INDC2030i_forever_gdp"),
    ]
    fsu_years = [2100, 2040]
    for bilat_step, label, name in bilat_bases:
        for year in fsu_years:
            wf.add_step(
                f"{label} - FSU{year}",
                bilat_step,
                _FSU_restriction,
                friction_endyear=year,
                target=f"ixmp://localdb/fuel_security/{name}_FSU{year}",
            ) # Run FSU{year} restriction on {name}_bilateral

    # Robustness check: FSU2040 with the WEU/EEU LNG import decline floor loosened
    # after 2030, so a 2030 LNG surge doesn't lock in imports later. NPi2030 only:
    # baseline_bilateral has no growth_activity_lo on LNG_shipped_imp to loosen.
    wf.add_step(
        "NPi2030 - FSU2040 - LNGfloor",
        "NPi2030 bilateralized",
        _FSU_restriction,
        friction_endyear=2040,
        relax_lng_decline=-0.3,
        target="ixmp://localdb/fuel_security/NPi2030_FSU2040_LNGfloor",
    ) # Run FSU2040 with relaxed LNG import decline on NPi2030_bilateral

    # MEA conflict shock sensitivities: applied to each bilateralized base and
    # each FSU-restricted variant, at 6 severity levels (1.0 = no shock)
    mea_bases = []
    for bilat_step, label, name in bilat_bases:
        mea_bases.append((bilat_step, f"{name}_bilateral"))
        mea_bases += [(f"{label} - FSU{year}", f"{name}_FSU{year}") for year in fsu_years]
    mea_levels = [1.0, 0.9, 0.8, 0.75, 0.5, 0.25]

    for base_step, base_name in mea_bases:
        for level in mea_levels:
            wf.add_step(
                f"{base_step} - MEA{level}",
                base_step,
                _mea_conflict,
                conf_level=level,
                target=f"ixmp://localdb/fuel_security/{base_name}_MEACON_{level}"
            ) # Apply MEA conflict shock (level={level}) onto {base_name}

    # NAM export boost sensitivities: applied to each bilateralized base and
    # each FSU-restricted variant, at 5 boost levels (EJ of additional NAM exports)
    nam_bases = mea_bases  # same base scenarios as the MEA conflict shocks
    nam_levels = [30, 25, 20, 15, 10]

    for base_step, base_name in nam_bases:
        for level in nam_levels:
            wf.add_step(
                f"{base_step} - NAM{level}EJ",
                base_step,
                _nam_boost,
                bound_level_ej=level,
                target=f"ixmp://localdb/fuel_security/{base_name}_NAM{level}EJ"
            ) # Apply NAM export boost (level={level}EJ) onto {base_name}

    return wf
