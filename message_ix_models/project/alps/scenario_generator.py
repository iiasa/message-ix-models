"""YAML-driven CID scenario batch generation.

Generate multiple MESSAGE scenarios with RIME water projections from a
configuration file. Each scenario is created by:
1. Clone starter scenario without solution
2. Add timeslices (if seasonal temporal resolution)
3. Run RIME predictions from MAGICC temperature
4. Replace water availability parameters
5. Commit

Usage:
    mix-models alps scen-gen --config scenario_config.yaml
    mix-models alps scen-gen --config scenario_config.yaml --budgets 600f --temporal annual
"""

from pathlib import Path
from typing import Optional
import yaml
import numpy as np
import pandas as pd

from ixmp import Platform
from message_ix import Scenario

from message_ix_models.util import package_data_path
from message_ix_models.project.alps.cid_utils import (
    MAGICC_OUTPUT_DIR,
    cached_rime_prediction,
    deinterleave_seasonal,
)
from message_ix_models.project.alps.rime import (
    compute_expectation,
    extract_all_run_ids,
)
from message_ix_models.project.alps.replace_water_cids import (
    prepare_demand_parameter,
    prepare_groundwater_share,
    replace_water_availability,
)
from message_ix_models.project.alps.timeslice import (
    generate_uniform_timeslices,
    time_setup,
    duration_time,
)


def load_config(config_path: Path) -> dict:
    """Load and validate scenario generation config from YAML.

    Parameters
    ----------
    config_path : Path
        Path to YAML configuration file

    Returns
    -------
    dict
        Validated configuration dictionary
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Validate required keys
    required = ['platform_info', 'starter', 'output', 'rime', 'scenarios']
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")

    return config


def derive_magicc_path(budget: str, model_prefix: str = "SSP_SSP2_v6.5_CID") -> Path:
    """Derive MAGICC file path from budget identifier.

    Parameters
    ----------
    budget : str or None
        Budget identifier (e.g., '600f', '850f', '1100f'), or None for baseline
    model_prefix : str
        Model prefix for MAGICC filename (default: SSP_SSP2_v6.5_CID)

    Returns
    -------
    Path
        Path to MAGICC all_runs Excel file
    """
    if budget is None or budget == "":
        filename = f"{model_prefix}_baseline_magicc_all_runs.xlsx"
    else:
        filename = f"{model_prefix}_baseline_{budget}_magicc_all_runs.xlsx"
    return MAGICC_OUTPUT_DIR / filename


def add_timeslices_to_scenario(scenario: Scenario, n_time: int = 2) -> Scenario:
    """Add timeslice structure to scenario.

    Creates h1, h2 timeslices with uniform duration (0.5 each).

    Parameters
    ----------
    scenario : Scenario
        MESSAGE scenario to modify (must be checked out)
    n_time : int
        Number of timeslices (default: 2 for seasonal)

    Returns
    -------
    Scenario
        Modified scenario with timeslices
    """
    print(f"   Adding {n_time} timeslices to scenario...")

    df_time = generate_uniform_timeslices(n_time)
    time_setup(scenario, df_time)
    duration_time(scenario, df_time)

    time_set = set(scenario.set("time").tolist())
    subannual = time_set - {'year'}
    print(f"   Timeslices added: {subannual}")

    return scenario


def generate_scenario(
    mp: Platform,
    starter_model: str,
    starter_scenario: str,
    output_model: str,
    output_scenario: str,
    magicc_file: Path,
    temporal_res: str,
    n_runs: int = 100,
    dry_run: bool = False,
) -> Optional[Scenario]:
    """Generate a single CID scenario.

    Clone starter → add timeslices (if seasonal) → run RIME → replace water params → commit

    Parameters
    ----------
    mp : Platform
        ixmp platform connection
    starter_model : str
        Model name of starter scenario
    starter_scenario : str
        Scenario name of starter scenario
    output_model : str
        Model name for output scenario
    output_scenario : str
        Scenario name for output scenario
    magicc_file : Path
        Path to MAGICC all_runs Excel file
    temporal_res : str
        Temporal resolution: 'annual' or 'seasonal'
    n_runs : int
        Number of MAGICC runs for expectation (default: 100)
    dry_run : bool
        If True, validate but don't create scenario

    Returns
    -------
    Scenario or None
        Created scenario, or None if dry_run
    """
    print(f"\n{'='*60}")
    print(f"Generating: {output_scenario}")
    print(f"{'='*60}")
    print(f"  Starter: {starter_model}/{starter_scenario}")
    print(f"  MAGICC: {magicc_file.name}")
    print(f"  Temporal: {temporal_res}")

    if not magicc_file.exists():
        raise FileNotFoundError(f"MAGICC file not found: {magicc_file}")

    if dry_run:
        print("  [DRY RUN] Validation passed")
        return None

    # Load starter scenario
    print("\n1. Loading starter scenario...")
    starter = Scenario(mp, starter_model, starter_scenario)
    print(f"   Loaded version {starter.version}")

    # Clone without solution
    print("\n2. Cloning scenario...")
    scen = starter.clone(
        model=output_model,
        scenario=output_scenario,
        keep_solution=False,
        shift_first_model_year=None,
    )
    print(f"   Created {scen.model}/{scen.scenario} version {scen.version}")

    # Add timeslices if seasonal (time_setup/duration_time use transact internally)
    if temporal_res == "seasonal":
        print("\n3. Adding timeslices...")
        add_timeslices_to_scenario(scen, n_time=2)
        print("   Timeslices committed")

    # Load MAGICC data and extract run IDs
    print("\n4. Loading MAGICC temperature data...")
    magicc_df = pd.read_excel(magicc_file, sheet_name="data")
    all_run_ids = extract_all_run_ids(magicc_df)
    run_ids = tuple(all_run_ids[:n_runs])
    print(f"   Using {len(run_ids)} runs for expectation")

    # Run RIME predictions
    rime_temporal = "seasonal2step" if temporal_res == "seasonal" else "annual"

    print(f"\n5. Running RIME predictions for qtot_mean ({rime_temporal})...")
    qtot_predictions = cached_rime_prediction(
        magicc_file, run_ids, "qtot_mean", temporal_res=rime_temporal
    )
    print(f"   Got {len(qtot_predictions)} prediction sets")

    print(f"\n6. Running RIME predictions for qr ({rime_temporal})...")
    qr_predictions = cached_rime_prediction(
        magicc_file, run_ids, "qr", temporal_res=rime_temporal
    )
    print(f"   Got {len(qr_predictions)} prediction sets")

    # Compute expectations
    print("\n7. Computing expectations...")
    qtot_expected = compute_expectation(
        qtot_predictions, run_ids=np.array(list(run_ids)), weights=None
    )
    qr_expected = compute_expectation(
        qr_predictions, run_ids=np.array(list(run_ids)), weights=None
    )
    print(f"   qtot shape: {qtot_expected.shape}, qr shape: {qr_expected.shape}")

    # Prepare MESSAGE parameters
    if temporal_res == "annual":
        print("\n8. Preparing MESSAGE parameters (annual)...")
        sw_demand = prepare_demand_parameter(
            qtot_expected, "surfacewater_basin", temporal_res="annual"
        )
        gw_demand = prepare_demand_parameter(
            qr_expected, "groundwater_basin", temporal_res="annual"
        )
        gw_share = prepare_groundwater_share(
            qtot_expected, qr_expected, temporal_res="annual"
        )
    else:
        print("\n8. Preparing MESSAGE parameters (seasonal)...")
        # De-interleave seasonal data
        qtot_dry, qtot_wet = deinterleave_seasonal(qtot_expected)
        qr_dry, qr_wet = deinterleave_seasonal(qr_expected)

        sw_demand = prepare_demand_parameter(
            (qtot_dry, qtot_wet), "surfacewater_basin", temporal_res="seasonal"
        )
        gw_demand = prepare_demand_parameter(
            (qr_dry, qr_wet), "groundwater_basin", temporal_res="seasonal"
        )
        gw_share = prepare_groundwater_share(
            (qtot_dry, qtot_wet), (qr_dry, qr_wet), temporal_res="seasonal"
        )

    print(f"   sw_demand: {len(sw_demand)} rows")
    print(f"   gw_demand: {len(gw_demand)} rows")
    print(f"   gw_share: {len(gw_share)} rows")

    # Replace water availability
    print("\n9. Replacing water availability...")
    commit_msg = (
        f"CID water projection: {magicc_file.stem.split('_')[-3]}f, {temporal_res}\n"
        f"MAGICC: {magicc_file.name}\n"
        f"RIME: n_runs={n_runs}, variables=qtot_mean,qr"
    )

    scen_updated = replace_water_availability(
        scen, sw_demand, gw_demand, gw_share, commit_message=commit_msg
    )
    print(f"   Committed version {scen_updated.version}")

    return scen_updated


def generate_all(
    config: dict,
    budgets: Optional[str] = None,
    temporal: str = "both",
    dry_run: bool = False,
) -> list:
    """Generate all scenarios from configuration.

    Parameters
    ----------
    config : dict
        Configuration from load_config()
    budgets : str, optional
        Comma-separated budget filter (e.g., '600f,850f')
    temporal : str
        Temporal filter: 'annual', 'seasonal', or 'both' (default: 'both')
    dry_run : bool
        If True, validate but don't create scenarios

    Returns
    -------
    list
        List of created scenarios (or empty list if dry_run)
    """
    # Parse budget filter
    budget_filter = None
    if budgets:
        budget_filter = set(b.strip() for b in budgets.split(','))

    # Parse temporal filter
    if temporal == "both":
        temporal_filter = {"annual", "seasonal"}
    else:
        temporal_filter = {temporal}

    # Connect to platform
    platform_name = config['platform_info']['name']
    jvmargs = config['platform_info'].get('jvmargs')

    print("="*60)
    print("CID SCENARIO GENERATION")
    print("="*60)
    print(f"Platform: {platform_name}")
    print(f"Starter: {config['starter']['model']}/{config['starter']['scenario']}")
    print(f"Budget filter: {budget_filter or 'all'}")
    print(f"Temporal filter: {temporal_filter}")
    print(f"Mode: {'DRY RUN' if dry_run else 'GENERATE'}")

    # Build scenario list
    scenarios_to_generate = []
    for scen_spec in config['scenarios']:
        budget = scen_spec['budget']
        if budget_filter and budget not in budget_filter:
            continue

        for temp_res in scen_spec['temporal']:
            if temp_res not in temporal_filter:
                continue

            # Handle null budget (baseline counterfactual)
            if budget is None or budget == "":
                output_name = f"nexus_baseline_{temp_res}"
            else:
                output_name = config['output']['scenario_template'].format(
                    budget=budget, temporal=temp_res
                )
            scenarios_to_generate.append({
                'budget': budget,
                'temporal': temp_res,
                'output_scenario': output_name,
                'magicc_file': derive_magicc_path(budget),
            })

    print(f"\nScenarios to generate: {len(scenarios_to_generate)}")
    for s in scenarios_to_generate:
        print(f"  - {s['output_scenario']}")

    if not scenarios_to_generate:
        print("No scenarios match filters")
        return []

    # Connect to platform
    mp = Platform(platform_name, jvmargs=jvmargs) if jvmargs else Platform(platform_name)

    # Generate scenarios
    created = []
    for spec in scenarios_to_generate:
        try:
            scen = generate_scenario(
                mp=mp,
                starter_model=config['starter']['model'],
                starter_scenario=config['starter']['scenario'],
                output_model=config['output']['model'],
                output_scenario=spec['output_scenario'],
                magicc_file=spec['magicc_file'],
                temporal_res=spec['temporal'],
                n_runs=config['rime']['n_runs'],
                dry_run=dry_run,
            )
            if scen:
                created.append(scen)
        except Exception as e:
            print(f"\nERROR generating {spec['output_scenario']}: {e}")
            raise

    print("\n" + "="*60)
    print(f"GENERATION COMPLETE: {len(created)} scenarios created")
    print("="*60)

    return created
