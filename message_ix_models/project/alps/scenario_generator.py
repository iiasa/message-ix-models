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

import numpy as np
import pandas as pd
import yaml
from ixmp import Platform
from message_ix import Scenario

from message_ix_models import Context
from message_ix_models.project.alps.cid_utils import (
    MAGICC_OUTPUT_DIR,
    cached_rime_prediction,
    deinterleave_seasonal,
)
from message_ix_models.project.alps.replace_water_cids import (
    generate_cooling_cid_scenario,
    prepare_water_cids,
    replace_water_availability,
)
from message_ix_models.project.alps.rime import (
    compute_expectation,
    extract_all_run_ids,
)
from message_ix_models.project.alps.timeslice import (
    duration_time,
    generate_uniform_timeslices,
    time_setup,
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
    required = ["platform_info", "starter", "output", "rime", "scenarios"]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")

    return config


def generate_synthetic_magicc_df(
    n_runs: int = 100,
    gwl: float = 1.0,
    noise_std: float = 0.01,
    years: tuple = tuple(range(1990, 2101, 5)),
) -> pd.DataFrame:
    """Generate synthetic MAGICC-style DataFrame with constant GWL.

    Creates an IAMC-format DataFrame mimicking MAGICC output but with
    constant global warming level. Used for no_climate counterfactual
    scenarios where climate is held at present-day levels.

    Parameters
    ----------
    n_runs : int
        Number of synthetic runs (default: 100)
    gwl : float
        Global warming level in °C (default: 1.0)
    noise_std : float
        Standard deviation of Gaussian noise added to GWL (default: 0.01)
    years : tuple
        Years to include in timeseries (default: 1990-2100 every 5 years)

    Returns
    -------
    pd.DataFrame
        IAMC-format DataFrame with columns: Model, Scenario, Region, Variable, Unit, {years}
    """
    rows = []
    variable = "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"

    for run_id in range(n_runs):
        row = {
            "Model": f"SYNTHETIC|run_{run_id}|",
            "Scenario": "no_climate",
            "Region": "World",
            "Variable": variable,
            "Unit": "K",
        }
        # Add year columns with constant GWL + small noise
        for year in years:
            row[str(year)] = gwl + np.random.normal(0, noise_std)
        rows.append(row)

    return pd.DataFrame(rows)


def derive_magicc_path(budget: str, model_prefix: str = "SSP_SSP2_v6.5_CID") -> Path:
    """Derive MAGICC file path from budget identifier.

    Parameters
    ----------
    budget : str or None
        Budget identifier (e.g., '600f', '850f', '1100f'), 'baseline' for SSP2 baseline
        emissions, or 'no_climate' to skip CID entirely
    model_prefix : str
        Model prefix for MAGICC filename (default: SSP_SSP2_v6.5_CID)

    Returns
    -------
    Path or None
        Path to MAGICC all_runs Excel file, or None for no_climate
    """
    if budget == "no_climate":
        return None
    elif budget is None or budget == "" or budget == "baseline":
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
    subannual = time_set - {"year"}
    print(f"   Timeslices added: {subannual}")

    return scenario


def generate_scenario(
    mp: Platform,
    starter_model: str,
    starter_scenario: str,
    output_model: str,
    output_scenario: str,
    magicc_file: Optional[Path],
    temporal_res: str,
    n_runs: int = 100,
    dry_run: bool = False,
    cid_type: str = "nexus",
) -> Optional[Scenario]:
    """Generate a single CID scenario.

    Clone starter → add timeslices (if seasonal) → run RIME → replace CID params → commit

    This is the only function that handles file I/O. All downstream functions
    receive DataFrame only.

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
    magicc_file : Optional[Path]
        Path to MAGICC all_runs Excel file, or None for synthetic no_climate (GWL=1.0C)
    temporal_res : str
        Temporal resolution: 'annual' or 'seasonal'
    n_runs : int
        Number of MAGICC runs for expectation (default: 100)
    dry_run : bool
        If True, validate but don't create scenario
    cid_type : str
        CID type: 'nexus' (water) or 'cooling' (capacity_factor)

    Returns
    -------
    Scenario or None
        Created scenario, or None if dry_run
    """
    print(f"\n{'=' * 60}")
    print(f"Generating: {output_scenario}")
    print(f"{'=' * 60}")
    print(f"  Starter: {starter_model}/{starter_scenario}")
    print(f"  MAGICC: {magicc_file.name if magicc_file else 'SYNTHETIC (GWL=1.0C)'}")
    print(f"  Temporal: {temporal_res}")
    print(f"  CID type: {cid_type}")

    # Validate file exists (unless synthetic no_climate)
    if magicc_file is not None and not magicc_file.exists():
        raise FileNotFoundError(f"MAGICC file not found: {magicc_file}")

    # Validate cid_type constraints
    if cid_type == "cooling" and temporal_res == "seasonal":
        raise ValueError("Cooling CID only supports annual temporal resolution")

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

    # Add timeslices if seasonal (nexus only, cooling is always annual)
    # Skip if starter already has h1/h2 timeslices
    if temporal_res == "seasonal" and cid_type == "nexus":
        existing_times = set(scen.set("time").tolist())
        if "h1" in existing_times and "h2" in existing_times:
            print("\n3. Timeslices already present in starter, skipping")
        else:
            print("\n3. Adding timeslices...")
            add_timeslices_to_scenario(scen, n_time=2)
            print("   Timeslices committed")

    # Load MAGICC data and extract run IDs
    print("\n4. Loading MAGICC temperature data...")
    if magicc_file is None:
        # Synthetic no_climate: constant GWL=1.0C
        print("   Generating synthetic MAGICC data (GWL=1.0C)...")
        magicc_df = generate_synthetic_magicc_df(n_runs=n_runs, gwl=1.0, noise_std=0.01)
    else:
        magicc_df = pd.read_excel(magicc_file, sheet_name="data")
    all_run_ids = extract_all_run_ids(magicc_df)
    run_ids = tuple(all_run_ids[:n_runs])
    print(f"   Using {len(run_ids)} runs for expectation")

    # Dispatch based on CID type (pass magicc_df to avoid re-reading)
    if cid_type == "nexus":
        scen_updated = _generate_nexus_cid(
            scen, magicc_df, run_ids, temporal_res, n_runs
        )
    elif cid_type == "cooling":
        scen_updated = generate_cooling_cid_scenario(scen, magicc_df, run_ids, n_runs)
    else:
        raise ValueError(f"Unknown cid_type: {cid_type}. Expected 'nexus' or 'cooling'")

    return scen_updated


def _generate_nexus_cid(
    scen: Scenario,
    magicc_df: pd.DataFrame,
    run_ids: tuple,
    temporal_res: str,
    n_runs: int,
) -> Scenario:
    """Generate nexus (water) CID scenario."""
    rime_temporal = "seasonal2step" if temporal_res == "seasonal" else "annual"

    print(f"\n5. Running RIME predictions for qtot_mean ({rime_temporal})...")
    qtot_predictions = cached_rime_prediction(
        magicc_df, run_ids, "qtot_mean", temporal_res=rime_temporal
    )
    print(f"   Got {len(qtot_predictions)} prediction sets")

    print(f"\n6. Running RIME predictions for qr ({rime_temporal})...")
    qr_predictions = cached_rime_prediction(
        magicc_df, run_ids, "qr", temporal_res=rime_temporal
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
    print(f"\n8. Preparing MESSAGE parameters ({temporal_res})...")
    if temporal_res == "annual":
        sw_data, gw_data, share_data = prepare_water_cids(
            qtot_expected, qr_expected, scen, temporal_res="annual"
        )
    else:
        qtot_dry, qtot_wet = deinterleave_seasonal(qtot_expected)
        qr_dry, qr_wet = deinterleave_seasonal(qr_expected)
        sw_data, gw_data, share_data = prepare_water_cids(
            (qtot_dry, qtot_wet), (qr_dry, qr_wet), scen, temporal_res="seasonal"
        )

    sw_new, sw_old = sw_data
    gw_new, gw_old = gw_data
    share_new, share_old = share_data
    print(f"   sw: {len(sw_new)} new, {len(sw_old)} old rows")
    print(f"   gw: {len(gw_new)} new, {len(gw_old)} old rows")
    print(f"   share: {len(share_new)} new, {len(share_old)} old rows")

    # Replace water availability
    print("\n9. Replacing water availability...")
    # Get source name from DataFrame (Scenario column contains source identifier)
    source_name = magicc_df["Scenario"].iloc[0] if "Scenario" in magicc_df.columns else "unknown"
    commit_msg = (
        f"CID water projection: {source_name}, {temporal_res}\n"
        f"RIME: n_runs={n_runs}, variables=qtot_mean,qr"
    )

    scen_updated = replace_water_availability(
        scen, sw_data, gw_data, share_data, commit_message=commit_msg
    )
    print(f"   Committed version {scen_updated.version}")

    return scen_updated


def _build_cooling_module(scen: Scenario) -> Scenario:
    """Build cooling module onto scenario if not already present.

    Checks if freshwater cooling technologies exist. If not, builds the
    water/cooling module to add them.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario to modify

    Returns
    -------
    Scenario
        Modified scenario with cooling technologies
    """
    from message_ix_models.model.water.build import main as build_water

    # Check if cooling technologies already exist
    # Cooling tech naming: {parent}__ot_fresh, {parent}__cl_fresh, etc.
    existing_cf = scen.par("capacity_factor")
    cooling_techs = existing_cf[
        existing_cf["technology"].str.contains(
            r"__(ot_fresh|cl_fresh)", regex=True, na=False
        )
    ]

    if len(cooling_techs) > 0:
        print(
            f"   Cooling technologies already present: {cooling_techs['technology'].nunique()} types"
        )
        return scen

    print("   Building cooling module...")

    # Set up context for cooling build
    context = Context.get_instance(-1)
    context.set_scenario(scen)
    # Use no climate and mid since CID replacement will happen anyway.
    context.nexus_set = "cooling"
    context.RCP = "no_climate"
    context.REL = "mid"

    # Build cooling module
    build_water(context, scen)
    print("   Cooling module built successfully")

    return scen


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
        budget_filter = set(b.strip() for b in budgets.split(","))

    # Parse temporal filter
    if temporal == "both":
        temporal_filter = {"annual", "seasonal"}
    else:
        temporal_filter = {temporal}

    # Connect to platform
    platform_name = config["platform_info"]["name"]
    jvmargs = config["platform_info"].get("jvmargs")

    # Get CID type (default: nexus for backward compatibility)
    cid_type = config.get("cid_type", "nexus")

    print("=" * 60)
    print("CID SCENARIO GENERATION")
    print("=" * 60)
    print(f"Platform: {platform_name}")
    print(f"CID type: {cid_type}")
    print(f"Starter: {config['starter']['model']}/{config['starter']['scenario']}")
    print(f"Budget filter: {budget_filter or 'all'}")
    print(f"Temporal filter: {temporal_filter}")
    print(f"Mode: {'DRY RUN' if dry_run else 'GENERATE'}")

    # Build scenario list
    scenarios_to_generate = []
    for scen_spec in config["scenarios"]:
        budget = scen_spec["budget"]
        if budget_filter and budget not in budget_filter:
            continue

        for temp_res in scen_spec["temporal"]:
            if temp_res not in temporal_filter:
                continue

            # Skip seasonal for cooling (not supported)
            if cid_type == "cooling" and temp_res == "seasonal":
                print(f"  Skipping {budget}/{temp_res}: cooling only supports annual")
                continue

            # Handle special budget values
            if budget == "no_climate":
                # No CID applied - just clone starter
                output_name = f"{cid_type}_no_climate"
            elif budget is None or budget == "" or budget == "baseline":
                # CID from baseline emissions trajectory
                # Cooling mode: no temporal suffix (only annual supported)
                if cid_type == "cooling":
                    output_name = f"{cid_type}_baseline"
                else:
                    output_name = f"{cid_type}_baseline_{temp_res}"
            else:
                output_name = config["output"]["scenario_template"].format(
                    budget=budget, temporal=temp_res
                )
            scenarios_to_generate.append(
                {
                    "budget": budget,
                    "temporal": temp_res,
                    "output_scenario": output_name,
                    "magicc_file": derive_magicc_path(budget),
                }
            )

    print(f"\nScenarios to generate: {len(scenarios_to_generate)}")
    for s in scenarios_to_generate:
        print(f"  - {s['output_scenario']}")

    if not scenarios_to_generate:
        print("No scenarios match filters")
        return []

    # Connect to platform
    mp = (
        Platform(platform_name, jvmargs=jvmargs) if jvmargs else Platform(platform_name)
    )

    # Generate scenarios
    created = []
    for spec in scenarios_to_generate:
        try:
            scen = generate_scenario(
                mp=mp,
                starter_model=config["starter"]["model"],
                starter_scenario=config["starter"]["scenario"],
                output_model=config["output"]["model"],
                output_scenario=spec["output_scenario"],
                magicc_file=spec["magicc_file"],
                temporal_res=spec["temporal"],
                n_runs=config["rime"]["n_runs"],
                cid_type=cid_type,
                dry_run=dry_run,
            )
            if scen:
                created.append(scen)
        except Exception as e:
            print(f"\nERROR generating {spec['output_scenario']}: {e}")
            raise

    print("\n" + "=" * 60)
    print(f"GENERATION COMPLETE: {len(created)} scenarios created")
    print("=" * 60)

    return created
