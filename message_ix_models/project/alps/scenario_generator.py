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

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yaml
from ixmp import Platform
from message_ix import Scenario

from message_ix_models import Context
from message_ix_models.project.alps.cid_utils import (
    cached_rime_prediction,
    get_magicc_file,
)
from message_ix_models.project.alps.replace_building_cids import (
    generate_building_cid_scenario,
)
from message_ix_models.project.alps.replace_cooling_cids import (
    generate_cooling_cid_scenario,
)
from message_ix_models.project.alps.replace_water_cids import (
    prepare_water_cids,
    replace_water_availability,
)
from message_ix_models.project.alps.rime import (
    extract_all_run_ids,
    get_gmt_ensemble,
    load_basin_mapping,
)
from message_ix_models.project.alps.timeslice import (
    duration_time,
    generate_timeslices,
    time_setup,
)

log = logging.getLogger(__name__)


def _ndarray_to_dataframe(
    arr: np.ndarray,
    years: np.ndarray,
    basin_mapping: pd.DataFrame = None,
) -> pd.DataFrame:
    """Wrap RIME ndarray prediction in DataFrame with basin metadata.

    Parameters
    ----------
    arr : np.ndarray
        Prediction array, shape (217, n_years)
    years : np.ndarray
        Year labels for columns
    basin_mapping : pd.DataFrame, optional
        Basin mapping with BCU_name. If None, loads from rime module.

    Returns
    -------
    pd.DataFrame
        DataFrame with BCU_name column + integer year columns
    """
    if basin_mapping is None:
        basin_mapping = load_basin_mapping()

    df = pd.DataFrame(arr, columns=years.astype(int))
    df.insert(0, "BCU_name", basin_mapping["BCU_name"].values)
    return df


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


def add_timeslices_to_scenario(
    scenario: Scenario, n_time: int = 2, split: list = None
) -> Scenario:
    """Add timeslice structure to scenario.

    Parameters
    ----------
    scenario : Scenario
        MESSAGE scenario to modify (must be checked out)
    n_time : int
        Number of timeslices (default: 2 for seasonal)
    split : list of int, optional
        Number of months per timeslice. Must sum to 12.
        If None, uses uniform split. Example: [10, 2] for optimal dry/wet.

    Returns
    -------
    Scenario
        Modified scenario with timeslices
    """
    log.info(f"Adding {n_time} timeslices to scenario with split={split}...")

    df_time = generate_timeslices(n_time, split)
    time_setup(scenario, df_time)
    duration_time(scenario, df_time)

    time_set = set(scenario.set("time").tolist())
    subannual = time_set - {"year"}
    log.info(f"Timeslices added: {subannual}")

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
        Path to MAGICC all_runs Excel file, or None for no_climate (skip CID)
    temporal_res : str
        Temporal resolution: 'annual' or 'seasonal'
    n_runs : int
        Number of MAGICC runs for expectation (default: 100)
    dry_run : bool
        If True, validate but don't create scenario
    cid_type : str
        CID type: 'nexus' (water), 'cooling', or 'buildings'

    Returns
    -------
    Scenario or None
        Created scenario, or None if dry_run
    """
    is_no_climate = magicc_file is None

    log.info("=" * 60)
    log.info(f"Generating: {output_scenario}")
    log.info("=" * 60)
    log.info(f"Starter: {starter_model}/{starter_scenario}")
    log.info(f"MAGICC: {'no_climate (skip CID)' if is_no_climate else magicc_file.name}")
    log.info(f"Temporal: {temporal_res}")
    log.info(f"CID type: {cid_type}")

    if magicc_file and not magicc_file.exists():
        raise FileNotFoundError(f"MAGICC file not found: {magicc_file}")

    match cid_type, temporal_res:
        case ("cooling" | "buildings", "seasonal"):
            raise ValueError(f"{cid_type} CID only supports annual temporal resolution")

    if dry_run:
        log.info("[DRY RUN] Validation passed")
        return None

    # Clone starter
    log.info("1. Loading starter scenario...")
    starter = Scenario(mp, starter_model, starter_scenario)
    log.info(f"Loaded version {starter.version}")

    log.info("2. Cloning scenario...")
    scen = starter.clone(
        model=output_model,
        scenario=output_scenario,
        keep_solution=False,
        shift_first_model_year=None,
    )
    log.info(f"Created {scen.model}/{scen.scenario} version {scen.version}")

    # no_climate: just clone, no CID application
    if is_no_climate:
        scen.set_as_default()
        log.info("no_climate: skipping CID, returning cloned scenario")
        return scen

    # Add timeslices if seasonal nexus
    if temporal_res == "seasonal" and cid_type == "nexus":
        existing_times = set(scen.set("time").tolist())
        if "h1" in existing_times and "h2" in existing_times:
            log.info("3. Timeslices already present in starter, skipping")
        else:
            log.info("3. Adding timeslices...")
            add_timeslices_to_scenario(scen, n_time=2)
            log.info("Timeslices committed")

    # Load MAGICC data
    log.info("4. Loading MAGICC temperature data...")
    magicc_df = pd.read_excel(magicc_file, sheet_name="data")
    all_run_ids = extract_all_run_ids(magicc_df)
    run_ids = tuple(all_run_ids[:n_runs])
    log.info(f"Using {len(run_ids)} runs for expectation")

    # Dispatch by CID type
    match cid_type:
        case "nexus":
            scen_updated = _generate_nexus_cid(
                scen, magicc_df, run_ids, temporal_res, n_runs
            )
        case "cooling":
            scen_updated = generate_cooling_cid_scenario(scen, magicc_df, run_ids, n_runs)
        case "buildings":
            scen_updated = generate_building_cid_scenario(
                scen, magicc_df, n_runs=n_runs, coeff_scenario="S1"
            )
        case _:
            raise ValueError(f"Unknown cid_type: {cid_type}")

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

    # Get years from MAGICC for DataFrame construction
    _, years = get_gmt_ensemble(magicc_df, list(run_ids)[:1])
    basin_mapping = load_basin_mapping()

    log.info(f"5. Running RIME predictions for qtot_mean ({rime_temporal})...")
    qtot_arr = cached_rime_prediction(
        magicc_df, run_ids, "qtot_mean", temporal_res=rime_temporal
    )

    log.info(f"6. Running RIME predictions for qr ({rime_temporal})...")
    qr_arr = cached_rime_prediction(
        magicc_df, run_ids, "qr", temporal_res=rime_temporal
    )

    # Prepare MESSAGE parameters
    log.info(f"7. Preparing MESSAGE parameters ({temporal_res})...")
    if temporal_res == "annual":
        qtot_df = _ndarray_to_dataframe(qtot_arr, years, basin_mapping)
        qr_df = _ndarray_to_dataframe(qr_arr, years, basin_mapping)
        log.debug(f"qtot shape: {qtot_df.shape}, qr shape: {qr_df.shape}")
        sw_data, gw_data, share_data = prepare_water_cids(
            qtot_df, qr_df, scen, temporal_res="annual"
        )
    else:
        # Seasonal: cached_rime_prediction returns (dry, wet) tuple
        qtot_dry_arr, qtot_wet_arr = qtot_arr
        qr_dry_arr, qr_wet_arr = qr_arr
        qtot_dry = _ndarray_to_dataframe(qtot_dry_arr, years, basin_mapping)
        qtot_wet = _ndarray_to_dataframe(qtot_wet_arr, years, basin_mapping)
        qr_dry = _ndarray_to_dataframe(qr_dry_arr, years, basin_mapping)
        qr_wet = _ndarray_to_dataframe(qr_wet_arr, years, basin_mapping)
        log.debug(f"qtot_dry shape: {qtot_dry.shape}, qr_dry shape: {qr_dry.shape}")

        context = Context.get_instance(-1)
        timeslice_months = context.timeslice_months
        sw_data, gw_data, share_data = prepare_water_cids(
            (qtot_dry, qtot_wet),
            (qr_dry, qr_wet),
            scen,
            temporal_res="seasonal",
            timeslice_months=timeslice_months,
        )

    sw_new, sw_old = sw_data
    gw_new, gw_old = gw_data
    share_new, share_old = share_data
    log.info(f"sw: {len(sw_new)} new, {len(sw_old)} old rows")
    log.info(f"gw: {len(gw_new)} new, {len(gw_old)} old rows")
    log.info(f"share: {len(share_new)} new, {len(share_old)} old rows")

    # Replace water availability
    log.info("9. Replacing water availability...")
    # Get source name from DataFrame (Scenario column contains source identifier)
    source_name = magicc_df["Scenario"].iloc[0] if "Scenario" in magicc_df.columns else "unknown"
    commit_msg = (
        f"CID water projection: {source_name}, {temporal_res}\n"
        f"RIME: n_runs={n_runs}, variables=qtot_mean,qr"
    )

    scen_updated = replace_water_availability(
        scen, sw_data, gw_data, share_data, commit_message=commit_msg
    )
    log.info(f"Committed version {scen_updated.version}")

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
        log.info(
            f"Cooling technologies already present: {cooling_techs['technology'].nunique()} types"
        )
        return scen

    log.info("Building cooling module...")

    # Set up context for cooling build
    context = Context.get_instance(-1)
    context.set_scenario(scen)
    # Use no climate and mid since CID replacement will happen anyway.
    context.nexus_set = "cooling"
    context.RCP = "no_climate"
    context.REL = "mid"

    # Build cooling module
    build_water(context, scen)
    log.info("Cooling module built successfully")

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

    log.info("=" * 60)
    log.info("CID SCENARIO GENERATION")
    log.info("=" * 60)
    log.info(f"Platform: {platform_name}")
    log.info(f"CID type: {cid_type}")
    log.info(f"Starter: {config['starter']['model']}/{config['starter']['scenario']}")
    log.info(f"Budget filter: {budget_filter or 'all'}")
    log.info(f"Temporal filter: {temporal_filter}")
    log.info(f"Mode: {'DRY RUN' if dry_run else 'GENERATE'}")

    # Build scenario list
    scenarios_to_generate = []
    for scen_spec in config["scenarios"]:
        budget = scen_spec["budget"]
        if budget_filter and budget not in budget_filter:
            continue

        for temp_res in scen_spec["temporal"]:
            if temp_res not in temporal_filter:
                continue

            # Skip seasonal for cooling and buildings (not supported)
            if cid_type in ("cooling", "buildings") and temp_res == "seasonal":
                log.debug(f"Skipping {budget}/{temp_res}: {cid_type} only supports annual")
                continue

            # Derive output name and MAGICC file from budget
            magicc_model = config["output"]["model"]
            match budget:
                case "no_climate":
                    output_name = f"{cid_type}_no_climate"
                    magicc_file = None
                case None | "" | "baseline":
                    suffix = "" if cid_type in ("cooling", "buildings") else f"_{temp_res}"
                    output_name = f"{cid_type}_baseline{suffix}"
                    magicc_file = get_magicc_file(magicc_model, "baseline")
                case _:
                    output_name = config["output"]["scenario_template"].format(
                        budget=budget, temporal=temp_res
                    )
                    magicc_file = get_magicc_file(magicc_model, f"baseline_{budget}")

            scenarios_to_generate.append(
                {
                    "budget": budget,
                    "temporal": temp_res,
                    "output_scenario": output_name,
                    "magicc_file": magicc_file,
                }
            )

    log.info(f"Scenarios to generate: {len(scenarios_to_generate)}")
    for s in scenarios_to_generate:
        log.info(f"  - {s['output_scenario']}")

    if not scenarios_to_generate:
        log.warning("No scenarios match filters")
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
            log.error(f"ERROR generating {spec['output_scenario']}: {e}")
            raise

    log.info("=" * 60)
    log.info(f"GENERATION COMPLETE: {len(created)} scenarios created")
    log.info("=" * 60)

    return created
