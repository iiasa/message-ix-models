"""
Build MESSAGEix-Nexus scenario with subannual timeslices.

This script implements a full pipeline for creating a water-energy nexus model
with subannual temporal resolution:
1. Load base annual scenario
2. Add subannual timeslices
3. Validate timesliced scenario
4. Build nexus module on timesliced scenario
5. Solve final subannual nexus model
"""

import logging
import sys

from message_ix_models import Context
from message_ix_models.model.water.build import main as build_nexus
from message_ix_models.model.water.cli import water_ini
from message_ix_models.project.alps.timeslice import add_timeslices

# Configure logging for SLURM - force unbuffered output to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.flush = sys.stdout.flush

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[handler],
    force=True
)
log = logging.getLogger(__name__)

# ========== CONFIGURATION ==========
MODEL = "MESSAGE_GLOBIOM_SSP2_v6.1"
BASE_SCEN = "baseline"
N_TIME = 2  # 2 seasonal timeslices
REGIONS = "R12"

# Nexus configuration
RCP = "7p0"
SDG = "baseline"
REL = "low"
SSP = "SSP2"

# Basin filtering for reduced nexus
REDUCED_BASIN = True
FILTER_LIST = [
    "10|SAS",
    "115|SAS",
    "124|SAS",
    "12|SAS",
    "141|SAS",
    "148|SAS",
    "15|SAS",
    "24|SAS",
    "30|SAS",
    "50|SAS",
    "53|SAS",
    "65|SAS",
    "66|SAS",
    "67|SAS",
    "70|SAS",
]

# Timeslice configuration
REMOVE_COOLING = True  # Remove cooling techs before adding timeslices (water module adds them back)

# Solve configuration
SOLVE_OPTIONS = {
    "lpmethod": "4",
    "scaind": "-1",
    "threads": "16",
    "iis": "1"
}

# Pipeline control
VALIDATE_TIMESLICE = True  # Set to True to solve timesliced scenario before adding nexus
SOLVE_FINAL = True  # Set to False to skip final solve

def main():
    """Execute the full subannual nexus pipeline."""

    # ========== STEP 1: Load Base Scenario ==========
    log.info("=" * 80)
    log.info("STEP 1: Loading base scenario")
    log.info("=" * 80)

    ctx = Context()
    ctx.handle_cli_args(url=f"ixmp://ixmp_dev/{MODEL}/{BASE_SCEN}")

    sc_base = ctx.get_scenario()
    log.info(f"Loaded base scenario: {sc_base.model}/{sc_base.scenario} v{sc_base.version}")

    # ========== STEP 2: Clone and Add Timeslices ==========
    log.info("=" * 80)
    log.info(f"STEP 2: Adding {N_TIME} monthly timeslices")
    log.info("=" * 80)

    sc_timeslice = sc_base.clone(
        model=MODEL,
        scenario=f"{BASE_SCEN}_t{N_TIME}",
        keep_solution=False
    )
    log.info(f"Cloned to: {sc_timeslice.model}/{sc_timeslice.scenario} v{sc_timeslice.version}")

    # Add timeslices
    ctx.regions = REGIONS
    sc_timeslice = add_timeslices(
        scenario=sc_timeslice,
        context=ctx,
        n_time=N_TIME,
        remove_cooling_tec=REMOVE_COOLING,
    )

    log.info(f"Timeslices added successfully")

    # Verify time structure
    times = sc_timeslice.set("time")
    log.info(f"Time slices in scenario: {list(times)}")

    # ========== STEP 3: Solve Timesliced Scenario ==========
    log.info("=" * 80)
    log.info("STEP 3: Solving timesliced scenario")
    log.info("=" * 80)

    if VALIDATE_TIMESLICE:
        log.info("Solving timesliced scenario...")
        sc_timeslice.set_as_default()
        try:
            sc_timeslice.solve(solve_options=SOLVE_OPTIONS)
            log.info("Timesliced scenario solved successfully!")
        except Exception as e:
            log.error(f"Failed to solve timesliced scenario: {e}")
            raise
    else:
        log.info("Skipping validation solve (VALIDATE_TIMESLICE=False)")

    # ========== STEP 4: Initialize Water Context ==========
    log.info("=" * 80)
    log.info("STEP 4: Initializing water/nexus context")
    log.info("=" * 80)

    ctx.ssp = SSP
    water_ini(ctx, regions=REGIONS, time=None)  # Will auto-detect timeslices from scenario

    log.info(f"Detected time structure: {ctx.time}")

    # Configure nexus
    ctx.nexus_set = "nexus"
    ctx.RCP = RCP
    ctx.SDG = SDG
    ctx.REL = REL
    ctx.reduced_basin = REDUCED_BASIN
    ctx.filter_list = FILTER_LIST

    log.info(f"Nexus configuration:")
    log.info(f"  SSP: {ctx.ssp}")
    log.info(f"  RCP: {ctx.RCP}")
    log.info(f"  SDG: {ctx.SDG}")
    log.info(f"  REL: {ctx.REL}")
    log.info(f"  Reduced basins: {ctx.reduced_basin}")
    if ctx.reduced_basin:
        log.info(f"  Filtered basins: {len(ctx.filter_list)} basins")

    # ========== STEP 5: Build Nexus on Timesliced Scenario ==========
    log.info("=" * 80)
    log.info("STEP 5: Building nexus module on timesliced scenario")
    log.info("=" * 80)

    sc_nexus = sc_timeslice.clone(
        model=MODEL,
        scenario=f"{BASE_SCEN}_t{N_TIME}_nexus_reduced",
        keep_solution=False
    )
    log.info(f"Cloned to: {sc_nexus.model}/{sc_nexus.scenario} v{sc_nexus.version}")

    log.info("Adding nexus structure and data...")
    build_nexus(ctx, sc_nexus)
    log.info("Nexus build completed")

    # ========== STEP 6: Solve Final Model ==========
    log.info("=" * 80)
    log.info("STEP 6: Solving final subannual nexus model")
    log.info("=" * 80)

    sc_nexus.set_as_default()

    log.info(f"Scenario: {sc_nexus.model}/{sc_nexus.scenario} v{sc_nexus.version}")
    log.info(f"Solve options: {SOLVE_OPTIONS}")

    if SOLVE_FINAL:
        log.info("Starting solve...")
        try:
            sc_nexus.solve(solve_options=SOLVE_OPTIONS)
            log.info("=" * 80)
            log.info("SUCCESS! Subannual nexus model solved successfully")
            log.info("=" * 80)
            log.info(f"Final scenario: {sc_nexus.model}/{sc_nexus.scenario} v{sc_nexus.version}")
        except Exception as e:
            log.error(f"Solve failed: {e}")
            log.error("Check solver logs for details")
            raise
    else:
        log.info("Solve skipped (SOLVE_FINAL=False)")
        log.info(f"Scenario ready for solving: {sc_nexus.model}/{sc_nexus.scenario} v{sc_nexus.version}")

    return sc_nexus


if __name__ == "__main__":
    try:
        scenario = main()
        print("\n" + "=" * 80)
        print("Pipeline completed successfully!")
        print("=" * 80)
    except Exception as e:
        log.error(f"Pipeline failed: {e}", exc_info=True)
        raise
