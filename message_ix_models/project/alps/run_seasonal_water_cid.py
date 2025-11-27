#!/usr/bin/env python3
"""Run seasonal water CID replacement on MESSAGE scenarios.

This script performs seasonal (h1/h2) water availability replacement:
1. Load MESSAGE scenario with nexus module
2. Add timeslices (h1, h2) if not present
3. Run RIME seasonal predictions from MAGICC temperature
4. Compute expectation over ensemble
5. Transform from RIME (dry/wet) to MESSAGE (h1/h2) using basin-specific mapping
6. Replace water availability parameters

Usage:
    python run_seasonal_water_cid.py --model MESSAGE_GLOBIOM_SSP2_v6.4 --scenario baseline
    python run_seasonal_water_cid.py --model MESSAGE_GLOBIOM_SSP2_v6.4 --scenario baseline_1000f
"""

import argparse
import sys
from pathlib import Path
import numpy as np

from message_ix_models.project.alps.cid_utils import (
    MAGICC_OUTPUT_DIR,
    cache,
    cached_rime_prediction,
    load_scenario_for_cid,
    verify_timeslices,
    verify_parameter_structure,
    deinterleave_seasonal,
    report_nan_values,
    report_value_ranges,
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
import pandas as pd


def add_timeslices_minimal(scenario, n_time: int = 2):
    """Add timeslice structure to scenario (minimal version without context).

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

    # Generate uniform timeslice structure
    df_time = generate_uniform_timeslices(n_time)

    # Add time sets and hierarchy
    time_setup(scenario, df_time)

    # Add duration_time parameter
    duration_time(scenario, df_time)

    # Verify
    time_set = set(scenario.set("time").tolist())
    subannual = time_set - {'year'}
    print(f"   Timeslices added: {subannual}")

    return scenario


def main():
    parser = argparse.ArgumentParser(
        description="Run seasonal water CID replacement on MESSAGE scenario",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run with default settings
  python run_seasonal_water_cid.py --scenario baseline --dry-run

  # Run for baseline_1000f
  python run_seasonal_water_cid.py --scenario baseline_1000f --n-runs 100

  # Use custom MAGICC file
  python run_seasonal_water_cid.py --magicc-file path/to/magicc.xlsx
        """,
    )
    parser.add_argument(
        "--platform",
        default="ixmp_dev",
        help="ixmp platform name (default: ixmp_dev)",
    )
    parser.add_argument(
        "--model",
        default="MESSAGE_GLOBIOM_SSP2_v6.4",
        help="MESSAGE model name (default: MESSAGE_GLOBIOM_SSP2_v6.4)",
    )
    parser.add_argument(
        "--scenario",
        required=True,
        help="MESSAGE scenario name (e.g., baseline, baseline_1000f)",
    )
    parser.add_argument(
        "--magicc-file",
        type=Path,
        help="MAGICC output file (default: derived from model/scenario)",
    )
    parser.add_argument(
        "--n-runs",
        type=int,
        default=100,
        help="Number of MAGICC runs to use for expectation (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare parameters but do not commit to scenario",
    )
    parser.add_argument(
        "--skip-timeslice-setup",
        action="store_true",
        help="Skip adding timeslices (assume already present)",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear RIME prediction cache before running",
    )

    args = parser.parse_args()

    # Derive MAGICC file if not provided
    if args.magicc_file is None:
        # Convention: SSP_SSP2_v6.4_baseline_magicc_all_runs.xlsx
        magicc_prefix = args.model.replace("MESSAGE_GLOBIOM_", "SSP_")
        args.magicc_file = MAGICC_OUTPUT_DIR / f"{magicc_prefix}_{args.scenario}_magicc_all_runs.xlsx"

    if not args.magicc_file.exists():
        print(f"Error: MAGICC file not found: {args.magicc_file}")
        print(f"Available files in {MAGICC_OUTPUT_DIR}:")
        for f in sorted(MAGICC_OUTPUT_DIR.glob("*_magicc_all_runs.xlsx"))[:10]:
            print(f"  {f.name}")
        return 1

    if args.clear_cache:
        print("Clearing RIME prediction cache...")
        cache.clear()

    print("=" * 80)
    print("SEASONAL WATER CID REPLACEMENT")
    print("=" * 80)
    print(f"Platform:    {args.platform}")
    print(f"Model:       {args.model}")
    print(f"Scenario:    {args.scenario}")
    print(f"MAGICC file: {args.magicc_file.name}")
    print(f"N runs:      {args.n_runs}")
    print(f"Mode:        {'DRY RUN' if args.dry_run else 'COMMIT'}")
    print()

    # Step 1: Load scenario
    print("1. Loading MESSAGE scenario...")
    try:
        scen = load_scenario_for_cid(
            args.platform, args.model, args.scenario,
            clone_without_solution=True
        )
    except Exception as e:
        print(f"   Error loading scenario: {e}")
        return 1

    # Step 2: Add timeslices if needed
    print("\n2. Checking/adding timeslices...")
    has_timeslices = verify_timeslices(scen, expected_times={'h1', 'h2'})

    if not has_timeslices and not args.skip_timeslice_setup:
        print("   Adding timeslices to scenario...")
        # time_setup/duration_time use transact internally
        add_timeslices_minimal(scen, n_time=2)
        print("   Timeslices committed")
    elif not has_timeslices and args.skip_timeslice_setup:
        print("   WARNING: Timeslices missing but --skip-timeslice-setup specified")
        print("   Proceeding anyway (may fail during parameter replacement)")

    # Step 3: Extract run IDs from MAGICC
    print("\n3. Loading MAGICC temperature data...")
    magicc_df = pd.read_excel(args.magicc_file, sheet_name="data")
    all_run_ids = extract_all_run_ids(magicc_df)
    run_ids = tuple(all_run_ids[:args.n_runs])
    print(f"   Total runs available: {len(all_run_ids)}")
    print(f"   Using {len(run_ids)} runs for expectation")

    # Step 4: Run RIME seasonal predictions
    print("\n4. Running RIME seasonal predictions for qtot_mean...")
    qtot_predictions = cached_rime_prediction(
        args.magicc_file, run_ids, "qtot_mean", temporal_res="seasonal2step"
    )
    print(f"   Got {len(qtot_predictions)} prediction sets")

    print("\n5. Running RIME seasonal predictions for qr...")
    qr_predictions = cached_rime_prediction(
        args.magicc_file, run_ids, "qr", temporal_res="seasonal2step"
    )
    print(f"   Got {len(qr_predictions)} prediction sets")

    # Step 5: Compute expectations
    print("\n6. Computing expectations (uniform weights)...")
    qtot_expected = compute_expectation(
        qtot_predictions, run_ids=np.array(list(run_ids)), weights=None
    )
    qr_expected = compute_expectation(
        qr_predictions, run_ids=np.array(list(run_ids)), weights=None
    )
    print(f"   qtot_expected shape: {qtot_expected.shape}")
    print(f"   qr_expected shape: {qr_expected.shape}")

    # Step 6: De-interleave seasonal data
    print("\n7. De-interleaving seasonal data (dry/wet)...")
    qtot_dry, qtot_wet = deinterleave_seasonal(qtot_expected)
    qr_dry, qr_wet = deinterleave_seasonal(qr_expected)

    year_cols = [c for c in qtot_dry.columns if isinstance(c, int)]
    print(f"   Year range: {min(year_cols)} - {max(year_cols)}")
    print(f"   Dry qtot sample (basin 1, 2020): {qtot_dry[qtot_dry['BASIN_ID']==1][2020].values[0]:.2f} km³")
    print(f"   Wet qtot sample (basin 1, 2020): {qtot_wet[qtot_wet['BASIN_ID']==1][2020].values[0]:.2f} km³")

    # Step 7: Transform to MESSAGE format (applies basin-specific dry/wet -> h1/h2)
    print("\n8. Transforming to MESSAGE parameter format (h1/h2)...")
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

    # Step 8: Verify structure
    print("\n9. Verifying parameter structure...")
    try:
        verify_parameter_structure(sw_demand, gw_demand, gw_share, temporal_res="seasonal")
    except AssertionError as e:
        print(f"   ERROR: {e}")
        return 1

    # Check NaN values
    total_nan = report_nan_values(sw_demand, gw_demand, gw_share)

    # Report value ranges
    print("\n10. Value ranges:")
    report_value_ranges(sw_demand, gw_demand, gw_share)

    # Show sample values
    print("\n11. Sample parameter values:")
    print("\n   sw_demand (first 5 rows):")
    print(sw_demand.head(5).to_string(index=False))

    # Step 9: Replace water availability
    if not args.dry_run:
        print("\n12. Replacing water availability in scenario...")
        commit_msg = (
            f"Replace water CID with seasonal RIME projections\n"
            f"Source: {args.magicc_file.name}\n"
            f"N runs: {args.n_runs} (expectation with uniform weights)\n"
            f"Variables: qtot_mean, qr (seasonal resolution h1/h2)\n"
            f"Transformation: basin-specific dry/wet -> h1/h2 via regime shift mapping"
        )

        # Clone for modification if needed
        if scen.has_solution():
            scen = scen.clone(keep_solution=False)
            print(f"   Cloned to version {scen.version}")

        scen_updated = replace_water_availability(
            scen, sw_demand, gw_demand, gw_share, commit_message=commit_msg
        )
        print(f"   Committed new version: {scen_updated.version}")
        print(f"   Model: {scen_updated.model}")
        print(f"   Scenario: {scen_updated.scenario}")
    else:
        print("\n12. Dry run - skipping commit")
        print("   (Use without --dry-run to commit changes)")

    print("\n" + "=" * 80)
    print("SEASONAL WATER CID REPLACEMENT COMPLETE")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
