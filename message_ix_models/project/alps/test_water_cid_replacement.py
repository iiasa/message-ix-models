#!/usr/bin/env python3
"""Test water CID replacement workflow on MESSAGE scenario.

This script demonstrates the complete workflow:
1. Load MESSAGE scenario with nexus module
2. Run RIME predictions from MAGICC temperature (with caching)
3. Compute expectation over ensemble
4. Transform to MESSAGE parameter format
5. Replace water availability parameters
"""

import argparse
from pathlib import Path
import pandas as pd
from diskcache import FanoutCache

from ixmp import Platform
from message_ix import Scenario
from message_ix_models.util import package_data_path
from message_ix_models.project.alps.rime import (
    batch_rime_predictions,
    compute_expectation,
    get_rime_dataset_path,
    extract_all_run_ids,
)
from message_ix_models.project.alps.replace_water_cids import (
    prepare_water_cids,
    replace_water_availability,
)

# Setup diskcache
CACHE_DIR = Path(__file__).parent / ".cache" / "rime_predictions"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
cache = FanoutCache(str(CACHE_DIR), shards=8)


def cached_rime_prediction(magicc_file: Path, run_ids: tuple, variable: str) -> dict:
    """Cached wrapper for RIME predictions.

    Parameters
    ----------
    magicc_file : Path
        Path to MAGICC Excel output
    run_ids : tuple
        Run IDs to process (must be tuple for hashing)
    variable : str
        Variable to predict ('qtot_mean' or 'qr')

    Returns
    -------
    dict
        Dictionary mapping run_id -> basin predictions DataFrame
    """
    cache_key = f"{magicc_file.stem}_{variable}_{len(run_ids)}runs_{hash(run_ids)}"

    if cache_key in cache:
        print(f"   Cache hit for {variable}")
        return cache[cache_key]

    print(f"   Cache miss for {variable} - computing predictions...")

    # Load MAGICC data
    magicc_df = pd.read_excel(magicc_file, sheet_name="data")

    # Load basin mapping
    basin_mapping_path = package_data_path(
        "water", "delineation", "basins_by_region_simpl_R12.csv"
    )
    basin_mapping = pd.read_csv(basin_mapping_path)

    # Get dataset path
    temporal_res = "annual"
    dataset_path = get_rime_dataset_path(variable, temporal_res)

    # Run predictions
    predictions = batch_rime_predictions(
        magicc_df, list(run_ids), dataset_path, basin_mapping, variable
    )

    # Cache result
    cache[cache_key] = predictions

    return predictions


def main():
    parser = argparse.ArgumentParser(
        description="Test water CID replacement on MESSAGE scenario",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with default settings (10 runs, dry-run)
  python test_water_cid_replacement.py --dry-run

  # Test and commit to scenario
  python test_water_cid_replacement.py

  # Use different MAGICC file
  python test_water_cid_replacement.py --magicc-file path/to/magicc_output.xlsx --n-runs 50
        """,
    )
    parser.add_argument(
        "--model",
        default="MESSAGE_GLOBIOM_SSP2_v6.1",
        help="MESSAGE model name (default: MESSAGE_GLOBIOM_SSP2_v6.1)",
    )
    parser.add_argument(
        "--scenario",
        default="Case_C_reduced1",
        help="MESSAGE scenario name (default: Case_C_reduced1)",
    )
    parser.add_argument(
        "--magicc-file",
        type=Path,
        help="MAGICC output file (default: SSP2 baseline 1000f)",
    )
    parser.add_argument(
        "--n-runs",
        type=int,
        default=10,
        help="Number of MAGICC runs to use (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare parameters but do not commit to scenario",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear RIME prediction cache before running",
    )

    args = parser.parse_args()

    # Default MAGICC file if not provided
    if args.magicc_file is None:
        magicc_dir = package_data_path(
            "report", "legacy", "reporting_output", "magicc_output"
        )
        args.magicc_file = (
            magicc_dir / "SSP_SSP2_v6.4_baseline_1000f_magicc_all_runs.xlsx"
        )

    if not args.magicc_file.exists():
        print(f"Error: MAGICC file not found: {args.magicc_file}")
        return 1

    if args.clear_cache:
        print("Clearing RIME prediction cache...")
        cache.clear()

    print("=" * 80)
    print("WATER CID REPLACEMENT TEST")
    print("=" * 80)
    print(f"Model:       {args.model}")
    print(f"Scenario:    {args.scenario}")
    print(f"MAGICC file: {args.magicc_file.name}")
    print(f"N runs:      {args.n_runs}")
    print(f"Mode:        {'DRY RUN' if args.dry_run else 'COMMIT'}")
    print()

    # Load MESSAGE scenario
    print("1. Loading MESSAGE scenario from ixmp_dev...")
    mp = Platform("ixmp_dev")
    scen = Scenario(mp, args.model, args.scenario)
    print(f"   Loaded: {scen.model} / {scen.scenario} (version {scen.version})")
    print(f"   Has solution: {scen.has_solution()}")

    # Clone without solution if scenario has one
    if scen.has_solution():
        print("   Cloning scenario without solution...")
        scen = scen.clone(keep_solution=False)
        print(f"   Cloned to version {scen.version} (no solution)")

    # Check scenario has nexus module (water availability parameters)
    try:
        existing_sw = scen.par("demand", {"commodity": "surfacewater_basin"})
        existing_gw = scen.par("demand", {"commodity": "groundwater_basin"})
        print(
            f"   ✓ Scenario has nexus module ({len(existing_sw)} surfacewater, {len(existing_gw)} groundwater rows)"
        )
    except Exception as e:
        print(f"   ✗ Error: Scenario missing water parameters: {e}")
        return 1

    # Extract run IDs from MAGICC data
    print("\n2. Loading MAGICC temperature data...")
    magicc_df = pd.read_excel(args.magicc_file, sheet_name="data")
    all_run_ids = extract_all_run_ids(magicc_df)
    run_ids = tuple(all_run_ids[: args.n_runs])
    print(f"   Total runs available: {len(all_run_ids)}")
    print(f"   Using runs: {run_ids}")

    # Run RIME predictions for qtot (with caching)
    print("\n3. Running RIME predictions for qtot_mean...")
    qtot_predictions = cached_rime_prediction(args.magicc_file, run_ids, "qtot_mean")
    print(f"   Got {len(qtot_predictions)} prediction sets")

    # Run RIME predictions for qr (with caching)
    print("\n4. Running RIME predictions for qr...")
    qr_predictions = cached_rime_prediction(args.magicc_file, run_ids, "qr")
    print(f"   Got {len(qr_predictions)} prediction sets")

    # Compute expectations (uniform weights)
    print("\n5. Computing expectations (uniform weights)...")
    qtot_expected = compute_expectation(
        qtot_predictions, run_ids=list(run_ids), weights=None
    )
    qr_expected = compute_expectation(
        qr_predictions, run_ids=list(run_ids), weights=None
    )
    print(f"   qtot_expected shape: {qtot_expected.shape}")
    print(f"   qr_expected shape: {qr_expected.shape}")
    year_cols = [c for c in qtot_expected.columns if isinstance(c, int)]
    if year_cols:
        print(f"   Year range: {min(year_cols)} - {max(year_cols)}")

    # Transform to MESSAGE parameter format
    print("\n6. Transforming to MESSAGE parameter format...")
    sw_data, gw_data, share_data = prepare_water_cids(
        qtot_expected, qr_expected, scen, temporal_res="annual"
    )
    sw_new, sw_old = sw_data
    gw_new, gw_old = gw_data
    share_new, share_old = share_data

    print(f"   sw: {len(sw_new)} new, {len(sw_old)} old rows")
    print(f"   gw: {len(gw_new)} new, {len(gw_old)} old rows")
    print(f"   share: {len(share_new)} new, {len(share_old)} old rows")

    # Verify parameter structure
    print("\n7. Verifying parameter structure...")
    assert set(sw_new.columns) == {
        "node",
        "commodity",
        "level",
        "year",
        "time",
        "value",
        "unit",
    }
    assert set(gw_new.columns) == {
        "node",
        "commodity",
        "level",
        "year",
        "time",
        "value",
        "unit",
    }
    assert set(share_new.columns) == {
        "shares",
        "node_share",
        "year_act",
        "time",
        "value",
        "unit",
    }
    print("   All parameters have correct columns")

    # Check for NaN values
    nan_sw = sw_new["value"].isna().sum()
    nan_gw = gw_new["value"].isna().sum()
    nan_share = share_new["value"].isna().sum()

    if nan_sw > 0:
        print(f"   Warning: {nan_sw} NaN values in sw_demand")
    if nan_gw > 0:
        print(f"   Warning: {nan_gw} NaN values in gw_demand")
    if nan_share > 0:
        print(f"   Warning: {nan_share} NaN values in gw_share")

    if nan_sw == 0 and nan_gw == 0 and nan_share == 0:
        print("   No NaN values detected")

    # Show sample values
    print("\n8. Sample parameter values:")
    print("\n   sw_demand (first 3 rows):")
    print(sw_new.head(3).to_string(index=False))
    print("\n   gw_share (first 3 rows):")
    print(share_new.head(3).to_string(index=False))

    # Value ranges
    print("\n9. Value ranges:")
    print(
        f"   sw_demand: [{sw_new['value'].min():.2f}, {sw_new['value'].max():.2f}] MCM/year"
    )
    print(
        f"   gw_demand: [{gw_new['value'].min():.2f}, {gw_new['value'].max():.2f}] MCM/year"
    )
    print(
        f"   gw_share:  [{share_new['value'].min():.4f}, {share_new['value'].max():.4f}] (fraction)"
    )

    if not args.dry_run:
        # Replace water availability
        print("\n10. Replacing water availability in scenario...")
        commit_msg = (
            f"Replace water CID with RIME projections\n"
            f"Source: {args.magicc_file.name}\n"
            f"N runs: {args.n_runs} (expectation with uniform weights)\n"
            f"Variables: qtot_mean, qr (annual resolution)"
        )
        scen_updated = replace_water_availability(
            scen, sw_data, gw_data, share_data, commit_message=commit_msg
        )
        print(f"   ✓ Committed new version: {scen_updated.version}")
        print(f"   ✓ Model: {scen_updated.model}")
        print(f"   ✓ Scenario: {scen_updated.scenario}")
    else:
        print("\n10. Dry run - skipping commit")
        print("   (Use without --dry-run to commit changes)")

    print("\n" + "=" * 80)
    print("✓ TEST COMPLETE")
    print("=" * 80)
    print(f"Cache location: {CACHE_DIR}")
    print(f"Cache size: {len(cache)} entries")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
