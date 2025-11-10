#!/usr/bin/env python3
"""
Run RIME predictions on MAGICC emissions output.

CLI wrapper for batch_rime module.

Usage:
    mix-models alps run-rime --model <model> --scenario <scenario>
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

from message_ix_models.util import package_data_path
from rime.rime_functions import predict_from_gmt
from .rime import (
    extract_temperature_timeseries,
    batch_rime_predictions,
    extract_all_run_ids,
    compute_expectation,
    compute_rime_cvar
)
from .importance_weighting import (
    extract_gmt_timeseries,
    compute_gwl_importance_weights
)


RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")
MAGICC_OUTPUT_DIR = package_data_path("report", "legacy", "reporting_output", "magicc_output")


def run_rime(model=None, scenario=None, magicc_file=None, percentile=None, run_id=None,
             variable='both', output_dir=None, weighted=False, n_runs=None,
             cvar_levels=None, gwl_bin_width=0.5):
    """Run RIME predictions on MAGICC temperature output.

    Args:
        model: Model name (e.g., MESSAGE_GLOBIOM_SSP2_v6.4)
        scenario: Scenario name (e.g., baseline)
        magicc_file: Path to MAGICC Excel output file (overrides model/scenario)
        percentile: GSAT percentile to use (single percentile, default: 50.0)
        run_id: Specific run_id to extract (0-599). If provided, percentile is ignored.
        variable: Hydrological variable to predict ('qtot_mean', 'qr', or 'both')
        output_dir: Output directory for results
        weighted: If True, compute importance-weighted expectations and CVaR
        n_runs: Number of runs to process in weighted mode (default: all)
        cvar_levels: CVaR percentiles for weighted mode (default: [10, 50, 90])
        gwl_bin_width: GWL bin width for importance weighting (default: 0.5°C)
    """
    if cvar_levels is None:
        cvar_levels = [10, 50, 90]
    # Construct path from model/scenario if provided
    if model and scenario and magicc_file is None:
        magicc_file = MAGICC_OUTPUT_DIR / f"{model}_{scenario}_magicc_all_runs.xlsx"
    elif magicc_file:
        magicc_file = Path(magicc_file)
    else:
        # Use most recent file
        magicc_files = list(MAGICC_OUTPUT_DIR.glob("*_magicc*.xlsx"))
        if not magicc_files:
            print(f"Error: No MAGICC output files found in {MAGICC_OUTPUT_DIR}")
            return 1
        magicc_file = max(magicc_files, key=lambda p: p.stat().st_mtime)
        print(f"Using most recent MAGICC file: {magicc_file.name}")

    if not magicc_file.exists():
        print(f"Error: MAGICC file not found: {magicc_file}")
        return 1

    if output_dir is None:
        output_dir = magicc_file.parent / "rime_predictions"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*80)
    print("RIME PREDICTIONS ON MAGICC OUTPUT")
    print("="*80)
    print(f"MAGICC file: {magicc_file}")
    print(f"Variable(s): {variable}")
    print(f"Output directory: {output_dir}")
    print()

    # Load MAGICC data
    print("Loading MAGICC output...")
    magicc_df = pd.read_excel(magicc_file, sheet_name='data')

    # Load basin mapping
    print("Loading MESSAGE basin mapping...")
    basin_mapping_path = package_data_path("water", "delineation", "basins_by_region_simpl_R12.csv")
    basin_mapping = pd.read_csv(basin_mapping_path)
    print(f"  Loaded mapping for {len(basin_mapping)} MESSAGE basins")
    print()

    # Handle weighted mode
    weights = None
    if weighted:
        print("\n" + "="*80)
        print("IMPORTANCE WEIGHTING MODE")
        print("="*80)

        # Construct reference file path
        scenario_prefix = magicc_file.stem.replace("_magicc_all_runs", "")
        ref_file = magicc_file.parent / f"{scenario_prefix}_magicc_reference_isimip3b.xlsx"

        if not ref_file.exists():
            print(f"Error: Reference file not found: {ref_file}")
            print("Generate reference by running MAGICC with --generate-reference flag")
            return 1

        print(f"Reference file: {ref_file}")
        print(f"GWL bin width: {gwl_bin_width}°C")

        # Load reference data
        print("\nLoading reference data...")
        ref_df = pd.read_excel(ref_file, sheet_name='data')

        # Compute importance weights
        print("Computing importance weights...")
        target_ts = extract_gmt_timeseries(magicc_df)
        ref_ts = extract_gmt_timeseries(ref_df)
        weight_result = compute_gwl_importance_weights(target_ts, ref_ts, gwl_bin_width=gwl_bin_width)

        weights = weight_result['weights']
        run_ids_all = weight_result['run_ids']
        ess = weight_result['diagnostics']['ess']

        # Normalize weights to sum to 1
        weights = weights / weights.sum()

        print(f"  Computed weights for {len(run_ids_all)} runs")
        print(f"  Effective Sample Size (ESS): {ess:.1f} / {len(run_ids_all)} ({100*ess/len(run_ids_all):.1f}%)")

        # Limit runs if requested
        if n_runs is not None and n_runs < len(run_ids_all):
            print(f"\nLimiting to first {n_runs} runs...")
            run_ids_all = run_ids_all[:n_runs]
            weights = weights[:n_runs]
            weights = weights / weights.sum()  # Renormalize

        run_ids = run_ids_all.tolist()
        print(f"\nProcessing {len(run_ids)} runs")

    # Determine which run_ids to process (non-weighted mode)
    elif run_id is not None:
        run_ids = [run_id]
    else:
        # Use percentile - extract that single temperature trajectory
        # For CLI simplicity, just process the percentile trajectory
        print(f"Extracting percentile {percentile or 50.0} trajectory...")
        temp_df = extract_temperature_timeseries(magicc_df, percentile=percentile)
        print(f"  Temperature range: {temp_df['gsat_anomaly_K'].min():.3f}K to {temp_df['gsat_anomaly_K'].max():.3f}K")

        # For single percentile, we'll just save directly without batch processing
        # This maintains backward compatibility with the old CLI
        run_ids = None  # Signal to use percentile mode

    variables = ['qtot_mean', 'qr'] if variable == 'both' else [variable]

    for var in variables:
        print("\n" + "="*80)
        print(f"Processing variable: {var}")
        print("="*80)

        dataset_filename = f"rime_regionarray_{var}_CWatM_annual_window0.nc"
        dataset_path = RIME_DATASETS_DIR / dataset_filename

        if not dataset_path.exists():
            print(f"Warning: RIME dataset not found: {dataset_path}")
            print(f"Skipping {var}...")
            continue

        print(f"Loading RIME dataset: {dataset_filename}")

        if run_ids is not None:
            # Batch mode (run_id specified or weighted mode)
            print(f"Running batch RIME predictions for {len(run_ids)} run(s)...")
            predictions = batch_rime_predictions(
                magicc_df,
                run_ids,
                dataset_path,
                basin_mapping,
                var
            )

            if weighted:
                # Weighted mode: compute expectations and CVaR
                print(f"\nComputing importance-weighted expectations and CVaR...")
                run_ids_array = np.array(run_ids)

                # Weighted results
                weighted_mean = compute_expectation(predictions, run_ids_array, weights=weights)
                weighted_cvar = compute_rime_cvar(predictions, weights, run_ids_array, cvar_levels)

                # Unweighted results
                unweighted_mean = compute_expectation(predictions, run_ids_array, weights=None)
                uniform_weights = np.ones(len(run_ids)) / len(run_ids)
                unweighted_cvar = compute_rime_cvar(predictions, uniform_weights, run_ids_array, cvar_levels)

                # Save results
                var_short = 'qtot' if 'qtot' in var else 'qr'
                scenario_name = f"{model}_{scenario}" if model and scenario else magicc_file.stem.replace("_magicc_all_runs", "")

                print(f"\nSaving weighted results...")
                weighted_mean.to_csv(output_dir / f"{var_short}_{scenario_name}_weighted_expectation.csv")
                for level in cvar_levels:
                    weighted_cvar[f'cvar_{int(level)}'].to_csv(
                        output_dir / f"{var_short}_{scenario_name}_weighted_cvar{int(level)}.csv"
                    )
                print(f"  {var_short}_{scenario_name}_weighted_expectation.csv")
                print(f"  {var_short}_{scenario_name}_weighted_cvar{{10,50,90}}.csv")

                print(f"\nSaving unweighted results...")
                unweighted_mean.to_csv(output_dir / f"{var_short}_{scenario_name}_unweighted_expectation.csv")
                for level in cvar_levels:
                    unweighted_cvar[f'cvar_{int(level)}'].to_csv(
                        output_dir / f"{var_short}_{scenario_name}_unweighted_cvar{int(level)}.csv"
                    )
                print(f"  {var_short}_{scenario_name}_unweighted_expectation.csv")
                print(f"  {var_short}_{scenario_name}_unweighted_cvar{{10,50,90}}.csv")

            else:
                # Non-weighted batch mode: save each run
                for rid, pred_df in predictions.items():
                    var_short = 'qtot' if 'qtot' in var else 'qr'
                    scenario_name = f"{model}_{scenario}" if model and scenario else "scenario"
                    filename = f"{var_short}_annual_{scenario_name}_run{rid}.csv"
                    output_file = output_dir / filename
                    pred_df.to_csv(output_file)
                    print(f"  Saved: {output_file}")
        else:
            # Single percentile mode
            print(f"Running RIME predictions for percentile {percentile or 50.0}...")
            years = temp_df['year'].values
            gmt_values = temp_df['gsat_anomaly_K'].values

            # Predict for each year
            predictions_list = []
            for gmt in gmt_values:
                pred = predict_from_gmt(gmt, str(dataset_path), var)
                predictions_list.append(pred)

            # Stack into DataFrame (n_basins × n_years)
            predictions_array = np.array(predictions_list).T
            pred_df = pd.DataFrame(predictions_array, columns=years)

            # Save
            var_short = 'qtot' if 'qtot' in var else 'qr'
            scenario_name = f"{model}_{scenario}" if model and scenario else "scenario"
            p_val = int(percentile) if percentile else 50
            filename = f"{var_short}_annual_{scenario_name}_p{p_val}.csv"
            output_file = output_dir / filename
            pred_df.to_csv(output_file)
            print(f"  Saved: {output_file}")

    print("\n" + "="*80)
    print("RIME PREDICTIONS COMPLETE")
    print("="*80)
    print(f"All results saved to: {output_dir}")

    return 0
