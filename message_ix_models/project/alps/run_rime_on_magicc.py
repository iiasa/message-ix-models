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
from .rime_functions import predict_from_gmt
from .rime import (
    extract_temperature_timeseries,
    batch_rime_predictions,
    batch_rime_predictions_with_percentiles,
    expand_predictions_with_emulator_uncertainty,
    extract_all_run_ids,
    compute_expectation,
    compute_rime_cvar,
    get_rime_dataset_path
)
from .importance_weighting import (
    extract_gmt_timeseries,
    compute_gwl_importance_weights
)

MAGICC_OUTPUT_DIR = package_data_path("report", "legacy", "reporting_output", "magicc_output")


def run_rime(model=None, scenario=None, magicc_file=None, percentile=None, run_id=None,
             variable='both', output_dir=None, weighted=False, n_runs=None,
             cvar_levels=None, gwl_bin_width=0.5, include_emulator_uncertainty=False,
             suban=False, cvar_method='coherent'):
    """Run RIME predictions on MAGICC temperature output.

    Args:
        model: Model name (e.g., MESSAGE_GLOBIOM_SSP2_v6.4)
        scenario: Scenario name (e.g., baseline)
        magicc_file: Path to MAGICC Excel output file (overrides model/scenario)
        percentile: GSAT percentile to use (single percentile, default: 50.0)
        run_id: Specific run_id to extract (0-599). If provided, percentile is ignored.
        variable: Variable to predict ('qtot_mean', 'qr', 'hydro' (both hydrological), or 'capacity_factor')
        output_dir: Output directory for results
        weighted: If True, compute importance-weighted expectations and CVaR
        n_runs: Number of runs to process in weighted mode (default: all)
        cvar_levels: CVaR percentiles for weighted mode (default: [10, 50, 90])
        gwl_bin_width: GWL bin width for importance weighting (default: 0.5°C)
        include_emulator_uncertainty: If True, propagate RIME emulator uncertainty using stratified sampling
        suban: If True, use seasonal (2-step) temporal resolution; if False, use annual (default: False)
        cvar_method: CVaR computation method ('coherent' or 'pointwise', default: 'coherent')
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

    # Determine processing mode using pattern matching
    match (weighted, include_emulator_uncertainty):
        case (True, True):
            # Mode: Importance weighting + emulator uncertainty
            print("\n" + "="*80)
            print("MODE: IMPORTANCE WEIGHTING + EMULATOR UNCERTAINTY")
            print("="*80)

            # Load reference and compute importance weights
            scenario_prefix = magicc_file.stem.replace("_magicc_all_runs", "")
            ref_file = magicc_file.parent / f"{scenario_prefix}_magicc_reference_isimip3b.xlsx"
            if not ref_file.exists():
                print(f"Error: Reference file not found: {ref_file}")
                return 1

            ref_df = pd.read_excel(ref_file, sheet_name='data')
            target_ts = extract_gmt_timeseries(magicc_df)
            ref_ts = extract_gmt_timeseries(ref_df)
            weight_result = compute_gwl_importance_weights(target_ts, ref_ts, gwl_bin_width=gwl_bin_width)

            weights = weight_result['weights'] / weight_result['weights'].sum()
            run_ids = weight_result['run_ids'][:n_runs] if n_runs else weight_result['run_ids']
            weights = weights[:len(run_ids)]
            weights = weights / weights.sum()

            ess = weight_result['diagnostics']['ess']
            print(f"ESS: {ess:.1f} / {len(run_ids)} ({100*ess/len(run_ids):.1f}%)")

        case (True, False):
            # Mode: Importance weighting only
            print("\n" + "="*80)
            print("MODE: IMPORTANCE WEIGHTING")
            print("="*80)

            scenario_prefix = magicc_file.stem.replace("_magicc_all_runs", "")
            ref_file = magicc_file.parent / f"{scenario_prefix}_magicc_reference_isimip3b.xlsx"
            if not ref_file.exists():
                print(f"Error: Reference file not found: {ref_file}")
                return 1

            ref_df = pd.read_excel(ref_file, sheet_name='data')
            target_ts = extract_gmt_timeseries(magicc_df)
            ref_ts = extract_gmt_timeseries(ref_df)
            weight_result = compute_gwl_importance_weights(target_ts, ref_ts, gwl_bin_width=gwl_bin_width)

            weights = weight_result['weights'] / weight_result['weights'].sum()
            run_ids = weight_result['run_ids'][:n_runs] if n_runs else weight_result['run_ids']
            weights = weights[:len(run_ids)]
            weights = weights / weights.sum()

            ess = weight_result['diagnostics']['ess']
            print(f"ESS: {ess:.1f} / {len(run_ids)} ({100*ess/len(run_ids):.1f}%)")

        case (False, True):
            # Mode: Emulator uncertainty only (uniform weights)
            print("\n" + "="*80)
            print("MODE: EMULATOR UNCERTAINTY (UNIFORM WEIGHTS)")
            print("="*80)

            all_runs = extract_all_run_ids(magicc_df)
            run_ids = all_runs[:n_runs] if n_runs else all_runs
            weights = np.ones(len(run_ids)) / len(run_ids)
            print(f"Processing {len(run_ids)} runs with uniform weights")

        case (False, False):
            # Mode: Baseline (p50 only, no special weighting)
            if run_id is not None:
                run_ids = [run_id]
                weights = None
            else:
                # Percentile mode
                print(f"Extracting percentile {percentile or 50.0} trajectory...")
                temp_df = extract_temperature_timeseries(magicc_df, percentile=percentile)
                print(f"  Temperature range: {temp_df['gsat_anomaly_K'].min():.3f}K to {temp_df['gsat_anomaly_K'].max():.3f}K")
                run_ids = None
                weights = None

    variables = ['qtot_mean', 'qr'] if variable == 'hydro' else [variable]

    # Determine temporal resolution
    temporal_res = "seasonal2step" if suban else "annual"

    # Save original weights and run_ids for reuse across variables
    original_weights = weights.copy() if weights is not None else None
    original_run_ids = run_ids.copy() if run_ids is not None else None

    for var in variables:
        # Reset weights and run_ids for each variable
        weights = original_weights.copy() if original_weights is not None else None
        run_ids = original_run_ids.copy() if original_run_ids is not None else None
        print("\n" + "="*80)
        print(f"Processing variable: {var}")
        print(f"Temporal resolution: {temporal_res}")
        print("="*80)

        # Get dataset path using shared helper function
        try:
            dataset_path = get_rime_dataset_path(var, temporal_res)
        except (FileNotFoundError, NotImplementedError) as e:
            print(f"Warning: {e}")
            print(f"Skipping {var}...")
            continue

        print(f"Loading RIME dataset: {dataset_path.name}")

        if run_ids is not None:
            # Batch mode - get predictions based on emulator uncertainty flag
            if include_emulator_uncertainty:
                # Extract p10, p50, p90 and expand to pseudo-runs
                print(f"Running batch RIME predictions with percentiles for {len(run_ids)} run(s)...")
                predictions_p10, predictions_p50, predictions_p90 = batch_rime_predictions_with_percentiles(
                    magicc_df,
                    run_ids,
                    dataset_path,
                    basin_mapping,
                    var,
                    suban
                )

                print(f"Expanding predictions with emulator uncertainty (stratified sampling K=5)...")
                predictions, run_ids_array, expanded_weights = expand_predictions_with_emulator_uncertainty(
                    predictions_p10,
                    predictions_p50,
                    predictions_p90,
                    run_ids,
                    weights
                )
                print(f"  Expanded from N={len(run_ids)} to N×K={len(run_ids_array)} pseudo-runs")

                # Update weights to expanded version
                weights = expanded_weights

            else:
                # Standard batch mode without emulator uncertainty
                print(f"Running batch RIME predictions for {len(run_ids)} run(s)...")
                predictions = batch_rime_predictions(
                    magicc_df,
                    run_ids,
                    dataset_path,
                    basin_mapping,
                    var
                )
                run_ids_array = np.array(run_ids)

            # Compute expectations and CVaR if needed
            if weighted or include_emulator_uncertainty:
                print(f"\nComputing expectations and CVaR...")

                # Results with configured weights (importance or uniform)
                result_mean = compute_expectation(predictions, run_ids_array, weights=weights)
                result_cvar = compute_rime_cvar(predictions, weights, run_ids_array, cvar_levels, method=cvar_method)

                # Comparison: uniform weights
                uniform_weights = np.ones(len(run_ids_array)) / len(run_ids_array)
                uniform_mean = compute_expectation(predictions, run_ids_array, weights=None)
                uniform_cvar = compute_rime_cvar(predictions, uniform_weights, run_ids_array, cvar_levels, method=cvar_method)

                # Save results
                if 'qtot' in var:
                    var_short = 'qtot'
                elif 'qr' in var:
                    var_short = 'qr'
                elif var == 'local_temp':
                    var_short = 'temp'
                else:
                    var_short = var
                scenario_name = f"{model}_{scenario}" if model and scenario else magicc_file.stem.replace("_magicc_all_runs", "")

                # Determine suffix based on weighting mode
                result_suffix = "weighted" if weighted else "emulator_unc"

                print(f"\nSaving {result_suffix} results...")
                result_mean.to_csv(output_dir / f"{var_short}_{temporal_res}_{scenario_name}_{result_suffix}_expectation.csv")
                for level in cvar_levels:
                    result_cvar[f'cvar_{int(level)}'].to_csv(
                        output_dir / f"{var_short}_{temporal_res}_{scenario_name}_{result_suffix}_cvar{int(level)}.csv"
                    )
                print(f"  {var_short}_{temporal_res}_{scenario_name}_{result_suffix}_expectation.csv")
                print(f"  {var_short}_{temporal_res}_{scenario_name}_{result_suffix}_cvar{{10,50,90}}.csv")

                print(f"\nSaving uniform results...")
                uniform_mean.to_csv(output_dir / f"{var_short}_{temporal_res}_{scenario_name}_uniform_expectation.csv")
                for level in cvar_levels:
                    uniform_cvar[f'cvar_{int(level)}'].to_csv(
                        output_dir / f"{var_short}_{temporal_res}_{scenario_name}_uniform_cvar{int(level)}.csv"
                    )
                print(f"  {var_short}_{temporal_res}_{scenario_name}_uniform_expectation.csv")
                print(f"  {var_short}_{temporal_res}_{scenario_name}_uniform_cvar{{10,50,90}}.csv")

            else:
                # Non-weighted batch mode: save each run
                for rid, pred_df in predictions.items():
                    if 'qtot' in var:
                        var_short = 'qtot'
                    elif 'qr' in var:
                        var_short = 'qr'
                    elif var == 'local_temp':
                        var_short = 'temp'
                    else:
                        var_short = var
                    scenario_name = f"{model}_{scenario}" if model and scenario else "scenario"
                    filename = f"{var_short}_{temporal_res}_{scenario_name}_run{rid}.csv"
                    output_file = output_dir / filename
                    pred_df.to_csv(output_file)
                    print(f"  Saved: {output_file}")
        else:
            # Single percentile mode
            print(f"Running RIME predictions for percentile {percentile or 50.0}...")
            years = temp_df['year'].values
            gmt_values = temp_df['gsat_anomaly_K'].values

            # Map variable name for dataset lookup
            var_map = {'local_temp': 'temp_mean_anomaly'}
            rime_var = var_map.get(var, var)

            # Predict for each year
            predictions_list = []
            for gmt in gmt_values:
                pred = predict_from_gmt(gmt, str(dataset_path), rime_var)
                predictions_list.append(pred)

            # Stack into DataFrame (n_basins × n_years)
            predictions_array = np.array(predictions_list).T
            pred_df = pd.DataFrame(predictions_array, columns=years)

            # Save
            if 'qtot' in var:
                var_short = 'qtot'
            elif 'qr' in var:
                var_short = 'qr'
            elif var == 'local_temp':
                var_short = 'temp'
            else:
                var_short = var
            scenario_name = f"{model}_{scenario}" if model and scenario else "scenario"
            p_val = int(percentile) if percentile else 50
            filename = f"{var_short}_{temporal_res}_{scenario_name}_p{p_val}.csv"
            output_file = output_dir / filename
            pred_df.to_csv(output_file)
            print(f"  Saved: {output_file}")

    print("\n" + "="*80)
    print("RIME PREDICTIONS COMPLETE")
    print("="*80)
    print(f"All results saved to: {output_dir}")

    return 0
