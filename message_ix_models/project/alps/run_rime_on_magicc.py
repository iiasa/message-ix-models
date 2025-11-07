#!/usr/bin/env python3
"""
Run RIME predictions on MAGICC emissions output.

Takes MAGICC temperature projections (GSAT) and runs RIME to predict
basin-level hydrological impacts (qtot and groundwater recharge) for
MESSAGE R12 regional basins.

Usage:
    uv run --no-sync run_rime_on_magicc.py --magicc-file <path> [options]
"""

import sys
from pathlib import Path

sys.path.insert(0, '/home/raghunathan/RIME')

import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

from rime.core import RegionArray
from rime.rime_functions import table_impacts_gwl

from message_ix_models.util import package_data_path


RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")
MAGICC_OUTPUT_DIR = package_data_path("report", "legacy", "reporting_output", "magicc_output")
CLIMATE_ASSESSMENT_OUTPUT_DIR = package_data_path("report", "legacy", "reporting_output", "climate_assessment_output")

R12_BASINS = {
    "1.0": "AFR",
    "2.0": "CHN",
    "3.0": "EEU",
    "4.0": "FSU",
    "5.0": "LAM",
    "6.0": "MEA",
    "7.0": "NAM",
    "8.0": "PAO",
    "9.0": "PAS",
    "10.0": "RCPA",
    "11.0": "SAS",
    "12.0": "WEU"
}


def load_magicc_temperature(
    magicc_file: Path,
    percentile: float = None,
    run_id: int = None
) -> pd.DataFrame:
    """
    Load GSAT temperature trajectory from MAGICC output.

    Args:
        magicc_file: Path to MAGICC Excel output
        percentile: Which percentile to use (5, 10, 50, 90, 95, etc.). Defaults to 50.0 if run_id not specified.
        run_id: Specific run_id to extract (e.g., 0-599). If provided, percentile is ignored.

    Returns:
        DataFrame with columns: year, gsat_anomaly_K, model, scenario, ssp_family
    """
    print(f"Loading MAGICC output from {magicc_file.name}...")

    df = pd.read_excel(magicc_file, sheet_name='data')

    # Determine selection mode
    if run_id is not None:
        # Select by run_id (new behavior for surrogate modeling)
        print(f"  Selecting run_id: {run_id}")
        model_pattern = f"|run_{run_id}"
        var_pattern = "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"

        temp_data = df[
            (df['Model'].str.contains(model_pattern, na=False)) &
            (df['Variable'].str.contains(var_pattern, na=False))
        ]

        if len(temp_data) == 0:
            available_models = df['Model'].unique()
            raise ValueError(
                f"No temperature data found for run_id {run_id}.\n"
                f"Available models:\n" + "\n".join(f"  - {m}" for m in available_models[:10])
            )
    else:
        # Select by percentile (old behavior)
        if percentile is None:
            percentile = 50.0
        print(f"  Selecting percentile: {percentile}")
        percentile_str = f"{percentile}th Percentile" if percentile != 50.0 else "50.0th Percentile"
        var_pattern = f"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3|{percentile_str}"

        temp_data = df[df['Variable'] == var_pattern]

        if len(temp_data) == 0:
            available = df[df['Variable'].str.contains('GSAT', case=False, na=False)]['Variable'].unique()
            raise ValueError(
                f"No temperature data found for percentile {percentile}.\n"
                f"Available percentiles:\n" + "\n".join(f"  - {v}" for v in available)
            )

    temp_row = temp_data.iloc[0]

    year_cols = [col for col in df.columns if str(col).isdigit()]
    temps = {int(year): temp_row[year] for year in year_cols if pd.notna(temp_row[year])}

    temp_df = pd.DataFrame({
        'year': list(temps.keys()),
        'gsat_anomaly_K': list(temps.values())
    })

    scenario_name = temp_row['Scenario']
    model_name = temp_row['Model']

    if 'SSP1' in scenario_name or 'ssp1' in scenario_name.lower():
        ssp_family = 'SSP1'
    elif 'SSP2' in scenario_name or 'ssp2' in scenario_name.lower():
        ssp_family = 'SSP2'
    elif 'SSP3' in scenario_name or 'ssp3' in scenario_name.lower():
        ssp_family = 'SSP3'
    elif 'SSP4' in scenario_name or 'ssp4' in scenario_name.lower():
        ssp_family = 'SSP4'
    elif 'SSP5' in scenario_name or 'ssp5' in scenario_name.lower():
        ssp_family = 'SSP5'
    else:
        ssp_family = 'SSP2'

    temp_df['model'] = model_name
    temp_df['scenario'] = scenario_name
    temp_df['ssp_family'] = ssp_family

    print(f"  Loaded temperature data: {len(temp_df)} years ({temp_df['year'].min()}-{temp_df['year'].max()})")
    print(f"  Model: {model_name}")
    print(f"  Scenario: {scenario_name}")
    print(f"  SSP Family: {ssp_family}")
    print(f"  Temperature range: {temp_df['gsat_anomaly_K'].min():.3f}K to {temp_df['gsat_anomaly_K'].max():.3f}K")

    return temp_df


def create_rime_input(temp_df: pd.DataFrame) -> pd.Series:
    """
    Convert temperature DataFrame to RIME-compatible input format.

    Args:
        temp_df: DataFrame with columns year, gsat_anomaly_K, model, scenario, ssp_family

    Returns:
        pandas Series with years as index and temperature values, plus metadata columns
    """
    gmt_series = pd.Series(
        temp_df['gsat_anomaly_K'].values,
        index=temp_df['year'].values,
        name='GMT'
    )

    gmt_df = gmt_series.to_frame().T
    gmt_df['model'] = temp_df['model'].iloc[0]
    gmt_df['scenario'] = temp_df['scenario'].iloc[0]
    gmt_df['Ssp_family'] = temp_df['ssp_family'].iloc[0]

    return gmt_df.iloc[0]


def run_rime_prediction(
    gmt_input: pd.Series,
    region_array: RegionArray,
    basin_ids: list,
    variable: str,
    suban: str = "annual"
):
    """
    Run RIME to predict hydrological impacts for multiple basins.

    Args:
        gmt_input: GMT timeseries formatted for RIME
        region_array: RIME RegionArray
        basin_ids: List of basin IDs to predict
        variable: Variable name (e.g., 'qtot_mean', 'qr')
        suban: Subannual aggregation mode ("annual" or "2step")

    Returns:
        DataFrame with RIME predictions for all basins
    """
    print(f"\nRunning RIME predictions for {len(basin_ids)} basins...")
    print(f"  Variable: {variable}")
    print(f"  Subannual mode: {suban}")

    result = table_impacts_gwl(
        gmt_input,
        region_array,
        prefix_indicator='RIME|',
        ssp_meta_col='Ssp_family',
        local_temp_series=None
    )

    # Don't filter - return all results and let format conversion handle basin matching
    n_basins = len(result['region'].unique()) if len(result) > 0 else 0
    print(f"  Generated predictions for {n_basins} basins")

    return result


def save_results_iamc(
    results: pd.DataFrame,
    output_dir: Path,
    variable: str,
    suban: str,
    scenario: str
):
    """
    Save RIME prediction results in IAMC format.

    Args:
        results: RIME predictions DataFrame (IAMC format)
        output_dir: Directory to save results
        variable: Variable name
        suban: Subannual mode
        scenario: Scenario name for file naming
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    base_filename = f"rime_{variable}_{suban}_{scenario}_iamc"

    excel_file = output_dir / f"{base_filename}.xlsx"
    csv_file = output_dir / f"{base_filename}.csv"

    results.to_excel(excel_file, index=False)
    results.to_csv(csv_file, index=False)

    print(f"\nResults saved (IAMC format):")
    print(f"  Excel: {excel_file}")
    print(f"  CSV: {csv_file}")

    return excel_file, csv_file


def convert_to_message_format(
    results: pd.DataFrame,
    basin_mapping: pd.DataFrame,
    variable: str
):
    """
    Convert RIME IAMC format to MESSAGE water availability format.

    Args:
        results: RIME predictions in IAMC format
        basin_mapping: Basin mapping dataframe with BASIN_ID and row indices
        variable: Variable name (qtot_mean or qr)

    Returns:
        DataFrame in MESSAGE format (217 rows x year columns)
    """
    print(f"\nConverting to MESSAGE format...")

    # Filter to the specific variable
    var_name = f'RIME|{variable}'
    var_data = results[results['variable'] == var_name].copy()

    # Get year columns (everything that's numeric)
    year_cols = [col for col in var_data.columns
                 if isinstance(col, (int, np.integer)) or
                 (isinstance(col, str) and col.isdigit())]
    year_cols_sorted = sorted(year_cols, key=lambda x: int(x))

    # Initialize output dataframe with NaN
    output_df = pd.DataFrame(index=range(217), columns=year_cols_sorted, dtype=float)

    # Create lookup dict for RIME regions (convert to float for matching)
    rime_by_region = {float(region): var_data[var_data['region'] == region].iloc[0]
                      for region in var_data['region'].unique()}

    # Map RIME basin predictions to MESSAGE row indices
    basins_with_data = 0
    for idx, row in basin_mapping.iterrows():
        basin_id = float(row['BASIN_ID'])

        if basin_id in rime_by_region:
            basin_pred = rime_by_region[basin_id]
            # Extract year values
            for year in year_cols_sorted:
                output_df.loc[idx, year] = basin_pred[year]
            basins_with_data += 1

    # Reset index to match MESSAGE format (unnamed first column)
    output_df.index.name = None

    print(f"  MESSAGE format: {len(output_df)} rows x {len(output_df.columns)} years")
    print(f"  Basins with RIME data: {basins_with_data}/217")

    return output_df


def save_results_message(
    results: pd.DataFrame,
    basin_mapping: pd.DataFrame,
    output_dir: Path,
    variable: str,
    suban: str,
    scenario: str,
    model: str = None,
    percentile: float = None,
    run_id: int = None
):
    """
    Save RIME predictions in MESSAGE water availability format.

    Args:
        results: RIME predictions in IAMC format
        basin_mapping: Basin mapping dataframe
        output_dir: Directory to save results
        variable: Variable name
        suban: Subannual mode
        scenario: Scenario name
        model: Model name (optional)
        percentile: Temperature percentile (optional)
        run_id: MAGICC run_id (optional, for surrogate modeling)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert to MESSAGE format
    message_df = convert_to_message_format(results, basin_mapping, variable)

    # Use MESSAGE naming convention (qtot vs qtot_mean, qr vs qr)
    var_short = 'qtot' if 'qtot' in variable else 'qr'

    # Build filename with all parameters
    parts = [var_short, suban, scenario]
    if model:
        parts.append(model)
    # Always include either percentile or run_id in filename
    if run_id is not None:
        parts.append(f"run{run_id}")
    elif percentile is not None:
        parts.append(f"p{int(percentile)}")
    else:
        # Default to p50 if neither specified
        parts.append("p50")

    base_filename = "_".join(parts)
    csv_file = output_dir / f"{base_filename}.csv"

    message_df.to_csv(csv_file, index=True)

    print(f"\nResults saved (MESSAGE format):")
    print(f"  CSV: {csv_file}")

    return csv_file


def create_summary_plots(
    results: pd.DataFrame,
    temp_df: pd.DataFrame,
    output_dir: Path,
    variable: str,
    scenario: str
):
    """
    Create summary visualization plots.

    Args:
        results: RIME predictions DataFrame
        temp_df: Temperature input DataFrame
        output_dir: Directory to save plots
        variable: Variable name
        scenario: Scenario name
    """
    print("\nCreating summary plots...")

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    year_cols = [int(col) if isinstance(col, str) and col.isdigit() else col
                 for col in results.columns if isinstance(col, int) or (isinstance(col, str) and col.isdigit())]

    ax = axes[0, 0]
    ax.plot(temp_df['year'], temp_df['gsat_anomaly_K'], 'r-', linewidth=2)
    ax.set_xlabel('Year')
    ax.set_ylabel('GSAT Anomaly (K)')
    ax.set_title('MAGICC Temperature Projection')
    ax.grid(True, alpha=0.3)

    ax = axes[0, 1]
    for basin_id in R12_BASINS.keys():
        basin_data = results[results['region'] == basin_id]
        if len(basin_data) > 0:
            if variable == 'qtot_mean':
                var_name = f'RIME|{variable}'
            else:
                var_name = f'RIME|{variable}'

            var_data = basin_data[basin_data['variable'] == var_name]
            if len(var_data) > 0:
                values = [var_data[year].values[0] for year in year_cols if year in var_data.columns]
                ax.plot(year_cols[:len(values)], values, label=R12_BASINS[basin_id], alpha=0.7)

    ax.set_xlabel('Year')
    ylabel = 'Total Runoff (km³/year)' if variable == 'qtot_mean' else 'Groundwater Recharge (km³/year)'
    ax.set_ylabel(ylabel)
    ax.set_title(f'RIME Predictions by Basin - {variable}')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    ax.grid(True, alpha=0.3)

    ax = axes[1, 0]
    impacts_2050 = []
    impacts_2100 = []
    basin_labels = []

    for basin_id in R12_BASINS.keys():
        basin_data = results[results['region'] == basin_id]
        if len(basin_data) > 0:
            var_name = f'RIME|{variable}'
            var_data = basin_data[basin_data['variable'] == var_name]

            if len(var_data) > 0 and 2050 in var_data.columns and 2100 in var_data.columns:
                impacts_2050.append(var_data[2050].values[0])
                impacts_2100.append(var_data[2100].values[0])
                basin_labels.append(R12_BASINS[basin_id])

    x = np.arange(len(basin_labels))
    width = 0.35

    ax.bar(x - width/2, impacts_2050, width, label='2050', alpha=0.8)
    ax.bar(x + width/2, impacts_2100, width, label='2100', alpha=0.8)
    ax.set_xlabel('Basin')
    ax.set_ylabel(ylabel)
    ax.set_title(f'Basin Impacts: 2050 vs 2100')
    ax.set_xticks(x)
    ax.set_xticklabels(basin_labels, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')

    ax = axes[1, 1]
    all_temps = []
    all_impacts = []

    for basin_id in R12_BASINS.keys():
        basin_data = results[results['region'] == basin_id]
        if len(basin_data) > 0:
            var_name = f'RIME|{variable}'
            var_data = basin_data[basin_data['variable'] == var_name]

            if len(var_data) > 0:
                for year in year_cols:
                    if year in var_data.columns:
                        temp_idx = temp_df['year'] == year
                        if temp_idx.any():
                            all_temps.append(temp_df.loc[temp_idx, 'gsat_anomaly_K'].values[0])
                            all_impacts.append(var_data[year].values[0])

    ax.scatter(all_temps, all_impacts, alpha=0.3, s=20)
    ax.set_xlabel('GSAT Anomaly (K)')
    ax.set_ylabel(ylabel)
    ax.set_title('Impact vs Temperature (All Basins)')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    plot_file = output_dir / f"rime_summary_{variable}_{scenario}.png"
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"  Saved plot: {plot_file}")
    plt.close()


def run_rime(model=None, scenario=None, magicc_file=None, percentile=None, run_id=None, variable='both', suban='annual', output_dir=None, output_format='message'):
    """Run RIME predictions on MAGICC temperature output.

    Args:
        model: Model name (e.g., MESSAGE_GLOBIOM_SSP2_v6.4)
        scenario: Scenario name (e.g., baseline)
        magicc_file: Path to MAGICC Excel output file (overrides model/scenario)
        percentile: GSAT percentile to use (default: 50.0 if run_id not specified)
        run_id: Specific run_id to extract (0-599). If provided, percentile is ignored.
        variable: Hydrological variable to predict ('qtot_mean', 'qr', or 'both')
        suban: Subannual aggregation mode ('annual' or '2step')
        output_dir: Output directory for results
        output_format: Output format ('iamc' for 12 R12 regions or 'message' for 217 basins)
    """
    import argparse

    class Args:
        pass

    args = Args()

    # Construct path from model/scenario if provided
    if model and scenario and magicc_file is None:
        args.magicc_file = MAGICC_OUTPUT_DIR / f"{model}_{scenario}_magicc.xlsx"
    elif magicc_file:
        args.magicc_file = Path(magicc_file)
    else:
        args.magicc_file = None

    args.percentile = percentile
    args.run_id = run_id
    args.variable = variable
    args.suban = suban
    args.output_dir = Path(output_dir) if output_dir else None
    args.output_format = output_format

    if args.magicc_file is None:
        # Choose directory based on mode
        if args.run_id is not None:
            # run_id mode: use climate-assessment output
            climate_assessment_file = CLIMATE_ASSESSMENT_OUTPUT_DIR / "data_rawoutput.xlsx"
            if climate_assessment_file.exists():
                args.magicc_file = climate_assessment_file
                print(f"Using climate-assessment output: {args.magicc_file.name}")
            else:
                print(f"Error: No climate-assessment output found at {climate_assessment_file}")
                return 1
        else:
            # percentile mode: use old MAGICC output
            magicc_files = list(MAGICC_OUTPUT_DIR.glob("*_magicc.xlsx"))
            if not magicc_files:
                print(f"Error: No MAGICC output files found in {MAGICC_OUTPUT_DIR}")
                return 1
            args.magicc_file = max(magicc_files, key=lambda p: p.stat().st_mtime)
            print(f"Using most recent MAGICC file: {args.magicc_file.name}")

    if not args.magicc_file.exists():
        print(f"Error: MAGICC file not found: {args.magicc_file}")
        return 1

    if args.output_dir is None:
        args.output_dir = args.magicc_file.parent / "rime_predictions"

    print("="*80)
    print("RIME PREDICTIONS ON MAGICC EMISSIONS OUTPUT")
    print("="*80)
    print(f"MAGICC file: {args.magicc_file}")
    print(f"Percentile: {args.percentile}th")
    print(f"Variable(s): {args.variable}")
    print(f"Subannual mode: {args.suban}")
    print(f"Output format: {args.output_format}")
    print(f"Output directory: {args.output_dir}")
    print()

    # Always load basin mapping and predict for all MESSAGE basins
    print("Loading MESSAGE basin mapping...")
    basin_mapping_path = package_data_path("water", "delineation", "basins_by_region_simpl_R12.csv")
    basin_mapping = pd.read_csv(basin_mapping_path)
    # Get unique basin IDs from mapping (predict for all)
    basin_ids_to_predict = [float(bid) for bid in basin_mapping['BASIN_ID'].unique()]
    print(f"  Loaded mapping for {len(basin_mapping)} MESSAGE basins")
    print(f"  Will predict for {len(basin_ids_to_predict)} unique basin IDs")
    print()

    temp_df = load_magicc_temperature(args.magicc_file, percentile=args.percentile, run_id=args.run_id)

    gmt_input = create_rime_input(temp_df)

    variables = ['qtot_mean', 'qr'] if args.variable == 'both' else [args.variable]

    scenario_name = temp_df['scenario'].iloc[0].replace(' ', '_').replace('/', '_')

    for variable in variables:
        print("\n" + "="*80)
        print(f"Processing variable: {variable}")
        print("="*80)

        dataset_filename = f"rime_regionarray_{variable}_CWatM_{args.suban}_window0.nc"
        dataset_path = RIME_DATASETS_DIR / dataset_filename

        if not dataset_path.exists():
            print(f"Warning: RIME dataset not found: {dataset_path}")
            print(f"Skipping {variable}...")
            continue

        print(f"Loading RIME dataset: {dataset_filename}")
        region_array = RegionArray(str(dataset_path))
        print(f"  Dataset dimensions: {dict(region_array.dataset.dims)}")

        results = run_rime_prediction(
            gmt_input,
            region_array,
            basin_ids_to_predict,
            variable,
            args.suban
        )

        # Save in the appropriate format
        if args.output_format == 'message':
            save_results_message(
                results,
                basin_mapping,
                args.output_dir,
                variable,
                args.suban,
                scenario_name,
                model=temp_df['model'].iloc[0],
                percentile=args.percentile,
                run_id=args.run_id
            )
        else:
            save_results_iamc(
                results,
                args.output_dir,
                variable,
                args.suban,
                scenario_name
            )

        # Only create plots for IAMC format (12 basins manageable, 217 too many)
        if args.output_format == 'iamc':
            create_summary_plots(
                results,
                temp_df,
                args.output_dir,
                variable,
                scenario_name
            )

    print("\n" + "="*80)
    print("RIME PREDICTIONS COMPLETE")
    print("="*80)
    print(f"All results saved to: {args.output_dir}")

    return 0


import click

@click.command()
@click.option(
    '--model',
    help='Model name (e.g., MESSAGE_GLOBIOM_SSP2_v6.4)'
)
@click.option(
    '--scenario',
    help='Scenario name (e.g., baseline)'
)
@click.option(
    '--magicc-file',
    type=click.Path(path_type=Path),
    default=None,
    help='Path to MAGICC Excel output file (overrides model/scenario, default: most recent)'
)
@click.option(
    '--percentile',
    type=float,
    default=None,
    help='GSAT percentile to use (default: 50.0 if run-id not specified)'
)
@click.option(
    '--run-id',
    type=int,
    default=None,
    help='Specific run_id to extract (0-599). If provided, percentile is ignored.'
)
@click.option(
    '--variable',
    type=click.Choice(['qtot_mean', 'qr', 'both']),
    default='both',
    help='Hydrological variable to predict (default: both)'
)
@click.option(
    '--suban',
    type=click.Choice(['annual', '2step']),
    default='annual',
    help='Subannual aggregation mode (default: annual)'
)
@click.option(
    '--output-dir',
    type=click.Path(path_type=Path),
    default=None,
    help='Output directory for results (default: magicc_output/rime_predictions/)'
)
@click.option(
    '--output-format',
    type=click.Choice(['iamc', 'message']),
    default='message',
    help='Output format: iamc (12 R12 regions) or message (217 basins) (default: message)'
)
def main(model, scenario, magicc_file, percentile, run_id, variable, suban, output_dir, output_format):
    """Run RIME predictions on MAGICC temperature output.

    Example:
        mix-models alps run-rime --model MESSAGE_GLOBIOM_SSP2_v6.4 --scenario baseline
        mix-models alps run-rime --magicc-file path/to/file.xlsx --run-id 42
    """
    try:
        result = run_rime(model, scenario, magicc_file, percentile, run_id, variable, suban, output_dir, output_format)
        sys.exit(result)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
