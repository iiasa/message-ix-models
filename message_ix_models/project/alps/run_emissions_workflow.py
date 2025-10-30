"""Complete emissions reporting workflow for MESSAGE-GLOBIOM scenarios.

This script runs:
1. Legacy emissions reporting (iamc_report_hackathon.report)
   - Default: uses emissions_run_config.yaml (REQUIRED for v6.x scenarios)
   - Reads CH4/N2O from emission_factor with GAINS naming conventions
   - Extracts all emission species + GDP (emissions tables only)
   - Outputs to data/report/legacy/reporting_output/

2. Prep submission processing
   - Reads raw reporting output
   - Interpolates emissions 2020-2025 (adds 2021-2024)
   - Outputs final submission file

Usage:
    uv run --no-sync run_emissions_workflow.py \\
        --model "MESSAGE_GLOBIOM_SSP2_v6.1" \\
        --scenario "Main_baseline_baseline_nexus_7p0_high"
"""

import ixmp
import click
from pathlib import Path
from message_ix import Scenario
from message_data.tools.post_processing.iamc_report_hackathon import report
from message_data.tools.prep_submission import interpolate_emissions
import pandas as pd

@click.command()
@click.option(
    "--model",
    required=True,
    help="Model name (e.g., MESSAGE_GLOBIOM_SSP2_v6.1)"
)
@click.option(
    "--scenario",
    required=True,
    help="Scenario name (e.g., Main_baseline_baseline_nexus_7p0_high)"
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default="data/report/legacy/reporting_output",
    help="Output directory for reporting files"
)
@click.option(
    "--platform",
    default="ixmp_dev",
    help="IXMP platform name"
)
@click.option(
    "--run-config",
    default="emissions_run_config.yaml",
    help="Run config YAML file (default: emissions_run_config.yaml for v6.x scenarios)"
)
@click.option(
    "--merge-hist/--no-merge-hist",
    default=True,
    help="Merge historical data"
)
def main(model, scenario, output_dir, platform, run_config, merge_hist):
    """Run complete emissions reporting workflow."""

    mp = ixmp.Platform(name=platform)
    scen = Scenario(mp, model=model, scenario=scenario)

    print(f"Loaded {model}/{scenario} v{scen.version}, has_solution={scen.has_solution()}")

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    # Run emissions reporting
    try:
        report(
            mp=mp,
            scen=scen,
            run_config=run_config,
            merge_hist=merge_hist,
            merge_ts=False,
        )
    except Exception as e:
        print(f"Reporting failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Load and interpolate
    raw_file = output_dir_path / f"{model}_{scenario}.xlsx"
    if not raw_file.exists():
        raw_file = Path("reporting_output") / f"{model}_{scenario}.xlsx"
    if not raw_file.exists():
        print(f"Output file not found: {raw_file}")
        return 1

    df = pd.read_excel(raw_file)
    year_cols = [c for c in df.columns if isinstance(c, int)]
    print(f"Raw output: {df.shape}, years: {sorted(year_cols)}")

    # Set static_index for interpolate_emissions
    import message_data.tools.prep_submission as prep_mod
    prep_mod.static_index = ["Model", "Scenario", "Region", "Variable", "Unit"]

    df_interpolated = interpolate_emissions(df)
    year_cols_interp = [c for c in df_interpolated.columns if isinstance(c, int)]
    print(f"Interpolated: {df_interpolated.shape}, years: {sorted(year_cols_interp)}")

    final_file = output_dir_path / f"{model}_{scenario}_interpolated.xlsx"
    with pd.ExcelWriter(final_file, engine='xlsxwriter') as writer:
        df_interpolated.to_excel(writer, sheet_name='data', index=False)
    file_size = final_file.stat().st_size / 1024**2
    print(f"Saved: {final_file} ({file_size:.1f} MB)")

    return 0

if __name__ == "__main__":
    exit(main())
