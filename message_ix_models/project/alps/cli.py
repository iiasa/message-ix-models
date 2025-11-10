import click


@click.group("alps")
def cli():
    """ALPS project workflows."""


@cli.command("emissions-report")
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
    type=click.Path(),
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
def emissions_report(**kwargs):
    """Run complete emissions reporting workflow."""
    from .run_emissions_workflow import main
    main(**kwargs)


@cli.command("run-magicc")
@click.option(
    '--scenario',
    required=True,
    help='Scenario name (Excel filename without .xlsx)'
)
@click.option(
    '--run-type',
    type=click.Choice(['fast', 'medium', 'complete']),
    default='medium',
    help='MAGICC run type: fast (1 config), medium (100 configs), complete (600 configs)'
)
@click.option(
    '--workers',
    type=int,
    default=4,
    help='Number of parallel workers for MAGICC'
)
@click.option(
    '--input-dir',
    type=click.Path(),
    default=None,
    help='Input directory for emissions files (default: data/report/legacy/reporting_output)'
)
@click.option(
    '--output-dir',
    type=click.Path(),
    default=None,
    help='Output directory for MAGICC results (default: data/report/legacy/reporting_output/climate_assessment_output)'
)
@click.option(
    '--return-all-runs/--no-return-all-runs',
    default=True,
    help='Return all individual run timeseries with run_id column (default: True)'
)
@click.option(
    '--generate-reference/--no-generate-reference',
    default=False,
    help='Also generate ISIMIP3b reference distribution (14 runs) for importance weighting (default: False)'
)
def run_magicc_cmd(**kwargs):
    """Run MAGICC climate processing on MESSAGE emissions."""
    from .run_magicc_climate import run_magicc
    run_magicc(**kwargs)


@cli.command("run-rime")
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
    type=click.Path(),
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
    help='Specific run_id to extract (0-99 for medium mode). If provided, percentile is ignored.'
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
    type=click.Path(),
    default=None,
    help='Output directory for results (default: magicc_output/rime_predictions/)'
)
@click.option(
    '--output-format',
    type=click.Choice(['iamc', 'message']),
    default='message',
    help='Output format: iamc (12 R12 regions) or message (217 basins) (default: message)'
)
def run_rime_cmd(**kwargs):
    """Run RIME predictions on MAGICC temperature output."""
    from .run_rime_on_magicc import run_rime
    run_rime(**kwargs)
