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
    # Call .callback() to bypass Click's context creation on the decorated function
    main.callback(**kwargs)


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
def run_magicc_cmd(**kwargs):
    """Run MAGICC climate processing on MESSAGE emissions."""
    from .run_magicc_climate import run_magicc
    run_magicc(**kwargs)


@cli.command("scen-gen")
@click.option(
    "--config",
    type=click.Path(exists=True),
    required=True,
    help="Path to scenario_config.yaml"
)
@click.option(
    "--budgets",
    default=None,
    help="Comma-separated budget filter (e.g., '600f,850f')"
)
@click.option(
    "--temporal",
    type=click.Choice(["annual", "seasonal", "both"]),
    default="both",
    help="Temporal resolution filter (default: both)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate config without creating scenarios"
)
def scen_gen_cmd(config, budgets, temporal, dry_run):
    """Generate CID scenarios from YAML config.

    Creates MESSAGE scenarios with RIME water projections for multiple
    forcing scenarios (600f-2350f) and temporal resolutions (annual/seasonal).

    Examples:

        # Generate all 14 scenarios
        mix-models alps scen-gen --config scenario_config.yaml

        # Generate only 600f scenarios
        mix-models alps scen-gen --config scenario_config.yaml --budgets 600f

        # Generate only annual scenarios
        mix-models alps scen-gen --config scenario_config.yaml --temporal annual

        # Dry run to validate
        mix-models alps scen-gen --config scenario_config.yaml --dry-run
    """
    from pathlib import Path
    from .scenario_generator import load_config, generate_all

    cfg = load_config(Path(config))
    generate_all(cfg, budgets=budgets, temporal=temporal, dry_run=dry_run)


@cli.command("report")
@click.option(
    "--model",
    required=True,
    help="Model name"
)
@click.option(
    "--scenario",
    required=True,
    help="Scenario name"
)
@click.option(
    "--key",
    type=click.Choice(["all", "cooling_capacity", "cooling_activity", "water_extraction"]),
    default="all",
    help="Report key: all (default), cooling_capacity, cooling_activity, water_extraction"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=".",
    help="Output directory for report files"
)
@click.option(
    "--format",
    type=click.Choice(["csv", "xlsx", "parquet"]),
    default="csv",
    help="Output format (default: csv)"
)
@click.option(
    "--platform",
    default="ixmp_dev",
    help="ixmp platform name"
)
def report_cmd(model, scenario, key, output_dir, format, platform):
    """Run water/cooling reporting on a solved scenario.

    Extracts cooling and water data directly from genno aggregations:
    - Cooling capacity by type (once-through, closed-loop, air, saline)
    - Cooling activity by type
    - Water extraction capacity

    Examples:

        # Full report (all variables)
        mix-models alps report --model MODEL_NAME --scenario SCENARIO_NAME

        # Only cooling capacity
        mix-models alps report --model MODEL_NAME --scenario SCENARIO_NAME \\
            --key cooling_capacity

        # Output as Excel
        mix-models alps report --model MODEL_NAME --scenario SCENARIO_NAME \\
            --format xlsx --output-dir ./reports
    """
    from pathlib import Path
    from ixmp import Platform
    from message_ix import Scenario as MsgScenario

    from .report import report_water_cooling

    mp = Platform(platform)
    sc = MsgScenario(mp, model=model, scenario=scenario)

    if not sc.has_solution():
        print(f"ERROR: Scenario {model}/{scenario} has no solution")
        return

    print(f"Running water/cooling report for {model}/{scenario}...")
    # Map CLI key option to function keys parameter
    keys_map = {
        "all": None,  # None means all keys
        "cooling_capacity": ["cooling_cap"],
        "cooling_activity": ["cooling_act"],
        "water_extraction": ["water_cap", "water_act"],
    }
    keys_param = keys_map.get(key)

    df = report_water_cooling(
        sc,
        output_dir=Path(output_dir),
        keys=keys_param,
        format=format,
    )

    print(f"\nReport complete: {len(df)} rows")
    if "variable" in df.columns:
        print("\nVariables reported:")
        for var in sorted(df["variable"].unique()):
            n = len(df[df["variable"] == var])
            print(f"  - {var}: {n} rows")


@cli.command("report-batch")
@click.option(
    "--model",
    required=True,
    help="Model name"
)
@click.option(
    "--pattern",
    default=None,
    help="Filter scenarios by pattern (e.g., 'cooling', 'nexus')"
)
@click.option(
    "--scenarios",
    default=None,
    help="Comma-separated scenario names (alternative to --pattern)"
)
@click.option(
    "--key",
    type=click.Choice(["all", "cooling_capacity", "cooling_activity", "water_extraction"]),
    default="all",
    help="Report key (default: all)"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=".",
    help="Output directory for report files"
)
@click.option(
    "--format",
    type=click.Choice(["csv", "xlsx", "parquet"]),
    default="csv",
    help="Output format (default: csv)"
)
@click.option(
    "--platform",
    default="ixmp_dev",
    help="ixmp platform name"
)
def report_batch_cmd(model, pattern, scenarios, key, output_dir, format, platform):
    """Run water/cooling reporting on multiple scenarios.

    Processes scenarios sequentially with garbage collection to manage memory.

    Examples:

        # Report all cooling scenarios
        mix-models alps report-batch --model SSP_SSP2_v6.5_CID --pattern cooling

        # Report specific scenarios
        mix-models alps report-batch --model SSP_SSP2_v6.5_CID \\
            --scenarios cooling_850f,cooling_1100f
    """
    import gc
    import time
    from pathlib import Path

    from ixmp import Platform
    from message_ix import Scenario as MsgScenario

    from .report import report_water_cooling

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    mp = Platform(platform)

    # Get scenario list
    if scenarios:
        scenario_list = [s.strip() for s in scenarios.split(",")]
    else:
        all_scenarios = mp.scenario_list(model=model, default=True)
        if pattern:
            all_scenarios = all_scenarios[all_scenarios["scenario"].str.contains(pattern)]
        scenario_list = all_scenarios["scenario"].tolist()

    print(f"Found {len(scenario_list)} scenarios")
    if len(scenario_list) <= 20:
        print(f"Scenarios: {scenario_list}")
    print()

    # Map CLI key option to function keys parameter
    keys_map = {
        "all": None,  # None means all keys
        "cooling_capacity": ["cooling_cap"],
        "cooling_activity": ["cooling_act"],
        "water_extraction": ["water_cap", "water_act"],
    }
    keys_param = keys_map.get(key)

    results = []
    for scen_name in scenario_list:
        start_time = time.time()

        try:
            sc = MsgScenario(mp, model=model, scenario=scen_name)

            if not sc.has_solution():
                print(f"  [{scen_name}] No solution, skipping")
                results.append({"scenario": scen_name, "status": "no_solution", "rows": 0})
                continue

            df = report_water_cooling(sc, output_dir=output_path, keys=keys_param, format=format)
            elapsed = time.time() - start_time

            print(f"  [{scen_name}] {len(df)} rows, {elapsed:.1f}s")
            results.append({"scenario": scen_name, "status": "ok", "rows": len(df), "time": elapsed})

            del sc, df
            gc.collect()

        except Exception as e:
            print(f"  [{scen_name}] ERROR: {e}")
            results.append({"scenario": scen_name, "status": "error", "error": str(e)})

    ok_count = len([r for r in results if r["status"] == "ok"])
    print(f"\nCompleted {ok_count} / {len(scenario_list)} scenarios")
    print(f"Output saved to {output_path}")
