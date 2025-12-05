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
    type=click.Choice(['qtot_mean', 'qr', 'hydro', 'capacity_factor']),
    default='hydro',
    help='Variable to predict: qtot_mean, qr, hydro (both hydrological), or capacity_factor (default: hydro)'
)
@click.option(
    '--suban/--no-suban',
    default=False,
    help='Use seasonal (2-step) temporal resolution instead of annual (default: annual)'
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
@click.option(
    '--weighted/--no-weighted',
    default=False,
    help='Compute importance-weighted expectations and CVaR (disabled by default: no significant effect detected)'
)
@click.option(
    '--n-runs',
    type=int,
    default=None,
    help='Number of runs to process in weighted mode (default: all)'
)
@click.option(
    '--cvar-levels',
    default='10,50,90',
    help='Comma-separated CVaR percentiles for weighted mode (default: 10,50,90)'
)
@click.option(
    '--gwl-bin-width',
    type=float,
    default=0.5,
    help='GWL bin width for importance weighting in °C (default: 0.5)'
)
@click.option(
    '--include-emulator-uncertainty/--no-include-emulator-uncertainty',
    default=False,
    help='Propagate RIME emulator uncertainty using stratified sampling (can be combined with --weighted)'
)
@click.option(
    '--cvar-method',
    type=click.Choice(['pointwise', 'coherent']),
    default='coherent',
    help='CVaR computation method: coherent (trajectory-based) or pointwise (independent per timestep) (default: coherent)'
)
def run_rime_cmd(**kwargs):
    """Run RIME predictions on MAGICC temperature output."""
    from .run_rime_on_magicc import run_rime
    # Parse CVaR levels string to list
    cvar_str = kwargs.pop('cvar_levels')
    kwargs['cvar_levels'] = [float(x.strip()) for x in cvar_str.split(',')]
    # Remove unused parameters
    kwargs.pop('output_format', None)
    run_rime(**kwargs)


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


# ============================================================================
# Analysis commands
# ============================================================================


@cli.command("extract-costs")
@click.option(
    "--model",
    required=True,
    help="Model name (e.g., SSP_SSP2_v6.5_CID)"
)
@click.option(
    "--scenarios",
    required=True,
    help="'annual', 'seasonal', or comma-separated scenario names"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=".",
    help="Output directory for CSV files"
)
@click.option(
    "--platform",
    default="ixmp_dev",
    help="ixmp platform name"
)
def extract_costs_cmd(model, scenarios, output_dir, platform):
    """Extract nodal costs from solved scenarios.

    Extracts COST_NODAL variable and saves as CSV (basins × years).

    Examples:

        # Extract from all annual scenarios
        mix-models alps extract-costs --model SSP_SSP2_v6.5_CID --scenarios annual

        # Extract from specific scenarios
        mix-models alps extract-costs --model SSP_SSP2_v6.5_CID \\
            --scenarios nexus_baseline_annual,nexus_baseline_600f_annual
    """
    from pathlib import Path
    from .analysis import expand_scenario_alias, load_scenarios, extract_nodal_costs, pivot_to_wide

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    scenario_list = expand_scenario_alias(scenarios)
    print(f"Loading {len(scenario_list)} scenarios...")

    mp, scens = load_scenarios(model, scenario_list, platform)
    print(f"\nExtracting costs...")
    costs = extract_nodal_costs(scens)
    del mp  # prevent GC until extraction complete

    print(f"\nSaving to {output_path}...")
    for name, df in costs.items():
        wide = pivot_to_wide(df, "node", "year", "cost")
        outfile = output_path / f"nodal_costs_{name}.csv"
        wide.to_csv(outfile)
        print(f"  {outfile.name}: {wide.shape}")

    print("\nDone!")


@cli.command("extract-cids")
@click.option(
    "--model",
    required=True,
    help="Model name"
)
@click.option(
    "--scenarios",
    required=True,
    help="'annual', 'seasonal', or comma-separated scenario names"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=".",
    help="Output directory for CSV files"
)
@click.option(
    "--platform",
    default="ixmp_dev",
    help="ixmp platform name"
)
def extract_cids_cmd(model, scenarios, output_dir, platform):
    """Extract water CID parameters from scenarios.

    Extracts surfacewater demand, groundwater demand, and GW share constraint.

    Examples:

        mix-models alps extract-cids --model SSP_SSP2_v6.5_CID --scenarios annual
    """
    from pathlib import Path
    from .analysis import expand_scenario_alias, load_scenarios, extract_water_cids

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    scenario_list = expand_scenario_alias(scenarios)
    print(f"Loading {len(scenario_list)} scenarios...")

    mp, scens = load_scenarios(model, scenario_list, platform, check_solved=False)
    print(f"\nExtracting water CIDs...")
    cids = extract_water_cids(scens)
    del mp  # prevent GC until extraction complete

    print(f"\nSaving to {output_path}...")
    for name, params in cids.items():
        for param_name, df in params.items():
            outfile = output_path / f"water_cid_{param_name}_{name}.csv"
            df.to_csv(outfile, index=False)
        print(f"  {name}: 3 parameter files")

    print("\nDone!")


@cli.command("compare")
@click.option(
    "--model",
    required=True,
    help="Model name"
)
@click.option(
    "--scenarios",
    required=True,
    help="'annual', 'seasonal', or comma-separated scenario names"
)
@click.option(
    "--baseline",
    required=True,
    help="Baseline scenario name for comparison"
)
@click.option(
    "--variable",
    type=click.Choice(["costs", "surfacewater", "groundwater", "all"]),
    default="costs",
    help="Variable to compare (default: costs)"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=".",
    help="Output directory for results"
)
@click.option(
    "--platform",
    default="ixmp_dev",
    help="ixmp platform name"
)
def compare_cmd(model, scenarios, baseline, variable, output_dir, platform):
    """Compare scenarios against baseline.

    Computes differences and summary statistics.

    Examples:

        mix-models alps compare --model SSP_SSP2_v6.5_CID --scenarios annual \\
            --baseline nexus_baseline_annual --variable costs
    """
    from pathlib import Path
    from .analysis import (
        expand_scenario_alias, load_scenarios, extract_nodal_costs,
        extract_water_cids, pivot_to_wide, compute_scenario_diffs,
        build_comparison_table, compute_yearly_comparison,
    )
    from .analysis.compare import compute_yearly_diffs

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    scenario_list = expand_scenario_alias(scenarios)
    print(f"Loading {len(scenario_list)} scenarios...")

    check_solved = variable == "costs"
    mp, scens = load_scenarios(model, scenario_list, platform, check_solved=check_solved)

    def compare_variable(data_dict, var_name):
        # Build wide-format data
        wide_data = {}
        for name, df in data_dict.items():
            if isinstance(df, dict):
                df = df[var_name]
            if "cost" in df.columns:
                wide_data[name] = pivot_to_wide(df, "node", "year", "cost")
            elif "value" in df.columns:
                wide_data[name] = pivot_to_wide(df, "node", "year", "value")
            else:
                wide_data[name] = df

        # Compute comparisons
        summary = build_comparison_table(wide_data, baseline)
        yearly = compute_yearly_comparison(wide_data)
        yearly_diffs = compute_yearly_diffs(wide_data, baseline)

        # Save results
        summary.to_csv(output_path / f"summary_{var_name}.csv", index=False)
        yearly.to_csv(output_path / f"yearly_{var_name}.csv")
        yearly_diffs.to_csv(output_path / f"yearly_diffs_{var_name}.csv")

        print(f"\n{var_name.upper()} Summary:")
        print(summary.to_string(index=False))

    if variable in ["costs", "all"]:
        print("\nExtracting costs...")
        costs = extract_nodal_costs(scens)
        compare_variable(costs, "costs")

    if variable in ["surfacewater", "groundwater", "all"]:
        print("\nExtracting water CIDs...")
        cids = extract_water_cids(scens)

        if variable in ["surfacewater", "all"]:
            sw_data = {name: params["surfacewater"] for name, params in cids.items()}
            compare_variable(sw_data, "surfacewater")

        if variable in ["groundwater", "all"]:
            gw_data = {name: params["groundwater"] for name, params in cids.items()}
            compare_variable(gw_data, "groundwater")

    print(f"\nResults saved to {output_path}")


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


@cli.command("validate")
@click.option(
    "--model",
    required=True,
    help="Model name"
)
@click.option(
    "--scenarios",
    required=True,
    help="'annual', 'seasonal', or comma-separated scenario names"
)
@click.option(
    "--baseline",
    required=True,
    help="Baseline scenario name"
)
@click.option(
    "--variable",
    type=click.Choice(["surfacewater", "groundwater", "all"]),
    default="all",
    help="Variable to validate (default: all)"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=".",
    help="Output directory for validation results"
)
@click.option(
    "--platform",
    default="ixmp_dev",
    help="ixmp platform name"
)
def validate_cmd(model, scenarios, baseline, variable, output_dir, platform):
    """Validate CID scenario ensemble for monotonicity and coherence.

    Computes per-basin Spearman correlations across forcing levels
    and temporal autocorrelations for coherence checking.

    Examples:

        mix-models alps validate --model SSP_SSP2_v6.5_CID --scenarios annual \\
            --baseline nexus_baseline_annual --output-dir ./validation
    """
    from pathlib import Path
    from .analysis import (
        expand_scenario_alias, load_scenarios, extract_water_cids,
        pivot_to_wide, validate_scenario_ensemble,
    )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    scenario_list = expand_scenario_alias(scenarios)
    print(f"Loading {len(scenario_list)} scenarios...")

    mp, scens = load_scenarios(model, scenario_list, platform, check_solved=False)

    print("\nExtracting water CIDs...")
    cids = extract_water_cids(scens)
    del mp  # prevent GC until extraction complete

    def run_validation(data_dict, var_name):
        # Build wide-format data
        wide_data = {}
        for name, params in cids.items():
            df = params[var_name]
            wide_data[name] = pivot_to_wide(df, "node", "year", "value")

        print(f"\nRunning {var_name} validation...")
        results = validate_scenario_ensemble(wide_data, baseline)

        # Save results
        results["monotonicity"].to_csv(output_path / f"monotonicity_{var_name}.csv", index=False)
        results["coherence"].to_csv(output_path / f"coherence_{var_name}.csv", index=False)
        results["summary"].to_csv(output_path / f"validation_summary_{var_name}.csv", index=False)

        print(f"\n{var_name.upper()} Validation Summary:")
        print(results["summary"].to_string(index=False))

    if variable in ["surfacewater", "all"]:
        run_validation(cids, "surfacewater")

    if variable in ["groundwater", "all"]:
        run_validation(cids, "groundwater")

    print(f"\nResults saved to {output_path}")
