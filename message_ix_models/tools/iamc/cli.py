"""CLI for IAMC community diagnostic assessment protocol."""

import click

from message_ix_models.util.click import common_params


@click.group("iamc")
@click.pass_obj
def cli(context):
    """IAMC community diagnostic assessment protocol tools."""


@cli.command("run-diagnostic")
@click.pass_obj
@click.option("--base-scenario", required=True, help="Scenario to clone from.")
@click.option(
    "--cp-scenario",
    default="CP",
    show_default=True,
    help="Current-policies reference scenario (source of pre-2030 prices).",
)
@click.option(
    "--first-model-year",
    type=int,
    default=2025,
    show_default=True,
    help="First model year after clone (shift_first_model_year).",
)
@click.option(
    "--scenarios",
    default="mandatory",
    show_default=True,
    help=(
        "Which scenarios to build. Options: "
        "'mandatory' (all TIER.Mandatory), "
        "'all' (all tiers), "
        "or comma-separated ids e.g. 'CP,C400-lin,C160-gr5'."
    ),
)
@click.option(
    "--solve/--no-solve",
    default=True,
    show_default=True,
    help="Solve after building each scenario.",
)
@common_params("run_reporting_only")
def run_diagnostic(
    context,
    base_scenario,
    cp_scenario,
    first_model_year,
    scenarios,
    solve,
    run_reporting_only,
):
    """Build and solve IAMC diagnostic assessment protocol scenarios.

    Clones BASE_SCENARIO for each active diagnostic scenario, then applies
    protocol carbon prices and constraints per Table 1 & 2 of
    doi:10.5281/zenodo.19554965.

    \b
    Examples
    --------
    Run all mandatory scenarios (solve each):

        mix-models --model "MESSAGEix-GLOBIOM 2.0" \\
            iamc run-diagnostic \\
            --base-scenario "SSP2-Base-2025" \\
            --cp-scenario "CP" \\
            --scenarios mandatory

    Build (no solve) a single scenario:

        mix-models --model "MESSAGEix-GLOBIOM 2.0" \\
            iamc run-diagnostic \\
            --base-scenario "SSP2-Base-2025" \\
            --scenarios "C400-lin" \\
            --no-solve
    """
    from message_ix_models.tools.iamc.runner import ScenarioRunner
    from message_ix_models.tools.iamc.structure import STATIC, TIER

    if scenarios == "all":
        active_ids = [e["id"] for e in STATIC]
    elif scenarios == "mandatory":
        active_ids = [e["id"] for e in STATIC if e["tier"] == TIER.Mandatory]
    else:
        valid = {e["id"] for e in STATIC}
        active_ids = [s.strip() for s in scenarios.split(",")]
        unknown = [s for s in active_ids if s not in valid]
        if unknown:
            raise click.BadParameter(
                f"Unknown scenario id(s): {unknown}. "
                f"Valid ids: {sorted(valid)}",
                param_hint="--scenarios",
            )

    dest_scen = {sid: {"active": True, "solve": solve} for sid in active_ids}

    context.run_reporting_only = run_reporting_only
    context.project_config = {
        "create_diagnostic": {
            "base_scenario": base_scenario,
            "cp_scenario": cp_scenario,
            "firstmodelyear": first_model_year,
            "reporting": False,
            "dest_scen": dest_scen,
        }
    }

    rs = ScenarioRunner(context)
    rs.run("create_diagnostic")
