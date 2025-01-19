import click


@click.group("prisma")
def cli():
    """PRISMA project.

    https://docs.messageix.org/projects/models/en/latest/project/prisma.html
    """


@cli.command("build-wp4")
@click.option("--tag", default="", help="Suffix to the scenario name")
@click.pass_obj
def build_wp4(context, tag):
    """Build PRISMA WP4 baseline"""
    from message_ix_models.project.prisma.build import (
        add_dri_update,
        add_eaf_bof_calibration,
        add_power_sector,
        resolve_infeasibilities,
    )

    scenario = context.get_scenario().clone(
        model=context.scenario_info["model"] + " (PRISMA)",
        scenario=context.scenario_info["scenario"] + tag,
        keep_solution=False,
    )
    add_power_sector(scenario)
    add_dri_update(scenario)
    add_eaf_bof_calibration(scenario)
    resolve_infeasibilities(scenario)
    scenario.solve(solve_options={"scaind": -1})
    scenario.set_as_default()


@click.command("report-wp4")
@click.option("--upload", default=True, help="Upload the results to the database")
def report_wp4(context, upload):
    from message_ix_models.model.material.report.run_reporting import run

    scenario = context.get_scenario().clone(
        model=context.scenario_info["model"] + "(PRISMA)",
        scenario=context.scenario_info["scenario"],
        keep_solution=False,
    )
    run(scenario, upload)
