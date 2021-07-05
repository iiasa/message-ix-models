import logging
from pathlib import Path

import click
from message_ix_models.util._logging import mark_time
from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


@click.group("transport")
@click.pass_obj
def cli(context):
    """MESSAGEix-Transport variant."""
    from .utils import read_config

    # Ensure transport model configuration is loaded
    read_config(context)


@cli.command()
@common_params("dest")
@click.option(
    "--version",
    default="geam_ADV3TRAr2_BaseX2_0",
    metavar="VERSION",
    help="Model version to read.",
)
@click.option(
    "--check-base/--no-check-base",
    is_flag=True,
    help="Check properties of the base scenario (default: no).",
)
@click.option(
    "--parse/--no-parse",
    is_flag=True,
    help="(Re)parse MESSAGE V data files (default: no).",
)
@click.option(
    "--region", default="", metavar="REGIONS", help="Comma-separated region(s)."
)
@click.argument("SOURCE_PATH", required=False, default=Path("reference", "data"))
@click.pass_obj
def migrate(context, version, check_base, parse, region, source_path, dest):
    """Migrate data from MESSAGE(V)-Transport.

    If --parse is given, data from .chn, .dic, and .inp files is read from SOURCE_PATH
    for VERSION. Values are extracted and cached.

    Data is transformed to be suitable for the target scenario, and stored in
    migrate/VERSION/*.csv.
    """
    from message_data.tools import ScenarioInfo

    from .build import main as build
    from .migrate import import_all, load_all, transform
    from .utils import silence_log

    # Load the target scenario from database
    # mp = context.get_platform()
    s_target = dest
    info = ScenarioInfo(s_target)

    # Check that it has the required features
    if check_base:
        with silence_log():
            build(s_target, dry_run=True)
            print(
                f"Scenario {s_target} is a valid target for building "
                "MESSAGEix-Transport."
            )

    if parse:
        # Parse raw data
        data = import_all(source_path, nodes=region.split(","), version=version)
    else:
        # Load cached data
        data = load_all(version=version)

    # Transform the data
    transform(data, version, info)


@cli.command("build")
@common_params("dest dry_run regions quiet")
@click.option(
    "--fast", is_flag=True, help="Skip removing data for removed set elements."
)
@click.option("--report", help="Path for diagnostic reports of the built scenario.")
@click.pass_obj
def build_cmd(context, dest, **options):
    """Prepare the model."""
    from message_ix_models.model import bare

    from message_data.model.transport import build

    # Handle --regions; use a sensible default for MESSAGEix-Transport
    regions = options.pop("regions", None)
    if not regions:
        log.info("Use default --regions=R11")
        regions = "R11"
    context.regions = regions
    context.years = "A"

    # Other defaults from .model.bare
    context.use_defaults(bare.SETTINGS)

    # Either clone from --dest, or create a new, bare RES
    scenario = context.clone_to_dest()
    platform = scenario.platform

    # Build MESSAGEix-Transport
    build.main(context, scenario, **options)

    mark_time()

    if options["report"]:
        # Also output diagnostic reports
        from message_data.model.transport import report
        from message_data.reporting import prepare_reporter, register

        register(report.callback)

        rep, key = prepare_reporter(
            scenario, context.get_config_file("report", "global")
        )
        rep.configure(output_dir=Path(options["report"]).expanduser())

        # Add a catch-all key, including plots etc.
        rep.add(
            "_plots",
            ["plot demand-exo", "plot var-cost", "plot fix-cost", "plot inv-cost"],
        )

        mark_time()

        log.info(f"Report plots to {rep.graph['config']['output_dir']}")
        log.debug(rep.describe("_plots"))

        rep.get("_plots")

        mark_time()

    del platform


@cli.command()
@click.option("--macro", is_flag=True)
@click.pass_obj
def solve(context, macro):
    """Run the model."""
    args = dict()

    scenario = context.get_scenario()

    if macro:
        from .callback import main as callback

        args["callback"] = callback

    scenario.solve(**args)
    scenario.commit()


@cli.command()
@click.pass_obj
def debug(context):
    """Temporary code for development."""
    from message_ix_models import testing
    from message_ix_models.util import private_data_path

    from message_data.model.transport import build
    from message_data.model.transport.report import callback
    from message_data.reporting import prepare_reporter, register

    request = None
    context.regions = "R11"
    context.years = "A"
    context.res_with_dummies = True

    # Retrieve (maybe generate) the bare RES with the same settings
    res = testing.bare_res(request, context, solved=False)
    # Derive the name for the transport scenario
    model_name = res.model.replace("-GLOBIOM", "-Transport")
    # Build
    log.info(f"Create '{model_name}/baseline' for testing")
    scenario = res.clone(model=model_name)
    build.main(context, scenario, fast=True, quiet=False)
    # Solve
    log.info(f"Solve '{scenario.model}/{scenario.scenario}'")
    scenario.solve(solve_options=dict(lpmethod=4))

    # Prepare reporting
    register(callback)
    rep, key = prepare_reporter(
        scenario,
        private_data_path("report", "global.yaml"),
        "plot ldv-tech-share-by-cg",
    )

    # Configure output
    rep.configure(output_dir=Path.cwd())

    # Get the catch-all key, including plots etc.
    rep.get(key)


@cli.command("export-emi")
@click.pass_obj
@click.argument("path_stem")
def export_emissions_factors(context, path_stem):
    """Export emissions factors from base scenarios.

    Only the subset corresponding to removed transport technologies are exported.
    """
    from datetime import datetime

    from message_ix_models.util import private_data_path

    # List of techs
    techs = context["transport set"]["technology"]["remove"]

    # Load the targeted scenario
    scenario = context.get_scenario()

    # Output path
    out_dir = private_data_path("transport", "emi")
    out_dir.mkdir(exist_ok=True, parents=True)

    for name in ("emission_factor", "relation_activity"):
        df = scenario.par(name, filters=dict(technology=techs))

        path = out_dir / f"{path_stem}-{name}.csv"
        log.info(f"Write {len(df)} rows to {path}")

        # Write header
        path.write_text(
            f"# Exported {datetime.today().isoformat()} from\n# {context.url}\n"
        )
        # Write data
        df.to_csv(path, mode="a", index=False)
