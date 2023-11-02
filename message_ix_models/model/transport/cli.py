"""Command-line interface for MESSAGEix-Transport.

Use the :doc:`CLI <message_ix_models:cli>` command :program:`mix-models transport` to
invoke the commands defined in this module.

Use :program:`mix-models transport --help` to show a help text like::

    Usage: mix-models transport [OPTIONS] COMMAND [ARGS]...

      MESSAGEix-Transport variant.

    Options:
      --help  Show this message and exit.

    Commands:
      build       Prepare the model.
      debug       Temporary code for development.
      export-emi  Export emissions factors from base scenarios.
      migrate     Migrate data from MESSAGE(V)-Transport.
      solve       Run the model.

Each individual command also has its own help text; try e.g. :program:`mix-models
transport build --help`.

"""
import logging
from pathlib import Path

import click
from message_ix_models.util._logging import mark_time
from message_ix_models.util.click import PARAMS, common_params
from message_ix_models.workflow import make_click_command

log = logging.getLogger(__name__)


@click.group("transport")
@click.pass_obj
def cli(context):
    """MESSAGEix-Transport variant."""


# "run" subcommand to interact with the transport workflow
cli.add_command(
    make_click_command(
        f"{__package__}.workflow.generate",
        name="MESSAGEix-Transport",
        slug="transport",
        params=[
            click.Option(
                ["--future", "futures_scenario"], help="Transport futures scenario."
            ),
            click.Option(
                ["--fast"],
                is_flag=True,
                help="Skip removing data for removed set elements.",
            ),
            click.Option(
                ["--report", "report_build"],
                is_flag=True,
                help="Generate diagnostic reports of the built scenario.",
            ),
        ]
        + [PARAMS[n] for n in "dest dry_run nodes quiet".split()],
    )
)


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
    from message_data.tools import ScenarioInfo, silence_log

    from .build import main as build
    from .migrate import import_all, load_all, transform

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
@click.option("--future", "futures_scenario", help="Transport futures scenario")
@click.option(
    "--fast", is_flag=True, help="Skip removing data for removed set elements."
)
@click.option(
    "--report",
    "report_build",
    is_flag=True,
    help="Generate diagnostic reports of the built scenario.",
)
@click.pass_obj
def build_cmd(context, report_build, **options):
    """Prepare the model."""
    from . import build

    # Tidy options that were already handled by the top-level CLI
    [options.pop(k) for k in "dest dry_run regions quiet".split()]

    # Either clone from --dest, or create a new, bare RES
    scenario = context.clone_to_dest()
    platform = scenario.platform

    # Build MESSAGEix-Transport
    build.main(context, scenario, **options)

    mark_time()

    if report_build:
        # Also output diagnostic reports
        from message_ix_models.report import prepare_reporter, register

        from . import report

        register(report.callback)

        rep, key = prepare_reporter(
            scenario, context.get_config_file("report", "global")
        )

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


@cli.command("gen-activity")
@common_params("nodes years")
@click.option("--ssp", type=click.Choice("12345"))
@click.option("--ssp-update", type=click.Choice("12345"))
@click.argument("output_dir", metavar="DIR", type=Path, required=False, default=None)
@click.pass_obj
def gen_activity(ctx, ssp, ssp_update, output_dir, **kwargs):
    """Compute activity (demand) data and write to file in DIR."""
    from message_ix_models.project.ssp import SSP_2017, SSP_2024

    from . import build
    from .plot import ComparePDT, ComparePDTCap

    if ssp and ssp_update:
        raise click.UsageError("--ssp is mutually exclusive with --ssp-update")
    elif ssp:
        options = {"ssp": SSP_2017[ssp]}
        label = f"SSP_2017.{ssp}"
    elif ssp_update:
        options = {"ssp": SSP_2024[ssp_update]}
        label = f"SSP_2024.{ssp_update}"
    else:
        raise click.UsageError("Must give one of --ssp or --ssp-update")

    # Prepare a Computer instance for calculations
    c = build.get_computer(ctx, options=options)

    # Path to output file
    output_dir = output_dir or ctx.get_local_path(
        "transport", "gen-activity", f"{label}-{ctx.regions}-{ctx.years}"
    )
    c.configure(output_dir=output_dir)

    # Compute total activity by mode
    c.add("_1", "write_report", "pdt:n-y-t", output_dir.joinpath("pdt.csv"))
    c.add(
        "_2",
        "write_report",
        "transport pdt:n-y-t:capita",
        output_dir.joinpath("pdt-cap.csv"),
    )

    key = c.add(
        "gen-activity", ["_1", "_2", "plot demand-exo", "plot demand-exo-capita"]
    )

    log.info(f"Compute {repr(key)}\n{c.describe(key)}")
    output_dir.mkdir(exist_ok=True, parents=True)
    c.get(key)
    log.info(f"Wrote to {output_dir}")

    # Compare data
    for cls in ComparePDT, ComparePDTCap:
        key = c.add(f"compare {cls.kind}", cls, output_dir.parent)
        # FIXME on GitHub Actions—but *not* locally—the line above sets `key` to None.
        #       Debug, fix, and then remove the line below.
        key = f"compare {cls.kind}"
        c.get(key)


@cli.command()
@click.pass_obj
def debug(context):
    """Temporary code for development."""
    from message_ix_models import testing
    from message_ix_models.report import prepare_reporter, register
    from message_ix_models.util import private_data_path

    from . import build
    from .report import callback

    request = None
    context.model.regions = "R11"
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
    techs = context.transport.set["technology"]["remove"]

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


@cli.command()
@click.option("--go", is_flag=True, help="Actually manipulate files.")
def refresh(go):
    """Overwrite a local ixmp HyperSQL database with a fresh copy.

    Source and target platforms are read from the "transport refresh db" key in the
    user's ixmp config.json.

    Without --go, no action occurs.
    """
    # TODO move upstream, e.g. to ixmp JDBCBackend
    import shutil

    import ixmp

    # Read the from/to database names from user's ixmp config
    ixmp.config.register("transport refresh db", dict)
    ixmp.config.read()
    cfg = ixmp.config.get("transport refresh db")

    name_src = cfg["source"]
    name_dest = cfg["dest"]

    # Base paths for file operations
    dir_src = Path(ixmp.config.get_platform_info(name_src)[1]["path"]).parent
    dir_dest = Path(ixmp.config.get_platform_info(name_dest)[1]["path"]).parent

    msg = "" if go else "(dry run) "

    for path in dir_dest.glob(f"{name_dest}.*"):
        if path.suffix == ".tmp":
            continue
        print(f"{msg}Unlink {path}")
        if not go:
            continue
        path.unlink()

    for path in dir_src.joinpath("backup").glob(f"{name_src}.*"):
        if path.suffix in (".log", ".properties"):
            continue
        dst = dir_dest.joinpath(name_dest).with_suffix(path.suffix)

        print(f"{msg}Copy {path} → {dst}")
        if not go:
            continue
        shutil.copyfile(path, dst)
