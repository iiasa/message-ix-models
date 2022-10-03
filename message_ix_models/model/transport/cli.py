"""Command-line interface for MESSAGEix-Transport.

Use the :doc:`CLI <cli>` command ``mix-models transport`` to invoke the commands
defined in this module.
Use :command:`mix-models transport --help` to show a help text like::

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

Each individual command also has its own help text; try e.g. :command:`mix-models
transport build --help`.

"""
import logging
from itertools import product
from pathlib import Path

import click
from message_ix_models.util._logging import mark_time
from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


@click.group("transport")
@click.pass_obj
def cli(context):
    """MESSAGEix-Transport variant."""


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
@click.option("--future", help="Transport futures scenario")
@click.option(
    "--fast", is_flag=True, help="Skip removing data for removed set elements."
)
@click.option("--report", help="Path for diagnostic reports of the built scenario.")
@click.pass_obj
def build_cmd(context, **options):
    """Prepare the model."""
    from message_data.model.transport import build

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
@click.option("--go", is_flag=True, hidden=True)  # Currently unused
@click.pass_context
def batch(click_ctx, go):
    """Generate commands to handle batches of scenarios."""
    # from message_ix_models.cli import solve_cmd

    # Items for Cartesian product
    actions = [
        "build",
        "solve",
        "report",
    ]
    model_names = ["MESSAGEix-Materials"]
    scenario_version = [
        "NoPolicy_2305#1",
        # "EN_NPi2020_1000f#1",  # with model name "ENGAGE_SSP2_v4.1.7"
    ]
    options = {
        "": "",
        # "A---": '--future="A---"',
    }

    # Accumulate command fragments
    commands = []

    guard = "" if go else "$"

    for action, m, sv, (label, opt) in product(
        actions, model_names, scenario_version, options.items()
    ):
        # Source and destination URLs
        src = f"ixmp://local/{m}/{sv}"
        dest = f"ixmp://local/{m}+transport/{sv.split('#')[0]} {label}".rstrip()

        # Assemble a command fragment
        if action == "build":
            print(
                f'{guard} mix-models --url="{src}" transport build --fast '
                f'--dest="{dest}" {opt}'
            )
            # commands.append([src, build_cmd, build_opts])
        elif action == "solve":
            print(f'{guard} message-ix --url="{dest}" solve')
            # commands.append([dest, solve_cmd, dict()])
        elif action == "report":
            print(
                f'{guard} mix-models --url="{dest}" report -m model.transport '
                '"transport all"'
            )
            # commands.append([])
        else:
            raise NotImplementedError

    return
    # NB the following code would run each of the commands directly through click self-
    #    invocation, within the same Python session. This tends to trigger memory
    #    overruns, so it is currently disabled.

    if getattr(click_ctx.obj, "url", False):
        log.warning(f"Ignoring --url={click_ctx.obj.url}")

    ctx = click_ctx.obj

    for url, cmd, opts in commands:
        log.info(f"Invoke: {cmd} {url} {opts}")
        # continue  # for debugging

        # Store certain settings on the message_ix_models.Context object which cannot
        # be passed through invoke()
        ctx.handle_cli_args(url=url)
        if "dest" in opts:
            ctx.handle_cli_args(
                url=opts.pop("dest"), _store_as=("dest_platform", "dest_scenario")
            )

        click_ctx.invoke(cmd, **opts)

        ctx.close_db()


@cli.command("gen-demand")
@common_params("nodes years")
@click.argument("source", metavar="DATASOURCE")
@click.argument("output_dir", metavar="DIR", type=Path, required=False, default=None)
@click.pass_obj
def gen_demand(ctx, source, nodes, years, output_dir):
    """Compute activity (demand) data and write to file in DIR.

    The indicated DATASOURCE is used for exogenous GDP and population data; both inputs
    to the calculation of demands.
    """
    import message_ix
    from genno import Key

    from message_data.model.transport import build, configure, demand

    # Read general transport config
    ctx.regions = nodes
    ctx.years = years

    options = {"data source": {"gdp": source, "population": source}}
    configure(ctx, options=options)

    # Get a spec for the structure of a target model
    spec = build.get_spec(ctx)

    # Prepare a Reporter object for demand calculations
    rep = message_ix.Reporter()
    demand.prepare_reporter(rep, context=ctx, exogenous_data=True, info=spec["add"])
    rep.configure(output_dir=output_dir)

    output_dir = output_dir or ctx.get_local_path("output")
    output_path = output_dir.joinpath(
        f"demand-{source.replace(' ', '_')}-{ctx.regions}-{ctx.years}.csv"
    )

    # Compute total demand by mode
    key = Key("transport pdt", "nyt")
    rep.add("write_report", "gen-demand", key, output_path)

    log.info(f"Compute {repr(key)}")
    output_dir.mkdir(exist_ok=True, parents=True)
    rep.get("gen-demand")
    log.info(f"Wrote to {output_path}")

    # # Generate diagnostic plots
    # rep.add("demand plots", ["plot demand-exo", "plot demand-exo-capita"])
    # rep.get("demand plots")


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
