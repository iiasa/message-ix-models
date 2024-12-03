from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click

from message_ix_models.util.click import common_params

if TYPE_CHECKING:
    from message_ix_models import Context


@click.group("ssp")
def cli():
    """Shared Socioeconomic Pathways (SSP) project."""


@cli.command("gen-structures")
@common_params("dry_run")
@click.pass_obj
def gen_structures(context, **kwargs):
    """(Re)Generate the SSP data structures in SDMX."""
    from .structure import generate

    generate(context)


@cli.command("transport")
@click.argument("path_in", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument(
    "path_out",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
)
@click.pass_obj
def transport_cmd(context: "Context", path_in: Path, path_out: Optional[Path]):
    """Postprocess aviation emissions.

    Data are read from PATH_IN, in .xlsx or .csv format. If .xlsx, the data are first
    temporarily converted to .csv. Data are written to PATH_OUT; if not given, this
    defaults to the same path and suffix as PATH_IN, with "_out" added to the stem.
    """
    import pandas as pd
    from platformdirs import user_cache_path

    from .transport import main

    if path_in.suffix == ".xlsx":
        path_in_user = path_in
        path_in = user_cache_path("message-ix-models").joinpath(path_in.stem + ".csv")
        print(f"Convert Excel input to {path_in}")
        pd.read_excel(path_in_user).to_csv(path_in, index=False)
    else:
        path_in_user = path_in

    if path_out is None:
        path_out = path_in_user.with_name(
            path_in_user.stem + "_out" + path_in_user.suffix
        )
        print(f"No PATH_OUT given; write to {path_out}")

    if path_out.suffix == ".xlsx":
        path_out_user = path_out
        path_out = user_cache_path("message-ix-models").joinpath(path_out.stem + ".csv")
    else:
        path_out_user = path_out

    main(path_in, path_out)

    if path_out_user != path_out:
        print(f"Convert CSV output to {path_out_user}")
        pd.read_csv(path_out).to_excel(path_out_user)
