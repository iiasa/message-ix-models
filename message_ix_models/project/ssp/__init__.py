import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import click

from message_ix_models.util.click import common_params

from .structure import SSP, SSP_2017, SSP_2024, generate

if TYPE_CHECKING:
    from message_ix_models import Context

__all__ = [
    "SSP",
    "SSP_2017",
    "SSP_2024",
    "generate",
    "parse",
    "ssp_field",
]

log = logging.getLogger(__name__)


def parse(value: Union[str, SSP_2017, SSP_2024]) -> Union[SSP_2017, SSP_2024]:
    """Parse `value` to a member of :data:`SSP_2017` or :data:`SSP_2024`."""
    if isinstance(value, (SSP_2017, SSP_2024)):
        return value

    log.debug(f"Assume {value!r} is from {SSP_2017}")

    if isinstance(value, str):
        return SSP_2017[re.sub("SSP([12345])", r"\1", value)]
    else:
        return SSP_2017(value)


class ssp_field:
    """SSP field for use in data classes."""

    def __init__(self, default: Union[SSP_2017, SSP_2024]):
        self._default = default

    def __set_name__(self, owner, name):
        self._name = "_" + name

    def __get__(self, obj, type) -> Union[SSP_2017, SSP_2024]:
        if obj is None:
            return None  # type: ignore [return-value]

        try:
            return obj.__dict__[self._name]
        except KeyError:
            return obj.__dict__.setdefault(self._name, self._default)

    def __set__(self, obj, value):
        if value is None:
            value = self._default
        setattr(obj, self._name, parse(value))


@click.group("ssp")
def cli():
    """Shared Socioeconomic Pathways (SSP) project."""


@cli.command("gen-structures")
@common_params("dry_run")
@click.pass_obj
def gen_structures(context, **kwargs):
    """(Re)Generate the SSP data structures in SDMX."""
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
    """Postprocess aviation emissions."""
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
