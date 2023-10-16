import logging
import re
from typing import Union

import click

from message_ix_models.util.click import common_params

from .structure import SSP, SSP_2017, SSP_2024, generate

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
    pass


@cli.command("gen-structures")
@common_params("dry_run")
@click.pass_obj
def gen_structures(context, **kwargs):
    """(Re)Generate the SSP data structures in SDMX."""
    generate(context)


@cli.command("make-test-data")
@click.argument(
    "filename",
    metavar="FILENAME",
    type=click.Choice(
        ["SSP-Review-Phase-1.csv.gz", "SspDb_country_data_2013-06-12.csv.zip"]
    ),
)
def make_test_data(filename):  # pragma: no cover
    """Create random data for testing."""
    from pathlib import Path

    import pandas as pd
    from numpy import char, random

    from message_ix_models.util import package_data_path, private_data_path

    # Paths
    p = Path("ssp", filename)
    path_in = private_data_path(p)
    path_out = package_data_path("test", p)

    # Read the data
    df = pd.read_csv(path_in, engine="pyarrow")

    # Determine its numeric columns (2000, 2001, etc.) and shape
    cols = list(filter(char.isnumeric, df.columns))
    size = (df.shape[0], len(cols))
    # - Generate random data of this shape.
    # - Keep only the elements corresponding to non-NA elements of `df`.
    # - Update `df` with these values.*
    generator = random.default_rng()
    df.update(df.where(df.isna(), pd.DataFrame(generator.random(size), columns=cols)))

    # Write to file, keeping only a few decimal points
    path_out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path_out, index=False, float_format="%.2f")
