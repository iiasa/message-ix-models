"""Utilities for using :doc:`Pooch <pooch:about>`."""
import logging
from pathlib import Path
from typing import Tuple

import click
import pooch

from .context import Context

log = logging.getLogger(__name__)


class Unzip:
    """:class:`pooch.Unzip` streamlined using :mod:`pathlib`."""

    def __init__(self, members=None):
        self.members = members

    def __call__(self, fname, action, pooch):
        import zipfile

        path = Path(fname)
        extract_dir = path.parent
        extract_dir.mkdir(parents=True, exist_ok=True)

        members = self.members
        with zipfile.ZipFile(path, "r") as zf:
            if members is None:
                members = zf.namelist()
                log.info(f"Unpack all {len(members)} members of {path}")

            zf.extractall(members=members, path=extract_dir)

        return members


#: Supported remote sources of data.
SOURCE = {
    "PRIMAP": dict(
        pooch_args=dict(
            base_url="ftp://datapub.gfz-potsdam.de/download/10.5880.PIK.2019.001/",
            registry={
                "PRIMAP-hist_v2.0_11-Dec-2018.zip": (
                    "md5:f28d58abef4ecfc36fc8ce3e9eef2871"
                ),
            },
        ),
        processor=Unzip(members=["PRIMAP-hist_v2.0_11-Dec-2018.csv"]),
    )
}


def fetch(pooch_args, **fetch_kwargs) -> Tuple[Path, ...]:
    """Create a :class:`~pooch.Pooch` instance and fetch a single file.

    Files are stored under the directory identified by :meth:`.Context.get_cache_path`,
    unless `args` provides another location.

    Parameters
    ----------
    args
        Passed to :func:`pooch.create`.
    kwargs
        Passed to :meth:`pooch.Pooch.fetch`.

    Returns
    -------
    Path
        Path to the fetched file.

    See also
    --------
    :func:`.snapshot.load`
    """
    pooch_args.setdefault("path", Context.get_instance(-1).get_cache_path())

    p = pooch.create(**pooch_args)

    if len(p.registry) > 1:  # pragma: no cover
        raise NotImplementedError("fetch() with registries with >1 files")

    filenames = p.fetch(next(iter(p.registry.keys())), **fetch_kwargs)

    # Convert to pathlib.Path
    paths = tuple(map(Path, filenames))

    log.info(
        "Fetched"
        + (f" {paths[0]}" if len(paths) == 1 else "\n".join(map(str, (":",) + paths)))
    )

    return paths


@click.command("fetch")
@click.argument("source", metavar="SOURCE", type=click.Choice(list(SOURCE.keys())))
@click.pass_obj
def cli(context, source):
    """Retrieve data from primary sources."""
    from message_ix_models.util import pooch

    pooch.fetch(**SOURCE[source], progressbar=True)
