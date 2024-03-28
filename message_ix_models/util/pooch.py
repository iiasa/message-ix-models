"""Utilities for using :doc:`Pooch <pooch:about>`."""

import logging
from pathlib import Path
from typing import Optional, Tuple

import click
import pooch

from .context import Context

log = logging.getLogger(__name__)


class Extract:
    """Similar to :class:`pooch.Unzip`, streamlined using :mod:`pathlib`.

    This version supports:

    - Absolute or relative paths for the `extract_dir` parameter.
    - :file:`.zip` or :file:`.tar.xz` archives.
    """

    def __init__(self, members=None, extract_dir=None):
        self.members = members
        self.extract_dir = Path(extract_dir or ".")

    def __call__(self, fname, action, pooch):
        import tarfile
        import zipfile

        path = Path(fname)

        # Identify the directory for extracted files
        if self.extract_dir.is_absolute():
            # Some absolute path
            extract_dir = self.extract_dir
        else:
            # A relative path, possibly the default
            extract_dir = path.parent.joinpath(self.extract_dir)

        # Ensure the directory exists
        extract_dir.mkdir(parents=True, exist_ok=True)

        members = self.members

        # Select the class/method to open the archive, and the method name for listing
        # members
        cls, list_method = {
            ".zip": (zipfile.ZipFile, "namelist"),
            ".xz": (tarfile.TarFile.open, "getnames"),
        }[path.suffix]

        with cls(path) as archive:
            if members is None:
                members = getattr(archive, list_method)()
                log.info(f"Unpack all {len(members)} members of {path}")

            archive.extractall(members=members, path=extract_dir)

        return members


class UnpackSnapshot:
    """Pooch processor that calls :func:`.snapshot.unpack`."""

    def __call__(self, fname, action, pooch):
        from message_ix_models.model.snapshot import unpack

        path = Path(fname)
        unpack(path)

        return path


#: Supported remote sources of data.
SOURCE: dict[str, dict] = {
    "PRIMAP": dict(
        pooch_args=dict(
            base_url="ftp://datapub.gfz-potsdam.de/download/10.5880.PIK.2019.001/",
            registry={
                "PRIMAP-hist_v2.0_11-Dec-2018.zip": (
                    "md5:f28d58abef4ecfc36fc8ce3e9eef2871"
                ),
            },
        ),
        processor=Extract(members=["PRIMAP-hist_v2.0_11-Dec-2018.csv"]),
    ),
    "MESSAGEix-Nexus": dict(
        pooch_args=dict(
            base_url="https://github.com/iiasa/message-ix-models/raw/enh/2023-W44/"
            "message_ix_models/data/water/",
            registry={"water.tar.xz": "sha1:ec9e0655af90ca844c0158968bb03a194b8fa6c6"},
        ),
        processor=Extract(extract_dir="water"),
    ),
    "snapshot-0": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.5793870",
            registry={
                "MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline.xlsx": (
                    "md5:222193405c25c3c29cc21cbae5e035f4"
                ),
            },
        ),
        processor=UnpackSnapshot(),
    ),
    "snapshot-1": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.10514052",
            registry={
                "MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline.xlsx": (
                    "md5:e7c0c562843e85c643ad9d84fecef979"
                ),
            },
        ),
    ),
}


def fetch(
    pooch_args: dict, *, extra_cache_path: Optional[str] = None, **fetch_kwargs
) -> Tuple[Path, ...]:
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
    path = (
        [str(Context.get_instance(-1).get_cache_path()), extra_cache_path]
        if extra_cache_path
        else Context.get_instance(-1).get_cache_path()
    )
    pooch_args.setdefault("path", path)

    p = pooch.create(**pooch_args)

    if len(p.registry) > 1:  # pragma: no cover
        raise NotImplementedError("fetch() with registries with >1 files")

    filenames = p.fetch(next(iter(p.registry.keys())), **fetch_kwargs)

    if isinstance(filenames, (str, Path)):
        filenames = [filenames]

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
