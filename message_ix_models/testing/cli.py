from typing import IO

import click


@click.group("testing")
def cli():
    """Manipulate test data."""
    pass


FILENAMES = [
    "advance/advance_compare_20171018-134445.csv.xz",
    "edits/pasta.csv",
    "gea/GEADB_ARCHIVE_20171108.zip",
    "iea/372f7e29-en.zip",
    "iea/8624f431-en.zip",
    "iea/cac5fa90-en.zip",
    "iea/web/2024-07-25/WBIG1.zip",
    "iea/web/2024-07-25/WBIG2.zip",
    "shape/gdp_v1p0.mif",
    "shape/gdp_v1p1.mif",
    "shape/gdp_v1p2.mif",
    "shape/gini_v1p0.csv",
    "shape/gini_v1p1.csv",
    "shape/population_v1p0.mif",
    "shape/population_v1p1.mif",
    "shape/population_v1p2.mif",
    "shape/urbanisation_v1p0.csv",
    "ssp/SSP-Review-Phase-1.csv.gz",
    "ssp/SspDb_country_data_2013-06-12.csv.zip",
    "transport/GFEI_FE_by_Powertrain_2017.csv",
]


@cli.command("fuzz-private-data")
@click.option("--frac", type=float, default=1.0, help="Fraction of rows (default: all)")
@click.argument("filename", metavar="FILENAME", type=click.Choice(FILENAMES))
def fuzz_private_data(filename, frac: float):  # pragma: no cover
    """Create random data for testing.

    This command creates data files in message_ix_models/data/test/… based on
    corresponding private files in either message_data/data/… or the local data
    directory. This supports testing of code in message_ix_models that handles these
    files.

    The files are identical in structure and layout, except the values are "fuzzed", or
    replaced with random values.

    To see valid FILENAMES, run the command with no arguments.
    """
    import zipfile
    from pathlib import Path
    from tempfile import TemporaryDirectory

    from message_ix_models.project.advance.data import NAME
    from message_ix_models.util import (
        package_data_path,
        path_fallback,
        random_sample_from_file,
    )

    # Paths
    p = Path(filename)
    path_in = path_fallback(p, where="private local")
    path_out = package_data_path("test", p)

    # Shared arguments for read_csv() and to_csv()
    args: dict = dict(sep=",")
    if "GFEI" in str(p):
        args.update(comment="#", engine="c")
    if p.suffix == ".mif":
        args.update(sep=";")
    if any(s in filename for s in ("iea", "edits")):
        # Specific columns in which to replace numeric data
        args.update(cols=["Value"])

    # Read the data
    zf_member_name = None
    with TemporaryDirectory() as td:
        td_path = Path(td)
        if "advance" in filename:
            # Manually unpack one member of the multi-member archive `path_in`
            zf_member_name = NAME
            target: IO | Path | str = zipfile.ZipFile(path_in).extract(
                zf_member_name, path=td_path
            )
        elif "iea" in filename:
            # Manually unpack so that dask.dataframe.read_csv() can be used
            from message_ix_models.tools.iea.web import fwf_to_csv
            from message_ix_models.util.zipfile import Archive

            with Archive(path_in) as a:
                target = a.extract()
            zf_member_name = target.name
            if target.suffix == ".TXT":
                target = fwf_to_csv(target, progress=True)
        else:
            target = path_in

        print(f"Read {target}")
        df = random_sample_from_file(target, frac, **args)

    # Write to file, keeping only a few decimal points
    path_out.parent.mkdir(parents=True, exist_ok=True)

    if path_out.suffix.lower() == ".zip":
        assert zf_member_name is not None

        zf = zipfile.ZipFile(path_out, "w", compression=zipfile.ZIP_BZIP2)
        target = zf.open(zf_member_name, "w")
        print(f"Write to member {zf_member_name} in {path_out}")
    else:
        target = path_out
        print(f"Write to {path_out}")

    df.to_csv(target, float_format="%.2f", index=False, sep=args["sep"])
