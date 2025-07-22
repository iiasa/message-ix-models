from typing import IO, Union

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

    import dask.dataframe as dd
    import pandas as pd
    from numpy import char, random

    from message_ix_models.project.advance.data import NAME
    from message_ix_models.util import package_data_path, path_fallback

    # Paths
    p = Path(filename)
    path_in = path_fallback(p, where="private local")
    path_out = package_data_path("test", p)

    # Shared arguments for read_csv() and to_csv()
    comment, engine, sep = None, "pyarrow", ","
    if "GFEI" in str(p):
        comment, engine = "#", "c"
    if p.suffix == ".mif":
        sep = ";"

    # Read the data
    zf_member_name = None
    with TemporaryDirectory() as td:
        td_path = Path(td)
        if "advance" in filename:
            # Manually unpack one member of the multi-member archive `path_in`
            zf_member_name = NAME
            target: Union[IO, Path, str] = zipfile.ZipFile(path_in).extract(
                zf_member_name, path=td_path
            )
        elif "iea" in filename:
            # Manually unpack so that dask.dataframe.read_csv() can be used
            from message_ix_models.tools.iea.web import fwf_to_csv, unpack_zip

            target = unpack_zip(path_in)
            zf_member_name = target.name
            if target.suffix == ".TXT":
                target = fwf_to_csv(target, progress=True)
        else:
            target = path_in

        print(f"Read {target}")

        # - Read the data
        #   - Use dask & pyarrow.
        #   - Prevent values like "NA" being auto-transformed to np.nan.
        # - Subset the data if `frac` < 1.0.
        # - Compute the resulting pandas.DataFrame.
        df = (
            dd.read_csv(
                target,
                comment=comment,
                engine=engine,
                keep_default_na=False,
                na_values=[],
                sep=sep,
            )
            .sample(frac=frac)
            .compute()
        )

    # Determine columns in which to replace numerical data
    if any(s in filename for s in ("iea", "edits")):
        # Specific column
        cols = ["Value"]
    else:
        # All columns with numeric names, for instance 2000, 2001, etc.
        cols = list(filter(lambda c: char.isnumeric(c) or c.lower() == c, df.columns))

    # Shape of random data
    size = (df.shape[0], len(cols))
    # - Generate random data of this shape, with columns `cols` and same index as `df`.
    # - Keep only the elements corresponding to non-NA elements of `df`.
    # - Update `df` with these values.*
    generator = random.default_rng()
    df.update(
        df.where(
            df.isna(),
            pd.DataFrame(generator.random(size), columns=cols, index=df.index),
        )
    )

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

    df.to_csv(target, float_format="%.2f", index=False, sep=sep)
