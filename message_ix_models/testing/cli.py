import click


@click.group("testing")
def cli():
    """Manipulate test data."""
    pass


FILENAMES = [
    "advance/advance_compare_20171018-134445.csv.zip",
    "ssp/SSP-Review-Phase-1.csv.gz",
    "ssp/SspDb_country_data_2013-06-12.csv.zip",
]


@cli.command("fuzz-private-data")
@click.argument("filename", metavar="FILENAME", type=click.Choice(FILENAMES))
def fuzz_private_data(filename):  # pragma: no cover
    """Create random data for testing.

    This command creates data files in message_ix_models/data/test/… based on
    corresponding private files in message_data/data/…. This supports testing of code in
    message_ix_models that handles these files.

    The files are identical in structure and layout, except the values are "fuzzed", or
    replaced with random values.

    To see valid FILENAMES, run the command with no arguments.
    """
    import zipfile
    from contextlib import nullcontext
    from pathlib import Path

    import pandas as pd
    from numpy import char, random

    from message_ix_models.project.advance.data import NAME
    from message_ix_models.util import package_data_path, private_data_path

    # Paths
    p = Path(filename)
    path_in = private_data_path(p)
    path_out = package_data_path("test", p)

    # Read the data
    member = NAME if "advance" in filename else None
    with zipfile.ZipFile(path_in) if member else nullcontext() as zf:
        df = pd.read_csv(zf.open(member) if member else path_in, engine="pyarrow")

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

    with zipfile.ZipFile(
        path_out, "w", compression=zipfile.ZIP_BZIP2
    ) if member else nullcontext() as zf:
        df.to_csv(
            zf.open(member, "w") if member else path_out,
            index=False,
            float_format="%.2f",
        )
