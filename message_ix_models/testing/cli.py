import click


@click.group("testing")
def cli():
    """Manipulate test data."""
    pass


@cli.command("fuzz-private-data")
@click.argument(
    "filename",
    metavar="FILENAME",
    type=click.Choice(
        ["SSP-Review-Phase-1.csv.gz", "SspDb_country_data_2013-06-12.csv.zip"]
    ),
)
def fuzz_private_data(filename):  # pragma: no cover
    """Create random data for testing.

    This command creates data files in message_ix_models/data/test/… based on
    corresponding private files in message_data/data/…. This supports testing of code in
    message_ix_models that handles these files.

    The files are identical in structure and layout, except the values are "fuzzed", or
    replaced with random values.
    """
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
