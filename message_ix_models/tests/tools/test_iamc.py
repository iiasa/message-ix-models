from message_ix_models.project.advance.data import LOCATION, NAME
from message_ix_models.tools.iamc import describe
from message_ix_models.util import private_data_path


def test_describe(test_context):
    import zipfile

    import pandas as pd

    path = private_data_path(*LOCATION)
    zf = zipfile.ZipFile(private_data_path(*LOCATION))
    source = zf.open(NAME)

    data = pd.read_csv(source, engine="pyarrow").rename(columns=lambda c: c.upper())

    sm = describe(data, f"ADVANCE data in {path}")

    # Message contains the expected code lists.
    # Code lists have the expected lengths.
    for id, N in (
        ("MODEL", 12),
        ("SCENARIO", 51),
        ("REGION", 14),
        ("VARIABLE", 3080),
        ("UNIT", 29),
    ):
        assert N == len(sm.codelist[id])
