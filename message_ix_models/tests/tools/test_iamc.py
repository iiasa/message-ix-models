from message_ix_models.project.advance.data import LOCATION, NAME
from message_ix_models.tools.iamc import describe
from message_ix_models.util import MESSAGE_MODELS_PATH, package_data_path


def test_describe(test_context):
    import tarfile

    import pandas as pd

    path = package_data_path("test", *LOCATION)
    with tarfile.open(path, "r:*") as tf:
        data = pd.read_csv(tf.extractfile(NAME), engine="pyarrow").rename(
            columns=lambda c: c.upper()
        )

    sm = describe(
        data, f"ADVANCE data in {path.relative_to(MESSAGE_MODELS_PATH.parent)}"
    )

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

    # from message_ix_models.util.sdmx import write

    # write(sm, basename="ADVANCE")
