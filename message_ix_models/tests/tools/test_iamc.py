from message_ix_models.project.advance.data import LOCATION, NAME
from message_ix_models.tools.iamc import describe
from message_ix_models.tools.iamc.structure import CL_SCENARIO_DIAGNOSTIC
from message_ix_models.util import MESSAGE_MODELS_PATH, package_data_path


class TestCL_SCENARIO_DIAGNOSTIC:
    def test_create(self) -> None:
        # Function runs without error
        result = CL_SCENARIO_DIAGNOSTIC.create()

        # Code list has expected number of items: 12 distinct scenarios × 5 SSPs
        assert 12 * 5 == len(result)

        # An expected item is in the code list
        item = result["CP"]

        # Description is dedented and trimmed
        assert str(item.description).startswith("The default ")
        assert str(item.description).endswith(" should be provided.")

        # Items are auto-generated for other SSPs
        assert "C400-lin-Policies-SSP5" in result


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
