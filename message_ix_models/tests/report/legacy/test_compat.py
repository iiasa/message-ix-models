from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest

from message_ix_models import Context, testing
from message_ix_models.report import prepare_reporter
from message_ix_models.report.legacy import compat


@pytest.fixture
def skip_tables() -> Iterator[None]:
    """Entirely skip some tables.

    These call :func:`.postprocess.land_out` that in turn raises SystemExit if LAND is
    empty—as it is in the scenario returned by :func:`.bare_res`. This exception cannot
    be caught and handled by :func:`.run_table`.
    """
    pre = compat.SKIP.copy()

    compat.SKIP = {
        "agri_dem",
        "agri_prd",
        "fertilizer_int",
        "fertilizer_use",
        "food_dem",
        "food_waste",
        "frst_dem",
        "frst_prd",
        "globiom_feedback",
        "lnd_cvr",
        "othemi",
        "price",
        "yield",
    }
    try:
        yield
    finally:
        compat.SKIP = pre


@compat.callback.minimum_version
@pytest.mark.usefixtures("skip_tables")
def test_callback(
    request: pytest.FixtureRequest, tmp_path: Path, test_context: Context
) -> None:
    """:func:`.report.legacy.compat.callback` prepares working Reporter."""
    # NB This is similar to test_report.test_reporter_bare_res

    # Prepare a solved, 'bare' scenario
    test_context.model.regions = "R12"
    scenario = testing.bare_res(request, test_context, solved=True)

    # Set up report.Config
    test_context.report.update(from_file="global.yaml", key=compat.KEY.result)
    # Append the .report.legacy.compat callback
    test_context.report.callback.append(compat.callback)

    # Prepare the reporter and compute the result
    rep, key = prepare_reporter(test_context, scenario)

    # rep.visualize("report-legacy-compat.svg", key, rankdir="LR")  # DEBUG

    # Result is computed without error
    result = rep.get(key)

    assert isinstance(result, pd.DataFrame)
    assert 4455 == len(result)
