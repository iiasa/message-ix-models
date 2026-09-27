"""Utilities for testing :mod:`.buildings`."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from message_ix_models import Context


@pytest.fixture
def mock_buildings_context(
    tmp_path: Path,
    test_context: Context,
    test_data_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Context]:
    """A context associated with a mock installation of MESSAGEix-Buildings.

    In the returned Context, :py:`context.buildings.code_dir` points to a temporary
    directory populated by copying the following contents from
    :file:`message_ix_models/data/test/buildings/mock/` in the package source.
    The same path is used by :func:`.sturm._message_buildings_install_dir` (patched)
    so :func:`.sturm.call_sturm` / :func:`.sturm.call_buildings_demand` resolve it.

      (temporary directory)/
      - message_ix_buildings/
        - sturm/
          - data/
            - input_prices_R12_default.csv
          - message_linking/
            - resid_sturm_aligned_R.csv
            - comm_sturm_aligned_R.csv
            - resid_comm_glance_aligned_R.csv
          - run_GLANCE_placeholder.R
          - run_MIXB_aligner.R
          - run_STURM_Circular_comm_glo.R
          - run_STURM_Circular_resid_glo.R
    """
    from shutil import copytree

    from message_ix_models.model.buildings.sturm import METHOD

    from .config import Config

    test_context.model.regions = "R12"
    test_context.buildings = Config(
        code="R",
        code_dir=tmp_path,
        sturm_scenario="NONE",
        sturm_method=METHOD.RSCRIPT_B,
    )

    # Copy the tree of test data into the target path
    copytree(test_data_path.joinpath("buildings", "mock"), tmp_path, dirs_exist_ok=True)

    # Match production path resolution used by call_sturm / call_buildings_demand
    monkeypatch.setattr(
        "message_ix_models.model.buildings.sturm._message_buildings_install_dir",
        lambda: tmp_path,
    )

    yield test_context
