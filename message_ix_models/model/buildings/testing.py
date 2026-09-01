"""Utilities for testing :mod:`.buildings`."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from message_ix_models import Context


@pytest.fixture
def mock_buildings_context(
    tmp_path: Path, test_context: Context, test_data_path: Path
) -> Iterator[Context]:
    """A context associated with a mock installation of MESSAGEix-Buildings.

    In the returned Context, :py:`context.buildings.code_dir` points to a temporary
    directory populated by copying the following contents from
    :file:`message_ix_models/data/test/buildings/mock/` in the package source.

      (temporary directory)/
      - message_ix_buildings/
        - sturm/
          - data/
            - comm_sturm_aligned_R12.csv
            - input_prices_R12_default.csv
            - resid_comm_glance_aligned_R12.csv
            - resid_sturm_aligned_R12.csv
          - message_linking/
            - (empty directory)
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

    yield test_context
