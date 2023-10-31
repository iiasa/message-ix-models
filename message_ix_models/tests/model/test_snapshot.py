import logging
import shutil
import sys
from importlib.metadata import version

import pytest
from message_ix import Scenario

from message_ix_models.model import snapshot
from message_ix_models.testing import GHA
from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


@pytest.fixture
def unpacked_snapshot_data(test_context, request):
    """Already-unpacked data for a snapshot.

    This copies the .csv.gz files from message_ix_models/tests/data/… to the directory
    where they *would* be unpacked by .model.snapshot._unpack. This causes the code to
    skip unpacking them, which can be very slow.
    """
    snapshot_id = request.getfixturevalue("snapshot_id")
    assert 0 == snapshot_id, f"No unpacked data for snapshot {snapshot_id}"

    dest = test_context.get_cache_path("MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline")
    log.debug(f"{dest = }")

    snapshot_data_path = package_data_path(
        "test", "MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline"
    )
    log.debug(f"{snapshot_data_path = }")

    shutil.copytree(snapshot_data_path, dest, dirs_exist_ok=True)


@pytest.mark.xfail(
    condition=version("message_ix") < "3.5",
    raises=NotImplementedError,
    reason="Not supported with message_ix < 3.5",
)
@pytest.mark.skipif(
    condition=GHA and sys.platform in ("darwin", "win32"), reason="Slow."
)
@pytest.mark.usefixtures("unpacked_snapshot_data")
@pytest.mark.parametrize("snapshot_id", snapshot.SNAPSHOTS.keys())
def test_load(test_context, snapshot_id):
    mp = test_context.get_platform()
    base = Scenario(mp, model="MODEL", scenario="baseline", version="new")

    snapshot.load(base, snapshot_id)
