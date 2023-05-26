import pytest
from message_ix import Scenario

from message_ix_models.model import snapshot


@pytest.mark.parametrize("snapshot_id", snapshot.SNAPSHOTS.keys())
def test_load(test_context, snapshot_id):
    mp = test_context.get_platform()
    base = Scenario(mp, model="MODEL", scenario="baseline", version="new")

    snapshot.load(base, snapshot_id)
