import logging
import sys

import pytest

from message_ix_models.model import snapshot
from message_ix_models.report import legacy_report
from message_ix_models.testing import GHA

log = logging.getLogger(__name__)


@snapshot.load.minimum_version
@pytest.mark.skipif(
    condition=GHA and sys.platform in ("darwin", "win32"), reason="Slow."
)
def test_legacy_report(test_context, load_snapshots):
    latest_scenario = [scenario for scenario in load_snapshots][-1]

    mp = test_context.get_platform()

    legacy_report(mp=mp, scen=latest_scenario)
