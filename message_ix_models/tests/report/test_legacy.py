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
@pytest.mark.snapshot
def test_legacy_report(test_context, load_snapshot):
    # TODO This probably shouldn't be hardcoded
    if load_snapshot.scenario == "baseline_v1":
        scenario = load_snapshot
    else:
        return

    mp = test_context.get_platform()

    legacy_report(mp=mp, scen=scenario)
