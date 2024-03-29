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
    if load_snapshot.scenario != "baseline_v1":
        pytest.skip(reason="Test only latest version of public baseline snapshot.")

    mp = test_context.get_platform()
    scenario = load_snapshot

    if not scenario.has_solution():
        log.info("Solve")
        scenario.solve(solve_options=dict(lpmethod=4), quiet=True)

    legacy_report(mp=mp, scen=scenario)
