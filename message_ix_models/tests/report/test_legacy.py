import logging
import sys

import pytest

from message_ix_models.report import report
from message_ix_models.testing import GHA

log = logging.getLogger(__name__)


@pytest.mark.skipif(
    condition=GHA and sys.platform in ("darwin", "win32"), reason="Slow."
)
@pytest.mark.snapshot
def test_legacy_report(test_context, loaded_snapshot):
    scenario = loaded_snapshot

    # TODO This probably shouldn't be hardcoded
    if scenario.scenario != "baseline_v1":
        pytest.skip(reason="Test only latest version of public baseline snapshot.")

    if not scenario.has_solution():
        log.info("Solve")
        scenario.solve(solve_options=dict(lpmethod=4), quiet=True)

    test_context.set_scenario(scenario)
    test_context.report.legacy.update(
        use=True,
        merge_hist=True,
        ref_sol="True",
        run_config="ENGAGE_SSP2_v417_run_config.yaml",
    )

    report(test_context)
