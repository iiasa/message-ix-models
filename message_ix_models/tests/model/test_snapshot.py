import logging
import sys

import pytest

from message_ix_models.testing import GHA

log = logging.getLogger(__name__)


@pytest.mark.skipif(
    condition=GHA and sys.platform in ("darwin", "win32"), reason="Slow."
)
@pytest.mark.snapshot
def test_load(test_context, loaded_snapshot):
    assert loaded_snapshot.model == "MESSAGEix-GLOBIOM_1.1_R11_no-policy"
