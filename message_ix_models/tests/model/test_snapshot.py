import logging
import sys

import pytest

from message_ix_models.model import snapshot
from message_ix_models.testing import GHA

log = logging.getLogger(__name__)


@snapshot.load.minimum_version
@pytest.mark.skipif(
    condition=GHA and sys.platform in ("darwin", "win32"), reason="Slow."
)
@pytest.mark.snapshot
def test_load(test_context, load_snapshot):
    assert load_snapshot.model == "MESSAGEix-GLOBIOM_1.1_R11_no-policy"
