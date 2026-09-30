"""Delete MESSAGE GDX files after a successful solve, to save disk space."""
import logging
import os
from pathlib import Path

import message_ix
from message_ix.message import MESSAGE

log = logging.getLogger(__name__)

#: Set this environment variable to "1" to enable deletion.
ENV_VAR = "FUEL_SECURITY_DELETE_GDX"


def delete_gdx(scenario: message_ix.Scenario) -> None:
    """Delete the MsgData, MsgOutput and MsgIterationReport GDX files of `scenario`.

    Does nothing unless the environment variable :data:`ENV_VAR` is set to "1". The
    solution is already stored on the platform once :meth:`.Scenario.solve` returns,
    so these files are not needed afterwards.
    """
    if os.environ.get(ENV_VAR) != "1":
        return

    # Same file names that message_ix.common.GAMSModel uses
    model_dir = Path(MESSAGE.defaults["model_dir"])
    case = MESSAGE.clean_path(f"{scenario.model}_{scenario.scenario}".replace(" ", "_"))
    for path in (
        model_dir / "data" / f"MsgData_{case}.gdx",
        model_dir / "output" / f"MsgOutput_{case}.gdx",
        model_dir / "output" / f"MsgIterationReport_{case}.gdx",
    ):
        if path.exists():
            path.unlink()
            log.info(f"Deleted {path}")
