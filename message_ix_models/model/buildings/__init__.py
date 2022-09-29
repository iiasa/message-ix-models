"""MESSAGEix-Buildings and related models."""
from dataclasses import dataclass
from pathlib import Path

import ixmp
from message_ix_models.util import MESSAGE_DATA_PATH

ixmp.config.register(
    "message buildings dir", Path, MESSAGE_DATA_PATH.parent.joinpath("buildings")
)


@dataclass
class Config:
    """Configuration options for :mod:`.buildings` code.

    The code responds to values set on an instance of this class.

    Raises
    ------
    FileNotFoundError
        if the "message buildings dir" configuration key (or its default value; see
        :attr:`code_dir`) does not point to a valid location.
    """

    #: Climate scenario. Either "BL" or "2C".
    climate_scenario: str = "BL"

    #: :obj:`True` if the base scenario should be cloned. If :prog:`--climate-scen` is
    #: given on the command line, this is set to "2C" automatically.
    clone: bool = True

    #: Path to the MESSAGE_Buildings code and data. This repository is not an
    #: installable Python package, so it cannot be imported without information on its
    #: location.
    #:
    #: The key ``message buildings dir`` can be set in the user's :ref:`ixmp
    #: configuration file <ixmp:configuration>`; if not set, it defaults to a directory
    #: named "buildings" located in the same parent directory that contains
    #: :mod:`message_data`.
    code_dir: Path = None

    #: Maximum number of iterations of the ACCESS–STURM–MESSAGE loop. Set to 1 for
    #: once-through mode.
    max_iterations: int = 0

    #: Path for STURM output.
    _output_path: Path = None

    #: Run the ACCESS model on every iteration (experimental/untested).
    run_access: bool = False

    #: Solve scenarios using :class:`.MESSAGE_MACRO` (:obj:`True`) or only
    #: :class:`.MESSAGE`.
    solve_macro: bool = False

    #: No longer used
    ssp: str = "SSP2"

    #: Method for running STURM. See :func:`.sturm.run`.
    sturm_method: str = None

    #: STURM scenario to run.
    sturm_scenario: str = None

    def __post_init__(self):
        try:
            self.code_dir = (
                Path(ixmp.config.get("message buildings dir")).expanduser().resolve()
            )
        except KeyError:
            raise RuntimeError(
                'message_data.model.buildings requires the "message buildings dir" '
                "configuration key to be set. See the documentation."
            )

        if not self.code_dir.exists():
            raise FileNotFoundError(
                f"MESSAGE_Buildings code directory not found at {self.code_dir}"
            )
