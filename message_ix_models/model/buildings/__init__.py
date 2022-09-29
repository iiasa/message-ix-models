"""MESSAGEix-Buildings and related models."""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """Configuration options for :mod:`.buildings` code.

    The code responds to values set on an instance of this class.
    """

    #: Climate scenario. Either "BL" or "2C".
    climate_scenario: str = "BL"

    #: :obj:`True` if the base scenario should be cloned. If :prog:`--climate-scen` is
    #: given on the command line, this is set to "2C" automatically.
    clone: bool = True

    #: Path to the MESSAGE_Buildings repository.
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
