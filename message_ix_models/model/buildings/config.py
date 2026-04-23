"""Buildings configuration."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ixmp

from message_ix_models.model.workflow import Config as SolveConfig
from message_ix_models.util.config import ConfigHelper

if TYPE_CHECKING:
    from message_ix_models import Context

log = logging.getLogger(__name__)


def _code_dir_factory() -> Path:
    """Return the default value for :attr:`.Config.code_dir`.

    In order of precedence:

    1. The directory where :mod:`message_ix_buildings` is installed.
    2. The :mod:`ixmp` configuration key ``message buildings dir``, if set. The older,
       private MESSAGE_Buildings repository is not an installable Python package, so it
       cannot be imported without information on its location.

       This key can be set in the local :ref:`ixmp configuration file
       <ixmp:configuration>`.
    3. A directory named :file:`./buildings` in the parent of the directory containing
       :mod:`message_ix_models`.
    """
    from importlib.util import find_spec

    from message_ix_models.util import MESSAGE_MODELS_PATH

    if spec := find_spec("message_ix_buildings"):
        assert spec.origin is not None
        return Path(spec.origin).parent

    try:
        return Path(ixmp.config.get("message buildings dir")).expanduser().resolve()
    except AttributeError:
        pass  # Not set

    return MESSAGE_MODELS_PATH.parents[1].joinpath("buildings")


@dataclass
class Config(ConfigHelper):
    """Configuration options for :mod:`.buildings` code.

    The code responds to values set on an instance of this class.

    Raises
    ------
    FileNotFoundError
        if :attr:`code_dir` points to a non-existent directory.
    """

    #: Name or ID of STURM scenario to run.
    sturm_scenario: str

    #: Climate scenario. Either `BL` or `2C`.
    climate_scenario: str = "BL"

    #: :obj:`True` if the base scenario should be cloned.
    clone: bool = False

    #: Path to the MESSAGEix-Buildings code and data.
    #:
    #: If not set explicitly, this is populated using :func:`_code_dir_factory`.
    code_dir: Path = field(default_factory=_code_dir_factory)

    #: Maximum number of iterations of the ACCESS–STURM–MESSAGE loop. Set to 1 for
    #: once-through mode.
    max_iterations: int = 0

    #: :obj:`True` if the MESSAGEix-Materials + MESSAGEix-Buildings combination is
    #: active
    with_materials: bool = True

    #: Path for STURM output.
    _output_path: Path | None = None

    #: Run the ACCESS model on every iteration.
    run_access: bool = False

    #: Keyword arguments for :meth:`.message_ix.Scenario.solve`. Set
    #: `model="MESSAGE_MACRO"` to solve scenarios using MESSAGE_MACRO.
    solve: dict[str, Any] = field(default_factory=lambda: dict(model="MESSAGE"))

    #: Similar to `solve`, but using another config class.
    solve_config: SolveConfig = field(
        default_factory=lambda: SolveConfig(
            solve=dict(model="MESSAGE"), reserve_margin=False
        )
    )

    #: .. todo:: Document the meaning of this setting.
    ssp: str = "SSP2"

    #: Method for running STURM. See :func:`.sturm.run`.
    sturm_method: str = "Rscript"

    def __post_init__(self) -> None:
        if not self.code_dir.exists():
            raise FileNotFoundError(f"MESSAGEix-Buildings not found at {self.code_dir}")

    def set_output_path(self, context: "Context") -> None:
        # Base path for output during iterations
        self._output_path = context.get_local_path("buildings")
        self._output_path.mkdir(parents=True, exist_ok=True)
