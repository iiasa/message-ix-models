import logging
from dataclasses import InitVar, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Union

from message_ix_models.util import local_data_path, package_data_path
from message_ix_models.util.config import ConfigHelper

if TYPE_CHECKING:
    from genno.core.key import KeyLike

log = logging.getLogger(__name__)


@dataclass
class Config(ConfigHelper):
    """Settings for :mod:`message_ix_models.report`.

    When initializing a new instance, the `from_file` and `_legacy` parameters are
    respected.
    """

    #: Shorthand to call :func:`use_file` on a new instance.
    from_file: InitVar[Optional[Path]] = package_data_path("report", "global.yaml")

    #: Shorthand to set :py:`legacy["use"]` on a new instance.
    _legacy: InitVar[Optional[bool]] = False

    #: Path to write reporting outputs when invoked from the command line.
    cli_output: Optional[Path] = None

    #: Configuration to be handled by :mod:`genno.config`.
    genno_config: Dict = field(default_factory=dict)

    #: Key for the Quantity or computation to report.
    key: Optional["KeyLike"] = None

    #: Directory for output.
    output_dir: Optional[Path] = field(
        default_factory=lambda: local_data_path("report")
    )

    #: :data:`True` to use an output directory based on the scenario's model name and
    #: name.
    use_scenario_path: bool = True

    #: Keyword arguments for :func:`.report.legacy.iamc_report_hackathon.report`, plus
    #: the key "use", which should be :any:`True` if legacy reporting is to be used.
    legacy: Dict = field(default_factory=lambda: dict(use=False, merge_hist=True))

    def __post_init__(self, from_file, _legacy) -> None:
        self.use_file(from_file)
        self.legacy.update(use=_legacy)

    def set_output_dir(self, arg: Optional[Path]) -> None:
        """Set :attr:`output_dir`, the output directory.

        The value is also stored to be passed to :mod:`genno` as the "output_dir"
        configuration key.
        """
        if arg:
            self.output_dir = arg.expanduser()

        self.genno_config["output_dir"] = self.output_dir

    def use_file(self, file_path: Union[str, Path, None]) -> None:
        """Use genno configuration from a (YAML) file at `file_path`.

        See :mod:`genno.config` for the format of these files. The path is stored at
        :py:`.genno_config["path"]`, where it is picked up by genno's configuration
        mechanism.

        Parameters
        ----------
        file_path : PathLike, optional
            This may be:

            1. The complete path to any existing file.
            2. A stem like "global" or "other". This is interpreted as referring to a
               file named, for instance, :file:`global.yaml`.
            3. A partial path like "project/report.yaml". This or (2) is interpreted
               as referring to a file within :file:`MESSAGE_MODELS_PATH/data/report/`;
               that is, a file packaged and distributed with :mod:`message_ix_models`.
        """
        if file_path is None:
            return
        try:
            path = next(
                filter(
                    Path.exists,
                    (
                        Path(file_path),
                        # Path doesn't exist; treat it as a stem in the metadata dir
                        package_data_path("report", file_path).with_suffix(".yaml"),
                    ),
                )
            )
        except StopIteration:
            raise FileNotFoundError(f"Reporting configuration in '{file_path}(.yaml)'")

        # Store for genno to handle
        self.genno_config["path"] = path

    def mkdir(self) -> None:
        """Ensure the :attr:`output_dir` exists."""
        if self.output_dir:
            self.output_dir.mkdir(exist_ok=True, parents=True)
