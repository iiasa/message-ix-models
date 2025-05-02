import logging
from collections.abc import Callable
from dataclasses import InitVar, dataclass, field
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Optional, TypeVar, Union

from message_ix_models.util import package_data_path
from message_ix_models.util.config import ConfigHelper, _local_data_factory

if TYPE_CHECKING:
    import genno
    from genno.core.key import KeyLike

    from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

#: Type signature of callback functions referenced by :attr:`.Config.callback` and
#: used by :func:`.prepare_reporter`.
ComputerT = TypeVar("ComputerT", bound="genno.Computer")
Callback = Callable[[ComputerT, "Context"], None]


def _default_callbacks() -> list[Callback]:
    from message_ix_models.report import plot

    from . import defaults

    return [defaults, plot.callback]


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

    # NB InitVars should appear first so they can be used positionally, followed by
    #    all others in alpha order. With Python ≥ 3.10, can use field(…, kw_only=True).

    #: List of callbacks for preparing the :class:`.Reporter`.
    #:
    #: Each registered function is called by :meth:`prepare_reporter`, in order to add
    #: or modify reporting keys. Specific model variants and projects can register a
    #: callback to extend the reporting graph.
    #:
    #: Callback functions must take two arguments: the Computer/Reporter, and a
    #: :class:`.Context`:
    #:
    #: .. code-block:: python
    #:
    #:     from message_ix.report import Reporter
    #:     from message_ix_models import Context
    #:
    #:     def cb(rep: Reporter, ctx: Context) -> None:
    #:         # Modify `rep` by calling its methods ...
    #:         pass
    #:
    #:     # Register this callback on an existing Context instance
    #:     context.report.register(cb)
    callback: list[Callback] = field(default_factory=_default_callbacks)

    #: Path to write reporting outputs when invoked from the command line.
    cli_output: Optional[Path] = None

    #: Configuration to be handled by :mod:`genno.config`.
    genno_config: dict = field(default_factory=dict)

    #: Key for the Quantity or computation to report.
    key: Optional["KeyLike"] = None

    #: Directory for output.
    output_dir: Path = field(
        default_factory=lambda: _local_data_factory().joinpath("report")
    )

    #: :data:`True` to use an output directory based on the scenario's model name and
    #: name.
    use_scenario_path: bool = True

    #: Keyword arguments for :func:`.report.legacy.iamc_report_hackathon.report`, plus
    #: the key "use", which should be :any:`True` if legacy reporting is to be used.
    legacy: dict = field(default_factory=lambda: dict(use=False, merge_hist=True))

    def __post_init__(self, from_file, _legacy) -> None:
        # Handle InitVars
        self.use_file(from_file)
        self.legacy.update(use=_legacy)

    def register(self, name_or_callback: Union[Callback, str]) -> Optional[str]:
        """Register a :attr:`callback` function for :func:`prepare_reporter`.

        Parameters
        ----------
        name_or_callback
            If a callable (function), it is used directly.

            If a string, it may name a submodule of :mod:`.message_ix_models`, or
            :mod:`message_data`, in which case the function
            :py:`{message_data,message_ix_models}.{name}.report.callback` is used. Or,
            it may be a fully-resolved package/module name, in which case
            :py:`{name}.callback` is used.
        """

        if isinstance(name_or_callback, str):
            # Resolve a string
            candidates = [
                name_or_callback,  # A fully-resolved package/module name
                f"message_ix_models.{name_or_callback}.report",  # A submodule here
                f"message_data.{name_or_callback}.report",  # A message_data submodule
            ]
            mod = None
            for name in candidates:
                try:
                    mod = import_module(name)
                except ModuleNotFoundError:
                    continue
                else:
                    break
            if mod is None:
                raise ModuleNotFoundError(" or ".join(candidates))
            callback = mod.callback
        else:
            callback = name_or_callback
            name = callback.__name__

        if callback in self.callback:
            log.info(f"Already registered: {callback}")
            return None

        self.callback.append(callback)
        return name

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
