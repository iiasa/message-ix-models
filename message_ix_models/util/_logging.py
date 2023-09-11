"""Logging utilities."""

import logging
import logging.config
from contextlib import contextmanager
from copy import deepcopy
from time import process_time

__all__ = [
    "Formatter",
    "make_formatter",
    "setup",
    "silence_log",
]

log = logging.getLogger(__name__)


@contextmanager
def silence_log(names=None, level=logging.ERROR):
    """Context manager to temporarily quiet 1 or more loggers.

    Parameters
    ----------
    names : str, *optional*
        Space-separated names of loggers to quiet.
    level : int, *optional*
        Minimum level of log messages to allow.

    Examples
    --------
    >>> with silence_log():
    >>>     log.warning("This message is not recorded.")
    """
    # Default: the top-level logger for the package containing this file
    if names is None:
        names = [__name__.split(".")[0], "message_data"]
    elif isinstance(names, str):
        names = [names]

    log.info(f"Set level={level} for logger(s): {' '.join(names)}")

    # Retrieve the logger objects
    loggers = list(map(logging.getLogger, names))
    # Store their current levels
    levels = []

    try:
        for logger in loggers:
            levels.append(logger.getEffectiveLevel())  # Store the current levels
            logger.setLevel(level)  # Set the level
        yield
    finally:
        # Restore the levels
        for logger, original_level in zip(loggers, levels):
            logger.setLevel(original_level)
        log.info("…restored.")


@contextmanager
def preserve_log_level():
    """Context manager to preserve the level of the ``message_ix_models`` logger."""
    # Get the top-level logger for the package containing this file
    main_log = logging.getLogger(__name__.split(".")[0])

    try:
        # Store the current level
        level = main_log.getEffectiveLevel()
        yield
    finally:
        # Restore the level
        main_log.setLevel(level)


class Formatter(logging.Formatter):
    """Formatter for log records.

    Parameters
    ----------
    colorama : module
        If provided, :mod:`colorama` is used to colour log messages printed to stdout.
    """

    CYAN = ""
    DIM = ""
    RESET_ALL = ""

    _short_name = None

    def __init__(self, colorama):
        super().__init__()
        if colorama:
            self.CYAN = colorama.Fore.CYAN
            self.DIM = colorama.Style.DIM
            self.RESET_ALL = colorama.Style.RESET_ALL

    def format(self, record):
        """Format `record`.

        Records are formatted like::

            model.transport.data.add_par_data  220 rows in 'input'
            ...add_par_data:  further messages

        …with the calling function name (e.g. 'add_par_data') coloured for legibility
        on first occurrence, then dimmed when repeated.
        """
        # Remove the leading 'message_data.' from the module name
        name_parts = record.name.split(".")
        if name_parts[0] in ("message_ix_models", "message_data"):
            short_name = ".".join(["—"] + name_parts[1:])
        else:
            short_name = record.name

        if self._short_name != short_name:
            self._short_name = short_name
            prefix = f"{self.CYAN}{short_name}."
        else:
            prefix = f"{self.DIM}..."

        return f"{prefix}{record.funcName}{self.RESET_ALL}  {record.getMessage()}"


def make_formatter():
    """Return a :class:`Formatter` instance for the ``message_ix_models`` logger.

    See also
    --------
    setup
    """
    try:
        # Initialize colorama
        import colorama

        colorama.init()
    except ImportError:  # pragma: no cover
        # Colorama not installed
        colorama = None

    return Formatter(colorama)


_TIMES = []


def mark_time(quiet=False):
    """Record and log (if `quiet` is :obj:`True`) a time mark."""
    _TIMES.append(process_time())
    if not quiet and len(_TIMES) > 1:
        logging.getLogger(__name__).info(
            f" +{_TIMES[-1] - _TIMES[-2]:.1f} = {_TIMES[-1]:.1f} seconds"
        )


CONFIG = dict(
    version=1,
    disable_existing_loggers=False,
    formatters=dict(simple={"()": "message_ix_models.util._logging.make_formatter"}),
    handlers=dict(
        console={
            "class": "logging.StreamHandler",
            "level": "NOTSET",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        },
        # commented: needs code in setup() to choose an appropriate file path
        # file_handler={
        #    "class": "logging.handlers.RotatingFileHandler",
        #    "level": "DEBUG",
        #    "formatter": "simple",
        #    "backupCount": "100",
        #    "delay": True,
        # },
    ),
    loggers=dict(
        message_ix_models=dict(
            level="NOTSET",
            # propagate=False,
            # handlers=[],
        ),
        message_data=dict(
            level="NOTSET",
            # propagate=False,
            # handlers=[],
        ),
    ),
    root=dict(
        handlers=[],
    ),
)


def setup(
    level="NOTSET",
    console=True,
    # file=False,
):
    """Initialize logging.

    Parameters
    ----------
    level : str, *optional*
        Log level for :mod:`message_ix_models` and :mod:`message_data`.
    console : bool, *optional*
        If :obj:`True`, print all messages to console using a :class:`Formatter`.
    """
    # Copy to avoid modifying with the operations below
    config = deepcopy(CONFIG)

    config["root"].setdefault("level", level)

    if console:
        config["root"]["handlers"].append("console")
    # if file:
    #     config["loggers"]["message_data"]["handlers"].append("file")

    # Apply the configuration
    logging.config.dictConfig(config)
