"""Logging utilities."""

import atexit
import logging
import logging.config
import logging.handlers
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from queue import SimpleQueue
from time import process_time
from typing import List, Union

# NB mark_time, preserve_log_level, and silence_log are exposed by util/__init__.py
__all__ = [
    "Formatter",
    "QueueHandler",
    "StreamHandler",
    "setup",
]

log = logging.getLogger(__name__)

# NB This is only separate to avoid complaints from mypy
_HANDLER_CONFIG = dict(
    console={
        "class": "message_ix_models.util._logging.StreamHandler",
        "level": 99,
        "formatter": "color",
        "stream_name": "stdout",
    },
    file={
        "class": "logging.FileHandler",
        "level": 99,
        "formatter": "plain",
        "delay": True,
    },
    queue={
        "class": "message_ix_models.util._logging.QueueHandler",
        "handlers": ["console", "file"],
        "respect_handler_level": True,
    },
)


_CONFIG = dict(
    version=1,
    disable_existing_loggers=False,
    formatters=dict(
        color={"()": "message_ix_models.util._logging.Formatter"},
        plain={"()": "message_ix_models.util._logging.Formatter", "use_colour": False},
    ),
    handlers=_HANDLER_CONFIG,
    loggers={
        # Ensure no level set for these packages; the level of the "console"/"file"
        # handlers determines outputs
        "message_ix_models": dict(level=logging.NOTSET),
        "message_data": dict(level=logging.NOTSET),
        # Hide DEBUG messages for some upstream packages from the file log
        "graphviz._tools": dict(level=logging.DEBUG + 1),
        "pycountry.db": dict(level=logging.DEBUG + 1),
        "matplotlib.backends": dict(level=logging.DEBUG + 1),
        "matplotlib.font_manager": dict(level=logging.DEBUG + 1),
    },
    root=dict(handlers=["queue"]),
)

# For mark_time()
_TIMES = []


class Formatter(logging.Formatter):
    """Formatter for log records.

    Parameters
    ----------
    use_color : bool, *optional*
        If :any:`True`, :mod:`colorama` is used to colour log messages printed to
        stdout.
    """

    CYAN = ""
    DIM = ""
    RESET_ALL = ""

    _short_name = None

    def __init__(self, use_colour: bool = True):
        super().__init__()

        try:
            if use_colour:
                # Import and initialize colorama
                import colorama

                colorama.init()
                self.CYAN = colorama.Fore.CYAN
                self.DIM = colorama.Style.DIM
                self.RESET_ALL = colorama.Style.RESET_ALL
        except ImportError:  # pragma: no cover
            pass  # Not installed

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


class QueueHandler(logging.handlers.QueueHandler):
    """Queue handler with custom set-up.

    This emulates the default behaviour available in Python 3.12.
    """

    #: Corresponding listener that dispatches to the actual handlers.
    listener: logging.handlers.QueueListener

    def __init__(
        self, *, handlers: List[str] = [], respect_handler_level: bool = False
    ) -> None:
        super().__init__(SimpleQueue())

        # Construct the listener
        # NB This relies on the non-public collection logging._handlers
        self.listener = logging.handlers.QueueListener(
            self.queue,
            *[logging._handlers[name] for name in handlers],  # type: ignore [attr-defined]
            respect_handler_level=respect_handler_level,
        )

        self.listener.start()
        atexit.register(self.listener.stop)


class StreamHandler(logging.StreamHandler):
    """Like :class:`.logging.StreamHandler`, but refresh sys.stdout on each access.

    This avoids the case that :mod:`click`, :mod:`pytest`, or something else adjusts
    :py:`sys.stdout` temporarily, but the handler's stored reference to the original is
    not updated.
    """

    #: Name of the :mod:`sys` stream to use, as :class:`str` rather than a direct
    #: reference.
    stream_name: str

    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        logging.Handler.__init__(self)

    @property
    def stream(self):
        return getattr(sys, self.stream_name)


def _get_handler(name: str) -> logging.Handler:
    """Retrieve one of the handlers of the :class:`logging.handlers.QueueListener`."""
    queue_handler = logging.getLogger().handlers[0]
    assert isinstance(queue_handler, QueueHandler)

    for h in queue_handler.listener.handlers:
        if h.name == name:
            return h

    raise ValueError(name)


def mark_time(quiet: bool = False) -> None:
    """Record and log (if `quiet` is :obj:`True`) a time mark."""
    _TIMES.append(process_time())
    if not quiet and len(_TIMES) > 1:
        logging.getLogger(__name__).info(
            f" +{_TIMES[-1] - _TIMES[-2]:.1f} = {_TIMES[-1]:.1f} seconds"
        )


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


def setup(
    level: Union[str, int] = 99,
    console: bool = True,
    *,
    file: bool = False,
) -> None:
    """Initialize logging.

    Parameters
    ----------
    level : str, *optional*
        Log level for the console log handler.
    console : bool, *optional*
        If :obj:`False`, do not print any messages to console.
    file : bool, *optional*
        If :obj:`False`, do not print any messages to file.
    """
    from platformdirs import user_log_path

    # Construct the file name for the log file
    filename = (
        datetime.now(timezone(timedelta(seconds=time.timezone)))
        .isoformat(timespec="seconds")
        .replace(":", "")
    )
    log_dir = user_log_path("message-ix-models", ensure_exists=True)
    _HANDLER_CONFIG["file"].setdefault("filename", log_dir.joinpath(filename))
    _CONFIG["handlers"] = _HANDLER_CONFIG

    root = logging.getLogger()
    if not root.handlers:
        # Not yet configured → apply the configuration
        logging.config.dictConfig(_CONFIG)

    # Apply settings to loggers and handlers: either just-created, or pre-existing

    # Set the level of the console handler
    _get_handler("console").setLevel(99 if console is False else level)

    file_handler = _get_handler("file")
    if file is False:
        file_handler.setLevel(99)
    else:
        file_handler.setLevel("DEBUG")
        log.info(f"Log to {_HANDLER_CONFIG['file']['filename']!s}")


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
