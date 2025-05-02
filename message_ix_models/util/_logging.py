"""Logging utilities."""

import atexit
import logging
import logging.config
import logging.handlers
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from queue import SimpleQueue
from time import process_time
from typing import TYPE_CHECKING, Optional, Union, cast
from warnings import warn

if TYPE_CHECKING:
    from logging import Logger, LogRecord

# NB mark_time, preserve_log_level, and silence_log are exposed by util/__init__.py
__all__ = [
    "Formatter",
    "QueueListener",
    "SilenceFilter",
    "StreamHandler",
    "setup",
]

log = logging.getLogger(__name__)

# References to handlers
_HANDLER: dict[str, logging.Handler] = dict()

# For mark_time()
_TIMES = []


class Formatter(logging.Formatter):
    """Formatter for log records.

    Parameters
    ----------
    use_color : bool, optional
        If :any:`True`, :mod:`colorama` is used to colour log messages.
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

    def format(self, record: "LogRecord") -> str:
        """Format `record`.

        Records are formatted like::

            model.transport.data.add_par_data  220 rows in 'input'
            ...add_par_data  further messages

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


class OnceFilter(logging.Filter):
    """Log filter that rejects matching messages `msg`."""

    __slots__ = ("msg",)

    def __init__(self, msg: str) -> None:
        self.msg = msg

    def filter(self, record: "LogRecord") -> bool:
        return not record.msg == self.msg


class QueueHandler(logging.handlers.QueueHandler):
    # For typing with Python ≤ 3.11 only; from 3.12 this attribute is described
    listener: "QueueListener"


class QueueListener(logging.handlers.QueueListener):
    """:class:`.logging.QueueListener` with a :meth:`.flush` method."""

    def flush(self) -> None:
        """Flush the queue: join the listener/monitor thread and then restart."""
        if self._thread is not None:
            super().stop()
            self.start()


class SilenceFilter(logging.Filter):
    """Log filter that only allows records from `names` that are at or above `level`."""

    __slots__ = ("level", "name_expr")

    def __init__(self, names: str, level: int) -> None:
        self.level = level
        # Compile a regular expression for the name
        self.name_re = re.compile("|".join(map(re.escape, sorted(names.split()))))

    def filter(self, record: "LogRecord") -> bool:
        return not (record.levelno < self.level and self.name_re.match(record.name))


class StreamHandler(logging.StreamHandler):
    """Like :class:`.logging.StreamHandler`, but retrieve the stream on each access.

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


def mark_time(quiet: bool = False) -> None:
    """Record and log (if `quiet` is :obj:`True`) a time mark."""
    _TIMES.append(process_time())
    if not quiet and len(_TIMES) > 1:
        logging.getLogger(__name__).info(
            f" +{_TIMES[-1] - _TIMES[-2]:.1f} = {_TIMES[-1]:.1f} seconds"
        )


def once(logger: "Logger", level: int, *args, **kwargs) -> None:
    """Log a message only once.

    The message indicated by `args` and `kwargs` is logged on `logger` at the given
    `level`. Then, a filter is added to the `logger` that rejects the same message if
    logged again, whether through :func:`once` or directly.
    """
    # Ensure record.funcName, as seen by Formatter.format() above, reflects the function
    # that called once()
    kwargs.setdefault("stacklevel", 2)

    # Actually log the message
    logger.log(level, *args, **kwargs)

    # Add a filter to `logger` to ignore matching messages in the future
    logger.addFilter(OnceFilter(args[0]))


@contextmanager
def preserve_log_handlers(name: Optional[str] = None):
    """Context manager to preserve the handlers of a `logger`."""
    # Access the named logger
    logger = logging.getLogger(name)
    # Make a copy of its list of handlers
    handlers = list(logger.handlers)

    try:
        yield
    finally:
        # Make a list of handlers which have disappeared from the logger
        to_restore = list(filter(lambda h: h not in logger.handlers, handlers))
        for h in to_restore:
            logger.addHandler(h)
        # Log after the handlers have been restored
        log.debug(f"Restore to {logger}.handlers: {to_restore or '(none)'}")


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


def configure() -> None:
    """Apply logging configuration."""
    # NB We do this programmatically as logging.config.dictConfig()'s automatic steps
    #    require adjustments that end up being more verbose and less clear.
    from platformdirs import user_log_path

    # Stream handler
    _HANDLER["console"] = h_console = StreamHandler(stream_name="stdout")
    h_console.setLevel(logging.CRITICAL)
    h_console.setFormatter(Formatter())

    # Construct the file name for the log file
    log_file_path = user_log_path("message-ix-models", ensure_exists=True).joinpath(
        datetime.now(timezone(timedelta(seconds=time.timezone)))
        .isoformat(timespec="seconds")
        .replace(":", "")
    )

    # File handler
    _HANDLER["file"] = h_file = logging.FileHandler(
        filename=str(log_file_path), delay=True
    )
    h_file.setLevel(logging.CRITICAL)
    h_file.setFormatter(Formatter(use_colour=False))

    # Queue handler
    queue: "SimpleQueue" = SimpleQueue()
    _HANDLER["queue"] = h_queue = QueueHandler(queue)
    logging.root.addHandler(h_queue)

    # Queue listener
    h_queue.listener = listener = QueueListener(
        queue, h_console, h_file, respect_handler_level=True
    )
    listener.start()
    atexit.register(listener.stop)

    for name, level in (
        (None, logging.DEBUG),
        # Ensure no level set for these packages; the level of the "console"/"file"
        # handlers determines outputs
        ("message_ix_models", logging.NOTSET),
        ("message_data", logging.NOTSET),
        # Hide lower-level messages for some upstream packages from the file log
        ("graphviz._tools", logging.WARNING),
        ("matplotlib", logging.WARNING),
        ("PIL", logging.INFO),
        ("pycountry.db", logging.WARNING),
    ):
        logging.getLogger(name).setLevel(level)


def setup(
    level: Union[str, int] = 99,
    console: bool = True,
    *,
    file: bool = False,
) -> None:
    """Initialize logging.

    Parameters
    ----------
    level : str, optional
        Log level for the console log handler.
    console : bool, optional
        If :obj:`False`, do not print any messages to console.
    file : bool, optional
        If :obj:`False`, do not print any messages to file.
    """

    root = logging.getLogger()
    if not any(isinstance(h, logging.handlers.QueueHandler) for h in root.handlers):
        # Not yet configured
        configure()

    # Apply settings to loggers and handlers: either just-created, or pre-existing

    # Set the level of the console handler
    _HANDLER["console"].setLevel(99 if console is False else level)

    if file is False:
        _HANDLER["file"].setLevel(99)
    else:
        _HANDLER["file"].setLevel("DEBUG")
        log.info(f"Log to {cast(logging.FileHandler, _HANDLER['file']).baseFilename}")


def flush() -> None:
    """Flush the queue."""
    cast(QueueHandler, _HANDLER["queue"]).listener.flush()


@contextmanager
def silence_log(names: Optional[str] = None, level: int = logging.ERROR):
    """Context manager to temporarily quiet 1 or more loggers.

    Parameters
    ----------
    names : str, optional
        Space-separated names of loggers to quiet.
    level : int, optional
        Minimum level of log messages to allow.

    Examples
    --------
    >>> with silence_log():
    >>>     log.warning("This message is not recorded.")
    """
    if isinstance(names, list):
        warn(
            "silence_log(names=…) as list of str; use a single, space-separated str",
            DeprecationWarning,
            stacklevel=2,
        )
        names = " ".join(names)

    # Create a filter; default, the top-level logger for the current package
    f = SilenceFilter(names or f"message_data {__name__.split('.')[0]}", level)
    log.info(f"Set level={level} for logger(s): {f.name_re.pattern.replace('|', ' ')}")

    try:
        # Add the same filter to every handler of the root logger
        for handler in logging.root.handlers:
            handler.addFilter(f)

        yield
    finally:
        # Remove the filter
        for handler in logging.root.handlers:
            handler.removeFilter(f)
        log.info("…restored.")
