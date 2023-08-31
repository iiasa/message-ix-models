import logging
import re

from message_ix_models.util._logging import mark_time, silence_log


def test_mark_time(caplog):
    # Call 3 times
    mark_time()  # Will only log if already called during the course of another test
    mark_time()
    mark_time()

    # Either 2 or 3 records
    assert len(caplog.records) in (2, 3)

    # Each message matches the expected format
    assert all(re.match(r" \+\d+\.\d = \d+\.\d seconds", m) for m in caplog.messages)


def test_silence_log(caplog):
    # An example logger
    log = logging.getLogger("message_ix_models.model")

    msg = "Here's a warning!"

    # pytest caplog fixture picks up warning messages
    log.warning(msg)
    assert [msg] == caplog.messages

    caplog.clear()

    # silence_log() hides the messages
    with silence_log():
        log.warning(msg)

    assert [
        "Set level=40 for logger(s): message_ix_models message_data",
        "…restored.",
    ] == caplog.messages
    caplog.clear()

    # After the "with" block, logging is restored
    log.warning(msg)
    assert [msg] == caplog.messages
