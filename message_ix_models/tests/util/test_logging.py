import logging
import re

from message_ix_models.util.logging import silence_log, mark_time


def test_mark_time(caplog):
    # Call 3 times
    mark_time()
    mark_time()
    mark_time()

    # First call to mark_time() doesn't print anything, because no delta → 2 records
    assert 2 == len(caplog.records)

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

    assert [] == caplog.messages

    # After the "with" block, logging is restored
    log.warning(msg)
    assert [msg] == caplog.messages
