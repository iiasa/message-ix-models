import logging
import re

import pytest

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


class TestQueueListener:
    #: Number of log messages to emit.
    N = 1_000

    #: Number of times to run the test.
    k = 4

    @pytest.mark.parametrize(
        "method",
        ("click", pytest.param("subprocess", marks=pytest.mark.skip(reason="Slow."))),
    )
    @pytest.mark.parametrize("k", range(k))
    def test_flush(self, caplog, mix_models_cli, method, k):
        """Test logging in multiple processes, multiple threads, and with :mod:`click`.

        With pytest-xdist, these :attr:`k` test cases will run in multiple processes.
        Each process will have its main thread, and the thread of the QueueListener.
        The test ensures that all :attr:`N` log records emitted by the :py:`func()` are
        "flushed" from the queue, transferred to stdout by the :class:`.StreamHandler`
        and captured by the :class:`.CliRunner`.
        """

        # Run the command, capture output
        # See message_ix_models.cli._log_threads
        result = mix_models_cli.assert_exit_0(
            ["_test", "log-threads", str(k), str(self.N)], method=method
        )

        # All records are emitted; the last record ends with N - 1
        assert result.output.rstrip().endswith(f"{self.N - 1}"), result.output.split(
            "\n"
        )[-2:]


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
        "Set level=40 for logger(s): message_data message_ix_models",
        "…restored.",
    ] == caplog.messages
    caplog.clear()

    # After the "with" block, logging is restored
    log.warning(msg)
    assert [msg] == caplog.messages
