import pytest
from ixmp.testing import assert_logs

from message_ix_models.report.config import Config


class TestConfig:
    def test_register(self, caplog) -> None:
        cfg = Config()

        # Exception raised for unfindable module
        with pytest.raises(ModuleNotFoundError):
            cfg.register("foo.bar")

        # Adding a callback of the same name twice triggers a log message
        def _cb(*args):
            pass

        cfg.register(_cb)
        with assert_logs(
            caplog,
            "Already registered: <function TestConfig.test_register.<locals>._cb",
        ):
            cfg.register(_cb)

    def test_use_file(self, tmp_path):
        cfg = Config()

        # No effect
        cfg.use_file(None)

        # Passing a missing path raises an exception
        with pytest.raises(
            FileNotFoundError, match="Reporting configuration in .*missing"
        ):
            cfg.use_file(tmp_path.joinpath("missing"))

        # Passing a file name that does not exist raises an exception
        with pytest.raises(
            FileNotFoundError, match=r"Reporting configuration in 'unknown\(.yaml\)'"
        ):
            cfg.use_file("unknown")
