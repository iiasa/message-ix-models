import pytest

from message_ix_models.report.config import Config


class TestConfig:
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
