import pytest

from message_ix_models.report.legacy.config import Config
from message_ix_models.util import package_data_path


class TestConfig:
    @pytest.mark.parametrize(
        "filename, N_tables",
        (
            ("default_run_config.yaml", 53),
            ("ENGAGE_SSP2_v417_run_config.yaml", 4),
        ),
    )
    def test_from_file(self, filename: str, N_tables: int) -> None:
        """:meth:`Config.from_file` handles known :file:`*_run_config.yaml` files."""
        result = Config.from_file(package_data_path("report", "legacy", filename))
        assert N_tables == len(result.table)
