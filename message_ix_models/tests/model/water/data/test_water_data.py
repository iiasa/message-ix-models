import logging
import os

import pytest

from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "folder", ["availability", "infrastructure", "ppl_cooling_tech"]
)
def test_water_data_files(folder):
    folder_path = package_data_path("water", folder)
    # List all files in the folder with .csv extension
    files = {f for f in os.listdir(folder_path) if f.endswith(".csv")}

    # Find files that contain "_R11" but not "6p0"
    r11_files = {f for f in files if "_R11" in f and "6p0" not in f}

    # List of files where _R12 counterpart doesn't exist
    r12_files = {r11_file.replace("_R11", "_R12") for r11_file in r11_files}
    missing_r12_files = r12_files - files

    # If there are missing _R12 files, print them and assert an error
    assert not missing_r12_files, (
        "Error: Missing _R12 files that have a corresponding _R11 file:\n"
        + "\n".join(missing_r12_files)
    )

    log.info("All _R11 files have corresponding _R12 files or contain '6p0'.")
