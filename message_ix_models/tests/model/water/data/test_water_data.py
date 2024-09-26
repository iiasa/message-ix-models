import os

from message_ix_models.util import package_data_path


def check_files_in_folder(folder_path):
    # List all files in the folder with .csv extension
    files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    # Find files that contain "_R11" but not "6p0"
    r11_files = [f for f in files if "_R11" in f and "6p0" not in f]

    # List of files where _R12 counterpart doesn't exist
    missing_r12_files = []

    for r11_file in r11_files:
        # Create the equivalent _R12 file name
        r12_file = r11_file.replace("_R11", "_R12")

        # Check if the _R12 file exists
        if r12_file not in files:
            missing_r12_files.append(r11_file)

    # If there are missing _R12 files, print them and assert an error
    assert not missing_r12_files, (
        "Error: Missing _R12 files for the following _R11 files:\n"
        + "\n".join(missing_r12_files)
    )

    print("All _R11 files have corresponding _R12 files or contain '6p0'.")


# Define the folder path
folder_path = package_data_path("water", "availability")

# Run the file check
check_files_in_folder(folder_path)
