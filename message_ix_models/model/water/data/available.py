import sys
from pathlib import Path
from message_ix_models.util import package_data_path

def main():
    # set region to check for files; change if needed
    region = "R12"

    # check yearly csv folder
    path = package_data_path("water", "demands", "harmonized", region, ".")
    print("directory:", path)
    yearly_files = sorted(path.glob("ssp2_regional_*.csv"))
    if yearly_files:
        print("found yearly files:")
        for f in yearly_files:
            print(f)
    else:
        print("no yearly files found in:", path)

    # check monthly file
    monthly_file = package_data_path("water", "demands", "harmonized", region, "ssp2_m_water_demands.csv")
    if monthly_file.exists():
        print("found monthly file:", monthly_file)
    else:
        print("no monthly file found:", monthly_file)

if __name__ == '__main__':
    main()