import pandas as pd

from message_ix_models.util import package_data_path


# Function to read in SSP Phase 1 Review data
# and filter out data for only the variables of interest.
def subset_ssp_phase_1_data():
    """Read in SSP Phase 1 Review data and only keep data with variables of interest.

    The reason for this function is because the complete data file is quite large
    and takes too long to read in the module. This is not an integral part of \
    the module, only a fix during the development and exploration phase.

    Returns
    -------
    df : pd.DataFrame
        Dataframe containing the filtered data.
        The data is still in the same format as the input spreadsheet (IAMC format).
    """
    # Set data path for SSP data
    f = package_data_path("ssp", "SSP-Review-Phase-1.xlsx")

    # Read in Phase 1 Review SSP data and do the following:
    # - Filter for population and GDP data only
    # - Filter for IIASA-WiC POP population data and OECD ENV-Growth GDP data only
    # - Remove World from regions and remove non-country regions
    df = (
        pd.read_excel(f, sheet_name="data", usecols="A:Z")
        .query("Variable == 'Population' or Variable == 'GDP|PPP'")
        .query(
            "Model.str.contains('IIASA-WiC POP') or\
                Model.str.contains('OECD ENV-Growth')"
        )
        .query(
            r"~(Region.str.contains('\(') or Region.str.contains('World'))",
            engine="python",
        )
    )

    return df


# Save subsetted SSP data to a csv file in the same location
def save_subset_ssp_phase_1_data():
    print("Reading in and filtering SSP data...")
    df = subset_ssp_phase_1_data()

    print("Saving subsetted SSP data to csv file...")
    df.to_csv(package_data_path("ssp", "SSP-Review-Phase-1-subset.csv"), index=False)


# Run to subset and save the SSP data
if __name__ == "__main__":
    save_subset_ssp_phase_1_data()
