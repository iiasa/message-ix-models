from typing import Dict

import numpy as np
import pandas as pd

from message_ix_models.util import package_data_path

# Dict of technology types and the learning rates under each SSP
# Data translated from excel form into python form from Sheet 1 in
# https://github.com/iiasa/message_data/blob/dev/data/model/investment_cost/SSP_technology_learning.xlsx
DICT_TECH_SSP_LEARNING = {
    "Biomass": {
        "SSP1": "high",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "medium",
    },
    "CCS": {
        "SSP1": "medium",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "high",
    },
    "Coal": {
        "SSP1": "medium",
        "SSP2": "medium",
        "SSP3": "high",
        "SSP4": "medium",
        "SSP5": "medium",
    },
    "Gas/Oil": {
        "SSP1": "high",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "medium",
        "SSP5": "high",
    },
    "Nuclear": {
        "SSP1": "medium",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "high",
    },
    "Renewable": {
        "SSP1": "high",
        "SSP2": "medium",
        "SSP3": "low",
        "SSP4": "high",
        "SSP5": "medium",
    },
    "NA": {
        "SSP1": "none",
        "SSP2": "none",
        "SSP3": "none",
        "SSP4": "none",
        "SSP5": "none",
    },
}


def get_cost_reduction_data() -> pd.DataFrame:
    """Create SSP technological learning data

    Raw data from GEA on cost reduction for technologies are read from \
        :file:`data/costs/gea_cost_reduction.csv`.

    This function takes the raw GEA (low, medium, and high) cost reduction \
        values and assign SSP-specific cost reduction values. The growth rate \
        under each SSP scenario (for each technology) is specified in \
        the input dictionary (`input_dict_tech_learning`). If the SSP \
        learning rate is "low", then the cost reduction rate is the minimum of the GEA \
        values for that technology. If the SSP learning rate is "medium" or "high", \
        then the cost reduction rate is the median of the GEA scenarios or the maximum \
        of the GEA scenarios, respectively.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - message_technology: technologies included in MESSAGE
        - technology_type: the technology type (either coal, gas/oil, biomass, CCS, \
            renewable, nuclear, or NA)
        - GEAL: cost reduction in 2100 (%) under the low (L) GEA scenario
        - GEAM: cost reduction in 2100 (%) under the medium (M) GEA scenario
        - GEAH: cost reduction in 2100 (%) under the high (H) GEA scenario
        - SSPX_learning: one corresponding column for each SSP scenario \
            (SSP1, SSP2, SSP3, SSP4, SSP5). These columns specify the learning \
            rate for each technology under that specific scenario
        - SSPX_cost_reduction: the cost reduction (%) of the technology under the \
            specific scenario
    """

    input_dict_tech_learning = DICT_TECH_SSP_LEARNING

    # Read in raw data files
    gea_file_path = package_data_path("costs", "gea_cost_reduction.csv")

    # Read in data and assign basic columns
    df_gea = (
        pd.read_csv(gea_file_path, header=6)
        .rename(
            columns={"Technologies": "message_technology", "Type": "technology_type"}
        )
        .assign(
            learning=lambda x: np.where(
                (x["GEAL"] == 0) & (x["GEAM"] == 0) & (x["GEAH"] == 0), "no", "yes"
            ),
            min_gea=lambda x: x[["GEAL", "GEAM", "GEAH"]].min(axis=1),
            median_gea=lambda x: np.median(x[["GEAL", "GEAM", "GEAH"]], axis=1),
            max_gea=lambda x: x[["GEAL", "GEAM", "GEAH"]].max(axis=1),
        )
        .replace({"technology_type": np.nan}, "NA")
    )

    # Assign SSP learning category and SSP-specific cost reduction rate
    def assign_ssp_learning():
        cols = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]
        for c in cols:
            df_gea[c + "_learning"] = np.where(
                df_gea["learning"] == "no",
                "none",
                df_gea.technology_type.map(lambda x: input_dict_tech_learning[x][c]),
            )
            df_gea[c + "_cost_reduction"] = np.where(
                df_gea[c + "_learning"] == "low",
                df_gea["min_gea"],
                np.where(
                    df_gea[c + "_learning"] == "medium",
                    df_gea["median_gea"],
                    np.where(
                        df_gea[c + "_learning"] == "high",
                        df_gea["max_gea"],
                        0,
                    ),
                ),
            )

    assign_ssp_learning()

    return df_gea
