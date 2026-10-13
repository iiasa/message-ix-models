"""This module reads and formats digital transformation index projections
from Fan et al. 2025."""

from typing import TYPE_CHECKING

import pandas as pd

from message_ix_models.util import pycountry

if TYPE_CHECKING:
    from pathlib import Path

#: Pooch information for fetching required files.
SOURCE = {  # noqa: E501
    "ssp-digitrans": dict(
        pooch_args=dict(
            base_url="https://media.springernature.com/original/springer-static/esm/art%3A10.1038%2Fs44168-025-00283-w/MediaObjects/",
            registry={
                "44168_2025_283_MOESM2_ESM.xlsx": (
                    "a3bbb1d39341fc0acf62c183b8dd7f770d1b4f76c55eed179a14ca92f03c0972"
                ),
            },
        ),
    )
}


def fetch() -> "Path":
    """Retrieve data for `version` of the Climate Policy Database from Zenodo."""
    from message_ix_models.util import pooch

    # Ensure sources for this module are registered
    pooch.SOURCE.update(SOURCE)
    return pooch.fetch(**pooch.SOURCE["ssp-digitrans"], extra_cache_path="ict")[0]


def read_country_data(ssp: str, full=False, col=7) -> pd.DataFrame:
    """Read country-level data and convert country names to ISO3 codes.

    Parameters
    ----------
    ssp :
        SSP scenario to read data for.
    full :
        If True, include out of sample data from the second sheet.
    col:
        Used to select the quantity of interest.
        7 = digital transformation index,
        8 = digital transformation index * population.

    Returns
    -------
    """
    col_names = ["Country", "Year", "Scenario", "Value"]
    df1 = pd.read_excel(fetch(), sheet_name=0, usecols=[0, 1, 3, col], names=col_names)
    df2 = pd.read_excel(fetch(), sheet_name=2, usecols=[0, 1, 2, 7], names=col_names)
    missing = {
        "Macedonia": "MKD",
        "Turkiye": "TUR",
        "Cote d'Ivoire": "CIV",
        "Democratic Republic of the Congo": "COD",
        "Micronesia": "FSM",
    }
    pycountry.COUNTRY_NAME.update(missing)
    if full:
        df = pd.concat([df1, df2]).query("Scenario == @ssp")
    else:
        df = df1.query("Scenario == @ssp")
    df["ISO"] = df["Country"].apply(lambda x: pycountry.iso_3166_alpha_3(x))
    return df


def read_region_data(ssp: str) -> pd.DataFrame:
    """Read pre-aggregated region data at MESSAGEix R12 resolution.

    Parameters
    ----------
    ssp:
        SSP scenario to read data for.
    """
    col_names = ["Region", "Scenario", "Year", "Value"]
    df = (
        pd.read_excel(
            fetch(), sheet_name=-1, usecols=[0, 1, 2, 5], names=col_names, skiprows=1
        )
        .query("Scenario == @ssp")
        .drop("Scenario", axis=1)
    )
    return df


def aggregate_to_region(df: pd.DataFrame, cmap: dict[str, str]) -> pd.Series:
    """Aggregate country-level data to given regions using population weights.

    Parameters
    ----------
    df :
        Country-level data with columns "ISO", "Year", and "Value".
    cmap :
        Mapping from ISO3 country codes to region names.

    Returns
    -------
    """
    from genno import Computer

    from message_ix_models.project.ssp.data import SSPUpdate

    c = Computer()
    keys = SSPUpdate.add_tasks(
        c,
        context=None,
        release="3.1",
        measure="POP",
        ssp_id="2",
        aggregate=False,
        interpolate=False,
    )
    pop = c.get(keys[0]).to_frame().rename_axis(index={"y": "Year", "n": "ISO"})
    pop = pop[pop.index.get_level_values(0).isin(df["ISO"].unique())]
    pop_dig = (
        df.set_index(["ISO", "Year"])["Value"].mul(pop["value"]).dropna().reset_index()
    )
    pop["Region"] = pop.index.get_level_values(0).map(cmap)
    # map to regions
    pop_dig["Region"] = pop_dig["ISO"].map(cmap)
    pop_dig_reg = pop_dig.groupby(["Region", "Year"]).sum(numeric_only=True)
    pop_reg = pop.groupby(["Region", "Year"]).sum(numeric_only=True)
    result = pop_dig_reg[0].div(pop_reg["value"]).dropna()
    return result
