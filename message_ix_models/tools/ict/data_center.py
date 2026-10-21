"""This module reads and formats ICT demand data from Fan et al. 2026.
It provides functions to generate ICT demand projections, read historical data,
and process regression parameters for modeling purposes."""

from typing import TYPE_CHECKING

import pandas as pd
from openpyxl import load_workbook

from message_ix_models.model.material.util import add_region_column
from message_ix_models.util import pycountry

if TYPE_CHECKING:
    from pathlib import Path

#: Pooch information for fetching required files.
SOURCE = {  # noqa: E501
    "fan_et_al_2026-SI": dict(
        pooch_args=dict(
            base_url="https://ars.els-cdn.com/content/image/",
            registry={
                "1-s2.0-S266627872600019X-mmc1.xlsx": (
                    "f971632daa709e62d65d809232766848fe027d472b682c85ac7fe0307e94fb51"
                ),
            },
        ),
    )
}
missing = {
    "Macedonia": "MKD",
    "Turkiye": "TUR",
    "Ethiopia": "ETH",
    "Cote d Ivoire": "CIV",
    "Venzuela": "VEN",
    "Curacao": "CUW",
    "Hong Kong (China)": "HKG",
    "Myanmar (Burma)": "MMR",
}


def fetch() -> "Path":
    """Retrieve data for `version` of the Climate Policy Database from Zenodo."""
    from message_ix_models.util import pooch

    # Ensure sources for this module are registered
    pooch.SOURCE.update(SOURCE)

    return pooch.fetch(**pooch.SOURCE["fan_et_al_2026-SI"], extra_cache_path="ict")[0]


def read_hist_data() -> pd.DataFrame:
    pre_2030 = pd.read_excel(
        fetch(), sheet_name="2|Pre2030 Regionally", usecols=[0, 1, 3]
    )
    return pre_2030


def get_cisco_region_mapping() -> dict[str, str]:
    """Read Cisco region-country mapping used in Fan et al. 2026."""
    sh_name = "4|Countries & Classification"
    reg_map = pd.concat(
        [
            pd.read_excel(
                fetch(),
                sheet_name=sh_name,
                usecols=[i],
            ).iloc[:32]
            for i in range(0, 17, 3)
        ]
    ).to_dict("series")
    cmap = {c: k for k, v in reg_map.items() for c in v.values.tolist()}
    cmap.update(
        {"Russian Federation": "Central and Eastern Europe", "Viet Nam": "Asia Pacific"}
    )
    return cmap


def get_ignored_countries() -> list[str]:
    """Return list of countries that were dropped from region map in Fan et al. 2026.

    Note: These countries are highlighted with a grey background color in the xlsx.
    Uses `openpyxl` to read the xlsx sheet and check the cell fill color for each
    country.
    """
    sh_name = "4|Countries & Classification"

    wb = load_workbook(fetch())
    ws = wb[sh_name]

    ignored = []
    for row in ws.iter_rows(min_row=2, max_row=32, min_col=1, max_col=16):
        for cell in row:
            if cell.fill.fgColor.tint != 0:
                ignored.append(cell.value)
    wb.close()
    return [i for i in ignored if isinstance(i, str)]


def read_data_centre_count_shares(n: str, merge_regions=True) -> pd.DataFrame:
    """Compute data center shares of given regions against Fan et al. region mapping.

    Parameters
    ----------
    n : str
        MESSAGEix region configuration code
    """

    dc_count = get_dc_numbers()
    # drop countries that lack data quality according to Fan et al. 2026
    ignored_countries = get_ignored_countries()
    ignored_isos = [
        pycountry.iso_3166_alpha_3(i) for i in ignored_countries if isinstance(i, str)
    ]
    dc_count = dc_count[~dc_count["ISO"].isin(ignored_isos)]

    # add column with assigned region (Cisco mapping)
    dc_count["Region"] = dc_count["Country"].map(get_cisco_region_mapping())
    if merge_regions:
        dc_count.replace(
            {"Central and Eastern Europe": "Europe", "Western Europe": "Europe"},
            inplace=True,
        )
    # add MESSAGEix region code column for given configuration
    dc_count[n] = add_region_column(dc_count, ("node", f"{n}.yaml"), "ISO")
    dc_count_cisco_regs = dc_count.groupby("Region").sum(numeric_only=True)
    iea_map = dc_count[["Region", n]].drop_duplicates().dropna().set_index(n)
    dc_count_reg = dc_count.groupby(n).sum(numeric_only=True)
    dc_shares = (
        dc_count_reg.join(iea_map)
        .set_index("Region", append=True)
        .div(dc_count_cisco_regs)
    )
    return dc_shares


def get_dc_numbers() -> pd.DataFrame:
    pycountry.COUNTRY_NAME.update(missing)
    sh_name = "4|Countries & Classification"

    dc_count = (
        pd.concat(
            [
                pd.read_excel(
                    fetch(),
                    sheet_name=sh_name,
                    usecols=[i, 1 + i],
                    names=["Country", "Value"],
                ).iloc[:32]
                for i in range(0, 17, 3)
            ]
        )
        .assign(Country=lambda x: x["Country"].str.strip())
        .dropna()
    )
    dc_count["ISO"] = dc_count["Country"].apply(lambda x: pycountry.iso_3166_alpha_3(x))
    dc_count["Value"] = dc_count["Value"].astype(int)
    return dc_count


def read_regr_pars() -> pd.DataFrame:
    """Read pre-computed regression parameters from Fan et al. 2026.

    Parameters
    ----------
    sensi : str
        Sensitivity scenario, e.g., "Efficiency Dominant" or "Demand Dominant".
    period : str
        Time window of regression sample, e.g., "2012-2017" or "2018-2023".

    Returns
    -------

    """
    df = pd.read_excel(
        fetch(),
        sheet_name=4,
        usecols=[0, 1, 2, 7, 8, 9],
        names=["Region", "Period", "Sensitivity", "beta", "const", "eps"],
        index_col=[0, 1, 2],
    ).swaplevel(0, 2)
    for col in ["beta", "const", "eps"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna().drop_duplicates()
    return df
