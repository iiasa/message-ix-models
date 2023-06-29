import numpy as np
import pandas as pd
from scipy.stats import linregress

from message_ix_models.util import package_data_path


def get_gdp_data() -> pd.DataFrame:
    """Read in raw GDP data for SSP1, SSP2, SSP3 and output GDP ratios

    Data are read from the files
    :file:`data/iea/gdp_pp_per_capita-ssp1_v9.csv`,
    :file:`data/iea/gdp_pp_per_capita-ssp2_v9.csv`, and
    :file:`data/iea/gdp_pp_per_capita-ssp3_v9.csv`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - scenario: SSP1, SSP2, or SSP3
        - r11_region: R11 region
        - year: values from 2000 to 2100
        - gdp_ppp_per_capita: GDP PPP per capita, in units of billion US$2005/yr/million
        - gdp_ratio_oecd: the maximum ratio of each region's GDP compared to OECD \
            regions
        - gdp_ratio_nam: the ratio of each region's GDP compared to NAM region
    """

    scens = ["ssp1", "ssp2", "ssp3"]
    l_dfs = []
    for s in scens:
        f = package_data_path("costs", "gdp_pp_per_capita-" + str(s) + "_v9.csv")
        df = (
            pd.read_csv(f, header=4)
            .melt(
                id_vars=["Model", "Scenario", "Region", "Variable", "Unit"],
                var_name="year",
                value_name="gdp_ppp_per_capita",
            )
            .drop(columns=["Model", "Scenario", "Variable", "Unit"])
            .rename(columns={"Region": "r11_region", "Scenario": "scenario"})
            .assign(scenario=s.upper(), units="billion US$2005/yr/million")
            .replace({"r11_region": {"R11": ""}}, regex=True)
            .pipe(
                lambda df_: pd.merge(
                    df_,
                    df_.loc[df_.r11_region.isin(["NAM", "PAO", "WEU"])]
                    .groupby("year")["gdp_ppp_per_capita"]
                    .aggregate(["min", "mean", "max"])
                    .reset_index(drop=0),
                    on="year",
                )
            )
            .pipe(
                lambda df_: pd.merge(
                    df_,
                    df_.loc[df_.r11_region == "NAM"][["year", "gdp_ppp_per_capita"]]
                    .rename(columns={"gdp_ppp_per_capita": "gdp_nam"})
                    .reset_index(drop=1),
                    on="year",
                )
            )
            .rename(columns={"min": "oecd_min", "mean": "oecd_mean", "max": "oecd_max"})
            .assign(
                ratio_oecd_min=lambda x: np.where(
                    x.r11_region.isin(["NAM", "PAO", "WEU"]),
                    1,
                    x.gdp_ppp_per_capita / x.oecd_min,
                ),
                ratio_oecd_max=lambda x: np.where(
                    x.r11_region.isin(["NAM", "PAO", "WEU"]),
                    1,
                    x.gdp_ppp_per_capita / x.oecd_max,
                ),
                gdp_ratio_oecd=lambda x: np.where(
                    (x.ratio_oecd_min >= 1) & (x.ratio_oecd_max <= 1),
                    1,
                    x[["ratio_oecd_min", "ratio_oecd_min"]].max(axis=1),
                ),
                gdp_ratio_nam=lambda x: x.gdp_ppp_per_capita / x.gdp_nam,
            )
            .reindex(
                [
                    "scenario",
                    "r11_region",
                    "year",
                    "gdp_ppp_per_capita",
                    "gdp_ratio_oecd",
                    "gdp_ratio_nam",
                ],
                axis=1,
            )
        )

        l_dfs.append(df)

    df_gdp = pd.concat(l_dfs).reset_index(drop=1)

    return df_gdp


def linearly_regress_tech_cost_vs_gdp_ratios(
    gdp_ratios: pd.DataFrame, tech_cost_ratios: pd.DataFrame
) -> pd.DataFrame:
    """Compute linear regressions of technology cost ratios to GDP ratios

    Parameters
    ----------
    gdp_ratios : pandas.DataFrame
        Dataframe output from :func:`.get_gdp_data`
    tech_cost_ratios : str -> tuple of (str, str)
        Dataframe output from :func:`.calculate_region_cost_ratios`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - scenario: SSP1, SSP2, or SSP3
        - r11_region: R11 region
        - year: values from 2000 to 2100
        - gdp_ppp_per_capita: GDP PPP per capita, in units of billion US$2005/yr/million
        - gdp_ratio_oecd: the maximum ratio of each region's GDP compared to OECD \
            regions
        - gdp_ratio_nam: the ratio of each region's GDP compared to NAM region
    """

    gdp_2020 = gdp_ratios.loc[gdp_ratios.year == "2020"][
        ["scenario", "r11_region", "gdp_ratio_nam"]
    ].reset_index(drop=1)
    cost_capital_2021 = tech_cost_ratios[
        ["technology", "r11_region", "cost_type", "cost_ratio"]
    ].reset_index(drop=1)

    df_gdp_cost = (
        pd.merge(gdp_2020, cost_capital_2021, on=["r11_region"])
        .reset_index(drop=2)
        .reindex(
            [
                "cost_type",
                "scenario",
                "r11_region",
                "technology",
                "gdp_ratio_nam",
                "cost_ratio",
            ],
            axis=1,
        )
        .groupby(["cost_type", "scenario", "technology"])
        .apply(lambda x: pd.Series(linregress(x["gdp_ratio_nam"], x["cost_ratio"])))
        .rename(
            columns={0: "slope", 1: "intercept", 2: "rvalue", 3: "pvalue", 4: "stderr"}
        )
        .reset_index()
    )

    return df_gdp_cost
