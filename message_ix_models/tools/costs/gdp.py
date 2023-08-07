import numpy as np
import pandas as pd
import yaml  # type: ignore
from nomenclature import countries
from scipy.stats import linregress  # type: ignore

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
        - gdp_ratio_reg_to_oecd: the maximum ratio of each region's GDP compared to \
            OECD regions
        - gdp_ratio_reg_to_nam: the ratio of each region's GDP compared to NAM region
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
                gdp_ratio_reg_to_oecd=lambda x: np.where(
                    (x.ratio_oecd_min >= 1) & (x.ratio_oecd_max <= 1),
                    1,
                    x[["ratio_oecd_min", "ratio_oecd_min"]].max(axis=1),
                ),
                gdp_ratio_reg_to_nam=lambda x: x.gdp_ppp_per_capita / x.gdp_nam,
            )
            .reindex(
                [
                    "scenario",
                    "r11_region",
                    "year",
                    "gdp_ppp_per_capita",
                    "gdp_ratio_reg_to_oecd",
                    "gdp_ratio_reg_to_nam",
                ],
                axis=1,
            )
        )

        l_dfs.append(df)

    df_gdp = pd.concat(l_dfs).reset_index(drop=1)

    return df_gdp


# Function to read in (under-review) SSP data
def process_raw_ssp_data(input_node: str, input_ref_region: str) -> pd.DataFrame:
    """Read in raw SSP data and process it

    This function takes in the raw SSP data (in IAMC format), aggregates \
    it to a specified node/regional level, and calculates regional GDP \
    per capita. The SSP data is read from the file \
    :file:`data/iea/SSP-Review-Phase-1-subset.csv`.

    Parameters
    ----------
    sel_node : str
        The node/region to aggregate the SSP data to. Valid values are \
        "R11", "R12", and "R20" (can be given in lowercase or uppercase). \
        Defaults to "R12".

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - scenario: SSP scenario
        - region: R11, R12, or R20 region
        - year
        - total_gdp: total GDP (in units of billion US$2005/yr)
        - total_population: total population (in units of million)
        - gdp_ppp_per_capita: GDP per capita (in units of billion US$2005/yr / million)
    """
    # Change node selection to upper case
    node_up = input_node.upper()

    # Check if node selection is valid
    if node_up not in ["R11", "R12", "R20"]:
        print("Please select a valid region: R11, R12, or R20")

    # Set default reference region
    if input_ref_region is None:
        if input_node.upper() == "R11":
            input_ref_region = "R11_NAM"
        if input_node.upper() == "R12":
            input_ref_region = "R12_NAM"
        if input_node.upper() == "R20":
            input_ref_region = "R20_NAM"
    else:
        input_ref_region = input_ref_region

    # Set data path for node file
    node_file = package_data_path("node", node_up + ".yaml")

    # Read in node file
    with open(node_file, "r") as file:
        nodes_data = yaml.load(file, Loader=yaml.FullLoader)

    # Remove World from regions
    nodes_data = {k: v for k, v in nodes_data.items() if k != "World"}

    # Create dataframe with regions and their respective countries
    regions_countries = (
        pd.DataFrame.from_dict(nodes_data)
        .stack()
        .explode()
        .reset_index()
        .query("level_0 == 'child'")
        .rename(columns={"level_1": "region", 0: "country_alpha_3"})
        .drop(columns=["level_0"])
    )

    # Set data path for SSP data
    f = package_data_path("ssp", "SSP-Review-Phase-1-subset.csv")

    # Read in SSP data and do the following:
    # - Rename columns
    # - Melt dataframe to long format
    # - Fix character errors in Réunion, Côte d'Ivoire, and Curaçao
    # - Use nomenclature to add country alpha-3 codes
    # - Drop model column and original country name column
    # - Merge with regions_countries dataframe to get country-region matching
    # - Aggregate GDP and population to model-scenario-region-year level
    # - Calculate GDP per capita by dividing total GDP by total population
    df = (
        pd.read_csv(f)
        .rename(
            columns={
                "Model": "model",
                "Scenario": "scenario_version",
                "Region": "country_name",
                "Variable": "variable",
                "Unit": "unit",
                "Year": "year",
                "Value": "value",
            }
        )
        .melt(
            id_vars=[
                "model",
                "scenario_version",
                "country_name",
                "variable",
                "unit",
            ],
            var_name="year",
            value_name="value",
        )
        .assign(
            scenario=lambda x: x.scenario_version.str[:4],
            year=lambda x: x.year.astype(int),
            country_name_adj=lambda x: np.where(
                x.country_name.str.contains("R?union"),
                "Réunion",
                np.where(
                    x.country_name.str.contains("C?te d'Ivoire"),
                    "Côte d'Ivoire",
                    np.where(
                        x.country_name.str.contains("Cura"),
                        "Curaçao",
                        x.country_name,
                    ),
                ),
            ),
            country_alpha_3=lambda x: x.country_name_adj.apply(
                lambda y: countries.get(name=y).alpha_3
            ),
        )
        .drop(columns=["model", "country_name", "unit"])
        .merge(regions_countries, on=["country_alpha_3"], how="left")
        .pivot(
            index=[
                "scenario_version",
                "scenario",
                "region",
                "country_name_adj",
                "country_alpha_3",
                "year",
            ],
            columns="variable",
            values="value",
        )
        .groupby(["scenario_version", "scenario", "region", "year"])
        .agg(total_gdp=("GDP|PPP", "sum"), total_population=("Population", "sum"))
        .reset_index()
        .assign(gdp_ppp_per_capita=lambda x: x.total_gdp / x.total_population)
    )

    # If reference region is not in the list of regions, print error message
    reference_region = input_ref_region.upper()
    if reference_region not in df.region.unique():
        print("Please select a valid reference region: " + str(df.region.unique()))
    # If reference region is in the list of regions, calculate GDP ratios
    else:
        df = (
            df.pipe(
                lambda df_: pd.merge(
                    df_,
                    df_.loc[df_.region == reference_region][
                        ["scenario_version", "scenario", "year", "gdp_ppp_per_capita"]
                    ]
                    .rename(columns={"gdp_ppp_per_capita": "gdp_per_capita_reference"})
                    .reset_index(drop=1),
                    on=["scenario_version", "scenario", "year"],
                )
            )
            .assign(
                gdp_ratio_reg_to_reference=lambda x: x.gdp_ppp_per_capita
                / x.gdp_per_capita_reference,
            )
            .reindex(
                [
                    "scenario_version",
                    "scenario",
                    "region",
                    "year",
                    "gdp_ppp_per_capita",
                    "gdp_ratio_reg_to_reference",
                ],
                axis=1,
            )
        )

    return df


def linearly_regress_tech_cost_vs_gdp_ratios(
    gdp_df: pd.DataFrame,
    cost_ratios_df: pd.DataFrame,
    input_base_year: int,
) -> pd.DataFrame:
    """Compute linear regressions of technology cost ratios to GDP ratios

    Parameters
    ----------
    gdp_ratios_df : pandas.DataFrame
        Dataframe output from :func:`.process_raw_ssp_data`
    region_diff_df : str -> tuple of (str, str)
        Dataframe output from :func:`.get_weo_region_differentiated_costs`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - cost_type: either "fix_cost" or "Inv_cost"
        - scenario: SSP1, SSP2, or SSP3
        - weo_technology: WEO technology name
        - slope: slope of the linear regression
        - intercept: intercept of the linear regression
        - rvalue: rvalue of the linear regression
        - pvalue: pvalue of the linear regression
        - stderr: standard error of the linear regression
    """

    gdp_base_year = gdp_df.query("year == @input_base_year").reindex(
        ["scenario_version", "scenario", "region", "gdp_ratio_reg_to_reference"], axis=1
    )
    inv_cost_base_year = cost_ratios_df.reindex(
        ["message_technology", "region", "reg_cost_ratio"], axis=1
    )

    df_gdp_cost = (
        pd.merge(gdp_base_year, inv_cost_base_year, on=["region"])
        .groupby(["scenario_version", "scenario", "message_technology"])
        .apply(
            lambda x: pd.Series(
                linregress(x["gdp_ratio_reg_to_reference"], x["reg_cost_ratio"])
            )
        )
        .rename(
            columns={
                0: "slope",
                1: "intercept",
                2: "rvalue",
                3: "pvalue",
                4: "stderr",
                "scenario": "scenario",
            }
        )
        .reset_index()
    )

    return df_gdp_cost


# Function to calculate adjusted region-differentiated cost ratios
# using the results from the GDP linear regressions
def calculate_gdp_adjusted_region_cost_ratios(
    region_diff_df, input_node, input_ref_region, input_base_year
) -> pd.DataFrame:
    """Calculate adjusted region-differentiated cost ratios

    This function calculates the adjusted region-differentiated cost ratios \
        using the results from the GDP linear regressions. The adjusted \
        region-differentiated cost ratios are calculated by multiplying the \
        slope of the linear regression with the GDP ratio of the region \
        compared to the reference region and adding the intercept.

    Parameters
    ----------
    gdp_df : pandas.DataFrame
        Dataframe output from :func:`.get_gdp_data`
    linear_regression_df : pandas.DataFrame
        Dataframe output from :func:`.linearly_regress_tech_cost_vs_gdp_ratios`

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - scenario: SSP1, SSP2, or SSP3
        - weo_technology: WEO technology name
        - region: R11 region
        - cost_ratio_adj: the adjusted region-differentiated cost ratio
    """

    df_gdp = process_raw_ssp_data(
        input_node=input_node, input_ref_region=input_ref_region
    ).query("year >= 2020")
    df_cost_ratios = region_diff_df.copy()

    # If base year does not exist in GDP data, then use earliest year in GDP data
    # and give warning
    base_year = int(input_base_year)
    if int(base_year) not in df_gdp.year.unique():
        base_year = int(min(df_gdp.year.unique()))
        print(
            f"Base year {input_base_year} not found in GDP data. \
                Using {base_year} for GDP data instead."
        )

    # Set default values for input arguments
    # If specified node is R11, then use R11_NAM as the reference region
    # If specified node is R12, then use R12_NAM as the reference region
    # If specified node is R20, then use R20_NAM as the reference region
    # However, if a reference region is specified, then use that instead
    if input_ref_region is None:
        if input_node.upper() == "R11":
            reference_region = "R11_NAM"
        if input_node.upper() == "R12":
            reference_region = "R12_NAM"
        if input_node.upper() == "R20":
            reference_region = "R20_NAM"
    else:
        reference_region = input_ref_region

    # Linearly regress technology cost ratios to GDP ratios
    df_linear_reg = linearly_regress_tech_cost_vs_gdp_ratios(
        df_gdp, df_cost_ratios, input_base_year=base_year
    )

    if reference_region.upper() not in df_gdp.region.unique():
        print("Please select a valid reference region: " + str(df_gdp.region.unique()))
    else:
        df = (
            df_linear_reg.merge(df_gdp, on=["scenario_version", "scenario"])
            .drop(
                columns=[
                    "gdp_ppp_per_capita",
                    "rvalue",
                    "pvalue",
                    "stderr",
                ]
            )
            .assign(
                reg_cost_ratio_adj=lambda x: np.where(
                    x.region == reference_region,
                    1,
                    x.slope * x.gdp_ratio_reg_to_reference + x.intercept,
                ),
                year=lambda x: x.year.astype(int),
                scenario_version=lambda x: np.where(
                    x.scenario_version.str.contains("2013"),
                    "Previous (2013)",
                    "Review (2023)",
                ),
            )
            .reindex(
                [
                    "scenario_version",
                    "scenario",
                    "message_technology",
                    "region",
                    "year",
                    "reg_cost_ratio_adj",
                ],
                axis=1,
            )
        )

    return df


# Function to project investment costs by
# multiplying the learning NAM costs with the adjusted regionally
# differentiated cost ratios
# def project_adjusted_inv_costs(
#     nam_learning_df: pd.DataFrame,
#     adj_cost_ratios_df: pd.DataFrame,
#     use_gdp: bool = False,
# ) -> pd.DataFrame:
#     """Project investment costs using adjusted region-differentiated cost ratios

#     This function projects investment costs by \
#         multiplying the learning rates-projected NAM costs with the adjusted \
#             regionally differentiated cost ratios.

#     Parameters
#     ----------
#     nam_learning_df : pandas.DataFrame
#         Dataframe output from :func:`.project_NAM_capital_costs_using_learning_rates`
#     adj_cost_ratios_df : pandas.DataFrame
#         Dataframe output from :func:`.calculate_adjusted_region_cost_ratios`

#     Returns
#     -------
#     pandas.DataFrame
#         DataFrame with columns:
#         - scenario: SSP1, SSP2, or SSP3
#         - message_technology: MESSAGE technology name
#         - weo_technology: WEO technology name
#         - r11_region: R11 region
#         - year: values from 2020 to 2100
#         - inv_cost_learning_region: the adjusted investment cost \
#             (in units of million US$2005/yr) based on the NAM learned costs \
#             and the GDP adjusted region-differentiated cost ratios
#     """

#     df_learning_gdp_regions = (
#         nam_learning_df.merge(
#             adj_cost_ratios_df, on=["scenario", "weo_technology", "year"]
#         )
#         .assign(
#             inv_cost_learning_region=lambda x: x.inv_cost_learning_NAM
#             * x.cost_ratio_adj
#         )
#         .reindex(
#             [
#                 "scenario",
#                 "message_technology",
#                 "weo_technology",
#                 "r11_region",
#                 "year",
#                 "inv_cost_learning_region",
#             ],
#             axis=1,
#         )
#     )

#     return df_learning_gdp_regions
