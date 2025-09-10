# util file
import gc
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyam
import yaml
from pycountry_convert import country_name_to_country_alpha3

from message_ix_models.util import package_data_path, private_data_path

log = logging.getLogger(__name__)


def load_config_from_path(config_path: str = "default") -> dict:
    """
    Load configuration from the given path or from the default package data path.

    Parameters:
    ----------
    config_path : str, optional
        Path to the configuration file. If set to "default", uses the packaged
        `GDP_climate_shocks/config.yaml` file.

    Returns:
    -------
    dict
        Configuration data loaded from the YAML file.
    """
    if config_path == "default":
        config_file = package_data_path() / "GDP_climate_shocks" / "config.yaml"
    else:
        config_file = Path(config_path)
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def maybe_shift_year(scenario, shift_year: int) -> dict:
    """
    Return a dictionary with 'shift_first_model_year' if a year shift is requested.

    Parameters:
    ----------
    scenario :
        Scenario object containing the model's first model year attribute.
    shift_year : int
        Year to shift the first model year to, if different from the scenario's.

    Returns:
    -------
    dict
        Dictionary with 'shift_first_model_year' if needed, otherwise empty.
    """
    if shift_year and scenario.firstmodelyear != shift_year:
        return {"shift_first_model_year": shift_year}
    return {}


def load_pop_data():  # NOT NEEDED
    """
    This function loads a CSV file containing population data from SSP
    24 projections and transforms it into a specific format.

    Returns:
        pop_df (pd.DataFrame): The transformed DataFrame.
    """
    pop_file_path = private_data_path(
        "projects", "GDP_climate_shocks", "SSP2_population.csv"
    )
    # Load the CSV file into a DataFrame
    pop_df = pd.read_csv(pop_file_path)

    # Specify the columns to use as identifier variables (non-melted columns)
    id_vars = ["Model", "Scenario", "Region", "Variable", "Unit"]
    # Melt the DataFrame to convert it to long format
    pop_df = pd.melt(pop_df, id_vars=id_vars, var_name="Year", value_name="Value")
    pop_df["Value"] = pop_df["Value"] * 10**6
    pop_df["Unit"] = "people"
    pop_df = pop_df[["Region", "Scenario", "Year", "Value"]]
    pop_df.columns = ["iso", "ssp", "year", "Population"]

    return pop_df


def load_gdp_data():
    """
    This function loads a CSV file containing GDP data from OECD ENV-Growth
    2023_GDP_PPP and transforms it into a specific format.

    Returns:
        gdp_df (pd.DataFrame): The transformed DataFrame.
    """
    csv_file_path = package_data_path(
        "GDP_climate_shocks", "OECD ENV-Growth 2023_GDP_PPP.csv"
    )
    assert os.path.exists(csv_file_path), f"File {csv_file_path} does not exist"
    # use pyam to read
    gdp_df = pyam.IamDataFrame(csv_file_path).as_pandas()
    # Côte d'Ivoire
    # change Region == "Cote d'Ivoire" into "Côte d'Ivoire"
    gdp_df["region"] = gdp_df["region"].replace("Cote d'Ivoire", "Côte d'Ivoire")
    # convert coutry names in "Region" to ISO3 code
    gdp_df["iso3"] = gdp_df["region"].apply(
        lambda x: country_name_to_country_alpha3(x, cn_name_format="default")
    )
    return gdp_df


def calculate_gdp_diff(gdp_df, pop_df):  # NOT NEEDED
    # GDP From Burke, RCP 6.0 and 2.6
    csv_file_path = private_data_path(
        "projects", "GDP_climate_shocks", "burke_damages_message2.csv"
    )
    gdp_df = pd.read_csv(csv_file_path)
    # this is per ita, need to mltiply by population
    # TEMP exclude the iso == MNG entrie in gdp_df
    gdp_df = gdp_df[gdp_df["iso"] != "MNG"]

    # left join pop_df to gdp_df
    gdp_df["year"] = gdp_df["year"].astype(int)
    pop_df["year"] = pop_df["year"].astype(int)
    gdp_df = gdp_df.merge(pop_df, on=["iso", "year"], how="left")
    # multuply GDPNoCF and GDPCF by population and convert from $ to T$
    gdp_df["GDPNoCC"] = gdp_df["GDPcapNoCC"] * gdp_df["Population"] / 10**9
    gdp_df["GDPCC"] = gdp_df["GDPcapCC"] * gdp_df["Population"] / 10**9

    return gdp_df


def run_legacy_reporting(sc=False, mp=False):
    # lazy import to allow tests pass
    from message_data.tools.post_processing.iamc_report_hackathon import (
        report as legacy_reporting,
    )

    legacy_reporting(
        mp=mp,
        scen=sc,
        merge_hist=True,
        merge_ts=False,
        run_config="GDP_shock_run_config.yaml",
    )


def run_emi_reporting(sc=False, mp=False):
    # lazy import to allow tests pass
    from message_data.tools.post_processing.iamc_report_hackathon import (
        report as legacy_reporting,
    )

    legacy_reporting(
        mp=mp,
        scen=sc,
        merge_hist=False,
        merge_ts=False,
        run_config="GDP_shock_emiss_run_config.yaml",
    )


## SECTION 3: APPLY CHANGES
# introduce shock in growth rates
# FUTURE loop with scenarios
def run_message_full(sc_ref, modelName, scenario, n_iter, run_mode):  # NOT USED
    """Initiate the scenario running the model, the reporting,
    prep submission and MAGICC.

    Parameters
    ----------
    sc_ref : message_ix.Scenario
        The reference scenario.
    modelName : str
        The name of the model.
    scenario : str
        The name of the scenario.
    n_iter : int
        The number of iterations.
    run_mode : str
        The run mode.

    Returns
    -------
    scs : message_ix.Scenario
    """
    scs = sc_ref.clone(
        modelName, scenario + "_GDP_CI_" + str(n_iter), keep_solution=False
    )
    scs.solve(solve_options={"lpmethod": "4"}, model=run_mode)
    scs.set_as_default()
    log.info("Model solved, running reporting")
    run_legacy_reporting(scs)
    log.info("Reporting completed, ready to run MAGICC")
    return scs


# def prep_sub_GDP_shock():  # NOT USED, leave if needed in the future
# lazy import to allow tests pass
#     from message_data.tools.prep_submission import main as prep_submission
#     # only at the end, when all scenarios are finished
#     prep_submission(
#         config_fil,
#         context.local_data / "reporting_output",
#         out_fil,
#         add_economic_indicator=True,
#     )


def regional_gdp_impacts(sc_string, damage_model, it, SSP, regions, pp=50):
    """
    This function calculates the regional GDP impacts of climate
    change using the RIME model.

    Parameters
    ----------
    sc_string : str
        The name of the scenario.
    damage_model : str
        The name of the damage model to use.
    it : int
        The iteration number.
    SSP : str
        The Shared Socioeconomic Pathway (SSP) to use.
    regions : list
        A list of region names.
    pp : int
        The percentile to use.

    Returns
    -------
    agg_gdp_pc_df : pd.DataFrame
        The aggregated regional GDP impacts of climate change.
    """
    # previous iteration it-1 as string
    pit = str(it - 1)
    # iteration as string
    it = str(it)
    # Step 1: load GDP SSP data, unit USD 2017
    gdp_df = load_gdp_data()
    gdp_df = gdp_df[gdp_df["scenario"] == SSP]
    gdp_df.drop(["region", "model", "scenario"], axis=1, inplace=True)

    # load RIME climate GDP impacts from csv
    # and filter variable==RIME|All indicators|mean
    rime_path = private_data_path().parent / "reporting_output" / "rime_output"
    # sc_string = f"{sc_string}_{damage_model}" if pit != "0" else sc_string
    rime_file = f"RIME_out_COUNTRIES_5yr_{sc_string}_{pp}_{damage_model}_{pit}.csv"
    # rename Region to iso3
    gdp_shock = pyam.IamDataFrame(rime_path / rime_file).as_pandas()

    # TEMP better to harmonizy the output variable name in the RIME scripts
    if damage_model == "Waidelich":
        gdp_shock = gdp_shock[gdp_shock["variable"] == "RIME|All indicators|mean"]
    #  next if damage_model == Burke or Kotz
    elif damage_model == "Burke" or damage_model == "Kotz":
        gdp_shock = gdp_shock[gdp_shock["variable"] == "RIME|pct.diff"]
    else:
        raise AssertionError(
            "Invalid damage model. Please define a suitable damage function."
        )

    gdp_shock.drop(["variable", "unit"], axis=1, inplace=True)
    # rename ["region"] to ["iso3"]
    gdp_shock.columns = ["model", "scenario", "iso3", "year", "perc_change"]
    # path will change with scenario
    gdp_region_df = gdp_df.copy()
    gdp_region_df.columns = ["variable", "unit", "year", "GDPNoCC", "iso3"]

    # Step 2: Load the YAML file
    yaml_file_path = package_data_path("node", f"{regions}.yaml")
    with open(yaml_file_path, "r", encoding="utf-8") as file:
        yaml_data = yaml.safe_load(file)

    # Step 3: Create a mapping of Region to iso3
    region_iso_mapping = {}
    for region, region_data in yaml_data.items():
        if "child" in region_data:
            for iso3 in region_data["child"]:
                region_iso_mapping[iso3] = region

    # Step 4: aggregate GDP at the Region level
    # mind that Burke database has no values for 2110
    gdp_region_df["region"] = gdp_region_df["iso3"].map(region_iso_mapping)
    # left join gdp_region_df with gdp_shock
    gdp_region_df = gdp_region_df.merge(gdp_shock, on=["iso3", "year"], how="left")

    gdp_region_df["weight"] = gdp_region_df["GDPNoCC"] * gdp_region_df["perc_change"]
    gdp_region_df["GDPNoCC_sum"] = gdp_region_df.groupby(["region", "year"])[
        "GDPNoCC"
    ].transform("sum")
    gdp_region_df["weight_sum"] = gdp_region_df.groupby(["region", "year"])[
        "weight"
    ].transform("sum")
    gdp_region_df["perc_change_sum"] = (
        gdp_region_df["weight_sum"] / gdp_region_df["GDPNoCC_sum"]
    )
    # only keep the unique values or perc_change_sum for region, year
    agg_gdp_pc_df = (
        gdp_region_df[["region", "year", "perc_change_sum"]].drop_duplicates().dropna()
    )
    agg_gdp_pc_df.columns = ["node", "year", "perc_change_sum"]
    del gdp_region_df
    gc.collect()
    return agg_gdp_pc_df


def apply_growth_rates(sc, gdp_change_df):
    """
    This function calculates the growth rates of the GDP.

    Parameters
    ----------
    sc : message_ix.Scenario
        The scenario to which the growth rates will be applied.
    gdp_change_df : pd.DataFrame
        The DataFrame containing the GDP data.

    Returns
    -------
    growth_df : pd.DataFrame
        The DataFrame containing the growth rates of the GDP.
    """
    # get gdp_calibrate from the scenario
    # take value in 2020 and add it to agg_gdp_pc_df
    # from now the GDP is the MESSAGE GDP
    gdp_calibrate = sc.par("gdp_calibrate").copy()
    # keep years 2015 and 2020
    # gdp_calibrate = gdp_calibrate[gdp_calibrate['year'].isin([2015, 2020])]
    gdp_calibrate = gdp_calibrate[["node", "year", "value"]]
    # gdp_calibrate['GDPCC'] = gdp_calibrate['value']
    gdp_calibrate.columns = ["node", "year", "GDPNoCC"]

    # merge agg_gdp_pc_df into gdp_calibrate
    gdp_impact = gdp_calibrate.merge(gdp_change_df, on=["node", "year"], how="left")
    gdp_impact["GDPCC"] = gdp_impact["GDPNoCC"] * (
        1 + gdp_impact["perc_change_sum"] / 100
    )
    # if gdp_impact['GDPCC'] is nan, gdp_impact['GDPCC'] = gdp_impact['GDPNoCC']
    gdp_impact["GDPCC"] = np.where(
        gdp_impact["GDPCC"].isnull(), gdp_impact["GDPNoCC"], gdp_impact["GDPCC"]
    )
    # Step 5: calculate the growth rate
    # calculate growth rates between time periods in add_gdp_reg_df
    gdp_impact["GDPCC_lag"] = gdp_impact.groupby(["node"])["GDPCC"].shift(1)
    gdp_impact["year_lag"] = gdp_impact.groupby(["node"])["year"].shift(1)
    gdp_impact["growth_CC"] = (gdp_impact["GDPCC"] / gdp_impact["GDPCC_lag"]) ** (
        1 / (gdp_impact["year"] - gdp_impact["year_lag"])
    ) - 1

    gdp_impact["GDPNoCC_lag"] = gdp_impact.groupby(["node"])["GDPNoCC"].shift(1)
    gdp_impact["growth_NoCC"] = (gdp_impact["GDPNoCC"] / gdp_impact["GDPNoCC_lag"]) ** (
        1 / (gdp_impact["year"] - gdp_impact["year_lag"])
    ) - 1
    gdp_impact = gdp_impact.drop(["GDPCC_lag"], axis=1)
    gdp_impact = gdp_impact.drop(["GDPNoCC_lag"], axis=1)
    gdp_impact = gdp_impact.drop(["year_lag"], axis=1)

    gdp_impact["growth_diff"] = gdp_impact["growth_CC"] - gdp_impact["growth_NoCC"]
    growth_diff_df = gdp_impact[["node", "year", "growth_diff"]]
    growth_diff_df.columns = ["node", "year", "growth_diff"]
    # Get the values for year 2100
    # Get the 'growth_diff' values for year 2100
    values_2100 = growth_diff_df.loc[growth_diff_df["year"] == 2100, "growth_diff"]

    # Assign these values to year 2110
    growth_diff_df.loc[growth_diff_df["year"] == 2110, "growth_diff"] = (
        values_2100.values
    )
    # get the grow parameter from the scenarios
    grow_par = sc.par("grow").copy()
    merge_grow = grow_par.merge(growth_diff_df, on=["node", "year"], how="left")
    merge_grow["value"] = merge_grow["value"] + merge_grow["growth_diff"]
    merge_grow = merge_grow[merge_grow["year"] >= 2025]
    merge_grow = merge_grow.drop(["growth_diff"], axis=1)
    # add the paramenter back to the scenario
    sc.check_out()
    sc.add_par("grow", merge_grow)
    # commit the changes
    sc.commit("growth rate shock")
    logging.info(
        f"Growth rate shock applied to {sc.model} {sc.scenario}. Ready to solve."
    )
    del (
        gdp_calibrate,
        gdp_change_df,
        gdp_impact,
        growth_diff_df,
        values_2100,
        merge_grow,
    )
    gc.collect()
    return


def add_slack_ix(sc):
    # lazy import to allow tests pass
    from message_data.tools.utilities import add_slack

    slack = {
        "growth_activity_up": {
            "a": ["R11_NAM", "eth_imp", 2030, 27, "M1"],
            "b": ["R11_NAM", "g_ppl_co2scr", 2030, 26, "M1"],
            "c": ["R11_NAM", "bco2_tr_dis", 2030, 12, "M1"],
            "d": ["R11_SAS", "eth_ic_trp", 2030, 7, "M1"],
            "e": ["R11_NAM", "coal_adv_ccs", 2030, 6, "M1"],
            "f": ["R11_NAM", "cement_co2scr", 2030, 5, "M1"],
            "g": ["R11_NAM", "leak_repair", 2030, 1, "M1"],
        },
        "bound_activity_up": {
            "a": ["R11_NAM", "RCspec_5", 2030, 3.5, "M1"],
            "b": ["R11_NAM", "recycling_gas1", 2030, 0.5, "M1"],
        },
    }

    add_slack(sc, slack)
