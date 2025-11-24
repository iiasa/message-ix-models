import os
from pathlib import Path

import click
import message_ix
import numpy as np
import pandas as pd
from message_ix_models.project.ssp.transport import METHOD, process_df

from message_data.tools.utilities.utilities import retrieve_region_mapping

# file_path = Path(r"C:/Repo/message_data/data/projects/ssp_dev/scenario_submission")

ceds_region_mapping = {
    "MESSAGEix-GLOBIOM 2.0-R12|Sub-Saharan Africa": "AFR",
    "MESSAGEix-GLOBIOM 2.0-R12|Rest of Centrally Planned Asia": "RCPA",
    "MESSAGEix-GLOBIOM 2.0-R12|China": "CHN",
    "MESSAGEix-GLOBIOM 2.0-R12|Eastern Europe": "EEU",
    "MESSAGEix-GLOBIOM 2.0-R12|Former Soviet Union": "FSU",
    "MESSAGEix-GLOBIOM 2.0-R12|Latin America and the Caribbean": "LAM",
    "MESSAGEix-GLOBIOM 2.0-R12|Middle East and North Africa": "MEA",
    "MESSAGEix-GLOBIOM 2.0-R12|North America": "NAM",
    "MESSAGEix-GLOBIOM 2.0-R12|Pacific OECD": "PAO",
    "MESSAGEix-GLOBIOM 2.0-R12|Other Pacific Asia": "PAS",
    "MESSAGEix-GLOBIOM 2.0-R12|South Asia": "SAS",
    "MESSAGEix-GLOBIOM 2.0-R12|Western Europe": "WEU",
}


def read_data(config, source_dir, load_from_database, mp=None):
    """Reads in data

    Parameters
    ----------
    config : :class:`pandas.DataFrame`
        configuration of which data should be read in
    source_dir : pathlib.Path()
        path from where data (reporting output files)
        should be read in
    """

    if mp is None:
        import ixmp

        mp = ixmp.Platform()

    dfs = []
    if load_from_database is True:
        print("    Reading data from scenario object")
    for model, scenario in zip(config.source_model_name, config.source_scenario_name):
        print(f"    Reading data for model {model} and scenario {scenario}")
        if load_from_database is True:
            # Load scenario object
            scen = message_ix.Scenario(mp, model, scenario)
            # Load timeseries
            tmp = scen.timeseries(iamc=True)
            # Rename columns
            tmp = tmp.rename(
                columns={
                    "model": "Model",
                    "scenario": "Scenario",
                    "region": "Region",
                    "variable": "Variable",
                    "unit": "Unit",
                }
            )
            region_id, reg_ts = retrieve_region_mapping(
                scen, mp, include_region_id=False
            )
            tmp.Region = tmp.Region.replace(reg_ts)

            for var in ["GLOBIOM Feedback|Price|Carbon", "Price|Carbon"]:
                if len(tmp.loc[tmp.Variable == var].Region) > len(reg_ts.keys()):
                    # Filter out variables with double entries
                    cprice_update = tmp.loc[tmp.Variable == var]

                    # Remove variables from dataframe
                    tmp = tmp.loc[tmp.Variable != var]

                    # groupby.sum to remove NaNs
                    cprice_update = (
                        cprice_update.groupby(
                            ["Model", "Scenario", "Region", "Variable", "Unit"]
                        )
                        .sum()
                        .reset_index()
                    )

                    tmp = pd.concat([tmp, cprice_update], ignore_index=True)

        #            # QUICK-FIX to correct for NDC 2030 incorrect carbon prices in 2030
        #            # These will be retrieved from the "forever_variant"
        #            # The scenario name is filtered out and the corresponding values are added
        #            # from the 2035 year (which is constant) and replaces the 2030 value
        #            reference_scenario = {
        #                "SSP2 - Very Low Emissions": "INDC2030i_uncon_forever",
        #                "SSP1 - Low Emissions": "INDC2030i_uncon_forever",
        #                "SSP1 - Low Emissions_a": "INDC2030i_uncon_forever",
        #                "SSP1 - Very Low Emissions": "INDC2030i_forever",
        #                "SSP2 - Low Overshoot": "NPiREF",
        #                "SSP2 - Medium Emissions": "NPiREF",
        #                "SSP2 - Medium Emissions_a": "NPiREF",
        #                "SSP2 - Medium-Low Emissions": "NPiREF",
        #                "SSP2 - Low Emissions_a": "INDC2030i_weak_forever",
        #                "SSP4 - Low Overshoot": "NPiREF",
        #                "SSP5 - Low Overshoot": "NPiREF",
        #            }
        #
        #            if scenario in reference_scenario:
        #                ref_scen = message_ix.Scenario(mp, model, reference_scenario[scenario])
        #                df_upd = ref_scen.timeseries(
        #                    iamc=True,
        #                    variable=["GLOBIOM Feedback|Price|Carbon", "Price|Carbon"],
        #                    year=2035,
        #                )
        #                df_upd = df_upd.rename(
        #                    columns={
        #                        "model": "Model",
        #                        "scenario": "Scenario",
        #                        "region": "Region",
        #                        "variable": "Variable",
        #                        "unit": "Unit",
        #                    }
        #                )
        #                df_upd.Scenario = scenario
        #                df_upd = df_upd.rename(columns={2035: 2030})
        #
        #                region_id, reg_ts = retrieve_region_mapping(
        #                    ref_scen, mp, include_region_id=False
        #                )
        #                df_upd.Region = df_upd.Region.replace(reg_ts)
        #
        #                # Filter out incorerectly added prices
        #                tmp = tmp.loc[
        #                    ~(
        #                        (
        #                            tmp.Variable.isin(
        #                                ["GLOBIOM Feedback|Price|Carbon", "Price|Carbon"]
        #                            )
        #                        )
        #                        & (tmp[2035].isnull())
        #                    )
        #                ]
        #
        #                for var in ["GLOBIOM Feedback|Price|Carbon", "Price|Carbon"]:
        #                    new_price = tmp.loc[tmp.Variable == var]
        #                    new_price = new_price.set_index(
        #                        ["Model", "Scenario", "Region", "Variable", "Unit"]
        #                    )
        #                    new_price[2030] = df_upd.set_index(
        #                        ["Model", "Scenario", "Region", "Variable", "Unit"]
        #                    )[2030]
        #
        #                    tmp = tmp.set_index(
        #                        ["Model", "Scenario", "Region", "Variable", "Unit"]
        #                    )
        #                    tmp = new_price.combine_first(tmp)
        #                    tmp = tmp.reset_index()

        else:
            tmp = pd.read_excel(source_dir.joinpath(f"{model}_{scenario}.xlsx"))
        target_model_name, target_scenario_name = config[
            (config["source_model_name"] == model)
            & (config["source_scenario_name"] == scenario)
        ][["target_model_name", "target_scenario_name"]].values[0]

        if target_model_name is not np.nan:
            tmp["Model"] = target_model_name
        if target_scenario_name is not np.nan:
            tmp["Scenario"] = target_scenario_name

        dfs.append(tmp)

    return pd.concat(dfs)


def add_economic_indicators(data, config):
    """Calculates GDP and Consumption losses.
    GDP and Consumption losses are calculated for scenarios,
    for which a reference scenario is provided.

    Parameters
    ----------
    data : :class:`pandas.DataFrame`
        reported data for scenarios
    config : :class:`pandas.DataFrame`
        configuration input specifying which reference scenario
        is used to calcualte losses
    """
    print("    Adding economic indicators")

    for i in config.index:
        row = config.iloc[i]
        if pd.isnull(row.reference_scenario):
            continue
        for var in ["GDP|MER", "Consumption"]:
            ref = (
                data[
                    (data.Model == row["target_model_name"])
                    & (data.Scenario == row["reference_scenario"])
                    & (data.Variable == var)
                ]
                .drop(["Scenario", "Variable"], axis=1)
                .set_index(["Model", "Region", "Unit"])
            )

            scen = (
                data[
                    (data.Model == row["target_model_name"])
                    & (data.Scenario == row["target_scenario_name"])
                    & (data.Variable == var)
                ]
                .drop(["Scenario", "Variable"], axis=1)
                .set_index(["Model", "Region", "Unit"])
            )

            losses = ref.subtract(scen).reset_index()
            if var == "GDP|MER":
                losses["Variable"] = "Policy Cost|GDP Loss"
            elif var == "Consumption":
                losses["Variable"] = "Policy Cost|Consumption Loss"
            losses["Scenario"] = row["target_scenario_name"]
            data = pd.concat([data, losses], sort=True)
    data = data.reset_index().drop("index", axis=1)
    return data


def interpolate_emissions(data):
    """Writes the final scenario data into a single xlsx file.
    Each scenario is stored in a separate sheet.

    Parameters
    ----------
    data : :class:`pandas.DataFrame`
        reported data for scenarios
    """
    print("    Interpolating emissions from 2020 to 2025")

    add_years = [2021, 2022, 2023, 2024]

    variables = [v for v in data.Variable if v.startswith("Emission")]
    tmp_other = data.loc[~data.Variable.isin(variables)].set_index(static_index).copy()
    tmp = data.loc[data.Variable.isin(variables)].set_index(static_index).copy()
    for y in add_years:
        tmp[y] = np.nan
    tmp = tmp[sorted(tmp.columns)].interpolate(axis=1, method="index")

    data = pd.concat([tmp, tmp_other]).reset_index().sort_values(by=static_index)
    return data


def add_gross_removals(data):

    # Filter out relevant CO2-Emissions
    co2 = data.loc[data.Variable.isin(["Emissions|CO2", "Emissions|CO2|AFOLU"])].copy()
    # Filter out relevant Gross CO2 Emissions
    co2_gross = data.loc[
        data.Variable.isin(["Gross Emissions|CO2", "Gross Emissions|CO2|AFOLU"])
    ].copy()
    # Ensure common vriable names
    co2_gross.Variable = co2_gross.Variable.str.replace("Gross ", "")
    # Subtract gross from net
    co2_gross_removal = (
        (co2.set_index(static_index))
        .subtract(co2_gross.set_index(static_index))
        .reset_index()
    )

    # Rename variables
    co2_gross_removal.Variable = co2_gross_removal.Variable.str.replace(
        "Emissions", "Gross Removals"
    )
    # Merge dataframes
    data = (
        co2_gross_removal.set_index(static_index)
        .combine_first(data.set_index(static_index))
        .reset_index()
        .sort_values(by=static_index)
    )
    return data


def recalculate_kyoto_gases(data):

    kyoto_vars = {
        "Emissions|CO2": 1.0,  # the number indicates the conversion factor to Mt CO2e
        "Emissions|F-Gases": 1.0,
        "Emissions|CH4": 25.0,
        "Emissions|N2O": 298.0 / 1000.0,
    }

    # Filter out variables and add to temporary dataframe
    df = None

    for var in kyoto_vars:
        tmp = data.loc[data.Variable == var].copy()
        tmp.Variable = "Emissions|Kyoto Gases"
        tmp.Unit = "Mt CO2-equiv/yr"
        tmp = tmp.set_index(static_index)
        tmp *= kyoto_vars[var]
        if df is None:
            df = tmp
        else:
            df = df.add(tmp, fill_value=0)

    data = df.combine_first(data.set_index(static_index)).reset_index()

    return data


def merge_xlsx(data, config, out_fil):
    """Writes the final scenario data into a single xlsx file.
    Each scenario is stored in a separate sheet.

    Parameters
    ----------
    data : :class:`pandas.DataFrame`
        reported data for scenarios
    config : :class:`pandas.DataFrame`
        configuration input specifying which reference scenario
        is used to calcualte losses
    out_fil : str
        path and name of the final xlsx file
    """
    print("    Merging files")

    # Creates output file
    writer = pd.ExcelWriter(out_fil, engine="xlsxwriter")

    for model, scenario in zip(config.target_model_name, config.target_scenario_name):

        if (
            config[
                (config["target_model_name"] == model)
                & (config["target_scenario_name"] == scenario)
            ]["final_scenario"].values[0]
            == True
        ):
            df = data[(data.Model == model) & (data.Scenario == scenario)]

            s = config[
                (config["target_model_name"] == model)
                & (config["target_scenario_name"] == scenario)
            ]["sheet_name"].values[0]

            if not pd.isnull(s):
                sheet_name = "data_{}".format(s)
            else:
                sheet_name = "data_{}".format(scenario)

            if "index" in df.columns:
                df = df.drop("index", axis=1)

            # Remove 2110 values
            df = df[[c for c in df.columns if c != 2110]]

            # Remove pre-2020 emissions data
            # Retrieve emission varaibles
            emis_var = [v for v in df.Variable.unique() if v.startswith("Emissions|")]
            df.loc[df.Variable.isin(emis_var), [1990, 1995, 2000, 2005, 2010, 2015]] = 0

            # Derive additional land-use variables
            # "Emissions|CO2|AFOLU|Land" = "Emissions|CO2|AFOLU|Land|Land Use and Land-Use Change" = "Emissions|CO2|AFOLU" - "Emissions|CO2|AFOLU|Agriculture"

            co2_afolu = (
                df.loc[df.Variable == "Emissions|CO2|AFOLU"]
                .copy()
                .drop("Variable", axis=1)
                .set_index(["Model", "Scenario", "Region", "Unit"])
            )
            co2_agri = (
                df.loc[df.Variable == "Emissions|CO2|AFOLU|Agriculture"]
                .copy()
                .drop("Variable", axis=1)
                .set_index(["Model", "Scenario", "Region", "Unit"])
            )
            co2_land = co2_afolu - co2_agri
            co2_land = co2_land.reset_index().assign(
                Variable="Emissions|CO2|AFOLU|Land"
            )
            df = pd.concat([df, co2_land], ignore_index=True)
            co2_land["Variable"] = (
                "Emissions|CO2|AFOLU|Land|Land Use and Land-Use Change"
            )
            df = pd.concat([df, co2_land], ignore_index=True)

            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Add sheet meta to configure if MAGICC should be run
    magicc = config.loc[config.run_magicc == True]
    if not magicc.empty:
        meta = config.loc[config.run_magicc == True][
            ["target_model_name", "target_scenario_name", "run_magicc"]
        ]
        meta = meta.rename(
            columns={"target_model_name": "model", "target_scenario_name": "scenario"}
        )
        meta.to_excel(writer, sheet_name="meta", index=False)

    writer.close()


def merge_indicators_trp(data, config):
    print("    Merging Energy Service Indicators for TRP-sector")
    for i in config.index:
        row = config.iloc[i]
        if pd.isnull(row.merge_indicator_trp):
            continue
        df = (
            pd.read_csv(file_path / row.merge_indicator_trp)
            .drop("Model", axis=1)
            .rename(columns={"Scenario": "SSP"})
        )
        df.Region = df.Region.str.replace("R12_", "")
        df = df.rename(
            columns={
                c: c.replace("outcome_", "") for c in df.columns if "outcome_" in c
            }
        )
        df.SSP = df.SSP.apply(
            lambda x: x.replace(" baseline", "").replace("_2024.", "")
        )
        df = df.set_index(["Region", "Variable", "Unit", "SSP"])
        df = df.rename(columns={c: int(c) for c in df.columns}).reset_index()

        # Get ssp from model name
        ssp = row.source_model_name.split("_")[1]
        ssp = "LED-SSP2" if ssp == "LED" else ssp

        tmp = (
            df.loc[df.SSP == ssp]
            .assign(Scenario=row.target_scenario_name, Model=row.target_model_name)
            .drop("SSP", axis=1)
        )

        data = pd.concat([data, tmp], ignore_index=True)

    return data


def merge_indicators_rc(data, config):
    print("    Merging Energy Service Indicators for RC-sector")
    for i in config.index:
        row = config.iloc[i]
        if pd.isnull(row.merge_indicator_rc):
            continue
        df = (
            pd.read_csv(file_path / row.merge_indicator_rc)
            .drop("Model", axis=1)
            .rename(columns={"Scenario": "SSP"})
        )
        df.Region = df.Region.str.replace("R12_", "")
        df.SSP = df.SSP.str.replace("_nopol", "")
        df = df.set_index(["Region", "Variable", "Unit", "SSP"])
        df = df.rename(columns={c: int(c) for c in df.columns}).reset_index()

        # Get ssp from model name
        ssp = row.source_model_name.split("_")[1]
        ssp = "SSP2_LED" if ssp == "LED" else ssp

        tmp = (
            df.loc[df.SSP == ssp]
            .assign(Scenario=row.target_scenario_name, Model=row.target_model_name)
            .drop("SSP", axis=1)
        )

        data = pd.concat([data, tmp], ignore_index=True)

    return data


def read_aviation_data():
    # Read in aviation data from CEDS is merged for historical timeperiods.
    df = (
        pd.read_csv(
            file_path / "ceds_cmip7_Aircraft_intlShipping_byfuel_v_2025_03_18.csv"
        )
        .drop(["model", "scenario"], axis=1)
        .rename(columns={"variable": "Variable", "unit": "Unit", "region": "Region"})
    )
    df = df.set_index(["Region", "Variable", "Unit"])
    df = df.rename(columns={c: int(c) for c in df.columns}).reset_index()
    return df


def read_co2_waste_data():
    # Read in aviation data from CEDS is merged for historical timeperiods.
    df = (
        pd.read_csv(file_path / "ceds_cmip7_MESSAGE_CO2_Waste_v_2025_03_18.csv")
        .drop(["model", "scenario"], axis=1)
        .rename(columns={"variable": "Variable", "unit": "Unit", "region": "Region"})
    )
    df.Region = df.Region.replace(ceds_region_mapping)
    df = df.set_index(["Region", "Variable", "Unit"])
    df = df.rename(columns={c: int(c) for c in df.columns}).reset_index()
    return df


def read_co_so2_data(add_co_so2_sheets):
    df = pd.DataFrame()
    for sheet in add_co_so2_sheets:
        tmp = pd.read_excel(
            file_path / "Fossil_fuel_production_ADD_emi.xlsx",
            sheet_name=sheet,
            skiprows=2,
        )
        tmp = tmp.rename(columns={"Unnamed: 0": "Region"}).assign(Variable=sheet)
        tmp.Region = tmp.Region.replace("Global", "World").str.replace("R12_", "")
        df = pd.concat([df, tmp], ignore_index=True)
    return df


def merge_cle_mtfr(df, row):
    df_cle = (
        pd.read_excel(file_path / row.merge_pollutant.replace("MTFR", "CLE"))
        .drop(["Report_Time", "SCENARIO"], axis=1)
        .rename(columns={"REGION": "Region", "VARIABLE": "Variable", "UNIT": "Unit"})
        .set_index(["Region", "Variable", "Unit"])
    )

    # Remove all values for the MTFR until 2025
    df = df.set_index(["Region", "Variable", "Unit"])

    # Step1. For the MTFR, all historic values are removed
    df = df[[c for c in df.columns if c > 2025]]

    if row["SSP"] in ["SSP2", "SSP3", "SSP4"]:
        factors = {
            2030: 0.0,
            2035: 0.054,
            2040: 0.107,
            2045: 0.161,
            2050: 0.214,
            2055: 0.268,
            2060: 0.321,
            2065: 0.375,
            2070: 0.429,
            2080: 0.536,
            2090: 0.643,
            2100: 0.75,
            2110: 0.75,
        }
    else:
        factors = {
            2030: 0.0,
            2035: 0.188,
            2040: 0.375,
            2045: 0.563,
            2050: 0.75,
            2055: 0.781,
            2060: 0.813,
            2065: 0.844,
            2070: 1,
            2080: 1,
            2090: 1,
            2100: 1,
            2110: 1,
        }

    # Iterate over MTFR values and multiply by factor
    # -> all values prior to 2025 stay fixed
    # -> all values > factor-years remain as are
    for y in [y for y in df.columns.tolist() if y > 2025]:
        if y in factors:
            df[y] *= factors[y]

    # Iterate over CLE and multiply my inverse of factors;
    # -> all values prior to 2025 stay fixed
    # -> all values > factor-years are set to 0, as it is assumed MTFR is achieved
    for y in [y for y in df_cle.columns.tolist() if y > 2025]:
        if y in factors:
            df_cle[y] *= 1 - factors[y]
        else:
            df_cle[y] = 0

    df = df.add(df_cle, fill_value=0).reset_index()

    return df


def merge_co_so2_data(row, df, data, add_co_so2, add_co_so2_sheets):
    print("Adding additional emissions for SO2 and CO to GAINS Energy sector")

    for sec in add_co_so2_sheets:
        # Filter out data to be added
        tmp_co_co2 = (
            add_co_so2.loc[add_co_so2.Variable == sec]
            .copy()
            .drop("Variable", axis=1)
            .set_index(["Region"])
        )
        # Determine max year of data provide
        year = max(tmp_co_co2.columns)

        # Filter out driver data
        driver = add_co_so2_sheets[sec][1]
        driver_data = data.loc[
            (data.Model == row.target_model_name)
            & (data.Scenario == row.target_scenario_name)
            & (data.Variable == driver)
        ].copy()
        # Determine the emission factor by dividing the total emissions by total actvity of the last year for which data is provided
        emif = tmp_co_co2[[year]]

        emif[year] /= driver_data.set_index("Region")[year]
        driver_data = driver_data.set_index(static_index)

        for y in driver_data.columns:
            if y in tmp_co_co2.columns:
                driver_data[y] = 0
                driver_data[y] += tmp_co_co2[y]
            else:
                driver_data[y] *= emif[year]

        driver_data = (
            driver_data.reset_index()
            .drop(["Model", "Scenario", "Variable", "Unit"], axis=1)
            .set_index("Region")
        )

        variable = f"Emissions|{add_co_so2_sheets[sec][0]}|Energy|Supply"

        df.loc(axis=0)[:, variable] += driver_data

    return df.reset_index()


def read_prep_pollution_data(row):
    # Read data from pollution input file
    df = (
        pd.read_excel(file_path / row.merge_pollutant)
        .drop(["Report_Time", "SCENARIO"], axis=1)
        .rename(columns={"REGION": "Region", "VARIABLE": "Variable", "UNIT": "Unit"})
    )

    # Because the MTFR historic values are obsolete, the CLE values are applied:
    # CLE <= 2025 ; MTFR > 2025
    if "MTFR" in row.merge_pollutant:
        df = merge_cle_mtfr(df, row)

    # Rename Variables
    df.Variable = df.Variable.str.replace("SO2", "Sulfur")
    df.Variable = df.Variable.str.replace("NOX", "NOx")
    df.Variable = df.Variable.str.replace("PM_BC", "BC")
    df.Variable = df.Variable.str.replace("PM_OC", "OC")
    df.Variable = df.Variable.str.replace(
        "Buildings", "Residential and Commercial and AFOFI"
    )

    # Rename Regions
    df.Region = df.Region.str.replace("R12_", "")
    df = df.set_index(["Region", "Variable", "Unit"])

    return df


def merge_pollutants(data, config):
    print("    Merging Pollution from GAINS")

    # Step 1. Retrieve data used for updating aviation related emissions based on CEDS
    aviation = read_aviation_data()

    # Step 2. Retrieve data used for updating CO2-Waste emissions
    co2_waste_data_raw = read_co2_waste_data()

    # Step 3. Retrieve data used for adding additional SO2 and CO emissions.
    add_co_so2_sheets = {
        "Coal_mining_CO": ["CO", "Resource|Extraction|Coal"],
        "Coal_mining_VOC": ["VOC", "Resource|Extraction|Coal"],
        "Oil_SO2": ["Sulfur", "Resource|Extraction|Oil|Conventional"],
        "Gas_SO2": ["Sulfur", "Resource|Extraction|Gas"],
    }
    add_co_so2 = read_co_so2_data(add_co_so2_sheets)

    # Step 4. Merge pollution data for each scenario from GAINS
    for i in config.index:
        row = config.iloc[i]
        if pd.isnull(row.merge_pollutant):
            continue

        # Read and prepare pollution data
        df = read_prep_pollution_data(row)

        # Add SO2 and CO data provided by Zig
        # kt CO from coal-mining (1B1_Fugitive-solid-fuels) driven by "Resource|Extraction|Coal"
        # kt SO2 from oil-exploration (1B2_Fugitive-petr) driven by "Resource|Extraction|Oil|Conventional"
        # kt SO2 from gas-exploration (1B2b_Fugitive-NG-prod) driven by "Resource|Extraction|Gas"
        df = merge_co_so2_data(row, df, data, add_co_so2, add_co_so2_sheets)

        # Identify Species to handle
        species = set([x.split("|")[1] for x in df.Variable.unique()]) | set(
            ["CO2", "CH4", "N2O"]
        )

        for s in species:
            if s in ["PM_2_5"]:
                continue
            remove_variables = [
                v
                for v in data.Variable.unique()
                if f"Emissions|{s}|" in v or v == f"Emissions|{s}"
            ]

            # Retrieve all data of the new dataframe from the main data
            tmp = data.loc[
                (data.Model == row.target_model_name)
                & (data.Scenario == row.target_scenario_name)
                & (data.Variable.isin(remove_variables))
            ].copy()

            # Remove all data of the new species from the main data
            data = data.loc[
                ~(
                    (data.Model == row.target_model_name)
                    & (data.Scenario == row.target_scenario_name)
                    & (data.Variable.isin(remove_variables))
                )
            ]

            if s not in ["CO2", "CH4", "N2O"]:
                # From the original data, remove selected variables
                remove_variables = [
                    v
                    for v in remove_variables
                    if any(
                        x in v
                        for x in [
                            f"Emissions|{s}|Energy",
                            f"Emissions|{s}|Industrial Processes",
                            f"Emissions|{s}|Other",
                            f"Emissions|{s}|Waste",
                            f"Emissions|{s}|Product Use",
                            f"Emissions|{s}|AFOLU|Agriculture",
                        ]
                    )
                    and v
                    not in [
                        f"Emissions|{s}|Energy|Demand|Bunkers|International Aviation",
                        f"Emissions|{s}|Energy|Demand|Bunkers|International Shipping",
                    ]
                ]
                tmp = tmp.loc[~tmp.Variable.isin(remove_variables)]

                # Add data
                keep_variables = [
                    v for v in df.Variable.unique() if f"Emissions|{s}" in v
                ]
                new_tmp = df.loc[df.Variable.isin(keep_variables)].copy()
                new_tmp = new_tmp.assign(
                    Model=row.target_model_name, Scenario=row.target_scenario_name
                )

                target_unit = tmp.Unit.unique().tolist()
                assert len(target_unit) == 1
                target_unit = target_unit[0]

                # Convert units from kt to Mt if required
                if "Mt" in target_unit:
                    new_tmp = new_tmp.set_index(static_index)
                    new_tmp = new_tmp / 1000
                    new_tmp = new_tmp.reset_index()

                # Assign correct units
                new_tmp = new_tmp.assign(Unit=target_unit)

                # Merge both dataframes
                tmp = pd.concat([tmp, new_tmp]).sort_values(by=static_index)

            # Assign CEDS data to Bunkers|International Aviation
            tmp_avi = aviation.loc[
                aviation.Variable.str.startswith(f"Emissions|{s}|Aircraft")
            ]
            tmp_avi = tmp_avi.assign(
                Model=row.target_model_name, Scenario=row.target_scenario_name
            )
            # Select only years which are in the target dataframe
            tmp_avi = tmp_avi[[c for c in tmp.columns if c in tmp_avi.columns]]
            # Rename all variables and sum as these are provided by fuel
            tmp_avi.Variable = (
                f"Emissions|{s}|Energy|Demand|Bunkers|International Aviation"
            )
            tmp_avi = tmp_avi.groupby(static_index).sum().reset_index()

            # Extend missing years
            for y in [
                c for c in tmp.columns if type(c) == int
            ]:  # if c not in tmp_avi]:
                tmp_avi[y] = 0
            tmp_avi = tmp_avi.set_index(static_index)

            # Extrapolate Waste-CO
            if s == "CO2":
                # Data is already provided in Mt-CO2
                co2_waste_data = co2_waste_data_raw.copy().set_index(
                    ["Region", "Variable", "Unit"]
                )
                co2_waste_data = co2_waste_data[
                    [
                        y
                        for y in data.set_index(static_index).columns
                        if y in co2_waste_data.columns
                    ]
                ]

                # Retrieve proxy data Emissions|BC|Waste from GAINS CLE used for extrpolating the growth of CO2|Waste
                co2_waste_driver = df_cle = (
                    pd.read_excel(
                        file_path / row.merge_pollutant.replace("MTFR", "CLE")
                    )
                    .drop(["Report_Time", "SCENARIO"], axis=1)
                    .rename(
                        columns={
                            "REGION": "Region",
                            "VARIABLE": "Variable",
                            "UNIT": "Unit",
                        }
                    )
                )
                co2_waste_driver.Region = co2_waste_driver.Region.str.replace(
                    "R12_", ""
                )

                # Filter out Emissions|BC|Waste
                co2_waste_driver = co2_waste_driver.loc[
                    co2_waste_driver.Variable == "Emissions|PM_BC|Waste"
                ]

                # Calculate rate of change
                co2_waste_driver = (
                    co2_waste_driver.drop(["Variable", "Unit"], axis=1)
                    .set_index("Region")
                    .pct_change(axis=1)
                )
                co2_waste_driver = co2_waste_driver + 1
                co2_waste_driver[2110] = co2_waste_driver[2100]

                for y in co2_waste_driver.columns:
                    if y in co2_waste_data.columns:
                        y_prev = y
                        continue
                    co2_waste_data[y] = co2_waste_data[y_prev] * co2_waste_driver[y]
                    y_prev = y

                pe_fossil = (
                    data.loc[
                        # (data.Variable == "Primary Energy|Fossil")
                        (
                            data.Variable
                            == "Final Energy|Industry|Chemicals|High-Value Chemicals"
                        )
                        & (data.Model == row.target_model_name)
                        & (data.Scenario == row.target_scenario_name)
                    ]
                    .drop(["Model", "Scenario", "Variable", "Unit"], axis=1)
                    .set_index("Region")
                )

                pe_fossil_woccs = (
                    data.loc[
                        # (data.Variable == "Primary Energy|Fossil|w/o CCS")
                        (
                            data.Variable.isin(
                                [
                                    "Final Energy|Industry|Chemicals|High-Value Chemicals|Gases|Gas",
                                    "Final Energy|Industry|Chemicals|High-Value Chemicals|Liquids|Coal",
                                    "Final Energy|Industry|Chemicals|High-Value Chemicals|Liquids|Gas",
                                    "Final Energy|Industry|Chemicals|High-Value Chemicals|Liquids|Oil",
                                    "Final Energy|Industry|Chemicals|High-Value Chemicals|Liquids|Other",
                                    "Final Energy|Industry|Chemicals|High-Value Chemicals|Solids|Coal",
                                    "Final Energy|Industry|Chemicals|High-Value Chemicals|Solids|Coke",
                                ]
                            )
                            & (data.Model == row.target_model_name)
                            & (data.Scenario == row.target_scenario_name)
                        )
                    ]
                    .drop(["Model", "Scenario", "Variable", "Unit"], axis=1)
                    .groupby("Region")
                    .sum()
                    # .set_index("Region")
                )

                ratio = pe_fossil_woccs / pe_fossil

                co2_waste_data = co2_waste_data * ratio
                co2_waste_data = pd.concat(
                    [
                        co2_waste_data,
                        co2_waste_data.reset_index()
                        .groupby(["Variable", "Unit"])
                        .sum()
                        .reset_index()
                        .assign(Region="World")
                        .set_index(["Region", "Variable", "Unit"]),
                    ]
                ).reset_index()
                co2_waste_data = co2_waste_data.assign(
                    Model=row.target_model_name, Scenario=row.target_scenario_name
                ).set_index(static_index)
                tmp = tmp.set_index(static_index)
                tmp = co2_waste_data.combine_first(tmp)
                tmp = tmp.reset_index()

            tmp_reg = tmp.loc[tmp.Region != "World"].copy()
            tmp_wrld = tmp.loc[tmp.Region == "World"].copy()
            if s in ["CO2", "CH4", "N2O"]:
                tmp_wrld_orig = tmp_wrld.copy()
            tmp_wrld = tmp_wrld.loc[
                tmp_wrld.Variable
                == f"Emissions|{s}|Energy|Demand|Bunkers|International Shipping"
            ].set_index(static_index)

            # Set regional aviation to 0
            tmp_reg.loc[
                tmp_reg.Variable
                == f"Emissions|{s}|Energy|Demand|Bunkers|International Aviation",
                [c for c in tmp_reg.columns if c not in static_index],
            ] = 0

            # Generate missing sums for regions
            aggregates = {
                f"Emissions|{s}|Energy|Demand|Bunkers": [
                    f"Emissions|{s}|Energy|Demand|Bunkers|International Aviation",
                    f"Emissions|{s}|Energy|Demand|Bunkers|International Shipping",
                ],
                f"Emissions|{s}|Energy|Demand": [
                    f"Emissions|{s}|Energy|Demand|Industry",
                    f"Emissions|{s}|Energy|Demand|Residential and Commercial and AFOFI",
                    f"Emissions|{s}|Energy|Demand|Transportation",
                    f"Emissions|{s}|Energy|Demand|Bunkers",
                    f"Emissions|{s}|Energy|Demand|Other Sector",
                ],
                f"Emissions|{s}|Energy": [
                    f"Emissions|{s}|Energy|Demand",
                    f"Emissions|{s}|Energy|Supply",
                ],
                f"Emissions|{s}|Fossil Fuels and Industry": [
                    f"Emissions|{s}|Energy",
                    f"Emissions|{s}|Industrial Processes",
                ],
                f"Emissions|{s}|AFOLU": [
                    f"Emissions|{s}|AFOLU|Agricultural Waste Burning",
                    f"Emissions|{s}|AFOLU|Land",
                    f"Emissions|{s}|AFOLU|Agriculture",
                    f"Emissions|{s}|AFOLU|Afforestation",  # Needed for CO2
                    f"Emissions|{s}|AFOLU|Agriculture",  # Needed for CO2
                    f"Emissions|{s}|AFOLU|Deforestation",  # Needed for CO2
                    f"Emissions|{s}|AFOLU|Forest Management",  # Needed for CO2
                    f"Emissions|{s}|AFOLU|Other",  # Needed for CO2
                    f"Emissions|{s}|AFOLU|Other LUC",  # Needed for CO2
                ],
                f"Emissions|{s}": [
                    f"Emissions|{s}|AFOLU",
                    f"Emissions|{s}|Capture and Removal",  # Needed for CO2
                    f"Emissions|{s}|Energy",
                    f"Emissions|{s}|Industrial Processes",
                    f"Emissions|{s}|Product Use",  # Needed for CH4/N2O
                    f"Emissions|{s}|Other",
                    f"Emissions|{s}|Waste",
                ],
            }

            for parent in aggregates:
                if s in ["CO2", "CH4", "N2O"] and parent not in [
                    f"Emissions|{s}|Energy|Demand|Bunkers",
                    f"Emissions|{s}|Energy|Demand",
                    f"Emissions|{s}|Energy",
                    f"Emissions|{s}",
                    f"Emissions|{s}|Fossil Fuels and Industry",
                ]:
                    continue

                update = tmp_reg.loc[tmp_reg.Variable.isin(aggregates[parent])].copy()
                update.Variable = parent
                update = update.groupby(static_index).sum()

                tmp_reg = tmp_reg.set_index(static_index)
                tmp_reg = update.combine_first(tmp_reg).reset_index()

            # Generate "World" data
            # Overwrite aviation emissions
            if s in ["CO2", "CH4", "N2O"]:
                sum_reg = tmp_wrld_orig.copy().set_index(static_index)
            else:
                sum_reg = tmp_reg.copy()
                sum_reg.Region = "World"
                sum_reg = sum_reg.groupby(static_index).sum()
            sum_reg = tmp_avi.combine_first(
                tmp_wrld.combine_first(sum_reg)
            ).reset_index()

            # Sum regional data sxcept for bunkers
            for parent in aggregates:
                if s in ["CO2", "CH4", "N2O"] and parent not in [
                    f"Emissions|{s}|Energy|Demand|Bunkers",
                    f"Emissions|{s}|Energy|Demand",
                    f"Emissions|{s}|Energy",
                    f"Emissions|{s}",
                    f"Emissions|{s}|Fossil Fuels and Industry",
                ]:
                    continue
                update = sum_reg.loc[sum_reg.Variable.isin(aggregates[parent])].copy()
                update.Variable = parent
                update = update.groupby(static_index).sum()

                sum_reg = sum_reg.set_index(static_index)
                sum_reg = update.combine_first(sum_reg).reset_index()

            data = pd.concat([data, sum_reg, tmp_reg]).sort_values(by=static_index)
    return data


def main(
    project_data_path,
    config_fil,
    source_dir,
    out_fil,
    add_economic_indicator=False,
    interpolate_emission=False,
    load_from_database=False,
    mp=None,
    derive_aviation=False,
    add_gross_removal=False,
    merge_indicator_trp=False,
    merge_indicator_rc=False,
    merge_pollutant=False,
    recalculate_kyoto=False,
):
    """Prepare scenarios for submission to an IIASA Scenario Explorer database.

    Parameters
    ----------
    config_fil : pathlib.Path()
        Path including the file name where the configuration file is located.
    source_dir : pathlib.Path()
        Path where the xlsx reporting files are located.
    out_fil : pathlib.Path()
        Path including file name to which the oupput should be written.
    add_economoic_indicator : boolean (default=True)
        option whether to derive consumption and GDP losses.
    """
    print("Starting preparation of submission data")

    global file_path, static_index
    file_path = project_data_path
    static_index = ["Model", "Scenario", "Region", "Variable", "Unit"]

    # Retrieve all the data
    all_data = pd.read_excel(config_fil, sheet_name=None)

    # Retrieve scenario configuration
    config = all_data["scenario_config"]
    print(f"    Using configuration {config_fil}")
    data = read_data(config, source_dir, load_from_database, mp)

    if add_economic_indicator:
        data = add_economic_indicators(data, config)

    if merge_indicator_trp:
        data = merge_indicators_trp(data, config)

    if merge_indicator_rc:
        data = merge_indicators_rc(data, config)

    if merge_pollutant:
        data = merge_pollutants(data, config)

    data = data.set_index(static_index)
    data = data.rename(columns={c: int(c) for c in data.columns}).reset_index()

    index = static_index + sorted([c for c in data.columns if c not in static_index])
    data = data[index]

    if derive_aviation:
        data = data.set_index(static_index).fillna(0.0)
        data = (
            data.rename(columns={c: str(c) for c in data.columns})
            .reset_index()
            .sort_values(by=static_index)
        )
        dfs = []
        for i in config.index:
            row = config.iloc[i]
            if row.final_scenario == False:
                continue
            df = data.loc[
                (data.Scenario == row.target_scenario_name)
                & (data.Model == row.target_model_name)
            ].copy()
            df["Model"] = row.source_model_name
            print(
                f"Prcessing aviation for {row.source_model_name} // {row.target_scenario_name}"
            )
            df.to_csv("SSP2_LOW.csv")
            df = process_df(df, method=METHOD.C)
            df.to_csv("SSP2_LOW_updated.csv")
            df["Model"] = row.target_model_name
            dfs.append(df)
        data = pd.concat(dfs, ignore_index=True)
        data = data.sort_values(by=static_index)
        # data.to_csv("data_post_aviation.csv", index=False)
        data = data.set_index(static_index)
        data = data.rename(columns={c: int(c) for c in data.columns}).reset_index()

    if interpolate_emission:
        data = interpolate_emissions(data)

    if add_gross_removal:
        data = add_gross_removals(data)

    if recalculate_kyoto:
        data = recalculate_kyoto_gases(data)

    # Remove unwanted variables
    if "remove_variable" in all_data.keys():
        to_remove = all_data["remove_variable"]["variable_name"].to_list()
        data = data.loc[~data["Variable"].isin(to_remove)]

    # Specify which index is used for renaming
    rename = {
        "region_mapping": ["Region"],
        "variable_mapping": ["Variable"],
        "unit_mapping": ["Unit"],
    }

    # Apply renaming
    for sheet in rename:
        if sheet not in all_data.keys():
            continue
        print(f"    Renaming {rename[sheet][0]}")
        if sheet != "unit_mapping":
            to_replace = (
                all_data[sheet].set_index("source_name").to_dict()["target_name"]
            )
            data[rename[sheet]] = data[rename[sheet]].replace(to_replace)
        # Exception is made for unit mapping.
        # Renaming is performed per row;
        # A check is made to see if the renaming applies only to select variables.
        else:
            for i, row in all_data[sheet].iterrows():
                source_name = row["source_name"]
                target_name = row["target_name"]
                idx_loc = data["Unit"] == source_name

                # Check if the renaming is only applied to certain variables.
                if not pd.isna(row["variable_name"]):
                    # Create list of variables from filter elements
                    parts = row["variable_name"].split(",")
                    has_parts = lambda v: any(y in v for y in parts)
                    variables = [v for v in data["Variable"] if has_parts(v)]
                    idx_loc = idx_loc & (data["Variable"].isin(variables))

                data.loc[idx_loc, "Unit"] = target_name

    #        if sheet == "variable_mapping":
    #            data.loc[
    #                data.Variable
    #                == "Energy Service|Transportation|Public Transport [share]",
    #                "Variable",
    #            ] = "Energy Service|Transportation|Passenger|Public Transport [Share]"
    #            data.loc[
    #                data.Variable == "Energy Service|Transportation|ROAD", "Variable"
    #            ] = "Energy Service|Transportation|Passenger|Road"
    #            data.loc[
    #                data.Variable == "Agricultural Demand|Crops|Bioenergy|2nd generation",
    #                "Variable",
    #            ] = "Agricultural Demand|Crops|Bioenergy|2nd Generation"

    # Filter out any regions with region as "NaN"
    data = data.loc[~data.Region.isnull()]

    merge_xlsx(data, config, out_fil)


@click.command("prep-submission", help=main.__doc__)
@click.argument(
    "config-fil", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument(
    "source-dir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
)
@click.argument("out-fil", type=click.Path(exists=False, path_type=Path))
@click.option(
    "--add-economic-indicator", is_flag=True, help="Add GDP and consumption losses."
)
@click.option(
    "--interpolate_emission",
    is_flag=True,
    help="Interpolate_emissions for 2020 tot 2025.",
)
@click.option(
    "--load_from_database",
    is_flag=True,
    help="Load data directly from scenario object.",
)
@click.option(
    "--derive_aviation",
    is_flag=True,
    help="Derive aviation final energy and emissions.",
)
@click.pass_obj
def cli(
    context,
    config_fil,
    source_dir,
    out_fil,
    add_economic_indicator,
    interpolate_emission,
    load_from_database,
    derive_aviation,
):
    main(
        config_fil,
        source_dir,
        out_fil,
        add_economic_indicator,
        interpolate_emission,
        load_from_database,
        derive_aviation,
    )

