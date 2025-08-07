# run MAGICC only
import os

import ixmp as ix
import message_ix
import pandas as pd

os.chdir(
    "C:/Users/vinca/Documents/Github/message-ix-models/message_ix_models/project/GDP_climate_shocks"
)
from call_climate_processor import run_climate_processor
from cli import run_magicc_rime

from message_ix_models.project.GDP_climate_shocks.util import regional_gdp_impacts

# Setup platform
mp = ix.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])
model = "ENGAGE_SSP2_T4.5_GDP_CI_2025"

# List of scenario names
scenarios = [
    "INDC2030i_GDP_CI_0",
    "INDC2030i_GDP_CI_33_Burke_1",
    "INDC2030i_GDP_CI_33_Burke_2",
    "INDC2030i_GDP_CI_33_Kotz_1",
    "INDC2030i_GDP_CI_33_Kotz_3",
    "INDC2030i_GDP_CI_33_Waidelich_2",
    "INDC2030i_GDP_CI_5_Burke_2",
    "INDC2030i_GDP_CI_5_Kotz_2",
    "INDC2030i_GDP_CI_5_Waidelich_1",
    "INDC2030i_GDP_CI_50_Burke_2",
    "INDC2030i_GDP_CI_50_Kotz_1",
    "INDC2030i_GDP_CI_50_Kotz_3",
    "INDC2030i_GDP_CI_50_Waidelich_2",
    "INDC2030i_GDP_CI_67_Burke_3",
    "INDC2030i_GDP_CI_67_Kotz_3",
    "INDC2030i_GDP_CI_67_Waidelich_2",
    "INDC2030i_GDP_CI_95_Burke_2",
    "INDC2030i_GDP_CI_95_Kotz_1",
    "INDC2030i_GDP_CI_95_Kotz_3",
    "INDC2030i_GDP_CI_95_Waidelich_2",
]

# Loop through each scenario and run the processor
for scen in scenarios:
    try:
        sc = message_ix.Scenario(mp, model=model, scenario=scen)
        run_climate_processor(sc)
        print(f"✅ Processed: {scen}")
    except Exception as e:
        print(f"❌ Failed: {scen} – {e}")


# new batch from file
csv_file = "C:\\Users\\vinca\\IIASA\\ECE.prog - GDP_damages\\magicc_rime\\scenario_set_summary 4.csv"
# load csv file
scenlist_df = pd.read_csv(csv_file)
for m, s, ssp in zip(scenlist_df["model"], scenlist_df["scenario"], scenlist_df["ssp"]):
    try:
        run_magicc_rime(
            model_name=m,
            ssp=ssp,
            scens_ref=[s],
            damage_model=["Waidelich", "Kotz", "Burke"],
            percentiles=50,
            regions="R12",
            shift_year=None,
            config="default",
            input_only=True,
        )
    except Exception as e:
        print(f"❌ Failed: {s} – {e}")


# convert from country to region

csv_file = "C:\\Users\\vinca\\IIASA\\ECE.prog - GDP_damages\\magicc_rime\\scenario_set_summary 4.csv"
it = 1
regions = "R12"
percentile = 50
gdp_change_dfs = []
# load csv file
scenlist_df = pd.read_csv(csv_file)
for m, s, ssp in zip(scenlist_df["model"], scenlist_df["scenario"], scenlist_df["ssp"]):
    for damage_model in ["Waidelich", "Kotz", "Burke"]:
        try:
            sc_str_full = f"{m}_{s}"
            gdp_change_df = regional_gdp_impacts(
                sc_str_full, damage_model, it, ssp, regions, percentile
            )
            gdp_change_df["model"] = m
            gdp_change_df["scenario"] = s
            gdp_change_df["damage_model"] = damage_model
            gdp_change_df["unit"] = "%"
            # rename node to region
            gdp_change_df = gdp_change_df.rename(columns={"node": "region"})
            gdp_change_dfs.append(gdp_change_df)
        except Exception as e:
            print(f"❌ Failed: {s} – {e}")

# Combine all DataFrames into one and save as CSV
combined_gdp_change_df = pd.concat(gdp_change_dfs, ignore_index=True)
combined_gdp_change_df = combined_gdp_change_df.rename(
    columns={"perc_change_sum": "value"}
)
# change order of columns: model, scenario, damage_model, region, year, unit,  value
combined_gdp_change_df = combined_gdp_change_df[
    ["model", "scenario", "damage_model", "region", "year", "unit", "value"]
]
combined_gdp_change_df.to_csv(
    "C:\\Users\\vinca\\IIASA\\ECE.prog - GDP_damages\\magicc_rime\\RIMeregion\\gdp_change_reg_combined.csv",
    index=False,
)
