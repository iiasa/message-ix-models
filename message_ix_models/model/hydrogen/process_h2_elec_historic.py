import pandas as pd
import numpy as np
from pathlib import Path

path_read = Path("IEA/")
file_name = "IEA_production_db.xlsx"

df = pd.read_excel(path_read / file_name, sheet_name="Projects", skiprows=3)


status = ["Operational", "Decommisioned"]


# taking all currently operating projects.
df_operational = df[df["Status"].isin(status)].copy().reset_index(drop=True)
df_operational = df_operational[~df_operational["Date online"].isna()].reset_index(
    drop=True
)

# Now we need to separate between the past and current.
# Meaning which projects/facilities are still operating and which
# are not.
# To our model, anything that is in decommission date below 2030
# is a past project!
columns = df_operational.columns.tolist()
columns_to_keep = [
    "Technology",
    "Technology_details",
    "Product",
    "Country",
    "Project name",
    "Date online",
    "Decomission date",
    "Announced Size",
    "Status",
    "Capacity_kt H2/y",
]
columns_to_drop = [col for col in columns if col not in columns_to_keep]
df_past = (
    df_operational[
        (df_operational["Decomission date"] < 2030)
        | df_operational["Decomission date"].isna()
    ]
    .copy()
    .reset_index(drop=True)
)
df_past.drop(columns=columns_to_drop, inplace=True)
techs = ["ALK", "PEM", "SOEC", "Other Electrolysis"]
df_past = df_past[df_past["Technology"].isin(techs)]
df_past = df_past[~(df_past["Technology_details"] == "Unknown PtX")]

### now we should have cleaned the database enough. We should be able to create a dataframe that we can eventually
# use for historical_activity and historical_new_capacity

# First of all we assume that any NaN decomission date is a still running project. In our case this
# means that it should be 2030.
df_past["Decomission date"] = df_past["Decomission date"].fillna(2030)
df_past["Operation_years"] = df_past["Decomission date"] - df_past["Date online"]
min_years = df_past["Date online"].min()
max_years = df_past["Decomission date"].max()

# we need to eventually bin the years according to what we have in message_ix
years_bin = np.arange(min_years, max_years + 1, 5)

# i would say the most convenient thing to do right now is to take the
# yearly production and then multiply it by 5. That should give us the
# production for each 5 year bin. Which will be always the same for each
# project. First we need to also convert the production from kt H2/y to GWa/y.
lhv_h2 = 33.33 / 8760 / 1000000 * 1000 * 1000  # GWa/ktH2

df_past["Production_GWa/y"] = df_past["Capacity_kt H2/y"] * lhv_h2
df_past["Production_lustrum"] = df_past["Production_GWa/y"] * 5

# now we need to create a dataframe that will have the production for each 5 year bin.
# we will use the years_bin to create the bins.

# now we need to create a dataframe that will have the production for each 5 year bin.
# add first the columns for each year.
for year in years_bin:
    print(year)
    df_past[int(year)] = np.nan


for idx, row in df_past.iterrows():
    start_year = row["Date online"]
    end_year = row["Decomission date"]
    if end_year < start_year:
        print(
            f"project {row['Project name']} has a decomission date before the start date"
        )
        continue
    first_year = int(start_year // 5 * 5)
    if end_year % 5 != 0:
        last_year = int(end_year // 5 * 5 + (5))
    else:
        last_year = int(end_year // 5 * 5)
    range_years = np.arange(first_year, last_year + 1, 5)
    multiplier_first = 5 - (start_year - first_year)
    multiplier_last = 5 - (last_year - end_year)
    length = len(range_years)
    for year, enum in zip(range_years, range(length)):
        if enum == 0:
            df_past.loc[idx, int(year)] = (
                row["Production_lustrum"] / 5 * multiplier_first
            )
        elif enum == length - 1:
            df_past.loc[idx, int(year)] = (
                row["Production_lustrum"] / 5 * multiplier_last
            )
        else:
            df_past.loc[idx, int(year)] = row["Production_lustrum"]

### now it is time for some mapping to regions!
iso_mapping = pd.read_csv(path_read / "Regions_R12.csv")
df_historical = df_past = df_past.merge(
    iso_mapping[["child", "Region"]], left_on="Country", right_on="child", how="left"
)

# remove unnecessary columns
df_historical.drop(
    columns=[
        "Country",
        "Date online",
        "Decomission date",
        "Project name",
        "Product",
        "Announced Size",
        "Status",
        "Capacity_kt H2/y",
        "Production_GWa/y",
        "Production_lustrum",
        "Operation_years",
    ],
    inplace=True,
)

# we assume that anything that is ALK+PEM is actually only ALK. We neglect PEM in that case.
mask_alk = (df_historical["Technology_details"] == "ALK+PEM") | (
    df_historical["Technology_details"] == "ALK + PEM"
)
df_historical.loc[mask_alk, "Technology_details"] = "ALK"
other_mask = df_historical["Technology"] == "Other Electrolysis"

df_historical.loc[df_historical["Technology_details"].isna(), "Technology_details"] = (
    "Unknown"
)
mask_alk = df_historical["Technology_details"].str.contains("ALK")
df_historical.loc[mask_alk, "Technology"] = "ALK"

mask_unknown = df_historical["Technology_details"] == "Unknown"
# df_historical.loc[mask_unknown, "Technology"] = "ALK"

residual_mask = ~(mask_alk | mask_unknown)
df_historical.loc[residual_mask, "Technology"] = df_historical.loc[
    residual_mask, "Technology_details"
]

df_historical = df_historical[df_historical["Technology"] != "Other Electrolysis"]

### now we are almost there. We have to sum over the Region, technology combo.
df_historical = df_historical.groupby(["Region", "Technology"]).sum().reset_index()
df_historical.drop(columns=["Technology_details", "child"], inplace=True)

# First, identify the year columns (assuming they're numeric)
year_columns = [col for col in df_historical.columns if str(col).isdigit()]

# Melt the DataFrame to convert years from columns to rows
df_melted = df_historical.melt(
    id_vars=["Region", "Technology"],  # Keep these as identifier columns
    value_vars=year_columns,  # These columns will become rows
    var_name="year",  # Name for the new year column
    value_name="value",  # Name for the new value column
)

# Add the additional columns you want
df_melted["mode"] = "M1"
df_melted["time"] = "year"

# Reorder columns to match your desired format
df_melted = df_melted[["Region", "Technology", "year", "mode", "value"]]

# Remove rows where value is 0 or NaN (optional, depending on your needs)
df_melted = df_melted[df_melted["value"] != 0]
df_melted = df_melted.dropna(subset=["value"])

# there is a technology called me450. But we are only adding
# ALK SOE and PEM. So let's filter for these three
df_melted = df_melted[df_melted["Technology"].isin(["ALK", "SOEC", "PEM"])]

# Reset index
df_melted = df_melted.reset_index(drop=True)

df_melted.rename(
    columns={"Region": "node_loc", "Technology": "technology", "year": "year_act"},
    inplace=True,
)
df_melted["time"] = "year"
df_melted["unit"] = "GWa"

df_melted.loc[df_melted["technology"] == "ALK", "technology"] = "h2_elec_alk"
df_melted.loc[df_melted["technology"] == "SOEC", "technology"] = "h2_elec_soe"
df_melted.loc[df_melted["technology"] == "PEM", "technology"] = "h2_elec_pem"


# Now for the historical new capacity. This is not historical new capacity. But it is
# just the new addition of capacity for a given region.
# we have to use the capacity factor of each technology first.

cf = 0.95  # this is what we have used. It comes straight from the old h2_elec tech.

# now we have to multiply the capacity factor by the production for each year.
df_capacity = df_melted.copy()
df_capacity["value"] = df_capacity["value"] / cf


# now we have the total installed capacity of each year. But we need the new additional
# capacity really. So we need to subtract the capacity of the previous year from the total
# capacity of the current year.

# IMPORTANT: Group by node_loc and technology, then sort by year within each group
df_capacity = df_capacity.sort_values(["node_loc", "technology", "year_act"])

# Calculate incremental capacity additions within each group
df_capacity["prev_capacity"] = df_capacity.groupby(["node_loc", "technology"])[
    "value"
].shift(1)

# For the first year of each group, the "new capacity" is the full capacity
# For subsequent years, it's the difference from the previous year
df_capacity["new_capacity"] = df_capacity["value"] - df_capacity[
    "prev_capacity"
].fillna(0)

# Only keep positive additions (negative would mean decommissioning, which we don't want for historical_new_capacity)
df_capacity["new_capacity"] = df_capacity["new_capacity"].clip(lower=0)

# Update the value column to contain the new capacity additions
df_capacity["value"] = df_capacity["new_capacity"]

# Clean up temporary columns and remove rows with zero additions
df_capacity = df_capacity.drop(columns=["prev_capacity", "new_capacity"])
df_capacity = df_capacity[df_capacity["value"] > 0].reset_index(drop=True)
df_capacity.rename(columns={"year_act": "year_vtg"}, inplace=True)
df_capacity.drop(columns=["time", "mode"], inplace=True)
df_capacity["unit"] = "GW"

# we also need to rename the

# let's now add the capacity and activity to the model

import ixmp
import message_ix
from message_ix import Scenario
from message_ix.util import make_df

mp = ixmp.Platform("ixmp-dev")
model = "hyway_SSP_SSP2_v6.2"
scenario = "baseline_1000f_h2_pyro_elec_multi_h2_elec"

base = Scenario(mp, model, scenario)

scen = base.clone(
    model,
    f"baseline_1000f_h2_elec_history",
    "add historical activity and new capacity for h2_elec technologies",
    keep_solution=False,
)

scen.check_out()

# now we have to add the historical activity and new capacity to the model.

scen.add_par("historical_activity", df_melted)
scen.add_par("historical_new_capacity", df_capacity)

scen.commit("add historical activity and new capacity for h2_elec technologies")
scen.set_as_default()
scen.solve(solve_options={"scaind": -1})

print("scenario solved")

import seaborn as sns
import matplotlib.pyplot as plt


techs = ["h2_elec_alk", "h2_elec_soe", "h2_elec_pem"]
act_tech = scen.var("ACT", {"technology": techs})

# now let's plot the activity of the new technologies:
act_pivot = act_tech.groupby(["year_act", "node_loc"]).sum().reset_index()
act_pivot = act_pivot.pivot(index="year_act", columns="node_loc", values="lvl")
act_pivot.plot(
    title="Total Activity of new h2_elec technologies",
    xlabel="Year",
    ylabel="Activity Level",
)
plt.savefig("plots/total_activity_by_region_multi_h2_elec.png")
act_by_years = act_tech.groupby(["year_act"]).sum().reset_index()
act_by_years.plot(
    x="year_act", y="lvl", legend=True, title="Activity of h2_elec technologies"
)

# let's check globa production by electrolyzer type

## let's check if there is any dynamic in terms of tech utilization over time:
act_differences = (
    act_tech.groupby(["year_act", "node_loc", "technology"]).sum().reset_index()
)

act_global = act_tech.groupby(["year_act", "technology"]).sum().reset_index()
sns.lineplot(data=act_global, x="year_act", y="lvl", hue="technology")
plt.savefig("plots/act_global_multi_h2_elec.png")

sns.relplot(
    data=act_differences,
    x="year_act",
    y="lvl",
    hue="technology",
    col="node_loc",
    col_wrap=3,
    kind="line",
)
plt.savefig("plots/act_regional_multi_h2_elec.png")

mp.close_db()
