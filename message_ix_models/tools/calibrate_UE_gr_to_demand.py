import pandas as pd
import numpy as np

from itertools import groupby
from operator import itemgetter

from .get_optimization_years import main as get_optimization_years
from .get_nodes import get_nodes

from .cagr import CAGR

# In some cases, end-use technologies have outputs onto multiple demands.
# If this is the case, then the a manual assignment is undertaken,
# based on the last part of the end-use tec. name
manual_demand_allocation = {
    "I": "i_spec",
    "RC": "rc_spec",
    "rc": "rc_therm",
    "trp": "transport",
    "fs": "i_feed",
    "i": "i_therm",
}

# Define index for final dataframe
index = ["node_loc", "technology", "parameter", "year_act"]


def main(scenario, data_path, ssp, region, first_mpa_year=None, intpol_lim=1, verbose=False):
    """Calibration of dynamic growth constraints for
    Useful Energy technologies.

    The general dynamic growth constraints for UE technologies as defined in
    the input file are added to scenario, while ensuring that these constraints
    allow demand in- and decreases to be met.

    Parameters
    ----------
    scenario : :class:`message_ix.Scenario`
        scenario to which changes should be applied
    data_path : :class:`pathlib.Path`
        path to model-data directory in message_data repositor
    ssp : str
        name of SSP for which the script is being run.
        (SSP1, SSP2 or SSP3)
    region: str
        (R12, R11)
    first_mpa_year : int
        the first year for which the dynamic bounds should be adjusted.
    intpol_lim : 1
        Number of time consecutive NaN values that can be interpolated.
        If changing, please check the results from manually adjusted mpas,
        and see that only values whcih should be adjusted are actually
        adjusted.
    verbose : boolean (default=False)
        option whether to print on screen messages.
    """
    # Retrieve years for which changes should be applied
    years = get_optimization_years(scenario)

    # Retrieve data for corresponding SSP
    data_filname = "SSP_UE_dyn_input.xlsx"
    data_fil = data_path / "material" /"UE_dynamic_constraints" / data_filname
    mpa_data = pd.read_excel(data_fil, sheet_name="SSP_data")
    mpa_tec = mpa_data["technology"].tolist()

    if not first_mpa_year:
        first_mpa_year = years[0]
    else:
        years = [y for y in years if y >= first_mpa_year]

    # Checks if the year as of which mpa should be generated is also
    # in the model
    assert int(first_mpa_year) in scenario.set("year").values, (
        "Year as of which mpas should be generated is not defined as"
        + "a year in the model"
    )

    # Checks whether all technologies contained in the config file are
    # also contained within the model
    df = scenario.par(
        "output",
        filters={"level": ["useful"], "year_act": years, "technology": mpa_tec},
    )
    df = df[df.year_act == df.year_vtg]

    missing_tec = [t for t in mpa_tec if t not in df["technology"].unique().tolist()]
    if missing_tec:
        print(missing_tec, "not included in scenario")
        mpa_data = mpa_data[~mpa_data["technology"].isin(missing_tec)]

    # Retrieves scenario demands
    demands = (
        scenario.par("demand")
        .drop(["time", "unit", "level"], axis=1)
        .pivot_table(index=["node", "commodity"], columns="year", values="value")
    )

    # For all the demands, calculate the growth rates
    demands_gr = demands.copy()
    demands_gr = demands_gr.apply(
        lambda x: x
        if int(x.name) < first_mpa_year
        else CAGR(
            demands[
                demands.reset_index().columns[
                    int(demands.reset_index().columns.get_loc(x.name)) - 1
                ]
            ],
            x,
            int(scenario.par("duration_period", filters={"year": [x.name]}).value),
        )
    )

    # Downselect only relevant data
    # This is not done when retrieving the data so that GR
    # can be calculated
    demands = demands[years]
    demands_gr = demands_gr[years]

    # Filter required columns for final df
    df = df[["node_loc", "technology", "year_act", "commodity"]]
    df["demand"] = 1
    df = df.set_index(["node_loc", "year_act", "commodity"])

    # Allocate demands to dataframe
    demands = (
        demands.stack()
        .reset_index()
        .rename(columns={0: "value", "node": "node_loc", "year": "year_act"})
        .set_index(["node_loc", "year_act", "commodity"])
    )

    df.demand = demands.value

    # Allocate growth rates to dataframe
    demands_gr = (
        demands_gr.stack()
        .reset_index()
        .rename(columns={0: "value", "node": "node_loc", "year": "year_act"})
        .set_index(["node_loc", "year_act", "commodity"])
    )

    df["dem_gr"] = demands_gr.value

    # Allocate input data to dataframe
    df = (
        df.reset_index()
        .set_index("technology")
        .join(mpa_data.set_index("technology"), how="outer")
        .reset_index()
        .set_index(["node_loc", "technology", "commodity", "year_act"])
    )

    # Calculate mpa_lo
    # Ensures that the declines in demand can be met
    tmp_df = df.copy()
    tmp_df["a"] = 1 + df["mpa_lo"]
    tmp_df["b"] = df["dem_gr"] + df["mpa_lo"]
    tmp_df = tmp_df[["a", "b"]]
    df.mpa_lo = tmp_df.min(axis=1)

    # Calculate mpa_up
    # Ensures that increases in demand can be met
    tmp_df = df.copy()
    tmp_df["a"] = 1 + df["mpa_up"]
    tmp_df["b"] = df["dem_gr"] + df["mpa_up"]
    tmp_df = tmp_df[["a", "b"]]
    tmp_df["a"] = tmp_df.min(axis=1)
    tmp_df["b"] = df["dem_gr"] + df["mpa_up"] / 2
    df.mpa_up = tmp_df.max(axis=1)

    # Calculate startup_lo
    df["startup_lo"] = ((df["mpa_lo"] - 1) * df["startup_lo"] * df["demand"]) / (
        df["mpa_lo"] ** (10) - 1
    )

    # Calculate startup_up
    df["startup_up"] = ((df["mpa_up"] - 1) * df["startup_up"] * df["demand"]) / (
        df["mpa_up"] ** (10) - 1
    )

    df = df[["startup_lo", "mpa_lo", "startup_up", "mpa_up"]]
    df.mpa_lo = df.mpa_lo - 1
    df.mpa_up = df.mpa_up - 1

    df = df.reset_index()

    # Identify all technologies with multiple outputs and assign correct demand
    tmp = df[["technology", "commodity"]].drop_duplicates()
    double = []
    for t in tmp["technology"].unique():
        if len(tmp[tmp["technology"] == t].commodity.tolist()) > 1:
            double.append(t)
    for tec in double:
        sector = manual_demand_allocation[tec.split("_")[-1]]
        df = df[~((df["technology"] == tec) & (df["commodity"] != sector))]

    df = (
        df.rename(
            columns={
                "startup_lo": "initial_activity_lo",
                "startup_up": "initial_activity_up",
                "mpa_lo": "growth_activity_lo",
                "mpa_up": "growth_activity_up",
            }
        )
        .drop(["commodity"], axis=1)
        .set_index(["node_loc", "technology", "year_act"])
        .stack()
        .reset_index()
        .rename(columns={"level_3": "parameter", 0: "value"})
        .set_index(index)
    )

    # Read data for manual overrides.
    mpa_overrides = pd.read_excel(data_fil, sheet_name=f"{ssp}_{region}_mpa_manual_override")

    # Retrieve region prefix and adapt overrides
    region_id = list(set([x.split("_")[0] for x in get_nodes(scenario)]))[0]
    mpa_overrides["node_loc"] = region_id + "_" + mpa_overrides["node_loc"]
    mpa_overrides = mpa_overrides.set_index(["node_loc", "technology", "parameter"])

    # Filter out all the values which are supposed to be NaN
    # These will be added at the end.

    # Create a list of years for which years need to be added.
    add_yr = [y for y in years if y not in mpa_overrides.columns]

    # Add missing years to dataframe with nan values
    for y in add_yr:
        mpa_overrides[y] = np.nan

    # A Check is made to see if the number of consecutive years missing, exceeds
    # the number of years allowed for interpolation. The number of years is not adjusted
    # automatically, as the results needed to be checked.
    for k, g in groupby(enumerate(add_yr), lambda ix: ix[0] - ix[1]):
        chk = len(list(map(itemgetter(1), g)))
        if chk > intpol_lim:
            raise ValueError(
                f"The number of consecutive years being added, {chk},"
                " exceeds the number of years which can be interpolated,"
                f" {intpol_lim}"
            )

    # Interpolate and readjust dataframe
    mpa_overrides = (
        mpa_overrides[sorted(mpa_overrides.columns)]
        .interpolate(method="index", limit=intpol_lim, axis=1)
        .stack()
        .reset_index()
        .rename(columns={"level_3": "year_act", 0: "value"})
        .set_index(index)
    )

    # Merge 'overrides' into dataframe
    final_results = mpa_overrides.combine_first(df).reset_index()

    scenario.check_out()

    growth_activity_lo = final_results[
        final_results["parameter"] == "growth_activity_lo"
    ].drop("parameter", axis=1)
    growth_activity_lo["time"] = "year"
    growth_activity_lo["unit"] = "%"
    growth_activity_lo["mode"] = "M1"
    growth_activity_lo.value = round(growth_activity_lo.value, 3)

    scenario.add_par("growth_activity_lo", growth_activity_lo)

    growth_activity_up = final_results[
        final_results["parameter"] == "growth_activity_up"
    ].drop("parameter", axis=1)
    growth_activity_up["time"] = "year"
    growth_activity_up["unit"] = "%"
    growth_activity_up["mode"] = "M1"
    growth_activity_up.value = round(growth_activity_up.value, 3)

    scenario.add_par("growth_activity_up", growth_activity_up)

    initial_activity_lo = final_results[
        final_results["parameter"] == "initial_activity_lo"
    ].drop("parameter", axis=1)
    initial_activity_lo["time"] = "year"
    initial_activity_lo["unit"] = "GWa"
    initial_activity_lo["mode"] = "M1"
    initial_activity_lo.value = round(initial_activity_lo.value, 3)

    scenario.add_par("initial_activity_lo", initial_activity_lo)

    initial_activity_up = final_results[
        final_results["parameter"] == "initial_activity_up"
    ].drop("parameter", axis=1)
    initial_activity_up["time"] = "year"
    initial_activity_up["unit"] = "GWa"
    initial_activity_up["mode"] = "M1"
    initial_activity_up.value = round(initial_activity_up.value, 3)

    scenario.add_par("initial_activity_up", initial_activity_up)

    scenario.commit("updated end-use mpas")
