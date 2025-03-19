
def read_water_availability(context: "Context") -> Sequence[pd.DataFrame]:
    """
    Reads water availability data and bias correct
    it for the historical years and no climate
    scenario assumptions.

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : (pd.DataFrame, pd.DataFrame)
    """

    # Reference to the water configuration
    info = context["water build info"]
    # reading sample for assiging basins
    PATH = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)

    if "year" in context.time:
        # path for reading basin delineation file
        PATH = package_data_path(
            "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
        )
        df_x = pd.read_csv(PATH)
        # Adding freshwater supply constraints
        # Reading data, the data is spatially and temprally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
        )
        # Read rcp 2.6 data
        df_sw = pd.read_csv(path1)
        df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)

        df_sw.index = df_x["BCU_name"].index
        df_sw = df_sw.stack().reset_index()
        df_sw.columns = pd.Index(["Region", "years", "value"])
        df_sw.fillna(0, inplace=True)
        df_sw.reset_index(drop=True, inplace=True)
        df_sw["year"] = pd.DatetimeIndex(df_sw["years"]).year
        df_sw["time"] = "year"
        df_sw["Region"] = df_sw["Region"].map(df_x["BCU_name"])
        df_sw2210 = df_sw[df_sw["year"] == 2100].copy()
        df_sw2210["year"] = 2110
        df_sw = pd.concat([df_sw, df_sw2210])
        df_sw = df_sw[df_sw["year"].isin(info.Y)]

        # Adding groundwater supply constraints
        # Reading data, the data is spatially and temprally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"qr_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
        )

        # Read groundwater data
        df_gw = pd.read_csv(path1)
        df_gw.drop(["Unnamed: 0"], axis=1, inplace=True)
        df_gw.index = df_x["BCU_name"].index
        df_gw = df_gw.stack().reset_index()
        df_gw.columns = pd.Index(["Region", "years", "value"])
        df_gw.fillna(0, inplace=True)
        df_gw.reset_index(drop=True, inplace=True)
        df_gw["year"] = pd.DatetimeIndex(df_gw["years"]).year
        df_gw["time"] = "year"
        df_gw["Region"] = df_gw["Region"].map(df_x["BCU_name"])
        df_gw2210 = df_gw[df_gw["year"] == 2100].copy()
        df_gw2210["year"] = 2110
        df_gw = pd.concat([df_gw, df_gw2210])
        df_gw = df_gw[df_gw["year"].isin(info.Y)]

    else:
        # Adding freshwater supply constraints
        # Reading data, the data is spatially and temprally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
        )
        df_sw = pd.read_csv(path1)
        df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)

        df_sw.index = df_x["BCU_name"].index
        df_sw = df_sw.stack().reset_index()
        df_sw.columns = pd.Index(["Region", "years", "value"])
        df_sw.sort_values(["Region", "years", "value"], inplace=True)
        df_sw.fillna(0, inplace=True)
        df_sw.reset_index(drop=True, inplace=True)
        df_sw["year"] = pd.DatetimeIndex(df_sw["years"]).year
        df_sw["time"] = pd.DatetimeIndex(df_sw["years"]).month
        df_sw["Region"] = df_sw["Region"].map(df_x["BCU_name"])
        df_sw2210 = df_sw[df_sw["year"] == 2100].copy()
        df_sw2210["year"] = 2110
        df_sw = pd.concat([df_sw, df_sw2210])
        df_sw = df_sw[df_sw["year"].isin(info.Y)]

        # Reading data, the data is spatially and temporally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"qr_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
        )
        df_gw = pd.read_csv(path1)
        df_gw.drop(["Unnamed: 0"], axis=1, inplace=True)

        df_gw.index = df_x["BCU_name"].index
        df_gw = df_gw.stack().reset_index()
        df_gw.columns = pd.Index(["Region", "years", "value"])
        df_gw.sort_values(["Region", "years", "value"], inplace=True)
        df_gw.fillna(0, inplace=True)
        df_gw.reset_index(drop=True, inplace=True)
        df_gw["year"] = pd.DatetimeIndex(df_gw["years"]).year
        df_gw["time"] = pd.DatetimeIndex(df_gw["years"]).month
        df_gw["Region"] = df_gw["Region"].map(df_x["BCU_name"])
        df_gw2210 = df_gw[df_gw["year"] == 2100].copy()
        df_gw2210["year"] = 2110
        df_gw = pd.concat([df_gw, df_gw2210])
        df_gw = df_gw[df_gw["year"].isin(info.Y)]

    return df_sw, df_gw


def add_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Adds water supply constraints

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """

    # define an empty dictionary
    results = {}
    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs

    df_sw, df_gw = read_water_availability(context)

    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=-df_sw["value"],
        unit="km3/year",
    )

    dmd_df = pd.concat(
        [
            dmd_df,
            make_df(
                "demand",
                node="B" + df_gw["Region"].astype(str),
                commodity="groundwater_basin",
                level="water_avail_basin",
                year=df_gw["year"],
                time=df_gw["time"],
                value=-df_gw["value"],
                unit="km3/year",
            ),
        ]
    )

    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x <= 0 else 0)

    results["demand"] = dmd_df

    # share constraint lower bound on groundwater
    df_share = make_df(
        "share_commodity_lo",
        shares="share_low_lim_GWat",
        node_share="B" + df_gw["Region"].astype(str),
        year_act=df_gw["year"],
        time=df_gw["time"],
        value=df_gw["value"]
        / (df_sw["value"] + df_gw["value"])
        * 0.95,  # 0.95 buffer factor to avoid numerical error
        unit="-",
    )

    df_share["value"] = df_share["value"].fillna(0)

    results["share_commodity_lo"] = df_share

    return results


def add_irrigation_demand(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Adds endogenous irrigation water demands from GLOBIOM emulator

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """
    # define an empty dictionary
    results = {}

    scen = context.get_scenario()
    # add water for irrigation from globiom
    land_out_1 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Cereals"}
    )
    land_out_1["level"] = "irr_cereal"
    land_out_2 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Oilcrops"}
    )
    land_out_2["level"] = "irr_oilcrops"
    land_out_3 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Sugarcrops"}
    )
    land_out_3["level"] = "irr_sugarcrops"

    land_out = pd.concat([land_out_1, land_out_2, land_out_3])
    land_out["commodity"] = "freshwater"

    land_out["value"] = 1e-3 * land_out["value"]

    # take land_out edited and add as a demand in  land_input
    results["land_input"] = land_out

    return results
