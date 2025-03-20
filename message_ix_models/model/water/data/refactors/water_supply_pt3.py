

def add_e_flow(context: "Context") -> dict[str, pd.DataFrame]:
    """Add environmental flows
    This function bounds the available water and allocates the environmental
    flows.Environmental flow bounds are calculated using Variable Monthly Flow
    (VMF) method. The VMF method is applied to wet and dry seasonal runoff
    values. These wet and dry seasonal values are then aggregated to annual
    values.Environmental flows in the model will be incorporated as bounds on
    'return_flow' technology. The lower bound on this technology will ensure
    that certain amount of water remain

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["water build info"]``, plus the additional year 2010.
    """
    # define an empty dictionary
    results = {}

    info = context["water build info"]

    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs
    df_sw, df_gw = read_water_availability(context)

    # reading sample for assiging basins
    PATH = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)

    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=df_sw["value"],
        unit="km3/year",
    )
    dmd_df = dmd_df[dmd_df["year"] >= 2025].reset_index(drop=True)
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x >= 0 else 0)

    if "year" in context.time:
        # Reading data, the data is spatially and temporally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"e-flow_{context.RCP}_{context.regions}.csv",
        )
        df_env = pd.read_csv(path1)
        df_env.drop(["Unnamed: 0"], axis=1, inplace=True)
        df_env.index = df_x["BCU_name"].index
        df_env = df_env.stack().reset_index()
        df_env.columns = pd.Index(["Region", "years", "value"])
        df_env.sort_values(["Region", "years", "value"], inplace=True)
        df_env.fillna(0, inplace=True)
        df_env.reset_index(drop=True, inplace=True)
        df_env["year"] = pd.DatetimeIndex(df_env["years"]).year
        df_env["time"] = "year"
        df_env["Region"] = df_env["Region"].map(df_x["BCU_name"])
        df_env2210 = df_env[df_env["year"] == 2100].copy()
        df_env2210["year"] = 2110
        df_env = pd.concat([df_env, df_env2210])
        df_env = df_env[df_env["year"].isin(info.Y)]
    else:
        # Reading data, the data is spatially and temporally aggregated from GHMs
        path1 = package_data_path(
            "water",
            "availability",
            f"e-flow_5y_m_{context.RCP}_{context.regions}.csv",
        )
        df_env = pd.read_csv(path1)
        df_env.drop(["Unnamed: 0"], axis=1, inplace=True)
        # new_cols = pd.to_datetime(df_env.columns, format="%Y/%m/%d")
        # df_env.columns = new_cols
        df_env.index = df_x["BCU_name"].index
        df_env = df_env.stack().reset_index()
        df_env.columns = pd.Index(["Region", "years", "value"])
        df_env.sort_values(["Region", "years", "value"], inplace=True)
        df_env.fillna(0, inplace=True)
        df_env.reset_index(drop=True, inplace=True)
        df_env["year"] = pd.DatetimeIndex(df_env["years"]).year
        df_env["time"] = pd.DatetimeIndex(df_env["years"]).month
        df_env["Region"] = df_env["Region"].map(df_x["BCU_name"])
        df_env2210 = df_env[df_env["year"] == 2100].copy()
        df_env2210["year"] = 2110
        df_env = pd.concat([df_env, df_env2210])
        df_env = df_env[df_env["year"].isin(info.Y)]

    # Return a processed dataframe for env flow calculations
    if context.SDG != "baseline":
        # dataframe to put constraints on env flows
        eflow_df = make_df(
            "bound_activity_lo",
            node_loc="B" + df_env["Region"].astype(str),
            technology="return_flow",
            year_act=df_env["year"],
            mode="M1",
            time=df_env["time"],
            value=df_env["value"],
            unit="km3/year",
        )

        eflow_df["value"] = eflow_df["value"].apply(lambda x: x if x >= 0 else 0)
        eflow_df = eflow_df[eflow_df["year_act"] >= 2025].reset_index(drop=True)

        dmd_df.sort_values(by=["node", "year"], inplace=True)
        dmd_df.reset_index(drop=True, inplace=True)
        eflow_df.sort_values(by=["node_loc", "year_act"], inplace=True)
        eflow_df.reset_index(drop=True, inplace=True)

        eflow_df["value"] = np.where(
            eflow_df["value"] >= 0.7 * dmd_df["value"],
            0.7 * dmd_df["value"],
            eflow_df["value"],
        )

        results["bound_activity_lo"] = eflow_df

    return results