

# water & electricity for cooling technologies
@minimum_version("message_ix 3.7")
def cool_tech(context: "Context") -> dict[str, pd.DataFrame]:
    """Process cooling technology data for a scenario instance.
    The input values of parent technologies are read in from a scenario instance and
    then cooling fractions are calculated by using the data from
    ``tech_water_performance_ssp_msg.csv``.
    It adds cooling technologies as addons to the parent technologies. The
    nomenclature for cooling technology is <parenttechnologyname>__<coolingtype>.
    E.g: `coal_ppl__ot_fresh`

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
    #: Name of the input file.
    # The input file mentions water withdrawals and emission heating fractions for
    # cooling technologies alongwith parent technologies:
    FILE = "tech_water_performance_ssp_msg.csv"
    # Investment costs & regional shares of hist. activities of cooling
    # technologies
    FILE1 = (
        "cooltech_cost_and_shares_"
        + (f"ssp_msg_{context.regions}" if context.type_reg == "global" else "country")
        + ".csv"
    )

    # define an empty dictionary
    results = {}

    # Reference to the water configuration
    info = context["water build info"]
    sub_time = context.time

    # reading basin_delineation
    FILE2 = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE2)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )

    node_region = df_node["region"].unique()
    # reading ppl cooling tech dataframe
    path = package_data_path("water", "ppl_cooling_tech", FILE)
    df = pd.read_csv(path)
    cooling_df = df.loc[df["technology_group"] == "cooling"].copy()
    # Separate a column for parent technologies of respective cooling
    # techs
    cooling_df["parent_tech"] = (
        cooling_df["technology_name"]
        .apply(lambda x: pd.Series(str(x).split("__")))
        .drop(columns=1)
    )

    scen = context.get_scenario()

    # Extracting input database from scenario for parent technologies
    ref_input = scen.par("input", {"technology": cooling_df["parent_tech"]})
    # list of tec in cooling_df["parent_tech"] that are not in ref_input
    missing_tec = cooling_df["parent_tech"][
        ~cooling_df["parent_tech"].isin(ref_input["technology"])
    ]
    # some techs only have output, like csp
    ref_output = scen.par("output", {"technology": missing_tec})
    # set columns names of ref_output to be the same as ref_input
    ref_output.columns = ref_input.columns
    # merge ref_input and ref_output
    ref_input = pd.concat([ref_input, ref_output])

    ref_input[["value", "level"]] = ref_input.apply(missing_tech, axis=1)

    # Combines the input df of parent_tech with water withdrawal data
    input_cool = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_input.set_index("technology"))
        .reset_index()
    )

    # Drops NA values from the value column
    input_cool = input_cool.dropna(subset=["value"])

    # Convert year values into integers to be compatibel for model
    input_cool.year_vtg = input_cool.year_vtg.astype(int)
    input_cool.year_act = input_cool.year_act.astype(int)
    # Drops extra technologies from the data. backwards compatibility
    input_cool = input_cool[
        (input_cool["level"] != "water_supply") & (input_cool["level"] != "cooling")
    ]
    # heat plants need no cooling
    input_cool = input_cool[
        ~input_cool["technology_name"].str.contains("hpl", na=False)
    ]
    # Swap node_loc if node_loc equals "{context.regions}_GLB"
    input_cool.loc[input_cool["node_loc"] == f"{context.regions}_GLB", "node_loc"] = (
        input_cool["node_origin"]
    )
    # Swap node_origin if node_origin equals "{context.regions}_GLB"
    input_cool.loc[
        input_cool["node_origin"] == f"{context.regions}_GLB", "node_origin"
    ] = input_cool["node_loc"]

    input_cool["cooling_fraction"] = input_cool.apply(cooling_fr, axis=1)

    # Converting water withdrawal units to MCM/GWa
    # this refers to activity per cooling requirement (heat)
    input_cool["value_cool"] = (
        input_cool["water_withdrawal_mid_m3_per_output"]
        * 60
        * 60
        * 24
        * 365
        * 1e-6  # MCM
        / input_cool["cooling_fraction"]
    )
    # set to 1e-6 if value_cool is negative
    input_cool["value_cool"] = np.where(
        input_cool["value_cool"] < 0, 1e-6, input_cool["value_cool"]
    )
    input_cool["return_rate"] = 1 - (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )
    # consumption to be saved in emissions rates for reporting purposes
    input_cool["consumption_rate"] = (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )

    input_cool["value_return"] = input_cool["return_rate"] * input_cool["value_cool"]

    # only for reporting purposes
    input_cool["value_consumption"] = (
        input_cool["consumption_rate"] * input_cool["value_cool"]
    )

    # Filter out technologies that requires parasitic electricity
    electr = input_cool[input_cool["parasitic_electricity_demand_fraction"] > 0.0]

    # Make a new column 'value_cool' for calculating values against technologies
    electr["value_cool"] = (
        electr["parasitic_electricity_demand_fraction"] / electr["cooling_fraction"]
    )
    # set to 1e-6 if value_cool is negative
    electr["value_cool"] = np.where(
        electr["value_cool"] < 0, 1e-6, electr["value_cool"]
    )
    # Filters out technologies requiring saline water supply
    saline_df = input_cool[
        input_cool["technology_name"].str.endswith("ot_saline", na=False)
    ]

    # input_cool_minus_saline_elec_df
    con1 = input_cool["technology_name"].str.endswith("ot_saline", na=False)
    con2 = input_cool["technology_name"].str.endswith("air", na=False)
    icmse_df = input_cool[(~con1) & (~con2)]
    # electricity inputs
    inp = make_df(
        "input",
        node_loc=electr["node_loc"],
        technology=electr["technology_name"],
        year_vtg=electr["year_vtg"],
        year_act=electr["year_act"],
        mode=electr["mode"],
        node_origin=electr["node_origin"],
        commodity="electr",
        level="secondary",
        time="year",
        time_origin="year",
        value=electr["value_cool"],
        unit="GWa",
    )
    # once through and closed loop freshwater
    inp = pd.concat(
        [
            inp,
            make_df(
                "input",
                node_loc=icmse_df["node_loc"],
                technology=icmse_df["technology_name"],
                year_vtg=icmse_df["year_vtg"],
                year_act=icmse_df["year_act"],
                mode=icmse_df["mode"],
                node_origin=icmse_df["node_origin"],
                commodity="freshwater",
                level="water_supply",
                time="year",
                time_origin="year",
                value=icmse_df["value_cool"],
                unit="MCM/GWa",
            ),
        ]
    )
    # saline cooling technologies
    inp = pd.concat(
        [
            inp,
            make_df(
                "input",
                node_loc=saline_df["node_loc"],
                technology=saline_df["technology_name"],
                year_vtg=saline_df["year_vtg"],
                year_act=saline_df["year_act"],
                mode=saline_df["mode"],
                node_origin=saline_df["node_origin"],
                commodity="saline_ppl",
                level="saline_supply",
                time="year",
                time_origin="year",
                value=saline_df["value_cool"],
                unit="MCM/GWa",
            ),
        ]
    )

    # Drops NA values from the value column
    inp = inp.dropna(subset=["value"])

    # append the input data to results
    results["input"] = inp

    # add water consumption as emission factor, also for saline tecs
    emiss_df = input_cool[(~con2)]
    emi = make_df(
        "emission_factor",
        node_loc=emiss_df["node_loc"],
        technology=emiss_df["technology_name"],
        year_vtg=emiss_df["year_vtg"],
        year_act=emiss_df["year_act"],
        mode=emiss_df["mode"],
        emission="fresh_return",
        value=emiss_df["value_return"],
        unit="MCM/GWa",
    )

    results["emission_factor"] = emi

    # add output for share contraints to introduce SSP assumptions
    # also in the nexus case, the share contraints are at the macro-regions

    out = make_df(
        "output",
        node_loc=input_cool["node_loc"],
        technology=input_cool["technology_name"],
        year_vtg=input_cool["year_vtg"],
        year_act=input_cool["year_act"],
        mode=input_cool["mode"],
        node_dest=input_cool["node_origin"],
        commodity=input_cool["technology_name"].str.split("__").str[1],
        level="share",
        time="year",
        time_dest="year",
        value=1,
        unit="-",
    )

    # add water return flows for cooling tecs
    # Use share of basin availability to distribute the return flow from
    df_sw = map_basin_region_wat(context)
    df_sw.drop(columns={"mode", "date", "MSGREG"}, inplace=True)
    df_sw.rename(
        columns={"region": "node_dest", "time": "time_dest", "year": "year_act"},
        inplace=True,
    )
    df_sw["time_dest"] = df_sw["time_dest"].astype(str)
    if context.nexus_set == "nexus":
        for nn in icmse_df.node_loc.unique():
            # input cooling fresh basin
            icfb_df = icmse_df[icmse_df["node_loc"] == nn]
            bs = list(df_node[df_node["region"] == nn]["node"])

            out_t = (
                make_df(
                    "output",
                    node_loc=icfb_df["node_loc"],
                    technology=icfb_df["technology_name"],
                    year_vtg=icfb_df["year_vtg"],
                    year_act=icfb_df["year_act"],
                    mode=icfb_df["mode"],
                    # node_origin=icmse_df["node_origin"],
                    commodity="surfacewater_basin",
                    level="water_avail_basin",
                    time="year",
                    value=icfb_df["value_return"],
                    unit="MCM/GWa",
                )
                .pipe(broadcast, node_dest=bs, time_dest=sub_time)
                .merge(df_sw, how="left")
            )
            # multiply by basin water availability share
            out_t["value"] = out_t["value"] * out_t["share"]
            out_t.drop(columns={"share"}, inplace=True)
            out = pd.concat([out, out_t])

        out = out.dropna(subset=["value"])
        out.reset_index(drop=True, inplace=True)
    # in any case save out into results
    results["output"] = out

    # costs and historical parameters
    path1 = package_data_path("water", "ppl_cooling_tech", FILE1)
    cost = pd.read_csv(path1)
    # Combine technology name to get full cooling tech names
    cost["technology"] = cost["utype"] + "__" + cost["cooling"]
    cost["share"] = cost["utype"] + "_" + cost["cooling"]

    # add contraint with relation, based on shares
    # Keep only "utype", "technology", and columns starting with "mix_"
    share_filtered = cost.loc[
        :,
        ["utype", "share"] + [col for col in cost.columns if col.startswith("mix_")],
    ]

    # Melt to long format
    share_long = share_filtered.melt(
        id_vars=["utype", "share"], var_name="node_share", value_name="value"
    )
    # filter with uypt in cooling_df["parent_tech"]
    share_long = share_long[
        share_long["utype"].isin(input_cool["parent_tech"].unique())
    ].reset_index(drop=True)

    # Remove "mix_" prefix from region names
    share_long["node_share"] = share_long["node_share"].str.replace(
        "mix_", "", regex=False
    )
    # Replace 0 values with 0.0001
    share_long["value"] = share_long["value"] * 1.05  # give some flexibility
    share_long["value"] = share_long["value"].replace(0, 0.0001)

    share_long["shares"] = "share_calib_" + share_long["share"]
    share_long.drop(columns={"utype", "share"}, inplace=True)
    # restore cost as it was for future use
    cost.drop(columns="share", inplace=True)
    share_long["time"] = "year"
    share_long["unit"] = "-"
    share_calib = share_long.copy()
    # Expand for years [2020, 2025]
    share_calib = share_calib.loc[share_calib.index.repeat(2)].reset_index(drop=True)
    years_calib = [2020, 2025]
    share_calib["year_act"] = years_calib * (len(share_calib) // 2)
    # take year in info.N but not years_calib
    years_fut = [year for year in info.Y if year not in years_calib]
    share_fut = share_long.copy()
    share_fut = share_fut.loc[share_fut.index.repeat(len(years_fut))].reset_index(
        drop=True
    )
    share_fut["year_act"] = years_fut * (len(share_fut) // len(years_fut))
    # filter only shares that contain "ot_saline"
    share_fut = share_fut[share_fut["shares"].str.contains("ot_saline")]
    # if value < 0.4 set to 0.4, not so allow too much saline where there is no
    share_fut["value"] = np.where(share_fut["value"] < 0.45, 0.45, share_fut["value"])
    # keep only after 2050
    share_fut = share_fut[share_fut["year_act"] >= 2050]
    # append share_calib and (share_fut only to add constraints on ot_saline)
    results["share_commodity_up"] = pd.concat([share_calib])

    # Filtering out 2015 data to use for historical values
    input_cool_2015 = input_cool[
        (input_cool["year_act"] == 2015) & (input_cool["year_vtg"] == 2015)
    ]
    # set of parent_tech and node_loc for input_cool
    input_cool_set = set(zip(input_cool["parent_tech"], input_cool["node_loc"]))
    year_list = [2020, 2010, 2030, 2050, 2000, 2080, 1990]

    for year in year_list:
        print(year)
        # Identify missing combinations in the current aggregate
        input_cool_2015_set = set(
            zip(input_cool_2015["parent_tech"], input_cool_2015["node_loc"])
        )
        missing_combinations = input_cool_set - input_cool_2015_set

        if not missing_combinations:
            break  # Stop if no missing combinations remain

        # Extract missing rows from input_cool with the current year
        missing_rows = input_cool[
            (input_cool["year_act"] == year)
            & (input_cool["year_vtg"] == year)
            & input_cool.apply(
                lambda row: (row["parent_tech"], row["node_loc"])
                in missing_combinations,
                axis=1,
            )
        ]

        if not missing_rows.empty:
            # Modify year columns to 2015
            missing_rows = missing_rows.copy()
            missing_rows["year_act"] = 2015
            missing_rows["year_vtg"] = 2015

            # Append to the aggregated dataset
            input_cool_2015 = pd.concat(
                [input_cool_2015, missing_rows], ignore_index=True
            )

    # Final check if there are still missing combinations
    input_cool_2015_set = set(
        zip(input_cool_2015["parent_tech"], input_cool_2015["node_loc"])
    )
    still_missing = input_cool_set - input_cool_2015_set

    if still_missing:
        print(
            f"Warning: Some combinations are still missing even after trying all years: {still_missing}"
        )

    # Filter out columns that contain 'mix' in column name
    # Rename column names to match with the previous df
    cost.rename(columns=lambda name: name.replace("mix_", ""), inplace=True)
    search_cols = [
        col
        for col in cost.columns
        if context.regions in col or col in ["technology", "utype"]
    ]
    hold_df = input_cool_2015[
        ["node_loc", "technology_name", "cooling_fraction"]
    ].drop_duplicates()
    search_cols_cooling_fraction = [
        col for col in search_cols if col not in ["technology", "utype"]
    ]
    # multiplication factor with cooling factor and shares
    hold_cost = cost[search_cols].apply(
        shares,
        axis=1,
        context=context,
        search_cols_cooling_fraction=search_cols_cooling_fraction,
        hold_df=hold_df,
        search_cols=search_cols,
    )

    # hist cap to be divided by cap_factor of the parent tec
    cap_fact_parent = scen.par(
        "capacity_factor", {"technology": cooling_df["parent_tech"]}
    )
    # cap_fact_parent = cap_fact_parent[
    #     (cap_fact_parent["node_loc"] == "R12_NAM")
    #     & (cap_fact_parent["technology"] == "coal_ppl_u") # nuc_lc
    # ]
    # keep node_loc, technology , year_vtg and value
    cap_fact_parent1 = cap_fact_parent[
        ["node_loc", "technology", "year_vtg", "value"]
    ].drop_duplicates()
    cap_fact_parent1 = cap_fact_parent1[
        cap_fact_parent1["year_vtg"] < scen.firstmodelyear
    ]
    # group by "node_loc", "technology", "year_vtg" and get the minimum value
    cap_fact_parent1 = cap_fact_parent1.groupby(
        ["node_loc", "technology", "year_vtg"], as_index=False
    ).min()
    # filter for values that have year_act < sc.firstmodelyear

    # in some cases the capacity parameters are used with year_all
    # (e.g. initial_new_capacity_up). need year_Act for this
    cap_fact_parent2 = cap_fact_parent[
        ["node_loc", "technology", "year_act", "value"]
    ].drop_duplicates()
    cap_fact_parent2 = cap_fact_parent2[
        cap_fact_parent2["year_act"] >= scen.firstmodelyear
    ]
    # group by "node_loc", "technology", "year_vtg" and get the minimum value
    cap_fact_parent2 = cap_fact_parent2.groupby(
        ["node_loc", "technology", "year_act"], as_index=False
    ).min()
    cap_fact_parent2.rename(columns={"year_act": "year_vtg"}, inplace=True)

    cap_fact_parent = pd.concat([cap_fact_parent1, cap_fact_parent2])

    # rename value to cap_fact
    cap_fact_parent.rename(
        columns={"value": "cap_fact", "technology": "utype"}, inplace=True
    )

    # Manually removing extra technologies not required
    # TODO make it automatic to not include the names manually
    techs_to_remove = [
        "mw_ppl__ot_fresh",
        "mw_ppl__ot_saline",
        "mw_ppl__cl_fresh",
        "mw_ppl__air",
        "nuc_fbr__ot_fresh",
        "nuc_fbr__ot_saline",
        "nuc_fbr__cl_fresh",
        "nuc_fbr__air",
        "nuc_htemp__ot_fresh",
        "nuc_htemp__ot_saline",
        "nuc_htemp__cl_fresh",
        "nuc_htemp__air",
    ]

    from message_ix_models.tools.costs.config import Config
    from message_ix_models.tools.costs.projections import create_cost_projections

    # Set config for cost projections
    # Using GDP method for cost projections
    cfg = Config(
        module="cooling", scenario=context.ssp, method="gdp", node=context.regions
    )

    # Get projected investment and fixed o&m costs
    cost_proj = create_cost_projections(cfg)

    # Get only the investment costs for cooling technologies
    inv_cost = cost_proj["inv_cost"][
        ["year_vtg", "node_loc", "technology", "value", "unit"]
    ]

    # Remove technologies that are not required
    inv_cost = inv_cost[~inv_cost["technology"].isin(techs_to_remove)]

    # Only keep cooling module technologies by filtering for technologies with "__"
    inv_cost = inv_cost[inv_cost["technology"].str.contains("__")]

    # Add the investment costs to the results
    results["inv_cost"] = inv_cost

    # Addon conversion
    adon_df = input_cool.copy()
    # Add 'cooling_' before name of parent technologies that are type_addon
    # nomenclature
    adon_df["tech"] = "cooling__" + adon_df["parent_tech"].astype(str)
    # technology : 'parent technology' and type_addon is type of addons such
    # as 'cooling__bio_hpl'
    addon_df = make_df(
        "addon_conversion",
        node=adon_df["node_loc"],
        technology=adon_df["parent_tech"],
        year_vtg=adon_df["year_vtg"],
        year_act=adon_df["year_act"],
        mode=adon_df["mode"],
        time="year",
        type_addon=adon_df["tech"],
        value=adon_df["cooling_fraction"],
        unit="-",  # from electricity to thermal
    )

    results["addon_conversion"] = addon_df

    # Addon_lo will remain 1 for all cooling techs so it allows 100% activity of
    # parent technologies
    addon_lo = make_matched_dfs(addon_df, addon_lo=1)
    results["addon_lo"] = addon_lo["addon_lo"]

    # technical lifetime
    year = info.yv_ya.year_vtg.drop_duplicates()
    year = year[year >= 1990]

    tl = (
        make_df(
            "technical_lifetime",
            technology=inp["technology"].drop_duplicates(),
            value=30,
            unit="year",
        )
        .pipe(broadcast, year_vtg=year, node_loc=node_region)
        .pipe(same_node)
    )

    results["technical_lifetime"] = tl

    cap_fact = make_matched_dfs(inp, capacity_factor=1)
    # Climate Impacts on freshwater cooling capacity
    # Taken from
    # https://www.sciencedirect.com/science/article/
    #  pii/S0959378016301236?via%3Dihub#sec0080
    if context.RCP == "no_climate":
        df = cap_fact["capacity_factor"]
    else:
        df = cap_fact["capacity_factor"]
        # reading ppl cooling impact dataframe
        path = package_data_path(
            "water", "ppl_cooling_tech", "power_plant_cooling_impact_MESSAGE.xlsx"
        )
        df_impact = pd.read_excel(path, sheet_name=f"{context.regions}_{context.RCP}")

        for n in df_impact["node"]:
            conditions = [
                df["technology"].str.contains("fresh")
                & (df["year_act"] >= 2025)
                & (df["year_act"] < 2050)
                & (df["node_loc"] == n),
                df["technology"].str.contains("fresh")
                & (df["year_act"] >= 2050)
                & (df["year_act"] < 2070)
                & (df["node_loc"] == n),
                df["technology"].str.contains("fresh")
                & (df["year_act"] >= 2070)
                & (df["node_loc"] == n),
            ]

            choices = [
                df_impact[(df_impact["node"] == n)]["2025s"],
                df_impact[(df_impact["node"] == n)]["2050s"],
                df_impact[(df_impact["node"] == n)]["2070s"],
            ]

            df["value"] = np.select(conditions, choices, default=df["value"])

    results["capacity_factor"] = df

    # Extract inand expand some paramenters from parent technologies
    # Define parameter names to be extracted
    param_names = [
        "historical_activity",
        "historical_new_capacity",
        "initial_activity_up",
        "initial_activity_lo",
        "initial_new_capacity_up",
        "soft_activity_up",
        "soft_activity_lo",
        "soft_new_capacity_up",
        "level_cost_activity_soft_up",
        "level_cost_activity_soft_lo",
        "growth_activity_lo",
        "growth_activity_up",
        "growth_new_capacity_up",
    ]

    multip_list = [
        "historical_activity",
        "historical_new_capacity",
        "initial_activity_up",
        "initial_activity_lo",
        "initial_new_capacity_up",
    ]

    # Extract parameters dynamically
    list_params = [
        (scen.par(p, {"technology": cooling_df["parent_tech"]}), p) for p in param_names
    ]

    # Expand parameters for cooling technologies
    suffixes = ["__ot_fresh", "__cl_fresh", "__air", "__ot_saline"]

    for df, param_name in list_params:
        df_param = pd.DataFrame()
        for suffix in suffixes:
            df_add = df.copy()
            df_add["technology"] = df_add["technology"] + suffix
            df_param = pd.concat([df_param, df_add])

        if param_name in multip_list:
            df_param_share = apply_act_cap_multiplier(
                df_param, hold_cost, cap_fact_parent, param_name
            )
        else:
            df_param_share = df_param

        results[param_name] = pd.concat(
            [results.get(param_name, pd.DataFrame()), df_param_share], ignore_index=True
        )

    # add share constraints for cooling technologies based on SSP assumptions
    df_share = cooling_shares_SSP_from_yaml(context)

    if not df_share.empty:
        # pd concat to the existing results["share_commodity_up"]
        results["share_commodity_up"] = pd.concat(
            [results["share_commodity_up"], df_share], ignore_index=True
        )

    return results

