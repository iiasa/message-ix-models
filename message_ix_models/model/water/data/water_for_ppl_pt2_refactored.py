



def cool_tech(context: "Context") -> dict[str, pd.DataFrame]:
    """
    Process cooling technology data for a scenario instance.
    
    This refactored version delegates individual tasks to helper functions.
    """
    results = {}
    
    # Load input data and parent scenario values.
    input_cool = load_and_prepare_input(context)
    
    # Compute emission factors.
    results["emission_factor"] = compute_emission_factors(context, input_cool)
    
    # Create input, output, and cost data frames.
    results["input"] = generate_input_df(context, input_cool)
    results["output"] = generate_output_df(context, input_cool)
    results["inv_cost"] = generate_investment_costs(context)
    
    # Process addon conversions and bounds.
    addon_conv = compute_addon_conversion(context, input_cool)
    results["addon_conversion"] = addon_conv
    results["addon_lo"] = compute_addon_bounds(addon_conv)
    
    # Process technical lifetime.
    results["technical_lifetime"] = compute_technical_lifetime(context)
    
    # Calculate capacity factors and apply climate impacts.
    results["capacity_factor"] = compute_capacity_factor(context)
    
    # Process parameters from parent technologies.
    params = process_parent_parameters(context, input_cool)
    results.update(params)
    
    # Process share constraints (if applicable).
    share_df = compute_share_constraints(context)
    if not share_df.empty:
        results["share_commodity_up"] = merge_share_constraints(
            existing_df=results.get("share_commodity_up"), new_df=share_df
        )
    
    return results


def load_and_prepare_input(context: "Context") -> pd.DataFrame:
    """
    Loads the cooling technology input file, merges scenario input/output
    for parent technologies, cleans the data and computes cooling fractions.
    """
    # Build file paths.
    file_cooling = "tech_water_performance_ssp_msg.csv"
    path_cooling = package_data_path("water", "ppl_cooling_tech", file_cooling)
    
    # Read cooling tech data.
    df = pd.read_csv(path_cooling)
    cooling_df = df.loc[df["technology_group"] == "cooling"].copy()
    
    # Compute parent technology names.
    cooling_df["parent_tech"] = cooling_df["technology_name"].apply(
        lambda x: str(x).split("__")[0]
    )
    
    # Get parent technology inputs from the scenario.
    scen = context.get_scenario()
    ref_input = scen.par("input", {"technology": cooling_df["parent_tech"]})
    
    # Identify missing parent techs, get outputs and merge.
    missing_tech = cooling_df["parent_tech"][~cooling_df["parent_tech"].isin(ref_input["technology"])]
    if not missing_tech.empty:
        ref_output = scen.par("output", {"technology": missing_tech})
        ref_output.columns = ref_input.columns
        ref_input = pd.concat([ref_input, ref_output])
    
    # Apply a helper function to process missing tech values.
    ref_input[["value", "level"]] = ref_input.apply(missing_tech, axis=1)
    
    # Merge cooling data with parent tech info.
    merged = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_input.set_index("technology"))
        .reset_index()
    )
    merged = merged.dropna(subset=["value"])
    merged["year_vtg"] = merged["year_vtg"].astype(int)
    merged["year_act"] = merged["year_act"].astype(int)
    
    # Exclude rows with specific levels and patterns.
    merged = merged[
        (merged["level"] != "water_supply") & (merged["level"] != "cooling")
    ]
    merged = merged[~merged["technology_name"].str.contains("hpl", na=False)]
    
    # Adjust node values (swapping if needed).
    merged.loc[merged["node_loc"] == f"{context.regions}_GLB", "node_loc"] = merged["node_origin"]
    merged.loc[merged["node_origin"] == f"{context.regions}_GLB", "node_origin"] = merged["node_loc"]
    
    # Apply cooling fraction calculations.
    merged["cooling_fraction"] = merged.apply(cooling_fr, axis=1)
    
    # Return the cleaned and merged input dataframe.
    return merged


def compute_emission_factors(context: "Context", input_cool: pd.DataFrame) -> pd.DataFrame:
    """
    Compute water return as emission factors based on input data.
    """
    emiss_df = input_cool.copy()
    emi = make_df(
        "emission_factor",
        node_loc=emiss_df["node_loc"],
        technology=emiss_df["technology_name"],
        year_vtg=emiss_df["year_vtg"],
        year_act=emiss_df["year_act"],
        mode=emiss_df["mode"],
        emission="fresh_return",
        value=emiss_df["return_rate"],
        unit="MCM/GWa",
    )
    return emi


def generate_input_df(context: "Context", input_cool: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the combined input DataFrame for cooling technologies using electricity
    and freshwater sources.
    """
    # Separate technologies needing electricity.
    electr = input_cool[input_cool["parasitic_electricity_demand_fraction"] > 0.0].copy()
    electr["value_cool"] = electr["parasitic_electricity_demand_fraction"] / electr["cooling_fraction"]
    electr["value_cool"] = np.where(electr["value_cool"] < 0, 1e-6, electr["value_cool"])
    
    # Build first input dataset for electricity.
    inp_elect = make_df(
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
    
    # For non-electric cooling, use freshwater.
    icmse = input_cool[~input_cool["technology_name"].str.contains("ot_saline", na=False) &
                        ~input_cool["technology_name"].str.contains("air", na=False)]
    inp_fresh = make_df(
        "input",
        node_loc=icmse["node_loc"],
        technology=icmse["technology_name"],
        year_vtg=icmse["year_vtg"],
        year_act=icmse["year_act"],
        mode=icmse["mode"],
        node_origin=icmse["node_origin"],
        commodity="freshwater",
        level="water_supply",
        time="year",
        time_origin="year",
        value=icmse["value_cool"],
        unit="MCM/GWa",
    )
    
    # Similarly, process saline cooling data.
    saline = input_cool[input_cool["technology_name"].str.endswith("ot_saline", na=False)]
    inp_saline = make_df(
        "input",
        node_loc=saline["node_loc"],
        technology=saline["technology_name"],
        year_vtg=saline["year_vtg"],
        year_act=saline["year_act"],
        mode=saline["mode"],
        node_origin=saline["node_origin"],
        commodity="saline_ppl",
        level="saline_supply",
        time="year",
        time_origin="year",
        value=saline["value_cool"],
        unit="MCM/GWa",
    )
    
    # Merge and drop NAs.
    df_input = pd.concat([inp_elect, inp_fresh, inp_saline])
    return df_input.dropna(subset=["value"])


def generate_output_df(context: "Context", input_cool: pd.DataFrame) -> pd.DataFrame:
    """
    Generates the output dataframe with shares and water return flows.
    """
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
    
    # Process basin water return flows.
    df_sw = map_basin_region_wat(context)
    df_sw.drop(columns={"mode", "date", "MSGREG"}, inplace=True)
    df_sw.rename(
        columns={"region": "node_dest", "time": "time_dest", "year": "year_act"}, inplace=True
    )
    df_sw["time_dest"] = df_sw["time_dest"].astype(str)
    
    # For nexus scenario, broadcast water return flows.
    if context.nexus_set == "nexus":
        # TODO: Replace the loop with a vectorized join if possible.
        unique_nodes = input_cool["node_loc"].unique()
        for n in unique_nodes:
            tech_df = input_cool[input_cool["node_loc"] == n]
            basin_nodes = list(df_sw[df_sw["node_dest"] == n]["node_dest"])
            out_temp = (
                make_df(
                    "output",
                    node_loc=tech_df["node_loc"],
                    technology=tech_df["technology_name"],
                    year_vtg=tech_df["year_vtg"],
                    year_act=tech_df["year_act"],
                    mode=tech_df["mode"],
                    commodity="surfacewater_basin",
                    level="water_avail_basin",
                    time="year",
                    value=tech_df["value_return"],
                    unit="MCM/GWa",
                )
                .pipe(broadcast, node_dest=basin_nodes, time_dest=context.time)
                .merge(df_sw, how="left")
            )
            out_temp["value"] = out_temp["value"] * out_temp["share"]
            out_temp.drop(columns={"share"}, inplace=True)
            out = pd.concat([out, out_temp])
        out = out.dropna(subset=["value"]).reset_index(drop=True)
    
    return out


def generate_investment_costs(context: "Context") -> pd.DataFrame:
    """
    Loads and processes cooling technology investment costs.
    """
    file_cost = (
        "cooltech_cost_and_shares_"
        + (f"ssp_msg_{context.regions}" if context.type_reg == "global" else "country")
        + ".csv"
    )
    path_cost = package_data_path("water", "ppl_cooling_tech", file_cost)
    cost = pd.read_csv(path_cost)
    cost["technology"] = cost["utype"] + "__" + cost["cooling"]
    # Remove technologies not required.
    techs_to_remove = [
        "mw_ppl__ot_fresh", "mw_ppl__ot_saline", "mw_ppl__cl_fresh",
        "mw_ppl__air", "nuc_fbr__ot_fresh", "nuc_fbr__ot_saline",
        "nuc_fbr__cl_fresh", "nuc_fbr__air", "nuc_htemp__ot_fresh",
        "nuc_htemp__ot_saline", "nuc_htemp__cl_fresh", "nuc_htemp__air",
    ]
    cost = cost[~cost["technology"].isin(techs_to_remove)]
    cost = cost[cost["technology"].str.contains("__")]
    return cost


def compute_addon_conversion(context: "Context", input_cool: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the addon conversion dataframe.
    """
    adon = input_cool.copy()
    adon["tech"] = "cooling__" + adon["parent_tech"].astype(str)
    addon_df = make_df(
        "addon_conversion",
        node=adon["node_loc"],
        technology=adon["parent_tech"],
        year_vtg=adon["year_vtg"],
        year_act=adon["year_act"],
        mode=adon["mode"],
        time="year",
        type_addon=adon["tech"],
        value=adon["cooling_fraction"],
        unit="-",
    )
    return addon_df


def compute_addon_bounds(addon_df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces lower bounds for addon conversions.
    """
    return make_matched_dfs(addon_df, addon_lo=1)["addon_lo"]


def compute_technical_lifetime(context: "Context") -> pd.DataFrame:
    """
    Creates a technical lifetime dataframe.
    """
    # For example, assume a 30-year lifetime for all technologies.
    node_region = pd.read_csv(package_data_path("water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"))["BCU_name"]
    year = context["water build info"].yv_ya.year_vtg.drop_duplicates()
    year = year[year >= 1990]
    return (
        make_df("technical_lifetime",
                technology=pd.Series(),  # List technologies as needed.
                value=30,
                unit="year")
        .pipe(broadcast, year_vtg=year, node_loc=node_region)
        .pipe(same_node)
    )


def compute_capacity_factor(context: "Context") -> pd.DataFrame:
    """
    Computes capacity factors and applies climate impacts.
    """
    cap_fact = make_matched_dfs(generate_input_df(context, load_and_prepare_input(context)), capacity_factor=1)["capacity_factor"]
    if context.RCP != "no_climate":
        # Read and apply climate impact data.
        path = package_data_path(
            "water", "ppl_cooling_tech", "power_plant_cooling_impact_MESSAGE.xlsx"
        )
        df_impact = pd.read_excel(path, sheet_name=f"{context.regions}_{context.RCP}")
        # TODO: Apply df_impact adjustments to cap_fact.
    return cap_fact


def process_parent_parameters(context: "Context", input_cool: pd.DataFrame) -> dict:
    """
    Extracts various parameters from parent technologies, applies multipliers,
    and expands them as required.
    """
    scen = context.get_scenario()
    param_names = [
        "historical_activity", "historical_new_capacity",
        "initial_activity_up", "initial_activity_lo", "initial_new_capacity_up",
        "soft_activity_up", "soft_activity_lo", "soft_new_capacity_up",
        "level_cost_activity_soft_up", "level_cost_activity_soft_lo",
        "growth_activity_lo", "growth_activity_up", "growth_new_capacity_up",
    ]
    multip_list = [
        "historical_activity", "historical_new_capacity",
        "initial_activity_up", "initial_activity_lo", "initial_new_capacity_up",
    ]
    results = {}
    for p in param_names:
        df_param = scen.par(p, {"technology": input_cool["parent_tech"]})
        # Expand for various cooling type suffixes.
        suffixes = ["__ot_fresh", "__cl_fresh", "__air", "__ot_saline"]
        expanded = [df_param.assign(technology=lambda d: d["technology"] + s) for s in suffixes]
        df_param_full = pd.concat(expanded)
        if p in multip_list:
            df_param_full = apply_act_cap_multiplier(
                df_param_full,
                hold_df=pd.DataFrame(),  # adjust as needed,
                cap_fact_parent=pd.DataFrame(),  # adjust as needed,
                param_name=p,
            )
        results[p] = df_param_full
    return results


def compute_share_constraints(context: "Context") -> pd.DataFrame:
    """
    Reads share constraints from YAML (or another source) and returns a DataFrame.
    """
    df_share = cooling_shares_SSP_from_yaml(context)
    return df_share


def merge_share_constraints(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges or concatenates share constraints data frames.
    """
    if existing_df is not None:
        return pd.concat([existing_df, new_df], ignore_index=True)
    return new_df