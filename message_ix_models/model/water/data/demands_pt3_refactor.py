@minimum_version("message_ix 3.7")
def add_sectoral_demands(context: "Context") -> dict[str, pd.DataFrame]:
    # facade function wrapping modular water demand processing

    results = {}
    info = context["water build info"]
    region = f"{context.regions}"
    sub_time = context.time

    # load and transform yearly csv data
    df_dmds = _load_yearly_csvs(context, region)
    # apply monthly adjustments for sub-annual timesteps
    df_dmds = _apply_subannual_adjustments(df_dmds, context, region)
    # extract data components for different variables
    comps = _extract_components(df_dmds)
    # adjust rates based on policy configuration if needed
    comps = _apply_sdg_adjustments(comps, df_dmds, context)

    # calculate urban and rural water demands (connected and disconnected)
    urban_mw = _calc_demand(
        comps["urban_withdrawal"], comps["urban_connection_rate"], "urban_mw"
    )
    urban_dis = _calc_demand(
        comps["urban_withdrawal"], comps["urban_connection_rate"], "urban_disconnected", connected=False
    )
    rural_mw = _calc_demand(
        comps["rural_withdrawal"], comps["rural_connection_rate"], "rural_mw"
    )
    rural_dis = _calc_demand(
        comps["rural_withdrawal"], comps["rural_connection_rate"], "rural_disconnected", connected=False
    )

    # industrial water demand and return (no rate merge for withdrawal)
    manuf_mw = _calc_industrial_demand(
        comps["manufacturing_withdrawal"], "industry_mw"
    )
    manuf_uncollected_wst = _calc_industrial_return(
        comps["manufacturing_return"], "industry_uncollected_wst"
    )

    # wastewater: merge with treatment rate and apply sign convention
    urban_collected_wst = _calc_wastewater(
        comps["urban_return"], comps["urban_treatment_rate"], "urban_collected_wst", collected=True
    )
    rural_collected_wst = _calc_wastewater(
        comps["rural_return"], comps["rural_treatment_rate"], "rural_collected_wst", collected=True
    )
    urban_uncollected_wst = _calc_wastewater(
        comps["urban_return"], comps["urban_treatment_rate"], "urban_uncollected_wst", collected=False
    )
    rural_uncollected_wst = _calc_wastewater(
        comps["rural_return"], comps["rural_treatment_rate"], "rural_uncollected_wst", collected=False
    )

    dmd_df = pd.concat(
        [
            urban_mw,
            urban_dis,
            rural_mw,
            rural_dis,
            manuf_mw,
            manuf_uncollected_wst,
            urban_collected_wst,
            rural_collected_wst,
            urban_uncollected_wst,
            rural_uncollected_wst,
        ]
    )
    dmd_df = dmd_df[dmd_df["year"].isin(info.Y)]
    results["demand"] = dmd_df

    hist_act, hist_cap = _assemble_historical_data(dmd_df, info)
    results["historical_activity"] = hist_act
    results["historical_new_capacity"] = hist_cap

    df_share_wat = _generate_share_constraint(comps["recycling"], sub_time, info)
    results["share_commodity_lo"] = df_share_wat

    return results


# ---------------- helper functions below ----------------

def _load_yearly_csvs(context, region) -> pd.DataFrame:
    # read in and process yearly csv files
    path = package_data_path("water", "demands", "harmonized", region, ".")
    list_of_csvs = list(path.glob("ssp2_regional_*.csv"))
    fns = [os.path.splitext(os.path.basename(x))[0] for x in list_of_csvs]
    fns = " ".join(fns).replace("ssp2_regional_", "").split()
    d = {}
    for i in range(len(fns)):
        d[fns[i]] = pd.read_csv(list_of_csvs[i])
    dfs = {}
    for key, df in d.items():
        df.rename(columns={"Unnamed: 0": "year"}, inplace=True)
        df.set_index("year", inplace=True)
        dfs[key] = df
    df_x = xr.Dataset(dfs).to_array()
    df_x_interp = df_x.interp(year=[2015, 2025, 2035, 2045, 2055])
    df_x_c = df_x.combine_first(df_x_interp)
    df_f = df_x_c.to_dataframe("").unstack()
    df_dmds = df_f.stack(future_stack=True).reset_index(level=0).reset_index()
    df_dmds.columns = ["year", "node", "variable", "value"]
    df_dmds.sort_values(["year", "node", "variable", "value"], inplace=True)
    df_dmds["time"] = "year"
    return df_dmds


def _apply_subannual_adjustments(df_dmds, context, region) -> pd.DataFrame:
    # if time steps are sub-annual, adjust by merging monthly data
    if "year" not in context.time:
        PATH = package_data_path("water", "demands", "harmonized", region, "ssp2_m_water_demands.csv")
        df_m = pd.read_csv(PATH)
        df_m.value *= 30  # mcm/day to mcm/month
        df_m.loc[df_m["sector"] == "industry", "sector"] = "manufacturing"
        df_m["variable"] = df_m["sector"] + "_" + df_m["type"] + "_baseline"
        df_m.loc[df_m["variable"] == "urban_withdrawal_baseline", "variable"] = "urbann_withdrawal2_baseline"
        df_m.loc[df_m["variable"] == "urban_return_baseline", "variable"] = "urbann_return2_baseline"
        df_m = df_m[["year", "pid", "variable", "value", "month"]]
        df_m.columns = pd.Index(["year", "node", "variable", "value", "time"])
        df_dmds = df_dmds[
            ~df_dmds["variable"].isin([
                "urban_withdrawal2_baseline",
                "rural_withdrawal_baseline",
                "manufacturing_withdrawal_baseline",
                "manufacturing_return_baseline",
                "urban_return2_baseline",
                "rural_return_baseline",
            ])
        ]
        df_dmds = pd.concat([df_dmds, df_m])
    return df_dmds


def _extract_components(df_dmds) -> dict:
    # extract demand and rate dataframes from combined data
    comps = {}
    comps["urban_withdrawal"] = df_dmds[df_dmds["variable"] == "urban_withdrawal2_baseline"]
    comps["rural_withdrawal"] = df_dmds[df_dmds["variable"] == "rural_withdrawal_baseline"]
    comps["manufacturing_withdrawal"] = df_dmds[df_dmds["variable"] == "manufacturing_withdrawal_baseline"]
    comps["manufacturing_return"] = df_dmds[df_dmds["variable"] == "manufacturing_return_baseline"]
    comps["urban_return"] = df_dmds[df_dmds["variable"] == "urban_return2_baseline"]
    comps["rural_return"] = df_dmds[df_dmds["variable"] == "rural_return_baseline"]
    comps["urban_connection_rate"] = df_dmds[df_dmds["variable"] == "urban_connection_rate_baseline"]
    comps["rural_connection_rate"] = df_dmds[df_dmds["variable"] == "rural_connection_rate_baseline"]
    comps["urban_treatment_rate"] = df_dmds[df_dmds["variable"] == "urban_treatment_rate_baseline"]
    comps["rural_treatment_rate"] = df_dmds[df_dmds["variable"] == "rural_treatment_rate_baseline"]
    comps["recycling"] = df_dmds[df_dmds["variable"] == "urban_recycling_rate_baseline"]
    comps["urban_return"].reset_index(drop=True, inplace=True)
    comps["rural_return"].reset_index(drop=True, inplace=True)
    comps["urban_connection_rate"].reset_index(drop=True, inplace=True)
    comps["rural_connection_rate"].reset_index(drop=True, inplace=True)
    comps["urban_treatment_rate"].reset_index(drop=True, inplace=True)
    comps["rural_treatment_rate"].reset_index(drop=True, inplace=True)
    comps["recycling"].reset_index(drop=True, inplace=True)
    return comps


def _apply_sdg_adjustments(comps, df_dmds, context) -> dict:
    # adjust rates for sdg or alternative policy scenarios
    if context.SDG != "baseline":
        if context.SDG == "SDG":
            file2 = f"basins_country_{context.regions}.csv"
            PATH = package_data_path("water", "delineation", file2)
            df_basin = pd.read_csv(PATH)
            comps["rural_treatment_rate"] = target_rate(comps["rural_treatment_rate"], df_basin, 0.8)
            comps["urban_treatment_rate"] = target_rate(comps["urban_treatment_rate"], df_basin, 0.95)
            comps["urban_connection_rate"] = target_rate(comps["urban_connection_rate"], df_basin, 0.99)
            comps["rural_connection_rate"] = target_rate(comps["rural_connection_rate"], df_basin, 0.8)
            comps["recycling"] = target_rate_trt(comps["recycling"], df_basin)
            suffix = "SDG"
        else:
            pol_scen = context.SDG
            comps["urban_connection_rate"] = df_dmds[df_dmds["variable"] == "urban_connection_rate_" + pol_scen]
            comps["urban_connection_rate"].reset_index(drop=True, inplace=True)
            comps["rural_connection_rate"] = df_dmds[df_dmds["variable"] == "rural_connection_rate_" + pol_scen]
            comps["rural_connection_rate"].reset_index(drop=True, inplace=True)
            comps["urban_treatment_rate"] = df_dmds[df_dmds["variable"] == "urban_treatment_rate_" + pol_scen]
            comps["urban_treatment_rate"].reset_index(drop=True, inplace=True)
            comps["rural_treatment_rate"] = df_dmds[df_dmds["variable"] == "rural_treatment_rate_" + pol_scen]
            comps["rural_treatment_rate"].reset_index(drop=True, inplace=True)
            comps["recycling"] = df_dmds[df_dmds["variable"] == "urban_recycling_rate_" + pol_scen]
            comps["recycling"].reset_index(drop=True, inplace=True)
            suffix = pol_scen
        all_rates = pd.concat(
            [
                comps["urban_connection_rate"],
                comps["rural_connection_rate"],
                comps["urban_treatment_rate"],
                comps["rural_treatment_rate"],
                comps["recycling"],
            ]
        )
        all_rates["variable"] = [x.replace("baseline", suffix) for x in all_rates["variable"]]
        save_path = package_data_path("water", "demands", "harmonized", context.regions)
        all_rates.to_csv(save_path / "all_rates_SSP2.csv", index=False)
    return comps


def _calc_demand(withdrawal_df, rate_df, commodity, conversion=1e-3, connected=True, node_prefix="B"):
    # merge withdrawal with connection rate and compute connected/disconnected demand
    df = withdrawal_df.reset_index(drop=True).merge(
        rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
    )
    if connected:
        df["value"] = conversion * df["value"] * df["rate"]
    else:
        df["value"] = conversion * df["value"] * (1 - df["rate"])
    return make_df(
        "demand",
        node=node_prefix + df["node"],
        commodity=commodity,
        level="final",
        year=df["year"],
        time=df["time"],
        value=df["value"],
        unit="km3/year",
    )


def _calc_industrial_demand(withdrawal_df, commodity, conversion=1e-3, node_prefix="B"):
    # compute industry demand from withdrawals
    df = withdrawal_df.reset_index(drop=True)
    df["value"] = conversion * df["value"]
    return make_df(
        "demand",
        node=node_prefix + df["node"],
        commodity=commodity,
        level="final",
        year=df["year"],
        time=df["time"],
        value=df["value"],
        unit="km3/year",
    )


def _calc_industrial_return(return_df, commodity, conversion=1e-3, node_prefix="B"):
    # compute industry return with negative value
    df = return_df.reset_index(drop=True)
    df["value"] = conversion * df["value"]
    return make_df(
        "demand",
        node=node_prefix + df["node"],
        commodity=commodity,
        level="final",
        year=df["year"],
        time=df["time"],
        value=-df["value"],
        unit="km3/year",
    )


def _calc_wastewater(return_df, treatment_rate_df, commodity, conversion=1e-3, collected=True, node_prefix="B"):
    # merge return with treatment rate and compute wastewater demand
    df = return_df.reset_index(drop=True).merge(
        treatment_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
    )
    if collected:
        df["value"] = conversion * df["value"] * df["rate"]
    else:
        df["value"] = conversion * df["value"] * (1 - df["rate"])
    return make_df(
        "demand",
        node=node_prefix + df["node"],
        commodity=commodity,
        level="final",
        year=df["year"],
        time=df["time"],
        value=-df["value"],
        unit="km3/year",
    )


def _assemble_historical_data(dmd_df, info):
    # extract historical activity and capacity data for selected years
    h_act = dmd_df[dmd_df["year"].isin([2010, 2015])]
    conditions = [
        (h_act["commodity"] == "urban_mw"),
        (h_act["commodity"] == "industry_mw"),
        (h_act["commodity"] == "rural_mw"),
        (h_act["commodity"] == "urban_disconnected"),
        (h_act["commodity"] == "rural_disconnected"),
        (h_act["commodity"] == "urban_collected_wst"),
        (h_act["commodity"] == "rural_collected_wst"),
        (h_act["commodity"] == "urban_uncollected_wst"),
        (h_act["commodity"] == "industry_uncollected_wst"),
        (h_act["commodity"] == "rural_uncollected_wst"),
    ]
    values = [
        "urban_t_d",
        "industry_unconnected",
        "rural_t_d",
        "urban_unconnected",
        "rural_unconnected",
        "urban_sewerage",
        "rural_sewerage",
        "urban_untreated",
        "industry_untreated",
        "rural_untreated",
    ]
    h_act["commodity"] = np.select(conditions, values, "unknown commodity")
    h_act["value"] = h_act["value"].abs()
    hist_act = make_df(
        "historical_activity",
        node_loc=h_act["node"],
        technology=h_act["commodity"],
        year_act=h_act["year"],
        mode="M1",
        time=h_act["time"],
        value=h_act["value"],
        unit="km3/year",
    )
    h_cap = h_act[h_act["year"] >= 2015]
    h_cap = (
        h_cap.groupby(["node", "commodity", "level", "year", "unit"])["value"]
        .sum()
        .reset_index()
    )
    hist_cap = make_df(
        "historical_new_capacity",
        node_loc=h_cap["node"],
        technology=h_cap["commodity"],
        year_vtg=h_cap["year"],
        value=h_cap["value"] / 5,
        unit="km3/year",
    )
    return hist_act, hist_cap


def _generate_share_constraint(df_recycling, sub_time, info):
    # generate share lower bound for recycling
    df_share_wat = make_df(
        "share_commodity_lo",
        shares="share_wat_recycle",
        node_share="B" + df_recycling["node"],
        year_act=df_recycling["year"],
        value=df_recycling["value"],
        unit="-",
    ).pipe(broadcast, time=pd.Series(sub_time))
    df_share_wat = df_share_wat[df_share_wat["year_act"].isin(info.Y)]
    return df_share_wat