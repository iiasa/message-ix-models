from pathlib import Path

import numpy as np
import pandas as pd
from message_ix_models import ScenarioInfo
from message_ix_models.util import identify_nodes, nodes_ex_world


def _fetch(df, tec, var, yr):
    """Retrieve data from input data

    Parameters
    ----------
    df : pandas.DataFrame()
        data to be filtered
    tec : str
        filter for technology
    var : str
        fitler for variable
    yr : int
        filter for year
    """
    return (
        df.loc(axis=0)[:, tec, var, yr]["value"]
        .reset_index()
        .set_index(["node_loc", "technology"])
        .drop(["year", "variable"], axis=1)
    )


def _read_input_data(scen, input_file, tecs, historic_years):
    """Read data from xlsx

    The data includes capacity and activity data for the technologies:
    solar_pv_ppl, wind_ppl (on-shore), wind_ppf (off-shore) and solar_csp_sm1

    These data represent TOTAL activity and installed capacity for historic timesteps.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    input_file : pathlib.Path()
        path including file-name and extension where data is stored.
    tecs : list
        list of technologies for which data should be retrieved.
    historic_years : list
        list of years for which historic data should be retrieved.
    """

    # Variable used to collect various input data dataframes
    df = []

    # Index used to sort data
    idx = ["node_loc", "variable"]

    # Read data
    xlsx = pd.ExcelFile(input_file)

    # Define sheets to be read and some formatting variables
    # Title: refers to the table title in the xlsx
    # Variable: refers to the variable name assigned to the data
    sheets = {
        "data_capacity": {
            "title": "Installed capacity (GW)",
            "variable": "Capacity_TOT",
        },
        "data_generation": {"title": "Generation (GWa)", "variable": "Activity_TOT"},
    }

    # Retrieve the region-id (prefix)
    region_id = identify_nodes(scen)
    regions = nodes_ex_world(ScenarioInfo(scen).N)

    # Iterate over the different sheets and retrieve data
    for sheet in sheets:
        hist_cap = xlsx.parse(sheet).rename(
            columns={sheets[sheet]["title"]: "node_loc"}
        )

        # Assign region_id (prefix e.g. "R11_")
        hist_cap["node_loc"] = region_id + "_" + hist_cap["node_loc"]

        # Filter out region data for regions contained in model
        hist_cap = hist_cap[hist_cap["node_loc"].isin(regions)]

        # Assign variable name
        hist_cap["variable"] = sheets[sheet]["variable"]

        # Filter out data for specified technologies
        for tec in tecs:
            cols = [c for c in hist_cap.columns if tec in c or c in idx]
            if not cols:
                raise ValueError(f"No input data found for technology {tec}")
            tmp = hist_cap[cols].melt(id_vars=idx, var_name=["year"])
            tmp["year"] = tmp["year"].str.replace("{},".format(tec), "")
            tmp["technology"] = tec
            df.append(tmp)

    df = (
        pd.concat(df)
        .reset_index()
        .drop("index", axis=1)
        .astype({"year": "int32"})
        .fillna(0)
    )

    # Filter out data only for specified years
    if historic_years:
        df = df[df["year"].isin(historic_years)]

    data_cap = xlsx.parse("data_capacity_annual")
    data_cap = data_cap[data_cap.technology.isin(tecs)]
    data_cap["node_loc"] = region_id + "_" + data_cap["node_loc"]
    data_cap = data_cap[data_cap.node_loc.isin(regions)]

    data_elecgen = xlsx.parse("data_electricity_market")
    data_elecgen = data_elecgen.rename(columns={data_elecgen.columns[0]: "node_loc"})
    data_elecgen["node_loc"] = region_id + "_" + data_elecgen["node_loc"]
    data_elecgen = data_elecgen[data_elecgen.node_loc.isin(regions)]

    return df, data_cap, data_elecgen


def _clean_data(df, idx, verbose):
    """Match activity to capacity

    If the capacity for a specific technology is 0,
    then the activity is matched i.e. set to zero.

    Parameters
    ----------
    df : pandas.DataFrame()
        data to which cleanup should be applied.
    idx : list
        index used for input_data
    verbose : booelan
        option whether to print onscreen messages
    """
    # Create filter where Capacity_TOT = 0
    filt = df.loc[(df.variable == "Capacity_TOT") & (df.value == 0)].drop(
        ["variable", "value"], axis=1
    )
    if not filt.empty:
        # Create tmp dataframe with only "Activity_TOT"
        tmp = df.loc[df.variable == "Activity_TOT"].set_index(
            ["node_loc", "technology", "year"]
        )
        if verbose:
            print(f"Activity correction will be made for \n{filt}")
        # Filter tmp dataframe for filt and set "Activity_TOT" to 0
        tmp = (
            filt.join(tmp, on=tmp.index.names)
            .set_index(tmp.index.names)
            .dropna()
            .assign(value=0)
            .reset_index()
            .set_index(idx)
        )
        # Update
        df = df.set_index(idx)
        return tmp.combine_first(df).reset_index()
    else:
        return df


def _fill_data_gap(scen, df, tecs, cf_adj, idx, ren_bins, method, verbose):
    """Fill data gaps of either activity or capacity

    Data gaps are filled for individual technologies, using complete data from adjacent
    years.

    For technologies where either only capacity or activity data has been
    supplied, model data is used. The CF (capacity factor) is retrieved from existing
    bins. By default, historical capcity in these cases will be assigned to the least
    efficient bins.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    df : pandas.DataFrame()
        data for which gaps should be filled.
    tecs : list
        list of technologies
    cf_adj : number
        adjustment of capacity factor for non-historical periods vis-a-vis
        the CF in the previous period.
    idx : list
        index used for input_data
    ren_bins : pandas.DataFrame()
        model data retrieved using `_retrieve_model_data()`
    method : str
        method applied for choice of efficency factors from model data if historic
        data only includes either capacity or activity data.
        "lowest_eff": will assign historic capacities to lowest CF bins.
        "highest_eff": will assign historic capacities to lowest CF bins.
    verbose : booelan
        option whether to print onscreen messages
    """

    for tec in tecs:
        all_years = sorted(
            (df.query("technology == @tec").reset_index().year.unique().tolist())
        )
        act_yrs = (
            df.query("technology == @tec and variable == 'Activity_TOT'")
            .reset_index()
            .year.unique()
            .tolist()
        )
        cap_yrs = (
            df.query("technology == @tec and variable == 'Capacity_TOT'")
            .reset_index()
            .year.unique()
            .tolist()
        )

        # -----------------------------------
        # Add data for missing activity years
        # -----------------------------------

        for par in ["Capacity", "Activity"]:
            # Create a list if years for which data additions are required
            if par == "Activity":
                missing = [x for x in cap_yrs if x not in act_yrs]
                # If there is no data for activity, then it is not
                # possible to calculate reference capacity years
                if len(act_yrs) == 0:
                    act_yrs = cap_yrs[:1]
            elif par == "Capacity":
                missing = [x for x in act_yrs if x not in cap_yrs]
                # If there is no data for capacity, then it is not
                # possible to calculate reference capacity years
                if missing == cap_yrs:
                    if verbose:
                        print(
                            f"No adjustments will be made for technology {tec}"
                            " as no reference capacity years are available"
                        )
                    continue
            for yr in missing:
                # The reference year is the year closest
                yr_ref = _find_closest(sorted(set(cap_yrs) & set(act_yrs)), yr)
                if verbose:
                    print(
                        f"{par} years {yr} will be added for technology {tec}"
                        f" based on the capacity factor from {yr_ref}"
                    )

                # Routine for future years
                if yr >= scen.firstmodelyear and par == "Activity":
                    # The totals for all previous years must be subtracted
                    # as these are totals. These can then be devided.
                    # e.g. for 2020, the 2015 CF is derived by subtracting 2010
                    #      data from 2015 data.
                    prev_act = _fetch(df, tec, "Activity_TOT", yr_ref)
                    prev_cap = _fetch(df, tec, "Capacity_TOT", yr_ref)

                    yr_pref = all_years[all_years.index(yr_ref) - 1]
                    # Ensure that there are two prior time-periods, otherwise dont
                    # subtract.
                    if yr_pref != yr:
                        pprev_act = _fetch(df, tec, "Activity_TOT", yr_pref)
                        pprev_cap = _fetch(df, tec, "Capacity_TOT", yr_pref)

                        # Derive the capacity factor, act/cap
                        prev_cf = (
                            (prev_act - pprev_act) / (prev_cap - pprev_cap)
                        ).fillna(0)
                    else:
                        prev_cf = ((prev_act) / (prev_cap)).fillna(0)

                    # Retrieve capacity data
                    act_yr = df.loc(axis=0)[:, tec, "Capacity_TOT", yr]

                    # Subtract capacity from previous years from "yr"
                    act_yr.loc[:, "value"] -= prev_cap.loc[:, "value"]

                    # Derive activity for "yr" using derived capacity_factor
                    # and adjustment factor
                    act_yr.loc[:, "value"] *= prev_cf.loc[:, "value"] * cf_adj

                    # Add the activity from the previous years to arrive
                    # at the cumulative activity
                    act_yr.loc[:, "value"] += prev_act.loc[:, "value"]
                    act_yr = act_yr.rename(
                        {"Capacity_TOT": "Activity_TOT"}, axis="index"
                    )
                    act_yr = act_yr.reset_index().set_index(idx)
                    df = act_yr.combine_first(df)

                # Routine for historical years
                else:
                    # If for a given technology, no data is available to fill gaps
                    # i.e. either no "activity_TOT" or no "Capacity_TOT" is
                    # available, the model efficiencies are applied.
                    if any(
                        x not in set(df.loc(axis=0)[:, tec].reset_index().variable)
                        for x in ["Capacity_TOT", "Activity_TOT"]
                    ):
                        # Filter correct technology data
                        tmp = ren_bins.copy()
                        tmp = tmp[(tmp.technology == tec) & (tmp.POT > 0)].set_index(
                            ["node_loc", "potential_category", "technology"]
                        )

                        # Filter out the potentials with the lowest efficiency
                        # Based on previous observations, historically,
                        if method == "highest_eff":
                            tmp = tmp.groupby(level=0, as_index=False).CF.apply(
                                lambda group: group.nsmallest(1)
                            )
                        elif method == "lowest_eff":
                            tmp = tmp.groupby(level=0, as_index=False).CF.apply(
                                lambda group: group.nlargest(1)
                            )
                        tmp = (
                            tmp.to_frame()
                            .rename(columns={"CF": "value"})
                            .reset_index()
                            .drop(["level_0", "potential_category"], axis=1)
                        )
                        tmp = tmp.set_index(["node_loc", "technology"])
                    else:
                        # Derive the capacity factor for the reference year, act/cap
                        tmp = (
                            _fetch(df, tec, "Activity_TOT", yr_ref)
                            / _fetch(df, tec, "Capacity_TOT", yr_ref)
                        ).fillna(0)

                    # Apply capacity factor from reference year to the
                    # available data from the year.
                    if par == "Capacity":
                        tmp = df.loc(axis=0)[:, tec, "Activity_TOT", yr] / tmp
                        tmp = (
                            tmp.rename({"Activity_TOT": "Capacity_TOT"}, axis="index")
                            .reset_index()
                            .set_index(idx)
                        )
                    else:
                        tmp = df.loc(axis=0)[:, tec, "Capacity_TOT", yr] * tmp
                        tmp = (
                            tmp.rename({"Capacity_TOT": "Activity_TOT"}, axis="index")
                            .reset_index()
                            .set_index(idx)
                        )
                    df = pd.concat([df, tmp], sort=True)

    return df


def _calc_periodic_data(df, tecs, idx):
    """Derive periodic values

    For each of the variables, "Capacity_TOT", "Activity_TOT", which represent
    cumulative values over time, i.e. that of the total installed capacity,
    the periodic values are derived and added as "Capacity_PER", "Activity_PER".
    In addition, the period specific capacity factor is derived: "CF_PER".

    Parameters
    ----------
    df : pandas.DataFrame()
        data for which gaps should be filled.
    tecs : list
        list of technologies
    idx : list
        index used for input_data
    """
    for tec in tecs:
        tec_years = sorted(set(df.loc(axis=0)[:, tec].reset_index().year))
        for yr in tec_years:
            yr_act = _fetch(df, tec, "Activity_TOT", yr)
            yr_cap = _fetch(df, tec, "Capacity_TOT", yr)

            if yr != tec_years[0]:
                yr_ref = tec_years[tec_years.index(yr) - 1]
                yr_act -= _fetch(df, tec, "Activity_TOT", yr_ref)
                yr_cap -= _fetch(df, tec, "Capacity_TOT", yr_ref)

            # Derive the capacity factor, act/cap
            yr_cf = (yr_act / yr_cap).fillna(0)

            yr_act = (
                yr_act.reset_index()
                .assign(variable="Activity_PER", year=yr)
                .set_index(idx)
            )
            yr_cap = (
                yr_cap.reset_index()
                .assign(variable="Capacity_PER", year=yr)
                .set_index(idx)
            )
            yr_cf = (
                yr_cf.reset_index().assign(variable="CF_PER", year=yr).set_index(idx)
            )

            # Merge dataframes
            df = pd.concat([df, yr_act, yr_cap, yr_cf], sort=True)
    return df.reset_index().sort_values(by=idx)


def _retrieve_model_data(scen, tecs):
    """Retrieve data required for varaible renewables

    The data retrieved from the scenario includes
    the technology specific potentials "bin", the relevant relation
    used to link the technologies to "capacity potentials", the respective
    "potential" categories as well as the attributed capacity-factor and
    their potentials.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    tecs : list
        list of technologies

    Returns
    -------
    df : pd.DataFrame()
         index= ["node_loc", "technology", "bin", "capacity_relation",
                 "potential_category", "CF", "POT"]
    """

    # Create dicitonary: "capacity_relations"
    # Format: {renewable technology: capacity_relation_name}
    capacity_relations = (
        scen.par("relation_total_capacity", filters={"technology": tecs})[
            ["relation", "technology"]
        ]
        .drop_duplicates()
        .set_index("technology")
        .to_dict()["relation"]
    )

    # Then use the capacity relation name to filter out which
    # potential categories are attributed to each technology
    df = scen.par(
        "relation_total_capacity", filters={"relation": capacity_relations.values()}
    )

    # Filter out all the relevant "res" tecs.
    res_tecs = df[["relation", "technology"]].drop_duplicates()
    res_tecs = res_tecs[~res_tecs["technology"].isin(tecs)].technology.tolist()

    # Filter relation capacity data for res_tecs
    df = (
        df[df["technology"].isin(res_tecs)]
        .drop(["year_rel", "unit", "value"], axis=1)
        .rename(
            columns={
                "node_rel": "node_loc",
                "relation": "capacity_relation",
                "technology": "bin",
            }
        )
        .drop_duplicates()
        .set_index(["node_loc", "bin"])
    )

    # Retrieve the capacity factors of the different bins
    tmp = (
        scen.par("capacity_factor", filters={"technology": res_tecs})[
            ["node_loc", "technology", "value"]
        ]
        .rename(columns={"technology": "bin"})
        .drop_duplicates()
        .set_index(["node_loc", "bin"])
    )
    df["CF"] = tmp["value"]

    # Retireve different potentials for each bin
    tmp = scen.par("relation_activity", {"technology": res_tecs})
    tmp = tmp[
        tmp.relation.isin(
            [
                r
                for r in tmp.relation.unique()
                if any([y in r for y in ["_pot", "_pof"]])
            ]
        )
    ]

    # Create dictionary "renewable_pot"
    # Format: {renewable_bins: renewable_potentials}
    renewable_pot = (
        tmp[["relation", "technology"]]
        .drop_duplicates()
        .set_index("technology")
        .to_dict()["relation"]
    )

    # Retrieve upper bounds for potentials
    tmp = (
        scen.par(
            "relation_upper",
            filters={"relation": [renewable_pot[i] for i in renewable_pot.keys()]},
        )
        .drop(["year_rel", "unit"], axis=1)
        .drop_duplicates()
        .rename(columns={"node_rel": "node_loc", "relation": "bin"})
    )
    tmp.bin = tmp.bin.map({v: k for k, v in renewable_pot.items()})
    tmp = tmp.set_index(["node_loc", "bin"])
    df["POT"] = tmp["value"]

    df = df.reset_index()
    # Add potential names
    df["potential_category"] = df["bin"].map(renewable_pot)
    df["technology"] = df["capacity_relation"].map(
        {v: k for k, v in capacity_relations.items()}
    )
    df = df[
        [
            "node_loc",
            "technology",
            "bin",
            "capacity_relation",
            "potential_category",
            "CF",
            "POT",
        ]
    ]
    return df


def _find_closest(lst, K):
    """Find closest value in a list for a given value

    Parameters
    ----------
    lst : list
        list of values
    K : number
        value for which the closest value in list should be retrieved
    """
    return lst[min(range(len(lst)), key=lambda i: abs(lst[i] - K))]


def _allocate_historic_values(ren_bins, data_df, cf_tol, idx):
    """Allocate historical data to corresonding bins

    Histroic renewable capacities are installed to the corresponding
    potential bins based on theis capacity factors. The "cf_tol"
    determines the allowable tolerance between the historical capacity
    factor and that of the bins. If there is no suitable bin, i.e. if the
    deviation between the modelled potential categories in the model is
    too large, then a new bin is created. The existing bins are adjusted
    in that the potentials are reduced, by the historic capacity built.
    The capacity factor difference is taken into account.
    The model is forced to maintain the capacity allocated historically
    for the remainder of the century, as it is assumed that existing
    plots of land occupied by renewables, will not be re-puprosed.

    Parameters
    ----------
    ren_bins : pandas.DataFrame()
        model data retrieved using `_retrieve_model_data()`
    data_df : pandas.DataFrame()
        input/calibration data
    cf_tol : number
        tolerance allowed between "actual" and "bin" capacity factors.
    idx : list
        index used for input_data
    """

    # ---------------------------------
    # Prep model data dataframe
    # for allocation of historical data
    # ---------------------------------

    # Filter model data to only include data for potential categories
    # greater than zero.
    df = ren_bins[["node_loc", "technology", "bin", "CF", "POT"]].set_index(
        ["node_loc", "technology", "CF"]
    )
    df = df[df.POT > 0]

    # For each of the years for which we have historical data,
    # add a column to the model data representing the year as well as a
    # column for the capacity factor per period.
    years = sorted(data_df.year.unique().tolist())
    for y in years:
        df[y] = ""
        df["{}_CF".format(y)] = ""

    # -----------------------------------
    # Filter historical data for "CF_PER"
    # -----------------------------------

    # Create a temporary dataframe including annual historical
    # capacity factors per period "Capacity_PER"
    # Drop all those with NaN or 0 values resulting from
    # activity=0 / capacity=0
    tmp = data_df[(data_df["variable"] == "CF_PER") & (data_df["value"] > 0)].dropna()
    data_df = data_df.set_index(idx)

    # Create a temporay bin which will incorporate all new historical bins
    hist_bin = pd.DataFrame()

    # -----------------------------------
    # Allocate historic capacities
    # by iterating of historical "CF_PER"
    # -----------------------------------
    for index, row in tmp.iterrows():
        # For each region/technology/period find the capacity factor nearest
        # of existing bins, which comes closest to the historical value.

        node = row.node_loc
        tec = row.technology
        yr = row.year

        # Create a list of capacity facors per tech/region
        cf_list = df.loc[(node, tec)].index.values.tolist()

        # If applying this script to a scenario which already
        # has had historical categories introduced, then these
        # need to filtered out for the corresponding year.
        check_list = df.loc[(node, tec), "bin"].values.tolist()
        for t in check_list:
            if "hist" in t and f"hist_{yr}" not in t:
                cf_list[check_list.index(t)] = 100

        #
        cf_closest = _find_closest(cf_list, row.value)
        cf_diff = row.value - cf_closest

        cap_PER = float(data_df.loc[[(node, tec, "Capacity_PER", yr)]].value)
        cf_PER = float(data_df.loc[[(node, tec, "CF_PER", yr)]].value)

        # First check if the difference between the categories is to great.
        if cf_diff <= cf_tol * -1:
            # Reduce the capacity of the closest "existing" bin taking into
            # account the difference in capacity factor
            # i.e. cap_Per * CF of cap_PER / res CF
            new_pot = (
                float(df.loc[[(node, tec, cf_closest)], "POT"])
                - cap_PER * cf_PER / cf_closest
            )

            # Check to see if there is enough potential in the bin from which
            # Capacity is being subtracted.
            if new_pot >= 0:
                df.loc[(node, tec, cf_closest), "POT"] = new_pot

            # Otherwise...
            if new_pot < 0:
                # Set the capacity of the first bin to 0
                df.loc[(node, tec, cf_closest), "POT"] = 0

                # Find next closest bin
                cf_list.pop(cf_list.index(cf_closest))
                cf_closest2 = _find_closest(cf_list, row.value)

                # The value smaller than zero, needs to be subtracted from the
                # next bin, hence it is * by -1 and then / back to the original
                # capacity
                new_pot2 = float(new_pot * -1 * cf_closest / cf_PER)

                # Now calculate the poetntial of the second category
                new_pot2 = (
                    float(df.loc[[(node, tec, cf_closest2)], "POT"])
                    - new_pot2 * cf_PER / cf_closest2
                )
                if new_pot2 < 0:
                    raise ValueError("Reduction of exisiting potentials exceeded")
                else:
                    df.loc[(node, tec, cf_closest2), "POT"] = new_pot2

            # Create a new bin
            new_bin_name = "_".join(
                df.loc[[(node, tec, cf_closest)], "bin"].values[0].split("_")[:-1]
            )
            if row.technology in ["wind_ppf"]:
                new_bin_name = "{}_ref_hist_{}".format(new_bin_name, row.year)
            else:
                new_bin_name = "{}_res_hist_{}".format(new_bin_name, row.year)

            # Copy the df slice from existing, closest CF,
            # and change accordingly
            assign = {
                "bin": new_bin_name,
                "CF": round(row.value, 3),
                "POT": cap_PER,
                yr: cap_PER,
                f"{yr}_CF": cf_PER,
            }
            hist_tmp = df.loc[[(node, tec, cf_closest)]].reset_index()
            # A loop needs to be used, as df.assign(**assign) doesnt work with
            # the yr which needs to passed as a string, but then a new column
            # would be added, instead of the existing column being modified.
            for x in assign:
                hist_tmp.loc[:, x] = assign[x]
            hist_tmp = hist_tmp.set_index(["node_loc", "technology", "CF"])
            if hist_bin.empty:
                hist_bin = hist_tmp
            else:
                hist_bin = pd.concat([hist_bin, hist_tmp])

        # If the CF difference is not to large, then the capacity
        # is allocated to the correct bin
        else:
            # The Capacity_PER value is compared with potential of
            # the bin with the closest CF
            # If the CF is too large, then capacity is allocated
            # to the next closest category.
            tmp_pot = df.loc[[(node, tec, cf_closest)], "POT"].values[0]

            # All capacities which have already been allocated are added up
            hist_pot_allocated = [
                df.loc[[(node, tec, cf_closest)], y].values[0] for y in years if y < yr
            ]
            hist_pot_allocated = sum([float(i) for i in hist_pot_allocated if i])

            # The available potential is the potential of the bin minus
            # already allocated potential in previuous years
            tmp_pot -= hist_pot_allocated

            if tmp_pot < cap_PER:
                # Adds the potential for the year
                df.loc[[(node, tec, cf_closest)], yr] = tmp_pot

                # Adds the CF for the year
                df.loc[[(node, tec, cf_closest)], "{}_CF".format(yr)] = cf_PER

                # Find next closest value
                cf_list.pop(cf_list.index(cf_closest))
                cf_closest = _find_closest(cf_list, row.value)
                df.loc[[(node, tec, cf_closest)], yr] = cap_PER - tmp_pot

                # Adds the CF for the year
                df.loc[[(node, tec, cf_closest)], "{}_CF".format(yr)] = cf_PER

            else:
                df.loc[[(node, tec, cf_closest)], yr] = cap_PER

                # Adds the CF for the year
                df.loc[[(node, tec, cf_closest)], "{}_CF".format(yr)] = cf_PER

    df = pd.concat([df, hist_bin], sort=True)
    df = (
        df.reset_index()
        .sort_values(["node_loc", "technology", "bin"])
        .reset_index()
        .drop("index", axis=1)
    )
    return df


def _remove_par(scen, df, remove_par):
    """Remove parameters

    Historical values are removed for ref_.
    For all others, values are removed for ALL timesteps and
    for ALL "parent" technologies (i.e. wind_ppl etc.)
    and bins (i.e. solar_res1 etc.)

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    df : pandas.DataFrame()
        dataframe for allocated historic values from `_allocate_historic_values`
    remove_par : list
        list of parameters which should be removed for the technologies.
    """

    nodel = df.node_loc.unique().tolist()
    tecl = df.technology.unique().tolist() + df.bin.unique().tolist()

    with scen.transact("History for renewable technologies removed"):
        for par in remove_par:
            rem_df = scen.par(par, filters={"node_loc": nodel, "technology": tecl})
            if "ref_" in par:
                # Retrieve the year index name
                year_idx_name = [i for i in scen.idx_names(par) if "year" in i][0]
                rem_df = rem_df[rem_df[year_idx_name] < scen.firstmodelyear]
            scen.remove_par(par, rem_df)


def _update_potentials(scen, df, ren_bins):
    """Update potentials of existing bins in a given scenario.

    This is done by comparing the POT of the original model data
    "ren_bins" with the updated POT of "df"

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    df : pandas.DataFrame()
        dataframe for allocated historic values from `_allocate_historic_values`
    ren_bins : pandas.DataFrame()
        model data retrieved using `_retrieve_model_data()`
    """
    tmp = ren_bins.set_index(["node_loc", "technology", "bin"])
    tmp["POT_new"] = df.set_index(["node_loc", "technology", "bin"])["POT"]
    tmp = tmp[(tmp["POT"] - tmp["POT_new"]) != 0].dropna().reset_index()

    with scen.transact("Update existing potential categories"):
        for index, row in tmp.iterrows():
            upd_df = scen.par(
                "relation_upper",
                filters={"node_rel": row.node_loc, "relation": row.potential_category},
            )
            upd_df["value"] = row.POT_new
            scen.add_par("relation_upper", upd_df)


def _add_new_hist_potential_bin(scen, df, remove_par, verbose):
    """Add new potential bins to a given scenario.

    For all new bin (res) categories which get introduced,
    "res4" or "ref4" will be used as a basis for copying parameters.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    df : pandas.DataFrame()
        dataframe for allocated historic values from `_allocate_historic_values`
    remove_par : list
        list of parameters which should be removed for the technologies.
    verbose : booelan
        option whether to print onscreen messages
    """
    new_resm = [
        t for t in df.bin.unique().tolist() if t not in scen.set("technology").tolist()
    ]
    tmp_df = df[df.bin.isin(new_resm)]

    with scen.transact("Added new potential categories"):
        for index, row in tmp_df.iterrows():
            # Assign variables
            node = row.node_loc
            rbin = row.bin
            tec = row.technology

            # Create name of the reference technology used for copying parameters.
            # Create name of new potential relation.
            if tec == "wind_ppf":
                ref_tec = "{}_ref4".format(rbin.split("_")[0])
                pot_rel_new = rbin.replace("ref", "pot")
            else:
                ref_tec = "{}_res4".format(rbin.split("_")[0])
                pot_rel_new = rbin.replace("res", "pot")

            # Update set technology and relation
            if rbin not in scen.set("technology").tolist():
                if verbose:
                    print("Adding new technology", rbin)
                scen.add_set("technology", rbin)
                if verbose:
                    print("Adding new relation", pot_rel_new)
                scen.add_set("relation", pot_rel_new)

            # Retrieve all the parameters which need to be copied
            # All parameters previosuly removed are excluded
            for par in [
                p
                for p in scen.par_list()
                if p not in remove_par and "technology" in scen.idx_names(p)
            ]:
                # Retrieve the node index name
                node_ix = [i for i in scen.idx_names(par) if "node" in i][0]

                # Retrieve the data for the reference technology
                # which is to be updated
                upd_par = scen.par(
                    par,
                    filters={"technology": [ref_tec], node_ix: [node]},
                )
                if not upd_par.empty:
                    upd_par["technology"] = rbin
                    if par == "relation_activity":
                        pot_rel = [
                            x
                            for x in set(upd_par.relation)
                            if any([y in x for y in ["_pot", "_pof"]])
                        ]
                        upd_par["relation"] = upd_par["relation"].replace(
                            pot_rel, pot_rel_new
                        )

                        # Update the potential for the new category
                        # These are set slightly higher than calculated to avoid
                        # infeasibilities due to rounding.
                        upd_pot = scen.par(
                            "relation_upper",
                            filters={
                                "relation": pot_rel,
                                node_ix: [node],
                            },
                        )
                        upd_pot["relation"] = pot_rel_new
                        upd_pot["value"] = round(row.POT * 1.001, 3)
                        scen.add_par("relation_upper", upd_pot)
                    elif par == "capacity_factor":
                        upd_par["value"] = row.CF
                    scen.add_par(par, upd_par)


def _add_par_for_res(scen, data_df, df, years, h_years, m_years, opt_years):
    """Populate parameters for `bins`.

    For each country and technology, the activity and capacity of the potential
    bins are now updated. The parameters are set to 0 if their isnt any activity
    or capacity, in order to ensure that resource bins do not exceed calibrated
    values. i.e. if bin 4 is x, which represents all the capacity in a given year
    then bins 1,2,3 need to be fixed to 0.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    data_df : pandas.DataFrame
        dataframe containing the cleaned up input data originally retrieved via
        `_read_input_data()`
    df : pandas.DataFrame()
        dataframe for allocated historic values from `_allocate_historic_values`
    years : list
        list of years for which input data is available
    h_years : list
        list of input data years which correspond to historic model periods
    m_years : list
        list of input data years which correspond to model optimization periods
    opt_years : list
        list of model optimization years
    """
    df = df.replace("", 0)

    # Calculate the activity and capacity values that will be
    # added to the model
    for y in years:
        per_len = float(scen.par("duration_period", {"year": y})["value"])

        # The activity is calculated by the either using the "actual" CF
        # if it is <= to the res CF, otherwise the res CF is used.
        df["{}_ACT".format(y)] = df.apply(
            lambda row: row[y] * row["{}_CF".format(y)]
            if row["{}_CF".format(y)] <= row.CF
            else row[y] * row.CF,
            axis=1,
        )

        # The capacity is divided buy the period length of the respective years
        df["{}_NewCAP".format(y)] = df.apply(lambda row: row[y] / per_len, axis=1)
    df = df.round(3)

    with scen.transact("Update parameters for potential bins"):
        for index, row in df.iterrows():
            act = {
                "node_loc": row.node_loc,
                "technology": row.bin,
                "mode": "M1",
                "time": "year",
                "unit": "GWa",
            }

            cap = {"node_loc": row.node_loc, "technology": row.bin, "unit": "GW"}

            # Historical Activity is built by the cumulative sum across time steps
            hist_act = pd.DataFrame(
                {
                    **act,
                    **{
                        "year_act": h_years,
                        "value": np.cumsum(
                            [row["{}_ACT".format(y)] for y in h_years]
                        ).tolist(),
                    },
                }
            )
            scen.add_par("historical_activity", hist_act)
            scen.add_par("ref_activity", hist_act)

            # Shouldnt these also be cumulative?
            bound_act_lo = pd.DataFrame(
                {
                    **act,
                    **{
                        "year_act": opt_years,
                        "value": sum([row["{}_ACT".format(y)] for y in years]),
                    },
                }
            )
            scen.add_par("bound_activity_lo", bound_act_lo)

            # Values represent historical capacity added in historic periods
            hist_new_cap = pd.DataFrame(
                {
                    **cap,
                    **{
                        "year_vtg": h_years,
                        "value": [row["{}_NewCAP".format(y)] for y in h_years],
                    },
                }
            )
            scen.add_par("historical_new_capacity", hist_new_cap)
            scen.add_par("ref_new_capacity", hist_new_cap)

            if m_years:
                bound_act_up = pd.DataFrame(
                    {
                        **act,
                        **{
                            "year_act": m_years,
                            "value": sum([row["{}_ACT".format(y)] for y in years])
                            * 1.001,
                        },
                    }
                )
                scen.add_par("bound_activity_up", bound_act_up)

                bound_new_cap = pd.DataFrame(
                    {
                        **cap,
                        **{
                            "year_vtg": m_years,
                            "value": [row["{}_NewCAP".format(y)] for y in m_years],
                        },
                    }
                )
                scen.add_par("bound_new_capacity_lo", bound_new_cap)

                bound_new_cap["value"] *= 1.001
                scen.add_par("bound_new_capacity_up", bound_new_cap)
    return df


def _add_par_for_tec(scen, tmp_df, years, h_years, m_years, opt_years):
    """Populate parameters for `tecs`.

    For each country and technology, the activity and capacity bounds are update.
    The technology specific capacity or activity bounds will correlate to the
    sum across all the technology specific bins.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    tmp_df : pandas.DataFrame
        dataframe for allocated historic values from `_add_par_for_res`
    years : list
        list of years for which input data is available
    h_years : list
        list of input data years which correspond to historic model periods
    m_years : list
        list of input data years which correspond to model optimization periods
    opt_years : list
        list of model optimization years
    """
    with scen.transact("Update parameters for parent technologies"):
        tmp_df = (
            tmp_df.drop("bin", axis=1)
            .groupby(["node_loc", "technology"])
            .sum()
            .reset_index()
        )
        for index, row in tmp_df.iterrows():
            act = {
                "node_loc": row.node_loc,
                "technology": row.technology,
                "mode": "M1",
                "time": "year",
                "unit": "GWa",
            }

            cap = {"node_loc": row.node_loc, "technology": row.technology, "unit": "GW"}

            # Historical Activity is built by the cumulative sum across time steps
            hist_act = pd.DataFrame(
                {
                    **act,
                    **{
                        "year_act": h_years,
                        "value": np.cumsum(
                            [row["{}_ACT".format(y)] for y in h_years]
                        ).tolist(),
                    },
                }
            )
            scen.add_par("historical_activity", hist_act)
            scen.add_par("ref_activity", hist_act)

            hist_new_cap = pd.DataFrame(
                {
                    **cap,
                    **{
                        "year_vtg": h_years,
                        "value": [row["{}_NewCAP".format(y)] for y in h_years],
                    },
                }
            )
            scen.add_par("historical_new_capacity", hist_new_cap)
            scen.add_par("ref_new_capacity", hist_new_cap)

            bound_act_lo = pd.DataFrame(
                {
                    **act,
                    **{
                        "year_act": opt_years,
                        "value": sum([row["{}_ACT".format(y)] for y in years]),
                    },
                }
            )
            scen.add_par("bound_activity_lo", bound_act_lo)

            if m_years:
                bound_act_up = pd.DataFrame(
                    {
                        **act,
                        **{
                            "year_act": m_years,
                            "value": sum([row["{}_ACT".format(y)] for y in years])
                            * 1.001,
                        },
                    }
                )
                scen.add_par("bound_activity_up", bound_act_up)

                bound_new_cap = pd.DataFrame(
                    {
                        **cap,
                        **{
                            "year_vtg": m_years,
                            "value": [row["{}_NewCAP".format(y)] for y in m_years],
                        },
                    }
                )
                scen.add_par("bound_new_capacity_lo", bound_new_cap)
                bound_new_cap["value"] *= 1.001
                scen.add_par("bound_new_capacity_up", bound_new_cap)

                bound_total_cap = pd.DataFrame(
                    {
                        **cap,
                        **{
                            "year_act": m_years,
                            "value": sum([row[y] for y in years]),
                        },
                    }
                )
                scen.add_par("bound_total_capacity_lo", bound_total_cap)


def _update_init_cap_bound(
    scen,
    data_cap,
    data_elecgen,
    tecs_init_cap,
    reg_grp=["SAS", "CPA", "NAM", "WEU"],
):
    """Update existing initial startup value for the dynamic capacity bound.

    In order to determine the correct startup values, historic annual capacity
    values are used. The difference between these values indicates the historical
    additions for each period, and the difference between these inidicates the
    maximum change between annual additions. The average across the market leaders,
    are then scaled with regional electricity market shares and used to parametrize
    the startup value.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    data_cap : pandas.DataFrame
        annual historic capacity data from `_read_input_data`
    data_elecgen : pandas.DataFrame
        data on regional 2020 secondary electricity generation for 2020 from
        `_read_input_data`.
    tecs_initi_cap : list
        list of technologies for which inital startup values should be updated
    reg_grp : list
    """
    data_cap = data_cap.set_index(["node_loc", "technology"])
    # Based on historical, annual TIC timeseries
    # 1. all 0 are replaced by NAN so that the jump from missing data
    # years is ignored.
    # 2. the difference for these values gives us the annual capacity
    # additions
    # 3. The difference of the annual capacity additions then gives
    # the values used for subsequent calculations of the parameter
    # "initial_new_cap_up".
    data_cap = (
        data_cap.replace(0, np.nan).diff(axis=1).diff(axis=1).max(axis=1).fillna(0)
    )
    # Set negative values to 0
    data_cap.loc[data_cap.values <= 0] = 0
    data_cap = data_cap.to_frame().rename(columns={0: "value"}).reset_index()

    # --------------------------------
    # A Manual fix is made to override
    # capacity additions for WEU.
    # --------------------------------
    data_cap.loc[
        (data_cap.node_loc.str.contains("WEU"))
        & (data_cap.technology == "solar_pv_ppl"),
        "value",
    ] = 8
    data_cap = data_cap.set_index(["node_loc"])

    # Data on electricity generation is converted from EJ/yr to TWh
    data_cap["elec_gen"] = data_elecgen.set_index(["node_loc"])["2020"] * 1000 / 3.6

    # Max-Capacity installation is normalized to electricty market share
    data_cap["max_by_size"] = data_cap["value"] / data_cap["elec_gen"] * 1000

    # Provisional values are added
    data_cap["norm_by_size"] = 0

    with scen.transact("Initial_activity_up updated for renewable ppls'"):
        for tec in tecs_init_cap:
            tmp = scen.par(
                "initial_new_capacity_up", filters={"technology": tec}
            ).set_index(["node_loc"])
            if tec in data_cap.technology.unique():
                # Get Average across frontier regions -> drop too small values
                val = data_cap[
                    (data_cap.technology == tec) & (data_cap.max_by_size > 0.01)
                ]
                val = val.loc[
                    [r for r in val.index if any([x in r for x in reg_grp])],
                    "max_by_size",
                ].mean()
                data_cap.loc[(data_cap.technology == tec), "norm_by_size"] = (
                    data_cap[(data_cap.technology == tec)]["elec_gen"] / 1000 * val
                )

                tmp.value = data_cap.loc[(data_cap.technology == tec), "norm_by_size"]
            else:
                tmp.value = 0.05
            scen.add_par("initial_new_capacity_up", tmp.reset_index())


def main(
    scen,
    path,
    tecs=None,
    method="lowest_eff",
    cf_adj=1.1,
    cf_tol=0.005,
    remove_par=None,
    upd_init_cap=True,
    verbose=False,
):
    """Calibrate variable renewable technologies.

    Historical capacities and activities are used to derive "actual"
    capacity factors. These are used to allocate historical capacities
    to the respective potential-bins, based on their capacity factors,
    depicted in MESSAGEix. Should the deviation of actual historical
    capacity factors be too large compared to those depiced in MESSAGEix,
    then new "bins" are created.
    The historical capacities must be maintained in the future. This is
    achieved by a lower bound on total capacity for the respective bin.
    If new bins are introduced, then the potential of the existing bins
    is reduced. The difference in the capacity factors is accounted for
    when doing so.

    Parameters
    ----------
    scen : message_ix.Scenario
        scenario for which calibration is carried out.
    path : pathlib.Path
        path to model data, e.g. 'message_data/data'
    tecs : string or list
        name of technologies for which calibration should be carried out.
    method : str
        method applied for choice of efficency factors from model data if historic
        data only includes either capacity or activity data.
        "lowest_eff": will assign historic capacities to lowest CF bins.
        "highest_eff": will assign historic capacities to lowest CF bins.
    cf_adj : number (default=1.1)
        adjustment of capacity factor for non-historical periods vis-a-vis
        the CF in the previous period.
    cf_tol : number (default=0.005)
        tolerance allowed between "actual" and "bin" capacity factors.
    remove_par : list
        list of parameters which should be removed for the calibrated
        technologies
    upd_init_cap : boolean (default=True)
        option whether to update existing initial capacity up values.
    verbose : booelan
        option whether to print onscreen messages
    """

    # --------------------------------
    # Definition of generic parameters
    # --------------------------------

    # Define the file from where data should be retrieved.
    input_file = (
        Path(path)
        / "model"
        / "calibration_renewables"
        / "VRE_capacity_MESSAGE_global.xlsx"
    )

    # The historic years refer to the input data and therefore include 2006
    # as opposed to the modeyear 2005. 2006 data is used for the model
    # year 2005
    historic_years = [2006, 2010, 2015, 2020]

    # Define the technologies for which changes should be applied
    if not tecs:
        tecs = ["solar_pv_ppl", "wind_ppl", "wind_ppf", "csp_sm1_ppl"]
        tecs_init_cap = tecs + ["csp_sm3_ppl"]
    else:
        tecs = [tecs] if type(tecs) != list else tecs
        tecs_init_cap = tecs

    # Retrieves the historical years
    opt_years = ScenarioInfo(scen).Y

    # Set index used for input_data
    idx = ["node_loc", "technology", "variable", "year"]

    if not remove_par:
        remove_par = [
            "historical_activity",
            "historical_new_capacity",
            "ref_activity",
            "ref_new_capacity",
            "bound_new_capacity_up",
            "bound_new_capacity_lo",
            "bound_activity_lo",
            "bound_activity_up",
        ]

    # -------------------------------------
    # Step1. Retrieve and format input data
    # -------------------------------------

    # Retrieves for historic capacity/activity
    # Data is assigned to "Capacity_TOT" and "Activity_TOT"
    data_df, data_cap, data_elecgen = _read_input_data(
        scen, input_file, tecs, historic_years
    )

    # --------------------------
    # Step2. Retrieve model data
    # --------------------------

    ren_bins = _retrieve_model_data(scen, tecs=tecs)

    # Check that the script hasnt already been applied to a scenario
    # otherwise a "reset" of historic bins is required.
    if any("hist" in x for x in ren_bins.bin.unique()):
        raise ValueError(
            "This scenario has already been calibrated and requires the reset of historic bins"
        )

    # ------------------
    # Step3. Format data
    # ------------------

    # -------------------------------
    # Step3.1 Correct data mismatches
    # -------------------------------

    # If capacity is 0, but activity is not, activity is set to 0.
    data_df = _clean_data(data_df, idx, verbose)

    # ------------------------
    # Step3.2 Reset data years
    # ------------------------

    # Those years which are not in the model horizon are set to closest year
    data_df.loc[data_df["year"] == 2006, "year"] = 2005
    data_df = data_df.sort_values(by=idx).set_index(idx)

    # ----------------------
    # Step3.3 Fill data gaps
    # ----------------------

    data_df = _fill_data_gap(
        scen, data_df, tecs, cf_adj, idx, ren_bins, method, verbose
    )

    # -------------------------
    # Step3.4 Add periodic data
    # -------------------------

    data_df = _calc_periodic_data(data_df, tecs, idx)

    # -----------------------
    # Step4. Allocate history
    # -----------------------

    # Allocates input data to repective bins depicted in the model
    df = _allocate_historic_values(ren_bins, data_df, cf_tol, idx)

    # ----------------------
    # Step5. Update scenario
    # ----------------------

    # Step5.1 Removes parameters
    if remove_par:
        _remove_par(scen, df, remove_par)

    # Step5.2 Update potentials of "exisiting" categories
    _update_potentials(scen, df, ren_bins)

    # Step5.3 Introduce new bins and set potentials
    _add_new_hist_potential_bin(scen, df, remove_par, verbose)

    # Retrieve years for which input data is available
    years = sorted(data_df.year.unique().tolist())

    # Retrieve historic years < first_model_year
    h_years = [y for y in years if y < min(opt_years)]

    # Retrieve model years >= first_model_year
    m_years = [y for y in years if y in opt_years]

    # Step5.4 Populate parameters for _res
    # Set historical_new_capacity, historical_activity,
    # ref_new_capacity, ref_activity
    # as well as lower_bounds on activity for the _res and _ppl
    tmp_df = _add_par_for_res(scen, data_df, df, years, h_years, m_years, opt_years)

    # Step 5.5 populate parameters for _tec
    _add_par_for_tec(scen, tmp_df, years, h_years, m_years, opt_years)

    # ------------------------------------------------
    # Step6. Add dynamic bound for capacity (optional)
    # ------------------------------------------------

    if upd_init_cap:
        _update_init_cap_bound(scen, data_cap, data_elecgen, tecs_init_cap)
