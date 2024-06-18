# in this version lifetime is treated time-dependent
import logging

import numpy as np
import pandas as pd

from .utilities import closest, f_index, f_slice, idx_memb, intpol, unit_uniform

log = logging.getLogger(__name__)


def _log(parname, tec, node):
    """Common logging shorthand, used in several places."""
    log.debug(f"Update {repr(parname)} for new lifetime of {repr(tec)} in {repr(node)}")


def main(
    scenario,
    tec,
    lifetime=None,
    year_vtg_start=None,
    year_vtg_end=None,
    nodes=all,
    par_exclude=[],
    remove_rest=False,
    test_run=False,
    use_firstmodelyear=False,
    extrapol_neg=0.5,
    commit=True,
    quiet=True,
):
    """
    This function extends or shortens the lifetime of an existing technology
    in a message_ix scenario and modifies all other parameters accordinglly.
    The function can be used for extending or shortening the vintage years
    as well, without changing the lifetime.

    Parameters
    ----------
    scenario: object
        ixmp scenario
    tec: string
        Technology name
    lifetime: integer or None
        New technical lifetime (if None the old lifetime will be used)
    year_vtg_start: integer or None
        The first vintage year for specified lifetime
    year_vtg_end: integer or None
        The last vintage year for specified lifetime
    nodes: string or a list/series of strings, default all
        Model regions in where the lifetime of technology should be updated
    par_exclude: string or a list of strings, default None
        Parameters with no need for being updated for new activity or vintage
        years (e.g., some bounds may not be needed)
    remove_rest: boolean, default False
        If True, the data for the rest of vintage years not modified via this
        function will be removed. (e.g., if lifetime is changed for vintages
        between 2020 and 2050, the rest of vintages will be removed.)
    test_run: boolean, default False
        If True, the script is run only for test, and no changes will be
        commited to MESSAGE scenario
    use_firstmodelyear: boolean, default False
        If true, changes will not be applied to timesteps prior to the firstmodelyear.
    extrapol_neg: integer or None
        Treatment of negative values from extrapolation of two positive values:
        None does nothing (negative accepted),
        and integer acts as a multiplier of the adjacent value (e.g., 0, 0.5 or
        1) for replacing the negative value
    commit: boolean, default True
        If True, changes will be applied to the scenario.
    quiet : bool, optional
        Don't display debug-level log messages.
    """
    # Set log level
    log.setLevel(logging.WARNING if quiet else logging.DEBUG)

    # ------------------------
    # Creates helper variables
    # ------------------------
    # Retrieve regions for which changes should be made
    if nodes == all:
        nodes = scenario.set("node")
    elif isinstance(nodes, str):
        nodes = [nodes]

    if isinstance(par_exclude, str):
        par_exclude = [par_exclude]

    # Retrieve firstmodelyear and set horzion
    if use_firstmodelyear:
        firstmodelyear = scenario.firstmodelyear
    horizon = sorted([int(i) for i in scenario.set("year")])

    # Retrieve time related parameters: 1-time-dimension
    parlist_1d = [
        x
        for x in set(scenario.par_list())
        if len([y for y in scenario.idx_names(x) if "year" in y]) == 1
        and "technology" in scenario.idx_names(x)
    ]

    # Retrieve time related parameters: 2-time-dimension
    parlist_2d = [
        x
        for x in set(scenario.par_list())
        if len([y for y in scenario.idx_names(x) if "year" in y]) == 2
    ]

    if not test_run and commit is True:
        scenario.check_out()

    # -----------------
    # Adjust parameters
    # -----------------

    for node in nodes:
        par_empty = []
        results = {}

        # ------------------------------
        # Parameter "technical_lifetime"
        # ------------------------------
        parname = "technical_lifetime"
        df_old = scenario.par(parname, {"node_loc": node, "technology": tec})
        df_inp = scenario.par("input", {"node_loc": node, "technology": tec})
        if df_inp.empty:
            df_inp = df_old.copy()

        if df_old.empty:
            log.info(
                f"No technical_lifetime data for technology {repr(tec)}; skip node "
                f"{repr(node)}"
            )
            continue

        df_new = df_old.copy()

        # The range of vintage years to be changed is defined
        year_vtg_st = min(df_old["year_vtg"]) if not year_vtg_start else year_vtg_start
        year_vtg_e = max(df_inp["year_vtg"]) if not year_vtg_end else year_vtg_end

        # The lifetime is only changed for the range specified
        if lifetime:
            chngyrs = [y for y in horizon if y >= year_vtg_st and y <= year_vtg_e]
            df_new.loc[df_new.year_vtg.isin(chngyrs), "value"] = lifetime

        # Retrieve parameter duration period
        dur = (
            scenario.par("duration_period")
            .sort_values(by="year")
            .drop("unit", axis=1)
            .set_index("year")
        )

        # Generating duration_period_sum matrix for masking
        df_dur = pd.DataFrame(index=horizon[:-1], columns=horizon)
        for i in df_dur.index:
            for j in [x for x in df_dur.columns if x >= i]:
                df_dur.loc[i, j] = int(
                    dur.loc[(dur.index >= i) & (dur.index <= j)].sum()
                )

        # Create a list of new vintage years
        vtg_years = sorted([x for x in horizon if x >= year_vtg_st and x <= year_vtg_e])

        # Adding new vintage years to parameter technical_lifetime
        for y in [y for y in vtg_years if y not in set(df_new["year_vtg"])]:
            tmp = df_new.iloc[0, :].copy()
            tmp["year_vtg"] = y
            if lifetime and y >= year_vtg_st and y <= year_vtg_e:
                tmp["value"] = lifetime
            else:
                cl = closest(df_new["year_vtg"].tolist(), y)
                tmp["value"] = float(df_new.loc[df_new["year_vtg"] == cl, "value"])
            df_new = df_new.append(tmp, ignore_index=True)
        df_new = df_new.set_index("year_vtg")

        # Finding the last active year for each vintage year
        act_years = []
        for y in [x for x in vtg_years if x < max(horizon)]:
            tmp = df_dur.loc[y].to_frame(name="max")

            # Create a range to identify the correct timestep
            tmp["min"] = tmp["max"] - dur["value"] + 1

            # Set the 'max' value fot the last period artifically high
            tmp.loc[max(horizon), "max"] = 100
            act_years.append(
                tmp.loc[
                    (df_new.loc[y, "value"] >= tmp["min"])
                    & (df_new.loc[y, "value"] <= tmp["max"])
                ].index[0]
            )

        # Adding last model year if needed (not in duration_period_sum)
        if max(horizon) in vtg_years:
            act_years.append(max(horizon))

        # Pairing vintage years with corresponding last active year
        vtg_act_yrs = pd.DataFrame(index=vtg_years, data={"act_years": act_years})

        # Creating a list of years for which changes need to be made
        years_all = [
            x
            for x in horizon
            if x >= year_vtg_st and x <= max(max(vtg_years, act_years))
        ]

        df_new = df_new.loc[df_new.index.isin(years_all)]
        df_new = df_new.reset_index()

        # For estimating required loop numbers later
        lifetime_old = min(df_new["value"])

        # Adding vintage years to technical_lifetime for missing active years
        for y in [y for y in years_all if y not in set(df_new["year_vtg"])]:
            tmp = df_new.iloc[0, :].copy()
            tmp["year_vtg"] = y
            if lifetime and y >= year_vtg_st and y <= year_vtg_e:
                tmp["value"] = lifetime
            else:
                cl = closest(df_new["year_vtg"].tolist(), y)
                tmp["value"] = float(df_new.loc[df_new["year_vtg"] == cl, "value"])
            df_new = df_new.append(tmp, ignore_index=True)
        df_new = df_new.sort_values("year_vtg").reset_index(drop=True)

        if remove_rest and not test_run:
            # Removing extra vintage years if desired
            scenario.remove_par(parname, df_old)
        if not test_run:
            # If remove_rest,
            if not remove_rest:
                df_old = df_old.loc[df_old["year_vtg"].isin(vtg_years)].copy()
                scenario.remove_par(parname, df_old)

            # When removing data, it needs to be ensured that the dataframe
            # updating the technical lifetime includes values for all the
            # activity years of the last vintage year. These values are
            # originally taken from the df_old and therefore need to be updated
            # to be equal to the value of the last vintage year.
            if remove_rest:
                for n in df_new.node_loc.unique():
                    df_tmp = df_new[df_new["node_loc"] == n]
                    for t in df_tmp.technology.unique():
                        val = float(
                            df_tmp.loc[
                                (df_tmp["technology"] == t)
                                & (df_tmp["year_vtg"] == max(vtg_years)),
                                "value",
                            ]
                        )
                        df_new.loc[
                            (df_new["node_loc"] == n)
                            & (df_new["technology"] == t)
                            & (df_new["year_vtg"] > max(vtg_years)),
                            "value",
                        ] = val
            scenario.add_par(parname, df_new)

        results[parname] = df_new

        _log(parname, tec, node)

        par_exclude.append(parname)

        # Estimating how many time steps will possibly have missing data (for
        # using in Part III.3)
        if max(df_new["value"]) <= lifetime_old:
            n = 1
        else:
            if len(vtg_years) == 1:
                div = int(
                    scenario.par("duration_period", filters={"year": vtg_years}).value
                )
            else:
                div = min(np.diff(vtg_years))
            n = int((max(df_new["value"]) - lifetime_old) / div)

        # ------------------------------------------------------------
        # Parameters with one index related to time (e.g., "inv_cost")
        # ------------------------------------------------------------
        for parname in set(parlist_1d) - set(par_exclude):
            # Node-related index
            node_col = [y for y in scenario.idx_names(parname) if "node" in y][0]

            # Retrieve existing data
            df_old = scenario.par(parname, {node_col: node, "technology": tec})

            # Year-related index
            year_ref = [y for y in df_old.columns if "year" in y][0]

            # Check if data is available for parameter; No update of missing
            # data for historical years (for 1d parameters)
            if df_old.empty:
                par_empty.append(parname)
                continue
            elif use_firstmodelyear and max(df_old[year_ref]) < firstmodelyear:
                par_empty.append(parname)
                continue

            # Ensure 'unit' column is available
            if "unit" in df_old.columns:
                df_old = unit_uniform(df_old)

            # Create index used for pivoting dataframe
            idx = [x for x in df_old.columns if x not in [year_ref, "value"]]

            # Making pivot table for extrapolation
            df2 = df_old.copy().pivot_table(index=idx, columns=year_ref, values="value")

            # Preparing for backward extrapolation if more than one new years
            # to be added before existing ones

            # Check for years prior to existing data
            year_list = sorted(
                [x for x in years_all if x < min(df_old[year_ref])], reverse=True
            )
            # Check for years after existing data
            year_list = year_list + sorted(
                [x for x in years_all if x > max(df_old[year_ref])]
            )

            if use_firstmodelyear:
                year_list = [y for y in year_list if y < firstmodelyear]

            # Add values for missing years
            for y in year_list:
                # Finding two adjacent years for extrapolation
                if y < max(df2.columns):
                    year_next = horizon[horizon.index(y) + 1]
                    year_nn = (
                        horizon[horizon.index(y) + 2]
                        if len([x for x in df2.columns if x > y]) >= 2
                        else year_next
                    )

                elif (
                    y > max(df2.columns)
                    and horizon[horizon.index(y) - 1] in df2.columns
                ):
                    year_next = horizon[horizon.index(y) - 1]
                    if len([x for x in df2.columns if x < y]) >= 2:
                        year_nn = list(df2.columns)[
                            list(df2.columns).index(year_next) - 1
                        ]
                    else:
                        year_nn = year_next
                else:
                    continue

                # Extrapolate data
                df2.loc[:, y] = intpol(
                    float(df2[year_next]),
                    float(df2[year_nn]),
                    year_next,
                    year_nn,
                    y,
                    dataframe=True,
                )

                # If adjacent value(s) are infinity, the missing value is
                # set to the same inf
                if not df2.loc[np.isinf(df2[year_next])].empty:
                    df2.loc[np.isinf(df2[year_next]), y] = df2.loc[:, year_next]

                # Negative values after extrapolation are ignored, if
                # adjacent value is positive a multiplier is applied.
                if (
                    extrapol_neg
                    and not df2[y].loc[(df2[y] < 0) & (df2[year_next] >= 0)].empty
                ):
                    df2.loc[(df2[y] < 0) & (df2[year_next] >= 0), y] = (
                        df2.loc[:, year_next] * extrapol_neg
                    )

            # Dataframe is turned from wide to long format
            df_new = (
                pd.melt(
                    df2.reset_index(),
                    id_vars=idx,
                    value_vars=[x for x in df2.columns if x not in idx],
                    var_name=year_ref,
                    value_name="value",
                )
                .dropna(subset=["value"])
                .reset_index(drop=True)
            )

            # Dataframe is sorted
            df_new = df_new.sort_values(idx + [year_ref]).reset_index(drop=True)

            # Any data not in the list of years to be modified is dropped
            df_new = df_new.loc[df_new[year_ref].isin(years_all)]

            # Removing extra vintage/activity years if desired
            if remove_rest and not test_run:
                scenario.remove_par(parname, df_old)
            if not test_run:
                if not remove_rest:
                    df_old = df_old.loc[df_old[year_ref].isin(vtg_years)].copy()
                    scenario.remove_par(parname, df_old)
                scenario.add_par(parname, df_new)

            results[parname] = df_new

            _log(parname, tec, node)

        # -----------------------------------------------------------
        # Parameters with two indexes related to time (e.g., 'input')
        # -----------------------------------------------------------
        for parname in set(parlist_2d) - set(par_exclude):
            # Node-related index
            node_col = [x for x in scenario.idx_names(parname) if "node" in x]

            # Retrieve existing data
            df_old = scenario.par(parname, {node_col[0]: node, "technology": tec})

            # Check if data is available for parameter
            if df_old.empty:
                par_empty.append(parname)
                continue

            # Ensure 'unit' column is available
            if "unit" in df_old.columns:
                df_old = unit_uniform(df_old)

            # Create index used for pivoting dataframe
            idx = [x for x in df_old.columns if x not in ["year_act", "value"]]

            # Making pivot table for extrapolation
            df2 = df_old.copy().pivot_table(
                index=idx, columns="year_act", values="value"
            )

            # Identify new active years missing in original data
            yr_missing = [
                x for x in horizon if x <= max(act_years) and x not in df2.columns
            ]

            # Adding NAN for missing years
            for y in yr_missing:
                df2[y] = np.nan

            # Identify the second index related to time,
            # to be paired with year_act
            year_ref = [
                y for y in df_old.columns if y in ["year", "year_vtg", "year_rel"]
            ][0]

            # Checking inconsistency in vintage years for
            # commodities/emissions/relations for one specific technology
            col_nontec = [
                x
                for x in scenario.idx_names(parname)
                if not any(
                    y in x
                    for y in ["time", "node", "tec", "mode", "year", "level", "rating"]
                )
            ]
            if col_nontec:
                df_count = (
                    df2.reset_index()
                    .loc[df2.reset_index()[year_ref].isin(vtg_years), col_nontec]
                    .apply(pd.value_counts)
                    .fillna(0)
                )

                if not df_count.empty:
                    df_count = df_count.loc[
                        df_count[col_nontec[0]] < int(df_count.mean())
                    ]

                if not df_count.empty:
                    # NOTICE: this is not resolved here and user should decide
                    log.warning(
                        f"In parameter {repr(parname)} the vintage years of "
                        f"{col_nontec[0]: df_count.index.tolist()}, {repr(tec)} in "
                        f"{repr(node)} are different from other input entries. Please "
                        "check the results!"
                    )

            # -------------------------------------------------------
            # Adding extra active years to existing vintage years, if
            # lifetime extended.
            # In the case of the parameter relation_activity, this is
            # not required or wanted, as the instead of year_vtg, the
            # index is "year_rel", and the "year_act" entries should
            # not be extended for the "year_rel".
            # -------------------------------------------------------
            if parname != "relation_activity":
                count = 0
                while count <= n:
                    # The counter of loop (no explicit use of k)
                    count = count + 1
                    for y in sorted(
                        [x for x in set(df_old[year_ref]) if x in vtg_years]
                    ):
                        # The last active year of this vintage year
                        yr_end = act_years[vtg_years.index(y)]

                        if y < max(df_old["year_act"]):
                            year_next = horizon[horizon.index(y) + 1]

                            if y < horizon[horizon.index(max(df_old["year_act"])) - 1]:
                                year_nn = horizon[horizon.index(y) + 2]

                            else:
                                year_nn = y

                            df_yr = df2[
                                df2.index.get_level_values(year_ref).isin([y])
                            ].reindex(sorted(df2.columns), axis=1)

                            # Creates a list of years for which values need to be
                            # extrapolated.
                            yr_act_list = sorted(
                                [
                                    x
                                    for x in df_yr.columns
                                    if df_yr[x].isnull().any() and x <= yr_end and x > y
                                ]
                            )
                            if yr_act_list:
                                for yr_act in yr_act_list:
                                    # Extrapolation across the activity years
                                    df_next = f_slice(
                                        df2, idx, year_ref, [year_next], y
                                    )[yr_act]
                                    df_nn = f_slice(df2, idx, year_ref, [year_nn], y)[
                                        yr_act
                                    ]

                                    # If no data is available for the years_active
                                    # in the next two vintages, then the values
                                    # from the previous years for the same vintage
                                    # are used.
                                    # e.g. vintage = 2015, year_active is 2050
                                    # so if the 2050 value for the vintages 2020
                                    # and 2025 are empty, the 2045 and 2040 values
                                    # are used from the same vintage.
                                    if (
                                        df_next.empty
                                        or any([np.isnan(v) for v in df_next.values])
                                    ) and (
                                        df_nn.empty
                                        or any([np.isnan(v) for v in df_nn.values])
                                    ):
                                        df_next = df_yr[
                                            horizon[horizon.index(yr_act) - 1]
                                        ]
                                        df_nn = df_yr[
                                            horizon[horizon.index(yr_act) - 2]
                                        ]

                                    # Same rule as above is applied
                                    if (
                                        df_next.empty
                                        or any([np.isnan(v) for v in df_next.values])
                                    ) and not (
                                        df_nn.empty
                                        or any([np.isnan(v) for v in df_nn.values])
                                    ):
                                        df_next = df_yr[
                                            horizon[horizon.index(yr_act) - 1]
                                        ]
                                        df_nn = df_yr[
                                            horizon[horizon.index(yr_act) - 2]
                                        ]

                                    # Comment OFR - i dont understand logic.
                                    if not (
                                        df_next.empty
                                        or any([np.isnan(v) for v in df_next.values])
                                    ) and (
                                        df_nn.empty
                                        or any([np.isnan(v) for v in df_nn.values])
                                    ):
                                        df_nn = df_yr[
                                            horizon[horizon.index(yr_act) - 1]
                                        ]

                                    df_yr[yr_act] = intpol(
                                        df_next,
                                        f_index(df_nn, df_next),
                                        year_next,
                                        year_nn,
                                        y,
                                        dataframe=True,
                                    )

                            # end year
                            df_yr.loc[:, df_yr.columns > yr_end] = np.nan
                            df2.loc[df_yr.index, :] = df_yr
                            df2 = df2.reindex(sorted(df2.columns), axis=1)

            # -----------------------------------------------------
            # Adding missing values for extended vintage and active
            # years (before and after existing vintage years)
            # -----------------------------------------------------

            # New vintage years before the first existing vintage year
            year_list = sorted(
                [x for x in vtg_years if x < min(df_old["year_act"])], reverse=True
            )

            # Plus new vintage years after the last existing vintage year
            year_list = year_list + sorted(
                [x for x in vtg_years if x > max(df_old[year_ref])]
            )

            # For the parameter relation_activity additional data is needed
            # for all activity years of the vintage
            if parname == "relation_activity":
                year_list = year_list + sorted(
                    [
                        x
                        for x in horizon
                        if x <= max(act_years) and x > max(df_old["year_act"])
                    ]
                )

            # Add values for missing years
            if year_list:
                for y in year_list:
                    if parname == "relation_activity":
                        yr_end = y
                    else:
                        yr_end = vtg_act_yrs.loc[y, "act_years"]

                    if y not in df2.columns:
                        df2[y] = np.nan

                    # Finding two adjacent years for extrapolation
                    if y < min(df_old["year_act"]):
                        year_next = horizon[horizon.index(y) + 1]
                        if idx_memb(horizon, y, 2) in set(df_old[year_ref]):
                            year_nn = horizon[horizon.index(y) + 2]
                        else:
                            year_nn = year_next

                    else:
                        year_next = horizon[horizon.index(y) - 1]
                        if horizon.index(y) >= 2:
                            year_nn = horizon[horizon.index(y) - 2]
                        else:
                            year_nn = year_next

                        # Adding missing active years for this new vintage year
                        if yr_end not in df2.columns:
                            yr_next = horizon[horizon.index(yr_end) - 1]
                            yr_nn = horizon[horizon.index(yr_end) - 2]
                            df2[yr_end] = intpol(
                                df2[yr_next],
                                df2[yr_nn],
                                yr_next,
                                yr_nn,
                                y,
                                dataframe=True,
                            )
                            df2[yr_end].loc[
                                pd.isna(df2[yr_end]) & ~pd.isna(df2[yr_next])
                            ] = df2.loc[:, yr_next].copy()
                            # Removing extra values from previous vintage year
                            if len(df2[yr_end]) > 1:
                                df2[yr_end].loc[pd.isna(df2[yr_end].shift(+1))] = np.nan

                            if extrapol_neg:
                                dd = df2.loc[:, yr_next].copy()
                                df2[yr_end].loc[
                                    (df2[yr_end] < 0) & (df2[yr_next] >= 0)
                                ] = dd * extrapol_neg

                    # Extrapolate data
                    df_next = f_slice(df2, idx, year_ref, [year_next], y)
                    df_nn = f_slice(df2, idx, year_ref, [year_nn], y)

                    if parname == "relation_activity":
                        df_yr = df_next
                        df_yr[year_nn] = df_nn[year_nn]
                        df_yr = df_yr[sorted(df_yr.columns)]
                        if y < min(df_old["year_act"]):
                            df_yr = df_yr.interpolate(
                                axis=1, limit=1, limit_direction="backward"
                            )
                        else:
                            df_yr = df_yr.interpolate(axis=1, limit=1)

                        # To make sure no extra active years for new vintage
                        # years in the loop
                        df_yr.loc[:, (df_yr.columns > y) | (df_yr.columns < y)] = np.nan

                    else:
                        # Configuring new vintage year to be added to
                        # vintage years
                        df_yr = intpol(
                            df_next,
                            f_index(df_nn, df_next),
                            year_next,
                            year_nn,
                            y,
                            dataframe=True,
                        )

                        # Excluding parameters with two time index, but not
                        # across all active years
                        if parname not in ["relation_activity"]:
                            df_yr[year_next].loc[pd.isna(df_yr[year_next])] = (
                                f_slice(df2, idx, year_ref, [year_next], y)
                                .loc[:, year_next]
                                .copy()
                            )
                            if (
                                year_nn not in df_yr.columns
                                or df_yr[year_nn].loc[~pd.isna(df_yr[year_nn])].empty
                            ):
                                year_nn = year_next
                            df_yr[y].loc[pd.isna(df_yr[y])] = intpol(
                                df_yr[year_next],
                                df_yr[year_nn],
                                year_next,
                                year_nn,
                                y,
                                dataframe=True,
                            )

                        # Adding missing data for columns in the corner
                        if not df_yr.loc[pd.isna(df_yr[yr_end])].empty:
                            if not df_yr.dropna(axis=1).empty:
                                yr_last = max(df_yr.dropna(axis=1).columns)

                                for yr in [x for x in df_yr.columns if x > yr_last]:
                                    yr_next = horizon[horizon.index(yr) - 1]
                                    yr_nn = horizon[horizon.index(yr) - 2]
                                    df_yr.loc[:, yr] = intpol(
                                        df_yr[yr_next],
                                        df_yr[yr_nn],
                                        yr_next,
                                        year_nn,
                                        yr_end,
                                        dataframe=True,
                                    )

                        # To make sure no extra active years for new
                        # vintage years in the loop
                        df_yr.loc[:, (df_yr.columns > yr_end) | (df_yr.columns < y)] = (
                            np.nan
                        )

                    if y in set(df_old[year_ref]):
                        df2.loc[df2.index.isin(df_yr.index), :] = df_yr
                    else:
                        df2 = df2.append(df_yr)
                    df2 = df2.reindex(sorted(df2.columns), axis=1)
                    df2 = df2.reset_index().sort_values(idx).set_index(idx)

            # The final check in case the liftime is being reduced
            if year_ref == "year_vtg":
                for y in vtg_years:
                    df2.loc[
                        df2.index.get_level_values(year_ref).isin([y]),
                        df2.columns > act_years[vtg_years.index(y)],
                    ] = np.nan

            # Dataframe is turned from wide to long format
            df_new = pd.melt(
                df2.reset_index(),
                id_vars=idx,
                value_vars=[x for x in df2.columns if x not in idx],
                var_name="year_act",
                value_name="value",
            ).dropna(subset=["value"])

            # Dataframe is sorted
            df_new = df_new.sort_values(idx).reset_index(drop=True)

            # Any data not in the list of years to be modified is dropped
            if parname == "relation_activity":
                df_new = df_new.loc[
                    df_new[year_ref].isin(set(act_years + vtg_years + year_list))
                ]
            else:
                df_new = df_new.loc[df_new[year_ref].isin(vtg_years)]

            # Removing extra (old) vintage/activity years if desired
            if remove_rest and not test_run:
                scenario.remove_par(parname, df_old)
            if not test_run:
                if not remove_rest:
                    if parname == "relation_activity":
                        df_old = df_old.loc[
                            df_old[year_ref].isin(
                                set(act_years + vtg_years + year_list)
                            )
                        ].copy()
                    else:
                        df_old = df_old.loc[df_old[year_ref].isin(vtg_years)].copy()
                    scenario.remove_par(parname, df_old)
                if not df_new.empty:
                    scenario.add_par(parname, df_new)

            results[parname] = df_new.pivot_table(
                index=idx, columns="year_act", values="value"
            ).reset_index()

            _log(parname, tec, node)

    if not test_run and commit is True:
        scenario.commit("Scenario updated for new lifetime.")
    return results
