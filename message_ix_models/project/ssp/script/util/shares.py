"""Functions used in :mod:`.project.ssp.script.scenarios`.

Originally added via :pull:`235`, cherry-picked and merged in :pull:`340`.

.. todo::
   - Move to another location as appropriate.
   - Add tests.
   - Improve documentation.
"""

import numpy as np
import pandas as pd


# FIXME Reduce complexity from 30 → ≤ 11
def main(  # noqa: C901
    scen,
    path_UE_share_input,
    ssp="SSP2",
    start_year=None,
    calibration_year=None,
    period_interpol=4,
    clean_relations=False,
    verbose=False,
):
    """Add share constraints for end-use technologies.

    The purpose of this script is to add share constraints for the end-use
    sectors, thereby replacing any exisiting relation based share constraints
    in the process.
    The share constraint parametrization is read from an Excel-file and has
    been setup so that it can be applied to all five SSPs. Some constraints
    are applicable for multiple SSPs and nodes.
    The share constraints are parameterized so that the total to which the
    share is applied is derived based on a level/commodity and the
    technologies contributing to a share are specified individually. Shares
    with a value of 1. are not added to the model. In the process of adding
    the share constraints, a check is undertaken to ensure that the share
    constraint values do do not violate the calibrated baseyear shares. If
    they do, then the baseyear share value is interpolated, so that the
    target-share value as specied in the Excel-file are gradually achieved
    over time.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        scenario to which changes should be applies
    ssp : string
        specify for which SSP the parameters should be added
    start_year : int
        specify the year as of which constraints should be added;
        If None, then `firstmodelyear`
    calibration_year : int
        specify the last year for data has been calibrated
        If None, the `firstmodelyear` -1
    period_intpol : int (default=4)
        the number of time periods (not years) over which deviations
        converge
    clean_relations : boolean
        option whether to entirely remove all relation based UE share
        constraints.
    verbose : boolean (default=False)
        option whether to primnt onscreen messages.
    """

    # Remove all existing UE_ growth constraints
    if clean_relations is True:
        with scen.transact("Remove UE relations"):
            remove_rel = [r for r in scen.set("relation") if r.find("UE_") >= 0]
            scen.remove_set("relation", remove_rel)

            remove_tec = [t for t in scen.set("technology") if t.find("useful_") >= 0]
            scen.remove_set("technology", remove_tec)

    # Retrieve share constraint input data
    data = pd.read_excel(path_UE_share_input)

    # Retrieve list of scenario years
    years = scen.set("year").tolist()

    # If start_year is None, set to `firstmodelyear`
    if start_year is None:
        start_year = scen.firstmodelyear

    # If calibration_year is None, set to `firstmodelyear`
    if calibration_year is None:
        calibration_year = years[years.index(scen.firstmodelyear) - 1]

    # Ensure that the data is relevant for the current SSP
    data_gen = data.loc[data.SSP == "all"]
    data_ssp = data.loc[data.SSP == ssp]

    for i in data_ssp.index:
        if data_ssp.loc[i, "node"] == "all":
            data_gen = data_gen.loc[
                ~(data_gen.share_name == data_ssp.loc[i, "share_name"])
            ]
        else:
            data_gen = data_gen.loc[
                ~(
                    (data_gen.share_name == data_ssp.loc[i, "share_name"])
                    & (data_gen.node == data_ssp.loc[i, "node"])
                )
            ]
    data = pd.concat([data_ssp, data_gen]).reset_index().drop("index", axis=1)

    # Start workflow to add parameters
    with scen.transact(""):
        for i in data.index:
            row = data.iloc[i]

            if row.share_name in scen.set("relation").tolist():
                if verbose:
                    print(f"Removing relation {row.share_name}")
                scen.remove_set("relation", row.share_name)

            # Skip if share is 100%
            if row.target_value == 1:
                continue

            # Derive individual elements required for parametrization
            # Assign share names
            share_name = row.share_name
            if share_name not in scen.set("shares").tolist():
                if verbose:
                    print(f"Adding share {row.share_name}")
                scen.add_set("shares", share_name)

            # Derive names of share total and share
            share_name_total = f"{share_name}_total"
            share_name_share = f"{share_name}_share"

            # Assign technologies
            tec_list_share = row.share_tec.split(",")

            # Ensure that the output of the technologies have the same output
            # as defined for "commodity" and "useful"
            check_output = scen.par("output", filters={"technology": tec_list_share})[
                ["commodity", "level"]
            ].drop_duplicates()
            assert row.commodity in check_output.commodity.tolist()
            assert row.level in check_output.level.tolist()

            # Retrieve all technologies which have an outout onto the desired
            # "commodity" and "level"
            output = scen.par(
                "output", filters={"level": row.level, "commodity": row.commodity}
            )
            tec_list_total = output.technology.unique().tolist()

            # Add technologies to new type_tec
            for tec in tec_list_total:
                cur_type_tec = scen.set(
                    "cat_tec", filters={"type_tec": share_name_total}
                ).technology.tolist()
                if tec not in cur_type_tec:
                    scen.add_cat("technology", share_name_total, tec)

            for tec in tec_list_share:
                cur_type_tec = scen.set(
                    "cat_tec", filters={"type_tec": share_name_share}
                ).technology.tolist()
                if tec not in cur_type_tec:
                    scen.add_cat("technology", share_name_share, tec)

            # Define nodes, if "all" then all are retrieved from the output
            # for which the target share technology is available
            if row.node == "all":
                nodes = output.loc[
                    output.technology.isin(tec_list_share)
                ].node_loc.unique()
            else:
                nodes = [f"R12_{r}" for r in row.node.split(",")]

            # Assign type_tec to map_shares_commodity_total
            for n in nodes:
                df = pd.DataFrame(
                    {
                        "shares": share_name,
                        "node_share": n,
                        "node": n,
                        "type_tec": share_name_total,
                        "mode": output.loc[
                            (output.technology.isin(tec_list_total))
                            & (output.node_loc == n)
                        ]["mode"]
                        .unique()
                        .tolist(),
                        "commodity": row.commodity,
                        "level": row.level,
                    }
                )
                scen.add_set("map_shares_commodity_total", df)

            # Assign type_tec to map_shares_commodity_total
            for n in nodes:
                df = pd.DataFrame(
                    {
                        "shares": share_name,
                        "node_share": n,
                        "node": n,
                        "type_tec": share_name_share,
                        "mode": output.loc[
                            (output.technology.isin(tec_list_share))
                            & (output.node_loc == n)
                        ]["mode"]
                        .unique()
                        .tolist(),
                        "commodity": row.commodity,
                        "level": row.level,
                    }
                )
                scen.add_set("map_shares_commodity_share", df)

            # Derive shares of activity in the calibration_year
            if calibration_year >= scen.firstmodelyear:
                par = "bound_activity_lo"
            else:
                par = "historical_activity"
            act_total = scen.par(
                par,
                filters={
                    "node_loc": nodes,
                    "technology": tec_list_total,
                    "year_act": calibration_year,
                },
            )
            act_total = act_total.groupby(["node_loc"]).sum(numeric_only=True)[
                ["value"]
            ]

            act_share = scen.par(
                "bound_activity_lo",
                filters={
                    "node_loc": nodes,
                    "technology": tec_list_share,
                    "year_act": calibration_year,
                },
            )
            act_share = act_share.groupby(["node_loc"]).sum(numeric_only=True)[
                ["value"]
            ]

            baseyear_share = round((act_share / act_total) * 1000) / 1000
            baseyear_share = baseyear_share.dropna()

            # Create timeseries for values
            share_type = (
                "share_commodity_lo"
                if row.share_type == "lower"
                else "share_commodity_up"
            )

            # If "baseyear" then add timeseries with baseyear values for all years
            if row.target_value == "baseyear":
                for n in nodes:
                    if n not in baseyear_share.index.values:
                        val = 0
                    else:
                        val = baseyear_share.loc[n].value
                    df = pd.DataFrame(
                        {
                            "shares": share_name,
                            "node_share": n,
                            "year_act": [
                                y for y in scen.set("year") if y >= start_year
                            ],
                            "time": "year",
                            "value": val,
                            "unit": "-",
                        }
                    )
                    scen.add_par(share_type, df)

            else:
                for n in nodes:
                    if row.target_value == "TS":
                        # Create timeseries dataframe
                        ts = pd.DataFrame(
                            row[[y for y in row.index if y in years]]
                        ).rename(columns={i: "value"})
                        ts.value = ts.value.astype(float)
                    else:
                        ts = pd.DataFrame(
                            {
                                "year_act": [y for y in years if y >= calibration_year],
                                "value": row.target_value,
                            }
                        ).set_index("year_act")
                    if n not in baseyear_share:
                        baseval = ts.iloc[0].value
                    else:
                        baseval = baseyear_share.loc[n].value
                    # Check if timeseries value is smaller than baseyear_share
                    check = True
                    if row.share_type == "lower":
                        # If lower, check if baseyear share value is smaller
                        # than the target value if it is, then the interpolate
                        # to gradually reduce the share.
                        check = baseval < ts.iloc[0].value
                    elif row.share_type == "upper":
                        # If upper, check if baseyear share value is larger
                        # than the target value if it is, then the interpolate
                        # to gradually increase the share.
                        check = baseval < ts.iloc[0].value
                    if check is False:
                        if calibration_year not in ts.index:
                            ts = pd.concat(
                                [
                                    pd.DataFrame(
                                        {"year": calibration_year, "value": [baseval]}
                                    ).set_index("year"),
                                    ts,
                                ]
                            )
                        else:
                            ts.loc[calibration_year, "value"] = baseval
                        ts.loc[
                            years[
                                years.index(calibration_year) + 1 : years.index(
                                    calibration_year
                                )
                                + period_interpol
                            ],
                            "value",
                        ] = np.nan

                        # Interpolate values
                        ts = ts.interpolate(method="index")

                    # Assign the remaining index values
                    ts = (
                        ts.assign(
                            shares=share_name, node_share=n, time="year", unit="-"
                        )
                        .reset_index()
                        .rename(columns={"index": "year_act"})
                    )

                    # Filter out years >= the start_year, so the year as of
                    # which parameters should be added
                    ts = ts.loc[ts.year_act >= start_year]
                    scen.add_par(share_type, ts)
