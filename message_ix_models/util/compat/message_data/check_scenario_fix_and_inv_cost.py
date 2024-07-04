import numpy as np
import pandas as pd
from message_ix_models import ScenarioInfo


def main(scen, vintaging=False, check_only=False, remove_zero=True, verbose=False):
    """Check and fix inv_cost and fix_cost.

    This module is designed to check over the parametrization of the
    `fix_cost` and `inv_cost`.
    The check is carried out per vintage, technology and region.
    Using the the message_ix utility `vintage_and_active_years`, the vintage-
    and activity-years for which there should be parameters available are retrieved.
    If the there are missing entries, these are added via interpolation for the
    vintage-years and are extended across activity-years in the case of `fix-cost`
    depending on whether vintaging is applied or not.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        Scenario for which the check should be carried out.
    vintaging : boolean (default=False)
        Option as to how vintage specific entries are to be extended over the
        activity-years for the `fix_cost`.
        If True, then entries over the activity years within a vintage will be
        constant.
        Otherwise, the entries for activity years will be assumed to be the same
        across all vintages.
    check_only : boolean (default=False)
        Option as whether to modify parameters to correct the issues, or whether
        to only check over the scenario.
    remove_zero : boolean (default=False)
        Option whether `zero` values can be removed for a given technology.
    verbose : booelan (default=False)
        Option whether to print log messages.
    """

    # Retrieve model years
    model_years = ScenarioInfo(scen).Y

    # Specify parameters which should be checked.
    # Currently, only `fix_cost` and `inv_cost` are covered.
    # A list is used for possible extension of this utility.
    parameters = ["inv_cost", "fix_cost"]

    with scen.transact(f"Correct parameters {parameters}."):
        for par in parameters:
            # -----------------------------------------------
            # Step 1.: Retrieve parameter data from scenario.
            # -----------------------------------------------
            df = scen.par(par)

            # All zero values are dropped from `fix_cost`.
            # Optionally, these could also be removed from the scenario.
            if par in ["fix_cost"]:
                if remove_zero:
                    scen.remove_par(par, df.loc[df.value == 0])
                df = df.loc[df.value != 0]

            # ---------------------------------------------
            # Step 2.: Iterate over nodes and technologies.
            # ---------------------------------------------
            for n in df.node_loc.unique():
                for t in df.loc[df.node_loc == n].technology.unique():
                    # Create filter for parametrized data.
                    loc_idx = (
                        (df.node_loc == n)
                        & (df.technology == t)
                        & (df.year_vtg.isin(model_years))
                    )

                    # ----------------------------
                    # Step 2.1.: Check `inv_cost`.
                    # ----------------------------
                    # For the parameter investment cost, a check is made to see that
                    # all required parameters are present for the model-years only
                    # i.e. all years >= `firstmodelyear`.
                    if par in ["inv_cost"]:
                        # Actual `year_vtg` are filtered for the technology.
                        obs = df.loc[loc_idx].year_vtg.unique().tolist()

                        # Expected `year_vtg` are retrieved and filtered.
                        try:
                            exp = (
                                scen.vintage_and_active_years((n, t), in_horizon=False)
                                .year_vtg.unique()
                                .tolist()
                            )
                            exp = [y for y in exp if y in model_years]
                        except Exception:
                            print(
                                f"Issues retrieving vintage_and_active_years for {t}",
                                f" in region {n}",
                            )
                            continue

                        # A check is made to ensure that the obeserved `year_vtg`
                        # match the expected `year_vtg`.
                        if sorted(exp) != sorted(obs):
                            assert f"{par} in region {n} for technology {t} do not match. Expected {exp} and found {obs}."

                    # ----------------------------
                    # Step 2.2.: Check `fix_cost`.
                    # ----------------------------
                    # For the parameter fix cost, a check is made to see that all
                    # required parameters are present for the model-years only
                    # i.e. all years >= `firstmodelyear`.
                    elif par in ["fix_cost"]:
                        # Actual `year_vtg` and `year_act` are filtered
                        # for the technology.
                        obs = df.loc[loc_idx].drop(
                            ["node_loc", "technology", "value", "unit"], axis=1
                        )

                        try:
                            exp = scen.vintage_and_active_years((n, t), in_horizon=True)
                        except Exception:
                            print(
                                "Issues retrieving vintage_and_active_years",
                                f" for {t} in region {n}",
                            )
                            continue

                        # Merge obvserved and expected results using the indicator
                        # option to analyse differences.
                        tmp = exp.merge(obs, how="outer", indicator=True)
                        tmp = tmp.loc[tmp["_merge"] != "both"]

                        # Handle differences
                        if not tmp.empty:
                            if verbose:
                                print(
                                    "Resolving differences for `fix_cost` of",
                                    f" technology {t} in region {n}.\n",
                                    tmp,
                                )

                            # Handle "right-only" issues.
                            # This indicates that there are excess values parametrized
                            # that do not correspond to the vintage/activity years as
                            # indicated by `vintage_and_active_years`.
                            # The excess values are filtered and removed.
                            if "right_only" in tmp["_merge"].unique():
                                tmp_right_only = tmp.loc[
                                    tmp["_merge"] == "right_only"
                                ].drop("_merge", axis=1)

                                # Filter out data to be removed.
                                remove_data = df.loc[loc_idx].merge(
                                    tmp_right_only, how="inner"
                                )
                                scen.remove_par(par, remove_data)

                            # Handle "left-only" issues.
                            # This indicates that there are values missing for
                            # vintage/activity years where there should be values as
                            # indicated `vintage_and_active_years`.

                            if "left_only" in tmp["_merge"].unique():
                                tmp_left_only = tmp.loc[
                                    tmp["_merge"] == "left_only"
                                ].drop("_merge", axis=1)

                                # Filter out data which needs to be extended.
                                add_data = df.loc[loc_idx]

                                # The dataframe is extended with vintage/activity years
                                # which need to be extended.
                                # Each entry will receive a Nan value, dealt with
                                # here-after.
                                for i, items in tmp_left_only.iterrows():
                                    add_row = add_data.loc[[add_data.index[0]]]
                                    add_row.year_vtg = items.year_vtg
                                    add_row.year_act = items.year_act
                                    add_row.value = np.nan
                                    add_data = (
                                        pd.concat([add_data, add_row])
                                        .reset_index()
                                        .drop("index", axis=1)
                                    )
                                add_data = add_data.sort_values(
                                    ["year_vtg", "year_act"]
                                ).set_index(["year_vtg"])

                                # Make a temporary dataframe select only the initial
                                # activity year for each vintage.
                                # These values are then interpolated.
                                # First and last vintages are "duplicates".
                                # NOTE: In some cases, because only filtering out
                                # values for the values relevant to the optimization
                                # time-period, there is a caveat.
                                # When the `firstmodelyear` is 2020, the technology
                                # parameters for the vintage 2015, will only be checked
                                # for the activity years from 2020 onwards; this means
                                # that for vtg:2015 and yact:2020, the value will be
                                # set to vtg:2020_yact:2020.
                                # This though is the same as what would result from
                                # backfill, because there are no values to interpolate.
                                tmp_add_data = pd.DataFrame()
                                for i in add_data.index.unique():
                                    tmp_add_data = pd.concat(
                                        [tmp_add_data, add_data.loc[[i]][:1]]
                                    )
                                tmp_add_data = (
                                    tmp_add_data.interpolate(
                                        method="index", limit_direction="both"
                                    )
                                    .reset_index()
                                    .set_index(["year_vtg", "year_act"])
                                )

                                # Combine dataframes and treat remaining `year_act`
                                # values per vintage.
                                add_data = add_data.reset_index().set_index(
                                    ["year_vtg", "year_act"]
                                )
                                add_data = tmp_add_data.combine_first(add_data)
                                for yv in tmp_left_only.year_vtg.unique():
                                    # If vintaging, then NaN Values for each vintage,
                                    # the activity-year values are set the same as
                                    # the vintage.
                                    if vintaging:
                                        update = (
                                            add_data.loc[yv]
                                            .fillna(method="ffill")
                                            .value
                                        )
                                    # If not vintaging, then the acitivty-year values
                                    # across all vintages are set to be the same.
                                    else:
                                        update = (
                                            add_data.loc(axis=0)[
                                                :, add_data.loc[yv].index
                                            ]
                                            .dropna()
                                            .reset_index()
                                            .drop("year_vtg", axis=1)
                                            .drop_duplicates()
                                            .set_index("year_act")
                                            .value
                                        )
                                    add_data.loc[yv, "value"].update(update)
                                scen.add_par(par, add_data.reset_index())
