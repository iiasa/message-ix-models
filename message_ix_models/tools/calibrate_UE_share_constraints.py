import numpy as np
import pandas as pd

from .get_historical_years import main as get_historical_years
from .get_optimization_years import main as get_optimization_years

from .intpol import intpol

relation_set = {
    "UE_res_comm": [["sp", "th"], "useful_res_comm"],
    "UE_feedstock": [["feedstock"], "useful"],
    "UE_industry": [["sp", "th"], "useful_industry"],
    "UE_transport": [["transport"], "useful"],
}


def _check_bound(x):
    if np.isnan(x.value) or np.isnan(x.share):
        return True
    # Check to see if the bound implies lower bound
    if x.bound < 0:
        if x.share < abs(x.bound):
            return True
        else:
            return False
    elif x.bound > 0:
        if x.share > abs(x.bound):
            return False
        else:
            return True


def _add_data(scenario, row, period_intpol, relation_year, verbose):
    """Creates final dataframe with new data and adds to scenario.
    For minimum shares, the new shares is replaced over the
    entire timeperiod.
    For all other shares, the new shares converge to the original
    values, defined by "period_intpol"

    Parameters
    ----------
    scenario : :class:`message_ix.Scenario`
        scenario to which changes should be applies
    row : dataframe with single row
        row based on which the new values in the modela re derived
    period_intpol : int
        the number of periods over which new shares should converge
        with original shares
    relation_year : int
        the year for which the share constraint should be
        adjusted.
    verbose : boolean (default=False)
        option whether to primnt onscreen messages.
    """
    # Retrieve current bounds
    tmp = scenario.par(
        "relation_activity",
        {
            "node_loc": [row.node_loc],
            "relation": [row.relation],
            "technology": [row.technology],
        },
    )
    # Filter bounds so that only those as of the 'relation_year'
    # are changed.
    tmp = tmp[tmp.year_act >= relation_year]
    tmp_print = tmp.copy()

    share = round(row.share, 4)
    if tmp.value.unique()[0] < 0:
        share *= -1

    # Values added over entire time horizon
    if "Minimum" in row.relation:
        tmp.value = tmp.value.replace(row.bound, share)
    # Values converge to original shares
    else:
        years = tmp.year_act.to_list()[: period_intpol + 1]
        yr_prev = years[0]
        yr_next = years[-1]
        v_prev = share
        v_next = float(tmp.loc[tmp.year_act == yr_next, "value"])
        for y in years:
            if y == yr_prev:
                tmp.loc[tmp.year_act == y, "value"] = share
            elif y == yr_next:
                continue
            else:
                value = round(intpol(v_prev, v_next, yr_prev, yr_next, y), 4)
                tmp.loc[tmp.year_act == y, "value"] = value
    if verbose:
        tmp_print = tmp_print.rename(columns={"value": "previous"})
        tmp_print["new"] = tmp.value
        print(tmp_print)
    scenario.add_par("relation_activity", tmp)


def main(
    scenario, historical_year=None, relation_year=None, period_intpol=4, verbose=False
):
    """Checks UE shares constraints against historical data.

    The function checks that the share constraints as of the first
    model year and therefore of relevance to the optimization matches
    the last historical timestep. This should avoid any sudden in- or
    decreases in activity in the near term due to ill calibrated
    share constraints. The number of time-periods over which the shares
    calculated for the last historical time-period converge with the
    original values can be defined.

    Parameters
    ----------
    scenario : :class:`message_ix.Scenario`
        scenario to which changes should be applies
    historical_year : int
        the last historical time period
    relation_year : int
        the year for which the share constraint should be
        adjusted.
    period_intpol : int (default=4)
        the number of time periods (not years) over which deviations
        converge
    verbose : boolean (default=False)
        option whether to primnt onscreen messages.
    """

    # Assigns the historical and relation_year if not defined
    if not historical_year:
        historical_year = get_historical_years(scenario)[-1]
    if not relation_year:
        relation_year = get_optimization_years(scenario)[0]

    exceedings = []

    # Iterate over four UE main sectors (excl. biomass_nc)
    for r in relation_set:
        relations = [
            x for x in scenario.set("relation").tolist() if any(s in x for s in [r])
        ]
        relations_sets = relation_set[r][0]
        relation_technology = relation_set[r][1]

        # Iterate over UE sub-sectors e.g. thermal/specific
        for sets in relations_sets:
            # Creates name of 'parent' relations
            parent_rel = [r for r in relations if r.split("_")[-1] == sets]

            # Retrieves the name of all technologies which write into
            # the parent relation.
            rel_tec = [
                t
                for t in scenario.par("relation_activity", {"relation": parent_rel})
                .technology.unique()
                .tolist()
                if t.find(relation_technology) < 0
            ]

            # Retrieve historical activity of all relevant technologies
            hist_activity = (
                scenario.par(
                    "historical_activity",
                    {"technology": rel_tec, "year_act": [historical_year]},
                )
                .set_index(["node_loc", "technology", "year_act", "mode"])
                .drop(["time", "unit"], axis=1)
            )
            hist_activity = hist_activity[hist_activity.value != 0]

            # Retrieves the all share constraints not 100%
            # e.g. share of solar or fuelcells in res-comm specific
            bound = scenario.par(
                "relation_activity",
                {
                    "technology": ["{}_{}".format(relation_technology, sets)],
                    "year_rel": [relation_year],
                },
            ).drop(["node_rel", "year_rel", "technology", "mode", "unit"], axis=1)
            bound = bound[bound.value != -1]

            # Iterates over each of the relevant shares constraints
            # to calculate whether the current share constraint is binding
            for rel in bound.relation.unique().tolist():
                # Retrieves a list of all technologies to
                # which make up the share
                tec = [
                    t
                    for t in scenario.par("relation_activity", {"relation": [rel]})
                    .technology.unique()
                    .tolist()
                    if t != "{}_{}".format(relation_technology, sets)
                ]

                tmp = hist_activity.reset_index()
                tmp = tmp[tmp["technology"].isin(tec)]
                if not tmp.empty:
                    tmp = tmp.groupby(["node_loc"]).sum().drop(["year_act"], axis=1)
                    # Calculate the total activity
                    tmp["total"] = (
                        hist_activity.reset_index().groupby(["node_loc"]).sum().value
                    )

                    # Calcaulte the activity of the technologies to which
                    # the share constraint applies
                    tmp["share"] = tmp["value"] / tmp["total"]

                    # Add the yearly bound
                    tmp["bound"] = (
                        bound[bound["relation"] == rel].set_index(["node_loc"]).value
                    )

                    # Check whether the bound is binding
                    tmp["bound_within_limit"] = tmp.apply(_check_bound, axis=1)

                    exceed = tmp[tmp["bound_within_limit"] == False].reset_index()
                    if not exceed.empty:
                        exceed = exceed[["node_loc", "bound", "share"]]
                        exceed["relation"] = rel
                        exceed["technology"] = "{}_{}".format(relation_technology, sets)
                        exceedings.append(exceed)
    if len(exceedings) != 0:
        exceedings = pd.concat(exceedings, sort=True).reset_index().drop("index", axis=1)
        if verbose:
            print(exceedings)

        scenario.check_out()
        exceedings.apply(
            lambda row: _add_data(scenario, row, period_intpol, relation_year, verbose),
            axis=1,
        )
        scenario.commit("Updated EU share constraints")
