# -*- coding: utf-8 -*-
"""
Rebase historical oil and gas extraction to IEA World Energy Balances

The SSP2 v6.6 values for the last historical period (2025) are model output, not
statistics, and they are off in both directions (e.g. NAM gas falls 13% from 2020
while actual output rose; MEA gas rises 43%). Because ``<fuel>_extr_mpen`` carries
a 5%/yr ``growth_activity_up`` anchored on that history, the 2025 values also cap
the first model period's extraction in each region.

For each region and fuel this scales, in the rebase year, by the ratio of IEA
production to the model's total over grades:

- ``historical_activity`` of every grade (``<fuel>_extr_<n>``) and of
  ``<fuel>_extr_mpen``, so the grade mix is unchanged;
- ``historical_extraction`` of the matching resource commodities
  (``crude_<n>`` / ``gas_<n>``);
- ``historical_new_capacity`` of each grade in the rebase year is raised where its
  activity rises, so the higher history is backed by capacity in the first model
  period. It is never lowered.
"""
import logging

import pandas as pd
import message_ix

from message_ix_models.tools.bilateralize.historical_calibration import (
    iea_production_by_region,
)

log = logging.getLogger(__name__)

#: Grade technology and resource commodity patterns per fuel.
FUELS = {
    "oil": {"tec": r"^oil_extr_\d+$", "mpen": "oil_extr_mpen", "commodity": r"^crude_\d+$"},
    "gas": {"tec": r"^gas_extr_\d+$", "mpen": "gas_extr_mpen", "commodity": r"^gas_\d+$"},
}


def _iea_target(iea: pd.DataFrame, year: int) -> pd.DataFrame:
    """Latest IEA year <= `year` per (node, fuel), with the year used."""
    iea = iea[iea["year"] <= year]
    idx = iea.groupby(["node", "fuel"])["year"].idxmax()
    return iea.loc[idx].rename(columns={"value": "iea", "year": "iea_year"})


def rebase_extraction_history(
    scenario: message_ix.Scenario,
    project_name: str,
    config_name: str,
    year: int = 2025,
    fuels: tuple = ("oil", "gas"),
    message_regions: str = "R12",
) -> pd.DataFrame:
    """
    Rebase `year` oil/gas extraction history in `scenario` to IEA WEB production.

    Args:
        scenario: Scenario to edit (checked in/out here)
        project_name: Passed to the IEA WEB loader for the data path
        config_name: Passed to the IEA WEB loader for the data path
        year: Historical period to rebase
        fuels: Subset of :data:`FUELS`
        message_regions: Regional resolution for the IEA aggregation

    Returns:
        One row per (node, fuel): model and IEA values, IEA year used, factor.
    """
    iea = iea_production_by_region(
        message_regions=message_regions,
        project_name=project_name,
        config_name=config_name,
    )
    target = _iea_target(iea, year)

    ha = scenario.par("historical_activity", filters={"year_act": [year, 2020]})
    he = scenario.par("historical_extraction", filters={"year": [year]})
    hnc = scenario.par("historical_new_capacity", filters={"year_vtg": [year]})
    cf = scenario.par("capacity_factor", filters={"year_vtg": [year], "year_act": [year]})
    duration = float(
        scenario.par("duration_period", filters={"year": [year]})["value"].iloc[0]
    )

    edits = {"historical_activity": [], "historical_extraction": [],
             "historical_new_capacity": []}
    summary = []
    for fuel in fuels:
        spec = FUELS[fuel]
        grades = ha[ha["technology"].str.match(spec["tec"])]

        # Diagnostic: does the WEB product definition match the model's 2020 history?
        m2020 = grades[grades["year_act"] == 2020].groupby("node_loc")["value"].sum()
        w2020 = iea[(iea["fuel"] == fuel) & (iea["year"] == 2020)].set_index("node")["value"]
        print(f"[{fuel}] 2020 check, IEA WEB / model historical_activity:")
        print((w2020 / m2020).round(2).to_string())

        grades = grades[grades["year_act"] == year]
        model = grades.groupby("node_loc")["value"].sum().rename("model").reset_index()
        df = model.merge(
            target[target["fuel"] == fuel], left_on="node_loc", right_on="node", how="left"
        )
        for _, r in df.iterrows():
            node = r["node_loc"]
            if pd.isna(r["iea"]) or r["model"] <= 0:
                log.warning(f"No rebase for {node} {fuel}: model={r['model']}, iea={r['iea']}")
                continue
            if r["iea_year"] < year:
                log.warning(f"{node} {fuel}: IEA WEB has no {year}; using {int(r['iea_year'])}")
            factor = r["iea"] / r["model"]
            summary.append(dict(node=node, fuel=fuel, model=r["model"], iea=r["iea"],
                                iea_year=int(r["iea_year"]), factor=factor))

            # Grades and mpen activity
            act = ha[(ha["node_loc"] == node) & (ha["year_act"] == year)
                     & (ha["technology"].str.match(spec["tec"])
                        | (ha["technology"] == spec["mpen"]))].copy()
            old = act.groupby("technology")["value"].sum()
            act["value"] *= factor
            edits["historical_activity"].append(act)

            # Resource extraction
            ext = he[(he["node"] == node) & he["commodity"].str.match(spec["commodity"])].copy()
            ext["value"] *= factor
            edits["historical_extraction"].append(ext)

            # Capacity: add what the extra activity needs, never remove
            if factor > 1:
                delta = old[old.index.str.match(spec["tec"])] * (factor - 1)
                cfn = cf[cf["node_loc"] == node].groupby("technology")["value"].first()
                add = (delta / cfn.reindex(delta.index).fillna(1.0) / duration).rename("add")
                cap = hnc[hnc["node_loc"] == node].set_index("technology")
                new = add.to_frame().join(cap[["value", "unit"]], how="left")
                new["value"] = new["value"].fillna(0) + new["add"]
                new["unit"] = new["unit"].fillna("GW")
                new = new.reset_index().assign(node_loc=node, year_vtg=year)
                edits["historical_new_capacity"].append(
                    new[["node_loc", "technology", "year_vtg", "value", "unit"]])

    with scenario.transact(f"Rebase {year} oil/gas extraction history to IEA WEB"):
        for par, frames in edits.items():
            if frames:
                scenario.add_par(par, pd.concat(frames))

    summary = pd.DataFrame(summary)
    print(f"Rebased {year} extraction history (GWa):")
    print(summary.round(2).to_string(index=False))
    return summary
