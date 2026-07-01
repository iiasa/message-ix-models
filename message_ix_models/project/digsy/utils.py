from typing import TYPE_CHECKING, Literal

import pandas as pd
import pint_pandas  # noqa: F401

if TYPE_CHECKING:
    from message_ix import Scenario

DIGSY_SCENS_v1 = ["BEST", "WORST", "baseline", "BESTEST", "WORSTEST"]
DIGSY_SCENS_v2 = [
    "Reference",
    "Enable-Cautious-R12Base",
    "Enable-Moderate-R12Base",
    "Enable-Extreme-R12Base",
    "Undermine-Cautious-R12Base",
    "Undermine-Moderate-R12Base",
    "Undermine-Extreme-R12Base",
    "Enable-Cautious-R12Converge",
    "Enable-Moderate-R12Converge",
    "Enable-Extreme-R12Converge",
    "Undermine-Cautious-R12Converge",
    "Undermine-Moderate-R12Converge",
    "Undermine-Extreme-R12Converge",
]
DIGSY_SCENS = Literal[*(DIGSY_SCENS_v2 + DIGSY_SCENS_v1)]


def fe_to_ue(df: pd.DataFrame, scen: "Scenario") -> pd.DataFrame:
    inp = scen.par("input", filters={"technology": "sp_el_RC"})
    inp = (
        inp[["node_loc", "year_act", "value"]]
        .rename(columns={"node_loc": "node", "year_act": "year", "value": "efficiency"})
        .set_index(["node", "year"])
    )
    df_new = (
        df.set_index(["node", "year"])
        .join(inp)
        .assign(value=lambda x: x["value"] / x["efficiency"])
        .reset_index()
        .drop(columns=["efficiency"])
    )
    return df_new


def adjust_act_calib(ict: pd.DataFrame, scen: "Scenario"):
    for par in ["bound_activity_up", "bound_activity_lo"]:
        bound = scen.par(par, filters={"technology": "sp_el_RC"})
        ict_tot = (
            ict.rename(columns={"node": "node_loc", "year": "year_act"})
            .groupby(
                [
                    "year_act",
                    "node_loc",
                ]
            )
            .sum(numeric_only=True)
            .loc[2020]
        )
        new_bound = (
            bound.set_index([i for i in bound.columns if i != "value"])
            .sub(ict_tot)
            .reset_index()
        ).dropna()
        with scen.transact():
            scen.add_par(par, new_bound)


def rename_trp_scenarios(df: pd.DataFrame) -> pd.DataFrame:
    mapy = {
        "BEST": "Enable",
        "WORST": "Undermine",
        "S": "Extreme",
        "M": "Moderate",
        "C": "Cautious",
        "converge": "R12Converge",
    }

    scen_cols = df["digsy_scenario"].str.split("-", expand=True)
    scen_cols[0] = scen_cols[0].str.replace("DIGSY", "")
    scen_cols[1] = scen_cols[1].map(mapy, na_action="ignore").fillna("")
    scen_cols[2] = scen_cols[2].map(mapy, na_action="ignore").fillna("")
    scen_cols[3] = scen_cols[3].map(mapy, na_action="ignore").fillna("R12Base")
    scen_cols = (
        scen_cols[0]
        .astype(str)
        .str.cat(scen_cols[1].astype(str))
        .str.cat(scen_cols[2].astype(str), sep="-")
        .str.cat(scen_cols[3].astype(str), sep="-")
    )
    scen_cols = scen_cols.str.replace("BASE--R12Base", "Reference").rename(
        "digsy_scenario"
    )
    df = pd.concat([df.drop("digsy_scenario", axis=1), scen_cols], axis=1)
    return df
