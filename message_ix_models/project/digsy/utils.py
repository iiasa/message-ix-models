# if TYPE_CHECKING:
from typing import Literal

import message_ix
import pandas as pd
import pint_pandas  # noqa: F401

DIGSY_SCENS = Literal["BEST", "WORST", "baseline", "BESTEST", "WORSTEST"]


def fe_to_ue(df: pd.DataFrame, scen: message_ix.Scenario) -> pd.DataFrame:
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


def adjust_act_calib(ict: pd.DataFrame, scen: message_ix.Scenario):
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
