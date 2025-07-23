from typing import Literal

import pandas as pd
import pint_pandas  # noqa: F401
from message_ix.util import make_df

from message_ix_models.util import private_data_path


def read_industry_modifiers(scenario: Literal["BEST", "WORST"]) -> pd.DataFrame:
    path = private_data_path("digsy", "DIGSY-MESSAGE_Industry_v4.xlsx")

    df = pd.read_excel(path, sheet_name="Final_modifier")

    subsector_message_map = {
        "Other": {"commodity": ["i_therm", "i_spec"], "par": "demand"},
        # other industry (do we treat intensity reduction correctly here?)
        # iron and steel
        "integrated cold rolling and finishing": {
            "technology": "finishing_steel",
            "par": "input",
            "commodity": "electr",
        },
        "cold rolling": {
            "technology": "finishing_steel",
            "par": "input",
            "commodity": "electr",
        },
        # chemicals
        "ethylene production": {
            "technology": "steam_cracker_petro",
            "par": "input",
            "commodity": ["ht_heat", "electr"],
        },
        # 'pure terephthalic acid (PTA) production': {"technology":
        # "steam_cracker_petro", "par": "input""commodity":"comm"},
        # aluminum
        "calcination (alumina refinery)": {
            "technology": "refining_aluminum",
            "par": "input",
            "commodity": "ht_heat",
        },
        # No energy representation in MESSAGE for this process
        # (not even finishing at the moment)
        # "die-casting (holding energy)": {
        #     "technology": "",
        #     "par": "input",
        #     "commodity": "comm"},
        "smelting (all plant types)": {
            "technology": "prebake_aluminum",
            "par": "input",
            "commodity": "electr",
        },
        # cement
        "vertical mill (dry process)": {
            "technology": "raw_meal_prep_cement",
            "par": "input",
            "commodity": "electr",
        },
        "clinker production (all kilns)": {
            "technology": "clinker_dry_cement",
            "par": "input",
            "commodity": ["ht_heat", "electr"],
        },
        "cement raw-mix blending": {
            "technology": "raw_meal_prep_cement",
            "par": "input",
            "commodity": "electr",
        },
        "cement mill (grinding)": {
            "technology": ["grinding_vertmill_cement", "grinding_ballmill_cement"],
            # only for ballmill for now, vertmill seems not to be used by MESSAGE
            "par": "input",
            "commodity": "electr",
        },
    }

    subsector_message_map = pd.DataFrame(subsector_message_map).T
    df = df.set_index("subsector").join(subsector_message_map).reset_index()
    return df[df["scenario"] == scenario]


def read_ict_demand() -> pd.DataFrame:
    path = private_data_path("digsy", "DIGSY-MESSAGE_ICTs.xls")
    dfs = pd.read_excel(path, sheet_name=None)

    scenario = "DIGSY-BEST"
    scen_map = {
        "DIGSY-BEST": "Lower Bound",
        "DIGSY-WORST": "Upper Bound",
        "PRISMA": "Mean",
    }
    ssp = "SSP2"

    df2030 = (
        dfs["2030"]
        .drop(columns=["Parent_Region"])
        .set_index(["Region", "Year"])["Allocated_TWh"]
    )
    df_proj = (
        dfs[scen_map[scenario]]
        .drop(columns=["Parent_Region", "Source"])
        .set_index(["Scenario", "Region", "Year"])["Allocated_TWh"]
    )
    df = pd.concat([df2030, df_proj.loc[ssp]])
    df.name = "value"

    df = make_df(
        "demand",
        **df.astype("pint[TWh]")
        .pint.to("GWa")
        .pint.magnitude.to_frame()
        .assign(unit="GWa")
        .reset_index()
        .rename(columns={"Region": "node", "Year": "year"}),
        commodity="rc_ict",
        level="final",
        time="year",
    ).head()
    return df


if __name__ == "__main__":
    read_industry_modifiers("BEST")
