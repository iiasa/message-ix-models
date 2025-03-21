from __future__ import annotations
import pandas as pd
import numpy as np
from message_ix import make_df
from message_ix_models.util import package_data_path
from message_ix_models.model.water.data.demands import read_water_availability
from message_ix_models.model.water.data.supply_tooling import update_e_flow_rule, apply_e_flow_dsl_transformations

# ----------------- E-flow rule templates -----------------
E_FLOW_RULES = [
    {
        "type": "bound_activity_lo",
        "technology": "return_flow",
        "mode": "M1",
        "value": None,  # set dynamically based on environmental flow calculations
        "unit": "km3/year",
    }
]

def add_e_flow(context: "Context") -> dict[str, pd.DataFrame]:
    # add env flows using dsl transformation
    results = {}
    info = context["water build info"]

    # read water avail data
    df_sw, df_gw = read_water_availability(context)
    
    # load basin delineation data
    file_basin = f"basins_by_region_simpl_{context.regions}.csv"
    path_basin = package_data_path("water", "delineation", file_basin)
    df_x = pd.read_csv(path_basin)

    # prepare water demand df
    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=df_sw["value"],
        unit="km3/year",
    )
    dmd_df = dmd_df[dmd_df["year"] >= 2025].reset_index(drop=True)
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x >= 0 else 0)
    # sort demand data for alignment
    dmd_df.sort_values(by=["node", "year"], inplace=True)
    dmd_df.reset_index(drop=True, inplace=True)


    # read env flow data (annual or monthly)
    file_env = (
        f"e-flow_{context.RCP}_{context.regions}.csv"
        if "year" in context.time
        else f"e-flow_5y_m_{context.RCP}_{context.regions}.csv"
    )
    path_env = package_data_path("water", "availability", file_env)
    df_env = pd.read_csv(path_env)
    df_env.drop(["Unnamed: 0"], axis=1, inplace=True)
    df_env.index = df_x["BCU_name"].index
    df_env = df_env.stack().reset_index()
    df_env.columns = pd.Index(["Region", "years", "value"])
    df_env.sort_values(["Region", "years", "value"], inplace=True)
    df_env.fillna(0, inplace=True)
    df_env.reset_index(drop=True, inplace=True)
    df_env["year"] = pd.DatetimeIndex(df_env["years"]).year
    df_env["time"] = "year" if "year" in context.time else pd.DatetimeIndex(df_env["years"]).month

    # remove unnecessary typecasting to mimic legacy behavior
    df_env["Region"] = df_env["Region"].map(df_x["BCU_name"])
    # sort env flow data on key columns to ensure alignment with dmd_df
    df_env.sort_values(by=["Region", "year"], inplace=True)
    df_env.reset_index(drop=True, inplace=True)

    df_env2210 = df_env[df_env["year"] == 2100].copy()
    df_env2210["year"] = 2110
    df_env = pd.concat([df_env, df_env2210])
    df_env = df_env[df_env["year"].isin(info.Y)]

    # Ensure indices match before numpy where operation
    # Add 'B' prefix to df_env regions to match dmd_df format
    df_env["node"] = "B" + df_env["Region"].astype(str)
    

    dmd_df = dmd_df.set_index(["node", "year"])
    df_env = df_env.set_index(["node", "year"])  # Use same index structure
    common_idx = dmd_df.index.intersection(df_env.index)
    
    
    dmd_df = dmd_df.loc[common_idx].reset_index()
    df_env = df_env.loc[common_idx].reset_index().rename(columns={"node": "node_loc"})

    if context.SDG != "baseline":

        eflow_rule = update_e_flow_rule(
            E_FLOW_RULES[0],
            extra={
                "year_act": df_env["year"],
                "time": df_env["time"],
                "value": df_env["value"]
            },
            broadcast_updates={
                "node_loc": df_env["node_loc"]
            },
            update_value_fn=lambda r: np.where(
                r["value"] >= 0.7 * dmd_df["value"], 0.7 * dmd_df["value"], r["value"]
            ),
        )
        
        transformed = apply_e_flow_dsl_transformations([eflow_rule])
        
        results["bound_activity_lo"] = transformed

    return results