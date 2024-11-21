from ast import literal_eval
from typing import TYPE_CHECKING

import pandas as pd
import yaml
from message_ix import make_df

import message_ix_models.util
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import read_config
from message_ix_models.util import broadcast, same_node

if TYPE_CHECKING:
    from message_ix import Scenario

ssp_mode_map = {
    "SSP1": "CTS core",
    "SSP2": "RTS core",
    "SSP3": "RTS high",
    "SSP4": "CTS high",
    "SSP5": "RTS high",
    "LED": "CTS core",  # TODO: move to even lower projection
}

iea_elasticity_map = {
    "CTS core": (1.2, 0.25),
    "CTS high": (1.3, 0.48),
    "RTS core": (1.25, 0.35),
    "RTS high": (1.4, 0.54),
}


def gen_data_methanol(scenario: "Scenario") -> dict[str, pd.DataFrame]:
    """
    Generates data for methanol industry model

    Parameters
    ----------
    scenario: .Scenario
    """
    context = read_config()
    df_pars = pd.read_excel(
        message_ix_models.util.package_data_path(
            "material", "methanol", "methanol_sensitivity_pars.xlsx"
        ),
        sheet_name="Sheet1",
        dtype=object,
    )
    pars = df_pars.set_index("par").to_dict()["value"]
    if pars["mtbe_scenario"] == "phase-out":
        pars_dict = pd.read_excel(
            message_ix_models.util.package_data_path(
                "material", "methanol", "methanol_techno_economic.xlsx"
            ),
            sheet_name=None,
            dtype=object,
        )
    else:
        pars_dict = pd.read_excel(
            message_ix_models.util.package_data_path(
                "material", "methanol", "methanol_techno_economic_high_demand.xlsx"
            ),
            sheet_name=None,
            dtype=object,
        )

    for i in pars_dict.keys():
        pars_dict[i] = unpivot_input_data(pars_dict[i], i)
    # TODO: only temporary hack to ensure SSP_dev compatibility
    if "SSP_dev" in scenario.model:
        file_path = message_ix_models.util.package_data_path(
            "material", "methanol", "missing_rels.yaml"
        )

        with open(file_path, "r") as file:
            missing_rels = yaml.safe_load(file)
        df = pars_dict["relation_activity"]
        pars_dict["relation_activity"] = df[~df["relation"].isin(missing_rels)]

    default_gdp_elasticity_2020, default_gdp_elasticity_2030 = iea_elasticity_map[
        ssp_mode_map[context["ssp"]]
    ]
    df_final = material_demand_calc.gen_demand_petro(
        scenario, "methanol", default_gdp_elasticity_2020, default_gdp_elasticity_2030
    )
    df_final["value"] = df_final["value"].apply(
        lambda x: x * pars["methanol_resid_demand_share"]
    )
    pars_dict["demand"] = df_final

    return pars_dict


def broadcast_nodes(
    df_bc_node: pd.DataFrame,
    df_final: pd.DataFrame,
    node_cols: list[str],
    node_cols_codes: dict[str, pd.Series],
    i: int,
) -> pd.DataFrame:
    """
    Broadcast nodes that were stored in pivoted row

    Parameters
    ----------
    df_bc_node: pd.DataFrame
    df_final: pd.DataFrame
    node_cols: list[str]
    node_cols_codes: dict[str, pd.Series]
    i: int
    """
    if len(node_cols) == 1:
        if "node_loc" in node_cols:
            df_bc_node = df_bc_node.pipe(
                broadcast, node_loc=node_cols_codes["node_loc"]
            )
        if "node_vtg" in node_cols:
            df_bc_node = df_bc_node.pipe(
                broadcast, node_vtg=node_cols_codes["node_vtg"]
            )
        if "node_rel" in node_cols:
            df_bc_node = df_bc_node.pipe(
                broadcast, node_rel=node_cols_codes["node_rel"]
            )
        if "node" in node_cols:
            df_bc_node = df_bc_node.pipe(broadcast, node=node_cols_codes["node"])
        if "node_share" in node_cols:
            df_bc_node = df_bc_node.pipe(
                broadcast, node_share=node_cols_codes["node_share"]
            )
    else:
        df_bc_node = df_bc_node.pipe(broadcast, node_loc=node_cols_codes["node_loc"])
        if len(df_final.loc[i][node_cols].T.unique()) == 1:
            # df_bc_node["node_rel"] = df_bc_node["node_loc"]
            df_bc_node = df_bc_node.pipe(
                same_node
            )  # not working for node_rel in installed message_ix_models version
        else:
            if "node_rel" in list(df_bc_node.columns):
                df_bc_node = df_bc_node.pipe(
                    broadcast, node_rel=node_cols_codes["node_rel"]
                )
            if "node_origin" in list(df_bc_node.columns):
                df_bc_node = df_bc_node.pipe(
                    broadcast, node_origin=node_cols_codes["node_origin"]
                )
            if "node_dest" in list(df_bc_node.columns):
                df_bc_node = df_bc_node.pipe(
                    broadcast, node_dest=node_cols_codes["node_dest"]
                )
    return df_bc_node


def broadcast_years(
    df_bc_node: pd.DataFrame,
    yr_col_out: list[str],
    yr_cols_codes: dict[str, list[str]],
    col: str,
) -> pd.DataFrame:
    """
    Broadcast years that were stored in pivoted row
    Parameters
    ----------
    df_bc_node: pd.DataFrame
    yr_col_out: list[str]
    yr_cols_codes: ict[str, list[str]]
    col: str
    """
    if len(yr_col_out) == 1:
        yr_list = [i[0] for i in yr_cols_codes[col]]
        # print(yr_list)
        if "year_act" in yr_col_out:
            df_bc_node = df_bc_node.pipe(broadcast, year_act=yr_list)
        if "year_vtg" in yr_col_out:
            df_bc_node = df_bc_node.pipe(broadcast, year_vtg=yr_list)
        if "year_rel" in yr_col_out:
            df_bc_node = df_bc_node.pipe(broadcast, year_rel=yr_list)
        if "year" in yr_col_out:
            df_bc_node = df_bc_node.pipe(broadcast, year=yr_list)
        df_bc_node[yr_col_out] = df_bc_node[yr_col_out].astype(int)
    else:
        if "year_vtg" in yr_col_out:
            y_v = [str(i) for i in yr_cols_codes[col]]
            df_bc_node = df_bc_node.pipe(broadcast, year_vtg=y_v)
            df_bc_node["year_act"] = [
                literal_eval(i)[1] for i in df_bc_node["year_vtg"]
            ]
            df_bc_node["year_vtg"] = [
                literal_eval(i)[0] for i in df_bc_node["year_vtg"]
            ]
        if "year_rel" in yr_col_out:
            if "year_act" in yr_col_out:
                df_bc_node = df_bc_node.pipe(
                    broadcast, year_act=[i[0] for i in yr_cols_codes[col]]
                )
            df_bc_node["year_rel"] = df_bc_node["year_act"]
    return df_bc_node


def unpivot_input_data(df: pd.DataFrame, par_name: str) -> pd.DataFrame:
    """
    Unpivot data that is already contains columns for respective MESSAGEix parameter
    Parameters
    ----------
    df: pd.DataFrame
        DataFrame containing parameter data with year and node values pivoted
    par_name: str
        name of MESSAGEix parameter
    """
    df_final = df
    df_final_full = pd.DataFrame()

    for i in df_final.index:
        # parse strings of node columns to dictionary
        node_cols = [i for i in df_final.columns if "node" in i]
        remove = ["'", "[", "]", " "]
        node_cols_codes = {}
        for col in node_cols:
            node_cols_codes[col] = pd.Series(
                "".join(x for x in df_final.loc[i][col] if x not in remove).split(",")
            )

        # create dataframe with required columns
        df_bc_node = make_df(par_name, **df_final.loc[i])

        # collect year values from year columns
        yr_cols_codes = {}
        yr_col_inp = [i for i in df_final.columns if "year" in i]
        yr_col_out = [i for i in df_bc_node.columns if "year" in i]
        df_bc_node[yr_col_inp] = df_final.loc[i][yr_col_inp].values

        # broadcast in node dimensions
        for colname in node_cols:
            df_bc_node[colname] = None
        df_bc_node = broadcast_nodes(
            df_bc_node, df_final, node_cols, node_cols_codes, i
        )

        # brodcast in year dimensions
        for col in yr_col_inp:
            yr_cols_codes[col] = literal_eval(df_bc_node[col].values[0])
            df_bc_node = broadcast_years(df_bc_node, yr_col_out, yr_cols_codes, col)
        df_bc_node[yr_col_out] = df_bc_node[yr_col_out].astype(int)

        df_final_full = pd.concat([df_final_full, df_bc_node])
    df_final_full = df_final_full.drop_duplicates().reset_index(drop=True)

    # special treatment for relation_activity dataframes:
    # relations parametrization should only contain columns where node_rel == node_loc
    # except for relations acting on a geographic global level
    # (where node_rel == "R**_GLB")
    if par_name == "relation_activity":
        df_final_full = df_final_full.drop(
            df_final_full[
                (df_final_full.node_rel.values != "R12_GLB")
                & (df_final_full.node_rel.values != df_final_full.node_loc.values)
            ].index
        )
    return make_df(par_name, **df_final_full)
