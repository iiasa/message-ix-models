import message_ix_models.util
import pandas as pd

from message_ix import make_df
from message_ix_models.util import (
broadcast,
same_node,
package_data_path,

)

from message_ix_models.model.material.util import read_config
from ast import literal_eval

context = read_config()

def gen_data_methanol_new(scenario):
    df_pars = pd.read_excel(
        package_data_path(
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
        pars_dict[i] = broadcast_reduced_df(pars_dict[i], i)

    return pars_dict


def broadcast_reduced_df(df, par_name):
    df_final = df
    df_final_full = pd.DataFrame()

    for i in df_final.index:
        remove = ["'", "[", "]", " "]
        node_cols = [i for i in df_final.columns if "node" in i]
        node_cols_codes = {}
        for col in node_cols:
            node_cols_codes[col] = pd.Series(''.join(x for x in df_final.loc[i][col] if not x in remove).split(","))

        df_bc_node = make_df(par_name, **df_final.loc[i])
        # brodcast in year dimensions
        yr_cols_codes = {}
        yr_col_inp = [i for i in df_final.columns if "year" in i]
        yr_col_out = [i for i in df_bc_node.columns if "year" in i]
        df_bc_node[yr_col_inp] = df_final.loc[i][yr_col_inp]

        for colname in node_cols:
            df_bc_node[colname] = None
        # broadcast in node dimensions
        if len(node_cols) == 1:
            if "node_loc" in node_cols:
                df_bc_node = df_bc_node.pipe(broadcast, node_loc=node_cols_codes["node_loc"])
            if "node_vtg" in node_cols:
                df_bc_node = df_bc_node.pipe(broadcast, node_vtg=node_cols_codes["node_vtg"])
            if "node_rel" in node_cols:
                df_bc_node = df_bc_node.pipe(broadcast, node_rel=node_cols_codes["node_rel"])
            if "node" in node_cols:
                df_bc_node = df_bc_node.pipe(broadcast, node=node_cols_codes["node"])
            if "node_share" in node_cols:
                df_bc_node = df_bc_node.pipe(broadcast, node_share=node_cols_codes["node_share"])
        else:
            df_bc_node = df_bc_node.pipe(broadcast, node_loc=node_cols_codes["node_loc"])
            if len(df_final.loc[i][node_cols].T.unique()) == 1:
                # df_bc_node["node_rel"] = df_bc_node["node_loc"]
                df_bc_node = df_bc_node.pipe(
                    same_node)  # not working for node_rel in installed message_ix_models version
            else:
                if "node_rel" in list(df_bc_node.columns):
                    df_bc_node = df_bc_node.pipe(broadcast, node_rel=node_cols_codes["node_rel"])
                if "node_origin" in list(df_bc_node.columns):
                    df_bc_node = df_bc_node.pipe(broadcast, node_origin=node_cols_codes["node_origin"])
                if "node_dest" in list(df_bc_node.columns):
                    df_bc_node = df_bc_node.pipe(broadcast, node_dest=node_cols_codes["node_dest"])

        for col in yr_col_inp:
            yr_cols_codes[col] = literal_eval(df_bc_node[col].values[0])
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
                df_bc_node["year_act"] = [literal_eval(i)[1] for i in df_bc_node["year_vtg"]]
                df_bc_node["year_vtg"] = [literal_eval(i)[0] for i in df_bc_node["year_vtg"]]
            if "year_rel" in yr_col_out:
                if "year_act" in yr_col_out:
                    df_bc_node = df_bc_node.pipe(broadcast, year_act=[i[0] for i in yr_cols_codes[col]])
                df_bc_node["year_rel"] = df_bc_node["year_act"]
            # return df_bc_node
        # df_bc_node["year_rel"] = df_bc_node["year_act"]
        df_bc_node[yr_col_out] = df_bc_node[yr_col_out].astype(int)

        df_final_full = pd.concat([df_final_full, df_bc_node])
    df_final_full = df_final_full.drop_duplicates().reset_index(drop=True)
    if par_name == "relation_activity":
        df_final_full = df_final_full.drop(df_final_full[(df_final_full.node_rel.values != "R12_GLB") &
                                                         (df_final_full.node_rel.values != df_final_full.node_loc.values)].index)
    return make_df(par_name, **df_final_full)
