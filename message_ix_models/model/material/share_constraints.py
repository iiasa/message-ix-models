from typing import TYPE_CHECKING, List, Literal

import message_ix
import pandas as pd
from message_data.tools.utilities import get_nodes
from message_ix import make_df
from share_constraints_constants import other_ind_th_tecs

from message_ix_models.util import broadcast, same_node

if TYPE_CHECKING:
    from message_ix import Scenario


def gen_com_share_df(
    shr_name: str, df_vals: pd.DataFrame, type: Literal["up", "lo"] = "up"
) -> pd.DataFrame:
    """
    Generates DataFrame for "share_commodity_up/lo" parameter of MESSAGEix

    Parameters
    ----------
    shr_name: str
        name of the share constraint
    df_vals: pd.DataFrame
        DataFrame with columns ["node", "Value"]
    type
        "lo" for minimum constraint and "up" for maximum constraint

    Returns
    -------
    pd.DataFrame
        in MESSAGEix "share_commodity_up/lo" parameter format
    -------
    """
    df_share_com_lo = make_df(f"share_commodity_{type}", **df_vals)
    df_share_com_lo["time"] = "year"
    df_share_com_lo["shares"] = shr_name
    df_share_com_lo["unit"] = "-"

    return df_share_com_lo


def add_new_share_cat(
    scen: "Scenario",
    shr_name: str,
    type_tec_all_name: str,
    type_tec_shr_name: str,
    all_tecs: List[str],
    shr_tecs: List[str],
):
    """

    Parameters
    ----------
    scen
        Scenario to add new share categories to
    shr_name
        name of share
    type_tec_all_name
        name of technology group used to calculate denominator for share constraint
    type_tec_shr_name
        name of technology group used to calculate numerator for share constraint
    all_tecs
        List of technologies that should belong to "type_tec_all_name"
    shr_tecs
        List of technologies that should belong to "type_tec_shr_name"
    """
    scen.check_out()
    scen.add_set("shares", shr_name)
    scen.add_cat("technology", type_tec_all_name, all_tecs)
    scen.add_cat("technology", type_tec_shr_name, shr_tecs)
    scen.commit(
        f"New share: {shr_name} registered. Total and share technologies "
        f"({type_tec_all_name} & {type_tec_shr_name}) category added."
    )


def remove_share_cat(
    scen: "Scenario",
    shr_name: str,
    type_tec_all_name: str,
    type_tec_shr_name: str,
    all_tecs: List[str],
    shr_tecs: List[str],
) -> None:
    """

    Parameters
    ----------
    scen
        Scenario to add new share categories to
    shr_name
        name of share
    type_tec_all_name
        name of technology group used to calculate denominator for share constraint
    type_tec_shr_name
        name of technology group used to calculate numerator for share constraint
    all_tecs
        List of technologies that should belong to "type_tec_all_name"
    shr_tecs
        List of technologies that should belong to "type_tec_shr_name"
    """
    scen.check_out()
    scen.remove_set("shares", shr_name)
    scen.remove_cat("technology", type_tec_all_name, all_tecs)
    scen.remove_cat("technology", type_tec_shr_name, shr_tecs)
    scen.commit(
        f"Share: {shr_name} deleted. Total and share technologies "
        f"({type_tec_all_name} & {type_tec_shr_name}) category removed."
    )


def gen_comm_map(
    scen: "Scenario",
    shr_const_name: str,
    type_tec: str,
    tecs: List[str],
    nodes: List[str],
) -> pd.DataFrame:
    """

    Parameters
    ----------
    scen
    shr_const_name
    type_tec
    tecs
    nodes

    Returns
    -------

    """
    df_set_all = pd.DataFrame(
        {
            "shares": [shr_const_name],
            "node_share": None,
            "node": None,
            "type_tec": type_tec,
            # 'mode': None,
            "commodity": None,
        }
    )

    df_tec = scen.par("input", filters={"technology": tecs})

    df_set_all = (
        df_set_all.pipe(broadcast, node=nodes)
        .pipe(same_node, from_col="node")
        .pipe(broadcast, commodity=df_tec["commodity"].unique())
    )

    df_set_all = (
        df_set_all.set_index("commodity")
        .join(df_tec.set_index("commodity")[["level", "mode"]])
        .reset_index()
        .drop_duplicates()
    )
    # df_set_all = df_set_all.set_index("commodity").join(df_tec.set_index(
    # "commodity")["mode"]).reset_index().drop_duplicates()
    return df_set_all


def gen_comm_shr_map(
    scen: "Scenario",
    cnstrnt_name: str,
    type_tec_all_name: str,
    type_tec_shr_name: str,
    tecs_all: List[str],
    tecs_shr: List[str],
    nodes: str or List[str] = "all",
) -> [pd.DataFrame, pd.DataFrame]:
    """

    Parameters
    ----------
    scen
    cnstrnt_name
    type_tec_all_name
    type_tec_shr_name
    tecs_all
    tecs_shr
    nodes

    Returns
    -------
    [pd.DataFrame, pd.DataFrame]
    -------
    """
    if nodes == "all":
        nodes = get_nodes(scen)
        nodes = [i for i in nodes if "GLB" not in i]
    elif nodes not in get_nodes(scen):
        print(
            f"The provided nodes: {nodes} are not contained in this scenario. "
            f"Valid nodes of this scenario are: {get_nodes(scen)}"
        )
        return

    df_all_set = gen_comm_map(scen, cnstrnt_name, type_tec_all_name, tecs_all, nodes)
    df_shr_set = gen_comm_map(scen, cnstrnt_name, type_tec_shr_name, tecs_shr, nodes)
    return (
        df_all_set,
        df_shr_set,
    )


def gen_comm_shr_par(
    scen: "Scenario",
    cname: str,
    shr_vals_df: pd.DataFrame,
    shr_type: Literal["up", "lo"] = "up",
    years: str or List[int] = "all",
) -> pd.DataFrame:
    """Generates data frame for "share_commodity_up/lo" parameter with given values for
    node_share and broadcasts them for given "years".

    Parameters
    ----------
    scen: .Scenario
        used if years == "all" to obtain model years for column "year"
    cname: str
        code to use for "share" column
    shr_vals_df: pd.DataFrame
        data frame with columns ["node_share", "value"]
    shr_type: str
        "up" or "lo"
    years: str or list of int
        "all" to generate for all optimization years of scen or list of years

    Returns
    -------
    pd.DataFrame
        with "share_commodity_up/lo" columns
    """
    req_cols = ["node_share", "value"]
    check_cols = any(item in shr_vals_df for item in req_cols)
    if not check_cols:
        raise ValueError(f"shr_vals_df does not have the columns {req_cols}")
    df_final = shr_vals_df.copy(deep=True)
    if isinstance(years, str):
        if years == "all":
            years = scen.yv_ya()["year_act"].drop_duplicates()
    if "year_act" not in shr_vals_df.columns:
        df_final["year_act"] = None
        df_final = df_final.pipe(broadcast, year_act=years)

    df_shares = gen_com_share_df(cname, df_final, shr_type)
    return df_shares


def add_comm_share(
    scen: "Scenario",
    name: str,
    set_tot: str,
    set_share: str,
    all_tecs: List[str],
    shr_tecs: List[str],
    shr_vals: pd.DataFrame,
    shr_type: Literal["up", "lo"] = "up",
    years: List[str] = "all",
):
    """
    Convenience function that adds commodity share constraint to a scenario.

    This process requires a numer of steps:
    1) Register name of share in "shares" set
    2a) Register new "type_tec" mapping of technologies that should be considered to
            calculate the "total"
    2b) Register new "type_tec" mapping of technologies that should be considered to
            calculate the "share"
    3) Add "map_shares_commodity_share/total" mapping:
        a) Obtain involved commodities by inspecting "input" of the list of technologies
            for both "total" and "share" technologies
        b) Create row for each combination of nodes and commodity
        c) Add generated DataFrames with scenario.add_cat()
    4) Add share constraint parametrization:
        a) Read values from "shr_vals" argument for each node
        b) Duplicate regional entry for each specified year in "years"
        c) Add generated DataFrames with scenario.add_par()

    Parameters
    ----------
    scen
    name
    set_tot
    set_share
    all_tecs
    shr_tecs
    shr_vals
    shr_type
    years
    """
    print(f"Adding share constraint: {name}.")

    add_new_share_cat(scen, name, set_tot, set_share, all_tecs, shr_tecs)

    scen.check_out()
    com_map_tot, com_map_shr = gen_comm_shr_map(
        scen, name, set_tot, set_share, all_tecs, shr_tecs
    )
    scen.add_set("map_shares_commodity_share", com_map_shr)
    scen.add_set("map_shares_commodity_total", com_map_tot)

    com_shr_par = gen_comm_shr_par(scen, name, shr_vals, shr_type=shr_type, years=years)
    scen.add_par(f"share_commodity_{shr_type}", com_shr_par)

    scen.commit(f"Added commodity share constraint: {name}")


def remove_comm_share(
    scen: "Scenario",
    name: str,
    set_tot: str,
    set_share: str,
    all_tecs: List[str],
    shr_tecs: List[str],
):
    """
    Convenience function that removes commodity share constraint
    that was previously added with .add_comm_share() from a scenario

    Parameters
    ----------
    scen: .Scenario
    name: str

    set_tot: str

    set_share: str

    all_tecs: list of str

    shr_tecs: list of str

    """
    print(f"Removing share constraint: {name}.")

    remove_share_cat(scen, name, set_tot, set_share, all_tecs, shr_tecs)

    scen.check_out()
    com_map_tot, com_map_shr = gen_comm_shr_map(
        scen, name, set_tot, set_share, all_tecs, shr_tecs
    )
    scen.remove_set("map_shares_commodity_share", com_map_shr)
    scen.remove_set("map_shares_commodity_total", com_map_tot)


def add_foil_shr_constraint():
    shr_const = "share_low_lim_foil_ind"
    type_tec_shr = "foil_cement"
    type_tec_tot = "all_cement"

    from share_constraints_constants import foil_ind_tecs_ht, non_foil_ind_tecs_ht

    df_furn_cement = pd.read_csv(
        r"C:/Users\maczek\PycharmProjects\IEA_activity_calib_SSP/furnace_foil_cement_share.csv",
        usecols=[0, 2],
    )
    df_furn_cement.columns = ["node_share", "value"]
    all_ind_tecs = {
        a[0]: [a[1], *b[1]]
        for a, b in zip(foil_ind_tecs_ht.items(), non_foil_ind_tecs_ht.items())
    }

    foil_sectors = set(all_ind_tecs.keys())
    foil_sectors.remove("resins")

    model = ""
    scenario = ""
    mp = Platform()
    scen = message_ix.Scenario(mp, model, scenario)
    sc_clone = scen.clone(scen.model, scen.scenario + "foil_furn", keep_solution=False)

    for sec in foil_sectors:
        df_sec_foil_shr = df_furn_cement = pd.read_csv(
            rf"C:/Users\maczek\PycharmProjects\IEA_activity_calib_SSP/furnace_foil_{sec}_share.csv",
            usecols=[0, 2],
        )
        df_furn_cement.columns = ["node_share", "value"]
        add_comm_share(
            sc_clone,
            f"{sec}_foil",
            f"cat_{sec}_total",
            f"cat_{sec}_foil",
            all_ind_tecs[sec],
            foil_ind_tecs_ht[sec],
            df_furn_cement,
            years=sc_clone.yv_ya()["year_act"].drop_duplicates()[1:],
        )


def add_coal_constraint(scen):
    name = "UE_industry_th_coal"
    share_reg_values = pd.read_csv(
        "C:/Users/maczek/PycharmProjects/IEA-statistics-analysis/notebooks/coal_i_shares_2020.csv"
    )
    add_comm_share(
        scen,
        name,
        f"{name}_total",
        f"{name}_share",
        other_ind_th_tecs,
        ["coal_i"],
        share_reg_values,
        years=scen.yv_ya()["year_act"].drop_duplicates()[1:],
    )


if __name__ == "__main__":
    add_coal_constraint()

    # shr_const = "share_low_lim_foil_ind"
    # type_tec_shr = "foil_cement"
    # type_tec_tot = "all_cement"
    #
    # from ixmp import Platform
    # from message_ix import Scenario
    # from share_constraints_constants import foil_ind_tecs_ht, non_foil_ind_tecs_ht
    #
    # # df_furn_cement = pd.read_csv(
    # #     r"C:/Users\maczek\PycharmProjects\IEA_activity_calib_SSP/furnace_foil_cement_share.csv",
    # #     usecols=[0, 2],
    # # )
    # # df_furn_cement.columns = ["node_share", "value"]
    # all_ind_tecs = {
    #     a[0]: [a[1], *b[1]]
    #     for a, b in zip(foil_ind_tecs_ht.items(), non_foil_ind_tecs_ht.items())
    # }
    #
    # foil_sectors = set(all_ind_tecs.keys())
    # foil_sectors.remove("resins")
    #
    # model = ""
    # scenario = ""
    # mp = Platform()
    # scen = message_ix.Scenario(mp, model, scenario)
    # sc_clone = scen.clone(scen.model, scen.scenario + "foil_furn", keep_solution=False)
    #
    # for sec in foil_sectors:
    #     df_sec_foil_shr = df_furn_cement = pd.read_csv(
    #         rf"C:/Users\maczek\PycharmProjects\IEA_activity_calib_SSP/furnace_foil_{sec}_share.csv",
    #         usecols=[0, 2],
    #     )
    #     df_furn_cement.columns = ["node_share", "value"]
    #     add_comm_share(
    #         sc_clone,
    #         f"{sec}_foil",
    #         f"cat_{sec}_total",
    #         f"cat_{sec}_foil",
    #         all_ind_tecs[sec],
    #         foil_ind_tecs_ht[sec],
    #         df_furn_cement,
    #         years=sc_clone.yv_ya()["year_act"].drop_duplicates()[1:],
    #     )
