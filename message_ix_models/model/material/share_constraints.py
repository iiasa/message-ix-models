from typing import TYPE_CHECKING, Any, List, Literal

import message_ix
import pandas as pd
from ixmp import Platform
from message_ix import make_df
from pandas import DataFrame

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.share_constraints_constants import (
    other_ind_th_tecs,
)
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

if TYPE_CHECKING:
    from message_ix import Scenario


def gen_com_share_df(
    shr_name: str, df_vals: pd.DataFrame, type: Literal["up", "lo"] = "up"
) -> pd.DataFrame:
    """Generate DataFrame for ``share_commodity_up/lo`` parameter.

    Parameters
    ----------
    shr_name
        name of the share constraint
    df_vals
        DataFrame with columns ["node", "Value"]
    type
        "lo" for minimum constraint and "up" for maximum constraint

    Returns
    -------
    pd.DataFrame
        ``share_commodity_up/lo`` parameter data
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
    """Register new share name and technology mappings to scenario.

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
    """Remove name from ``shares`` and ``type_tec`` categories from scenario.

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
    """Generate ``map_shares_commodity_total/share`` for technology ``input`` commodity.

    Parameters
    ----------
    scen
    shr_const_name
        name of share
    type_tec
        name of technology group used to calculate numerator or denominator for share
    tecs
        list of technologies that should belong to ``type_tec``
    nodes
        list of nodes to create mapping for
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
    nodes: Union[str, List[str]] = "all",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate ``map_shares_commodity_total`` and ``map_shares_commodity_share``.

    Parameters
    ----------
    scen
    cnstrnt_name
        name of share
    type_tec_all_name
        ``type_tec`` name for technologies used to calculate denominator of share
    type_tec_shr_name
        ``type_tec`` name for technologies used to calculate numerator of share
    tecs_all
        list of technologies for ``type_tec`` type_tec_all
    tecs_shr
        list of technologies for ``type_tec`` type_tec_shr
    nodes
        list of nodes to create mapping for or "all" for all nodes of scenario
    """
    s_info = ScenarioInfo(scen)
    if nodes == "all":
        nodes = nodes_ex_world(s_info.N)
    elif nodes not in nodes_ex_world(s_info.N):
        print(
            f"The provided nodes: {nodes} are not contained in this scenario. "
            f"Valid nodes of this scenario are: {nodes_ex_world(s_info.N)}"
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
    years: Union[str, List[int]] = "all",
) -> DataFrame:
    """Generates data for ``share_commodity_up/lo`` parameter.

    Parameters
    ----------
    scen
        used if years == "all" to obtain model years for column "year"
    cname
        code to use for "share" column
    shr_vals_df
        data frame with columns ["node_share", "value"]
    shr_type
        "up" or "lo"
    years
        "all" to generate for all optimization years of scen or list of years
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
    years: Union[str, List[int]] = "all",
) -> None:
    """Convenience function that adds a commodity share constraint to a scenario.

    This process requires a numer of steps:

    1. Register name of share in ``shares`` set
    2. Register new ``type_tec`` mapping of technologies that should be considered to
       calculate the ``total``
    3. Register new ``type_tec`` mapping of technologies that should be considered to
       calculate the ``share``
    4. Add ``map_shares_commodity_share/total`` mapping
        a. Obtain involved commodities by inspecting ``input`` of the list of
           technologies for both "total" and "share" technologies
        b. Create row for each combination of nodes and commodity
        c. Add generated mapping to ``scen``

    5. Add share constraint parametrization
        a. Read values from ``shr_vals`` argument for each node
        b. Duplicate regional entry for each specified year in ``years``
        c. Add generated data to ``scen``
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
) -> None:
    """Convenience function that removes commodity share constraint.

    Parameters
    ----------
    scen
    name
    set_tot
    set_share
    all_tecs
    shr_tecs
    """
    print(f"Removing share constraint: {name}.")

    remove_share_cat(scen, name, set_tot, set_share, all_tecs, shr_tecs)

    scen.check_out()
    com_map_tot, com_map_shr = gen_comm_shr_map(
        scen, name, set_tot, set_share, all_tecs, shr_tecs
    )
    scen.remove_set("map_shares_commodity_share", com_map_shr)
    scen.remove_set("map_shares_commodity_total", com_map_tot)


def add_foil_shr_constraint() -> None:
    """Generate fuel oil share constraint for MESSAGEix-Materials industry sectors.
    ** Not used in model build at the moment. **

    """
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
        df_sec_foil_shr = pd.read_csv(
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


def add_industry_coal_shr_constraint(scen: "Scenario") -> None:
    """Add an upper share constraint for coal use in residual industry sector."""
    name = "UE_industry_th_coal"
    share_reg_values = pd.read_csv(
        package_data_path("material", "other", "coal_i_shares_2020.csv")
    )
    add_comm_share(
        scen,
        name,
        f"{name}_total",
        f"{name}_share",
        other_ind_th_tecs,
        ["coal_i"],
        share_reg_values,
        years=scen.yv_ya()["year_act"].drop_duplicates()[2:],
    )


def get_ssp_low_temp_shr_up(s_info: ScenarioInfo, ssp) -> pd.DataFrame:
    """Generate SSP-specific parametrization for ``UE_industry_th_low_temp_heat``.

    Updates the original constraint values of MESSAGEix-GLOBIOM to reflect structural
    differences in MESSAGEix-Materials industry sector based on SSP narrative.
    """
    lt_heat_shr_start = 0.35
    ssp_lt_heat_shr_end = {
        "SSP1": 0.65,
        "SSP2": 0.5,
        "SSP3": 0.35,
        "SSP4": 0.6,
        "SSP5": 0.5,
        "LED": 0.65,
    }
    end_year = {
        "SSP1": 2040,
        "SSP2": 2055,
        "SSP3": 2055,
        "SSP4": 2045,
        "SSP5": 2050,
        "LED": 2035,
    }
    start_year = 2030
    end_years = pd.DataFrame(index=list(end_year.keys()), data=end_year.values())
    end_vals = pd.DataFrame(
        index=list(ssp_lt_heat_shr_end.keys()), data=ssp_lt_heat_shr_end.values()
    )
    val_diff = end_vals - lt_heat_shr_start
    year_diff = end_years - start_year
    common = {
        "shares": "UE_industry_th_low_temp_heat",
        "time": "year",
        "unit": "-",
        "value": lt_heat_shr_start,
    }
    df = make_df("share_commodity_up", **common)
    df = df.pipe(broadcast, node_share=nodes_ex_world(s_info.N)).pipe(
        broadcast,
        year_act=[i for i in s_info.yv_ya.year_act.unique() if i >= start_year],
    )

    def get_shr(row):
        if row["year_act"] <= end_year[ssp]:
            val = (
                row["value"]
                + (row["year_act"] - start_year)
                * (val_diff / year_diff).loc[ssp].values[0]
            )
        else:
            val = ssp_lt_heat_shr_end[ssp]
        return val

    df = df.assign(value=df.apply(lambda x: get_shr(x), axis=1))
    return df
