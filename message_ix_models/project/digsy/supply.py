from typing import TYPE_CHECKING

import pandas as pd
from message_ix import make_df

from message_ix_models.util import package_data_path, same_node

if TYPE_CHECKING:
    from message_ix import Scenario


def add_reserve_margin_constraint() -> None:
    raise NotImplementedError


def read_peak_load_data() -> pd.DataFrame:
    path = package_data_path("digsy", "sector_map.csv")
    path2 = package_data_path("digsy", "sector_peak_load_factors.csv")
    path3 = package_data_path("digsy", "sector_coincidence_factors.csv")
    df = pd.read_csv(path)
    peak_map = pd.read_csv(path2)
    coninc_map = pd.read_csv(path3, index_col=0)
    df = (
        df.merge(peak_map, on="sector")
        .set_index(["sector", "technology"])
        .mul(coninc_map)
        .dropna()
        .droplevel("sector")
    )
    return df


def query_elec_inp(scen: "Scenario"):
    df = scen.par("input", filters={"commodity": "electr"})
    df.set_index([i for i in df.columns if i not in ["value"]], inplace=True)
    return df


def remove_elec_t_d_res_marg(scen: "Scenario") -> None:
    with scen.transact():
        df = scen.par(
            "relation_activity",
            filters={"relation": "res_marg", "technology": "elec_t_d"},
        )
        df = df[df["year_act"] > 2025]
        scen.remove_par("relation_activity", df)


def gen_res_marg_demand_tec_data(
    scen: "Scenario", contingency_factor=0.15
) -> pd.DataFrame:
    factors = read_peak_load_data()
    vals = (
        query_elec_inp(scen)
        .mul(factors)
        .mul(-(1 + contingency_factor))
        .dropna()
        .reset_index()
    )
    rel = (
        make_df("relation_activity", **vals, relation="res_marg")
        .pipe(same_node)
        .assign(year_rel=lambda x: x.year_act)
    )
    rel = rel[rel["year_act"] > 2025]
    return rel


def gen_oper_res_demand_tec_data(scen: "Scenario", scaling_factor=0.17) -> pd.DataFrame:
    factors = read_peak_load_data()
    vals = query_elec_inp(scen).mul(factors).mul(scaling_factor).dropna().reset_index()
    rel = (
        make_df("relation_activity", **vals, relation="oper_res")
        .pipe(same_node)
        .assign(year_rel=lambda x: x.year_act)
    )
    return rel


def gen_res_marg_demand_par_data(data, contingency_factor=0.15) -> pd.DataFrame:
    df = read_peak_load_data().assign()
    raise NotImplementedError


def gen_sector_tec_list() -> pd.DataFrame:
    path = package_data_path("digsy", "elec_consumers.csv")
    path2 = package_data_path("digsy", "elec_consumers_secondary.csv")
    df = pd.concat([pd.read_csv(path), pd.read_csv(path2)])
    from collections import defaultdict

    # default factory always returns "industry"
    secmap = defaultdict(
        lambda: "industry",
        {
            "rc": "rc_therm",
            "RC": "rc_spec",
            "ccs": "CCS",
            "co2scr": "CCS",
            "trp": "transport",
        },
    )
    df = df.assign(sector=lambda x: x.technology.str.split("_").str[-1].map(secmap))
    df.sort_values(["sector", "technology"]).to_csv("sector_map.csv", index=False)
    return df


def change_res_marg(scen: "Scenario") -> None:
    remove_elec_t_d_res_marg(scen)
    rel = gen_res_marg_demand_tec_data(scen, 0.15)
    with scen.transact():
        scen.add_par("relation_activity", rel)
