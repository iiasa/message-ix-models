import logging
from typing import TYPE_CHECKING

import message_ix
import pandas as pd
from message_ix import make_df

from message_ix_models import Context, Spec
from message_ix_models.model.build import apply_spec
from message_ix_models.util import (
    ScenarioInfo,
    broadcast,
    merge_data,
    nodes_ex_world,
    private_data_path,
    same_node,
)

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import ParameterData

target_commodities = ["cement", "steel", "aluminum"]
log = logging.getLogger(__name__)


scen_map = {
    "R": "BAU-SSP2-baseline-SSP2-CT-BAU",
    "N": "Narrow-digsyC-Medium-medSharing-CC-SSP2",
    "S": "Slow-SSP2-baseline-SSP2-CT-highLife",
    "C": "Combined-digsyC-medRecycle-medSharing-CC-highLife",
}


def retrieve_total_demand(scen: "Scenario"):
    mat_demand = scen.par("demand", {"commodity": target_commodities})
    return mat_demand


def format_data(df: pd.DataFrame):
    name_mat_map = {
        "Aluminum": "Non-Ferrous Metals|Aluminum",
        "Concrete": "Non-Metallic Minerals|Cement",
        "Iron_steel": "Iron and Steel|Steel",
    }
    var_dmnd = "Material Demand|{material}|Transportation|Infrastructure"
    var_scrap = "Scrap|{material}|Transportation|Infrastructure"

    new_df = pd.DataFrame()
    for material in ["Aluminum", "Concrete", "Iron_steel"]:
        df_mat = df[df.Variable.str.contains(material)]
        # convert concrete to cement assuming 17.5% of concrete is cement
        if material == "Concrete":
            df_mat = (df_mat.set_index(list(df_mat.columns[:5])) * 0.15).reset_index()
        df_demand = (
            df_mat[df_mat.Variable.str.contains("Demand")]
            .groupby("Region")
            .sum(numeric_only=True)
            .assign(Variable=var_dmnd.format(material=name_mat_map[material]))
        )
        df_scrap = (
            df_mat[df_mat.Variable.str.contains("Release")]
            .groupby("Region")
            .sum(numeric_only=True)
            .assign(Variable=var_scrap.format(material=name_mat_map[material]))
        )
        new_df = pd.concat([new_df, df_demand, df_scrap])

    new_df = new_df.reset_index().assign(Region=lambda x: "R12_" + x["Region"])
    return new_df


def get_intensity(
    io: pd.Series | pd.DataFrame, area: pd.Series | pd.DataFrame
) -> pd.DataFrame:
    df_par_inp = (
        io.div(area).melt(ignore_index=False, var_name="year_act").reset_index()
    )
    df_par_inp["commodity"] = df_par_inp["Variable"].str.split("|").str[2].str.lower()
    df_par_inp.drop(columns=["Variable"], inplace=True)
    df_par_inp = df_par_inp.rename(columns={"Region": "node_loc"})
    return df_par_inp


def gen_par(df: pd.DataFrame, par: str, dims: dict) -> "ParameterData":
    return {
        par: make_df(par, **df, **dims)
        .pipe(same_node)
        .assign(year_vtg=lambda x: x["year_act"])
    }


def calc_adjusted_total_demands(df_scen, scen) -> "ParameterData":
    mat_demand = retrieve_total_demand(scen)

    df_par = format_data(df_scen).melt(
        id_vars=["Region", "Variable"], var_name="Year", value_name="Value"
    )

    df_par_demand = df_par[df_par["Variable"].str.startswith("Material Demand|")]
    df_par_demand["commodity"] = (
        df_par_demand["Variable"].str.split("|").str[2].str.lower()
    )
    df_par_demand.drop(columns=["Variable"], inplace=True)
    df_par_demand = df_par_demand.rename(
        columns={"Value": "value", "Region": "node", "Year": "year"}
    ).assign(time="year", unit="t", level="demand")

    inf = df_par_demand.set_index([i for i in mat_demand.columns if i != "value"])
    tot = mat_demand.set_index([i for i in mat_demand.columns if i != "value"])
    tot_adjusted = tot.sub(inf).assign(value=lambda x: x.clip(lower=0)).reset_index()
    return {"demand": tot_adjusted}


def gen_demands(area: pd.DataFrame) -> "ParameterData":
    df_dem_area = area.melt(ignore_index=False, var_name="year").reset_index()
    df_dem_area = df_dem_area.rename(columns={"Region": "node"})
    dims = {
        "time": "year",
        "unit": "bn m2/yr",
        "level": "demand",
        "commodity": "infra_trp_area",
    }
    dmnd_area = make_df("demand", **df_dem_area, **dims)
    return {"demand": dmnd_area}


def gen_io(results: dict, df_scen: pd.DataFrame, area, s_info: "ScenarioInfo"):
    ### generate IO for infrastructure construction
    glomis_formatted = format_data(df_scen)

    demand_pivoted = (
        glomis_formatted[glomis_formatted["Variable"].str.contains("Demand")]
        .groupby(["Variable", "Region"])
        .sum(numeric_only=True)
    )
    common = {
        "technology": "construction_infra_trp",
        "mode": "M1",
        "time_origin": "year",
        "time_dest": "year",
        "time": "year",
    }
    dims_in = {
        "unit": "t",
        "level": "product",
    }
    intensity = get_intensity(demand_pivoted, area)

    dims_out = {
        "unit": "t",
        "level": "end_of_life",
    }
    scrap_pivoted = (
        glomis_formatted[glomis_formatted["Variable"].str.contains("Scrap")]
        .groupby(["Variable", "Region"])
        .sum(numeric_only=True)
    )
    scrap_int = get_intensity(scrap_pivoted, area)

    dims_dem = {
        "level": "demand",
        "value": 1.0,
        "unit": "bn m2/yr",
    }
    out_area = (
        make_df(
            "output",
            commodity="infra_trp_area",
            **dims_dem,
            **common,
            node_loc=nodes_ex_world(s_info.N),
        )
        .pipe(same_node)
        .pipe(broadcast, year_act=s_info.Y)
        .assign(node_dest=lambda x: x["node_loc"], year_vtg=lambda x: x["year_act"])
    )
    merge_data(results, {"output": out_area})
    merge_data(results, gen_par(scrap_int, "output", dims_out | common))
    merge_data(results, gen_par(intensity, "input", dims_in | common))


def gen_data(df_scen: pd.DataFrame, s_info: "ScenarioInfo") -> dict:
    area = (
        df_scen[df_scen["Variable"].str.startswith("Infrastructure")]
        .assign(Region=lambda x: "R12_" + x["Region"])
        .groupby("Region")
        .sum(numeric_only=True)
    )

    results = {}
    gen_io(results, df_scen, area, s_info)
    merge_data(results, gen_demands(area))
    return results


def read_data(s_info: "ScenarioInfo", scenario: str) -> pd.DataFrame:
    path = private_data_path(
        "projects", "circeular", "stocks_forecast_MESSAGE__All.parquet"
    )
    df_scen = pd.read_parquet(
        path, filters=[("Scenario", "==", scenario), ("Sensitivity", "==", "mean")]
    ).drop(columns=["Model"])
    df_scen.columns = [int(i) if i.isdigit() else i for i in df_scen.columns]
    df_scen = df_scen[
        [i for i in df_scen.columns if not (isinstance(i, int) & (i not in s_info.Y))]
    ]
    df_scen[2110] = df_scen[2100]
    return df_scen


def make_spec() -> Spec:
    s = Spec()
    s["add"].set["technology"].extend(["construction_infra_trp"])
    s["add"].set["commodity"].extend(["infra_trp_area"])

    s["require"].set["commodity"].extend(target_commodities)
    s["require"].set["level"].extend(["product", "end_of_life", "demand"])
    s["require"].set["mode"].extend(["M1"])
    return s


def main(context: Context, scenario: message_ix.Scenario, **kwargs) -> None:
    spec = make_spec()
    s_info = ScenarioInfo(scenario)
    code = kwargs.get("code", "R")
    input_data = read_data(s_info, scen_map[code])
    input_data_ref = read_data(s_info, scen_map["R"])
    data = gen_data(input_data, s_info)
    merge_data(data, calc_adjusted_total_demands(input_data_ref, scenario))

    options = dict(fast=True, dry_run=True)
    apply_spec(scenario, spec, data, **options)

    scenario.set_as_default()
    log.info(f"Built GLOMIS soft-link on {scenario.url} and set as default")
