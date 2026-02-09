from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pint
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import read_yaml_file
from message_ix_models.util import package_data_path, same_node

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import ParameterData


def gen_data_power_sector(
    scenario: "Scenario", dry_run: bool = False
) -> "ParameterData":
    """Generate data for materials representation of power industry."""
    info = ScenarioInfo(scenario)
    int_dict = gen_cap_par_data(read_material_intensities(info), scenario, info)

    # create new parameters input_cap_new, output_cap_new, input_cap_ret,
    # output_cap_ret, input_cap and output_cap if they don't exist
    maybe_init_pars(scenario)

    return int_dict


def read_material_intensities(s_info: "ScenarioInfo") -> pd.DataFrame:
    """Read and process material intensity data for power sector technologies."""
    path = package_data_path("material", "power_sector")
    # read technology, region and commodity mappings
    tec_map = (
        pd.read_csv(
            path.joinpath("MESSAGE_global_model_technologies.csv"),
            usecols=[0, 7],
        )
        .dropna()
        .groupby("LCA mapping")["Type of Technology"]
        .apply(list)
        .to_dict()
    )
    reg_map = read_yaml_file(
        package_data_path("material", "power_sector", "themis_region_map.yaml")
    )
    comm_map = pd.read_csv(path.joinpath("lca_commodity_mapping.csv"), index_col=0)

    data = pd.read_csv(path.joinpath("NTNU_LCA_coefficients.csv"), comment="#").rename(
        columns=lambda c: int(c) if isinstance(c, str) and c.isdigit() else c
    )
    data = overwrite_hydro_intensities(data)
    data = (
        data.assign(technology=data["technology"].map(tec_map))
        .explode("technology")
        .assign(region=data["region"].map(reg_map))
        .explode("region")
        .rename(columns={"region": "node_loc"})
    )

    data = data.set_index([i for i in data.columns if not isinstance(i, int)])

    # filter relevant scenario, technology variant (residue for biomass,
    # mix for others) and remove operation phase (and remove duplicates)
    # TODO: move hardcoded scenario filter to build config
    data = data.loc["THEMIS"].loc["Baseline"]
    data = data.loc[
        :,
        "Environmental impacts",
        :,
        ["mix", "residue"],
        :,
        ["Construction", "End-of-life"],
    ]
    pre_2010 = [i for i in s_info.set["year"] if i < 2010]
    data[pre_2010] = np.tile(data[[2010]].to_numpy(), (1, len(pre_2010)))
    intermed_years = [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045, 2050]
    data = data.reindex(columns=pre_2010 + intermed_years).interpolate(
        axis=1, method="linear"
    )
    post_2050 = [i for i in s_info.Y if i > 2050]
    data[post_2050] = np.tile(data[[2050]].to_numpy(), (1, len(post_2050)))
    data = (
        data.join(
            comm_map,
            how="inner",
        )
        .dropna()
        .set_index(["commodity", "level"], append=True)
        .melt(ignore_index=False, var_name="year_vtg")
        .reset_index()
        .assign(unit="t/kW")
    )
    data["value"] = pint.Quantity(data["value"].values, "kt").to("Mt").round(7)
    return data


def gen_cap_par_data(
    data_lca: pd.DataFrame, scen: "Scenario", s_info: ScenarioInfo
) -> "ParameterData":
    """Generate capacity-related parameter data for material flows.

    Parameters
    ----------
    data_lca :
        DataFrame with material intensity data.
    scen :
        MESSAGEix Scenario object.
    s_info:
        ScenarioInfo object for `scen`.
    """
    hist_cap = scen.par(
        "historical_new_capacity",
        filters={"technology": data_lca["technology"].unique()},
    )
    hist_cap = (
        hist_cap[hist_cap["value"] != 0]
        .set_index(["node_loc", "technology", "year_vtg"])
        .drop("unit", axis=1)
    )
    inflow = data_lca.loc[data_lca["phase"] == "Construction"].copy()
    outflow = data_lca.loc[data_lca["phase"] == "End-of-life"].copy()
    input_cap_new = make_df(
        "input_cap_new", **inflow[inflow["year_vtg"].ge(s_info.Y[0])], time="year"
    ).pipe(same_node)
    input_cap_ret = make_df("input_cap_ret", **inflow, time="year").pipe(same_node)
    input_cap_ret_future = input_cap_ret[input_cap_ret["year_vtg"] > s_info.Y[0]]
    input_cap_ret_hist = input_cap_ret.set_index(
        ["node_loc", "technology", "year_vtg"]
    )[
        input_cap_ret.set_index(["node_loc", "technology", "year_vtg"]).index.isin(
            hist_cap.index
        )
    ].reset_index()
    input_cap_ret = pd.concat([input_cap_ret_hist, input_cap_ret_future])

    output_cap_ret = make_df(
        "output_cap_ret",
        **outflow.replace({"level": {"product": "end_of_life"}}),
        time="year",
    ).pipe(same_node)
    output_cap_ret_future = output_cap_ret[output_cap_ret["year_vtg"] > s_info.Y[0]]
    output_cap_ret_hist = output_cap_ret.set_index(
        ["node_loc", "technology", "year_vtg"]
    )[
        output_cap_ret.set_index(["node_loc", "technology", "year_vtg"]).index.isin(
            hist_cap.index
        )
    ].reset_index()
    output_cap_ret = pd.concat([output_cap_ret_hist, output_cap_ret_future])
    return {
        "input_cap_new": input_cap_new,
        "input_cap_ret": input_cap_ret,
        "output_cap_ret": output_cap_ret,
    }


def overwrite_hydro_intensities(data_lca: pd.DataFrame) -> pd.DataFrame:
    """Overwrite hydropower material intensities with values from Kalt et al., 2021."""
    # Unit: t/MW
    idx = (
        (data_lca["technology"] == "Hydro")
        & (data_lca["technology variant"] == "mix")
        & (data_lca["phase"] == "Construction")
    )
    data_lca.loc[idx & (data_lca["impact"] == "Iron"), [2010, 2030]] = 45
    data_lca.loc[idx & (data_lca["impact"] == "Aluminium"), [2010, 2030]] = 0.572
    data_lca.loc[idx & (data_lca["impact"] == "Cement"), [2010, 2030]] = 787.5
    return data_lca


def maybe_init_pars(scenario: "Scenario") -> None:
    """Initialize IO capacity parameters if they do not exist."""
    if not scenario.has_par("input_cap_new"):
        scenario.init_par(
            "input_cap_new",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap_new"):
        scenario.init_par(
            "output_cap_new",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
    if not scenario.has_par("input_cap_ret"):
        scenario.init_par(
            "input_cap_ret",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap_ret"):
        scenario.init_par(
            "output_cap_ret",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
    if not scenario.has_par("input_cap"):
        scenario.init_par(
            "input_cap",
            idx_sets=[
                "node",
                "technology",
                "year",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "year_act",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap"):
        scenario.init_par(
            "output_cap",
            idx_sets=[
                "node",
                "technology",
                "year",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "year_act",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
