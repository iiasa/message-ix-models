import os
from typing import List, Literal

import message_ix
import pandas as pd
import pyam
from message_ix.report import Reporter
from pydantic import BaseModel, ConfigDict

from message_ix_models.model.material.report.reporter_utils import (
    add_biometh_final_share,
    create_var_map_from_yaml_dict,
)
from message_ix_models.model.material.util import read_yaml_file
from message_ix_models.util import broadcast, package_data_path


class ReporterConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    iamc_prefix: str
    message_query_key: Literal[
        "out", "in", "ACT", "emi", "CAP"
    ]  # TODO: try to import message_ix.Reporter keys here
    unit: Literal["Mt/yr", "GWa", "Mt CH4/yr", "GW"]
    df_mapping: pd.DataFrame  # TODO: use pandera to check on columns/dtypes etc.


def create_agg_var_map_from_yaml(dictionary: dict, level: int = 1):
    """Returns dataframe with 3 columns containing:
    * the IAMC variable name of each aggregate variable defined in mapping dictionary at
        the given level
    * a short name of the variable and
    * the short name of the sub-variable that belongs to the aggregate

    The returned df can be used to join with the non-aggregate
    sub variable produced with :func:create_var_map_from_yaml_dict2
    to get the elements to compute the aggregate with message_ix.Reporter

    Returns
    -------
    pd.DataFrame
    """
    data = dictionary[f"level_{level}"]
    all = pd.DataFrame()
    # iterate through all aggregate variable keys and compute dataframe with aggregate
    # components mapped to aggregate short and IAMC name
    for iamc_key, values in data.items():
        # Extract aggregate components and short name from dictionary
        components = values["components"]
        short_name = values["short"]

        # Create DataFrame
        df = pd.DataFrame(components)
        df["iamc_name"] = iamc_key
        df["agg_short_name"] = short_name

        # append to already created mapping rows
        all = pd.concat([all, df])
    all.columns = ["short_name", "iamc_name", "agg_short_name"]
    all = all.set_index("short_name")
    return all


def pyam_df_from_rep(
    rep: message_ix.Reporter, reporter_var: str, mapping_df: pd.DataFrame
):
    """Uses mapping_df with a message.Reporter to compute all values
    for the IAMC variables defined in the mapping

    Returns
    -------
    pd.DataFrame
    """
    filters_dict = {
        col: list(mapping_df.index.get_level_values(col).unique())
        for col in mapping_df.index.names
    }
    rep.set_filters(**filters_dict)
    df_var = pd.DataFrame(rep.get(f"{reporter_var}:nl-t-ya-m-c-l"))
    df = (
        df_var.join(mapping_df[["iamc_name", "unit"]])
        .dropna()
        .groupby(["nl", "ya", "iamc_name"])
        .sum(numeric_only=True)
    )
    rep.set_filters()
    return df


def format_reporting_df(
    df: pd.DataFrame,
    variable_prefix: str,
    model_name: str,
    scenario_name: str,
    unit: str,
    mappings,
):
    """Returns an pyam.IamDataFrame based on a DataFrame created with
        :func:pyam_df_from_rep to an pyam.IamDataFrame

    Returns
    -------
    pyam.IamDataFrame
    """
    df.columns = ["value"]
    df = df.reset_index()
    df = df.rename(columns={"iamc_name": "variable", "nl": "region", "ya": "Year"})
    df["variable"] = variable_prefix + df["variable"]
    df["Model"] = model_name
    df["Scenario"] = scenario_name
    df["Unit"] = unit
    py_df = pyam.IamDataFrame(df)

    missing = [
        variable_prefix + i
        for i in mappings.iamc_name.unique().tolist()
        if i not in [i.replace(variable_prefix, "") for i in py_df.variable]
    ]
    if missing:
        zero_ts = pyam.IamDataFrame(
            pd.DataFrame()
            .assign(
                variable=missing,
                region=None,
                unit=unit,
                value=0,
                scenario=scenario_name,
                model=model_name,
                year=None,
            )
            .pipe(broadcast, region=py_df.region, year=py_df.year)
        )
        py_df = pyam.concat([py_df, zero_ts])
    return py_df


def load_config(name: str):
    path = package_data_path("material", "reporting")
    file = f"{name}_reporting.yaml"
    file_agg = f"{name}_reporting_aggregates.yaml"

    rep_var_dict = read_yaml_file(path.joinpath(file))
    mappings = create_var_map_from_yaml_dict(rep_var_dict)
    variable_prefix = rep_var_dict.get("iamc_prefix")
    unit = rep_var_dict.get("common").get("unit")
    var = rep_var_dict.get("var")

    if os.path.exists(path.joinpath(file_agg)):
        rep_var_dict_agg = read_yaml_file(path.joinpath(file_agg))

        filters_mapping = (
            mappings.copy(deep=True)
            .drop(["iamc_name"], axis=1)
            .reset_index()
            .set_index("short_name")
        )

        # TODO: generalize to make this loop-able over arbitrary number of levels
        # load aggregates mapping to components for level 1 variables
        counter = 1
        while rep_var_dict_agg.get(f"level_{counter}", None):
            agg1_mapping = create_agg_var_map_from_yaml(rep_var_dict_agg, level=counter)
            # Join aggregate mapping df with components df on the "short_name" values
            # to get a row for each component of each aggregate
            # drop the short name column afterward and replace it with the
            #   aggregate short name
            # finally concat the components mapping with the newly created
            #   aggregate mapping
            agg1_mapping = (
                agg1_mapping.join(filters_mapping)
                .set_index(["m", "t", "l", "c"])
                .rename(columns={"agg_short_name": "short_name"})
            )
            mappings = pd.concat([mappings, agg1_mapping])
            filters_mapping = (
                mappings.copy(deep=True)
                .drop(["iamc_name"], axis=1)
                .reset_index()
                .set_index("short_name")
            )
            counter += 1

    # Creating config model and validate with pydantic
    config = ReporterConfig(
        iamc_prefix=variable_prefix,
        message_query_key=var,
        unit=unit,
        df_mapping=mappings,
    )
    return config


def run_fe_methanol_nh3_reporting(
    rep: message_ix.Reporter, model_name: str, scen_name: str
):
    """Runs final energy reporting for ammonia and methanol.
    Process energy input for methanol and ammonia is defined as the
    difference between energy input and the embodied energy in the final product.
    Since the model structure does not differentiate between feedstock
    and process energy input for NH3 and CH3OH production, this needs to
    calculated in post-processing steps:
    1a) Query input flows to methanol and ammonia production
    1b) Map to IAMC variable names
    2a) Query methanol and ammonia output flows and map to same IAMC variables
    2b) Convert ammonia output flows from Mt to GWa (not needed for methanol)
    3) Subtract product output from energy input to get process energy
    4) Format dataframe to IAMC standard

    Parameters
    ----------
    rep
    model_name
    scen_name

    Returns
    -------

    """
    config = load_config("fe_methanol_ammonia")
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)

    config2 = load_config("fs1")
    config2.iamc_prefix = config.iamc_prefix
    df2 = pyam_df_from_rep(rep, config2.message_query_key, config2.df_mapping)
    df2.loc[df2.index.get_level_values("iamc_name").str.contains("Ammonia")] *= 0.697615
    df_final = df.sub(df2, fill_value=0)
    py_df = format_reporting_df(
        df_final,
        config2.iamc_prefix,
        model_name,
        scen_name,
        config.unit,
        config.df_mapping,
    )
    return py_df


def run_ch4_reporting(rep, model_name: str, scen_name: str):
    var = "ch4_emi"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.df_mapping
    )
    return py_df


def run_fe_reporting(rep: message_ix.Reporter, model: str, scenario: str):
    dfs = []

    config = load_config("fe")
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)
    dfs.append(
        format_reporting_df(
            df, config.iamc_prefix, model, scenario, config.unit, config.df_mapping
        )
    )

    config = load_config("fe_solar")
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)
    dfs.append(
        format_reporting_df(
            df, config.iamc_prefix, model, scenario, config.unit, config.df_mapping
        )
    )

    py_df_all = add_chemicals_to_final_energy_variables(dfs, rep, model, scenario)

    py_df_all = split_fe_other(rep, py_df_all, model, scenario)

    vars = [
        "Final Energy|Industry|Other Sector",
        "Final Energy|Industry|Iron and Steel",
        "Final Energy|Industry|Non-Ferrous Metals|Aluminium",
        "Final Energy|Industry|Non-Metallic Minerals|Cement",
        "Final Energy|Industry|Chemicals",
    ]
    vars2 = [
        "Final Energy|Industry|Electricity",
        "Final Energy|Industry|Solids",
        "Final Energy|Industry|Gases",
        "Final Energy|Industry|Liquids",
        "Final Energy|Industry|Hydrogen",
        "Final Energy|Industry|Solar",
        "Final Energy|Industry|Heat",
    ]
    py_df_all.aggregate("Final Energy|Industry", components=vars2, append=True)

    df_final = (
        py_df_all.filter(unit="dimensionless", keep=False)
        .convert_unit("GWa", "EJ")
        .timeseries()
        .reset_index()
    )
    df_final.unit = "EJ/yr"
    return df_final


def add_chemicals_to_final_energy_variables(
    dfs: List[pyam.IamDataFrame], rep: message_ix.Reporter, model: str, scenario: str
):
    dfs.append(run_fe_methanol_nh3_reporting(rep, model, scenario))
    py_df_all = pyam.concat(dfs)
    chem_aggs = {
        "Chemicals": [
            "Chemicals|Ammonia",
            "Chemicals|Methanol",
            "Chemicals|High-Value Chemicals",
        ],
        "Chemicals|Electricity": [
            "Chemicals|Ammonia|Electricity",
            "Chemicals|Methanol|Electricity",
            "Chemicals|High-Value Chemicals|Electricity",
        ],
        "Chemicals|Gases": [
            "Chemicals|Ammonia|Gases",
            "Chemicals|Methanol|Gases",
            "Chemicals|High-Value Chemicals|Gases",
        ],
        "Chemicals|Gases|Gas": [
            "Chemicals|Ammonia|Gases|Gas",
            "Chemicals|Methanol|Gases|Gas",
            "Chemicals|High-Value Chemicals|Gases|Gas",
        ],
        "Chemicals|Liquids": [
            "Chemicals|Ammonia|Liquids",
            "Chemicals|High-Value Chemicals|Liquids",
        ],
        "Chemicals|Liquids|Oil": [
            "Chemicals|Ammonia|Liquids|Oil",
            "Chemicals|High-Value Chemicals|Liquids|Oil",
        ],
        "Chemicals|Solids": [
            "Chemicals|Ammonia|Solids",
            "Chemicals|Methanol|Solids",
            "Chemicals|High-Value Chemicals|Solids",
        ],
        "Chemicals|Solids|Biomass": [
            "Chemicals|Ammonia|Solids|Biomass",
            "Chemicals|Methanol|Solids|Biomasss",
            "Chemicals|High-Value Chemicals|Solids|Biomass",
        ],
        "Chemicals|Solids|Coal": [
            "Chemicals|Ammonia|Solids|Coal",
            "Chemicals|Methanol|Solids|Coal",
            "Chemicals|High-Value Chemicals|Solids|Coal",
        ],
        "Chemicals|Hydrogen": [
            "Chemicals|Ammonia|Hydrogen",
            "Chemicals|Methanol|Hydrogen",
            "Chemicals|High-Value Chemicals|Hydrogen",
        ],
    }
    prefix = "Final Energy|Industry|"
    fe_aggs = {prefix + k: [prefix + i for i in v] for k, v in chem_aggs.items()}
    for k, v in fe_aggs.items():
        py_df_all.aggregate(k, v, append=True)

    updated_fe_totals = []
    for comm in [
        "Electricity",
        "Gases",
        "Gases|Gas",
        "Liquids",
        "Liquids|Oil",
        "Solids",
        "Solids|Biomass",
        "Solids|Coal",
        "Hydrogen",
    ]:
        updated_fe_totals.append(
            py_df_all.add(
                prefix + comm,
                prefix + f"Chemicals|{comm}",
                prefix + comm,
                ignore_units=False,
            )
        )
    py_df_updates = pyam.concat(updated_fe_totals)
    py_df_all.filter(variable=py_df_updates.variable, keep=False, inplace=True)
    py_df_all = pyam.concat([py_df_all, py_df_updates])
    return py_df_all


def split_fe_other(
    rep: message_ix.Reporter, py_df_all: pyam.IamDataFrame, model: str, scenario: str
):
    """This function takes the Final Energy|Industry|*|Liquids|Other values
    and reallocates it to Liquids|Biomass/Coal/Oil/Gas based on the methanol
    feedstock shares.
    1) calculates the feedstock shares of methanol production with message_ix.Reporter
    2) append the shares as temporary iamc variables them to the existing reporting
        pyam object
    3) Uses pyam multiply feature to calculate shares with each "Liquids|Other"
        timeseries
    4) Uses pyam aggregate to sum existing Liquids|Biomass/Coal/Oil/Gas with new
        variables and store in separate pyam object
    5) Filters out existing (outdated) Liquids|Biomass/Coal/Oil/Gas from reporting
        pyam object
    6) Concats the updated variables with the full reporting

    Parameters
    ----------
    rep
    py_df_all
    model
    scenario

    Returns
    -------

    """
    add_biometh_final_share(rep, mode="fuel")
    # set temporary filter on Reporter to speed up queries
    rep.set_filters(
        t=[
            "meth_bunker",
            "meth_tobunker",
            "meth_bio",
            "meth_bio_ccs",
            "meth_h2",
            "meth_t_d_material",
            "meth_coal",
            "meth_coal_ccs",
            "meth_ng",
            "meth_ng_ccs",
            "meth_t_d",
            "meth_bal",
            "meth_trd",
            "meth_exp",
            "meth_imp",
            "meth_ind_fs",
            "furnace_methanol_refining",
        ]
    )
    for c, full_name in zip(
        ["coal", "gas", "bio", "h2"],
        ["Coal", "Gas", "Biomass", "Hydrogen"],
    ):
        df_shrs = pd.DataFrame(rep.get(f"share::{c}methanol-final"))
        if df_shrs.empty:
            continue
        df_shrs = df_shrs.reset_index()
        df_shrs.rename(columns={"nl": "Region"}, inplace=True)
        df_shrs = df_shrs.pivot(columns="ya", values=0, index="Region")
        to_append = pyam.IamDataFrame(
            df_shrs.assign(
                scenario=scenario,
                model=model,
                unit="dimensionless",
                variable=f"Share|{c}-methanol",
            )
        )
        py_df_all = pyam.concat([py_df_all, to_append])
        updated_rows = []

        for var in [i for i in py_df_all.variable if "Liquids|Other" in i]:
            py_df_all.multiply(
                var,
                f"Share|{c}-methanol",
                var.replace("Liquids|Other", f"Liquids|{c}-methanol"),
                append=True,
            )
            updated_rows.append(
                py_df_all.aggregate(
                    var.replace("Liquids|Other", f"Liquids|{full_name}"),
                    [
                        var.replace("Liquids|Other", f"Liquids|{full_name}"),
                        var.replace("Liquids|Other", f"Liquids|{c}-methanol"),
                    ],
                )
            )
        py_df_update = pyam.concat(updated_rows)
        py_df_all.filter(variable=py_df_update.variable, keep=False, inplace=True)
        py_df_all = pyam.concat([py_df_all, py_df_update])
    rep.set_filters()
    return py_df_all


def run_fs_reporting(rep: message_ix.Reporter, model_name: str, scen_name: str):
    dfs = []
    config = load_config("fs2")
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)
    dfs.append(
        format_reporting_df(
            df,
            config.iamc_prefix,
            model_name,
            scen_name,
            config.unit,
            config.df_mapping,
        )
    )
    config = load_config("fs1")
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)
    df.loc[df.index.get_level_values("iamc_name").str.contains("Ammonia")] *= 0.697615
    dfs.append(
        format_reporting_df(
            df,
            config.iamc_prefix,
            model_name,
            scen_name,
            config.unit,
            config.df_mapping,
        )
    )
    py_df = pyam.concat(dfs)
    prefix = "Final Energy|Non-Energy Use|"
    chem_aggs = {
        "Chemicals": [
            "Chemicals|Ammonia",
            "Chemicals|Methanol",
            "Chemicals|High-Value Chemicals",
        ],
        "Chemicals|Electricity": [
            "Chemicals|Ammonia|Electricity",
            "Chemicals|Methanol|Electricity",
            "Chemicals|High-Value Chemicals|Electricity",
        ],
        "Chemicals|Gases": [
            "Chemicals|Ammonia|Gases",
            "Chemicals|Methanol|Gases",
            "Chemicals|High-Value Chemicals|Gases",
        ],
        "Chemicals|Gases|Gas": [
            "Chemicals|Ammonia|Gases|Gas",
            "Chemicals|Methanol|Gases|Gas",
            "Chemicals|High-Value Chemicals|Gases|Gas",
        ],
        "Chemicals|Liquids": [
            "Chemicals|Ammonia|Liquids",
            "Chemicals|High-Value Chemicals|Liquids",
        ],
        "Chemicals|Liquids|Oil": [
            "Chemicals|Ammonia|Liquids|Oil",
            "Chemicals|High-Value Chemicals|Liquids|Oil",
        ],
        "Chemicals|Liquids|Biomass": ["Chemicals|High-Value Chemicals|Liquids|Biomass"],
        "Chemicals|Liquids|Other": ["Chemicals|High-Value Chemicals|Liquids|Other"],
        "Chemicals|Solids": [
            "Chemicals|Ammonia|Solids",
            "Chemicals|Methanol|Solids",
            "Chemicals|High-Value Chemicals|Solids",
        ],
        "Chemicals|Solids|Biomass": [
            "Chemicals|Ammonia|Solids|Biomass",
            "Chemicals|Methanol|Solids|Biomasss",
            "Chemicals|High-Value Chemicals|Solids|Biomass",
        ],
        "Chemicals|Solids|Coal": [
            "Chemicals|Ammonia|Solids|Coal",
            "Chemicals|Methanol|Solids|Coal",
            "Chemicals|High-Value Chemicals|Solids|Coal",
        ],
        "Chemicals|Hydrogen": [
            "Chemicals|Methanol|Hydrogen",
        ],
    }
    fe_chem_aggs = {prefix + k: [prefix + i for i in v] for k, v in chem_aggs.items()}
    fe_aggs = {
        prefix[:-1] + k.replace("Chemicals", ""): [prefix + i for i in v]
        for k, v in chem_aggs.items()
    }
    for k, v in fe_chem_aggs.items():
        py_df.aggregate(k, v, append=True)
    for k, v in fe_aggs.items():
        py_df.aggregate(k, v, append=True)

    py_df = split_mto_feedstock(rep, py_df, model_name, scen_name)
    df_final = (
        py_df.filter(unit="dimensionless", keep=False)
        .convert_unit("GWa", "EJ")
        .timeseries()
        .reset_index()
    )
    df_final.unit = "EJ/yr"
    return df_final


def split_mto_feedstock(
    rep: message_ix.Reporter, py_df_all: pyam.IamDataFrame, model: str, scenario: str
):
    add_biometh_final_share(rep, mode="feedstock")
    rep.set_filters(
        t=[
            "meth_bunker",
            "meth_tobunker",
            "meth_bio",
            "meth_bio_ccs",
            "meth_h2",
            "meth_t_d_material",
            "meth_coal",
            "meth_coal_ccs",
            "meth_ng",
            "meth_ng_ccs",
            "meth_t_d",
            "meth_bal",
            "meth_trd",
            "meth_exp",
            "meth_imp",
            "meth_ind_fs",
        ]
    )
    for c, full_name in zip(
        ["coal", "gas", "bio", "h2"],
        ["Coal", "Gas", "Biomass", "Hydrogen"],
    ):
        df_shrs = pd.DataFrame(rep.get(f"share::{c}methanol-final"))
        df_shrs = df_shrs.reset_index()
        df_shrs.rename(columns={"nl": "Region"}, inplace=True)
        df_shrs = df_shrs.pivot(columns="ya", values=0, index="Region")
        to_append = pyam.IamDataFrame(
            df_shrs.assign(
                scenario=scenario,
                model=model,
                unit="dimensionless",
                variable=f"Share|{c}-methanol-fs",
            )
        )
        py_df_all = pyam.concat([py_df_all, to_append])
        updated_rows = []

        for var in [i for i in py_df_all.variable if "Liquids|Other" in i]:
            py_df_all.multiply(
                var,
                f"Share|{c}-methanol-fs",
                var.replace("Liquids|Other", f"Liquids|{c}-methanol"),
                append=True,
            )
            updated_rows.append(
                py_df_all.aggregate(
                    var.replace("Liquids|Other", f"Liquids|{full_name}"),
                    [
                        var.replace("Liquids|Other", f"Liquids|{full_name}"),
                        var.replace("Liquids|Other", f"Liquids|{c}-methanol"),
                    ],
                )
            )
        py_df_update = pyam.concat(updated_rows)
        py_df_all.filter(variable=py_df_update.variable, keep=False, inplace=True)
        py_df_all = pyam.concat([py_df_all, py_df_update])
    rep.set_filters()
    return py_df_all


def run_prod_reporting(rep: message_ix.Reporter, model_name: str, scen_name: str):
    dfs = []
    config = load_config("prod")
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)
    df.loc[df.index.get_level_values("iamc_name").str.contains("Methanol")] /= 0.697615
    dfs.append(
        format_reporting_df(
            df,
            config.iamc_prefix,
            model_name,
            scen_name,
            config.unit,
            config.df_mapping,
        )
    )

    config = load_config("prod_addon")
    df = pyam_df_from_rep(rep, config.message_query_key, config.df_mapping)
    dfs.append(
        format_reporting_df(
            df,
            config.iamc_prefix,
            model_name,
            scen_name,
            config.unit,
            config.df_mapping,
        )
    )

    py_df = pyam.concat(dfs)
    # assume 15% of BOF output is secondary steel since input is 15% "new_scrap"
    bof_var = "Production|Iron and Steel|Steel|Secondary|BOF"
    bof = py_df.multiply(bof_var, 0.15, bof_var)
    py_df.filter(variable=bof_var, keep=False, inplace=True)
    py_df = pyam.concat([py_df, bof])
    updated = []
    to_update = [
        "Production|Iron and Steel|Steel|Secondary",
        "Production|Steel",
        "Production|Iron and Steel|Steel",
    ]
    for agg_var in to_update:
        updated.append(py_df.add(agg_var, bof_var, agg_var))
    py_df.filter(variable=to_update, keep=False, inplace=True)
    py_df = pyam.concat([py_df, pyam.concat(updated)])

    # assume 85% of BOF output is primary steel since input is 85% "pig_iron"
    bof_var = "Production|Iron and Steel|Steel|Primary|BOF"
    bof = py_df.multiply(bof_var, 0.85, bof_var)
    py_df.filter(variable=bof_var, keep=False, inplace=True)
    py_df = pyam.concat([py_df, bof])
    updated = []
    to_update = [
        "Production|Iron and Steel|Steel|Primary",
        "Production|Steel",
        "Production|Iron and Steel|Steel",
    ]
    for agg_var in to_update:
        updated.append(py_df.add(agg_var, bof_var, agg_var))
    py_df.filter(variable=to_update, keep=False, inplace=True)
    py_df = pyam.concat([py_df, pyam.concat(updated)])

    return py_df


def run_all_categories(rep: message_ix.Reporter, model_name: str, scen_name: str):
    dfs = []
    dfs.append(run_fs_reporting(rep, model_name, scen_name))
    dfs.append(run_fe_reporting(rep, model_name, scen_name))
    dfs.append(run_prod_reporting(rep, model_name, scen_name))
    dfs.append(run_ch4_reporting(rep, model_name, scen_name))
    return dfs


def run(scenario, upload_ts=False, region=False):
    rep = Reporter.from_scenario(scenario)

    dfs = run_all_categories(rep, scenario.model, scenario.scenario)

    py_df = pyam.concat(dfs)
    if region:
        py_df.aggregate_region(py_df.variable, region=region, append=True)
    else:
        py_df.aggregate_region(py_df.variable, append=True)
    py_df.filter(variable="Share*", keep=False, inplace=True)
    py_df.filter(
        year=[i for i in scenario.set("year") if i >= scenario.firstmodelyear],
        inplace=True,
    )
    if upload_ts:
        scenario.add_timeseries(py_df.timeseries())
    return py_df
