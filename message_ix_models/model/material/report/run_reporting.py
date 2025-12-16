from typing import TYPE_CHECKING, List, Union

import numpy as np
import pandas as pd
import pyam

from message_ix_models.model.material.report import reporter_utils as ut
from message_ix_models.util import broadcast

from .config import Config

if TYPE_CHECKING:
    from message_ix import Reporter, Scenario


def pyam_df_from_rep(
    rep: "Reporter", reporter_var: str, mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """Queries data from Reporter and maps to IAMC variable names.

    Parameters
    ----------
    rep
        "Reporter" to query
    reporter_var
        Registered key of Reporter to query, e.g. "out", "in", "ACT", "emi", "CAP"
    mapping_df
        DataFrame mapping Reporter dimension values to IAMC variable names
    """
    filters_dict = {
        col: list(mapping_df.index.get_level_values(col).unique())
        for col in mapping_df.index.names
    }
    # rep.set_filters(**filters_dict)
    key = f"{reporter_var}:nl-t-ya-m-c-l" if ":" not in reporter_var else reporter_var
    df_var = pd.DataFrame(rep.get(key))
    node_col = "nl" if "nl" in df_var.index.names else "n"
    y_col = "ya" if "ya" in df_var.index.names else "y"
    df = (
        df_var.join(mapping_df[["iamc_name", "unit"]])
        .dropna()
        .groupby([node_col, y_col, "iamc_name"])
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
) -> pyam.IamDataFrame:
    """Formats a DataFrame created with :func:pyam_df_from_rep to pyam.IamDataFrame."""
    df.columns = ["value"]
    reg_col = "nl" if "nl" in df.index.names else "n"
    year_col = "ya" if "ya" in df.index.names else "y"
    df = (
        df.reset_index()
        .rename(columns={"iamc_name": "variable", reg_col: "region", year_col: "Year"})
        .assign(
            variable=lambda x: variable_prefix + x["variable"],
            Model=model_name,
            Scenario=scenario_name,
            Unit=unit,
        )
    )
    py_df = pyam.IamDataFrame(df)
    if py_df.empty:
        return py_df
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


def load_config(folder, name: str) -> "Config":
    """Load a config for a given reporting variable category from the YAML files.

    This is a thin wrapper around :meth:`.Config.from_files`.
    """
    return Config.from_files(folder, name)


def run_ch4_reporting(rep, model_name: str, scen_name: str) -> pyam.IamDataFrame:
    """Generate reporting for industry methane emissions."""
    var = "ch4_emi"
    config = load_config("emission", var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    return py_df


def run_fe_reporting(rep: "Reporter", model: str, scenario: str) -> pd.DataFrame:
    """Generate reporting for industry final energy variables."""
    ut.add_fe_key(rep)
    dfs = []

    config = load_config("energy", "fe")
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    dfs.append(
        format_reporting_df(
            df, config.iamc_prefix, model, scenario, config.unit, config.mapping
        )
    )

    config = load_config("energy", "fe_solar")
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    dfs.append(
        format_reporting_df(
            df, config.iamc_prefix, model, scenario, config.unit, config.mapping
        )
    )

    # py_df_all = add_chemicals_to_final_energy_variables(dfs, rep, model, scenario)
    py_df_all = pyam.concat(dfs)
    py_df_all = split_fe_other(rep, py_df_all, model, scenario)

    # Add solar to "other industry" aggregate
    var = "Final Energy|Industry|Other Sector"
    other_sector_agg = py_df_all.add(
        f"{var}|Solar",
        var,
        var,
    )
    py_df_all = pyam.concat(
        [py_df_all.filter(variable=var, keep=False), other_sector_agg]
    )
    # alternative aggregation components
    vars = [  # noqa: F841
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


def split_fe_other(
    rep: "Reporter", py_df_all: pyam.IamDataFrame, model: str, scenario: str
) -> pyam.IamDataFrame:
    """Splits Final Energy|Industry|*|Liquids|Other values.

    It reallocates it to Liquids|Biomass/Coal/Oil/Gas based on the methanol feedstock
    shares.

    1. Calculate the feedstock shares of methanol production with "Reporter".
    2. Append the shares as temporary iamc variables them to the existing reporting pyam
       object.
    3. Use pyam multiply feature to calculate shares with each "Liquids|Other"
       timeseries.
    4. Use pyam aggregate to sum existing Liquids|Biomass/Coal/Oil/Gas with new
       variables and store in separate pyam object.
    5. Filter out existing (outdated) Liquids|Biomass/Coal/Oil/Gas from reporting pyam
       object.
    6. Concat the updated variables with the full reporting.
    """
    ut.add_biometh_final_share(rep, mode="fuel")
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
        to_append = pyam.IamDataFrame(to_append.data.replace([np.inf, -np.inf], 0))
        py_df_all = pyam.concat([py_df_all, to_append])
        updated_rows = []

        for var in [i for i in py_df_all.variable if "Liquids|Other" in i]:
            # calculate variable with methanol feed share to allocate proportionally
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
    # set all Liquids|Other to 0 since they are fully allocated to other variables now
    rows = []
    for var in [i for i in py_df_all.variable if "Liquids|Other" in i]:
        rows.append(
            py_df_all.multiply(
                var,
                0,
                var,
            )
        )
    py_df_all = pyam.concat(
        [
            py_df_all.filter(
                variable=[i for i in py_df_all.variable if "Liquids|Other" in i],
                keep=False,
            ),
            pyam.concat(rows),
        ]
    )
    return py_df_all


def run_prod_reporting(
    rep: "Reporter", model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for industry production variables."""
    dfs = []
    ut.iron_prod(rep)
    config = load_config("", "prod")
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    df.loc[df.index.get_level_values("iamc_name").str.contains("Methanol")] /= 0.697615
    dfs.append(
        format_reporting_df(
            df,
            config.iamc_prefix,
            model_name,
            scen_name,
            config.unit,
            config.mapping,
        )
    )

    config = load_config("", "prod_addon")
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    dfs.append(
        format_reporting_df(
            df,
            config.iamc_prefix,
            model_name,
            scen_name,
            config.unit,
            config.mapping,
        )
    )

    py_df = pyam.concat(dfs)

    dry_cli = "Production|Non-Metallic Minerals|Clinker"
    dry_cli_ccs = "Production|Non-Metallic Minerals|Clinker|w/ CCS"
    dry_cli_wo_ccs = "Production|Non-Metallic Minerals|Clinker|w/o CCS"
    py_df.subtract(dry_cli, dry_cli_ccs, dry_cli_wo_ccs, fillna=0, append=True)
    share_ccs = "Production|Non-Metallic Minerals|Clinker|w/ CCS [Share]"
    share_non_ccs = "Production|Non-Metallic Minerals|Clinker|w/o CCS [Share]"
    py_df.divide(dry_cli, dry_cli_wo_ccs, share_non_ccs, fillna=0, append=True)
    py_df.subtract(1, share_non_ccs, share_ccs, append=True)
    return py_df


def run_se(rep: "Reporter", model_name: str, scen_name: str):
    dfs = []
    ut.add_se_elec(rep)
    ut.add_heat_calcs(rep)
    ut.add_se_elec_stor(rep)
    for group in [
        "se_elec",
        "se_elec_curt",
        "se_elec_thermal",
        "se_fuels",
        "se_heat",
        "se_elec_storage",
    ]:
        cfg = load_config("energy", group)
        df = pyam_df_from_rep(rep, cfg.var, cfg.mapping)
        dfs.append(
            format_reporting_df(
                df, cfg.iamc_prefix, model_name, scen_name, cfg.unit, cfg.mapping
            )
        )
    df = pyam.concat(dfs)
    df_final = (
        df.filter(unit="dimensionless", keep=False)
        .convert_unit("GWa", "EJ")
        .timeseries()
        .reset_index()
    )
    df_final.unit = "EJ/yr"
    return df_final


def run_all_categories(
    rep: "Reporter", model_name: str, scen_name: str
) -> List[pyam.IamDataFrame]:
    """Generate all industry reporting variables for a given scenario."""
    dfs = [
        run_fe_reporting(rep, model_name, scen_name),
        run_prod_reporting(rep, model_name, scen_name),
        run_ch4_reporting(rep, model_name, scen_name),
    ]
    return dfs


def calculate_clinker_ccs_energy(scenario: "Scenario", rep, py_df):
    ut.add_cement_heat_share_calculations(rep)
    df1 = (
        pd.DataFrame(
            rep.get(
                "share::cement-heat-non-ccs",
            )
        )
        .reset_index()
        .drop(columns=["m", "c"])
        .pivot(index=["nl"], columns="ya", values=0)
        .reset_index()
        .rename(columns={"nl": "region"})
        .assign(
            variable="heat-share-cement-non-ccs",
            unit="dimensionless",
            scenario=scenario.scenario,
            model=scenario.model,
        )
    )
    df2 = (
        pd.DataFrame(
            rep.get(
                "share::cement-heat-ccs",
            )
        )
        .reset_index()
        .drop(columns=["m", "c"])
        .pivot(index=["nl"], columns="ya", values=0)
        .reset_index()
        .rename(columns={"nl": "region"})
        .assign(
            variable="heat-share-cement-ccs",
            unit="dimensionless",
            scenario=scenario.scenario,
            model=scenario.model,
        )
    )
    py_df = pyam.concat([py_df, pyam.IamDataFrame(df1), pyam.IamDataFrame(df2)])
    rep.set_filters()

    fe_vars = [
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Electricity|Heat",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Gases|Gas",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Hydrogen",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Liquids|Biomass",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Liquids|Coal",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Liquids|Gas",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Liquids|Hydrogen",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Liquids|Oil",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Solids|Biomass",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Solids|Coal",
        "Final Energy|Industry|Non-Metallic Minerals|Cement|Solids|Coke",
    ]
    for var in fe_vars:
        py_df.multiply(
            var,
            "heat-share-cement-ccs",
            var.replace("Cement", "Cement|w/ CCS"),
            append=True,
        )
        py_df.multiply(
            var,
            "heat-share-cement-non-ccs",
            var.replace("Cement", "Cement|w/o CCS"),
            append=True,
        )
    prefix = "Final Energy|Industry|Non-Metallic Minerals|Cement"
    # calculate proportional electricity use for grinding and other electricity use
    #   excluding clinker production electricty use for both CCS and non CCS production
    py_df.multiply(
        f"{prefix}|Electricity|Other",
        "Production|Non-Metallic Minerals|Clinker|w/o CCS [Share]",
        f"{prefix}|w/o CCS|Electricity|Other",
        append=True,
    )
    # sum clinker electricity and proportional electricity for CCS
    #   and conventional cement making
    py_df.add(
        f"{prefix}|w/o CCS|Electricity|Other",
        f"{prefix} Clinker|w/o CCS|Electricity",
        f"{prefix}|w/o CCS|Electricity",
        append=True,
    )
    try:
        py_df.multiply(
            f"{prefix}|Electricity|Other",
            "Production|Non-Metallic Minerals|Clinker|w/ CCS [Share]",
            f"{prefix}|w/ CCS|Electricity|Other",
            append=True,
        )
        py_df.add(
            f"{prefix}|w/ CCS|Electricity|Other",
            f"{prefix} Clinker|w/ CCS|Electricity",
            f"{prefix}|w/ CCS|Electricity",
            append=True,
        )
    except:
        print("No clinker with CCS production in this scenario.")
    return py_df


def run(
    scenario: "Scenario",
    upload_ts: bool = False,
    region: Union[bool, str] = False,
) -> pyam.IamDataFrame:
    """Run industry reporter for a given scenario.

    Parameters
    ----------
    upload_ts
        Option to upload reporting timeseries to the scenario.
    region
        Option to aggregate regional timeseries to a single (global) region.
    """
    rep = Reporter.from_scenario(scenario)

    dfs = run_all_categories(rep, scenario.model, scenario.scenario)

    py_df = pyam.concat(dfs)
    calculate_clinker_ccs_energy(scenario, rep, py_df)

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


if __name__ == "__main__":
    import ixmp
    import message_ix

    mp = ixmp.Platform("local3")
    scen = message_ix.Scenario(mp, "SSP_SSP2_v6.2", "baseline_wo_GLOBIOM_ts")
    rep = message_ix.Reporter.from_scenario(scen)
    df = run_fe_reporting(rep, scen.model, scen.scenario)
    print()
