import os
from typing import TYPE_CHECKING

import constants as c
import pandas as pd
import pyam
import yaml
from message_ix import Reporter

from message_ix_models.model.material.report import reporter_utils
from message_ix_models.model.material.report.reporter_utils import add_eff
from message_ix_models.model.material.report.run_reporting import (
    calculate_clinker_ccs_energy,
    format_reporting_df,
    load_config,
    pyam_df_from_rep,
    run_fe_reporting,
    run_prod_reporting,
    run_se,
)

if TYPE_CHECKING:
    from message_ix import Scenario


def run_co2(rep: Reporter, model_name: str, scen_name: str):
    dfs = []
    config = load_config("emission", "co2_w_ccs")
    filter = {
        "t": ["DUMMY_coal_supply", "DUMMY_gas_supply", "DUMMY_limestone_supply_steel"],
        "r": ["CO2_ind", "CO2_Emission"],
    }
    filter_ccs = {"t": ["dri_gas_ccs_steel", "bf_ccs_steel"], "c": ["fic_co2"]}
    reporter_utils.add_net_co2_calcs(rep, filter, filter_ccs, "steel")
    filter = {
        "t": ["clinker_dry_cement", "clinker_wet_cement"],
        "r": ["CO2_Emission"],
    }
    filter_ccs = {
        "t": ["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"],
        "c": ["fic_co2"],
    }
    reporter_utils.add_net_co2_calcs(rep, filter, filter_ccs, "cement")
    rep.add("co2:nl-t-ya:industry", "concat", "co2:nl-t-ya:cement", "co2:nl-t-ya:steel")
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    df = format_reporting_df(
        df,
        config.iamc_prefix,
        model_name,
        scen_name,
        config.unit,
        config.mapping,
    )
    df_final = df.convert_unit("Mt C/yr", "Mt CO2/yr")
    return df_final


def run_other(rep: Reporter, model_name: str, scen_name: str):
    """Run other categories needed for premise.

    Currently, includes:
    - GDP/population
    - carbon storage
    - CCS (DAC, steel, cement)
    """
    data = []
    for folder, cfg in zip(
        ["", "", "emission", "emission"], ["pop", "gdp", "ccs", "removal"]
    ):
        config = load_config(folder, cfg)
        df = pyam_df_from_rep(rep, config.var, config.mapping)
        data.append(
            format_reporting_df(
                df,
                config.iamc_prefix,
                model_name,
                scen_name,
                config.unit,
                config.mapping,
            )
        )
    df = pyam.concat(data).convert_unit("Mt C/yr", "Mt CO2/yr")
    return df


def run_pe(rep: Reporter, model_name: str, scen_name: str):
    """Run primary energy reporting for biomass variables needed for premise."""
    config = load_config("energy", "pe_globiom")
    rep.set_filters(
        l="land_use_reporting", c=config.mapping.index.get_level_values("c").tolist()
    )
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    rep.set_filters()
    df = format_reporting_df(
        df,
        config.iamc_prefix,
        model_name,
        scen_name,
        config.unit,
        config.mapping,
    )
    df_final = df.filter(unit="dimensionless", keep=False).timeseries().reset_index()
    df_final.unit = "EJ/yr"
    return df_final


def run_eff(rep, model_name: str, scen_name: str):
    add_eff(rep)
    config = load_config("energy", "eff")
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    df = format_reporting_df(
        df,
        config.iamc_prefix,
        model_name,
        scen_name,
        config.unit,
        config.mapping,
    )
    return df


def query_from_scenario(scen: "Scenario") -> pyam.IamDataFrame:
    df = scen.timeseries(variable=c.vars)
    df["region"] = df["region"].map(c.reg_map)
    py_df = pyam.IamDataFrame(df)
    return py_df


def run(rep, scenario: "Scenario", model_name: str, scen_name: str):
    dfs = []
    reporter_utils.pe_gas(rep)
    dfs.append(run_pe(rep, model_name, scen_name))
    dfs.append(run_fe_reporting(rep, model_name, scen_name))
    dfs.append(run_eff(rep, model_name, scen_name))
    dfs.append(run_se(rep, model_name, scen_name))
    dfs.append(run_other(rep, model_name, scen_name))
    dfs.append(run_co2(rep, model_name, scen_name))
    dfs.append(run_prod_reporting(rep, model_name, scen_name))
    dfs.append(query_from_scenario(scenario))
    py_df = pyam.concat(dfs)
    py_df = calculate_clinker_ccs_energy(scenario, rep, py_df)

    py_df.aggregate_region(
        [i for i in py_df.variable if ("Efficiency" not in i) & (i != "Emissions|CO2")],
        append=True,
    )
    py_df.filter(variable="Share*", keep=False, inplace=True)
    py_df.filter(
        year=[i for i in scenario.set("year") if i >= scenario.firstmodelyear],
        inplace=True,
    )
    return py_df


def query_magicc_data() -> None:
    conn = pyam.iiasa.Connection("ece_internal")
    prefix = "AR6 climate diagnostics|"
    suffix = "|MAGICCv7.5.3|50.0th Percentile"
    varis = [
        "Raw Surface Temperature (GMST)",
        "Effective Radiative Forcing|CH4",
        "Effective Radiative Forcing|CO2",
        "Effective Radiative Forcing|N2O",
        "Atmospheric Concentrations|CH4",
        "Atmospheric Concentrations|CO2",
        "Atmospheric Concentrations|N2O",
    ]
    varis = [prefix + v + suffix for v in varis]
    scenarios = [
        "SSP1 - High Emissions",
        "SSP1 - Low Emissions",
        "SSP1 - Very Low Emissions",
        "SSP2 - High Emissions",
        "SSP2 - Low Emissions",
        "SSP2 - Low Overshoot",
        "SSP2 - Medium Emissions",
        "SSP2 - Medium-Low Emissions",
        "SSP2 - Very Low Emissions",
        "SSP3 - High Emissions",
        "SSP4 - Low Overshoot",
        "SSP5 - High Emissions",
        "SSP5 - Low Overshoot",
    ]
    test = conn.query(
        model=["MESSAGEix-GLOBIOM-GAINS 2.1-M-R12"], scenario=scenarios, variable=varis
    )
    test.to_excel("magicc_output.xlsx")
    return


def merge_reports() -> None:
    path = "/Users/florianmaczek/Downloads/legacy_output.xlsx"
    df = pd.read_excel(path)
    df["region"] = df["region"].map(c.reg_map)
    df = (
        df.pivot(
            columns="year",
            values="value",
            index=["model", "scenario", "region", "variable", "unit"],
        )
        .reset_index()
        .drop(columns=2110)
    )
    df.rename(
        columns={i: i.capitalize() if isinstance(i, str) else i for i in df.columns},
        inplace=True,
    )
    path = "magicc_output.xlsx"
    df_magicc = pd.read_excel(path)
    df_magicc.rename(
        columns={i: int(i) if i.isdigit() else i for i in df_magicc.columns},
        inplace=True,
    )
    path = os.getcwd() + "/output/"
    with open("report_merge_config.yaml", "r") as file:
        config = yaml.safe_load(file)
    idx = [2, 3, 4]
    for final_name, components in config.items():
        dfs = []
        for fname, years in components.items():
            if isinstance(years, str) and years[-1] == "+":
                years = list(range(int(years[:-1]), 2060, 5)) + list(
                    range(2060, 2110, 10)
                )
            dfs.append(
                pd.read_excel(path + fname + ".xlsx", index_col=idx)[
                    [str(year) for year in years]
                ]
            )
        df_merged = pd.concat(dfs, axis=1, ignore_index=False).reset_index()
        df_merged.rename(
            columns={i: int(i) if i.isdigit() else i for i in df_merged.columns},
            inplace=True,
        )
        full_df = pd.concat(
            [
                df_merged,
                df[df["Scenario"] == final_name][
                    [i for i in df.columns if i in df_merged.columns]
                ],
                df_magicc[df_magicc["Scenario"] == final_name][
                    [i for i in df_magicc.columns if i in df_merged.columns]
                ],
            ]
        ).assign(Model=c.final_model_name, Scenario=final_name)
        full_df.to_csv(f"{final_name}.csv", index=False)
