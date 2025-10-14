from typing import TYPE_CHECKING

import pyam
from message_ix import Reporter

from message_ix_models.model.material.report.reporter_utils import (
    add_net_co2_calcs,
    pe_gas,
)
from message_ix_models.model.material.report.run_reporting import (
    calculate_clinker_ccs_energy,
    format_reporting_df,
    load_config,
    pyam_df_from_rep,
    run_fe_reporting,
    run_fs_reporting,
    run_prod_reporting,
    run_se,
)
from message_ix_models.report import prepare_reporter

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
    add_net_co2_calcs(rep, filter, filter_ccs, "steel")
    filter = {
        "t": ["clinker_dry_cement", "clinker_wet_cement"],
        "r": ["CO2_Emission"],
    }
    filter_ccs = {
        "t": ["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"],
        "c": ["fic_co2"],
    }
    add_net_co2_calcs(rep, filter, filter_ccs, "cement")
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
    for folder, cfg in zip(["", "emission", "emission"], ["soc-eco", "ccs", "removal"]):
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
    df = pyam.concat(data)
    return df


def run_pe(rep: Reporter, model_name: str, scen_name: str):
    """Run primary energy reporting for biomass variables needed for premise."""
    config = load_config("energy", "pe_globiom")
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


def run(rep, scenario: "Scenario", model_name: str, scen_name: str):
    dfs = []
    pe_gas(rep)
    dfs.append(run_se(rep, model_name, scen_name))
    dfs.append(run_other(rep, model_name, scen_name))
    dfs.append(run_co2(rep, model_name, scen_name))
    # dfs.append(run_pe(rep, model_name, scen_name))
    dfs.append(run_fs_reporting(rep, model_name, scen_name))
    dfs.append(run_fe_reporting(rep, model_name, scen_name))
    dfs.append(run_prod_reporting(rep, model_name, scen_name))
    py_df = pyam.concat(dfs)
    calculate_clinker_ccs_energy(scenario, rep, py_df)

    py_df.aggregate_region(py_df.variable, append=True)
    py_df.filter(variable="Share*", keep=False, inplace=True)
    py_df.filter(
        year=[i for i in scenario.set("year") if i >= scenario.firstmodelyear],
        inplace=True,
    )
    return py_df


if __name__ == "__main__":
    from message_ix_models import Context

    ctx = Context()
    import ixmp
    import message_ix

    mp = ixmp.Platform("local3")
    scen = message_ix.Scenario(mp, "SSP_SSP2_v6.2", "baseline_wo_GLOBIOM_ts")
    rep = Reporter.from_scenario(scen)
    prepare_reporter(ctx, reporter=rep)
    df = run(rep, scen, scen.model, scen.scenario)
    print()
