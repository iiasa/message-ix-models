"""Run Infrastructure Scenarios"""

from datetime import date

import click
import yaml
from message_ix_models.util.click import common_params
from message_ix_models.util import private_data_path
from yaml.loader import SafeLoader

# Valid values
_DEMAND_SCENARIOS = [
"baseline",
"ScP2",
"ScP3",
"ScP4",
"ScP5",
"ScP6",
"all",
]

_DEMAND_SENSITIVITY = [
"mean",
"high",
"low",
"all",
]

_SUPPLY_SCENARIOS = [
"recycling",
"substitution",
"fuel_switching",
"ccs",
"all",
]

@cli.command("run")
@click.option("--demand_scenario", type=click.Choice(_DEMAND_SCENARIOS))
@click.option("--supply_scenario", type=click.Choice(_SUPPLY_SCENARIOS))
@click.option("--demand_sensitivity", type=click.Choice(_DEMAND_SENSITIVITY))
@click.option("--tag", default="", help="Suffix to the scenario name")
@click.option("--old_calib", default=False)
@click.option(
    "--update_costs",
    default=False,
)
@click.pass_obj

def build_scen(context, tag, demand_scenario, supply_scenario, demand_sensitivity, old_calib, update_costs):
    """Build a scenario.

    Use the --url option to specify the base scenario. If this scenario is on a
    Platform stored with ixmp.JDBCBackend, it should be configured with >16 GB of
    memory, i.e. ``jvmargs=["-Xmx16G"]``.
    """

    import message_ix

    mp = context.get_platform()



    if mode == "by_url":
        # Determine the output scenario name based on the --url CLI option. If the
        # user did not give a recognized value, this raises an error.
        output_scenario_name = {
            "baseline": "NoPolicy",
            "baseline_macro": "NoPolicy",
            "baseline_new": "NoPolicy",
            "baseline_new_macro": "NoPolicy",
            "NPi2020-con-prim-dir-ncr": "NPi",
            "NPi2020_1000-con-prim-dir-ncr": "NPi2020_1000",
            "NPi2020_400-con-prim-dir-ncr": "NPi2020_400",
        }.get(context.scenario_info["scenario"])

        if type(output_scenario_name).__name__ == "NoneType":
            output_scenario_name = context.scenario_info["scenario"]

        # context.metadata_path = context.metadata_path / "data"
        context.datafile = datafile

        if context.scenario_info["model"] != "CD_Links_SSP2":
            print("WARNING: this code is not tested with this base scenario!")

        # Clone and set up

        if "SSP_dev" in context.scenario_info["model"]:
            scenario = context.get_scenario().clone(
                model=context.scenario_info["model"],
                scenario=context.scenario_info["scenario"] + "_" + tag,
                keep_solution=False,
            )
            if float(context.scenario_info["model"].split("Blv")[1]) < 0.12:
                context.model.regions = "R12"
                measures = ["GDP", "POP"]
                tecs = ["GDP_PPP", "Population"]
                models = ["IIASA GDP 2023", "IIASA-WiC POP 2023"]
                for measure, model, tec in zip(measures, models, tecs):
                    df = get_ssp_soc_eco_data(context, model, measure, tec)
                    scenario.check_out()
                    if "GDP_PPP" not in list(scenario.set("technology")):
                        scenario.add_set("technology", "GDP_PPP")
                    scenario.commit("update projections")
                    scenario.check_out()
                    scenario.add_par("bound_activity_lo", df)
                    scenario.add_par("bound_activity_up", df)
                    scenario.commit("update projections")
            scenario = build(scenario, old_calib=old_calib)
        else:
            scenario = build(
                context.get_scenario().clone(
                    model="MESSAGEix-Materials",
                    scenario=output_scenario_name + "_" + tag,
                ),
                old_calib=old_calib,
            )
        # Set the latest version as default
        scenario.set_as_default()

    # Create a two degrees scenario by copying carbon prices from another scenario.
    elif mode == "by_copy":
        output_scenario_name = "2degrees"
        mod_mitig = "ENGAGE_SSP2_v4.1.8"
        scen_mitig = "EN_NPi2020_1000f"
        print("Loading " + mod_mitig + " " + scen_mitig + " to retrieve carbon prices.")
        scen_mitig_prices = message_ix.Scenario(mp, mod_mitig, scen_mitig)
        tax_emission_new = scen_mitig_prices.var("PRICE_EMISSION")

        scenario = message_ix.Scenario(mp, "MESSAGEix-Materials", scenario_name)
        print("Base scenario is " + scenario_name)
        output_scenario_name = output_scenario_name + "_" + tag
        scenario = scenario.clone(
            "MESSAGEix-Materials",
            output_scenario_name,
            keep_solution=False,
            shift_first_model_year=2025,
        )
        scenario.check_out()
        tax_emission_new.columns = scenario.par("tax_emission").columns
        tax_emission_new["unit"] = "USD/tCO2"
        scenario.add_par("tax_emission", tax_emission_new)
        scenario.commit("2 degree prices are added")
        print("New carbon prices added")
        print("New scenario name is " + output_scenario_name)
        scenario.set_as_default()

    elif mode == "cbudget":
        scenario = context.get_scenario()
        print(scenario.version)
        output_scenario_name = scenario.scenario + "_" + tag
        scenario_new = scenario.clone(
            "MESSAGEix-Materials",
            output_scenario_name,
            keep_solution=False,
            shift_first_model_year=2025,
        )
        emission_dict = {
            "node": "World",
            "type_emission": "TCE_CO2",
            "type_tec": "all",
            "type_year": "cumulative",
            "unit": "???",
        }
        df = message_ix.make_df("bound_emission", value=990, **emission_dict)
        scenario_new.check_out()
        scenario_new.add_par("bound_emission", df)
        scenario_new.commit("add emission bound")
        print("New carbon budget added")
        print("New scenario name is " + output_scenario_name)
        scenario_new.set_as_default()

    if update_costs:
        log.info(f"Updating costs with {message_ix_models.tools.costs.projections}")
        inv, fix = gen_te_projections(scenario, context["ssp"])
        scenario.check_out()
        scenario.add_par("fix_cost", fix)
        scenario.add_par("inv_cost", inv)
        scenario.commit(f"update cost assumption to: {update_costs}")
