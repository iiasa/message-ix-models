from typing import Mapping

import click
import logging
import ixmp

# from .build import apply_spec
# from message_data.tools import ScenarioInfo
from message_data.model.material.build import apply_spec
from message_ix_models import ScenarioInfo
from message_ix_models.util import add_par_data, private_data_path
from message_data.tools.utilities import (
    calibrate_UE_gr_to_demand,
    calibrate_UE_share_constraints,
)

# from .data import add_data
from .data_util import modify_demand_and_hist_activity, add_emission_accounting
from .data_util import add_coal_lowerbound_2020, add_macro_COVID, add_cement_bounds_2020
from .data_util import add_elec_lowerbound_2020, add_ccs_technologies, read_config
import pandas as pd

log = logging.getLogger(__name__)


def build(scenario):
    """Set up materials accounting on `scenario`."""

    # Get the specification
    # Apply to the base scenario
    spec = get_spec()

    if "water_supply" not in list(scenario.set("level")):
        scenario.check_out()
        # add missing water tecs
        scenario.add_set("technology", "extract__freshwater_supply")
        scenario.add_set("level", "water_supply")
        scenario.add_set("commodity", "freshwater_supply")

        water_dict = pd.read_excel(
            private_data_path("material", "other", "water_tec_pars.xlsx"),
            sheet_name=None
        )
        for par in water_dict.keys():
            scenario.add_par(par, water_dict[par])
        scenario.commit("add missing water tecs")

    apply_spec(scenario, spec, add_data_2)
    spec = None
    apply_spec(scenario, spec, add_data_1)  # dry_run=True

    s_info = ScenarioInfo(scenario)
    nodes = s_info.N

    # Adjust exogenous energy demand to incorporate the endogenized sectors
    # Adjust the historical activity of the useful level industry technologies
    # Coal calibration 2020
    add_ccs_technologies(scenario)
    modify_demand_and_hist_activity(scenario)
    try:
        add_emission_accounting(scenario)
    except:
        pass
        # scenario.commit("no changes")
    add_coal_lowerbound_2020(scenario)
    add_cement_bounds_2020(scenario)

    # Market penetration adjustments
    # NOTE: changing demand affects the market penetration levels for the enduse technologies.
    # Note: context.ssp doesnt work
    calibrate_UE_gr_to_demand(
        scenario, data_path=private_data_path(), ssp="SSP2", region="R12"
    )
    calibrate_UE_share_constraints(scenario)

    # Electricity calibration to avoid zero prices for CHN.
    if "R12_CHN" in nodes:
        add_elec_lowerbound_2020(scenario)

    # i_feed demand is zero creating a zero division error during MACRO calibration
    scenario.check_out()
    scenario.remove_set("sector", "i_feed")
    scenario.commit("i_feed removed from sectors.")

    return scenario


# add as needed/implemented
SPEC_LIST = [
    "generic",
    "common",
    "steel",
    "cement",
    "aluminum",
    "petro_chemicals",
    "buildings",
    "power_sector",
    "fertilizer",
    "methanol",
]


def get_spec() -> Mapping[str, ScenarioInfo]:
    """Return the specification for materials accounting."""
    require = ScenarioInfo()
    add = ScenarioInfo()
    remove = ScenarioInfo()

    # Load configuration
    # context = Context.get_instance(-1)
    context = read_config()

    # Update the ScenarioInfo objects with required and new set elements
    for type in SPEC_LIST:
        for set_name, config in context["material"][type].items():
            # for cat_name, detail in config.items():
            # Required elements
            require.set[set_name].extend(config.get("require", []))

            # Elements to add
            add.set[set_name].extend(config.get("add", []))

            # Elements to remove
            remove.set[set_name].extend(config.get("remove", []))

    return dict(require=require, add=add, remove=remove)


# Group to allow for multiple CLI subcommands under "material"
@click.group("material")
def cli():
    """Model with materials accounting."""


@cli.command("create-bare")
@click.option("--regions", type=click.Choice(["China", "R11", "R14"]))
@click.option("--dry_run", "-n", is_flag=True, help="Only show what would be done.")
@click.pass_obj
def create_bare(context, regions, dry_run):
    """Create the RES from scratch."""
    from message_data.model.bare import create_res

    if regions:
        context.regions = regions

    # to allow historical years
    context.period_start = 1980

    # Otherwise it can not find the path to read the yaml files..
    # context.metadata_path = context.metadata_path / "data"

    scen = create_res(context)
    build(scen)

    # Solve
    if not dry_run:
        scen.solve()


@cli.command("build")
@click.option(
    "--datafile",
    default="Global_steel_cement_MESSAGE.xlsx",
    metavar="INPUT",
    help="File name for external data input",
)
@click.option("--tag", default="", help="Suffix to the scenario name")
@click.option("--mode", default="by_url")
@click.option("--scenario_name", default="NoPolicy_3105_macro")
@click.pass_obj
def build_scen(context, datafile, tag, mode, scenario_name):
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
            scenario = build(
                context.get_scenario().clone(
                    model=context.scenario_info["model"],
                    scenario=context.scenario_info["scenario"] + "_" + tag,
                    keep_solution=False,
                )
            )
        else:
            scenario = build(
                context.get_scenario().clone(
                    model="MESSAGEix-Materials",
                    scenario=output_scenario_name + "_" + tag,
                )
            )
        # Set the latest version as default
        scenario.set_as_default()

    # Create a two degrees scenario by copying carbon prices from another scenario.
    if mode == "by_copy":
        output_scenario_name = "2degrees"
        mod_mitig = "ENGAGE_SSP2_v4.1.8"
        scen_mitig = "EN_NPi2020_1000f"
        print("Loading " + mod_mitig + " " + scen_mitig + " to retreive carbon prices.")
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

    if mode == "cbudget":
        scenario = context.get_scenario()
        print(scenario.version)
        # print('Base scenario is: ' + scenario.scenario + ", version: " + scenario.version)
        output_scenario_name = scenario.scenario + "_" + tag
        scenario_new = scenario.clone(
            "MESSAGEix-Materials",
            output_scenario_name,
            keep_solution=False,
            shift_first_model_year=2025,
        )
        emission_dict = {
            "node": "World",
            "type_emission": "TCE",
            "type_tec": "all",
            "type_year": "cumulative",
            "unit": "???",
        }
        df = message_ix.make_df("bound_emission", value=3667, **emission_dict)
        scenario_new.check_out()
        scenario_new.add_par("bound_emission", df)
        scenario_new.commit("add emission bound")
        print("New carbon budget added")
        print("New scenario name is " + output_scenario_name)
        scenario_new.set_as_default()


@cli.command("solve")
@click.option("--scenario_name", default="NoPolicy")
@click.option("--version", default=None)
@click.option("--model_name", default="MESSAGEix-Materials")
@click.option(
    "--datafile",
    default="Global_steel_cement_MESSAGE.xlsx",
    metavar="INPUT",
    help="File name for external data input",
)
@click.option("--add_macro", default=True)
@click.option("--add_calibration", default=False)
@click.pass_obj
def solve_scen(
        context, datafile, model_name, scenario_name, add_calibration, add_macro, version
):
    """Solve a scenario.

    Use the --model_name and --scenario_name option to specify the scenario to solve.
    """
    # Clone and set up
    from message_ix import Scenario

    mp = ixmp.Platform("ixmp_dev")
    if version:
        scenario = Scenario(mp, model_name, scenario_name, version=int(version))
    else:
        scenario = Scenario(mp, model_name, scenario_name)
    # scenario = Scenario(context.get_platform(), model_name, scenario_name)

    if scenario.has_solution():
        scenario.remove_solution()

    if add_calibration:
        # Solve
        print("Solving the scenario without MACRO")
        scenario.solve(model="MESSAGE", solve_options={"lpmethod": "4", "scaind": "-1"})
        scenario.set_as_default()

        # After solving, add macro calibration
        print("Scenario solved, now adding MACRO calibration")
        scenario = add_macro_COVID(
            scenario, "R12-CHN-5y_macro_data_NGFS_w_rc_ind_adj_mat.xlsx"
        )
        print("Scenario calibrated.")

    if add_macro:  # Default True
        print(
            "After macro calibration a new scneario with the suffix _macro is created."
        )
        print("Make sure to use this scenario to solve with MACRO iterations.")

        scenario.solve(
            model="MESSAGE-MACRO", solve_options={"lpmethod": "4", "scaind": "-1"}
        )
        scenario.set_as_default()

    if not add_macro:
        # Solve
        print("Solving the scenario without MACRO")
        scenario.solve(model="MESSAGE", solve_options={"lpmethod": "4", "scaind": "-1"})
        scenario.set_as_default()


@cli.command("add_buildings_ts")
@click.option("--scenario_name", default="NoPolicy")
@click.option("--model_name", default="MESSAGEix-Materials")
def add_building_ts(scenario_name, model_name):
    from message_data.reporting.materials.add_buildings_ts import (
        add_building_timeseries,
    )
    from message_ix import Scenario
    from ixmp import Platform

    print(model_name)
    mp = Platform()

    scenario = Scenario(mp, model_name, scenario_name)

    add_building_timeseries(scenario)


@cli.command("report")
# @cli.command("report-1")
@click.option(
    "--remove_ts",
    default=False,
    help="If True the existing timeseries in the scenario is removed.",
)
@click.option("--profile", default=False)
@click.pass_obj
def run_reporting(context, remove_ts, profile):
    """Run materials, then legacy reporting."""
    from message_data.reporting.materials.reporting import report
    from message_data.tools.post_processing.iamc_report_hackathon import (
        report as reporting,
    )

    # Retrieve the scenario given by the --url option
    scenario = context.get_scenario()
    mp = scenario.platform

    if remove_ts:
        df_rem = scenario.timeseries()

        if not df_rem.empty:
            scenario.check_out(timeseries_only=True)
            scenario.remove_timeseries(df_rem)
            scenario.commit("Existing timeseries removed.")
            scenario.set_as_default()
            print("Existing timeseries are removed.")
        else:
            print("There are no timeseries to be removed.")
    else:

        if profile:
            import cProfile
            import pstats
            import io
            import atexit

            print("Profiling...")
            pr = cProfile.Profile()
            pr.enable()
            print("Reporting material-specific variables")
            report(context, scenario)
            print("Reporting standard variables")
            reporting(
                mp,
                scenario,
                # NB(PNK) this is not an error; .iamc_report_hackathon.report() expects a
                #         string containing "True" or "False" instead of an actual bool.
                "False",
                scenario.model,
                scenario.scenario,
                merge_hist=True,
                merge_ts=True,
                run_config="materials_run_config.yaml",
            )

            def exit():
                pr.disable()
                print("Profiling completed")
                s = io.StringIO()
                pstats.Stats(pr, stream=s).sort_stats("cumulative").dump_stats(
                    "profiling.dmp"
                )
                print(s.getvalue())

            atexit.register(exit)
        else:

            # Remove existing timeseries and add material timeseries
            print("Reporting material-specific variables")
            report(context, scenario)
            print("Reporting standard variables")
            reporting(
                mp,
                scenario,
                # NB(PNK) this is not an error; .iamc_report_hackathon.report() expects a
                #         string containing "True" or "False" instead of an actual bool.
                "False",
                scenario.model,
                scenario.scenario,
                merge_hist=True,
                merge_ts=True,
                run_config="materials_run_config.yaml",
            )
        # util.prepare_xlsx_for_explorer(
        #     Path(os.getcwd()).parents[0].joinpath(
        #         "reporting_output", scenario.model+"_"+scenario.scenario+".xlsx"))


@cli.command("report-2")
@click.pass_obj
def run_old_reporting(context):
    from message_data.tools.post_processing.iamc_report_hackathon import (
        report as reporting,
    )

    # Retrieve the scenario given by the --url option
    scenario = context.get_scenario()
    mp = scenario.platform

    reporting(
        mp,
        scenario,
        # NB(PNK) this is not an error; .iamc_report_hackathon.report() expects a
        #         string containing "True" or "False" instead of an actual bool.
        "False",
        scenario.model,
        scenario.scenario,
        merge_hist=True,
        merge_ts=True,
        run_config="materials_run_config.yaml",
    )


from .data_cement import gen_data_cement
from .data_steel import gen_data_steel
from .data_aluminum import gen_data_aluminum
from .data_generic import gen_data_generic
from .data_petro import gen_data_petro_chemicals
from .data_power_sector import gen_data_power_sector
from .data_methanol_new import gen_data_methanol_new
from .data_ammonia_new import gen_all_NH3_fert

DATA_FUNCTIONS_1 = [
    # gen_data_buildings,
    gen_data_methanol_new,
    gen_all_NH3_fert,
    # gen_data_ammonia, ## deprecated module!
    gen_data_generic,
    gen_data_steel,
]
DATA_FUNCTIONS_2 = [
    gen_data_cement,
    gen_data_petro_chemicals,
    gen_data_power_sector,
    gen_data_aluminum,
]


# Try to handle multiple data input functions from different materials
def add_data_1(scenario, dry_run=False):
    """Populate `scenario` with MESSAGEix-Materials data."""
    # Information about `scenario`
    info = ScenarioInfo(scenario)

    # Check for two "node" values for global data, e.g. in
    # ixmp://ene-ixmp/CD_Links_SSP2_v2.1_clean/baseline
    if {"World", "R11_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R11_GLB' from node list for data generation")
        info.set["node"].remove("R11_GLB")
    if {"World", "R12_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R12_GLB' from node list for data generation")
        info.set["node"].remove("R12_GLB")

    for func in DATA_FUNCTIONS_1:
        # Generate or load the data; add to the Scenario
        log.info(f"from {func.__name__}()")
        add_par_data(scenario, func(scenario), dry_run=dry_run)

    log.info("done")


def add_data_2(scenario, dry_run=False):
    """Populate `scenario` with MESSAGEix-Materials data."""
    # Information about `scenario`
    info = ScenarioInfo(scenario)

    # Check for two "node" values for global data, e.g. in
    # ixmp://ene-ixmp/CD_Links_SSP2_v2.1_clean/baseline
    if {"World", "R11_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R11_GLB' from node list for data generation")
        info.set["node"].remove("R11_GLB")
    if {"World", "R12_GLB"} < set(info.set["node"]):
        log.warning("Remove 'R12_GLB' from node list for data generation")
        info.set["node"].remove("R12_GLB")

    for func in DATA_FUNCTIONS_2:
        # Generate or load the data; add to the Scenario
        log.info(f"from {func.__name__}()")
        # TODO: remove this once emission_factors are back in SSP_dev
        data = func(scenario)
        if "SSP_dev" in scenario.model:
            if "emission_factor" in list(data.keys()):
                data.pop("emission_factor")
        add_par_data(scenario, data, dry_run=dry_run)

    log.info("done")


@cli.command("modify_cost")
@click.option("--ssp", default="SSP2", help="Suffix to the scenario name")
@click.option("--scen_name", default="SSP_supply_cost_test_baseline_macro")
@click.pass_obj
def modify_costs_with_tool(context, scen_name, ssp):
    import message_ix
    from message_ix_models.tools.costs.config import Config
    from message_ix_models.tools.costs.projections import create_cost_projections

    mp = ixmp.Platform("ixmp_dev")
    base = message_ix.Scenario(mp, "MESSAGEix-Materials", scenario=scen_name)
    scen = base.clone(model=base.model, scenario=base.scenario.replace("baseline", ssp))

    tec_set = list(scen.set("technology"))

    cfg = Config(
        module="materials",
        ref_region="R12_NAM",
        method="convergence",
        format="message",
        scenario=ssp,
    )

    out_materials = create_cost_projections(
        node=cfg.node,
        ref_region=cfg.ref_region,
        base_year=cfg.base_year,
        module=cfg.module,
        method=cfg.method,
        scenario_version=cfg.scenario_version,
        scenario=cfg.scenario,
        convergence_year=cfg.convergence_year,
        fom_rate=cfg.fom_rate,
        format=cfg.format,
    )
    scen.check_out()
    fix_cost = out_materials.fix_cost.drop_duplicates().drop(
        ["scenario_version", "scenario"], axis=1
    )
    scen.add_par("fix_cost", fix_cost[fix_cost["technology"].isin(tec_set)])
    inv_cost = out_materials.inv_cost.drop_duplicates().drop(
        ["scenario_version", "scenario"], axis=1
    )
    scen.add_par("inv_cost", inv_cost[inv_cost["technology"].isin(tec_set)])

    scen.commit(f"update cost assumption to: {ssp}")
    scen.solve(model="MESSAGE-MACRO", solve_options={"scaind": -1})


@cli.command("run_cbud_scenario")
@click.option("--ssp", default="SSP2", help="Suffix to the scenario name")
@click.option(
    "--scenario",
    default="1000f",
    help="description of carbon budget for mitigation target",
)
@click.pass_obj
def run_cbud_scenario(context, ssp, scenario):
    import message_ix

    if scenario == "1000f":
        budget = 3667
    elif scenario == "650f":
        budget = 1750
    else:
        print("chosen budget not available yet please choose 650f or 1000f")
        return

    mp = ixmp.Platform("ixmp_dev")
    base = message_ix.Scenario(
        mp, "MESSAGEix-Materials", scenario=f"SSP_supply_cost_test_{ssp}_macro"
    )
    scenario_cbud = base.clone(
        model=base.model,
        scenario=base.scenario + "_" + scenario,
        shift_first_model_year=2025,
    )

    emission_dict = {
        "node": "World",
        "type_emission": "TCE",
        "type_tec": "all",
        "type_year": "cumulative",
        "unit": "???",
    }
    df = message_ix.make_df("bound_emission", value=budget, **emission_dict)
    scenario_cbud.check_out()
    scenario_cbud.add_par("bound_emission", df)
    scenario_cbud.commit("add emission bound")
    pre_model_yrs = scenario_cbud.set(
        "cat_year", {"type_year": "cumulative", "year": [2020, 2015, 2010]}
    )
    scenario_cbud.check_out()
    scenario_cbud.remove_set("cat_year", pre_model_yrs)
    scenario_cbud.commit("remove cumulative years from cat_year set")
    scenario_cbud.set("cat_year", {"type_year": "cumulative"})

    scenario_cbud.solve(model="MESSAGE-MACRO", solve_options={"scaind": -1})
    return


@cli.command("run_LED_cprice_scenario")
@click.option("--ssp", default="SSP2", help="Suffix to the scenario name")
@click.option(
    "--scenario",
    default="1000f",
    help="description of carbon budget for mitigation target",
)
@click.pass_obj
def modify_costs_with_tool(context, ssp, scenario):
    import message_ix

    mp = ixmp.Platform("ixmp_dev")
    if scenario == "1000f":
        price_scen = message_ix.Scenario(
            mp, "MESSAGEix-Materials", scenario="SSP_supply_cost_test_LED_macro_1000f"
        )
    if scenario == "650f":
        price_scen = message_ix.Scenario(
            mp,
            "MESSAGEix-Materials",
            scenario=f"SSP_supply_cost_test_LED_macro_{scenario}",
        )

    base = message_ix.Scenario(
        mp, "MESSAGEix-Materials", scenario=f"SSP_supply_cost_test_{ssp}_macro"
    )
    scen_cprice = base.clone(
        model=base.model,
        scenario=base.scenario + f"_{scenario}_LED_prices",
        shift_first_model_year=2025,
    )

    tax_emission_new = price_scen.var("PRICE_EMISSION")

    scen_cprice.check_out()
    tax_emission_new.columns = scen_cprice.par("tax_emission").columns
    tax_emission_new["unit"] = "USD/tCO2"
    scen_cprice.add_par("tax_emission", tax_emission_new)
    scen_cprice.commit("2 degree LED prices are added")
    print("New LED 1000f carbon prices added")

    scen_cprice.solve(model="MESSAGE-MACRO", solve_options={"scaind": -1})
    return
