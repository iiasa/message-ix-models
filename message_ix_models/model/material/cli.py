"""Command-line interface for MESSAGEix-Materials.

Use the :doc:`CLI <message_ix_models:cli>` command :program:`mix-models material-ix` to
invoke the commands defined in this module.
"""

import logging
import os

import click
import message_ix

import message_ix_models.tools.costs.projections
from message_ix_models.model.material.data_util import (
    add_macro_COVID,
    gen_te_projections,
)
from message_ix_models.model.material.util import (
    excel_to_csv,
    get_all_input_data_dirs,
    update_macro_calib_file,
)
from message_ix_models.util import (
    package_data_path,
    private_data_path,
)
from message_ix_models.util.click import common_params

from .build import build

log = logging.getLogger(__name__)


# Group to allow for multiple CLI subcommands under "material-ix"
@click.group("material-ix")
@common_params("ssp")
def cli(ssp):
    """MESSAGEix-Materials variant."""


@cli.command("build")
@click.option(
    "--iea_data_path",
    default="P:ene.model\\IEA_database\\Florian\\",
    help="File path for external data input",
)
@click.option("--tag", default="", help="Suffix to the scenario name")
@click.option(
    "--mode", default="by_url", type=click.Choice(["by_url", "cbudget", "by_copy"])
)
@click.option("--scenario_name", default="NoPolicy_3105_macro")
@click.option("--old_calib", default=False)
@click.option(
    "--update_costs",
    default=False,
)
@common_params("nodes")
@click.pass_obj
def build_scen(
    context, iea_data_path, tag, mode, scenario_name, old_calib, update_costs
):
    """Build a scenario.

    Use the --url option to specify the base scenario. If this scenario is on a
    Platform stored with ixmp.JDBCBackend, it should be configured with >16 GB of
    memory, i.e. ``jvmargs=["-Xmx16G"]``.
    """

    if not os.path.isfile(iea_data_path + "REV2022_allISO_IEA.parquet") & ~old_calib:
        log.warning(
            "The proprietary data file: 'REV2022_allISO_IEA.parquet' based on IEA"
            "Extended Energy Balances required for the build with --old_calib=False"
            " cannot be found in the given location. Aborting build..."
        )
        return
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

        if context.scenario_info["model"] != "CD_Links_SSP2":
            log.warning("WARNING: this code is not tested with this base scenario!")

        # Clone and set up

        if "SSP_dev" in context.scenario_info["model"]:
            scenario = context.get_scenario().clone(
                model=context.scenario_info["model"],
                scenario=context.scenario_info["scenario"] + "_" + tag,
                keep_solution=False,
            )
            scenario = build(
                context, scenario, old_calib=old_calib, iea_data_path=iea_data_path
            )
        else:
            scenario = build(
                context,
                context.get_scenario().clone(
                    model="MESSAGEix-Materials",
                    scenario=output_scenario_name + "_" + tag,
                ),
                old_calib=old_calib,
                iea_data_path=iea_data_path,
            )
        # Set the latest version as default
        scenario.set_as_default()

    # Create a two degrees scenario by copying carbon prices from another scenario.
    elif mode == "by_copy":
        output_scenario_name = "2degrees"
        mod_mitig = "ENGAGE_SSP2_v4.1.8"
        scen_mitig = "EN_NPi2020_1000f"
        log.info(
            "Loading " + mod_mitig + " " + scen_mitig + " to retrieve carbon prices."
        )
        scen_mitig_prices = message_ix.Scenario(mp, mod_mitig, scen_mitig)
        tax_emission_new = scen_mitig_prices.var("PRICE_EMISSION")

        scenario = context.get_scenario()
        log.info("Base scenario is " + scenario_name)
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
        log.info("New carbon prices added")
        log.info("New scenario name is " + output_scenario_name)
        scenario.set_as_default()

    elif mode == "cbudget":
        scenario = context.get_scenario()
        log.info(scenario.version)
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
        log.info("New carbon budget added")
        log.info("New scenario name is " + output_scenario_name)
        scenario_new.set_as_default()

    if update_costs:
        log.info(f"Updating costs with {message_ix_models.tools.costs.projections}")
        inv, fix = gen_te_projections(scenario, context["ssp"])
        scenario.check_out()
        scenario.add_par("fix_cost", fix)
        scenario.add_par("inv_cost", inv)
        scenario.commit(f"update cost assumption to: {update_costs}")


def validate_macrofile_path(ctx, param, value):
    if value and ctx.params["macro_file"]:
        if not package_data_path(
            "material", "macro", ctx.params["macro_file"]
        ).is_file():
            raise FileNotFoundError(
                "Specified file name of MACRO calibration file does not exist. Please"
                "place in data/material/macro or use other file that exists."
            )


@cli.command("solve")
@click.option("--add_macro", default=True)
@click.option("--add_calibration", default=False, callback=validate_macrofile_path)
@click.option("--macro_file", default=None, is_eager=True)
@click.option("--shift_model_year", default=False)
@click.pass_obj
def solve_scen(context, add_calibration, add_macro, macro_file, shift_model_year):
    """Solve a scenario.

    Use the --model_name and --scenario_name option to specify the scenario to solve.
    """
    # default scenario: MESSAGEix-Materials NoPolicy
    scenario = context.get_scenario()
    default_solve_opt = {
        "model": "MESSAGE",
        "solve_options": {"lpmethod": "4", "scaind": "-1"},
    }
    if shift_model_year:
        if not scenario.has_solution():
            scenario.solve(**default_solve_opt)
        if scenario.timeseries(year=scenario.firstmodelyear).empty:
            log.info(
                "Scenario has no timeseries data in baseyear. Starting"
                "reporting workflow before shifting baseyear."
            )
            run_reporting(context, False, False)
        # Shift base year
        scenario = scenario.clone(
            model=scenario.model,
            scenario=scenario.scenario + f"_{shift_model_year}",
            shift_first_model_year=shift_model_year,
        )

    if add_calibration:
        log.info(
            "After macro calibration a new scenario with the suffix _macro is created."
            "Make sure to use this scenario to solve with MACRO iterations."
        )
        if not scenario.has_solution():
            log.info(
                "Uncalibrated scenario has no solution. Solving the scenario"
                "without MACRO before calibration"
            )
            scenario.solve(**default_solve_opt)
            scenario.set_as_default()

        # update cost_ref and price_ref with new solution
        # f"SSP_dev_{context['ssp']}-R12-5y_macro_data_v0.12_mat.xlsx"
        update_macro_calib_file(scenario, macro_file)

        # After solving, add macro calibration
        log.info("Scenario solved, now adding MACRO calibration")
        # f"SSP_dev_{context['ssp']}-R12-5y_macro_data_v0.12_mat.xlsx"
        scenario = add_macro_COVID(scenario, macro_file)
        log.info("Scenario successfully calibrated.")

    if add_macro:
        default_solve_opt.update({"model": "MESSAGE-MACRO"})

    log.info("Start solving the scenario")
    scenario.solve(**default_solve_opt)
    scenario.set_as_default()


@cli.command("report")
@click.option(
    "--remove_ts",
    default=False,
    help="If True the existing timeseries in the scenario is removed.",
)
@click.option("--profile", default=False)
@click.pass_obj
def run_reporting(context, remove_ts, profile):
    """Run materials specific reporting, then legacy reporting."""
    from message_ix_models.model.material.report.reporting import report
    from message_ix_models.report.legacy.iamc_report_hackathon import (
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
            log.info("Existing timeseries are removed.")
        else:
            log.info("There are no timeseries to be removed.")
    else:
        if profile:
            import atexit
            import cProfile
            import io
            import pstats

            log.info("Profiling started...")
            pr = cProfile.Profile()
            pr.enable()
            log.info("Reporting material-specific variables")
            report(scenario)
            log.info("Reporting standard variables")
            reporting(
                mp,
                scenario,
                "False",
                scenario.model,
                scenario.scenario,
                merge_hist=True,
                merge_ts=True,
                run_config="materials_run_config.yaml",
            )

            def exit():
                pr.disable()
                log.info("Profiling completed")
                s = io.StringIO()
                pstats.Stats(pr, stream=s).sort_stats("cumulative").dump_stats(
                    "profiling.dmp"
                )

            atexit.register(exit)
        else:
            # Remove existing timeseries and add material timeseries
            log.info("Reporting material-specific variables")
            report(scenario)
            log.info("Reporting standard variables")
            reporting(
                mp,
                scenario,
                "False",
                scenario.model,
                scenario.scenario,
                merge_hist=True,
                merge_ts=True,
                run_config="materials_run_config.yaml",
            )


@cli.command("modify-cost", hidden=True)
@click.option("--ssp", default="SSP2", help="Suffix to the scenario name")
@click.pass_obj
def modify_costs_with_tool(context, ssp):
    base = context.get_scenario()
    scen = base.clone(model=base.model, scenario=base.scenario.replace("baseline", ssp))

    inv, fix = gen_te_projections(scen, ssp)
    scen.check_out()
    scen.add_par("fix_cost", fix)
    scen.add_par("inv_cost", inv)
    scen.commit(f"update cost assumption to: {ssp}")

    scen.solve(model="MESSAGE-MACRO", solve_options={"scaind": -1})


@cli.command("run-cbud-scenario", hidden=True)
@click.option(
    "--scenario",
    default="baseline_prep_lu_bkp_solved_materials_2025_macro",
    help="description of carbon budget for mitigation target",
)
@click.option("--budget", default="1000f")
@click.option("--model", default="MESSAGEix-Materials")
@click.pass_obj
def run_cbud_scenario(context, model, scenario, budget):
    if budget == "1000f":
        budget_i = 3667
    elif budget == "650f":
        budget_i = 1750
    else:
        log.error("chosen budget not available yet please choose 650f or 1000f")
        return

    base = context.get_scenario()
    scenario_cbud = base.clone(
        model=base.model,
        scenario=base.scenario + "_" + budget,
        shift_first_model_year=2030,
    )

    emission_dict = {
        "node": "World",
        "type_emission": "TCE",
        "type_tec": "all",
        "type_year": "cumulative",
        "unit": "???",
    }
    df = message_ix.make_df("bound_emission", value=budget_i, **emission_dict)
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


@cli.command("run-LED-cprice-scenario", hidden=True)
@click.option("--ssp", default="SSP2", help="Suffix to the scenario name")
@click.option(
    "--budget",
    default="1000f",
    help="description of carbon budget for mitigation target",
)
@click.pass_obj
def run_LED_cprice(context, ssp, budget):
    if budget in ["650f", "1000f"]:
        price_scen = message_ix.Scenario(
            context.get_platform(),
            "MESSAGEix-Materials",
            scenario=f"SSP_supply_cost_test_LED_macro_{budget}",
        )
    else:
        log.error(f"No price scenario available for budget: {budget}. Aborting..")
        return
    base = message_ix.Scenario(
        context.get_platform(),
        "MESSAGEix-Materials",
        scenario=f"SSP_supply_cost_test_{ssp}_macro",
    )
    scen_cprice = base.clone(
        model=base.model,
        scenario=base.scenario + f"_{budget}_LED_prices",
        shift_first_model_year=2025,
    )

    tax_emission_new = price_scen.var("PRICE_EMISSION")

    scen_cprice.check_out()
    tax_emission_new.columns = scen_cprice.par("tax_emission").columns
    tax_emission_new["unit"] = "USD/tCO2"
    scen_cprice.add_par("tax_emission", tax_emission_new)
    scen_cprice.commit("2 degree LED prices are added")
    log.info("New LED 1000f carbon prices added")

    scen_cprice.solve(model="MESSAGE-MACRO", solve_options={"scaind": -1})
    return


@cli.command("make-xls-input-vc-able", hidden=True)
@click.option(
    "--files",
    default="all",
    help="optionally specify which files to make VC-able - not implemented yet",
)
@click.pass_obj
def make_xls_input_vc_able(context, files):
    if files == "all":
        dirs = get_all_input_data_dirs()
        dirs = [i for i in dirs if i != "version control"]
        for dir in dirs:
            log.info(dir)
            files = os.listdir(package_data_path("material", dir))
            files = [i for i in files if ((i.endswith(".xlsx")) & ~i.startswith("~$"))]
            log.info(files)
            for filename in files:
                excel_to_csv(dir, filename)
    else:
        raise NotImplementedError
    return


@cli.command("test-calib", hidden=True)
@click.pass_obj
def test_calib(context):
    """Solve a scenario.

    Use the --model_name and --scenario_name option to specify the scenario to solve.
    """
    # Clone and set up
    from sdmx.model.common import Code
    from sdmx.model.v21 import Annotation

    from message_ix_models.model import macro
    from message_ix_models.util import identify_nodes

    scenario = context.get_scenario().clone("MESSAGEix-Materials", "test_macro_calib")
    scenario.set_as_default()

    def _c(id, sector):
        return Code(id=id, annotations=[Annotation(id="macro-sector", text=sector)])

    commodities = [
        _c("i_therm", "i_therm"),
        _c("i_spec", "i_spec"),
        _c("rc_spec", "rc_spec"),
        _c("rc_therm", "rc_therm"),
        _c("transport", "transport"),
    ]
    context.model.regions = identify_nodes(scenario)
    data = dict(
        config=macro.generate("config", context, commodities),
        aeei=macro.generate("aeei", context, commodities, value=0.02),
        drate=macro.generate("drate", context, commodities, value=0.05),
        depr=macro.generate("depr", context, commodities, value=0.05),
        lotol=macro.generate("lotol", context, commodities, value=0.05),
    )
    # Load other MACRO data from file
    data2 = macro.load(private_data_path("macro", "SSP1"))
    data.update(data2)

    scenario.add_macro(data, check_convergence=False)
    return
