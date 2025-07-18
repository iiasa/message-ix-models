# cli for GDP impacts
"""
GDP-Climate Impact Iteration CLI

Use the :doc:`CLI <message_ix_models:cli>` command :program:`mix-models gdp-ci`
to invoke the commands defined in this module.

This module defines a `click` CLI group with a command to perform iterative GDP
adjustments in MESSAGEix using outputs from MAGICC and climate damage functions.
"""

import gc
import logging
from pathlib import Path

import click
import ixmp as ix
import message_ix
import pandas as pd
import yaml

from message_ix_models.project.GDP_climate_shocks.call_climate_processor import (
    read_magicc_output,
    run_climate_processor,
    run_climate_processor_from_file,
)
from message_ix_models.project.GDP_climate_shocks.gdp_table_out_ISO import run_rime
from message_ix_models.project.GDP_climate_shocks.util import (
    add_slack_ix,
    apply_growth_rates,
    regional_gdp_impacts,
    run_emi_reporting,
    run_legacy_reporting,
)
from message_ix_models.util import package_data_path, private_data_path
from message_ix_models.util.click import common_params, scenario_param

log = logging.getLogger(__name__)


# functions that could go in utils
def load_config_from_path(config_path: str = "default") -> dict:
    if config_path == "default":
        config_file = package_data_path() / "GDP_climate_shocks" / "config.yaml"
    else:
        config_file = Path(config_path)
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def maybe_shift_year(scenario, shift_year):
    """Return dict with shift_firstmodelyear if requested and needed."""
    if shift_year and scenario.firstmodelyear != shift_year:
        return {"shift_first_model_year": shift_year}
    return {}


def run_initial_scenario_if_needed(
    mp, sc_ref, model_name_clone, scenario, shift_year, sc_str_rime
):
    """Run the base iteration (0) if MAGICC output not found."""
    input_path = private_data_path().parent / "reporting_output" / "magicc_output"
    file_in = input_path / f"{sc_str_rime}_0_magicc.xlsx"

    try:
        with open(file_in, "rb"):
            log.info(
                f"Found existing MAGICC output: {file_in.name}, skipping base run."
            )
            return
    except FileNotFoundError:
        log.info("No MAGICC output found, running base scenario...")

    kwargs = maybe_shift_year(sc_ref, shift_year)
    sc0 = sc_ref.clone(
        model_name_clone,
        scenario + "_GDP_CI_0",
        keep_solution=False,
        **kwargs,
    )

    if "INDC2030" in scenario:
        log.info("Adding slack to scenario with INDC2030")
        add_slack_ix(sc0)

    sc0.solve(solve_options={"lpmethod": "4"}, model="MESSAGE")
    sc0.set_as_default()

    run_emi_reporting(sc0, mp)
    run_climate_processor(sc0)
    log.info("Initial scenario completed")

    del sc0
    gc.collect()


def iterate_with_climate_impacts(
    sc_ref,
    model_name,
    scenario_name,
    damage_model,
    percentile,
    ssp,
    run_mode,
    shift_year,
    regions,
    rime_path,
):
    """Run climate-GDP iterations for convergence."""
    meanT = []
    sc_str = f"{scenario_name}_GDP_CI"
    sc_str_rime = f"{model_name}_{sc_str}"
    sc_str0 = f"{model_name}_{scenario_name}_GDP_CI_0"
    meanT.append(read_magicc_output(sc_str0, percentile))
    it = 0
    delta = 1

    while delta > 0.05:
        it += 1
        sc_name_new = f"{scenario_name}_GDP_CI_{percentile}_{damage_model}_{it}"
        sc_str_full = f"{model_name}_{sc_str}"

        kwargs = maybe_shift_year(sc_ref, shift_year)
        scs = sc_ref.clone(
            model_name,
            sc_name_new,
            keep_solution=False,
            **kwargs,
        )

        gdp_change_df = regional_gdp_impacts(
            sc_str_full, damage_model, it, ssp, regions, percentile
        )
        apply_growth_rates(scs, gdp_change_df)

        if "INDC2030" in scenario_name:
            log.info("Adding slack to scenario with INDC2030")
            add_slack_ix(scs)

        scs.solve(
            solve_options={"lpmethod": "4", "barcrossalg": "2"},
            model=run_mode,
        )
        scs.set_as_default()
        log.info("Model solved, running reporting")
        run_emi_reporting(scs, scs.platform)
        log.info("Reporting completed, ready to run MAGICC")
        run_climate_processor(scs)
        log.info("Run RIME")
        run_rime(sc_str_rime, damage_model, it, rime_path, percentile)

        sc_str1 = f"{scs.model}_{scs.scenario}"
        meanT.append(read_magicc_output(sc_str1, percentile))
        delta = abs(meanT[it - 1] - meanT[it])
        log.info(f"Delta after iteration {it}: {delta:.3f}")

    logging.info(f"Convergence with scenario {scs.scenario}. Run full reporting")
    run_legacy_reporting(scs, scs.platform)
    del scs, meanT, gdp_change_df
    gc.collect()


@click.group("gdp-ci")
def cli():
    """GDP-Climate Impact iteration workflow."""


@cli.command("run_full")
@click.option("--model_name", help="Original model name (e.g., ENGAGE_SSP2...).")
@click.option("--model_name-clone", help="Cloned model name for GDP-CI.")
@click.option(
    "--shift_year",
    type=int,
    default=None,
    help="First model year to shift to (optional; only used if different from scenario).",
)
@click.option("--scens_ref", multiple=True, help="Reference scenario(s) to run.")
@click.option("--damage_model", multiple=True, help="Damage model(s) to apply.")
@click.option(
    "--percentiles",
    multiple=True,
    type=int,
    required=False,
    help="Percentile(s) to run in RIME and MAGICC.",
)
@common_params("regions")
@click.option("--ssp", help="SSP scenario name (e.g., SSP2).")
@click.option(
    "--config",
    type=str,
    default="default",
    help='Optional config file path, or "default" to use built-in config.',
)
def run_full(
    model_name,
    model_name_clone,
    ssp,
    scens_ref,
    damage_model,
    percentiles,
    shift_year,
    regions,
    config,
):
    """Run full workflow.

    Args:
        model_name: Original model name (e.g., ENGAGE_SSP2...).
        model_name_clone: Cloned model name for GDP-CI.
        ssp: SSP scenario name (e.g., SSP2).
        scens_ref: Reference scenario(s) to run.
        damage_model: Damage model(s) to apply.
        percentiles: Percentile(s) to run in RIME and MAGICC.
        shift_year: First model year to shift to (optional; only used if different from scenario).
        regions: Region(s) to run.
        config: Optional config file path, or "default" to use built-in config.
    """
    cfg = load_config_from_path(config)

    # Replace values from CLI if given
    model_name = model_name or cfg["model_name"]
    model_name_clone = model_name_clone or cfg["model_name_clone"]
    ssp = ssp or cfg.get("ssp")
    scens_ref = scens_ref or cfg["scens_ref"]
    damage_model = damage_model or cfg["damage_model"]
    percentiles = percentiles or cfg["percentiles"]
    shift_year = shift_year if shift_year is not None else cfg.get("shift_year")
    regions = regions or cfg.get("regions")
    if isinstance(regions, list):
        region = regions[0]
    else:
        region = regions
    rime_path = cfg["rime_path"]

    # Log final config values
    logging.info("Final configuration:")
    for k, v in {
        "model_name": model_name,
        "model_name_clone": model_name_clone,
        "ssp": ssp,
        "scens_ref": scens_ref,
        "damage_model": damage_model,
        "percentiles": percentiles,
        "shift_year": shift_year,
        "regions": region,
        "rime_path": rime_path,
    }.items():
        logging.info(f"  {k}: {v}")
    # run message-macro
    run_mode = "MESSAGE-MACRO"

    #### actual build block
    ## for loop across scens_ref
    for scenario in scens_ref:
        # initiate scenario
        mp = ix.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])
        sc_ref = message_ix.Scenario(mp, model_name, scenario, cache=True)
        sc_str = f"{scenario}_GDP_CI"
        sc_str_rime = f"{model_name_clone}_{sc_str}"
        # only run the initial scenario, if magicc file not existing
        run_initial_scenario_if_needed(
            mp, sc_ref, model_name_clone, scenario, shift_year, sc_str_rime
        )

        # run RIME on sc0_magicc

        for pp in percentiles:
            run_rime(sc_str_rime, damage_model, 0, rime_path, pp)
            logging.info(
                f"Iteraction 0 for {sc_str}_{pp} completed. Ready to apply climate impacts."
            )

            ## for loop across damage_model
            for dam_mod in damage_model:
                iterate_with_climate_impacts(
                    sc_ref=sc_ref,
                    model_name=model_name_clone,
                    scenario_name=scenario,
                    damage_model=dam_mod,
                    percentile=pp,
                    ssp=ssp,
                    run_mode=run_mode,
                    regions=region,
                    shift_year=shift_year,
                    rime_path=rime_path,
                )

        mp.close_db()
        del mp
        gc.collect()
        log.info("All scenarios completed")


@cli.command("run_magicc_rime")
@click.option(
    "--config",
    type=str,
    default="default",
    help='Optional config file path, or "default" to use built-in config.',
)
@click.option("--model_name", help="Original model name (e.g., ENGAGE_SSP2...).")
@click.option("--model_name-clone", help="Cloned model name for GDP-CI.")
@click.option("--ssp", help="SSP scenario name (e.g., SSP2).")
@click.option("--scens_ref", multiple=True, help="Reference scenario(s) to run.")
@click.option("--damage_model", multiple=True, help="Damage model(s) to apply.")
@click.option(
    "--percentiles",
    multiple=True,
    type=int,
    required=False,
    help="Percentile(s) to run in RIME and MAGICC.",
)
@click.option(
    "--shift_year",
    type=int,
    default=None,
    help="First model year to shift to (optional; only used if different from scenario).",
)
@common_params("regions")
@click.option(
    "--input_only",
    type=str,
    required=False,
    help="Use only input files, do not use ixmp.",
)
def run_magicc_rime_cli(
    config,
    model_name,
    model_name_clone,
    ssp,
    scens_ref,
    damage_model,
    percentiles,
    shift_year,
    regions,
    input_only,
):
    """Launches the run_magicc_rime function from cli."""
    run_magicc_rime(
        config,
        model_name,
        model_name_clone,
        ssp,
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        regions,
        input_only,
    )


def run_magicc_rime(
    config="default",
    model_name=None,
    model_name_clone=None,
    ssp=None,
    scens_ref=None,
    damage_model=None,
    percentiles=None,
    shift_year=None,
    regions=None,
    input_only=None,
):
    """Run Magicc and RIME on a scenario or a iam reporting file.

    Parameters
    ----------
    config : str
        Optional config file path, or "default" to use built-in config.
    model_name : str
        Original model name (e.g., ENGAGE_SSP2...).
    model_name_clone : str
        Cloned model name for GDP-CI.
    ssp : str
        SSP scenario name (e.g., SSP2).
    scens_ref : str
        Reference scenario(s) to run.
    damage_model : str
        Damage model(s) to apply.
    percentiles : int
        Percentile(s) to run in RIME and MAGICC.
    shift_year : int
        First model year to shift to (optional; only used if different from scenario).
    regions : str
        Region(s) to run.

    input_only : bool
        Use only input files, do not use ixmp.
    """
    logging.info("Config:", config)
    cfg = load_config_from_path(config)

    # Replace values from CLI if given
    model_name = model_name or cfg["model_name"]
    model_name_clone = model_name_clone or cfg["model_name_clone"]
    ssp = ssp or cfg.get("ssp")
    scens_ref = scens_ref or cfg["scens_ref"]
    damage_model = damage_model or cfg["damage_model"]
    percentiles = percentiles or cfg["percentiles"]
    shift_year = shift_year if shift_year is not None else cfg.get("shift_year")
    regions = regions or cfg.get("regions")
    if isinstance(regions, list):
        region = regions[0]
    else:
        region = regions
    rime_path = cfg["rime_path"]

    # Log final config values
    logging.info("Final configuration:")
    for k, v in {
        "model_name": model_name,
        "model_name_clone": model_name_clone,
        "ssp": ssp,
        "scens_ref": scens_ref,
        "damage_model": damage_model,
        "percentiles": percentiles,
        "shift_year": shift_year,
        "regions": region,
        "rime_path": rime_path,
    }.items():
        logging.info(f"  {k}: {v}")

    if input_only == "single":
        # load path from cfg
        magicc_input_path = cfg["magicc_input_path"]
        logging.info("Using input-only singe mode from file: %s", magicc_input_path)
        for scenario in scens_ref:
            run_climate_processor_from_file(magicc_input_path, model_name, scenario)
            logging.info("Scenario completed with MAGICC, proceed to RIME")
            # run RIME on sc0_magicc
            sc_str_rime = f"{model_name}_{scenario}"
            for pp in percentiles:
                run_rime(sc_str_rime, damage_model, 0, rime_path, pp)
                logging.info(f"Iteraction 0 with RIME for {sc_str}_{pp} completed.")

    elif input_only == "list":
        # load path from cfg
        magicc_input_path = cfg["magicc_input_path"]
        magicc_input_list = cfg["magicc_input_list"]
        logging.info(
            "Using input-only singe mode from file: %s and list: %s",
            magicc_input_path,
            magicc_input_list,
        )
        scenlist_df = pd.read_csv(magicc_input_list)
        for m, s in zip(scenlist_df["model"], scenlist_df["scenario"]):
            try:
                logging.info("running MAGICC for %s, %s", m, s)
                run_climate_processor_from_file(magicc_input_path, m, s)
                logging.info("MAGICC done")
                sc_str_rime = f"{m}_{s}"
                for pp in percentiles:
                    run_rime(sc_str_rime, damage_model, 0, rime_path, pp)
                    logging.info(
                        f"Iteraction 0 with RIME for {sc_str_rime}_{pp} completed."
                    )
            except Exception as e:
                print(f"❌ Failed: {s} – {e}")
        logging.info("All scenarios completed with MAGICC and RIME")

    else:
        #### actual build block
        ## for loop across scens_ref
        for scenario in scens_ref:
            # initiate scenario
            mp = ix.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])
            sc_ref = message_ix.Scenario(mp, model_name, scenario, cache=True)
            sc_str = f"{scenario}_GDP_CI"
            sc_str_rime = f"{model_name_clone}_{sc_str}"
            # only run the initial scenario, if magicc file not existing
            run_initial_scenario_if_needed(
                mp, sc_ref, model_name_clone, scenario, shift_year, sc_str_rime
            )
            # run RIME on sc0_magicc
            for pp in percentiles:
                run_rime(sc_str_rime, damage_model, 0, rime_path, pp)
                logging.info(f"Iteraction 0 with RIME for {sc_str}_{pp} completed.")

            mp.close_db()
            del mp
            gc.collect()
    log.info("All scenarios completed")
