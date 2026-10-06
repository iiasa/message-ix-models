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
from typing import Optional

import click
import ixmp as ix
import message_ix
import pandas as pd

from message_ix_models.project.GDP_climate_shocks.call_climate_processor import (
    read_magicc_output,
    run_climate_processor,
    run_climate_processor_from_file,
)
from message_ix_models.project.GDP_climate_shocks.gdp_table_out_ISO import run_rime
from message_ix_models.project.GDP_climate_shocks.util import (
    add_slack_ix,
    apply_growth_rates,
    load_config_from_path,
    maybe_shift_year,
    regional_gdp_impacts,
    run_emi_reporting,
    run_legacy_reporting,
)
from message_ix_models.util import private_data_path
from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


def run_initial_scenario_if_needed(
    mp,
    sc_ref,
    model_name_clone: str,
    scenario: str,
    shift_year: int,
    sc_str_rime: str,
) -> None:
    """
    Run the base iteration (0) of the scenario if MAGICC output is not found.

    Parameters:
    ----------
    mp :
        MESSAGEix platform instance for running scenarios.
    sc_ref :
        Reference scenario to be cloned.
    model_name_clone : str
        Name for the cloned model.
    scenario : str
        Scenario name for the run.
    shift_year : int
        Year to shift the scenario's first model year to, if needed.
    sc_str_rime : str
        Scenario string used to locate MAGICC output files.

    Returns:
    -------
    None
        Runs the scenario and processes output if needed.
    """
    rep_path = input_path = private_data_path().parent / "reporting_output"
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

    file_report = rep_path / f"{sc_str_rime}_0.xlsx"
    if file_report.exists():
        log.info(
            f"Found existing reporting output: {file_report.name}, skipping reporting."
        )
        sc0 = message_ix.Scenario(mp, model_name_clone, scenario + "_GDP_CI_0")
    else:
        kwargs = maybe_shift_year(sc_ref, shift_year)
        sc0 = sc_ref.clone(
            model_name_clone,
            scenario + "_GDP_CI_0",
            keep_solution=False,
            **kwargs,
        )
        # specific to the ENGAGE scenario
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
    model_name: str,
    scenario_name: str,
    damage_model: str,
    percentile: float,
    ssp: str,
    run_mode: str,
    shift_year: int,
    regions: list[str],
    rime_path: str,
) -> None:
    """
    Run iterative climate–GDP feedback calculations until temperature change converges.

    Parameters:
    ----------
    sc_ref :
        Reference MESSAGEix scenario to be cloned for iterations.
    model_name : str
        Name of the model to be used in cloned scenarios.
    scenario_name : str
        Base scenario name without GDP–climate iteration suffixes.
    damage_model : str
        Name of the damage function model used for GDP impact estimation.
    percentile : float
        Percentile of climate damages to use in impact calculations (no decimals).
    ssp : str
        Shared Socioeconomic Pathway identifier (e.g. SSP2).
    run_mode : str
        Model type for solving (e.g., "MESSAGE").
    shift_year : int
        First model year shift, if needed.
    regions : list of str
        List with a region code for regional GDP calculations (e.g. R12).
    rime_path : str
        Path to store RIME (regional integrated model evaluation) outputs.

    Returns:
    -------
    None
        Runs the iterative process until convergence and produces reporting outputs.
    """
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
        # specific to the ENGAGE scenario
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


def load_and_override_config(
    config,
    model_name,
    model_name_clone,
    ssp,
    scens_ref,
    damage_model,
    percentiles,
    shift_year,
    regions,
):
    cfg = load_config_from_path(config)
    model_name = model_name or cfg["model_name"]
    model_name_clone = model_name_clone or cfg["model_name_clone"]
    ssp = ssp or cfg.get("ssp")
    scens_ref = scens_ref or cfg["scens_ref"]
    damage_model = damage_model or cfg["damage_model"]
    percentiles = percentiles or cfg["percentiles"]
    shift_year = shift_year if shift_year is not None else cfg.get("shift_year")
    regions = regions or cfg.get("regions")
    region = regions[0] if isinstance(regions, list) else regions
    rime_path = cfg["rime_path"]
    return (
        cfg,
        model_name,
        model_name_clone,
        ssp,
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        region,
        rime_path,
    )


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
    help="First model year to shift to (optional; used if different from scenario).",
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
    config: str = "default",
    model_name: Optional[str] = None,
    model_name_clone: Optional[str] = None,
    ssp: Optional[str] = None,
    scens_ref: Optional[tuple[str, ...]] = None,
    damage_model: Optional[tuple[str, ...]] = None,
    percentiles: Optional[tuple[int, ...]] = None,
    shift_year: Optional[int] = None,
    regions: Optional[list[str]] = None,
) -> None:
    """
    Run the full GDP–Climate Impact iteration workflow.
    1. Runs an initial scenario with MAGICC output.
    2. For a damage function and percentile it applies the GDP damages running RIME,
       MESSAGE-MACRO and MAGICC, untill temperature converges below a given treshold.
    3. Runs full reporting (based on legacy reporting setup on message_data).

    Parameters:
    ----------
    model_name : str
        Original model name (e.g., 'ENGAGE_SSP2').
    model_name_clone : str
        Cloned model name for the GDP–CI workflow.
    ssp : str
        Shared Socioeconomic Pathway identifier (e.g., 'SSP2').
    scens_ref : tuple of str
        Reference scenario name(s) to run.
    damage_model : tuple of str
        One or more damage model names to apply.
    percentiles : tuple of int
        Percentile values to run in RIME and MAGICC.
    shift_year : int
        First model year to shift to (only used if different from the scenario's).
    regions : list of str
        Single region code provided as a list for CLI compatibility.
    config : str
        Path to an optional configuration file, or "default" to use built-in settings.
    """
    (
        _,
        model_name,
        model_name_clone,
        ssp,
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        region,
        rime_path,
    ) = load_and_override_config(
        config,
        model_name,
        model_name_clone,
        ssp,
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        regions,
    )

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

    # actual build block
    # for loop across scens_ref
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
                f"Iteraction 0 for {sc_str}_{pp} completed. Now apply climate impacts."
            )

            # for loop across damage_model
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


# auxiliary functions for run_magicc_rime
def run_from_single_input(
    cfg: dict[str, object],
    model_name: str,
    scens_ref: tuple[str, ...],
    damage_model: tuple[str, ...],
    percentiles: tuple[int, ...],
    rime_path: str,
):
    """Run MAGICC and RIME from a single pre-generated MAGICC input file."""
    magicc_input_path = cfg["magicc_input_path"]
    # path to check if magicc file already exist (reduce rerunning time)
    input_path = private_data_path().parent / "reporting_output" / "magicc_output"
    logging.info("Using input-only singe mode from file: %s", magicc_input_path)
    for scenario in scens_ref:
        sc_str_rime = f"{model_name}_{scenario}"
        file_in = input_path / f"{sc_str_rime}_0_magicc.xlsx"

        if file_in.exists():
            log.info(
                f"Found existing MAGICC output: {file_in.name}, skipping base run."
            )
        else:
            log.info("No MAGICC output found, running base scenario...")
            run_climate_processor_from_file(magicc_input_path, model_name, scenario)
            logging.info("Scenario completed with MAGICC, proceed to RIME")

        # run RIME on sc0_magicc
        for pp in percentiles:
            run_rime(sc_str_rime, damage_model, 0, rime_path, pp)
            logging.info(f"Iteraction 0 with RIME for {sc_str_rime}_{pp} completed.")


def run_from_input_list(
    cfg: dict[str, object],
    damage_model: tuple[str, ...],
    percentiles: tuple[int, ...],
    rime_path: str,
):
    """Run MAGICC and RIME for all scenarios listed in the config CSV."""
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
            logging.info(f"❌ Failed: {s} – {e}")
    logging.info("All scenarios completed with MAGICC and RIME")


def run_from_messageix_scenarios(
    model_name: str,
    model_name_clone: str,
    scens_ref: tuple[str, ...],
    damage_model: tuple[str, ...],
    percentiles: tuple[int, ...],
    shift_year: Optional[int],
    rime_path: str,
):
    """Run MAGICC and RIME directly from MESSAGEix scenarios."""
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


@cli.command("run_magicc_rime")
@click.option(
    "--config",
    type=str,
    default="default",
    help='Optional config file path, or "default" to use built-in config.',
)
@click.option("--model_name", help="Original model name (e.g., ENGAGE_SSP2...).")
@click.option("--model_name-clone", help="Cloned model name for GDP-CI.")
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
    help="First model year to shift to (optional; used if different from scenario).",
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
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        regions,
        input_only,
    )


def run_magicc_rime(
    config: str = "default",
    model_name: Optional[str] = None,
    model_name_clone: Optional[str] = None,
    scens_ref: Optional[tuple[str, ...]] = None,
    damage_model: Optional[tuple[str, ...]] = None,
    percentiles: Optional[tuple[int, ...]] = None,
    shift_year: Optional[int] = None,
    regions: Optional[list[str]] = None,
    input_only: Optional[str] = None,
) -> None:
    """
    Run MAGICC and RIME climate impact calculations for one or more scenarios.

    This function runs the MAGICC climate model and the RIME impact model
    either:
      - Directly from MESSAGEix scenarios (default case),
      - Or from pre-generated MAGICC input files (`input_only="single"`),
      - Or from a list of MAGICC input scenarios (`input_only="list"`).

    Cases
    -----
    1. **Default case** (input_only is None):
       - Loads each reference scenario (`scens_ref`) from the MESSAGEix database.
       - Runs MAGICC if no existing MAGICC output exists for the scenario.
       - Runs RIME for each specified percentile.

    2. **Single input file** (`input_only="single"`):
       - Uses a single pre-generated MAGICC input file from `config:magicc_input_path`.
       - Runs MAGICC and RIME for each `scens_ref` scenario.

    3. **List of inputs** (`input_only="list"`):
       - Reads scenario model names from a CSV file in `config:magicc_input_path`.
       - Runs MAGICC and RIME for each listed scenario in 'config:magicc_input_list'.

    Parameters
    ----------
    config : str
        Path to a config file, or `"default"` to use the built-in one.
    model_name : str, optional
        Original model name (e.g., "ENGAGE_SSP2").
    model_name_clone : str, optional
        Cloned model name for GDP-CI variant.
    scens_ref : tuple[str, ...], optional
        One or more reference scenarios to process.
    damage_model : tuple[str, ...], optional
        One or more damage models to apply.
    percentiles : tuple[int, ...], optional
        Percentiles to run in RIME and MAGICC.
    shift_year : int, optional
        First model year to shift to (only used if different from scenario start year).
    regions : list[str], optional
        Region(s) to run.
    input_only : {"single", "list"}, optional
        Use pre-generated MAGICC input files instead of running from MESSAGEix.

    Returns
    -------
    None
    """
    logging.info("Config:", config)
    (
        cfg,
        model_name,
        model_name_clone,
        _,
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        region,
        rime_path,
    ) = load_and_override_config(
        config,
        model_name,
        model_name_clone,
        None,
        scens_ref,
        damage_model,
        percentiles,
        shift_year,
        regions,
    )

    # Log final config values
    logging.info("Final configuration:")
    for k, v in {
        "model_name": model_name,
        "model_name_clone": model_name_clone,
        "scens_ref": scens_ref,
        "damage_model": damage_model,
        "percentiles": percentiles,
        "shift_year": shift_year,
        "regions": region,
        "rime_path": rime_path,
    }.items():
        logging.info(f"  {k}: {v}")

    if input_only == "single":
        run_from_single_input(
            cfg, model_name, scens_ref, damage_model, percentiles, rime_path
        )
    elif input_only == "list":
        run_from_input_list(cfg, damage_model, percentiles, rime_path)
    else:
        run_from_messageix_scenarios(
            model_name,
            model_name_clone,
            scens_ref,
            damage_model,
            percentiles,
            shift_year,
            rime_path,
        )
    log.info("All scenarios completed")
