# script to add GDP scock to the MESSAGE-MACRO model
# the script follows the process in buil.py but it automates it
# author: Adriano Vinca, 2024

#### preamble ####
import argparse
import gc
import logging
import os

import ixmp as ix
import message_ix
import psutil
from call_climate_processor import read_magicc_output, run_climate_processor
from gdp_table_out_ISO import run_rime
from message_ix_models.util import private_data_path
from util import (
    apply_growth_rates,
    regional_gdp_impacts,
    run_emi_reporting,
    run_legacy_reporting,
)

log = logging.getLogger(__name__)


def setup_logging():
    log_directory = private_data_path().parent / "reporting_output"
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, "memory_usage.log")

    # Test writing to the file directly to check for permission issues
    try:
        with open(log_file_path, "w") as f:
            f.write("Test log file creation\n")
        print(f"Successfully created and wrote to the log file: {log_file_path}")
    except Exception as e:
        print(f"Failed to create or write to the log file: {e}")

    # Configure logging to overwrite the file each time the script runs
    logging.basicConfig(
        filename=log_file_path,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )
    logging.info("Logging configuration set up.")

    # Check if the logging module has any handlers set up
    print(f"Logging has handlers: {logging.getLogger().hasHandlers()}")

    # Print all handlers (should show the file handler)
    for handler in logging.getLogger().handlers:
        print(f"Handler: {handler}")

    # Create a file handler explicitly
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(message)s")
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().setLevel(logging.INFO)


def log_memory_usage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    logging.info(
        f"RSS: {memory_info.rss / 1024**2:.2f} MB, VMS: {memory_info.vms / 1024**2:.2f} MB"
    )
    print(
        f"Logged memory usage: RSS: {memory_info.rss / 1024**2:.2f} MB, VMS: {memory_info.vms / 1024**2:.2f} MB"
    )  # Debugging: Print to ensure function is called


#### build process ####
def main(scens_ref, damage_model, percentiles):
    setup_logging()
    log_memory_usage()
    # IIASA users to access the database
    # starting scenario model and scenario names
    modelName = "ENGAGE_SSP2_v4.1.8.3.1_T4.5v2_r3.1"
    model_name_clone = "ENGAGE_SSP2_T4.5_GDP_CI_2025"
    SSP = "SSP2"
    run_mode = "MESSAGE-MACRO"

    #### actual build block

    ## for loop across scens_ref
    for scenario in scens_ref:
        # initiate scenario
        mp = ix.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])
        sc_ref = message_ix.Scenario(mp, modelName, scenario, cache=True)
        sc_str = f"{scenario}_GDP_CI"
        sc_str_rime = f"{model_name_clone}_{sc_str}"
        # only run the initial scenario, if magic file not existing (comment this after big updates)
        input_path = private_data_path().parent / "reporting_output" / "magicc_output"
        fname_input = f"{sc_str_rime}_0_magicc.xlsx"
        file_in = input_path / fname_input
        # check if file_in exists
        if not file_in.exists():
            log.info("Solving loop 0 scenario")
            sc0 = sc_ref.clone(
                model_name_clone,
                scenario + "_GDP_CI_" + str(0),
                keep_solution=False,
                shift_first_model_year=2030,
            )
            # solve the scenario without climate impacts
            sc0.solve(solve_options={"lpmethod": "4"}, model="MESSAGE")
            log_memory_usage()
            sc0.set_as_default()
            log.info("Model solved, running reporting")
            run_emi_reporting(sc0, mp)
            log_memory_usage()
            log.info("Reporting completed, ready to run MAGICC")

            # run MAGICC
            run_climate_processor(sc0)
            del sc0
            gc.collect()
            log_memory_usage()
        # run RIME on sc0_magicc

        for pp in percentiles:
            run_rime(sc_str_rime, damage_model, 0, "vinca", pp)
            logging.info(
                f"Iteraction 0 for {sc_str}_{pp} completed. Ready to apply climate impacts."
            )
            sc_str0 = f"{model_name_clone}_{scenario}_GDP_CI_0"

            ## for loop across damage_model
            for dam_mod in damage_model:
                # start with iterations
                meanT = []
                meanT.append(read_magicc_output(sc_str0, pp))

                # some while loop with convergence criterion
                it = 0
                # delta is the diff in global average temperature in 2100
                delta = 1
                while delta > 0.05:
                    it = it + 1
                    sc_name_new = f"{scenario}_GDP_CI_{pp}_{dam_mod}_{it}"
                    sc_str_full = f"{model_name_clone}_{sc_str}"
                    scs = sc_ref.clone(
                        model_name_clone,
                        sc_name_new,
                        keep_solution=False,
                        shift_first_model_year=2030,
                    )
                    # extract gdp impacts, aggregate at the regional level and apply to the model
                    gdp_change_df = regional_gdp_impacts(
                        sc_str_full, dam_mod, it, SSP, pp
                    )
                    apply_growth_rates(scs, gdp_change_df)
                    scs.solve(
                        solve_options={"lpmethod": "4", "barcrossalg": "2"},
                        model=run_mode,  # "barcrossalg": "2"
                    )
                    log_memory_usage()
                    scs.set_as_default()
                    log.info("Model solved, running reporting")
                    run_emi_reporting(scs, mp)
                    log.info("Reporting completed, ready to run MAGICC")
                    # run MAGICC
                    run_climate_processor(scs)
                    log_memory_usage()
                    # run RIME on sc_magicc
                    log.info("Run RIME")
                    run_rime(sc_str_rime, dam_mod, it, "vinca", pp)
                    log_memory_usage()
                    # this already makes output for all dam_mod, which is redundant
                    sc_str1 = f"{scs.model}_{scs.scenario}"
                    meanT.append(read_magicc_output(sc_str1, pp))
                    # criteria for ending the loop, difference in temperature
                    delta = abs(meanT[it - 1] - meanT[it])

                # define official scenario
                logging.info(
                    f"Convergence with scenario {scs.scenario}. Run full reporting"
                )
                run_legacy_reporting(scs, mp)
                # clean up

                del scs, meanT, gdp_change_df
                gc.collect()
                log_memory_usage()

        mp.close_db()
        del mp
        gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run climate impact analysis.")
    parser.add_argument(
        "--scens_ref", nargs="+", required=True, help="List of reference scenarios"
    )
    parser.add_argument(
        "--damage_model", nargs="+", required=True, help="List of damage models"
    )
    parser.add_argument(
        "--percentiles", nargs="+", type=int, required=True, help="List of percentiles"
    )

    args = parser.parse_args()

    main(
        scens_ref=args.scens_ref,
        damage_model=args.damage_model,
        percentiles=args.percentiles,
    )
