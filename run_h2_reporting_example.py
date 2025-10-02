import ixmp
import message_ix
from message_ix.report import Reporter
import pyam
from pathlib import Path

# Import the reporting function
from message_ix_models.report.hydrogen.h2_reporting import run_h2_reporting


def main():
    """
    Main function to run the hydrogen fugitive emissions reporting and export the results.
    """
    # --- Configuration ---
    # Name of the ixmp platform to connect to
    platform_name = "ixmp-dev"  # TODO: User needs to change this

    # Scenario identifiers
    model_name = "hyway_SSP_SSP2_v6.2"
    scenario_name = "baseline_1000f_h2_ct_h2_emissions"

    # Output file path
    output_dir = Path(".")  # Save in the current directory
    output_filename = f"{model_name}_{scenario_name}_h2_report.xlsx"
    output_path = output_dir / output_filename
    # --- End of Configuration ---

    print(f"Connecting to platform '{platform_name}'...")
    mp = ixmp.Platform(name=platform_name)

    print(f"Loading scenario '{model_name}/{scenario_name}'...")
    try:
        scenario = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)
    except ValueError as e:
        print(f"Error loading scenario: {e}")
        print("Please make sure the platform is running and the scenario exists.")
        return

    print("Creating reporter...")
    rep = Reporter.from_scenario(scenario)

    print("Running hydrogen fugitive emissions reporting...")
    dfs = run_h2_reporting(rep, scenario.model, scenario.scenario)

    print("Concatenating results...")
    py_df = pyam.concat(dfs)

    print(f"Saving report to '{output_path}'...")
    try:
        py_df.to_excel(output_path)
        print("Report successfully saved.")
    except Exception as e:
        print(f"Error saving report: {e}")

    print("\n--- Reporting Summary ---")
    print(py_df.timeseries())
    print("--- End of Summary ---")


if __name__ == "__main__":
    main()
