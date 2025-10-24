"""Generate emissions report for MESSAGE scenarios.

This script:
1. Loads a MESSAGE-GLOBIOM scenario from ixmp database
2. Clones it for emissions reporting
3. Runs legacy emissions reporting with full species coverage
4. Outputs IAMC-format Excel file with all emission categories

Usage:
    python generate_emissions_report.py

Configuration:
    - Emissions species controlled by GDP_shock_emiss_run_config.yaml
    - Unit conversions in GDP_shock_units.yaml

Output:
    - message_ix_models/data/report/legacy/reporting_output/<scenario_name>.xlsx
"""

import ixmp
from message_ix import Scenario
from message_ix_models.report.legacy.iamc_report_hackathon import report as legacy_report

# Connect to platform
print("Connecting to ixmp_dev platform...")
mp = ixmp.Platform(name="ixmp_dev")

# Load scenario
model = "MESSAGE_GLOBIOM_SSP2_v6.1"
scenario_name = "Main_baseline_baseline_nexus_7p0_high"  # Has solution!

print(f"\nLoading scenario: {model} / {scenario_name}")
sc_orig = Scenario(mp, model=model, scenario=scenario_name)
print(f"  Version: {sc_orig.version}")
print(f"  Has solution: {sc_orig.has_solution()}")

# Clone for testing
clone_name = f"{scenario_name}_emissions_test"
print(f"\nCloning to: {model} / {clone_name}")

try:
    sc = sc_orig.clone(model=model, scenario=clone_name, keep_solution=True)
    print(f"  Cloned version: {sc.version}")
except Exception as e:
    print(f"  Clone exists, loading: {e}")
    sc = Scenario(mp, model=model, scenario=clone_name)
    print(f"  Loaded version: {sc.version}")

print(f"  Has solution: {sc.has_solution()}")

# Run emissions reporting
print("\n" + "="*70)
print("RUNNING EMISSIONS REPORTING")
print("="*70)

print("\nCalling legacy reporting with GDP_shock_emiss_run_config.yaml...")
print(f"  Model: {sc.model}")
print(f"  Scenario: {sc.scenario}")

try:
    result = legacy_report(
        mp=mp,
        scen=sc,
        merge_hist=True,
        merge_ts=False,
        run_config="GDP_shock_emiss_run_config.yaml"
    )

    print("\n" + "="*70)
    print("SUCCESS!")
    print("="*70)

    if result is not None:
        print(f"\nReturned DataFrame shape: {result.shape}")
        print(f"Variables: {result['Variable'].nunique()}")
        print(f"\nEmissions variables:")
        emi_vars = result[result['Variable'].str.startswith('Emissions|')]['Variable'].unique()
        for var in sorted(emi_vars)[:20]:
            print(f"  - {var}")
        if len(emi_vars) > 20:
            print(f"  ... and {len(emi_vars) - 20} more")
    else:
        print("\nNote: legacy_report() returns None (writes to file instead)")
        print("Check reporting_output/ directory for Excel file")

except Exception as e:
    print("\n" + "="*70)
    print("ERROR!")
    print("="*70)
    print(f"\n{type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\nDone!")
