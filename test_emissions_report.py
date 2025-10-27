"""Test full emissions reporting on MESSAGE-GLOBIOM SSP2 baseline scenario.

This runs the complete legacy emissions reporting pipeline which:
1. Extracts CO2, CH4, N2O, BC, OC, CO, NH3, SO2, NOx, VOC, HFCs, F-gases
2. Aggregates to IAMC format with hierarchical reporting variables
3. Outputs to Excel file in reporting_output/ directory

Expected memory usage: ~15GB (may OOM on systems with less memory)
"""

import ixmp
from message_ix import Scenario
from message_ix_models.report.legacy.iamc_report_hackathon import report
from pathlib import Path

print("=" * 70)
print("FULL EMISSIONS REPORTING TEST")
print("=" * 70)

# Connect to platform
print("\nConnecting to ixmp_dev platform...")
mp = ixmp.Platform(name="ixmp_dev")

# Scenario details
model = "MESSAGE_GLOBIOM_SSP2_v6.1"
scenario_name = "Main_baseline_baseline_nexus_7p0_high"

print(f"\nLoading scenario: {model} / {scenario_name}")
scen = Scenario(mp, model=model, scenario=scenario_name)

print(f"  Version: {scen.version}")
print(f"  Has solution: {scen.has_solution()}")
print(f"  Years: {sorted(scen.set('year').tolist())}")

# Run emissions reporting
print("\n" + "=" * 70)
print("RUNNING EMISSIONS REPORTING")
print("=" * 70)
print("\nThis will extract and aggregate all emission species...")
print("Expected time: 5-10 minutes")
print("Expected memory: ~15GB")
print("\nConfig: GDP_shock_emiss_run_config.yaml")
print("Output: message_ix_models/data/report/legacy/reporting_output/\n")

try:
    report(
        mp=mp,
        scen=scen,
        run_config='GDP_shock_emiss_run_config.yaml',
        merge_hist=False,
        merge_ts=False
    )

    print("\n" + "=" * 70)
    print("SUCCESS")
    print("=" * 70)

    # Check output files
    output_dir = Path("message_ix_models/data/report/legacy/reporting_output")
    output_files = list(output_dir.glob("*.xlsx"))

    if output_files:
        print(f"\nGenerated {len(output_files)} output file(s):")
        for f in output_files:
            size_mb = f.stat().st_size / 1024**2
            print(f"  - {f.name} ({size_mb:.1f} MB)")
    else:
        print("\nWarning: No output files found in reporting_output/")

except Exception as e:
    print("\n" + "=" * 70)
    print("FAILED")
    print("=" * 70)
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\nDone!")
