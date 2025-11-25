import logging
import ixmp
import message_ix
from message_ix import Scenario
from message_ix.report import Reporter
from message_ix_models.report.hydrogen.h2_reporting import run_h2_reporting

# Enable logging to see what's happening
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

mp = ixmp.Platform("ixmp-dev")

model = "hyway_SSP_SSP2_v6.4"
scenario = "SSP2 - Low Emissions_gains"

scenario = Scenario(mp, model, scenario)

rep = Reporter.from_scenario(scenario)

# Check what keys exist
print("\n" + "=" * 70)
print("CHECKING REPORTER KEYS AND SCENARIO INFO")
print("=" * 70)

# Check model years
cat_year = list(scenario.set("cat_year", filters={"type_year": "firstmodelyear"}))
if cat_year:
    print(f"First model year: {cat_year[0]}")
all_years = sorted(scenario.set("year"))
print(f"All years in scenario: {all_years}")

# Check reporter keys
has_output = any("output" == str(k).split(":")[0] for k in rep.keys())
has_ref_activity = any("ref_activity" == str(k).split(":")[0] for k in rep.keys())
has_historical_activity = any(
    "historical_activity" == str(k).split(":")[0] for k in rep.keys()
)
has_emission_factor = any("emission_factor" == str(k).split(":")[0] for k in rep.keys())

print(f"\nReporter keys:")
print(f"  output: {has_output}")
print(f"  ref_activity: {has_ref_activity}")
print(f"  historical_activity: {has_historical_activity}")
print(f"  emission_factor: {has_emission_factor}")
print("=" * 70 + "\n")

# Test creating keys manually (commented out - done automatically now)
# rep.add("out_hist", "mul", "output", "ref_activity")
# rep.add("emi_hist", "mul", "emission_factor", "ref_activity")

# df_out_hist = rep.get("out_hist:nl-t-ya-m")
# df_emi_hist = rep.get("emi_hist:nl-t-ya-m-e")

print("Running H2 reporting...")
try:
    h2_df = run_h2_reporting(rep, model, scenario.scenario)
    print(f"\n✓ Reporting complete! Got {len(h2_df)} data points")

    # Convert to timeseries
    h2_df_ts = h2_df.timeseries().reset_index()
    print(f"Timeseries shape: {h2_df_ts.shape}")

    # Check for historical data
    hist_vars = h2_df_ts[h2_df_ts.variable.str.contains("hist", case=False, na=False)]
    if not hist_vars.empty:
        print(f"\n✓ Found {len(hist_vars)} historical data points")
        print(f"Historical variables: {hist_vars.variable.unique()}")
    else:
        print("\n✗ No historical data found")

    # Show a few variables
    print("\nVariables found:")
    for var in sorted(h2_df_ts.variable.unique())[:10]:
        print(f"  - {var}")

except ValueError as e:
    if "Duplicate rows" in str(e):
        print("\n✗ ERROR: Duplicate rows detected!")
        print("This likely means the same variable is being reported multiple times.")
        print("Possible causes:")
        print("  1. Both model and historical reporting returning data for same years")
        print("  2. Aggregate and leaf variables both included")
        print(f"\nError details: {str(e)[:500]}")
    else:
        raise
