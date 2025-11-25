#!/usr/bin/env python
"""Check what years are in ref_activity parameter."""

import ixmp
from message_ix import Scenario
from message_ix.report import Reporter

mp = ixmp.Platform("ixmp-dev")
scenario = Scenario(mp, "hyway_SSP_SSP2_v6.4", "SSP2 - Low Emissions_gains")

print("=" * 70)
print("CHECKING ref_activity PARAMETER")
print("=" * 70)

# Check the scenario's year set
years = scenario.set("year")
print(f"\nModel years in scenario: {sorted(years)}")

# Try to get ref_activity parameter
try:
    ref_act = scenario.par("ref_activity")
    if ref_act.empty:
        print("\n✗ ref_activity parameter exists but is EMPTY")
    else:
        print(f"\n✓ ref_activity parameter has {len(ref_act)} rows")
        print(f"\nYears in ref_activity: {sorted(ref_act.year_act.unique())}")
        print(
            f"\nTechnologies in ref_activity: {sorted(ref_act.technology.unique())[:20]}"
        )

        # Check for hydrogen technologies
        h2_techs = ref_act[ref_act.technology.str.contains("h2", case=False, na=False)]
        if not h2_techs.empty:
            print(f"\n✓ Found {len(h2_techs)} rows for hydrogen technologies")
            print(f"H2 technologies: {sorted(h2_techs.technology.unique())}")
        else:
            print("\n✗ No hydrogen technologies in ref_activity")

except Exception as e:
    print(f"\n✗ Error accessing ref_activity: {e}")

print("\n" + "=" * 70)
print("CONCLUSION")
print("=" * 70)
print(
    """
If ref_activity only contains MODEL years (>= 2020), then:
  - Historical reporting will duplicate model data
  - You don't need historical reporting YAML files
  - The regular reporting is sufficient

If ref_activity contains PRE-MODEL years (< first model year), then:
  - Historical reporting makes sense
  - We need to filter to only report those years
"""
)
