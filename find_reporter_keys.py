#!/usr/bin/env python
"""Find what keys are actually available in the reporter."""

import ixmp
from message_ix import Scenario
from message_ix.report import Reporter

mp = ixmp.Platform("ixmp-dev")
scenario = Scenario(mp, "hyway_SSP_SSP2_v6.4", "SSP2 - Low Emissions_gains")
rep = Reporter.from_scenario(scenario)

print("=" * 70)
print("SEARCHING FOR RELEVANT KEYS IN REPORTER")
print("=" * 70)
print(f"\nTotal keys: {len(rep.keys())}")
print()

# Search for keys related to output, activity, emission
search_terms = ["out", "act", "emi", "hist", "ref"]

for term in search_terms:
    matching = [str(k) for k in rep.keys() if term.lower() in str(k).lower()]
    if matching:
        print(f"\nKeys containing '{term}':")
        for key in sorted(matching)[:15]:  # Show first 15
            print(f"  - {key}")
        if len(matching) > 15:
            print(f"  ... and {len(matching) - 15} more")

# Check for specific MESSAGE parameters
print("\n" + "=" * 70)
print("CHECKING MESSAGE PARAMETERS")
print("=" * 70)

params_to_check = ["output", "ACT", "ref_activity", "emission_factor", "CAP"]
for param in params_to_check:
    exists = param in rep.keys()
    print(f"{param:20} {'✓ EXISTS' if exists else '✗ MISSING'}")

