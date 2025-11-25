#!/usr/bin/env python
"""
Check if historical data is available for hydrogen reporting.

This script helps debug why historical hydrogen reporting might not be working.
"""

from message_ix.report import Reporter


def check_historical_data_availability(scenario):
    """Check if the necessary keys for historical reporting exist.

    Parameters
    ----------
    scenario : message_ix.Scenario
        The scenario to check
    """
    print("=" * 70)
    print("HISTORICAL DATA AVAILABILITY CHECK")
    print("=" * 70)
    print()

    # Create reporter
    print("Creating reporter from scenario...")
    rep = Reporter.from_scenario(scenario)
    print(f"✓ Reporter created with {len(rep.keys())} keys")
    print()

    # Check for required keys
    print("Checking for required keys:")
    print("-" * 70)

    required_keys = {
        "output": "Base output data (for out_hist)",
        "ref_activity": "Historical activity data (REQUIRED for all historical reporting)",
        "emission_factor": "Emission factors (for emi_hist)",
        "ACT": "Model activity data (for comparison)",
    }

    available = {}
    for key, description in required_keys.items():
        exists = key in rep.keys()
        available[key] = exists
        status = "✓ EXISTS" if exists else "✗ MISSING"
        print(f"{status:12} {key:20} - {description}")

    print()
    print("=" * 70)
    print("DIAGNOSIS")
    print("=" * 70)

    if available.get("ref_activity"):
        print("✓ ref_activity EXISTS - Historical reporting should work!")
        print()
        print("If you're still not seeing historical data:")
        print("  1. Check that ref_activity has data for hydrogen technologies")
        print("  2. Run reporting with logging level INFO to see detailed messages")
        print("  3. Check the warning messages in the output")
    else:
        print("✗ ref_activity MISSING - Historical reporting will return empty data")
        print()
        print("To enable historical reporting:")
        print("  1. Add ref_activity parameter to your scenario with historical data")
        print("  2. Populate it with activity values for historical years")
        print("  3. Make sure historical years are in the year set")
        print()
        print("Example code to add ref_activity:")
        print("  ```python")
        print("  # Add historical activity data")
        print("  scenario.add_par('ref_activity', {")
        print("      'node_loc': 'R12_GLB',")
        print("      'technology': 'h2_smr',")
        print("      'year_act': 2020,")
        print("      'mode': 'M1',")
        print("      'value': 10.0,  # Historical activity value")
        print("      'unit': 'GWa'")
        print("  })")
        print("  ```")

    print()
    print("=" * 70)

    # Check if there are any keys with "hist" in them
    hist_keys = [k for k in rep.keys() if "hist" in str(k).lower()]
    if hist_keys:
        print(f"\nFound {len(hist_keys)} keys with 'hist' in name:")
        for k in hist_keys[:10]:  # Show first 10
            print(f"  - {k}")
        if len(hist_keys) > 10:
            print(f"  ... and {len(hist_keys) - 10} more")

    return available


if __name__ == "__main__":
    print("This script requires a scenario object.")
    print("Run it from your notebook/script like this:")
    print()
    print("  from check_historical_data import check_historical_data_availability")
    print("  check_historical_data_availability(your_scenario)")
