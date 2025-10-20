"""Example usage of the hydrogen technology module.

This script demonstrates how to add hydrogen technologies to a MESSAGE-IX scenario.
"""


def example_basic_usage():
    """Basic example of adding hydrogen technologies to a scenario."""
    from message_ix import Scenario
    from message_ix_models import Context
    from message_ix_models.model.hydrogen import build

    # Setup context
    context = Context.get_instance()
    context.model.regions = "R12"

    # Get platform
    mp = context.get_platform()

    # Get or create base scenario
    # Replace with your actual model and scenario names
    base_scenario = Scenario(
        mp,
        model="MESSAGEix-GLOBIOM",  # Your base model
        scenario="baseline",  # Your base scenario
    )

    # Clone scenario for hydrogen build
    scenario = base_scenario.clone(
        model="MESSAGEix-Hydrogen",
        scenario="baseline_with_hydrogen",
        keep_solution=False,
    )

    print("Building hydrogen technologies...")

    # Add hydrogen technologies to the scenario
    scenario = build(context, scenario)

    # Commit the changes
    scenario.commit("Added hydrogen production and utilization technologies")

    print(f"✓ Hydrogen technologies added to scenario: {scenario.scenario}")

    # Validate that technologies were added
    validate_build(scenario)

    return scenario


def example_component_usage():
    """Example showing how to use individual components."""
    from message_ix import Scenario
    from message_ix_models import Context
    from message_ix_models.model.hydrogen.build import make_spec, add_data
    from message_ix_models.model.hydrogen.data_hydrogen import gen_data_hydrogen
    from message_ix_models.model.build import apply_spec

    context = Context.get_instance()
    context.model.regions = "R12"
    mp = context.get_platform()

    # Get scenario
    scenario = Scenario(mp, model="MESSAGEix-GLOBIOM", scenario="baseline")
    scenario = scenario.clone(
        model="MESSAGEix-Hydrogen-Components",
        scenario="test_components",
        keep_solution=False,
    )

    # Step 1: Create specification
    print("Step 1: Creating specification...")
    spec = make_spec(regions="R12")
    print(f"  - Technologies to add: {len(spec.add.set['technology'])}")
    print(f"  - Technologies required: {len(spec.require.set['technology'])}")

    # Step 2: Apply specification (adds/removes sets)
    print("Step 2: Applying specification...")
    apply_spec(scenario, spec, data=None, fast=True)

    # Step 3: Generate data
    print("Step 3: Generating parameter data...")
    data_dict = gen_data_hydrogen(scenario, dry_run=True)
    print(f"  - Generated {len(data_dict)} parameter types")
    for param, df in data_dict.items():
        print(f"    * {param}: {len(df)} rows")

    # Step 4: Add data to scenario
    print("Step 4: Adding data to scenario...")
    add_data(scenario, dry_run=False)

    scenario.commit("Added hydrogen technologies (component-by-component)")

    print("✓ Complete!")
    validate_build(scenario)

    return scenario


def validate_build(scenario):
    """Validate that hydrogen technologies were properly added."""
    print("\nValidating build...")

    # Check sets
    technologies = scenario.set("technology")
    commodities = scenario.set("commodity")
    emissions = scenario.set("emission")

    # Technologies we expect
    expected_tech = [
        "h2_elec_alk",
        "h2_elec_pem",
        "h2_elec_soe",
        "h2_pyro_elec",
        "h2_ct",
        "carbon_black_stor",
        "carbon_black_trans",
    ]

    for tech in expected_tech:
        if tech in technologies:
            print(f"  ✓ Technology '{tech}' added")
        else:
            print(f"  ✗ Technology '{tech}' MISSING")

    # Check commodities
    if "carbon_black" in commodities:
        print(f"  ✓ Commodity 'carbon_black' added")
    else:
        print(f"  ✗ Commodity 'carbon_black' MISSING")

    # Check emissions
    if "H2_leak" in emissions:
        print(f"  ✓ Emission 'H2_leak' added")
    else:
        print(f"  ✗ Emission 'H2_leak' MISSING")

    # Check parameters for one technology
    input_data = scenario.par("input", filters={"technology": "h2_elec_alk"})
    output_data = scenario.par("output", filters={"technology": "h2_elec_alk"})

    if not input_data.empty:
        print(f"  ✓ h2_elec_alk has {len(input_data)} input flows")
        print(f"    Commodities: {', '.join(input_data.commodity.unique())}")
    else:
        print(f"  ✗ h2_elec_alk has no input flows")

    if not output_data.empty:
        print(f"  ✓ h2_elec_alk has {len(output_data)} output flows")
        print(f"    Commodities: {', '.join(output_data.commodity.unique())}")
    else:
        print(f"  ✗ h2_elec_alk has no output flows")

    # Check historical data
    hist_act = scenario.par(
        "historical_activity", filters={"technology": "h2_elec_alk"}
    )
    if not hist_act.empty:
        print(f"  ✓ h2_elec_alk has historical activity ({len(hist_act)} entries)")
        print(f"    Regions: {', '.join(hist_act.node_loc.unique())}")
        print(f"    Years: {', '.join(map(str, sorted(hist_act.year_act.unique())))}")
    else:
        print(f"  ⊘ h2_elec_alk has no historical activity (optional)")

    print("\n✓ Validation complete!")


if __name__ == "__main__":
    print("=" * 70)
    print("Hydrogen Technology Module - Usage Examples")
    print("=" * 70)
    print()
    print("This script demonstrates how to use the hydrogen module.")
    print("You will need to:")
    print("  1. Have a MESSAGE-IX platform configured")
    print("  2. Have a base scenario to build upon")
    print("  3. Update the model/scenario names in this script")
    print()
    print("=" * 70)

    # Uncomment the example you want to run:

    # Example 1: Basic usage (recommended)
    # scenario = example_basic_usage()

    # Example 2: Component-by-component (advanced)
    # scenario = example_component_usage()

    print()
    print("To run these examples, uncomment the example_*() calls above")
    print("and update the model/scenario names to match your setup.")
