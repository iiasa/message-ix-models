"""Validation script for hydrogen technology module.

This script provides functions to validate that hydrogen technologies
are properly configured and can be added to a MESSAGE-IX scenario.
"""

import logging

import pandas as pd

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def validate_data_files():
    """Validate that all required data files exist and have correct structure."""
    from message_ix_models.util import package_data_path

    log.info("Validating data files...")

    files = {
        "techno_economic": "hydrogen_techno_economic.csv",
        "timeseries": "timeseries_hydrogen.csv",
        "relations": "relations_hydrogen.csv",
        "historical": "historical_data.csv",
        "set_config": "set.yaml",
    }

    required_columns = {
        "techno_economic": [
            "parameter",
            "technology",
            "value",
            "unit",
            "availability",
        ],
        "timeseries": ["technology", "parameter", "year", "value", "unit"],
        "relations": ["relation", "parameter", "Region", "value"],
        "historical": ["parameter", "node_loc", "technology", "value", "unit"],
    }

    errors = []
    for key, filename in files.items():
        if key == "set_config":
            # Just check if it exists
            try:
                path = package_data_path("hydrogen", filename)
                log.info(f"✓ Found {filename}")
            except Exception as e:
                errors.append(f"✗ Missing {filename}: {e}")
        else:
            try:
                path = package_data_path("hydrogen", filename)
                df = pd.read_csv(path, comment="#")

                # Check required columns
                missing_cols = set(required_columns[key]) - set(df.columns)
                if missing_cols:
                    errors.append(f"✗ {filename} missing columns: {missing_cols}")
                else:
                    log.info(f"✓ {filename} has correct structure ({len(df)} rows)")

            except FileNotFoundError:
                errors.append(f"✗ Missing {filename}")
            except Exception as e:
                errors.append(f"✗ Error reading {filename}: {e}")

    if errors:
        log.error("Validation failed:")
        for error in errors:
            log.error(f"  {error}")
        return False
    else:
        log.info("All data files validated successfully!")
        return True


def validate_set_configuration():
    """Validate set.yaml configuration."""
    from message_ix_models.model.hydrogen.utils import read_config

    log.info("Validating set configuration...")

    try:
        context = read_config()
        # The configuration is nested under "hydrogen" key
        if "hydrogen" not in context:
            log.error("✗ 'hydrogen' key not found in context")
            return False

        # The YAML structure is: hydrogen: {node: {...}, technology: {...}, ...}
        # So context["hydrogen"] returns {'hydrogen': {...}}
        # We need to unwrap one more level
        hydrogen_outer = context["hydrogen"]
        if "hydrogen" in hydrogen_outer:
            hydrogen_config = hydrogen_outer["hydrogen"]
        else:
            hydrogen_config = hydrogen_outer

        # In set.yaml, the structure is hydrogen: { technology: {...}, commodity: {...}, etc.}
        # So hydrogen_config itself should have these keys
        if not isinstance(hydrogen_config, dict):
            log.error(f"✗ hydrogen_config is not a dict: {type(hydrogen_config)}")
            return False

        # Check required sections
        required_sections = ["technology", "commodity", "level", "mode"]
        missing = [s for s in required_sections if s not in hydrogen_config]

        if missing:
            log.error(f"✗ Missing sections in set.yaml: {missing}")
            log.info(f"  Available sections: {list(hydrogen_config.keys())}")
            return False

        # Check technologies
        tech_add = [
            t.id if hasattr(t, "id") else t
            for t in hydrogen_config["technology"].get("add", [])
        ]
        tech_require = [
            t.id if hasattr(t, "id") else t
            for t in hydrogen_config["technology"].get("require", [])
        ]

        log.info(f"✓ Technologies to add: {len(tech_add)}")
        log.info(f"  {', '.join(tech_add)}")
        log.info(f"✓ Technologies required: {len(tech_require)}")

        # Check commodities
        comm_add = hydrogen_config["commodity"].get("add", [])
        comm_require = hydrogen_config["commodity"].get("require", [])

        log.info(f"✓ Commodities to add: {len(comm_add)}")
        log.info(f"✓ Commodities required: {len(comm_require)}")

        return True

    except Exception as e:
        log.error(f"✗ Error validating set configuration: {e}")
        import traceback

        traceback.print_exc()
        return False


def validate_spec_creation():
    """Validate that spec can be created from set.yaml."""
    from message_ix_models.model.hydrogen.build import make_spec

    log.info("Validating spec creation...")

    try:
        spec = make_spec("R12")

        # Check that spec has required sections
        assert hasattr(spec, "add"), "Spec missing 'add' section"
        assert hasattr(spec, "require"), "Spec missing 'require' section"
        assert hasattr(spec, "remove"), "Spec missing 'remove' section"

        # Check that technologies are in spec
        tech_add = spec.add.set.get("technology", [])
        tech_require = spec.require.set.get("technology", [])

        log.info(f"✓ Spec created successfully")
        log.info(f"  Technologies to add: {len(tech_add)}")
        log.info(f"  Technologies required: {len(tech_require)}")

        return True

    except Exception as e:
        log.error(f"✗ Error creating spec: {e}")
        import traceback

        traceback.print_exc()
        return False


def validate_data_generation(scenario=None):
    """Validate that data can be generated from CSV files.

    Parameters
    ----------
    scenario : message_ix.Scenario, optional
        Scenario to use for validation. If None, will create a minimal mock.
    """
    log.info("Validating data generation...")

    if scenario is None:
        log.warning("No scenario provided - skipping data generation validation")
        log.info(
            "To fully validate, provide a scenario: "
            "validate_data_generation(scenario)"
        )
        return None

    try:
        from message_ix_models.model.hydrogen.data_hydrogen import (
            gen_data_hydrogen,
            read_data_hydrogen,
        )

        # Test reading data files
        data, data_rel, data_ts = read_data_hydrogen(scenario)
        log.info(f"✓ Read techno-economic data: {len(data)} rows")
        log.info(f"✓ Read relations data: {len(data_rel)} rows")
        log.info(f"✓ Read timeseries data: {len(data_ts)} rows")

        # Test generating parameter data
        param_data = gen_data_hydrogen(scenario, dry_run=True)
        log.info(f"✓ Generated {len(param_data)} parameter types:")

        for param, df in param_data.items():
            log.info(f"  - {param}: {len(df)} rows")

        return True

    except Exception as e:
        log.error(f"✗ Error generating data: {e}")
        import traceback

        traceback.print_exc()
        return False


def run_all_validations(scenario=None):
    """Run all validation checks.

    Parameters
    ----------
    scenario : message_ix.Scenario, optional
        Scenario to use for data generation validation

    Returns
    -------
    bool
        True if all validations pass, False otherwise
    """
    log.info("=" * 70)
    log.info("Running Hydrogen Module Validation")
    log.info("=" * 70)

    results = {}

    results["data_files"] = validate_data_files()
    log.info("")

    results["set_config"] = validate_set_configuration()
    log.info("")

    results["spec_creation"] = validate_spec_creation()
    log.info("")

    results["data_generation"] = validate_data_generation(scenario)
    log.info("")

    log.info("=" * 70)
    log.info("Validation Summary")
    log.info("=" * 70)

    for check, result in results.items():
        if result is None:
            status = "⊘ SKIPPED"
        elif result:
            status = "✓ PASSED"
        else:
            status = "✗ FAILED"
        log.info(f"{status}: {check}")

    log.info("=" * 70)

    # Return True only if all non-skipped checks passed
    all_passed = all(r in [True, None] for r in results.values())

    if all_passed:
        log.info("✓ All validations passed!")
    else:
        log.error("✗ Some validations failed")

    return all_passed


if __name__ == "__main__":
    # Run validation without scenario (basic checks only)
    success = run_all_validations()

    if success:
        print("\n" + "=" * 70)
        print("Next steps:")
        print("  1. Review the generated data files in data/hydrogen/")
        print("  2. Adjust parameters as needed for your use case")
        print("  3. To fully validate with a scenario:")
        print("     from message_ix import Scenario")
        print("     scenario = Scenario(mp, model='...', scenario='...')")
        print("     validate_data_generation(scenario)")
        print("  4. Use build(context, scenario) to add to a scenario")
        print("=" * 70)
