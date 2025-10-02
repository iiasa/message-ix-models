# Hydrogen Fugitive Emissions Reporting

This document describes the new reporting module for hydrogen (H2) and liquefied hydrogen (LH2) fugitive emissions.

- [Hydrogen Fugitive Emissions Reporting](#hydrogen-fugitive-emissions-reporting)
  - [Introduction](#introduction)
  - [Module Structure](#module-structure)
    - [Data Configuration](#data-configuration)
    - [Reporting Code](#reporting-code)
  - [Configuration Files](#configuration-files)
    - [`h2_fgt_emi.yaml`](#h2_fgt_emiyaml)
    - [`lh2_fgt_emi.yaml`](#lh2_fgt_emiyaml)
  - [Reporting Logic (`h2_reporting.py`)](#reporting-logic-h2_reportingpy)
  - [Command-Line Interface (CLI)](#command-line-interface-cli)
  - [Example Workflow](#example-workflow)


## Introduction

A new, modular reporting system has been implemented to account for fugitive emissions of hydrogen. This system is designed to be extensible and is separate from the existing material and legacy reporting frameworks. It provides detailed reporting for H2 and LH2 fugitive emissions from production, distribution, and usage across different sectors.

## Module Structure

The new reporting module is organized into two main parts: data configuration and Python code.

### Data Configuration

The configuration for the hydrogen reporting is located in:
`message_ix_models/data/hydrogen/reporting/`

This directory contains YAML files that define the variables, technologies, and IAMC mappings for the reporting.

-   `h2_fgt_emi.yaml`: Configures reporting for gaseous hydrogen fugitive emissions.
-   `lh2_fgt_emi.yaml`: Configures reporting for liquefied hydrogen fugitive emissions.

### Reporting Code

The Python code for the hydrogen reporting is located in:
`message_ix_models/report/hydrogen/`

This directory contains:
-   `__init__.py`: Makes the directory a Python package.
-   `config.py`: A configuration class that loads and parses the YAML files from the data directory.
-   `h2_reporting.py`: The main reporting script containing the logic to process the data.
-   `cli.py`: Defines the command-line interface for the hydrogen reporting.

## Configuration Files

The YAML files define the structure of the reporting. They specify the source variable from the `message_ix` scenario (`var`), the prefix for the IAMC variable name (`iamc_prefix`), the unit, and a list of variables to be reported (`vars`).

### `h2_fgt_emi.yaml`

This file configures the reporting for gaseous hydrogen fugitive emissions. It covers production, distribution, and end-use sectors.

**Snippet:**
```yaml
# Fugitive hydrogen emissions

var:
  emi

iamc_prefix:
  Emissions|H2|Fugitive|

unit: Mt H2/yr

vars:
  Energy|Supply|Hydrogen|Production:
    filter:
      { technology: [h2_smr, h2_smr_ccs, h2_coal, h2_coal_ccs, h2_bio, h2_bio_ccs, h2_elec, h2_elec_ccs] }
    short:
      emi_h2_fgt_en_sup_h2_prod

  # ... other variables for distribution and demand ...
```

### `lh2_fgt_emi.yaml`

This file configures the reporting for fugitive emissions from liquefied hydrogen technologies.

**Snippet:**
```yaml
# Fugitive liquefied hydrogen emissions

var:
  emi

iamc_prefix:
  Emissions|LH2|Fugitive|

unit: Mt H2/yr

vars:
  Energy|Supply|LH2:
    filter:
      { technology: [h2_liq, lh2_bal, lh2_exp, lh2_imp, lh2_regas, lh2_t_d] }
    short:
      emi_lh2_fgt_en_sup_lh2
```

## Reporting Logic (`h2_reporting.py`)

The `h2_reporting.py` script contains the core logic for the hydrogen reporting. It defines functions to read the configuration, query the `message_ix.Reporter`, and format the data into a `pyam.IamDataFrame`.

The main functions are:
-   `run_h2_fgt_reporting()`: Processes `h2_fgt_emi.yaml`.
-   `run_lh2_fgt_reporting()`: Processes `lh2_fgt_emi.yaml`.
-   `run_h2_reporting()`: A wrapper that calls both of the above functions and combines the results.

These functions are designed to be called from the CLI, but can also be used programmatically.

## Command-Line Interface (CLI)

A new CLI command has been added to run the hydrogen reporting.

**Command:**
```bash
mix-models --url <scenario_url> hydrogen report
```

**Description:**
This command runs the hydrogen-specific reporting for the scenario specified by the `--url` option. It will generate the fugitive emission variables for H2 and LH2 as defined in the YAML configuration files.

**Expected Output:**
The command will print a table to the console containing the time series data for the reported variables in a long format.

Example output:
```
Successfully generated H2 reporting dataframe:
                  model      scenario   region  ... unit      year     value
0     MESSAGEix-Materials  test_scenario  R11_AFR  ... Mt H2/yr  2020  0.000000
1     MESSAGEix-Materials  test_scenario  R11_AFR  ... Mt H2/yr  2030  0.001234
...
```

## Example Workflow

Here is a step-by-step example of how to use the new hydrogen reporting.

1.  **Run a scenario:**
    First, you need a solved `MESSAGEix-Materials` scenario that contains hydrogen technologies and emissions.

2.  **Run the hydrogen reporting:**
    Use the `mix-models` CLI to run the hydrogen reporting on your scenario. Replace `<scenario_url>` with the URL of your scenario.

    ```bash
    mix-models --url "ixmp://your-platform/MESSAGEix-Materials/your-scenario#1" hydrogen report
    ```

3.  **Analyze the output:**
    The command will print the time series data for the hydrogen fugitive emissions to the console. You can redirect this output to a file for further analysis.

    ```bash
    mix-models --url "ixmp://your-platform/MESSAGEix-Materials/your-scenario#1" hydrogen report > h2_fugitive_emissions.csv
    ```

This workflow allows for a quick and easy way to generate and analyze hydrogen fugitive emissions from your scenarios.

## Programmatic Usage (Python Script)

It is also possible to run the reporting directly from a Python script without using the CLI. This provides more flexibility to integrate the reporting into other workflows.

Here is an example of how you can do it:

```python
import ixmp
import message_ix
from message_ix.report import Reporter
import pyam

from message_ix_models.report.hydrogen.h2_reporting import run_h2_reporting

# 1. Connect to the ixmp platform
# Replace 'your_platform_name' with the name of your platform
mp = ixmp.Platform(name="your_platform_name")

# 2. Get the scenario object
# Replace with your model and scenario names
model_name = "MESSAGEix-Materials"
scenario_name = "your_scenario"
scenario = message_ix.Scenario(mp, model=model_name, scenario=scenario_name)

# 3. Create a Reporter object from the scenario
rep = Reporter.from_scenario(scenario)

# 4. Run the hydrogen reporting
# This will return a list of pyam.IamDataFrame objects
dfs = run_h2_reporting(rep, scenario.model, scenario.scenario)

# 5. Concatenate the results into a single IamDataFrame
py_df = pyam.concat(dfs)

# Now you can work with the results
print("Successfully generated H2 reporting dataframe:")
print(py_df.timeseries())

# You can also save the results to a file
# py_df.to_excel("h2_fugitive_emissions.xlsx")
```

This script shows the main steps:
1.  Connect to your `ixmp` database platform.
2.  Load the specific scenario you want to report.
3.  Create a `Reporter` for that scenario.
4.  Call the `run_h2_reporting` function to generate the data.
5.  Process the results as needed.