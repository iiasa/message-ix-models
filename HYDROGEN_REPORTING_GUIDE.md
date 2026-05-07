# Hydrogen Reporting Guide

## Overview

This guide explains the hydrogen reporting system in `message-ix-models`. The system generates IAMC-formatted reports for:
1. **Hydrogen Production** by technology and fuel source
2. **H2 Fugitive Emissions** across production, distribution, and demand sectors
3. **LH2 Fugitive Emissions** from liquefied hydrogen supply chain

## File Structure

```
message_ix_models/
├── data/hydrogen/reporting/              # Configuration files (YAML)
│   ├── h2_prod.yaml                     # Production base variables
│   ├── h2_prod_aggregates.yaml          # Production aggregations
│   ├── h2_fgt_emi.yaml                  # H2 fugitive emissions base variables
│   ├── h2_fgt_emi_aggregates.yaml       # H2 fugitive emissions aggregations
│   └── lh2_fgt_emi.yaml                 # LH2 fugitive emissions variables
│
└── report/hydrogen/                      # Python code
    ├── __init__.py
    ├── config.py                         # Config class for loading YAML
    ├── h2_reporting.py                   # Main reporting functions
    └── cli.py                            # Command-line interface
```

## Configuration Files

### Base Variable Files (e.g., `h2_prod.yaml`, `h2_fgt_emi.yaml`)

These files define **individual variables** at the most granular level:

```yaml
var: out                           # Which model variable to query (out, in, emi, etc.)
iamc_prefix: Production|           # Prefix for all IAMC variable names
unit: Mt H2/yr                     # Default unit

vars:
  Hydrogen|Gas|SMR w/o CCS:        # IAMC variable name (fragment)
    filter:
      technology: h2_smr           # Model technology
      mode: M1                     # Model mode
      commodity: hydrogen          # Model commodity
      level: secondary             # Model level
    short: prod_h2_smr_wo_ccs     # Short identifier for aggregations
```

**Key Points:**
- Each entry maps model dimensions (technology, mode, commodity, level) to an IAMC variable
- The `short` name is used as a reference in aggregation files
- Multiple dimensions can be lists: `mode: [M1, feedstock, fuel]`

### Aggregation Files (e.g., `h2_prod_aggregates.yaml`, `h2_fgt_emi_aggregates.yaml`)

These files define **how base variables aggregate** into higher-level totals:

```yaml
var: out
iamc_prefix: Production|
unit: Mt H2/yr

level_1:
  Hydrogen|Gas:                    # Aggregate variable name
    short: prod_h2_gas             # Short identifier for this aggregate
    components:                     # List of base variables to sum
      - prod_h2_smr_wo_ccs
      - prod_h2_smr_w_ccs
      - prod_h2_pyro

level_2:
  Hydrogen:                        # Higher-level aggregate
    short: prod_h2
    components:                     # Can reference level_1 aggregates
      - prod_h2_gas
      - prod_h2_coal
      - prod_h2_bio
      - prod_h2_elec
```

**Key Points:**
- Aggregations happen in levels: level_1, level_2, level_3, etc.
- Each level can reference components from previous levels or from base variables
- The `short` names must match between files
- This creates a hierarchy: Base Variables → Level 1 → Level 2 → ...

## Hydrogen Production Reporting

### Technologies Covered

**Gas-based:**
- `h2_smr` - Steam Methane Reforming without CCS
- `h2_smr_ccs` - Steam Methane Reforming with CCS
- `h2_pyro_elec` - Pyrolysis

**Coal-based:**
- `h2_coal` - Coal gasification without CCS
- `h2_coal_ccs` - Coal gasification with CCS

**Biomass-based:**
- `h2_bio` - Biomass gasification without CCS
- `h2_bio_ccs` - Biomass gasification with CCS

**Electrolysis:**
- `h2_elec_alkaline` - Alkaline electrolyzer
- `h2_elec_pem` - PEM (Proton Exchange Membrane) electrolyzer
- `h2_elec_soe` - SOE (Solid Oxide Electrolyzer)

### Aggregation Hierarchy

```
Production|Hydrogen                           (level_2)
├── Production|Hydrogen|Gas                   (level_1)
│   ├── Production|Hydrogen|Gas|SMR w/o CCS   (base)
│   ├── Production|Hydrogen|Gas|SMR w/ CCS    (base)
│   └── Production|Hydrogen|Gas|Pyrolysis     (base)
├── Production|Hydrogen|Coal                  (level_1)
│   ├── Production|Hydrogen|Coal|w/o CCS      (base)
│   └── Production|Hydrogen|Coal|w/ CCS       (base)
├── Production|Hydrogen|Biomass               (level_1)
│   ├── Production|Hydrogen|Biomass|w/o CCS   (base)
│   └── Production|Hydrogen|Biomass|w/ CCS    (base)
└── Production|Hydrogen|Electricity           (level_1)
    ├── Production|Hydrogen|Electricity|Alkaline  (base)
    ├── Production|Hydrogen|Electricity|PEM       (base)
    └── Production|Hydrogen|Electricity|SOE       (base)
```

## H2 Fugitive Emissions Reporting

### Sectors Covered

**Production:**
- Gas (SMR, SMR w/ CCS, Pyrolysis)
- Coal (w/o CCS, w/ CCS)
- Biomass (w/o CCS, w/ CCS)
- Electricity (aggregates all 3 electrolyzer types)

**Distribution:**
- Transmission & Distribution (`h2_t_d`)
- Mixing/blending (`h2_mix`)

**Demand - Industry:**
- Iron and Steel
- Cement
- Aluminium
- Chemicals (HVCs, other petrochemicals)
- Methanol production
- Other industrial uses

**Demand - Other Sectors:**
- Residential and Commercial
- Transportation
- Power generation

### Aggregation Hierarchy

```
Emissions|H2|Fugitive|Energy|Demand                               (level_4)
├── Emissions|H2|Fugitive|Energy|Demand|Industry                  (level_3)
│   ├── Emissions|H2|Fugitive|Energy|Demand|Industry|Iron and Steel     (base)
│   ├── Emissions|H2|Fugitive|Energy|Demand|Industry|Cement             (base)
│   ├── Emissions|H2|Fugitive|Energy|Demand|Industry|Aluminium          (base)
│   ├── Emissions|H2|Fugitive|Energy|Demand|Industry|Chemicals          (base)
│   ├── Emissions|H2|Fugitive|Energy|Demand|Industry|Methanol           (base)
│   └── Emissions|H2|Fugitive|Energy|Demand|Industry|Other              (base)
├── Emissions|H2|Fugitive|Energy|Demand|Residential and Commercial      (base)
├── Emissions|H2|Fugitive|Energy|Demand|Transportation                  (base)
└── Emissions|H2|Fugitive|Energy|Demand|Power                           (base)

Emissions|H2|Fugitive|Energy|Supply|Hydrogen                     (level_3)
├── Emissions|H2|Fugitive|Energy|Supply|Hydrogen|Production      (level_2)
│   ├── Emissions|H2|Fugitive|...|Production|Gas                 (level_1)
│   ├── Emissions|H2|Fugitive|...|Production|Coal                (level_1)
│   ├── Emissions|H2|Fugitive|...|Production|Biomass             (level_1)
│   └── Emissions|H2|Fugitive|...|Production|Electricity         (base)
└── Emissions|H2|Fugitive|Energy|Supply|Hydrogen|Distribution    (base)
```

## How to Use

### 1. Running from Python Script

```python
import ixmp
import message_ix
from message_ix.report import Reporter
import pyam

# Import the reporting function
from message_ix_models.report.hydrogen.h2_reporting import run_h2_reporting

# Load your scenario
mp = ixmp.Platform(name="your-platform")
scenario = message_ix.Scenario(mp, model="your_model", scenario="your_scenario")

# Create reporter
rep = Reporter.from_scenario(scenario)

# Run hydrogen reporting (returns list of IamDataFrames)
dfs = run_h2_reporting(rep, scenario.model, scenario.scenario)

# Concatenate all results
py_df = pyam.concat(dfs)

# Export to Excel
py_df.to_excel("hydrogen_report.xlsx")

# Or view the data
print(py_df.timeseries())
```

### 2. Running the Example Script

Use the provided example script:

```bash
python run_h2_reporting_example.py
```

**Configure the script** by editing these variables:
```python
platform_name = "ixmp-dev"              # Your platform name
model_name = "your_model"               # Your model name
scenario_name = "your_scenario"         # Your scenario name
output_filename = "h2_report.xlsx"      # Output filename
```

### 3. Individual Reporting Functions

You can also run individual reporting functions:

```python
from message_ix_models.report.hydrogen.h2_reporting import (
    run_h2_prod_reporting,      # Production only
    run_h2_fgt_reporting,        # H2 fugitive emissions only
    run_lh2_fgt_reporting,       # LH2 fugitive emissions only
)

# Run only production reporting
prod_df = run_h2_prod_reporting(rep, model_name, scenario_name)
```

## Verification and Quality Checks

### 1. Check for Double-Counting

Verify that base variables are not duplicated across filters:
- Each technology-mode-commodity-level combination should appear only once in base files
- Aggregates should not include the same base variable twice

### 2. Check for Complete Coverage

Verify all relevant technologies are included:
- Check model technology list against YAML files
- Ensure all production technologies have corresponding emission entries

### 3. Validate Aggregations

Check that aggregation hierarchies are correct:
- All `components` in aggregates files must reference existing `short` names
- No circular references between levels
- Higher levels should only reference lower levels or base variables

### 4. Test Output

```python
# Check for missing data
assert not py_df.empty, "No data was reported!"

# Check variable names
print("Variables reported:")
print(py_df.variable)

# Check for negative values (usually indicates errors)
negative = py_df.filter(variable="*", year="*").timeseries()
negative = negative[negative < 0]
if not negative.empty:
    print("WARNING: Negative values found:")
    print(negative)

# Check aggregates sum correctly
prod_gas = py_df.filter(variable="Production|Hydrogen|Gas")
prod_gas_components = py_df.filter(variable="Production|Hydrogen|Gas|*", level=0)
# Sum should match (within tolerance)
```

## Troubleshooting

### Error: "FileNotFoundError: h2_prod.yaml not found"

**Cause:** YAML file is not in the expected location.

**Solution:** Ensure files are in `message_ix_models/data/hydrogen/reporting/`

### Error: "KeyError in use_aggregates_dict"

**Cause:** A `components` entry references a `short` name that doesn't exist.

**Solution:** 
1. Check all `short` names in base YAML file
2. Ensure aggregates only reference existing short names
3. Check for typos in short names

### Error: "Empty DataFrame returned"

**Possible causes:**
1. **Technologies don't exist in model:** Check that technologies in filters actually exist
2. **Wrong mode/level/commodity:** Verify filter values match model structure
3. **No data for time period:** Check that scenario has data for queried years

**Solution:**
```python
# Debug by checking what's in the scenario
scenario.par("output", filters={"technology": "h2_smr"})
```

### Warning: "Aggregates don't sum correctly"

**Cause:** Missing components in aggregation or double-counting.

**Solution:**
1. List all base variables for a category
2. Ensure each is included exactly once in aggregates
3. Check for overlapping filters in base file

## Maintenance

### Adding a New Production Technology

1. **Add to `h2_prod.yaml`:**
```yaml
  Hydrogen|NewFuel|NewTech:
    filter: { technology: h2_newfuel_newtech, mode: M1, commodity: hydrogen, level: secondary }
    short: prod_h2_newfuel_newtech
```

2. **Update `h2_prod_aggregates.yaml`:**
Add the new short name to appropriate aggregate components.

3. **Test:** Run reporting and verify new variable appears.

### Adding a New Demand Sector

1. **Add to `h2_fgt_emi.yaml`:**
```yaml
  Energy|Demand|NewSector:
    filter: { technology: [tech1, tech2], mode: M1 }
    short: emi_h2_fgt_dem_newsector
```

2. **Update `h2_fgt_emi_aggregates.yaml`:**
Add to appropriate demand aggregate (usually `Energy|Demand` or `Energy|Demand|Industry`).

## Best Practices

1. **Consistent Naming:** Follow IAMC conventions with `|` separators
2. **Clear Short Names:** Use descriptive short names that indicate content
3. **Document Special Cases:** Add comments for unusual filters or aggregations
4. **Test Incrementally:** Add one variable/aggregate at a time and test
5. **Version Control:** Track changes to YAML files carefully
6. **Validate Sums:** Always check that aggregates equal sum of components

## See Also

- Material reporting documentation (similar structure)
- IAMC variable naming conventions
- MESSAGE-ix Reporter documentation

