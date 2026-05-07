# Hydrogen Technology Module for MESSAGE-IX

This module provides hydrogen production, storage, and utilization technologies for MESSAGE-IX scenarios.

## Overview

The hydrogen module adds the following technologies to MESSAGE-IX scenarios:

### Production Technologies
- **h2_elec_alk**: Alkaline electrolyzer (65% efficiency)
- **h2_elec_pem**: PEM electrolyzer (70% efficiency)
- **h2_elec_soe**: Solid oxide electrolyzer (82% efficiency, available from 2025)
- **h2_pyro_elec**: Pyrolysis electrolyzer (produces hydrogen and carbon black)

### Utilization Technologies
- **h2_ct**: Hydrogen combined cycle for power generation (60% efficiency)

### Carbon Black Handling
- **carbon_black_stor**: Carbon black storage
- **carbon_black_trans**: Carbon black transport

## Data Files

The module uses CSV files in `message_ix_models/data/hydrogen/`:

1. **hydrogen_techno_economic.csv**: Static techno-economic parameters
   - Technical lifetime, capacity factors, construction time
   - Input/output coefficients
   - Emission factors

2. **timeseries_hydrogen.csv**: Time-varying parameters
   - Variable costs over time
   - Efficiency improvements

3. **relations_hydrogen.csv**: Constraints and relations
   - Growth constraints
   - Activity bounds
   - Regional capacity limits

4. **historical_data.csv**: Historical calibration
   - Historical activity (2015, 2020)
   - Historical new capacity
   - Activity bounds for base years

5. **set.yaml**: Set configuration
   - Technologies to add/require/remove
   - Commodities, levels, modes
   - Emissions

## Usage

### Basic Usage from Another Module

```python
from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.model.hydrogen import build

# Set up context
context = Context.get_instance()
context.model.regions = "R12"

# Get base scenario
mp = context.get_platform()
base_scenario = Scenario(mp, model="MESSAGEix-GLOBIOM", scenario="baseline")

# Clone scenario
scenario = base_scenario.clone(
    model="MESSAGEix-Hydrogen",
    scenario="baseline_with_hydrogen",
    keep_solution=False
)

# Build hydrogen technologies onto scenario
scenario = build(context, scenario)

# Commit changes
scenario.commit("Added hydrogen technologies")

# Solve scenario
scenario.solve()
```

### Using Individual Components

```python
from message_ix_models.model.hydrogen.data_hydrogen import gen_data_hydrogen
from message_ix_models.model.hydrogen.build import make_spec, add_data

# Generate data only (without applying to scenario)
data_dict = gen_data_hydrogen(scenario)

# Create specification
spec = make_spec(regions="R12")

# Add data to scenario
add_data(scenario, dry_run=False)
```

## Data Structure

### Techno-Economic Parameters

Parameters use the format `parameter|commodity|level|mode` for input/output:

```csv
parameter,technology,mode,commodity,level,value,unit,availability
input,h2_elec_alk,M1,electr,secondary,1.538,GWa/Mt,2020
input,h2_elec_alk,M1,freshwater_supply,water_supply,0.018,Mt/Mt,2020
output,h2_elec_alk,M1,hydrogen,secondary,1.0,Mt,2020
```

### Time-Series Parameters

```csv
technology,parameter,region,mode,year,value,unit
h2_elec_alk,var_cost,default,M1,2020,10.0,USD/Mt
h2_elec_alk,var_cost,default,M1,2030,8.0,USD/Mt
h2_elec_alk,var_cost,default,M1,2050,5.0,USD/Mt
```

### Relations/Constraints

```csv
relation,parameter,technology,Region,value,Units
h2_elec_growth_2020,relation_activity,h2_elec_alk,R12_CHN,1.0,-
h2_elec_growth_2020,relation_upper,,R12_CHN,500.0,Mt/yr
```

### Historical Data

```csv
parameter,node_loc,technology,year_act,year_vtg,mode,value,unit
historical_activity,R12_CHN,h2_elec_alk,2015.0,,M1,5.0,Mt/yr
historical_activity,R12_CHN,h2_elec_alk,2020.0,,M1,8.0,Mt/yr
historical_new_capacity,R12_CHN,h2_elec_alk,,2020.0,,2.0,GW
```

## Adding New Technologies

To add a new hydrogen technology:

1. **Update set.yaml**: Add technology to the `add` list under `technology:`

2. **Add techno-economic data**: In `hydrogen_techno_economic.csv`:
   - Add rows for all required parameters (technical_lifetime, capacity_factor, etc.)
   - Add input/output flows with proper commodity|level|mode format
   - Specify availability year

3. **Add time-series data** (optional): In `timeseries_hydrogen.csv`:
   - Add var_cost or other time-varying parameters
   - Use "default" region for parameters that apply to all regions

4. **Add constraints** (optional): In `relations_hydrogen.csv`:
   - Define any growth constraints or bounds
   - Specify regional limits if needed

5. **Add historical data** (optional): In `historical_data.csv`:
   - Add historical_activity for calibration years
   - Add historical_new_capacity for base years
   - Add bounds if needed

## Regional Support

Currently supports MESSAGE-IX-GLOBIOM R12 regions:
- R12_AFR (Africa)
- R12_CHN (China)
- R12_EEU (Eastern Europe)
- R12_FSU (Former Soviet Union)
- R12_LAM (Latin America)
- R12_MEA (Middle East)
- R12_NAM (North America)
- R12_PAO (Pacific OECD)
- R12_PAS (Other Pacific Asia)
- R12_RCPA (Centrally Planned Asia)
- R12_SAS (South Asia)
- R12_WEU (Western Europe)
- R12_GLB (Global - for trade)

## Integration with Cost Tool

The module is designed to integrate with `message_ix_models.tools.costs` for cost projections. 

Currently, costs are read from CSV files. To enable cost tool integration:

1. Implement the cost tool call in `integrate_costs_tool()` in `data_hydrogen.py`
2. Ensure hydrogen technologies are defined in the cost database
3. Local CSV data will override cost tool values where specified

## Key Design Patterns

Following material module patterns:

- **make_df()**: Create parameter dataframes
- **broadcast()**: Expand data across nodes/years
- **same_node()**: Set node_origin/dest equal to node_loc
- **merge_data()**: Combine parameter dictionaries
- **nodes_ex_world()**: Exclude global regions from broadcasting

## Validation

To validate the implementation:

```python
# Check that sets were added
assert "h2_elec_alk" in scenario.set("technology")
assert "carbon_black" in scenario.set("commodity")
assert "H2_leak" in scenario.set("emission")

# Check that parameters were added
input_data = scenario.par("input", filters={"technology": "h2_elec_alk"})
assert not input_data.empty
assert "electr" in input_data.commodity.values
assert "freshwater_supply" in input_data.commodity.values

output_data = scenario.par("output", filters={"technology": "h2_elec_alk"})
assert not output_data.empty
assert "hydrogen" in output_data.commodity.values

# Check historical data
hist_act = scenario.par("historical_activity", filters={"technology": "h2_elec_alk"})
assert not hist_act.empty
```

## Troubleshooting

### ModuleNotFoundError: No module named 'message_ix'
Ensure you're running in an environment with message_ix installed.

### FileNotFoundError: CSV file not found
Check that CSV files exist in `message_ix_models/data/hydrogen/`

### KeyError in set.yaml
Ensure all required sets (technology, commodity, level, etc.) are defined in set.yaml

### Empty DataFrames
Check CSV file format - ensure proper column names and no extra whitespace

### Region mismatch
Currently only R12 regions are supported. Ensure `context.model.regions = "R12"`

## References

- Material module: `message_ix_models/model/material/`
- MESSAGE-IX documentation: https://docs.messageix.org/
- Set configuration: `message_ix_models/data/hydrogen/set.yaml`

