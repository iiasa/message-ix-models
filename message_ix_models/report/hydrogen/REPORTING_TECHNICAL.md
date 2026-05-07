# Hydrogen Reporting System - Technical Documentation

## Architecture Overview

The hydrogen reporting system consists of two main Python modules and YAML configuration files:

- **`config.py`**: Configuration management and YAML parsing
- **`h2_reporting.py`**: Data processing and aggregation logic
- **YAML files**: Declarative reporting specifications

---

## Module: config.py

### Class: Config

The `Config` class manages reporting configuration loaded from YAML files.

#### Key Attributes

```python
@dataclass
class Config:
    iamc_prefix: str              # IAMC variable prefix (e.g., "Production|")
    unit: str                     # Target unit (e.g., "EJ/yr")
    var: str                      # MESSAGE Reporter key (e.g., "out", "emi")
    mapping: pd.DataFrame         # Technology-level mapping for leaf variables
    aggregates: dict              # Aggregate definitions by level
```

#### The `mapping` DataFrame Structure

The mapping DataFrame contains one row per technology/mode/commodity/level combination:

**Index**: MultiIndex with dimensions from MESSAGE (subset of `t, m, c, l, e`)

**Columns**:
- `iamc_name`: IAMC variable name fragment
- `short_name`: Short reference name for aggregation
- `unit`: Target unit
- `original_unit`: Source MESSAGE unit
- `stoichiometric_factor`: Conversion factor (default 1.0)

**Example Row**:
```
Index: (technology='biomass_NH3', mode='M1', commodity='NH3', level='secondary_material')
Values: {
    'iamc_name': 'Transient|Ammonia|Biomass w/o CCS',
    'short_name': 'prod_h2_transient_nh3_biomass',
    'unit': 'EJ/yr',
    'original_unit': 'GWa',
    'stoichiometric_factor': 1.114
}
```

#### The `aggregates` Dictionary Structure

```python
{
    "level_1": {
        "Transient|Methanol|Coal": {
            "short": "prod_h2_transient_meth_coal_total",
            "components": ["prod_h2_transient_meth_coal", "prod_h2_transient_meth_coal_ccs"]
        }
    },
    "level_2": {
        "Transient|Methanol": {
            "short": "prod_h2_transient_meth",
            "components": ["prod_h2_transient_meth_coal_total", ...]
        }
    }
}
```

#### Key Methods

##### `from_files(category: str) -> Config`

**Purpose**: Factory method to create Config from YAML files.

**Process**:
1. Load base YAML file (`{category}.yaml`)
2. Parse metadata (iamc_prefix, unit, var)
3. Parse `vars` section → populate `mapping` via `use_vars_dict()`
4. Load aggregate YAML file (`{category}_aggregates.yaml`)
5. Store aggregate definitions via `store_aggregates()`

**Example**:
```python
config = Config.from_files("h2_transient")
# Loads: h2_transient.yaml + h2_transient_aggregates.yaml
```

##### `use_vars_dict(data: dict) -> None`

**Purpose**: Populate mapping DataFrame from leaf variable definitions.

**Process**:
1. For each variable in `vars` section:
   - Extract MESSAGE filter (technology, mode, commodity, level)
   - Create Cartesian product of filter values
   - Assign metadata (iamc_name, short_name, unit, original_unit, stoichiometric_factor)
2. Combine all variables into single DataFrame
3. Set multi-index based on MESSAGE dimensions present

**Result**: `self.mapping` contains all leaf variables with their specifications.

##### `store_aggregates(data: dict) -> None`

**Purpose**: Store aggregate hierarchy definitions without creating mappings.

**Process**:
1. Remove metadata keys (iamc_prefix, unit, var)
2. For each level (level_1, level_2, ...):
   - Store aggregate definitions as-is
3. Result: `self.aggregates` contains hierarchy specifications

**Design Decision**: Unlike the old approach, this does NOT create technology-level mappings for aggregates. Aggregates are now computed at the IAMC variable level, not by re-querying MESSAGE data.

##### `get_aggregate_definitions() -> dict`

**Purpose**: Retrieve aggregate definitions for IAMC-level aggregation.

**Returns**: The `self.aggregates` dictionary.

---

## Module: h2_reporting.py

### Function: pyam_df_from_rep()

**Signature**:
```python
def pyam_df_from_rep(
    rep: message_ix.Reporter,
    reporter_var: str,
    mapping_df: pd.DataFrame
) -> pd.DataFrame
```

**Purpose**: Query raw MESSAGE data and join with mapping specifications.

**Process**:
1. Extract filter dimensions from mapping index
2. Apply filters to MESSAGE Reporter
3. Query data: `rep.get(f"{reporter_var}:nl-t-ya-m-c-l-e")`
4. Join with mapping DataFrame to add IAMC metadata
5. Group by (region, year, iamc_name, original_unit, stoichiometric_factor)
6. Sum across technologies/modes that map to same IAMC variable

**Returns**: DataFrame with columns:
- Index: (nl, ya, iamc_name, original_unit, stoichiometric_factor)
- Column: `value`

**Note**: At this stage, data is still in MESSAGE units (typically GWa) and stoichiometric factors have NOT been applied yet.

---

### Function: convert_units_from_mapping()

**Signature**:
```python
def convert_units_from_mapping(
    df: pd.DataFrame,
    target_unit: str
) -> pd.DataFrame
```

**Purpose**: Apply unit conversion and stoichiometric factors.

**Process**:

#### Step 1: Unit Conversion
For each unique `original_unit` in the DataFrame:
1. Check YAML-defined conversions (`unit_conversions.yaml`)
2. If not found, use `iam_units.registry` for conversion
3. Apply: `value = value * conversion_factor`

#### Step 2: Stoichiometric Factor Application
If `stoichiometric_factor` exists in the index:
1. For each row: `value = value * stoichiometric_factor`
2. This converts commodity output (e.g., NH3) to H2 content

**Example**:
```python
# Input row
value: 10.0
original_unit: GWa
stoichiometric_factor: 1.114

# After unit conversion (GWa → EJ/yr: factor ~31.536)
value: 315.36

# After stoichiometric factor
value: 351.37  # = 315.36 * 1.114
```

**Returns**: DataFrame with converted values, same index structure.

---

### Function: format_reporting_df()

**Signature**:
```python
def format_reporting_df(
    df: pd.DataFrame,
    variable_prefix: str,
    model_name: str,
    scenario_name: str,
    unit: str,
    mappings: pd.DataFrame
) -> pyam.IamDataFrame
```

**Purpose**: Format processed data into IAMC structure.

**Process**:
1. Call `convert_units_from_mapping()` to apply conversions/factors
2. Drop `original_unit` and `stoichiometric_factor` from index
3. Rename columns: `iamc_name` → `variable`, `nl` → `region`, `ya` → `Year`
4. Add: `Model`, `Scenario`, `Unit` columns
5. Clean region names (remove `R12_` prefix)
6. Group by IAMC dimensions and sum (handles duplicate rows after dropping factors)
7. Convert to `pyam.IamDataFrame`
8. Add zero time series for missing variables (for completeness)

**Returns**: `pyam.IamDataFrame` with fully processed leaf variables.

---

### Function: compute_aggregates_from_iamc()

**Signature**:
```python
def compute_aggregates_from_iamc(
    df: pyam.IamDataFrame,
    aggregates: dict,
    iamc_prefix: str,
    short_to_iamc: dict
) -> pyam.IamDataFrame
```

**Purpose**: Compute hierarchical aggregates from processed IAMC variables.

**Critical Design**: This function operates on **already-processed** IAMC variables (after unit conversion and stoichiometric factors). It does NOT re-query MESSAGE data.

#### Process Flow

```python
# Setup
df_work = df.as_pandas().copy()  # Working DataFrame
short_to_full_var = {
    short: iamc_prefix + iamc_name 
    for short, iamc_name in short_to_iamc.items()
}

# Process level by level
for level_key in sorted(aggregates.keys()):  # level_1, level_2, ...
    level_rows = []
    
    for iamc_name, agg_def in aggregates[level_key].items():
        # 1. Resolve component short names to full IAMC variable names
        component_vars = [
            short_to_full_var[comp_short]
            for comp_short in agg_def["components"]
            if comp_short in short_to_full_var
        ]
        
        # 2. Filter DataFrame for component variables
        df_components = df_work[df_work["variable"].isin(component_vars)]
        
        # 3. Sum components by (model, scenario, region, year, unit)
        df_agg = (
            df_components
            .groupby(["model", "scenario", "region", "year", "unit"])
            .agg({"value": "sum"})
            .reset_index()
        )
        
        # 4. Assign aggregate variable name
        full_var_name = iamc_prefix + iamc_name
        df_agg["variable"] = full_var_name
        
        # 5. Collect this aggregate
        level_rows.append(df_agg)
        
        # 6. Register for use in next level
        short_to_full_var[agg_def["short"]] = full_var_name
    
    # Add this level's aggregates to working DataFrame
    # This allows level_2 to reference level_1 aggregates
    if level_rows:
        df_work = pd.concat([df_work] + level_rows, ignore_index=True)

return pyam.IamDataFrame(df_work)
```

#### Key Design Elements

1. **Level-by-Level Processing**: Each level's aggregates are added to `df_work` before processing the next level, enabling hierarchical references.

2. **Component Resolution**: Components are referenced by short names and resolved to full IAMC variable names dynamically.

3. **No Factor Application**: Aggregates sum already-factored values. No conversion logic needed here.

4. **Flexible Hierarchy**: Any number of levels supported; automatically handles dependencies.

---

### Function: run_reporting()

**Signature**:
```python
def run_reporting(
    var: str,
    rep: Reporter,
    model_name: str,
    scen_name: str
) -> pyam.IamDataFrame
```

**Purpose**: Main entry point for reporting a single variable category.

**Process**:

```python
# 1. Load configuration
config = Config.from_files(var)
# Result: config.mapping has leaf variables, config.aggregates has hierarchy

# 2. Query and process leaf variables (Stage 1)
df = pyam_df_from_rep(rep, config.var, config.mapping)
py_df = format_reporting_df(df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping)
# Result: py_df contains fully processed leaf IAMC variables

# 3. Build short_name → iamc_name mapping
short_to_iamc = (
    config.mapping
    .reset_index()[["short_name", "iamc_name"]]
    .drop_duplicates()
    .set_index("short_name")["iamc_name"]
    .to_dict()
)

# 4. Compute aggregates (Stage 2)
aggregates = config.get_aggregate_definitions()
if aggregates:
    py_df = compute_aggregates_from_iamc(
        py_df, aggregates, config.iamc_prefix, short_to_iamc
    )
# Result: py_df now contains both leaves and all aggregate levels

return py_df
```

**Returns**: `pyam.IamDataFrame` with complete reporting output (leaves + aggregates).

---

### Function: run_h2_reporting()

**Signature**:
```python
def run_h2_reporting(
    rep: Reporter,
    model_name: str,
    scen_name: str,
    add_world: bool = True
) -> pyam.IamDataFrame
```

**Purpose**: Generate all hydrogen reporting variables for a scenario.

**Process**:
1. Discover all variable categories: `fetch_variables()`
   - Scans `data/hydrogen/reporting/` for `*.yaml` files
   - Excludes `*_aggregates.yaml` and `unit_conversions.yaml`
2. For each category: call `run_reporting()`
3. Concatenate all DataFrames: `pyam.concat(dfs)`
4. Optionally add World region: `py_df.aggregate_region(...)`

**Returns**: Single `pyam.IamDataFrame` with all hydrogen reporting.

---

## YAML File Specifications

### Leaf Variable Files (e.g., h2_transient.yaml)

```yaml
var: out                    # MESSAGE Reporter variable to query
iamc_prefix: Production|Hydrogen|  # Prefix for all IAMC variables
unit: EJ/yr                 # Target unit for output

vars:
  Transient|Ammonia|Biomass w/o CCS:
    filter:                 # MESSAGE filter specification
      technology: biomass_NH3
      mode: M1
      commodity: NH3
      level: secondary_material
    original_unit: GWa      # MESSAGE output unit
    stoichiometric_factor: 1.114  # NH3 → H2 conversion
    short: prod_h2_transient_nh3_biomass  # Reference name
```

**Filter Specification**:
- Keys: MESSAGE dimensions (`technology`, `mode`, `commodity`, `level`, `emission`)
- Values: Single value (string) or list of values
- Cartesian product created for lists

**Optional Fields**:
- `original_unit`: defaults to `"GWa"`
- `stoichiometric_factor`: defaults to `1.0`
- `unit`: defaults to top-level `unit`

### Aggregate Files (e.g., h2_transient_aggregates.yaml)

```yaml
var: out                    # Must match base file
iamc_prefix: Production|Hydrogen|  # Must match base file
unit: EJ/yr                 # Must match base file

level_1:                    # First aggregation level
  Transient|Ammonia:
    short: prod_h2_transient_nh3        # Short name for this aggregate
    components:             # List of short names to sum
      - prod_h2_transient_nh3_biomass
      - prod_h2_transient_nh3_gas
      - prod_h2_transient_nh3_coal

level_2:                    # Second aggregation level
  Transient:
    short: prod_h2_transient
    components:
      - prod_h2_transient_nh3          # Can reference level_1 aggregates
      - prod_h2_transient_meth
```

**Important**: 
- NO `original_unit` or `stoichiometric_factor` in aggregates
- Components reference `short` names from leaves or lower-level aggregates
- Levels processed in order: level_1, level_2, level_3, ...

---

## Design Patterns and Decisions

### Why Separate Leaf Processing from Aggregation?

**Problem**: Different technologies producing the same commodity may have different conversion factors.

**Example**: Methanol
- From coal/bio/gas: 0.937 EJ H2 per EJ methanol
- From H2: 1.124 EJ H2 per EJ methanol

**Failed Approach** (Old System):
```python
# Query aggregate "Methanol" from MESSAGE
# Get: 10 EJ (coal) + 10 EJ (H2) = 20 EJ
# Apply single factor: 20 * 1.0 = 20 EJ H2  ❌ WRONG
```

**Correct Approach** (New System):
```python
# Process leaves first
# Coal: 10 * 0.937 = 9.37 EJ H2
# H2:   10 * 1.124 = 11.24 EJ H2
# Aggregate: 9.37 + 11.24 = 20.61 EJ H2  ✓ CORRECT
```

### Why Not Use old `use_aggregates_dict()`?

The old `use_aggregates_dict()` method created technology-level mappings for aggregates by:
1. Merging aggregate definitions with leaf mappings
2. Creating duplicate rows for each technology in each component

**Problems**:
- Re-queried MESSAGE data for aggregates
- Couldn't handle mixed conversion factors
- Required `original_unit` at every level
- Tight coupling between aggregation and data querying

**New Approach**:
- Aggregates stored separately (`store_aggregates()`)
- No technology-level expansion
- Aggregation happens on processed IAMC variables
- Clean separation of concerns

### Why Use `short_to_iamc` Mapping?

Aggregates reference components by **short names**, but DataFrame contains **full IAMC variable names**.

**Mapping Purpose**: Translate short references to actual variable names.

**Example**:
```python
# Aggregate definition
components: ["prod_h2_gas", "prod_h2_coal"]

# short_to_iamc mapping
{
    "prod_h2_gas": "Hydrogen|Gas",
    "prod_h2_coal": "Hydrogen|Coal"
}

# Resolved to full names
component_vars = [
    "Production|Hydrogen|Gas",    # = prefix + "Hydrogen|Gas"
    "Production|Hydrogen|Coal"     # = prefix + "Hydrogen|Coal"
]
```

---

## Error Handling and Edge Cases

### Missing Components

If an aggregate references a component that doesn't exist:
```python
if not component_vars:
    # No components found - skip this aggregate
    continue
```

**Result**: Aggregate silently skipped. No error raised.

**Rationale**: Some components may not exist in all scenarios (e.g., refinery H2 may be disabled).

### Empty Data

If no data exists for a variable category:
```python
if df_components.empty:
    continue
```

**Result**: Aggregate not created. Leaf variables may still exist.

### Unit Conversion Failures

If unit conversion fails (unsupported units):
```python
except Exception as e:
    log.warning(f"Could not convert from {orig_unit} to {target_unit}: {e}")
    continue  # Keep original values
```

**Result**: Values retained without conversion. Warning logged.

---

## Testing Strategy

### Unit Tests (Recommended)

Test individual functions with mock data:

```python
def test_compute_aggregates_mixed_factors():
    """Test aggregation with different stoichiometric factors."""
    # Create mock IAMC DataFrame with factored values
    data = pd.DataFrame([
        {"variable": "Prod|Methanol|Coal", "value": 9.37},   # 10 * 0.937
        {"variable": "Prod|Methanol|H2", "value": 11.24},    # 10 * 1.124
    ])
    
    aggregates = {
        "level_1": {
            "Methanol": {
                "short": "meth",
                "components": ["meth_coal", "meth_h2"]
            }
        }
    }
    
    result = compute_aggregates_from_iamc(...)
    assert result[result.variable == "Prod|Methanol"].value == 20.61
```

### Integration Tests

Test with actual MESSAGE scenarios:
1. Create small test scenario with known values
2. Run full reporting
3. Verify leaf variables and aggregates match expected values

---

## Performance Considerations

### Single Pass Processing

Leaf variables are processed once:
- Query MESSAGE: O(n) where n = number of technologies
- Unit conversion: O(m) where m = number of unique units
- Factor application: O(k) where k = number of rows

### Hierarchical Aggregation

Aggregates computed iteratively:
- Level 1: O(l₁) where l₁ = number of level 1 aggregates
- Level 2: O(l₂) sums level 1 results (already computed)
- Total: O(Σlᵢ) - linear in number of aggregates

### Memory Usage

- Leaf DataFrame: ~(rows × columns) for MESSAGE data
- Working DataFrame: Grows linearly with each aggregation level
- Final DataFrame: Contains all leaves + all aggregates

**Optimization**: Could use generator pattern for very large scenarios, but current approach is sufficient for typical MESSAGE-GLOBIOM outputs.

---

## Future Enhancements

### Potential Improvements

1. **Validation**: Add schema validation for YAML files
2. **Caching**: Cache Config objects to avoid re-parsing YAMLs
3. **Parallelization**: Process variable categories in parallel
4. **Custom Aggregation Functions**: Support non-sum aggregations (weighted averages, etc.)
5. **Metadata Propagation**: Track data provenance through pipeline

### Known Limitations

1. **Fixed Conversion Factors**: Factors are constant across time/space
2. **Sum-Only Aggregation**: Only summation supported, no other operations
3. **YAML-Only Configuration**: No programmatic API for defining variables
4. **No Validation**: YAML structure not validated at load time

---

## Debugging Tips

### Enable Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Inspect Intermediate Results

```python
# After leaf processing
print(py_df.as_pandas())

# After each aggregation level
print(df_work[df_work["variable"].str.contains("Level1Aggregate")])
```

### Verify Mapping

```python
config = Config.from_files("h2_transient")
print(config.mapping)
print(config.aggregates)
```

### Check Short Name Resolution

```python
short_to_iamc = {...}
print(f"Resolved components: {[short_to_iamc.get(s) for s in components]}")
```

---

## Related Files

- **REPORTING_OVERVIEW.md**: High-level system architecture
- **config.py**: Config class implementation
- **h2_reporting.py**: Main reporting functions
- **data/hydrogen/reporting/**: YAML configuration files

