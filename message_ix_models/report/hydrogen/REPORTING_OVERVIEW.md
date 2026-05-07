# Hydrogen Reporting System - Overview

## Purpose

This reporting system transforms raw MESSAGE-IX model results into standardized IAMC-format variables for hydrogen production, fugitive emissions, and related metrics. The system handles complex scenarios including unit conversions and stoichiometric factors for transient hydrogen production.

## Core Concept

The reporting system operates in **two distinct stages**:

1. **Leaf-Level Processing**: Raw MESSAGE data → Processed IAMC variables
2. **Hierarchical Aggregation**: Processed variables → Aggregate totals

This separation ensures that variables with different conversion factors (e.g., methanol from different feedstocks) are correctly aggregated.

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     MESSAGE-IX Model Results                     │
│              (Scenario data in native MESSAGE format)            │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STAGE 1: LEAF PROCESSING                      │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. Query Raw Data                                         │  │
│  │    - Read from MESSAGE Reporter (technology/mode/level)   │  │
│  │    - Filter based on YAML specifications                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 2. Apply Unit Conversion                                  │  │
│  │    - GWa → EJ/yr (using iam_units registry)              │  │
│  │    - Original units defined in leaf YAML files            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 3. Apply Stoichiometric Factors                           │  │
│  │    - Convert commodity output to H2 content               │  │
│  │    - E.g., NH3 → H2: multiply by 1.114                    │  │
│  │    - E.g., Methanol → H2: multiply by 0.937 or 1.124      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│                   IAMC Leaf Variables                            │
│              (Fully processed, ready for output)                │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGE 2: HIERARCHICAL AGGREGATION                │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Level 1 Aggregates                                        │  │
│  │  - Sum related leaf variables                             │  │
│  │  - E.g., Methanol|Coal = sum(Coal w/o CCS, Coal w/ CCS)  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Level 2 Aggregates                                        │  │
│  │  - Sum Level 1 aggregates                                 │  │
│  │  - E.g., Methanol = sum(Methanol|Coal, Methanol|Gas, ...) │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Level 3+ Aggregates                                       │  │
│  │  - Continue hierarchical summation                        │  │
│  │  - E.g., Transient = sum(Ammonia, Methanol, Refinery)    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                            │                                     │
└────────────────────────────┼────────────────────────────────────┘
                             │
                             ▼
                ┌────────────────────────┐
                │  Final IAMC DataFrame  │
                │  (Leaves + Aggregates) │
                └────────────────────────┘
```

---

## Key Design Principles

### 1. **Single Processing at Leaf Level**

All transformations (unit conversion, stoichiometric factors) are applied **once** at the leaf level. This ensures:
- Consistent treatment of raw data
- Correct handling of mixed conversion factors
- No redundant calculations

### 2. **Hierarchical Aggregation of Processed Values**

Aggregates sum **already-processed** IAMC variables, not raw MESSAGE data. Benefits:
- Properly handles variables with different factors
- Simpler aggregate definitions (no conversion logic)
- Clear separation of concerns

### 3. **Data-Driven Configuration**

All reporting specifications are defined in YAML files:
- **Leaf files** (`h2_prod.yaml`, `h2_transient.yaml`): Define MESSAGE filters, units, factors
- **Aggregate files** (`*_aggregates.yaml`): Define hierarchies using component short names

---

## Example: Methanol Production

This example demonstrates why the two-stage approach is essential.

### Problem Statement

Methanol can be produced from different feedstocks with different H2 content:
- **Coal/Biomass/Gas methanol**: 0.937 EJ H2 per EJ methanol
- **H2-based methanol**: 1.124 EJ H2 per EJ methanol

### Data Flow

```
STAGE 1: Leaf Processing
────────────────────────
MESSAGE Output:
  Methanol|Coal → 10 EJ methanol
  Methanol|H2   → 10 EJ methanol

Apply Factors (at leaf level):
  Methanol|Coal → 10 × 0.937 = 9.37 EJ H2 ✓
  Methanol|H2   → 10 × 1.124 = 11.24 EJ H2 ✓

STAGE 2: Aggregation
────────────────────
Sum Processed Values:
  Methanol = 9.37 + 11.24 = 20.61 EJ H2 ✓

Old Approach (Incorrect):
  Methanol = (10 + 10) × 1.0 = 20.00 EJ H2 ✗
```

---

## Configuration Files

### Leaf-Level Files (Define Base Variables)

| File | Purpose | Contains |
|------|---------|----------|
| `h2_prod.yaml` | Direct H2 production | MESSAGE filters, unit specs |
| `h2_transient.yaml` | Transient H2 (NH3, methanol) | MESSAGE filters, units, **stoichiometric factors** |
| `h2_fgt_emi.yaml` | Fugitive H2 emissions | MESSAGE filters, unit specs |

**Key Fields:**
- `filter`: MESSAGE technology/mode/commodity/level selectors
- `original_unit`: Raw MESSAGE unit (usually GWa)
- `stoichiometric_factor`: Conversion factor (default: 1.0)
- `short`: Short name for referencing in aggregates

### Aggregate Files (Define Hierarchies)

| File | Purpose | Contains |
|------|---------|----------|
| `h2_prod_aggregates.yaml` | H2 production totals | Component hierarchies |
| `h2_transient_aggregates.yaml` | Transient H2 totals | Component hierarchies |
| `h2_fgt_emi_aggregates.yaml` | Fugitive emission totals | Component hierarchies |

**Key Fields:**
- `level_1`, `level_2`, etc.: Hierarchical levels
- `short`: Short name for this aggregate
- `components`: List of short names to sum

**Note:** No `original_unit` or `stoichiometric_factor` needed in aggregates!

---

## Workflow Entry Points

### Running Full Hydrogen Reporting

```python
from message_ix_models.report.hydrogen.h2_reporting import run_h2_reporting

# Generate all hydrogen reporting for a scenario
df = run_h2_reporting(
    rep=reporter,           # MESSAGE Reporter
    model_name="MESSAGEix-GLOBIOM",
    scen_name="SSP2-baseline"
)
```

**What happens:**
1. Discovers all reporting variable categories (h2_prod, h2_transient, h2_fgt_emi, etc.)
2. For each category:
   - Loads configuration from YAML files
   - Processes leaf variables (Stage 1)
   - Computes aggregates (Stage 2)
3. Concatenates all results into single IAMC DataFrame
4. Adds World region as sum of all regions

### Running Individual Variable Category

```python
from message_ix_models.report.hydrogen.h2_reporting import run_reporting

# Generate only transient hydrogen reporting
df = run_reporting(
    var="h2_transient",
    rep=reporter,
    model_name="MESSAGEix-GLOBIOM",
    scen_name="SSP2-baseline"
)
```

---

## Output Format

The system produces **pyam.IamDataFrame** objects with standard IAMC structure:

| Column | Description | Example |
|--------|-------------|---------|
| `model` | Model name | MESSAGEix-GLOBIOM |
| `scenario` | Scenario name | SSP2-baseline |
| `region` | Geographic region | R12_NAM |
| `variable` | IAMC variable name | Production\|Hydrogen\|Transient\|Methanol |
| `unit` | Unit of measure | EJ/yr |
| `year` | Time period | 2020 |
| `value` | Numerical value | 20.61 |

---

## Benefits of This Architecture

✅ **Correctness**: Properly handles mixed conversion factors

✅ **Maintainability**: Clear separation between data processing and aggregation

✅ **Flexibility**: Easy to add new variables or modify hierarchies via YAML

✅ **Performance**: Each raw data point processed only once

✅ **Transparency**: Clear audit trail from MESSAGE data to final IAMC output

---

## Related Documentation

- **[REPORTING_TECHNICAL.md](REPORTING_TECHNICAL.md)**: Detailed implementation guide
- **[config.py](config.py)**: Configuration class implementation
- **[h2_reporting.py](h2_reporting.py)**: Main reporting functions

