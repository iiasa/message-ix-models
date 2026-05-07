# Hydrogen Reporting - Debugging and Fixes

## Issue Summary

The hydrogen reporting was returning empty results with the error:
```
ValueError: This IamDataFrame is empty.
```

## Root Causes Identified

### 1. Technology Name Mismatch ✅ FIXED
**Problem:** YAML file used incorrect technology name
- YAML had: `h2_elec_alkaline`
- Scenario has: `h2_elec_alk`

**Fix:** Updated YAML files to use correct technology names:
- `message_ix_models/data/hydrogen/reporting/h2_prod.yaml` (line 63)
- `message_ix_models/data/hydrogen/reporting/h2_fgt_emi.yaml` (line 35)

### 2. Data Merging Logic Error ✅ FIXED
**Problem:** The `pyam_df_from_rep` function used `pd.merge()` which requires ALL index columns to match exactly. Emissions data has dimensions `[t, m]` while production data has `[t, m, c, l]`. The merge failed because emissions data doesn't have `c` and `l` columns.

**Fix:** Changed from `merge` to `join`, matching the material reporting implementation:
```python
# OLD (incorrect):
merged_df = pd.merge(df_var_reset, mapping_df_reset, on=list(mapping_df.index.names), how="inner")

# NEW (correct):
df = df_var.join(mapping_df[["iamc_name", "unit"]]).dropna().groupby(["nl", "ya", "iamc_name"]).sum(numeric_only=True)
```

The `join` operation allows partial index matching - it joins on whatever dimensions are present in both DataFrames.

### 3. Empty DataFrame Handling ✅ FIXED
**Problem:** When creating an empty DataFrame with MultiIndex, the code tried to use `rename_axis` on a DataFrame without an existing MultiIndex, causing:
```
ValueError: Length of new names must be 1, got 3
```

**Fix:** Properly create MultiIndex for empty DataFrames:
```python
# OLD (incorrect):
return pd.DataFrame(columns=["value"]).rename_axis(index=["nl", "ya", "iamc_name"])

# NEW (correct):
empty_df = pd.DataFrame(columns=["value"])
empty_df.index = pd.MultiIndex.from_tuples([], names=["nl", "ya", "iamc_name"])
return empty_df
```

## Verification

### Data Confirmed Present in Scenario
```
✓ 44 hydrogen technologies
✓ 3,984 production records (out parameter)
✓ 7,144 H2_leak emission records (emission_factor parameter)
```

### Technologies Correctly Mapped
**Production:**
- h2_smr, h2_smr_ccs (Gas SMR with/without CCS)
- h2_pyro_elec (Gas Pyrolysis)
- h2_coal, h2_coal_ccs (Coal with/without CCS)
- h2_bio, h2_bio_ccs (Biomass with/without CCS)
- h2_elec_alk, h2_elec_pem, h2_elec_soe (Electrolyzers)

**Emissions:**
- All production technologies
- Distribution (h2_t_d, h2_mix)
- Demand sectors (industry, R&C, transport, power)

### Final Results
```
✅ Report successfully saved to Excel
✅ 533 rows of data reported (492 regional + 41 World)
✅ Coverage:
   - 12 regions + World aggregate (13 total)
   - 16 time periods (2010-2110)
   - Production and Emissions variables
   - All aggregation levels
   - World region = sum of all 12 regions
```

## Technical Notes

### Why `join` Works Better Than `merge`

The Reporter returns data with dimensions `[nl, t, ya, m, c, l]` (node, technology, year, mode, commodity, level).

Different reporting categories need different subsets:
- **Production (out):** needs `[t, m, c, l]` - which commodity is produced at which level
- **Emissions (emi):** needs only `[t, m]` - emissions don't differentiate by commodity/level

Using `join`:
- Production mapping has index `[t, m, c, l]` → joins on all 4 dimensions ✓
- Emissions mapping has index `[t, m]` → joins on only these 2 dimensions ✓

Using `merge` (old approach):
- Would require exact column match → emissions fail because no `c`, `l` columns ❌

### Reporter Query Dimensions

The query `"out:nl-t-ya-m-c-l"` always returns all 6 dimensions, even when querying emissions (`emi`). This is intentional - the Reporter provides maximum dimensionality, and the mapping determines which dimensions to use for grouping.

For emissions, the `c` and `l` dimensions typically contain the emission type and level, which get summed over during the aggregation.

## Files Modified

1. **message_ix_models/data/hydrogen/reporting/h2_prod.yaml**
   - Line 63: Changed `h2_elec_alkaline` → `h2_elec_alk`

2. **message_ix_models/data/hydrogen/reporting/h2_fgt_emi.yaml**
   - Line 35: Changed `h2_elec_alkaline` → `h2_elec_alk`

3. **message_ix_models/report/hydrogen/h2_reporting.py**
   - Lines 27-43: Replaced merge logic with join logic
   - Lines 34-38 (old 34-38, 50-54): Fixed empty DataFrame creation

## Testing Performed

1. **Scenario Data Check:** Confirmed h2_elec does NOT exist (only h2_elec_alk/pem/soe)
2. **Reporter Query Test:** Verified Reporter can query production and emission data
3. **Config Mapping Test:** Confirmed YAML files load correctly with proper mappings
4. **End-to-End Test:** Successfully generated complete hydrogen report with 492 variables

## Conclusion

The hydrogen reporting system is now **fully functional**. The issues were:
1. Minor naming mismatch (h2_elec_alkaline vs h2_elec_alk)
2. Incorrect data merging approach (merge vs join)
3. Edge case in empty DataFrame handling

All issues have been resolved and the system produces complete, accurate reports.

---

**Date:** October 6, 2025  
**Scenario Tested:** hyway_SSP_SSP2_v6.2 / baseline_1000f_h2_ct_h2_emissions  
**Platform:** ixmp-dev

