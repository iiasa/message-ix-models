"""ALPS scenario analysis utilities.

Composable functions for extracting, comparing, and validating CID scenario results.

Public API:
- extract: Scenario data extraction (costs, water CIDs)
- compare: Cross-scenario comparison and differencing
- validate: Monotonicity and coherence validation
- aggregate: Basin-to-region aggregation
"""

from .extract import (
    expand_scenario_alias,
    extract_nodal_costs,
    extract_water_cids,
    load_scenarios,
    pivot_to_wide,
    SCENARIO_ALIASES,
)
from .compare import (
    build_comparison_table,
    compute_relative_change,
    compute_scenario_diffs,
    compute_yearly_comparison,
    compute_yearly_diffs,
)
from .validate import (
    compute_basin_monotonicity,
    compute_temporal_coherence,
    validate_scenario_ensemble,
)
from .aggregate import (
    aggregate_by_year,
    aggregate_to_r12,
    compute_basin_contributions,
)
from .sensitivity import (
    build_gmt_scenario_mapping,
    clear_gmt_cache,
    compute_basin_sensitivity,
    compute_rime_sensitivity,
    compute_sensitivity_from_data,
    compute_sensitivity_from_scenarios,
    extract_expected_gmt_for_scenario,
)
from .visualize import (
    plot_metric_map,
    plot_monotonicity_map,
    plot_sensitivity_map,
)

__all__ = [
    # extract
    "expand_scenario_alias",
    "extract_nodal_costs",
    "extract_water_cids",
    "load_scenarios",
    "pivot_to_wide",
    "SCENARIO_ALIASES",
    # compare
    "build_comparison_table",
    "compute_relative_change",
    "compute_scenario_diffs",
    "compute_yearly_comparison",
    # validate
    "compute_basin_monotonicity",
    "compute_temporal_coherence",
    "validate_scenario_ensemble",
    # aggregate
    "aggregate_by_year",
    "aggregate_to_r12",
    "compute_basin_contributions",
    # sensitivity
    "build_gmt_scenario_mapping",
    "clear_gmt_cache",
    "compute_basin_sensitivity",
    "compute_rime_sensitivity",
    "compute_sensitivity_from_data",
    "compute_sensitivity_from_scenarios",
    "extract_expected_gmt_for_scenario",
    # visualize
    "plot_metric_map",
    "plot_monotonicity_map",
    "plot_sensitivity_map",
]
