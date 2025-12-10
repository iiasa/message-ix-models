"""Shared constants for ALPS/RIME pipeline.

Consolidates hardcoded values and paths used across multiple modules.
"""

from message_ix_models.util import package_data_path

# ==============================================================================
# Paths
# ==============================================================================

# MAGICC climate model output directory
MAGICC_OUTPUT_DIR = package_data_path(
    "report", "legacy", "reporting_output", "magicc_output"
)

# RIME emulator datasets directory
RIME_DATASETS_DIR = package_data_path("alps", "rime_datasets")


# ==============================================================================
# Region Definitions
# ==============================================================================

# R12 regions used in MESSAGE-GLOBIOM
R12_REGIONS = [
    'AFR',   # Africa
    'CHN',   # China
    'EEU',   # Eastern Europe
    'FSU',   # Former Soviet Union
    'LAM',   # Latin America
    'MEA',   # Middle East and North Africa
    'NAM',   # North America
    'PAO',   # Pacific OECD
    'PAS',   # Other Pacific Asia
    'RCPA',  # Reforming Economies of Asia
    'SAS',   # South Asia
    'WEU',   # Western Europe
]


# ==============================================================================
# Basin and Region Counts
# ==============================================================================

# Number of RIME basins (native emulator resolution)
N_RIME_BASINS = 157

# Number of MESSAGE basins (after split_basin_macroregion expansion)
N_MESSAGE_BASINS_R12 = 217

# Number of R12 regions
N_R12_REGIONS = 12


# ==============================================================================
# MESSAGE Model Years
# ==============================================================================

# Standard MESSAGE model years for optimization
MESSAGE_YEARS = [
    2020, 2025, 2030, 2035, 2040, 2045, 2050,
    2055, 2060, 2070, 2080, 2090, 2100, 2110
]


# ==============================================================================
# Timeslice Definitions
# ==============================================================================

# Default timeslice month assignments (n_time=2)
DEFAULT_H1_MONTHS = {1, 2, 3, 4, 5, 6}
DEFAULT_H2_MONTHS = {7, 8, 9, 10, 11, 12}


# ==============================================================================
# Climate Reference Values
# ==============================================================================

# Baseline global warming level for normalization (2020 reference, degrees C)
BASELINE_GWL = 1.0

# Carbon budget forcing levels (GtCO2)
FORCING_ORDER = ["600f", "850f", "1100f", "1350f", "1850f", "2100f", "2350f"]


# ==============================================================================
# Variable Mappings
# ==============================================================================

# RIME variable name mapping (external name -> dataset name)
VAR_MAP = {"local_temp": "temp_mean_anomaly"}


