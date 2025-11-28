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
