import logging
from collections import defaultdict
from functools import lru_cache
from itertools import product
from typing import TYPE_CHECKING, Optional
from warnings import warn

import pandas as pd
import xarray as xr
from iam_units import registry
from sdmx.model.v21 import Code

from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import load_package_data

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from message_ix_models import ScenarioInfo
# Configuration files
METADATA = [
    # Information about MESSAGE-water
    ("water", "config"),
    ("water", "set"),
    ("water", "technology"),
]

# Conversion factors used in the water module

MONTHLY_CONVERSION = (
    (30 * registry.day / registry.month).to_base_units().magnitude
)  # MCM/day to MCM/month
# Convert USD/(m³/day) to USD/MCM: m³/day * 365 days/year / 1e6 m³/MCM
USD_M3DAY_TO_USD_MCM = (registry("m^3/day").to("m^3/year").magnitude) / 1e6
USD_KM3_TO_USD_MCM = registry("USD/km^3").to("USD/m^3").magnitude * 1e6
GWa_KM3_TO_GWa_MCM = registry("GWa/km^3").to("GWa/m^3").magnitude * 1e6
ANNUAL_CAPACITY_FACTOR = 5  # Convert 5-year capacity to annual
# Convert km³ to MCM: 1 km³ = 1e9 m³, 1 MCM = 1e6 m³, so factor = 1000
KM3_TO_MCM = registry("1 km^3").to("meter^3").magnitude / 1e6  # km³ to MCM conversion
kWh_m3_TO_GWa_MCM = registry("kWh/m^3").to("GWa/m^3").magnitude * 1e6

# Convert m3/GJ to MCM/GWa
m3_GJ_TO_MCM_GWa = registry("m^3/GJ").to("m^3/GWa").magnitude / 1e6
# MCM not standard so have to remember to divide by 1e6 each time.


def map_month_to_timeslice(month_number: int, n_time: int) -> str:
    """Map month number (1-12) to timeslice name (h1, h2, ..., hn).

    This function enables flexible temporal aggregation by mapping monthly data
    to arbitrary timeslice resolutions. It supports both uniform and non-uniform
    aggregation patterns.

    Parameters
    ----------
    month_number : int
        Month number (1-12 for Jan-Dec)
    n_time : int
        Number of timeslices per year

    Returns
    -------
    str
        Timeslice name (h1, h2, ..., hn)

    Raises
    ------
    ValueError
        If month_number is not in range 1-12
    NotImplementedError
        If n_time > 12 (sub-monthly resolution)

    Examples
    --------
    Monthly (n_time=12):
    >>> map_month_to_timeslice(1, 12)  # January
    'h1'
    >>> map_month_to_timeslice(12, 12)  # December
    'h12'

    Quarterly (n_time=4):
    >>> map_month_to_timeslice(1, 4)  # Jan-Mar → Q1
    'h1'
    >>> map_month_to_timeslice(6, 4)  # Apr-Jun → Q2
    'h2'

    Seasonal (n_time=2):
    >>> map_month_to_timeslice(1, 2)  # Jan-Jun → first half
    'h1'
    >>> map_month_to_timeslice(12, 2)  # Jul-Dec → second half
    'h2'
    """
    if not (1 <= month_number <= 12):
        raise ValueError(f"month_number must be 1-12, got {month_number}")

    if n_time > 12:
        raise NotImplementedError(
            f"Sub-monthly timeslices (n_time={n_time}) not yet supported. "
            "Monthly data cannot be disaggregated to sub-monthly resolution."
        )

    if n_time == 12:
        # Direct mapping: month → timeslice
        return f"h{month_number}"

    elif n_time < 12 and 12 % n_time == 0:
        # Equal aggregation: 12 months divide evenly into n_time slices
        # Examples: n_time=6 (2 months/slice), n_time=4 (3 months/slice),
        #          n_time=2 (6 months/slice)
        months_per_slice = 12 // n_time
        timeslice_idx = ((month_number - 1) // months_per_slice) + 1
        return f"h{timeslice_idx}"

    else:
        # Non-uniform aggregation: use proportional mapping
        # This handles cases like n_time=5 where months don't divide evenly
        timeslice_idx = ((month_number - 1) * n_time // 12) + 1
        return f"h{timeslice_idx}"


def get_days_per_timeslice(n_time: int) -> float:
    """Calculate average days per timeslice for unit conversions.

    Parameters
    ----------
    n_time : int
        Number of timeslices per year

    Returns
    -------
    float
        Average number of days per timeslice

    Examples
    --------
    >>> get_days_per_timeslice(12)  # Monthly
    30.4375
    >>> get_days_per_timeslice(4)   # Quarterly
    91.3125
    >>> get_days_per_timeslice(2)   # Seasonal
    182.625
    """
    return 365.25 / n_time


def read_config(context: Optional[Context] = None):
    """Read the water model configuration / metadata from file.

    Numerical values are converted to computation-ready data structures.

    Returns
    -------
    .Context
        The current Context, with the loaded configuration.
    """

    context = context or Context.get_instance(-1)

    # if context.nexus_set == 'nexus':
    if "water set" in context:
        # Already loaded
        return context

    # Load water configuration
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_package_data(*_parts)

    return context


def filter_basins_by_region(
    df_basins: pd.DataFrame,
    context: Optional[Context] = None,
    n_per_region: int = 3,
) -> pd.DataFrame:
    """Filter basins based on context configuration.

    Parameters
    ----------
    df_basins : pd.DataFrame
        DataFrame with basin data including 'REGION' and 'BCU_name' columns
    context : Context, optional
        Context object that may contain basin filtering configuration
    n_per_region : int, default 3
        Default number of basins to keep per region (used as fallback)

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame based on configuration
    """
    if not context:
        context = Context.get_instance(-1)

    # Check if reduced basin filtering is enabled
    reduced_basin = getattr(context, 'reduced_basin', False)

    if not reduced_basin:
        # No filtering, return original dataframe
        log.info("Basin filtering disabled, returning all basins")
        return df_basins

    # Basin filtering is enabled
    filter_list = getattr(context, 'filter_list', None)
    num_basins = getattr(context, 'num_basins', None)

    if filter_list:
        # Filter to specific basin list
        filtered = df_basins[df_basins['BCU_name'].isin(filter_list)]

        # Check if we have at least 1 basin per R12 region
        all_regions = set(df_basins['REGION'].unique())
        filtered_regions = set(filtered['REGION'].unique())
        missing_regions = all_regions - filtered_regions

        if missing_regions:
            log.info(f"Adding one basin per missing region: {missing_regions}")
            # Add one basin from each missing region
            for region in missing_regions:
                region_basins = df_basins[df_basins['REGION'] == region]
                # Add the first basin from this region
                filtered = pd.concat(
                    [filtered, region_basins.head(1)], ignore_index=True
                )

        log.info(
            f"Filtered basins from {len(df_basins)} to {len(filtered)} "
            f"using custom filter list: {filter_list} (with 1 basin per missing region)"
        )

        return filtered.reset_index(drop=True)

    elif num_basins is not None:
        # Use specified number of basins per region
        n_per_region = num_basins
    # else: use function default n_per_region

    # Group by region and take first n rows from each group
    if 'REGION' not in df_basins.columns:
        log.info("REGION column not found, cannot filter by region")
        return df_basins

    filtered = df_basins.groupby('REGION', group_keys=False).apply(
        lambda x: x.head(n_per_region)
    ).reset_index(drop=True)

    log.info(f"Filtered basins from {len(df_basins)} to {len(filtered)} "
            f"(keeping first {n_per_region} per region)")

    return filtered


@lru_cache()
def map_add_on(rtype=Code):
    """Map addon & type_addon in ``sets.yaml``."""
    dims = ["add_on", "type_addon"]

    # Retrieve configuration
    context = read_config()

    # Assemble group information
    result = defaultdict(list)

    for indices in product(*[context["water set"][d]["add"] for d in dims]):
        # Create a new code by combining two
        result["code"].append(
            Code(
                id="".join(str(c.id) for c in indices),
                name=", ".join(str(c.name) for c in indices),
            )
        )

        # Tuple of the values along each dimension
        result["index"].append(tuple(c.id for c in indices))

    if rtype == "indexers":
        # Three tuples of members along each dimension
        indexers = zip(*result["index"])
        indexers = {
            d: xr.DataArray(list(i), dims="consumer_group")
            for d, i in zip(dims, indexers)
        }
        indexers["consumer_group"] = xr.DataArray(
            [c.id for c in result["code"]],
            dims="consumer_group",
        )
        return indexers
    elif rtype is Code:
        return sorted(result["code"], key=str)
    else:
        raise ValueError(rtype)


def add_commodity_and_level(df: pd.DataFrame, default_level=None):
    # Add input commodity and level
    t_info: list = Context.get_instance()["water set"]["technology"]["add"]
    c_info: list = get_codes("commodity")

    @lru_cache()
    def t_cl(t):
        input = t_info[t_info.index(t)].annotations["input"]
        # Commodity must be specified
        commodity = input["commodity"]
        # Use the default level for the commodity in the RES (per
        # commodity.yaml)
        level = (
            input.get("level", "water_supply")
            or c_info[c_info.index(commodity)].annotations.get("level", None)
            or default_level
        )

        return commodity, level

    def func(row: pd.Series):
        row[["commodity", "level"]] = t_cl(row["technology"])
        return row

    return df.apply(func, axis=1)


def get_vintage_and_active_years(
    info: Optional["ScenarioInfo"],
    technical_lifetime: Optional[int] = None,
    same_year_only: bool = False,
) -> pd.DataFrame:
    """Calculate valid vintage-activity year combinations without scenario dependency.

    This implements similar logic as scenario.vintage_and_active_years() but
    uses the technical lifetime data directly instead of requiring it to be in
    the scenario first.

    Parameters
    ----------
    info : ScenarioInfo
        Contains the base yv_ya combinations and duration_period data
    technical_lifetime : int, optional
        Technical lifetime in years. If None, returns all combinations.
    same_year_only : bool, optional
        If True, returns only combinations where year_vtg == year_act.
        Useful for dummy technologies where vintage doesn't matter.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['year_vtg', 'year_act'] containing valid combinations
    """
    # Get base yv_ya from ScenarioInfo property
    yv_ya = info.yv_ya

    # If same_year_only is requested, return only year_vtg == year_act combinations
    if same_year_only:
        same_year_mask = yv_ya["year_vtg"] == yv_ya["year_act"]
        return yv_ya[same_year_mask].reset_index(drop=True)

    # If no technical lifetime specified or is nan, default to same year
    if technical_lifetime is None or pd.isna(technical_lifetime):
        warn(
            "no technical_lifetime provided, defaulting to same year",
            UserWarning,
        )
        same_year_mask = yv_ya["year_vtg"] == yv_ya["year_act"]
        return yv_ya[same_year_mask].reset_index(drop=True)

    # Memory optimization: use same-year logic for short-lived technologies
    # to reduce unused equations. Time steps are 5-year intervals pre-2060,
    # 10-year intervals post-2060. Short lifetimes don't benefit from advance
    # construction.
    kink_year = 2060

    short_lifetime_condition = (
        (technical_lifetime <= 5) or
        (technical_lifetime <= 10 and (yv_ya["year_act"] >= kink_year).any())
    )
    if short_lifetime_condition:
        # Pre-2060: use same-year if lifetime <= 5
        # Post-2060: use same-year if lifetime <= 10
        if technical_lifetime <= 5:
            # Same-year for entire horizon
            same_year_mask = yv_ya["year_vtg"] == yv_ya["year_act"]
            return yv_ya[same_year_mask].reset_index(drop=True)
        else:
            # Same-year only for post-2060, normal logic for pre-2060
            pre_kink = yv_ya[yv_ya["year_act"] < kink_year]
            post_kink = yv_ya[yv_ya["year_act"] >= kink_year]

            # Pre-2060: normal lifetime filtering
            lifetime_condition = (
                (pre_kink["year_act"] - pre_kink["year_vtg"]) <= technical_lifetime
            )
            pre_kink_filtered = pre_kink[lifetime_condition]

            # Post-2060: same-year only
            same_year_condition = post_kink["year_vtg"] == post_kink["year_act"]
            post_kink_same_year = post_kink[same_year_condition]

            result = pd.concat(
                [pre_kink_filtered, post_kink_same_year], ignore_index=True
            )
            return result.reset_index(drop=True)

    # Apply simple lifetime logic: year_act - year_vtg <= technical_lifetime
    condition_values = yv_ya["year_act"] - yv_ya["year_vtg"]
    valid_mask = condition_values <= technical_lifetime
    result = yv_ya[valid_mask].reset_index(drop=True)

    return result
