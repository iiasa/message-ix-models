import logging
from collections import defaultdict
from functools import lru_cache
from itertools import product
from typing import TYPE_CHECKING
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

#: Configuration files.
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


def read_config(context: Context | None = None):
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
    info: "ScenarioInfo",
    technical_lifetime: int | None = None,
    same_year_only: bool = False,
) -> pd.DataFrame:
    """Get valid vintage and active year combinations.

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
    # 10-year intervals post-2060. Short lifetimes don't benefit from
    # advance construction.
    kink_year = 2060

    has_post_kink = (yv_ya["year_act"] >= kink_year).any()
    short_lived = technical_lifetime <= 5
    medium_lived = technical_lifetime <= 10 and has_post_kink

    if short_lived or medium_lived:
        # Pre-2060: use same-year if lifetime <= 5
        # Post-2060: use same-year if lifetime <= 10
        if short_lived:
            # Same-year for entire horizon
            same_year_mask = yv_ya["year_vtg"] == yv_ya["year_act"]
            return yv_ya[same_year_mask].reset_index(drop=True)
        else:
            # Same-year only for post-2060, normal logic for pre-2060
            pre_kink = yv_ya[yv_ya["year_act"] < kink_year]
            post_kink = yv_ya[yv_ya["year_act"] >= kink_year]

            # Pre-2060: normal lifetime filtering
            age = pre_kink["year_act"] - pre_kink["year_vtg"]
            pre_kink_filtered = pre_kink[age <= technical_lifetime]

            # Post-2060: same-year only
            same_yr = post_kink["year_vtg"] == post_kink["year_act"]
            post_kink_same_year = post_kink[same_yr]

            result = pd.concat(
                [pre_kink_filtered, post_kink_same_year], ignore_index=True
            )
            return result.reset_index(drop=True)

    # Apply simple lifetime logic: year_act - year_vtg <= technical_lifetime
    condition_values = yv_ya["year_act"] - yv_ya["year_vtg"]
    valid_mask = condition_values <= technical_lifetime
    result = yv_ya[valid_mask].reset_index(drop=True)

    return result
