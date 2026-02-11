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
from message_ix_models.util import load_package_data, package_data_path

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


def filter_basins_by_region(
    df_basins: pd.DataFrame,
    context: Context | None = None,
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
    reduced_basin = getattr(context, "reduced_basin", False)

    if not reduced_basin:
        # No filtering, return original dataframe
        log.info("Basin filtering disabled, returning all basins")
        return df_basins

    # Basin filtering is enabled
    filter_list = getattr(context, "filter_list", None)
    num_basins = getattr(context, "num_basins", None)

    if filter_list:
        # Filter to specific basin list
        filtered = df_basins[df_basins["BCU_name"].isin(filter_list)]

        # Check if we have at least 1 basin per R12 region
        all_regions = set(df_basins["REGION"].unique())
        filtered_regions = set(filtered["REGION"].unique())
        missing_regions = all_regions - filtered_regions

        if missing_regions:
            log.info(f"Adding one basin per missing region: {missing_regions}")
            # Add one basin from each missing region
            for region in missing_regions:
                region_basins = df_basins[df_basins["REGION"] == region]
                # Add the first basin from this region
                filtered = pd.concat(
                    [filtered, region_basins.head(1)], ignore_index=True
                )

        log.info(
            f"Filtered basins from {len(df_basins)} to {len(filtered)} "
            f"using custom filter list: {filter_list} (with 1 basin per missing region)"
        )

        return filtered.reset_index(drop=True)

    # Check for stress-based selection mode
    basin_selection = getattr(context, "basin_selection", "first_k")

    if basin_selection == "stress":
        n = num_basins if num_basins is not None else n_per_region
        ssp = getattr(context, "ssp", "SSP2")
        stress_df = compute_basin_demand_ratio(context.regions, ssp=ssp)
        selected = _select_by_stress(stress_df, n_per_region=n)

        filtered = df_basins[df_basins["BCU_name"].isin(selected)].reset_index(
            drop=True
        )
        log.info(
            f"Stress-based selection: {len(df_basins)} -> {len(filtered)} basins "
            f"(n_per_region={n})"
        )
        return filtered

    if num_basins is not None:
        n_per_region = num_basins

    # Group by region and take first n rows from each group
    if "REGION" not in df_basins.columns:
        log.info("REGION column not found, cannot filter by region")
        return df_basins

    filtered = (
        df_basins.groupby("REGION", group_keys=False)
        .apply(lambda x: x.head(n_per_region))
        .reset_index(drop=True)
    )

    log.info(
        f"Filtered basins from {len(df_basins)} to {len(filtered)} "
        f"(keeping first {n_per_region} per region)"
    )

    return filtered


def compute_basin_demand_ratio(
    regions: str = "R12",
    ssp: str = "SSP2",
    demand_year: int = 2050,
) -> pd.DataFrame:
    """Compute basin-level demand/supply ratio from pre-build CSV data.

    Demand = urban + rural + manufacturing withdrawals (MCM/year).
    Supply = (surface water + groundwater recharge) mean across years (km3 -> MCM).

    Parameters
    ----------
    regions : str
        Region codelist (e.g. "R12").
    ssp : str
        SSP scenario for demand file naming.
    demand_year : int
        Year to use for demand values (later years show higher stress).

    Returns
    -------
    pd.DataFrame
        Columns: BCU_name, REGION, supply_mcm, demand_mcm, demand_ratio.
    """
    ssp_label = ssp.lower().replace("ssp", "ssp")  # SSP2 -> ssp2

    basins = pd.read_csv(
        package_data_path(
            "water", "delineation", f"basins_by_region_simpl_{regions}.csv"
        )
    )

    # Supply: surface + groundwater, mean across year columns, km3 -> MCM
    qtot = pd.read_csv(
        package_data_path(
            "water", "availability", f"qtot_5y_no_climate_low_{regions}.csv"
        )
    ).drop(columns=["Unnamed: 0"], errors="ignore")
    qr = pd.read_csv(
        package_data_path(
            "water", "availability", f"qr_5y_no_climate_low_{regions}.csv"
        )
    ).drop(columns=["Unnamed: 0"], errors="ignore")
    supply_mcm = (qtot.mean(axis=1) + qr.mean(axis=1)) * KM3_TO_MCM

    # Demand: urban + rural + manufacturing withdrawals at demand_year
    demand_path = package_data_path("water", "demands", "harmonized", regions)
    demand_files = [
        f"{ssp_label}_regional_urban_withdrawal2_baseline.csv",
        f"{ssp_label}_regional_rural_withdrawal_baseline.csv",
        f"{ssp_label}_regional_manufacturing_withdrawal_baseline.csv",
    ]

    total_demand = pd.Series(0.0, index=basins["BCU_name"].astype(str))
    for fname in demand_files:
        df = pd.read_csv(demand_path / fname)
        row = df[df.iloc[:, 0] == demand_year]
        if row.empty:
            log.warning(f"Year {demand_year} not found in {fname}")
            continue
        vals = row.iloc[0, 1:].astype(float)
        # Align by basin name
        for bcu in total_demand.index:
            if bcu in vals.index:
                total_demand[bcu] += vals[bcu]

    result = pd.DataFrame(
        {
            "BCU_name": basins["BCU_name"],
            "REGION": basins["REGION"],
            "supply_mcm": supply_mcm.values,
        }
    )
    result["demand_mcm"] = result["BCU_name"].astype(str).map(total_demand).fillna(0.0)
    safe_supply = result["supply_mcm"].replace(0, float("inf"))
    result["demand_ratio"] = result["demand_mcm"] / safe_supply

    return result


def _diversity_select(group_sorted: pd.DataFrame, n_per_region: int) -> set[str]:
    """Select basins spanning a range via evenly spaced quantile positions.

    Parameters
    ----------
    group_sorted : pd.DataFrame
        Single-region subset, pre-sorted by the target metric.
    n_per_region : int
        Target number of basins.

    Returns
    -------
    set[str]
        Selected BCU_name values.
    """
    n = len(group_sorted)
    if n <= n_per_region:
        return set(group_sorted["BCU_name"])
    if n_per_region == 1:
        return {group_sorted.iloc[n // 2]["BCU_name"]}
    if n_per_region == 2:
        return {
            group_sorted.iloc[0]["BCU_name"],
            group_sorted.iloc[-1]["BCU_name"],
        }
    positions = [i / (n_per_region - 1) for i in range(n_per_region)]
    indices = {int(round(p * (n - 1))) for p in positions}
    return {group_sorted.iloc[i]["BCU_name"] for i in indices}


def _select_by_stress(
    stress_df: pd.DataFrame,
    n_per_region: int = 3,
) -> set[str]:
    """Select basins spanning the demand/supply ratio range per region.

    Ensures the reduced model includes basins across the stress spectrum:
    low-stress (demand << supply) through high-stress (demand ~ supply).

    Parameters
    ----------
    stress_df : pd.DataFrame
        Output of compute_basin_demand_ratio().
    n_per_region : int
        Target number of basins per region.
    """
    selected: set[str] = set()

    for region, group in stress_df.groupby("REGION"):
        group_sorted = group.sort_values("demand_ratio").reset_index(drop=True)
        basins = _diversity_select(group_sorted, n_per_region)
        selected.update(basins)
        log.info(f"{region}: {len(basins)} basins selected")

    return selected


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
