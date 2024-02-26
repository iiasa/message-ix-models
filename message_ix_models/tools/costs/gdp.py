import logging

import numpy as np
import pandas as pd

from message_ix_models import Context

from .config import Config

log = logging.getLogger(__name__)


def process_raw_ssp_data(context: Context, config: Config) -> pd.DataFrame:
    """Retrieve SSP data as required for :mod:`.tools.costs`.

    This method uses :class:`.SSPOriginal` and :class:`.SSPUpdate` via
    :func:`.exo_data.prepare_computer`

    Returns
    -------
    pandas.DataFrame
        with the columns:

        - scenario_version
        - scenario
        - region
        - year
        - total_gdp
        - total_population
        - gdp_ppp_per_capita
        - gdp_ratio_reg_to_reference
    """
    from collections import defaultdict

    import xarray as xr
    from genno import Computer, Key, Quantity, quote

    from message_ix_models.project.ssp.data import SSPUpdate  # noqa: F401
    from message_ix_models.tools.exo_data import prepare_computer

    # Computer to hold computations
    c = Computer()

    # Common dimensions
    dims = ("n", "y", "scenario")

    def broadcast_qty(s) -> Quantity:
        """Return a quantity with a "scenario" dimension with the single label `s`.

        Multiplying this by any other quantity adds the "scenario" dimension."""
        return Quantity(xr.DataArray([1.0], coords={"scenario": [s]}))

    c.add("LED:scenario", broadcast_qty("LED"))

    # Keys prepared in the loop
    keys = defaultdict(list)
    for n in "12345":
        # Source/scenario identifier
        ssp = f"ICONICS:SSP(2024).{n}"

        # Add a quantity for broadcasting
        c.add(f"SSP{n}:scenario", broadcast_qty(f"SSP{n}"))

        # Both population and GDP data
        for source_kw in (
            dict(measure="POP", model="IIASA-WiC POP 2023", name=f"_pop {n}"),
            dict(measure="GDP", model="OECD ENV-Growth 2023", name=f"_gdp {n}"),
        ):
            m = source_kw["measure"].lower()

            # Add tasks to `c` that retrieve and (partly) process data from the database
            key, *_ = prepare_computer(context, c, ssp, source_kw, strict=False)

            # Add a "scenario" dimension
            for label in [f"SSP{n}"] + (["LED"] if n == "2" else []):
                keys[m].append(c.add(f"{m} {label}", "mul", key, f"{label}:scenario"))

    # Concatenate single-scenario data
    k_pop = Key("pop", dims)
    c.add(k_pop, "concat", *keys["pop"])
    k_gdp = Key("gdp", dims)
    c.add(k_gdp, "concat", *keys["gdp"])

    # Further calculations

    # GDP per capita
    k_gdp_cap = k_gdp + "cap"
    c.add(k_gdp_cap, "div", k_gdp, k_pop)

    # Ratio to reference region value
    c.add(k_gdp_cap + "indexed", "index_to", k_gdp_cap, quote("n"), quote(ref_region))

    def merge(pop, gdp, gdp_cap, gdp_cap_indexed) -> pd.DataFrame:
        """Merge data to a single data frame with the expected format."""
        return (
            pd.concat(
                [
                    pop.to_series().rename("total_gdp"),
                    gdp.to_series().rename("total_population"),
                    gdp_cap.to_series().rename("gdp_ppp_per_capita"),
                    gdp_cap_indexed.to_series().rename("gdp_ratio_reg_to_reference"),
                ],
                axis=1,
            )
            .reset_index()
            .rename(columns={"n": "region", "y": "year"})
            .sort_values(by=["scenario", "region", "year"])
            .assign(scenario_version="2023")
        )

    k_result = "data::pyam"
    c.add(k_result, merge, k_pop, k_gdp, k_gdp_cap, k_gdp_cap + "indexed")

    # log.debug(c.describe(k_result))  # Debug
    return c.get(k_result)


def adjust_cost_ratios_with_gdp(region_diff_df, config: Config):
    """Calculate adjusted region-differentiated cost ratios.

    This function takes in a data frame with region-differentiated cost ratios and
    calculates adjusted region-differentiated cost ratios using GDP per capita data.

    Parameters
    ----------
    region_diff_df : pandas.DataFrame
        Output of :func:`apply_regional_differentiation`.
    config : .Config
        The function responds to, or passes on to other functions, the fields:
        :attr:`~.Config.base_year`,
        :attr:`~.Config.node`,
        :attr:`~.Config.ref_region`,
        :attr:`~.Config.scenario`, and
        :attr:`~.Config.scenario_version`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:

        - scenario_version: scenario version
        - scenario: SSP scenario
        - message_technology: message technology
        - region: R11, R12, or R20 region
        - year
        - gdp_ratio_reg_to_reference: ratio of GDP per capita in respective region to
          GDP per capita in reference region.
        - reg_cost_ratio_adj: adjusted region-differentiated cost ratio
    """
    from .projections import _maybe_query_scenario, _maybe_query_scenario_version

    context = Context.get_instance(-1)
    context.model.regions = config.node

    # - Retrieve GDP from SSP databases and compute and ratios (per capita; versus
    #   ref_region.
    # - Keep only data from y₀ onwards.
    # - Map "scenario_version" strings to the desired output.
    # - Set the dtype of the "year" column.
    # - Filter on config.scenario and config.scenario_version, if configured.
    df_gdp = (
        process_raw_ssp_data(context, config)
        .query("year >= @config.y0")
        .drop(columns=["total_gdp", "total_population"])
        .assign(
            scenario_version=lambda x: np.where(
                x.scenario_version.str.contains("2013"),
                "Previous (2013)",
                "Review (2023)",
            )
        )
        .astype({"year": int})
        .pipe(_maybe_query_scenario, config)
        .pipe(_maybe_query_scenario_version, config)
    )

    # If base year does not exist in GDP data, then use earliest year in GDP data and
    # give warning
    base_year = config.base_year
    if base_year not in df_gdp.year.unique():
        new_base_year = min(df_gdp.year.unique())
        log.warning(f"Use year={new_base_year} GDP data as proxy for {base_year}")
        base_year = new_base_year

    def _constrain_cost_ratio(df: pd.DataFrame, base_year):
        """Constrain "reg_cost_ratio_adj".

        In cases where gdp_ratio_reg_to_reference is < 1 and reg_cost_ratio_adj > 1 in
        the base period, ensure reg_cost_ratio_adj(y) <= reg_cost_ratio_adj(base_year)
        for all future periods y.
        """
        ref = df.query("year == @base_year").iloc[0]
        if ref.gdp_ratio_reg_to_reference < 1 and ref.reg_cost_ratio_adj > 1:
            return df.assign(
                reg_cost_ratio_adj=df.reg_cost_ratio_adj.clip(
                    upper=ref.reg_cost_ratio_adj
                )
            )
        else:
            return df

    #  1. Select base-year GDP data for "gdp_ratio_reg_to_reference".
    #  2. Drop "year".
    #  3. Merge `df_region_diff` for "reg_cost_ratio".
    #  4. Compute slope.
    #  5. Compute intercept.
    #  6. Drop "gdp_ratio_reg_to_reference"—because of (1–2), this is the base period
    #     value only.
    #  7. Merge `df_gdp` again to re-adds "year" and "gdp_ratio_reg_to_reference" with
    #     distinct values for each period.
    #  8. Compute ref_cost_ratio_adj
    #  9. Fill 1.0 where NaNs occur in (8), i.e. for the reference region.
    # 10. Group by (sv, s, r, t) and apply _constrain_cost_ratio(), above, to each
    #     group.
    # 11. Select the desired columns.
    return (
        df_gdp.query("year == @base_year")
        .drop("year", axis=1)
        .merge(region_diff_df, on=["region"])
        .eval("slope = (reg_cost_ratio - 1) / (gdp_ratio_reg_to_reference - 1)")
        .eval("intercept = 1 - slope")
        .drop("gdp_ratio_reg_to_reference", axis=1)
        .merge(df_gdp, on=["scenario_version", "scenario", "region"], how="right")
        .eval("reg_cost_ratio_adj = slope * gdp_ratio_reg_to_reference + intercept")
        .fillna({"reg_cost_ratio_adj": 1.0})
        .groupby(
            ["scenario_version", "scenario", "region", "message_technology"],
            group_keys=False,
        )
        .apply(_constrain_cost_ratio, base_year)[
            [
                "scenario_version",
                "scenario",
                "message_technology",
                "region",
                "year",
                "gdp_ratio_reg_to_reference",
                "reg_cost_ratio_adj",
            ]
        ]
    )
