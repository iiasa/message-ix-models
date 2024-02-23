from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import linregress  # type: ignore

from message_ix_models import Context

from .config import Config


def default_ref_region(node: str, ref_region: Optional[str] = None) -> str:
    """Return a default for the reference region or raise :class:`ValueError`."""
    result = ref_region or {"R11": "R11_NAM", "R12": "R12_NAM", "R20": "R20_NAM"}.get(
        node
    )
    if result is None:
        raise ValueError(f"No ref_region supplied, and no default for {node = }")
    return result


def process_raw_ssp_data(
    context: Context, ref_region: Optional[str] = None, *, node: Optional[str] = None
) -> pd.DataFrame:
    """Equivalent to :func:`.process_raw_ssp_data`, using :mod:`.exo_data`."""
    from collections import defaultdict

    import xarray as xr
    from genno import Computer, Key, Quantity, quote

    from message_ix_models.project.ssp.data import SSPUpdate  # noqa: F401
    from message_ix_models.tools.exo_data import prepare_computer

    # Set default reference region
    ref_region = default_ref_region(context.model.regions, ref_region)

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

    # print(c.describe(k_result))  # Debug
    return c.get(k_result)


# Function to calculate adjusted region-differentiated cost ratios
def adjust_cost_ratios_with_gdp(region_diff_df, config: Config):
    """Calculate adjusted region-differentiated cost ratios

    This function takes in a dataframe with region-differentiated \
    cost ratios and calculates adjusted region-differentiated cost ratios \
    using GDP per capita data.

    Parameters
    ----------
    region_diff_df : pandas.DataFrame
        Output of :func:`apply_regional_differentation`.
    node : str
        Node/region to aggregate to.
    ref_region : str
        Reference region to use.
    scenario : str
        Scenario to use.
    scenario_version : str
        Scenario version to use.
    base_year : int
        Base year to use.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - scenario_version: scenario version
        - scenario: SSP scenario
        - message_technology: message technology
        - region: R11, R12, or R20 region
        - year
        - gdp_ratio_reg_to_reference: ratio of GDP per capita \
            in respective region to GDP per capita in reference region
        - reg_cost_ratio_adj: adjusted region-differentiated cost ratio
    """
    from .projections import _maybe_query_scenario, _maybe_query_scenario_version

    context = Context.get_instance(-1)
    context.model.regions = config.node

    df_gdp = (
        process_raw_ssp_data(context=context, ref_region=config.ref_region)
        .query("year >= 2020")
        .drop(columns=["total_gdp", "total_population"])
        .assign(
            scenario_version=lambda x: np.where(
                x.scenario_version.str.contains("2013"),
                "Previous (2013)",
                "Review (2023)",
            )
        )
    )
    df_cost_ratios = region_diff_df.copy()

    # If base year does not exist in GDP data, then use earliest year in GDP data
    # and give warning
    base_year = int(config.base_year)
    if int(base_year) not in df_gdp.year.unique():
        base_year = int(min(df_gdp.year.unique()))
        print("......(Using year " + str(base_year) + " data from GDP.)")

    # Set default values for input arguments

    # Filter for scenarios and scenario versions
    df_gdp = df_gdp.pipe(_maybe_query_scenario, config).pipe(
        _maybe_query_scenario_version, config
    )

    gdp_base_year = df_gdp.query("year == @base_year").reindex(
        ["scenario_version", "scenario", "region", "gdp_ratio_reg_to_reference"], axis=1
    )

    df_gdp_cost = pd.merge(gdp_base_year, df_cost_ratios, on=["region"])

    dfs = [
        x
        for _, x in df_gdp_cost.groupby(
            ["scenario_version", "scenario", "message_technology", "region"]
        )
    ]

    def indiv_regress_tech_cost_ratio_vs_gdp_ratio(df):
        if df.iloc[0].region == config.ref_region:
            df_one = (
                df.copy()
                .assign(
                    slope=np.NaN,
                    intercept=np.NaN,
                    rvalue=np.NaN,
                    pvalue=np.NaN,
                    stderr=np.NaN,
                )
                .reindex(
                    [
                        "scenario_version",
                        "scenario",
                        "message_technology",
                        "region",
                        "slope",
                        "intercept",
                        "rvalue",
                        "pvalue",
                        "stderr",
                    ],
                    axis=1,
                )
            )
        else:
            df_one = (
                df.copy()
                .assign(gdp_ratio_reg_to_reference=1, reg_cost_ratio=1)
                ._append(df)
                .reset_index(drop=1)
                .groupby(
                    ["scenario_version", "scenario", "message_technology", "region"]
                )
                .apply(
                    lambda x: pd.Series(
                        linregress(x["gdp_ratio_reg_to_reference"], x["reg_cost_ratio"])
                    )
                )
                .rename(
                    columns={
                        0: "slope",
                        1: "intercept",
                        2: "rvalue",
                        3: "pvalue",
                        4: "stderr",
                    }
                )
                .reset_index()
            )

        return df_one

    out_reg = pd.Series(dfs).apply(indiv_regress_tech_cost_ratio_vs_gdp_ratio)
    l_reg = [x for x in out_reg]
    df_reg = pd.concat(l_reg).reset_index(drop=1)

    df = (
        df_gdp.merge(df_reg, on=["scenario_version", "scenario", "region"], how="left")
        .drop(
            columns=[
                "rvalue",
                "pvalue",
                "stderr",
            ]
        )
        .query("year >= @base_year")
        .assign(
            reg_cost_ratio_adj=lambda x: np.where(
                x.region == config.ref_region,
                1,
                x.slope * x.gdp_ratio_reg_to_reference + x.intercept,
            ),
            year=lambda x: x.year.astype(int),
        )
        .reindex(
            [
                "scenario_version",
                "scenario",
                "message_technology",
                "region",
                "year",
                "gdp_ratio_reg_to_reference",
                "reg_cost_ratio_adj",
            ],
            axis=1,
        )
    )

    negative_slopes = df.query(
        "year == 2020 and gdp_ratio_reg_to_reference < 1 and reg_cost_ratio_adj > 1"
    )

    un_ratios = (
        negative_slopes.reindex(
            [
                "scenario_version",
                "scenario",
                "message_technology",
                "region",
                "reg_cost_ratio_adj",
            ],
            axis=1,
        )
        .drop_duplicates()
        .rename(columns={"reg_cost_ratio_adj": "reg_cost_ratio_2020"})
        .assign(constrain="yes")
    )

    df = df.merge(
        un_ratios,
        on=["scenario_version", "scenario", "message_technology", "region"],
        how="left",
    ).fillna({"constrain": "no"})

    # For cases that need to be constrained,
    # if the adjusted cost ratio goes above the 2020 cost ratio,
    # then set the adjusted cost ratio to be equal to the 2020 cost ratio
    df = df.assign(
        reg_cost_ratio_adj=lambda x: np.where(
            (x.constrain == "yes") & (x.reg_cost_ratio_adj > x.reg_cost_ratio_2020),
            x.reg_cost_ratio_2020,
            x.reg_cost_ratio_adj,
        )
    ).drop(columns=["reg_cost_ratio_2020", "constrain"])

    return df
