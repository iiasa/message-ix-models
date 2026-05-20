import logging
from pathlib import Path

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    private_data_path,
    same_node,
)

log = logging.getLogger(__name__)


def interpolate_c_price(scenario, price_2100, start_year=2030):
    """Interpolate carbon prices from start_year to 2110.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario to read base prices from
    price_2100 : float
        Target price for 2100 and 2110
    start_year : int, optional
        Starting year for interpolation (default: 2030)

    Returns
    -------
    pd.DataFrame
        DataFrame formatted for tax_emission parameter
    """

    log.info(
        f"Interpolating carbon prices for {scenario.model}/{scenario.scenario} "
        f"from {start_year} to 2100/2110 (target price: {price_2100} USD/tC)"
    )

    info = ScenarioInfo(scenario)
    regions = set(info.N) - {"World", "R12_GLB"}
    model_years = [y for y in info.Y if y > 2025]

    # Read base year prices
    price_var = scenario.var("PRICE_EMISSION")
    base_prices = price_var.loc[price_var.year == start_year, ["node", "lvl"]].copy()
    base_prices["year"] = start_year

    # Add missing regions with zero price
    missing_regions = regions - set(base_prices.node)
    if missing_regions:
        missing_df = pd.DataFrame(
            {"node": list(missing_regions), "lvl": 0, "year": start_year}
        )
        base_prices = pd.concat([base_prices, missing_df], ignore_index=True)

    # Prepare interpolation points: start_year, 2100, 2110
    interpolation_data = [
        base_prices,
        pd.DataFrame({"node": list(regions), "lvl": price_2100, "year": 2100}),
        pd.DataFrame({"node": list(regions), "lvl": price_2100, "year": 2110}),
    ]

    # Combine and pivot for interpolation
    price_df = pd.concat(interpolation_data, ignore_index=True)
    price_pivot = price_df.pivot_table(values="lvl", index="year", columns="node")

    # Interpolate for all model years
    all_years = sorted(set(price_pivot.index) | set(model_years))
    price_interp = (
        price_pivot.reindex(all_years)
        .sort_index()
        .interpolate(method="index")
        .loc[model_years]  # Keep only model years
    )

    # Convert back to long format
    price_long = price_interp.reset_index().melt(
        id_vars="year", var_name="node", value_name="lvl"
    )

    # Create tax_emission parameter dataframe
    return make_df(
        "tax_emission",
        node=price_long["node"],
        type_emission="TCE",
        type_tec="all",
        type_year=price_long["year"],
        unit="USD/tC",
        value=price_long["lvl"],
    )


def extend_c_price(scenario, start_year=2030):
    """Extend carbon prices from start_year to 2100, keeping base price constant.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario to read base prices from
    start_year : int, optional
        Starting year for price extension (default: 2030)

    Returns
    -------
    pd.DataFrame
        DataFrame formatted for tax_emission parameter
    """

    log.info(
        f"Extending carbon prices for {scenario.model}/{scenario.scenario} "
        f"from {start_year} to 2100 (constant base price)"
    )

    info = ScenarioInfo(scenario)
    regions = set(info.N) - {"World", "R12_GLB"}
    model_years = [y for y in info.Y if y > 2025 and y <= 2100]

    # Read base year prices
    price_var = scenario.var("PRICE_EMISSION")
    base_prices = price_var.loc[price_var.year == start_year, ["node", "lvl"]].copy()
    base_prices["year"] = start_year

    # By, e.g., base_prices.loc[base_prices.node == 'R12_AFR', 'lvl'] *= 1.1
    # One can adjust prices here for specific regions before extending

    # Add missing regions with zero price
    missing_regions = regions - set(base_prices.node)
    if missing_regions:
        missing_df = pd.DataFrame(
            {"node": list(missing_regions), "lvl": 0, "year": start_year}
        )
        base_prices = pd.concat([base_prices, missing_df], ignore_index=True)

    # Extend base price to all model years up to 2100
    price_list = []
    for year in model_years:
        year_prices = base_prices.copy()
        year_prices["year"] = year
        price_list.append(year_prices)

    price_long = pd.concat(price_list, ignore_index=True)

    # Create tax_emission parameter dataframe
    return make_df(
        "tax_emission",
        node=price_long["node"],
        type_emission="TCE",
        type_tec="all",
        type_year=price_long["year"],
        unit="USD/tC",
        value=price_long["lvl"],
    )


# Add additional assumptions (AAS) for specific NGFS scenarios
# AAS1.1, bound CO2 storage activity (mode M3 DAC) from near to mid term
def aas_co2_storage_bound_activity(
    context,
    scenario,
    technologies: list[str] = ["co2_stor"],
    *,
    mode: str = "M3",
    start_year: int = 2025,
    end_year: int = 2060,
    limit: float = 0.0,
):
    """Add ``bound_activity_up`` for CO2 storage technologies."""
    info = ScenarioInfo(scenario)
    years = [y for y in info.Y if start_year <= y <= end_year]

    df = make_df(
        "bound_activity_up",
        technology=technologies,
        mode=mode,
        time="year",
        value=limit,
        unit="???",
    ).pipe(
        broadcast,
        node_loc=nodes_ex_world(info.N),
        year_act=years,
    )

    with scenario.transact(
        f"Add bound_activity_up for CO2 storage technologies (mode {mode})."
    ):
        scenario.add_par("bound_activity_up", df)

    log.info(
        "Added bound_activity_up=%s for technologies %s (mode %s), "
        "year_act %s–%s, all regions (%d nodes, %d periods)",
        limit,
        technologies,
        mode,
        start_year,
        end_year,
        len(nodes_ex_world(info.N)),
        len(years),
    )

    return scenario


# AAS1.2, constraining CO2 storage activity growth from near term
def aas_co2_storage_growth(
    context,
    scenario,
    technologies: list[str] = ["co2_stor"],
    *,
    start_year: int = 2035,
    end_year: int = 2080,
    limit: float = 0.047,
):
    """Add ``growth_activity_up`` for CO2 storage technologies."""
    info = ScenarioInfo(scenario)
    years = [y for y in info.Y if start_year <= y <= end_year]

    df = make_df(
        "growth_activity_up",
        technology=technologies,
        time="year",
        value=limit,
        unit="???",
    ).pipe(
        broadcast,
        node_loc=nodes_ex_world(info.N),
        year_act=years,
    )

    with scenario.transact("Add growth_activity_up for CO2 storage technologies."):
        scenario.add_par("growth_activity_up", df)

    log.info(
        "Added growth_activity_up=%s for technologies %s, "
        "year_act %s–%s, all regions (%d nodes, %d periods)",
        limit,
        technologies,
        start_year,
        end_year,
        len(nodes_ex_world(info.N)),
        len(years),
    )

    return scenario


# AAS1.3, constraining CO2 storage activity growth lo to avoid bumpy trends
def aas_co2_storage_smooth(
    context,
    scenario,
    technologies: list[str] = ["co2_stor"],
    *,
    start_year: int = 2025,
    end_year: int = 2110,
    limit: float = -0.17,
):
    """Add ``growth_activity_lo`` for CO2 storage technologies."""
    info = ScenarioInfo(scenario)
    years = [y for y in info.Y if start_year <= y <= end_year]

    df = make_df(
        "growth_activity_lo",
        technology=technologies,
        time="year",
        value=limit,
        unit="???",
    ).pipe(
        broadcast,
        node_loc=nodes_ex_world(info.N),
        year_act=years,
    )

    with scenario.transact("Add growth_activity_lo for CO2 storage technologies."):
        scenario.add_par("growth_activity_lo", df)

    log.info(
        "Added growth_activity_lo=%s for technologies %s, "
        "year_act %s–%s, all regions (%d nodes, %d periods)",
        limit,
        technologies,
        start_year,
        end_year,
        len(nodes_ex_world(info.N)),
        len(years),
    )

    return scenario


# AAS1.4, put mode shares for CO2 storage (co2_stor M2/M3)
def aas_co2_storage_share_mode(
    context,
    scenario,
    technology: str = "co2_stor",
    *,
    share: str = "co2_stor_aas1",
):
    """Add ``share_mode_up`` for ``co2_stor`` mode shares ``co2_stor_aas1``."""
    m2_limit = 0.52
    m3_limit = 0.14
    m3_limit_near_term = 0.01
    m3_near_start = 2020
    m3_near_end = 2040

    info = ScenarioInfo(scenario)
    nodes = nodes_ex_world(info.N)
    years_all = sorted(int(y) for y in info.set["year"])
    years_m3_near = [y for y in years_all if m3_near_start <= y <= m3_near_end]
    years_m3_other = [y for y in years_all if y not in years_m3_near]

    def _share_mode_up(mode: str, value: float, year_act: list[int]) -> pd.DataFrame:
        return make_df(
            "share_mode_up",
            shares=share,
            technology=technology,
            mode=mode,
            time="year",
            value=value,
            unit="???",
        ).pipe(broadcast, node_share=nodes, year_act=year_act)

    parts = [_share_mode_up("M2", m2_limit, years_all)]
    if years_m3_near:
        parts.append(_share_mode_up("M3", m3_limit_near_term, years_m3_near))
    if years_m3_other:
        parts.append(_share_mode_up("M3", m3_limit, years_m3_other))
    df = pd.concat(parts, ignore_index=True)

    with scenario.transact(f"Add share_mode_up for {share} on {technology}."):
        if share not in scenario.set("shares").tolist():
            scenario.add_set("shares", share)
        scenario.add_par("share_mode_up", df)

    log.info(
        "Added share_mode_up for %s on %s, %d rows",
        share,
        technology,
        len(df),
    )

    return scenario


# AAS2, constraining fossil phaseout speed in the near term
def aas_coal_growth_near_term(
    context,
    scenario,
    technologies: list[str] = [
        "coal_adv",
        # "coal_adv_ccs",
        "coal_ppl",
        # "igcc",
        # "igcc_ccs",
    ],
    *,
    start_year: int = 2020,
    end_year: int = 2035,
    limit: float = 0.03,
):
    """Add ``growth_activity_lo`` for coal power technologies.

    Removes overlapping ``growth_activity_up`` entries for the same technologies,
    nodes, and years before adding the lower bound.
    """
    info = ScenarioInfo(scenario)
    nodes = ["R12_CHN", "R12_SAS"]
    years = [y for y in info.Y if start_year <= y <= end_year]

    growth_up = scenario.par(
        "growth_activity_up",
        filters={
            "technology": technologies,
            "node_loc": nodes,
            "year_act": years,
        },
    )

    df = make_df(
        "growth_activity_lo",
        technology=technologies,
        time="year",
        value=limit,
        unit="???",
    ).pipe(
        broadcast,
        node_loc=nodes,
        year_act=years,
    )

    with scenario.transact("Add growth_activity_lo for coal power technologies."):
        if len(growth_up):
            scenario.remove_par("growth_activity_up", growth_up)
        scenario.add_par("growth_activity_lo", df)

    log.info(
        "Added growth_activity_lo=%s for technologies %s, "
        "year_act %s–%s, nodes R12_CHN and R12_SAS (%d periods); "
        "removed %d growth_activity_up rows",
        limit,
        technologies,
        start_year,
        end_year,
        len(years),
        len(growth_up),
    )

    return scenario


# AAS3, constraining coal power capacity revival after net zero years
def aas_coal_growth_long_term(
    context,
    scenario,
    technologies: list[str] = [
        "coal_adv",
        "coal_adv_ccs",
        "coal_ppl",
        "igcc",
        "igcc_ccs",
    ],
    *,
    start_year: int = 2060,
    end_year: int = 2110,
    limit: float = 0,
):
    """Add ``growth_new_capacity_up`` and ``growth_activity_up`` for coal power.

    Technologies are listed in the ``technologies`` argument.
    """
    info = ScenarioInfo(scenario)
    years = [y for y in info.Y if start_year <= y <= end_year]
    nodes = nodes_ex_world(info.N)

    df_capacity = make_df(
        "growth_new_capacity_up",
        technology=technologies,
        value=limit,
        unit="???",
    ).pipe(broadcast, node_loc=nodes, year_vtg=years)

    df_activity = make_df(
        "growth_activity_up",
        technology=technologies,
        time="year",
        value=limit,
        unit="???",
    ).pipe(broadcast, node_loc=nodes, year_act=years)

    with scenario.transact(
        "Add growth_new_capacity_up and growth_activity_up for coal power technologies."
    ):
        scenario.add_par("growth_new_capacity_up", df_capacity)
        scenario.add_par("growth_activity_up", df_activity)

    log.info(
        "Added growth_new_capacity_up=%s and growth_activity_up=%s "
        "for technologies %s, year_vtg/year_act %s–%s, "
        "all regions (%d nodes, %d periods)",
        limit,
        limit,
        technologies,
        start_year,
        end_year,
        len(nodes),
        len(years),
    )

    return scenario


# AAS4, adding emission factors for transport technologies
# and constraint truck emissions
def _transport_emission_factor_csv_path() -> Path:
    """Path to :file:`emission_factor.csv` for transport technologies.

    Simply derived from fuel inputs times the emission factors below:

    ==========  =========  ============================
    fueltype    converter  unit
    ==========  =========  ============================
    lightoil    2113.49    kt CO2 per GWa lightoil
    gas         1582.58    kt CO2 per GWa gas
    methanol    1020.26    kt CO2 per GWa methanol
    ethanol     1430.94    kt CO2 per GWa ethanol
    coal        2672.66    kt CO2 per GWa coal
    fueloil     2113.49    kt CO2 per GWa fueloil
    ==========  =========  ============================
    """
    return private_data_path("projects", "ngfs", "emission_factor.csv")


# Global ``bound_emission`` trajectory for ``type_emission`` CO2_t_truck (Mt CO2/yr).
_CO2_T_TRUCK_BOUND_EMISSION: tuple[tuple[int, float], ...] = (
    (2030, 1777.41),
    (2035, 1580.00),
    (2040, 1405.00),
    (2045, 1250.00),
    (2050, 1115.00),
    (2055, 1000.00),
    (2060, 920.00),
    (2070, 860.00),
    (2080, 825.00),
    (2090, 800.00),
    (2100, 790.00),
    (2110, 785.00),
)


def _co2_t_truck_bound_emission_df(scenario, node: str = "World") -> pd.DataFrame:
    """Build ``bound_emission`` for global CO2_t_truck cap.

    Trajectory values in :data:`_CO2_T_TRUCK_BOUND_EMISSION` are in Mt CO2/yr.
    """
    info = ScenarioInfo(scenario)
    year_set = {int(y) for y in info.set["year"]}
    trajectory = [(y, v) for y, v in _CO2_T_TRUCK_BOUND_EMISSION if y in year_set]

    years, values = zip(*trajectory)
    return make_df(
        "bound_emission",
        node=node,
        type_emission="CO2_t_truck",
        type_tec="all",
        type_year=years,
        value=values,
        unit="???",  # values are Mt CO2/yr
    )


def aas_bound_emission_transport(context, scenario):
    """Add transport ``emission_factor`` and global CO2_t_truck ``bound_emission``."""
    path = _transport_emission_factor_csv_path()
    raw = pd.read_csv(path)

    # Expected CSV columns; rename to MESSAGE parameter dimensions.
    _expected = {"node_loc", "technology", "mode", "emission", "value"}
    if missing := _expected - set(raw.columns):
        raise ValueError(
            f"{path}: missing columns {sorted(missing)}; expected {sorted(_expected)}"
        )

    emissions = sorted(raw["emission"].unique())
    info = ScenarioInfo(scenario)
    yv_ya = info.yv_ya

    # Match units used elsewhere in the scenario when possible.
    unit = "???"  # Mt CO2 actually
    for tech in raw["technology"].unique():
        sample = scenario.par("emission_factor", filters={"technology": tech})
        if len(sample):
            unit = sample.iloc[0]["unit"]
            break

    df = (
        make_df(
            "emission_factor",
            node_loc=raw["node_loc"],
            technology=raw["technology"],
            mode=raw["mode"],
            emission=raw["emission"],
            value=raw["value"],
            unit=unit,
            time="year",
            time_origin="year",
            time_dest="year",
        )
        .pipe(broadcast, year_vtg=yv_ya.year_vtg, year_act=yv_ya.year_act)
        .pipe(same_node)
    )

    bound_emission_df = _co2_t_truck_bound_emission_df(scenario)

    existing_type_emission = set(scenario.set("type_emission").tolist())
    existing_emission = set(scenario.set("emission").tolist())
    existing_cat = {
        tuple(row)
        for row in scenario.set("cat_emission").itertuples(index=False, name=None)
    }

    with scenario.transact(
        "Add transport emission_factor and CO2_t_truck bound_emission"
    ):
        for type_emission in ("CO2_t", "CO2_t_truck"):
            if type_emission not in existing_type_emission:
                scenario.add_set("type_emission", type_emission)

        for emission in emissions:
            if emission not in existing_emission:
                scenario.add_set("emission", emission)
            cat = ("CO2_t", emission)
            if cat not in existing_cat:
                scenario.add_set("cat_emission", list(cat))

        if "CO2_t_truck" not in existing_emission:
            scenario.add_set("emission", "CO2_t_truck")
        cat_truck = ("CO2_t_truck", "CO2_t_truck")
        if cat_truck not in existing_cat:
            scenario.add_set("cat_emission", list(cat_truck))

        scenario.add_par("emission_factor", df)
        scenario.add_par("bound_emission", bound_emission_df)

    log.info(
        "Added %d emission_factor rows from %s (%d emissions, unit=%s); "
        "added %d bound_emission rows for CO2_t_truck (Mt CO2/yr trajectory)",
        len(df),
        path,
        len(emissions),
        unit,
        len(bound_emission_df),
    )

    return scenario


# AAS5, loosen regional TCE emission bounds for two periods after bottleneck years
# (node, anchor type_year) — edit placeholders as needed
_LOOSE_BOTTLENECK_YEAR_BOUND_EMISS: tuple[tuple[str, int], ...] = (
    ("R12_WEU", 2055),
    ("R12_CHN", 2060),
    ("R12_SAS", 2070),
    ("R12_EEU", 2070),
)


def aas_loose_bottleneck_year_tce(context, scenario):
    info = ScenarioInfo(scenario)
    model_years = sorted(int(y) for y in info.Y)

    bound = scenario.par("bound_emission", filters={"type_emission": "TCE"})
    if bound.empty:
        log.warning(
            "No TCE bound_emission in %s/%s; skipping loose bottleneck bounds",
            scenario.model,
            scenario.scenario,
        )
        return scenario

    new_rows: list[dict] = []
    remove_mask = pd.Series(False, index=bound.index)

    for node, anchor_year in _LOOSE_BOTTLENECK_YEAR_BOUND_EMISS:
        try:
            anchor_idx = model_years.index(anchor_year)
        except ValueError:
            log.warning(
                "Anchor year %s not in model years; skipping %s",
                anchor_year,
                node,
            )
            continue

        target_years = model_years[anchor_idx + 1 : anchor_idx + 3]
        if not target_years:
            log.warning(
                "No following model years after %s for %s",
                anchor_year,
                node,
            )
            continue

        anchor_rows = bound.loc[
            (bound["node"] == node) & (bound["type_year"].astype(int) == anchor_year)
        ]
        if anchor_rows.empty:
            log.warning(
                "No TCE bound_emission at %s, %s; skipping",
                node,
                anchor_year,
            )
            continue

        ref = anchor_rows.iloc[0]
        base_value = float(ref["value"])
        factors = (0.5, 0.25)

        for type_year, factor in zip(target_years, factors):
            new_rows.append(
                {
                    "node": node,
                    "type_emission": "TCE",
                    "type_tec": ref["type_tec"],
                    "type_year": type_year,
                    "unit": ref["unit"],
                    "value": base_value * factor,
                }
            )
            remove_mask |= (
                (bound["node"] == node)
                & (bound["type_year"].astype(int) == type_year)
                & (bound["type_tec"] == ref["type_tec"])
            )

    df_add = pd.DataFrame(new_rows)

    with scenario.transact("Loosen TCE bound_emission after bottleneck years (AAS5)"):
        scenario.add_par("bound_emission", df_add)

    log.info(
        "AAS5: added %d loose bottleneck TCE bound_emission rows for %s/%s",
        len(df_add),
        scenario.model,
        scenario.scenario,
    )

    return scenario
