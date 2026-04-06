import logging

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo

log = logging.getLogger(__name__)


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
