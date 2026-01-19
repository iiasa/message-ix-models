import logging

from message_ix_models import ScenarioInfo
from message_ix import make_df
import pandas as pd

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
    regions = set(info.N) - {'World', 'R12_GLB'}
    model_years = [y for y in info.Y if y > 2025]

    # Read base year prices
    price_var = scenario.var("PRICE_EMISSION")
    base_prices = price_var.loc[price_var.year == start_year, ['node', 'lvl']].copy()
    base_prices['year'] = start_year
    
    # Add missing regions with zero price
    missing_regions = regions - set(base_prices.node)
    if missing_regions:
        missing_df = pd.DataFrame({
            'node': list(missing_regions),
            'lvl': 0,
            'year': start_year
        })
        base_prices = pd.concat([base_prices, missing_df], ignore_index=True)

    # Prepare interpolation points: start_year, 2100, 2110
    interpolation_data = [
        base_prices,
        pd.DataFrame({'node': list(regions), 'lvl': price_2100, 'year': 2100}),
        pd.DataFrame({'node': list(regions), 'lvl': price_2100, 'year': 2110})
    ]
    
    # Combine and pivot for interpolation
    price_df = pd.concat(interpolation_data, ignore_index=True)
    price_pivot = price_df.pivot_table(
        values='lvl', 
        index='year', 
        columns='node'
    )
    
    # Interpolate for all model years
    all_years = sorted(set(price_pivot.index) | set(model_years))
    price_interp = (
        price_pivot
        .reindex(all_years)
        .sort_index()
        .interpolate(method='index')
        .loc[model_years]  # Keep only model years
    )
    
    # Convert back to long format
    price_long = (
        price_interp
        .reset_index()
        .melt(id_vars='year', var_name='node', value_name='lvl')
    )
       
    # Create tax_emission parameter dataframe
    return make_df(
        'tax_emission',
        node=price_long['node'],
        type_emission="TCE",
        type_tec="all",
        type_year=price_long['year'],
        unit="USD/tC",
        value=price_long['lvl'],
    )