import logging

from message_ix.reporting import Reporter
import numpy as np
import xarray as xr

from .utils import ScenarioInfo, config


log = logging.getLogger(__name__)


def main(scenario):
    """Callback for :meth:`ixmp.Scenario.solve`."""
    log.info('Executing callback on {!r}'.format(scenario))

    info = ScenarioInfo(scenario)

    if not info.is_message_macro:
        log.info('Not a MESSAGE-MACRO scenario; cannot iterate.')
        return True

    ds = xr.Dataset()

    rep = Reporter.from_scenario(scenario)

    # Add coordinates from the MESSAGE Scenario
    for name in ('node', ):
        key = MESSAGE_sets[name]  # FIXME use config['set']
        ds[key] = (key, scenario.set(name))
        ds = ds.set_coords(key)

    # Add transport modes coordinate
    ds['m'] = ('m', config['modes'])
    ds = ds.set_coords('m')

    # Add data from configuration: mer_to_ppp
    ds = ds.update(config['data'])

    # Load from scenario output
    ds['price'] = rep.get('PRICE_COMMODITY')
    ds['gdp'] = rep.get('GDP')

    # Shorthands for indexing
    y0 = dict(y=ds['y'].iloc[0])
    yC = dict(y=ds['time_convergence'])

    # PPP GDP per capita
    ds['gdp_ppp_pc'] = ds['gdp_macro'] * ds['mer_to_ppp'] / ds['pop'] * 1000

    # Value of time multiplier.
    # A value of 1 means the VoT is equal to the wage rate per hour.
    ds['votm'] = 1 / (1 + np.exp((30000 - ds['gdp_pp_pc'] * 1000) / 20000))

    # “Smoothing of prices to avoid zig-zag in share projections”
    ds['price smooth'] = (ds['price'].shift(y=-1) + 2 * ds['price'] +
                          ds['price'].shift(y=1)) / 4
    # First period
    ds['price smooth'].loc[y0] = (
        2 * ds['price'].loc[y0] + 2 * ds['price'].sel(y='2010')
        + ds['price'].sel(y='2020')) / 5
    # Final period. “closer to the trend line”
    ds['price smooth'].loc[dict(y=2100)] = (
        ds['price smooth'].sel(y=2090)
        + (ds['price smooth'].sel(y=2090) - ds['price smooth'].sel(y=2080))
        + ds['price'].sel(y=2100)) / 2

    print(ds['price smooth'].sel(c='transport').to_series().dropna())

    # # Costs “[k$ / hours / km * hours] => *1e6 => [$ / thousand km]”
    # ds['cost'] = ds['price smooth'] + (
    #     ds['gdp_ppp_pc'] / ds['whours'] / ds['speeds'] * 1e6 * ds['votm'])

    # Share weights
    ds['sweight'] = (('n', 'y', 'm'), np.zeros([len(ds[d]) for d in 'nym']))

    # Base year share weights: set first mode's weight to 1
    m0 = ds['m'].iloc[0]
    ds['sweight'].loc[dict(y=2005, m=m0)] = 1
    # Weights for other modes in relative terms
    ds['sweight'].loc[y0] = (
        ds['share'].loc[y0] / (ds['cost'].loc[y0] ** ds['lambda'])
        * (ds['cost'].sel(y=2005, m=m0) ** ds['lambda'])
        / ds['shares'].sel(y=2005, m=m0))
    ds['sweight'].loc[y0] = ds['sweight'].loc[y0].fillna(0)
    # Normalize
    ds['sweight'].loc[y0] = (ds['sweight'].loc[y0] /
                             ds['sweight'].loc[y0].sum('m'))

    # Share weights at time_convergence
    ds['endweights'] = xr.zeros_like(ds['sweight'].loc[y0])
    convergence_groups = {
        'A': ['FSU', 'NAM'],
        'B': ['AFR', 'CPA', 'EEU', 'LAM', 'PAO', 'PAS', 'SAS', 'WEU'],
        'C': ['MEA'],
        }
    for group, regions in convergence_groups.items():
        if group == 'A':
            # Ratio between region's GDP and NAM
            scale = (ds['gdp_ppp_pc'].loc[yC] /
                     ds['gdp_ppp_pc'].sel(n='NAM', **yC))
            ew = (ds['sweight'].sel(n='NAM', **y0) * scale
                  + ds['sweight'].loc[y0] * (1 - scale))
        elif group == 'B':
            # Ratio between region's GDP and PAO
            scale = (ds['gdp_ppp_pc'].loc[yC] /
                     ds['gdp_ppp_pc'].sel(n='PAO', **yC))
            ew = (0.5 * ds['sweight'].sel(n='PAO', **y0) * scale
                  + 0.5 * ds['sweight'].sel(n='WEU', **y0) * scale
                  + ds['sweight'].loc[y0] * (1 - scale))
        elif group == 'C':
            scale = (ds['gdp_ppp_pc'].loc[yC] /
                     ds['gdp_ppp_pc'].sel(n='NAM', **yC))
            ew = (0.5 * ds['sweight'].sel(n='NAM', **y0) * scale
                  + 0.5 * ds['sweight'].sel(n='WEU', **y0) * scale
                  + ds['sweight'].loc[y0] * (1 - scale))

        ds['endweight'].loc[dict(n=regions)] = ew

    # “Set 2010 sweight to 2005 value in order not to have rail in 2010, where
    # technologies become available only in 2020”
    ds['sweight'].loc[dict(y='2010')] = ds['sweight'].loc[dict(y=2005)]

    # Linear interpolation
    # for (per in periods){
    #   for (region in region_list){
    #     for (modi in mode_list){
    #       if (per > 2010) {
    #         sweights[region, as.character(per), modi] = (
    #           baseweights[region, modi] * (time_convergence - per) +
    #           endweights[region, modi] * (per - 2010)) /
    #           (time_convergence - 2010)
    #       }
    #     }
    #   }
    # }

    # Convergence criterion. If not True, the model is run again
    converged = True

    return converged
