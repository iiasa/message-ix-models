# -*- coding: utf-8 -*-
"""
Add explicit re-exports
"""
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

# Candidate technologies to use as parameter templates for the balancing technology.
# Tried in order; first match wins.
_BAL_TEMPLATE_CANDIDATES = ['coal_bal', 'gas_bal', 'biomass_bal', 'oil_bal', 'loil_bal', 'foil_bal']


def _build_bal_params_from_structure(dom_prod_base, trade_commodity, base_level):
    """
    Build input, output, capacity_factor, and technical_lifetime DataFrames for
    a balancing technology directly from the domestic production output structure.

    Used when no template balancing technology exists in the scenario.
    The balancing technology has a 1-year lifetime (year_vtg == year_act).
    """
    import pandas as pd

    bal_tec = trade_commodity + '_bal'

    # Derive node/year/time scope from domestic production; lifetime=1 so vtg==act
    base = (
        dom_prod_base[['node_loc', 'year_act', 'time']]
        .drop_duplicates()
        .assign(year_vtg=lambda df: df['year_act'])
    )

    fb_input = base.assign(
        node_origin=base['node_loc'],
        technology=bal_tec,
        commodity=trade_commodity,
        level='primary',
        mode='M1',
        time_origin=base['time'],
        value=1.0,
        unit='-',
    )

    fb_output = base.assign(
        node_dest=base['node_loc'],
        technology=bal_tec,
        commodity=trade_commodity,
        level=base_level,
        mode='M1',
        time_dest=base['time'],
        value=1.0,
        unit='-',
    )

    fb_cap = base[['node_loc', 'year_vtg', 'year_act', 'time']].assign(
        technology=bal_tec,
        value=1.0,
        unit='%',
    )

    fb_lt = (
        base[['node_loc', 'year_vtg']]
        .drop_duplicates()
        .assign(technology=bal_tec, value=1, unit='y')
    )

    return fb_input, fb_output, fb_cap, fb_lt


def adjust_reexports(base_scenario,
                     trade_commodity_list: list,
                     base_level: str):

    existing_tecs = list(base_scenario.set('technology'))

    for trade_commodity in trade_commodity_list:

        # Output from domestic production should have updated level
        dom_prod = base_scenario.par('output', filters={'commodity': trade_commodity,
                                                        'level': base_level})
        dom_prod = dom_prod[~dom_prod['technology'].str.contains('_imp')]

        if dom_prod.empty:
            continue

        dom_prod_base = dom_prod.copy()
        dom_prod['level'] = 'primary'

        bal_tec = trade_commodity + '_bal'

        # Find the first available template balancing technology
        template_tec = next(
            (t for t in _BAL_TEMPLATE_CANDIDATES if t in existing_tecs),
            None,
        )

        fb_lt = None  # only needed in the fallback path

        if template_tec is not None:
            # Template path: clone parameter structure from existing bal technology
            fb_input = base_scenario.par('input', filters={'technology': template_tec}).copy()
            fb_output = base_scenario.par('output', filters={'technology': template_tec}).copy()
            fb_cap = base_scenario.par('capacity_factor', filters={'technology': template_tec}).copy()

            fb_input['commodity'] = trade_commodity
            fb_input['level'] = 'primary'
            fb_input['technology'] = bal_tec
            fb_input['mode'] = 'M1'

            fb_output['commodity'] = trade_commodity
            fb_output['level'] = base_level
            fb_output['technology'] = bal_tec
            fb_output['mode'] = 'M1'

            fb_cap['technology'] = bal_tec
        else:
            # Fallback path: build parameters from domestic production structure
            fb_input, fb_output, fb_cap, fb_lt = _build_bal_params_from_structure(
                dom_prod_base, trade_commodity, base_level
            )

        # Update fuel export input to draw from primary level
        export_input = base_scenario.par('input', filters={'commodity': trade_commodity,
                                                           'level': base_level})
        export_input = export_input[export_input['technology'].str.contains('_exp')].copy()
        export_input_base = export_input.copy()
        export_input['level'] = 'primary'

        with base_scenario.transact(f"Add {bal_tec} to technology set"):
            base_scenario.add_set('technology', bal_tec)

        with base_scenario.transact("Update output from domestic production to primary level"):
            base_scenario.remove_par('output', dom_prod_base)
            base_scenario.add_par('output', dom_prod)

        with base_scenario.transact(f"Add {bal_tec} parameters"):
            base_scenario.add_par('input', fb_input)
            base_scenario.add_par('output', fb_output)
            base_scenario.add_par('capacity_factor', fb_cap)
            if fb_lt is not None:
                base_scenario.add_par('technical_lifetime', fb_lt)

        with base_scenario.transact("Update export input to primary level"):
            base_scenario.remove_par('input', export_input_base)
            base_scenario.add_par('input', export_input)



