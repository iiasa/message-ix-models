# -*- coding: utf-8 -*-
"""
This script includes postprocessing scripts and a plotting function

"""
import time
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from message_data.projects.leap_re_nest.script.postprocess import (
    plotdf_sec, plotdf, group, multiply_df
    )


# %% A function for attaching history
def add_history(msgSC, tecs, nodeloc, df2, groupby):
    df1_hist = msgSC.par('historical_activity',
                         {'technology': tecs, 'node_loc': nodeloc}
                         ).rename({'value': 'lvl'}, axis=1)
    df2_hist = df2.groupby(['year_act', 'technology', 'mode', 'node_loc',
                            'commodity', 'time'], as_index=False
                           ).mean().drop(['year_vtg'], axis=1)
    df_hist = multiply_df(df1_hist, 'lvl', df2_hist, 'value')
    df_hist = group(df_hist, ['year_act', groupby], 'product', 0.0, None)
    return df_hist


# A function for making model output ready
def model_output(msgSC, tecs, nodeloc, parname, coms=None):
    df1 = msgSC.var('ACT', {'technology': tecs, 'node_loc': nodeloc})
    df2 = msgSC.par(parname, {'technology': tecs, 'node_loc': nodeloc})
    if coms:
        if isinstance(coms, str):
            coms = [coms]
        df2 = df2.loc[df2['commodity'].isin(coms)]
    df = multiply_df(df1, 'lvl', df2, 'value')
    return df, df2


# %% The main function
def plotter(caseName, msgSC, nodeloc, path, yr_min=2015, yr_max=2050,
            emission_relation=True):
    """
    Plots the model output for key indicators, saves the data of plots in
    an Excel file, and creates a PDF of the figures.

    Parameters
    ----------
    caseName : string
        Scenario name specified by the user, as a name for saving the files.
    msgSC : message_ix.Scenario
        Scenario from which the results of a node to be plotted.
    nodeloc : string
        Node name for which the plots to be generated.
    path : string (path)
        Path for saving the results files.
    yr_min : int, optional
        Smallest year to be plotted. The default is 2015.
    yr_max : int, optional
        Largest year to be plotted. The default is 2050.
    emission_relation : bool, optional
        Reading emission factors from relations. The default is False.
        If False, emission factors will be read from parameter "emission_factor".

    """

    # 1) Initialization and importing required data
    # Prepration for Excel writings
    writer_xls = pd.ExcelWriter(path + '\\plots\\' + caseName + '.xlsx',
                                engine='xlsxwriter')

    # Reading the solution from gdx
    if not msgSC.has_solution():
        print('WARNING: the scenario has no solution, postprocessing is '
              'reading the solution form GDX!')

    # Unit conversion
    unitc = 8.76 * 3.6      # Conversion from GWa to PJ
    unitname = '(PJ)'

    unit_el = 8760/1000     # Conversion from GWa to TWh
    unitname_el = '(TWh)'

    last_yr = msgSC.set('cat_year', {'type_year': 'lastmodelyear'}
                        )['year'].item()
    plotyrs = [int(i) for i in list(msgSC.set('year')) if
               int(i) < int(last_yr)]
    yr = msgSC.set('cat_year', {'type_year': 'firstmodelyear'}).year.item()

    # 2) Reading and sorting solution data
    # A dictionary for saving all the results for plotting
    alldf = {}

    # 2.1) Power plant activity and capacity
    cap = msgSC.var('CAP', {'year_vtg': plotyrs})
    cap_new = msgSC.var('CAP_NEW', {'year_vtg': plotyrs})
    cap_hist = msgSC.par('historical_new_capacity', {'year_vtg': plotyrs})

    tec = msgSC.par('output', {'commodity': 'electr', 'level': 'secondary'}
                    )['technology']

    tec = tec.tolist() + ['stor_ppl']
    # Power plant capacity
    ppl_cap = cap.loc[cap.technology.isin(tec)][['technology', 'year_act',
                                                 'lvl']]
    ppl_cap = ppl_cap.groupby(['technology', 'year_act'], as_index=False).sum()
    ppl_cap = ppl_cap.pivot('year_act', 'technology')
    ppl_cap = ppl_cap[ppl_cap.columns[(ppl_cap != 0).any()]]
    ppl_cap.columns = ppl_cap.columns.droplevel(0)
    ppl_cap = ppl_cap.loc[:, (ppl_cap > 0).any()]

    # Power plant new capacity
    ppl_cap_new = cap_new.loc[(cap_new.technology.isin(tec)
                               ) & (cap_new.lvl > 0)][['technology',
                                                       'year_vtg', 'lvl']]
    ppl_cap_new = ppl_cap_new.pivot('year_vtg', 'technology')
    ppl_cap_new.columns = ppl_cap_new.columns.droplevel(0)

    # Power plant historical capacity
    ppl_cap_hist = cap_hist.loc[(cap_hist.technology.isin(tec)
                                 ) & (cap_hist.value > 0)][['technology',
                                                            'year_vtg',
                                                            'value']]
    ppl_cap_hist = ppl_cap_hist.pivot('year_vtg', 'technology')
    ppl_cap_hist.columns = ppl_cap_hist.columns.droplevel(0)

    cap_new_tot = (ppl_cap_new.add(ppl_cap_hist, fill_value=0)).fillna(0)
    cap_new_tot = cap_new_tot[cap_new_tot.columns[(cap_new_tot > 0.001).any()]]

    # Power plant activity
    elec_out_plot = plotdf(msgSC, tec, ['electr'], 'output', plotyrs, yr
                           ) * unit_el

    # Treatment of storage losses
#    d_stor = msgSC.par('input', {'technology': 'stor_ppl'})
#    d_stor = d_stor.loc[d_stor['year_act'].isin(plotyrs)
#                        ][['technology', 'year_act', 'value']]
#    d_stor = d_stor.groupby(['year_act']).mean()
#    elec_out_plot['stor_ppl'] = elec_out_plot['stor_ppl'] * -d_stor['value']
#    elec_out_plot.rename({'stor_ppl':'storage_loss'}, axis=1, inplace=True)

    alldf['Electricity generation ' + unitname_el] = elec_out_plot
    alldf['Power plant capacity (GW)'] = ppl_cap
    alldf['Power plant new capacity (GW)'] = cap_new_tot

    # 2.2) Electricity usage
    tecs = list(set(msgSC.par('input', {'commodity': 'electr',
                                        'level': 'final'}).technology))
    tecs = tecs + ['stor_ppl']
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input', 'electr')
    df = group(df, ['year_act', 'technology'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'technology')
    df = (df_hist + df).fillna(0)

    df.rename({'stor_ppl': 'storage_loss'}, axis=1, inplace=True)
    alldf['Electricity use ' + unitname_el] = df * unit_el

    # 2.3) Natural Gas supply (incl. import) and usage
    gas_tecs = ['gas_t_d', 'gas_t_d_ch4', 'gas_bal']
    tecs = list(set(msgSC.par('output', {'commodity': 'gas'}
                              ).technology) - set(gas_tecs))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'output', 'gas')
    df = group(df, ['year_act', 'technology'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'technology')
    alldf['Gas supply ' + unitname] = (df_hist + df).fillna(0) * unitc

    # Natural gas usage
    gas_tecs = ['gas_t_d', 'gas_t_d_ch4', 'gas_bal']
    tecs = list(set(msgSC.par('input', {'commodity': 'gas'}
                              ).technology) - set(gas_tecs))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input', 'gas')
    df = plotdf_sec(plotyrs, df, ['year_act', 'technology'],
                    'product', 0.0, yr)

    # adding historical data
    # df_hist = add_history(msgSC, tecs, nodeloc, df2)
    # df = (df_hist[df.columns] + df).fillna(0)

    alldf['Gas utilization ' + unitname] = df * unitc

    # 2.4) Coal supply and utilization
    coal_tecs = ['coal_t_d', 'coal_bal', 'coal_exp']
    tecs = list(set(msgSC.par('output', {'commodity': 'coal'}
                              ).technology) - set(coal_tecs))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'output', 'coal')
    df = group(df, ['year_act', 'technology'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'technology')
    alldf['Coal supply ' + unitname] = (df + df_hist).fillna(0) * unitc

    # Coal utilization
    coal_tecs = ['coal_t_d', 'coal_bal', 'coal_extr', 'coal_extr_ch4']
    tecs = list(set(msgSC.par('input', {'commodity': 'coal'}
                              ).technology) - set(coal_tecs))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input', 'coal')
    df = plotdf_sec(plotyrs, df, ['year_act', 'technology'],
                    'product', 0.0, yr)

    # adding historical data
    # df_hist = add_history(msgSC, tecs, nodeloc, df2)
    # df = (df + df_hist).fillna(0)

    alldf['Coal utilization ' + unitname] = df * unitc

    # 2.5) Oil derivatives supply and use
    tecs = list(set(msgSC.par('output', {'commodity': ['fueloil', 'lightoil'],
                                         'level': ['secondary']}).technology))

    df, df2 = model_output(msgSC, tecs, nodeloc, 'output',
                           ['fueloil', 'lightoil'])
    df = group(df, ['year_act', 'technology'], 'product', 0.0, yr)

    # adding historical data
    # df_hist = add_history(msgSC, tecs, nodeloc, df2)
    alldf['Oil derivative supply ' + unitname] = df * unitc

    # Oil derivatives utilisation
    tecs = list(set(msgSC.par('input', {'commodity': ['fueloil', 'lightoil']}
                              ).technology) - set(['loil_t_d', 'foil_t_d']))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input',
                           ['fueloil', 'lightoil'])
    alldf['Oil derivative use ' + unitname
          ] = plotdf_sec(plotyrs, df, ['year_act', 'technology'],
                         'product', 0.0, yr) * unitc

    # 2.6) Oil supply
    tecs = list(set(msgSC.par('output', {'commodity': ['crudeoil']}
                              ).technology) - set(['oil_bal', 'oil_exp']))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'output', 'crudeoil')
    df = group(df, ['year_act', 'technology'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'technology')
    alldf['Oil supply ' + unitname] = (df + df_hist).fillna(0) * unitc

    # 2.7) Biomass provision
    tecs = list(set(msgSC.par('output', {'commodity': ['biomass'],
                                         'level': ['primary']}).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'output')
    df = group(df, ['year_act', 'technology'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'technology')
    alldf['Biomass supply ' + unitname] = (df + df_hist).fillna(0) * unitc

    # Biomass utilisation
    tecs = list(set(msgSC.par('input',
                              {'commodity': ['biomass'],
                               'level': ['primary', 'final']}).technology
                    ) - set(['biomass_t_d']))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input', 'biomass')
    alldf['Biomass utilization ' + unitname
          ] = plotdf_sec(plotyrs, df, ['year_act', 'technology'],
                         'product', 0.0, yr) * unitc

    # 3) Sector related results
    # 3.1) Transport
    tecs = list(set(msgSC.par('output',
                              {'commodity': ['transport']}).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input')
    df = group(df, ['year_act', 'commodity'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'commodity')
    df = (df_hist + df).fillna(0)
    alldf['Transportation ' + unitname] = df * unitc

    # 3.2) Industry

    tecs = list(set(msgSC.par('output', {'commodity': ['i_spec', 'i_therm']}
                              ).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input')
    df = group(df, ['year_act', 'commodity'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'commodity')
    df = (df_hist + df).fillna(0)
    alldf['Industry ' + unitname] = df * unitc

    # 3.3) Non-energy (feedstock) use in Industry
    tecs = list(set(msgSC.par('output', {'commodity': ['i_feed']}).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input')
    df = group(df, ['year_act', 'commodity'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'commodity')
    df = (df_hist + df).fillna(0)
    alldf['Non-energy (feedstock) ' + unitname] = df * unitc

    # 3.4) Residencial and commercial
    tecs = list(set(msgSC.par('output',
                              {'commodity': ['rc_spec', 'rc_therm',
                                             'non-comm']}).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input')
    df = group(df, ['year_act', 'technology'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'technology')
    df = (df_hist + df).fillna(0)
    alldf['Residential/Commercial '+unitname] = df * unitc

    # 4) Energy balances
    # 4.1) Primary energy : production + imports

    tecs = list(set(msgSC.par('output', {'level': 'primary'}).technology))
    tecs_imp = list(['coal_imp', 'oil_imp', 'gas_imp', 'u5_imp', 'meth_imp',
                     'loil_imp', 'LNG_imp', 'lh2_imp', 'foil_imp', 'eth_imp',
                     'elec_imp'])
    # TODO: export must be deducted from import and PE
    tecs_exp = [x.split('_')[0] + '_exp' for x in tecs_imp if
                x.split('_')[0] + '_exp' in list(msgSC.set('technology'))]

    df, df2 = model_output(msgSC, tecs + tecs_imp, nodeloc, 'output')
    df = group(df, ['year_act', 'commodity'], 'product', 0.0, yr)
    df = df.reindex(['crudeoil', 'biomass', 'electr', 'gas', 'lightoil',
                     'coal', 'fueloil', 'u5', 'rc_therm', 'others'], axis=1)

#    df_exp, df2_exp = model_output(msgSC, tecs_exp, nodeloc, 'output')
#    df_exp = group(df_exp, ['year_act', 'commodity'], 'product', 0.0, yr)
#    df_exp = df_exp.reindex(['crudeoil','biomass', 'electr', 'gas',
#                             'lightoil', 'coal', 'fueloil', 'u5', 'rc_therm',
#                             'others'], axis=1)
#    df = df - df_exp

    # adding historical data
#    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'commodity')
#    df = (df_hist + df).fillna(0)

    alldf['Primary energy supply ' + unitname] = df * unitc

    # 4.2) Useful consumption
    tecs = list(set(msgSC.par('output', {'level': ['useful']}).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'output')
    alldf['Useful energy ' + unitname
          ] = plotdf_sec(plotyrs, df, ['year_act', 'technology'],
                         'product', 0.001, yr) * unitc

    # 4.3) Final energy consumption
    tecs = list(set(msgSC.par('output', {'level': ['useful']}).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'input')
    alldf['Final energy ' + unitname
          ] = plotdf_sec(plotyrs, df, ['year_act', 'technology'],
                         'product', 0.001, yr) * unitc

    # Final energy consumptionp by source
    tecs = list(set(msgSC.par('output', {'level': ['final']}).technology))
    df, df2 = model_output(msgSC, tecs, nodeloc, 'output')
    df = group(df, ['year_act', 'commodity'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'commodity')
    df = (df_hist + df).fillna(0).reindex(['lightoil', 'fueloil', 'gas',
                                           'biomass', 'coal', 'ethanol',
                                           'electr', 'others'], axis=1)
    alldf['Final energy consumption ' + unitname] = df * unitc

    # 5) Energy trade
    # 5.1) Exports
    df, df2 = model_output(msgSC, tecs_exp, nodeloc, 'output')
    df = group(df, ['year_act', 'commodity'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'commodity')
    df = (df_hist + df).fillna(0)
    alldf['Energy exports ' + unitname] = df * unitc

    # 5.2) Imports
    df, df2 = model_output(msgSC, tecs_imp, nodeloc, 'output')
    df = group(df, ['year_act', 'commodity'], 'product', 0.0, yr)

    # adding historical data
    df_hist = add_history(msgSC, tecs, nodeloc, df2, 'commodity')
    df = (df_hist + df).fillna(0)
    alldf['Energy imports ' + unitname] = df * unitc

    # 6) Emissions
    ems = ['TCE']
    df1 = msgSC.var('EMISS', {'emission': ems, 'node': nodeloc})
    alldf['Total GHG emissions (MtCeq)'] = group(df1, ['year', 'emission'],
                                                 'lvl', 0.0, yr)

    # 7) Plotting
    plt.style.use('ggplot')
    cmap = plt.get_cmap('tab20')
    fntsz = 5
    fntcol = 'dimgray'
    lw = 0.1
    ticksz = -1
    tickwdt = 0.2

    # Setting tick width
    plt.rcParams['xtick.major.size'] = ticksz
    plt.rcParams['xtick.major.width'] = tickwdt
    plt.rcParams['ytick.major.size'] = ticksz
    plt.rcParams['ytick.major.width'] = tickwdt
    plt.rcParams['axes.linewidth'] = 0.1

    # Creating a pdf file for receiving the results
    with PdfPages(path + '\\plots\\{}-msgResults.pdf'.format(
            caseName + '_' + time.strftime("%m.%d_%H.%M", time.localtime()
                                           ))) as pdf:
        j = 0
        for subplt in ['Oil supply ' + unitname,
                       'Oil derivative supply ' + unitname,
                       'Oil derivative use ' + unitname,
                       'Gas supply ' + unitname,
                       'Gas utilization ' + unitname,
                       'Coal supply ' + unitname,
                       'Coal utilization ' + unitname,
                       'Biomass supply ' + unitname,
                       'Biomass utilization ' + unitname,
                       'Electricity generation (TWh)',
                       'Power plant capacity (GW)',
                       'Power plant new capacity (GW)',
                       'Electricity use (TWh)',
                       'Transportation ' + unitname,
                       'Industry ' + unitname,
                       'Residential/Commercial ' + unitname,
                       'Non-energy (feedstock) ' + unitname,
                       'Primary energy supply ' + unitname,
                       'Final energy consumption ' + unitname,
                       'Final energy ' + unitname,
                       'Useful energy ' + unitname,
                       'Energy exports ' + unitname,
                       'Energy imports ' + unitname,
                       'Total GHG emissions (MtCeq)']:

            j = j + 1
            if alldf[subplt].empty:
                print('> WARNING: There is no solution data to be'
                      ' plotted for {}!'.format(subplt))
            else:
                # Removing those with zero data
                alldf[subplt] = alldf[subplt].loc[:, (alldf[subplt] > 0.01
                                                      ).any()]

                # Writing to Excel
                alldf[subplt].to_excel(
                    writer_xls,
                    sheet_name=subplt.replace("[", "("
                                              ).replace("]", ")"
                                                        ).replace("/", ""))

                # Plotting
                fig = plt.figure(j)
                ax = fig.add_subplot(111)
                box = ax.get_position()

                if subplt in ['Power plant new capacity (GW)']:
                    dfs = alldf[subplt]
                    ax.set_position([box.x0, box.y0, box.width * 0.8,
                                     box.height * 0.6])
                else:
                    dfs = alldf[subplt].loc[(alldf[subplt].index >= yr_min
                                             ) & (alldf[subplt].index <= yr_max
                                                  )]
                    ax.set_position([box.x0, box.y0, box.width * 0.6,
                                     box.height * 0.6])
                if dfs.empty:
                    continue
                indices = np.linspace(0, cmap.N, len(dfs.columns))
                my_colors = [cmap(int(i)) for i in indices]
                dfs.index.name = None
                dfs.plot(ax=ax, kind='bar', stacked=True, color=my_colors,
                         grid=None, title=None, width=0.5, linewidth=lw)

                plt.ylabel(subplt, size=fntsz, color=fntcol)
                handles, labels = ax.get_legend_handles_labels()

                plt.tick_params(axis='both', which='major', labelsize=fntsz)
                plt.grid(b=True, which='both', linewidth=0.1, color='silver',
                         linestyle='-')
                ax.set_xticklabels(dfs.index, rotation=30)

                # Put a legend to the right of the current axis
                ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),
                          fontsize=fntsz*1)

                pdf.savefig()
                plt.show()
                plt.close()
    # Savig Excel file
    writer_xls.save()
