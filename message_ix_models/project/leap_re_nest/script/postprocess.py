# -*- coding: utf-8 -*-
"""
This file is used for postprocessing before plotting.
"""
import pandas as pd


# %% function for creating the plotting dataframe
def group(df, groupby, result, limit, lyr):
    df = df.groupby(groupby, as_index=False).sum()
    df = pd.pivot_table(df, index=groupby[0], columns=groupby[1],
                        values=result, fill_value=0)
    return(df)


def multiply_df(df1, column1, df2, column2):
    '''The function merges dataframe df1 with df2 and multiplies column1 with
    column2. The function returns the new merged dataframe with the result
    of the muliplication in the column 'product'.
    '''
    index = [x for x in ['mode', 'node_loc', 'technology', 'time',
                         'year_act', 'year_vtg'] if x in df1.columns]
    df = df1.merge(df2, how='outer', on=index)
    df['product'] = df.loc[:, column1] * df.loc[:, column2]
    return df


# function for attaching history
def attach_history(msgSC, tec, plotyrs):
    parname = 'historical_activity'
    act_hist = msgSC.par(parname, {'technology': tec, 'year_act': plotyrs})
    act_hist = act_hist[['technology', 'year_act', 'value']]
    act_hist = act_hist.pivot('year_act', 'technology').fillna(0)
    act_hist = act_hist[act_hist.columns[(act_hist > 0).any()]]
    act_hist.columns = act_hist.columns.droplevel(0)
    return act_hist


def plotdf(msgSC, tec, com, direction, plotyrs, yr):
    inputs = msgSC.par(direction)
    inputs = inputs.loc[inputs.year_act.isin(plotyrs)]
    inputs = inputs.loc[(inputs.technology.isin(tec)) &
                        (inputs.commodity.isin(com))][['technology',
                                                       'year_act', 'value']]
    inputs = inputs.groupby(['technology', 'year_act'], as_index=False).mean()
    inputs = inputs.pivot('year_act', 'technology')
    inputs = inputs[inputs.columns[(inputs != 0).any()]]
    inputs.columns = inputs.columns.droplevel(0)

    act = msgSC.var('ACT').loc[msgSC.var('ACT').year_act.isin(plotyrs)]
    activity = act.loc[act.technology.isin(tec)][
                     ['technology', 'year_act', 'lvl']]

    activity = activity.groupby(['technology', 'year_act'],
                                as_index=False).sum()
    activity = activity.pivot('year_act', 'technology').fillna(0)
    activity = activity[activity.columns[(activity != 0).any()]]
    activity.columns = activity.columns.droplevel(0)

    # Addding Historical Activity (to the graphs)
    act_hist = attach_history(msgSC, tec, plotyrs)
    activity_tot = activity.add(act_hist, fill_value=0)

    df_plot = inputs * activity_tot
    df_plot = df_plot.fillna(0)
    df_plot = df_plot[df_plot.columns[(df_plot > 0).any()]]
    return(df_plot)


# Aggregating commodities
def plotdf_com(msgSC, tec, com, direction, plotyrs, yr):
    df_plot_com = df_plot_el = pd.DataFrame(index=plotyrs)
    for commodities in com:

        inputs = msgSC.par(direction)
        inputs = inputs.loc[inputs.year_act.isin(plotyrs)]
        inputs = inputs.loc[(inputs.technology.isin(tec)) &
                            (inputs.commodity == commodities)
                            ][['technology', 'year_act', 'value']]
        inputs = inputs.groupby(['technology', 'year_act'],
                                as_index=False).mean()
        inputs = inputs.pivot('year_act', 'technology')
        inputs = inputs[inputs.columns[(inputs != 0).any()]]
        inputs.columns = inputs.columns.droplevel(0)
        act = msgSC.var('ACT').loc[msgSC.var('ACT').year_act.isin(plotyrs)]
        activity = act.loc[act.technology.isin(tec)][
                         ['technology', 'year_act', 'lvl']]

        activity = activity.groupby(['technology', 'year_act'],
                                    as_index=False).sum()
        activity = activity.pivot('year_act', 'technology')
        activity = activity[activity.columns[(activity != 0).any()]]
        activity.columns = activity.columns.droplevel(0)

        # Attaching history
        act_hist = attach_history(msgSC, tec, plotyrs)
        activity = activity.add(act_hist, fill_value=0)

        df_plot = inputs * activity
        df_plot = df_plot.fillna(0)
        df_plot = df_plot.loc[:, (df_plot != 0).any(axis=0)]

        cols_hydro = [col for col in df_plot.columns if 'hydro' in col]
        cols_wind = [col for col in df_plot.columns if 'wind' in col]
        cols_solar = [col for col in df_plot.columns if 'solar' in col]
        cols_others = [x for x in df_plot.columns if
                       x not in (cols_hydro + cols_wind + cols_solar)]
        dict_ele = {'cols_hydro': (cols_hydro, 'hydro'),
                    'cols_wind': (cols_wind, 'wind'),
                    'cols_solar': (cols_solar, 'solar'),
                    'cols_others': (cols_others, 'electr')}

        if commodities == 'electr':
            for tecs in list(dict_ele.keys()):
                df_plot_el[dict_ele['{}'.format(tecs)][1]
                           ] = df_plot.filter(items=dict_ele['{}'.format(tecs)
                                                             ][0]
                                              ).sum(axis=1).to_frame()
        else:
            df_plot = df_plot.sum(axis=1).to_frame()
            df_plot[commodities] = df_plot
            df_plot_com = df_plot_com.join(df_plot[commodities])
    df_plot_com = df_plot_com.join(df_plot_el)

    return(df_plot_com)


# Aggregating sectors
def plotdf_sec(plotyrs, df, groupby, result, limit, lyr):
    df_sec = pd.DataFrame(index=plotyrs)
    df = df.groupby(groupby, as_index=False).sum()
    df = pd.pivot_table(df, index=groupby[0], columns=groupby[1],
                        values=result, fill_value=0)

    cols_ind = [col for col in df.columns if '_i' in col
                ] + [col for col in df.columns if '_I' in col]
    cols_ind = [x for x in cols_ind if
                x not in ['eth_ic_trp', 'meth_ic_trp', 'bio_istig',
                          'bio_istig_ccs']]
    cols_trp = [col for col in df.columns if '_trp' in col]
    cols_rc = [col for col in df.columns if any(y in col for y in ['_rc',
                                                                   '_RC'])]
    cols_nc = [col for col in df.columns if '_nc' in col]
    cols_ene = [col for col in df.columns if '_fs' in col]
    cols_exp = [col for col in df.columns if '_exp' in col]
    cols_ppl = [col for col in df.columns if
                any(y in col for y in ['_ppl', '_adv', 'bio_istig', 'gas_cc',
                                       'gas_cc_ccs', 'gas_ct', 'igcc',
                                       'igcc_ccs', 'loil_cc'])]
    cols_eth = [col for col in df.columns if
                any(y in col for y in ['eth_bio', 'liq_bio'])]
    cols_meth = [col for col in df.columns if
                 any(y in col for y in ['meth_ng', 'meth_coal'])]
    cols_loil = [col for col in df.columns if
                 any(y in col for y in ['syn_liq'])]
    cols_gas = [col for col in df.columns if
                any(y in col for y in ['coal_gas', 'gas_bio'])]
    cols_hyd = [col for col in df.columns if
                any(y in col for y in ['h2_'])]

    cols_others = [x for x in df.columns if
                   x not in (cols_ind + cols_trp + cols_rc + cols_nc +
                             cols_exp + cols_ppl + cols_eth + cols_ene +
                             cols_meth + cols_loil + cols_gas + cols_hyd)]

    sectors = {'cols_ind': cols_ind, 'cols_trp': cols_trp,
               'cols_ene': cols_ene, 'cols_rc': cols_rc, 'cols_nc': cols_nc,
               'cols_exp': cols_exp, 'cols_ppl': cols_ppl,
               'cols_eth': cols_eth, 'cols_meth': cols_meth,
               'cols_loil': cols_loil, 'cols_gas': cols_gas,
               'cols_hyd': cols_hyd, 'cols_others': cols_others}

    dict_sectors = {'cols_ind': ['industry'], 'cols_trp': ['transport'],
                    'cols_ene': ['energy'], 'cols_rc': ['resid/comm'],
                    'cols_nc': ['non-commercial'], 'cols_ppl': ['ppl'],
                    'cols_exp': ['exports'], 'cols_eth': ['ethanol'],
                    'cols_meth': ['methanol'], 'cols_loil': ['light oil'],
                    'cols_gas': ['gas'], 'cols_hyd': ['hydrogen'],
                    'cols_others': ['others']}

    for sec in list(sectors.keys()):
        df[dict_sectors['{}'.format(sec)]
           ] = df.filter(items=sectors['{}'.format(sec)]).sum(axis=1
                                                              ).to_frame()
        df_sec = df_sec.join(df[dict_sectors['{}'.format(sec)]])

    return(df_sec)


# Aggregating results for industry and energy sectors
def plotdf_ind(msgSC, tec, com, direction, plotyrs, yr):
    df_plot_ind = pd.DataFrame(index=plotyrs)

    inputs = msgSC.par(direction)
    inputs = inputs.loc[inputs.year_act.isin(plotyrs)]
    inputs = inputs.loc[(inputs.technology.isin(tec)) &
                        (inputs.commodity.isin(com))][['technology',
                                                       'year_act', 'value']]
    inputs = inputs.groupby(['technology', 'year_act'], as_index=False).mean()
    inputs = inputs.pivot('year_act', 'technology')
    inputs = inputs[inputs.columns[(inputs != 0).any()]]
    inputs.columns = inputs.columns.droplevel(0)
    act = msgSC.var('ACT').loc[msgSC.var('ACT').year_act.isin(plotyrs)]
    activity = act.loc[act.technology.isin(tec)][
                     ['technology', 'year_act', 'lvl']]

    activity = activity.groupby(['technology', 'year_act'],
                                as_index=False).sum()
    activity = activity.pivot('year_act', 'technology')
    activity = activity[activity.columns[(activity != 0).any()]]
    activity.columns = activity.columns.droplevel(0)

    # Attaching history
    act_hist = attach_history(msgSC, tec, plotyrs)
    activity = activity.add(act_hist, fill_value=0)

    df_plot = inputs * activity
    df_plot = df_plot.fillna(0)
    df_plot = df_plot.loc[:, (df_plot != 0).any(axis=0)]

    fuel = {'biomass': ['biomass_i'], 'coal': ['coal_i'],
            'foil': ['foil_fs', 'foil_i'],
            'electr': ['elec_i', 'sp_el_I', 'sp_liq_I'],
            'gas': ['gas_fs', 'gas_i'],
            'd_heat': ['heat_i'], 'loil': ['loil_fs', 'loil_i'],
            'solar': ['solar_i']}

    for source in list(fuel.keys()):
        df_plot['{}'.format(source)
                ] = df_plot.filter(items=fuel['{}'.format(source)]
                                   ).sum(axis=1).to_frame()
        df_plot_ind = df_plot_ind.join(df_plot['{}'.format(source)])

    others = df_plot_ind.loc[:,
                             (df_plot_ind.ix[yr] <= df_plot_ind.ix[yr].sum() *
                              0.01)].sum(axis=1)
    df_plot_ind = df_plot_ind.loc[:, (
        df_plot_ind.ix[yr] > df_plot_ind.ix[yr].sum() * 0.01)]
    df_plot_ind['others'] = others

    return(df_plot_ind)
