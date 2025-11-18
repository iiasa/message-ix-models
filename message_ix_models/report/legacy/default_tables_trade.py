from itertools import tee
import numpy as np
import pandas as pd

import message_ix_models.report.legacy.pp_utils as pp_utils

pp = None
mu = None
run_history = None
urban_perc_data = None
kyoto_hist_data = None
lu_hist_data = None

# Dictionary where all functions defined in the script are stored.
func_dict = {}  # type: dict


def return_func_dict():
    """Returns functions defined in script"""
    return func_dict


def _register(func):
    """Function to register reporting functions.

    Parameters
    ----------

    func : str
        Function name
    """

    func_dict[func.__name__] = func
    return func


def _pe_wCCSretro(tec, scrub_tec, group, inpfilter, units, share=1):
    """Calculates primary energy use of technologies with scrubbers.

    Parameters
    ----------

    tec : str
        Technology name
    scrub_tec : str
        Name of CO2 scrubbing technology linked to `tec`.
    group : list
        Indexes by which results are to be grouped.
    inpfilter : dict
       `level` and/or `commodity` used for retrieving the input.
    units : str
        Units to which variables should be converted.
    share : number or dataframe (default: 1)
        Share of `tec` activity if multiple technologies share the same scrubber.
    """

    # The multiplication of `share` is required to determine in which years
    # the powerplant is active, because the scrubber activity
    # is not necessarily equal to the powerplant activity if
    # there are two powerplants using the same scrubber.

    df = (
        (
            pp.out(
                scrub_tec,
                units,
                outfilter={"level": ["secondary"], "commodity": ["exports"]},
            )
            * share
            / pp_utils.ppgroup(
                (pp.act(tec, group=group) / pp.act(tec)).fillna(0)
                * pp.eff(
                    tec,
                    inpfilter=inpfilter,
                    outfilter={"level": ["secondary"], "commodity": ["electr"]},
                    group=group,
                )
            )
        )
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
    )

    return df


def _pe_elec_woCCSretro(tec, scrub_tec, group, inpfilter, units, _Frac, share=1):
    """Calculates primary energy electricity generation equivalent.

    This applies to technologies WITHOUT scrubbers.

    Parameters
    ----------

    tec : str
        Technology name
    scrub_tec : str
        Name of CO2 scrubbing technology linked to `tec`.
    inpfilter : dict
       `level` and/or `commodity` used for retrieving the input.
    units : str
        Units to which variables should be converted.
    _Frac : dataframe
        Regional share of actual cogeneration (po_turbine).
    share : number or dataframe (default: 1)
        Share of `tec` activity if multiple technologies share the same scrubber.
    """

    df = (
        (
            (
                pp.out(
                    tec = tec,
                    units = units,
                    outfilter={"level": ["secondary"], "commodity": ["electr"]},
                )
                * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
                - (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
                * pp.out(
                    tec = scrub_tec,
                    units = units,
                    outfilter={"level": ["secondary"], "commodity": ["exports"]},
                )
                * share
            )
            / pp_utils.ppgroup(
                (pp.act(tec, group=group) / pp.act(tec)).fillna(0)
                * pp.eff(
                    tec,
                    inpfilter=inpfilter,
                    outfilter={"level": ["secondary"], "commodity": ["electr"]},
                    group=group,
                )
            )
        )
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
    )

    return df


def _pe_elec_wCCSretro(tec, scrub_tec, group, inpfilter, units, _Frac, share=1):
    """Calculates primary energy electricity generation equivalent.

    This applies to technologies WITH scrubbers.

    Parameters
    ----------

    tec : str
        Technology name
    scrub_tec : str
        Name of CO2 scrubbing technology linked to `tec`.
    group : list
        Indexes by which resultsa re to be grouped.
    inpfilter : dict
       `level` and/or `commodity` used for retrieving the input.
    units : str
        Units to which variables should be converted.
    _Frac : dataframe
        Regional share of actual cogeneration (po_turbine).
    share : number or dataframe (default: 1)
        Share of `tec` activity if multiple technologies share the same scrubber.
    """

    df = (
        (
            (
                (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
                * pp.out(
                    tec = scrub_tec,
                    units = units,
                    outfilter={"level": ["secondary"], "commodity": ["exports"]},
                )
                * share
            )
            / pp_utils.ppgroup(
                (pp.act(tec, group=group) / pp.act(tec)).fillna(0)
                * pp.eff(
                    tec,
                    inpfilter=inpfilter,
                    outfilter={"level": ["secondary"], "commodity": ["electr"]},
                    group=group,
                )
            )
        )
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
    )

    return df


def _se_elec_woCCSretro(tec, scrub_tec, units, _Frac, share=1, outfilter=None):
    """Calculates secondary energy electricity generation.

    This applies to technologies WITHOUT scrubbers.

    Parameters
    ----------

    tec : str
        Technology name
    scrub_tec : str
        Name of CO2 scrubbing technology linked to `tec`.
    units : str
        Units to which variables should be converted.
    _Frac : dataframe
        Regional share of actual cogeneration (po_turbine).
    share : number or dataframe (default: 1)
        Share of `tec` activity if multiple technologies share the same scrubber.
    outfilter : dict
       `level` and/or `commodity` used for retrieving the output.
    """

    df = (
        pp.out(tec = tec, units = units, outfilter = outfilter)
        * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
        - (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
        * pp.out(
            tec = scrub_tec,
            units = units,
            outfilter={"level": ["secondary"], "commodity": ["exports"]},
        )
        * share
    )

    return df


def _se_elec_wCCSretro(tec, scrub_tec, units, _Frac, share=1):
    """Calculates secondary energy electricity generation.

    This applies to technologies WITH scrubbers.

    Parameters
    ----------

    tec : str
        Technology name
    scrub_tec : str
        Name of CO2 scrubbing technology linked to `tec`.
    units : str
        Units to which variables should be converted.
    _Frac : dataframe
        Regional share of actual cogeneration (po_turbine).
    share : number or dataframe (default: 1)
        Share of `tec` activity if multiple technologies share the same scrubber.
    """

    df = (
        (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
        * pp.out(
            tec = scrub_tec,
            units = units,
            outfilter={"level": ["secondary"], "commodity": ["exports"]},
        )
        * share
    )

    return df


def _pe_elec_po_turb(tec, group, units, _Frac, inpfilter, outfilter):
    """Calculates primary energy electricity equivalent generation.

    This calcualtes the amount of electricity used in primary energy equivalent used
    for cogeneration (po-turbine).

    Parameters
    ----------

    tec : str
        Technology name
    group : list
        Indexes by which resultsa re to be grouped.
    units : str
        Units to which variables should be converted.
    _Frac : dataframe
        Regional share of actual cogeneration (po_turbine).
    inpfilter : dict
       `level` and/or `commodity` used for retrieving the input.
    outfilter : dict
       `level` and/or `commodity` used for retrieving the output.
    """

    df = pp_utils.ppgroup(
        (
            (
                pp.out(tec = tec, units = units, outfilter = outfilter, group = group)
                * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
            )
            / pp.eff(tec, inpfilter=inpfilter, outfilter=outfilter, group=group)
        )
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
    )

    return df


def _se_elec_po_turb(tec, units, _Frac, outfilter=None):
    """Calculates secondary energy electricity generation.

    This calcualtes the amount of electricity used for cogeneration (po-turbine).

    Parameters
    ----------

    tec : str
        Technology name
    units : str
        Units to which variables should be converted.
    _Frac : dataframe
        Regional share of actual cogeneration (po_turbine).
    outfilter : dict
       `level` and/or `commodity` used for retrieving the output.
    """

    df = pp.out(tec = tec, units = units, outfilter = outfilter) * (
        1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac
    )

    return df


def _se_heat_po_turb(tec, units, _Frac, outfilter=None):
    """Calculates secondary energy heat generation.

    This calcualtes the amount of heat produced from cogeneration (po-turbine)
    for a specific technology.

    Parameters
    ----------

    tec : str
        Technology name
    units : str
        Units to which variables should be converted.
    _Frac : dataframe
        Regional share of actual cogeneration (po_turbine).
    outfilter : dict
       `level` and/or `commodity` used for retrieving the output.
    """

    df = pp.out(tec = tec, units = units, outfilter = outfilter) * (
        pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac
    )

    return df


def _out_div_eff(tec, group, inpfilter, outfilter):
    """Calculates input based on output.

    This calculates the amount of input required based on the output.
    Mainly this is used for water related reporting.

    Parameters
    ----------

    tec : str
        Technology name
    group : list
        Indexes by which resultsa re to be grouped.
    inpfilter : dict
       `level` and/or `commodity` used for retrieving the input.
    outfilter : dict
       `level` and/or `commodity` used for retrieving the output.
    """

    tec = [tec] if type(tec) == str else tec

    dfs = []
    for t in tec:
        dfs.append(
            pp_utils.ppgroup(
                (pp.out(tec = t, outfilter = outfilter, group = group))
                / pp.eff(t, inpfilter=inpfilter, outfilter=outfilter, group=group)
            )
            .fillna(0)
            .replace([np.inf, -np.inf], 0)
        )
    df = pd.concat(dfs, sort=True)

    return df.groupby(df.index.name).sum()


def _calc_scrubber_capacity(
    prmfunc, tec, group, share=1, cumulative=False, efficiency=True
):
    """Calculate the capacity of CO2-retrofit scrubbers.

    Parameters
    ----------
    prmfunc : pp_utils.py function
        Function which retrieves data for `tec`
    tec : string
        CO2-scrubber technology name
    group : list
        List of index names by which to group various prameters
    share : :class:`pandas.DataFrame()`
        Share values to be applied to data; must be indexed by `group`.
    cumulative :  boolean
        Switch to indicate if values are being cumulated over time or not.
    efficiency : boolemna
        Switch whether the data should be corrected by the efficiency factor of `tec`.
    """

    # Retrieve vintage specific technology parameter
    df = prmfunc(tec, group=group)

    # Retrieve efficiency
    eff = (
        1
        if efficiency is False
        else (1 - 1 / pp.eff(tec, group=group)).replace([np.inf, -np.inf], 0)
    )

    # When cumulative values are being calculated, then it is necessary
    # to extend vintage specific parameters for all activity years, independent
    # if the technology is active or not, becuse the parameter e.g. CAP-Additions
    # are also returned in the same way.
    if cumulative and type(eff) != int:
        eff = eff.replace(0, np.nan).ffill(axis=1).fillna(0)

    df = df * share * eff

    df = df.groupby("Region").sum(numeric_only=True)

    return df


# -------------------
# Reporting functions
# -------------------


@_register
def retr_extraction(units):
    """Resources: Extraction (Fossil + Uranium).

    Annual extraction of fossil and uranium resources.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    Coal = pp.extr("coal", units)
    Lignite = pp.extr("lignite", units)
    Conv_oil = pp.extr(["crude_1", "crude_2", "crude_3"], units)
    Unconv_oil = pp.extr(["crude_4", "crude_5", "crude_6", "crude_7", "crude_8"], units)
    Conv_gas = pp.extr(["gas_1", "gas_2", "gas_3", "gas_4"], units)
    Unconv_gas = pp.extr(["gas_5", "gas_6", "gas_7", "gas_8"], units)
    Uranium = pp.extr("uranium", units)

    vars["Coal"] = Coal + Lignite
    vars["Gas|Conventional"] = Conv_gas
    vars["Gas|Unconventional"] = Unconv_gas
    vars["Oil|Conventional"] = Conv_oil
    vars["Oil|Unconventional"] = Unconv_oil
    vars["Uranium"] = Uranium

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_cumulative_extraction(units):
    """Resources: Extraction cumulative (Fossil + Uranium).

    Cumulative extraction of fossil and uranium resources,
    from 2005 onwards

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    Coal = pp_utils.cum_vals(pp.extrc("coal", units))
    Lignite = pp_utils.cum_vals(pp.extrc("lignite", units))
    Conv_oil = pp_utils.cum_vals(pp.extrc(["crude_1", "crude_2", "crude_3"], units))
    Unconv_oil = pp_utils.cum_vals(
        pp.extrc(["crude_4", "crude_5", "crude_6", "crude_7", "crude_8"], units)
    )
    Conv_gas = pp_utils.cum_vals(pp.extrc(["gas_1", "gas_2", "gas_3", "gas_4"], units))
    Unconv_gas = pp_utils.cum_vals(
        pp.extrc(["gas_5", "gas_6", "gas_7", "gas_8"], units)
    )
    Uranium = pp_utils.cum_vals(pp.extrc("uranium", units))

    vars["Coal"] = Coal + Lignite
    vars["Gas|Conventional"] = Conv_gas
    vars["Gas|Unconventional"] = Unconv_gas
    vars["Oil|Conventional"] = Conv_oil
    vars["Oil|Unconventional"] = Unconv_oil
    vars["Uranium"] = Uranium

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_remaining_resources(units):
    """Resources: Remaining (Fossil + Uranium).

    Remaining resources based on the initial resources
    volumes and accounting for extraction over time.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    Coal = pp.extr("coal", units)
    Lignite = pp.extr("lignite", units)
    Conv_oil = pp.extr(["crude_1", "crude_2", "crude_3"], units)
    Unconv_oil = pp.extr(["crude_4", "crude_5", "crude_6", "crude_7", "crude_8"], units)
    Conv_gas = pp.extr(["gas_1", "gas_2", "gas_3", "gas_4"], units)
    Unconv_gas = pp.extr(["gas_5", "gas_6", "gas_7", "gas_8"], units)
    Uranium = pp.extr("uranium", units)

    vars["Coal"] = Coal + Lignite
    vars["Gas|Conventional"] = Conv_gas
    vars["Gas|Unconventional"] = Unconv_gas
    vars["Oil|Conventional"] = Conv_oil
    vars["Oil|Unconventional"] = Unconv_oil
    vars["Uranium"] = Uranium

    df = pp_utils.make_outputdf(vars, units)
    pp_utils.cum_vals(df)
    return df


@_register
def retr_agri_dem(units):
    """Landuse: Agricultural Demand.

    Land-use related agricultural demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Total"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand"],
        }
    )

    vars["Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops"],
        }
    )

    vars["Crops|Bioenergy|1st Generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Bioenergy|1st generation"],
        }
    )

    vars["Crops|Bioenergy|2nd generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Bioenergy|2nd generation"],
        }
    )

    vars["Crops|Bioenergy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Bioenergy"],
        }
    )

    vars["Crops|Feed"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Feed"],
        }
    )

    vars["Crops|Feed|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Feed|Cereals"],
        }
    )

    vars["Crops|Feed|Oil Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Feed|Oil Crops"],
        }
    )

    vars["Crops|Feed|Sugar Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Feed|Sugar Crops"],
        }
    )

    vars["Crops|Feed|Other Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Feed|Other Crops"],
        }
    )

    vars["Crops|Food"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Food"],
        }
    )

    vars["Crops|Food|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Food|Cereals"],
        }
    )

    vars["Crops|Food|Oil Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Food|Oil Crops"],
        }
    )

    vars["Crops|Food|Sugar Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Food|Sugar Crops"],
        }
    )

    vars["Crops|Food|Other Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Food|Other Crops"],
        }
    )

    vars["Crops|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Crops|Other"],
        }
    )

    vars["Energy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Energy"],
        }
    )

    vars["Energy|Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Energy|Crops"],
        }
    )

    vars["Energy|Crops|1st generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Energy|Crops|1st generation"],
        }
    )

    vars["Energy|Crops|2nd generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Energy|Crops|2nd generation"],
        }
    )

    vars["Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock"],
        }
    )

    vars["Livestock|Food"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food"],
        }
    )

    vars["Livestock|Food|Non-Ruminant"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food|Non-Ruminant"],
        }
    )

    vars["Livestock|Food|Non-Ruminant|Eggs"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food|Non-Ruminant|Eggs"],
        }
    )

    vars["Livestock|Food|Non-Ruminant|Meat"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food|Non-Ruminant|Meat"],
        }
    )

    vars["Livestock|Food|Non-Ruminant|Meat|Pig"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food|Non-Ruminant|Meat|Pig"],
        }
    )

    vars["Livestock|Food|Non-Ruminant|Meat|Poultry"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": [
                "Agricultural Demand|Livestock|Food|Non-Ruminant|Meat|Poultry"
            ],
        }
    )

    vars["Livestock|Food|Ruminant"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food|Ruminant"],
        }
    )

    vars["Livestock|Food|Ruminant|Dairy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food|Ruminant|Dairy"],
        }
    )

    vars["Livestock|Food|Ruminant|Meat"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Food|Ruminant|Meat"],
        }
    )

    vars["Livestock|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Livestock|Other"],
        }
    )

    vars["Non-Energy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy"],
        }
    )

    vars["Non-Energy|Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Crops"],
        }
    )

    vars["Non-Energy|Crops|Feed"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Crops|Feed"],
        }
    )

    vars["Non-Energy|Crops|Food"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Crops|Food"],
        }
    )

    vars["Non-Energy|Crops|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Crops|Other"],
        }
    )

    vars["Non-Energy|Crops|Other|Waste"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Crops|Other|Waste"],
        }
    )

    vars["Non-Energy|Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Livestock"],
        }
    )

    vars["Non-Energy|Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Livestock"],
        }
    )

    vars["Non-Energy|Livestock|Food"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Livestock|Food"],
        }
    )

    vars["Non-Energy|Livestock|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Livestock|Other"],
        }
    )

    vars["Non-Energy|Livestock|Other|Waste"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Livestock|Other|Waste"],
        }
    )

    vars["Residues|Bioenergy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Residues|Bioenergy"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_agri_prd(units):
    """Landuse: Agricultural Production.

    Land-use related agricultural production.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Total"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production"],
        }
    )

    vars["Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Crops"],
        }
    )

    vars["Crops|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Crops|Cereals"],
        }
    )

    vars["Crops|Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Crops|Energy Crops"],
        }
    )

    vars["Crops|Oil Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Crops|Oil Crops"],
        }
    )

    vars["Crops|Other Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Crops|Other Crops"],
        }
    )

    vars["Crops|Sugar Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Crops|Sugar Crops"],
        }
    )

    vars["Energy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Energy"],
        }
    )

    vars["Energy|Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Energy|Crops"],
        }
    )

    vars["Energy|Crops|1st generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Energy|Crops|1st generation"],
        }
    )

    vars["Energy|Crops|2nd generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Energy|Crops|2nd generation"],
        }
    )

    vars["Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Livestock"],
        }
    )

    vars["Livestock|Dairy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Livestock|Dairy"],
        }
    )

    vars["Livestock|Non-Ruminant"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Livestock|Non-Ruminant"],
        }
    )

    vars["Livestock|Ruminant"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Livestock|Ruminant"],
        }
    )

    vars["Non-Energy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Non-Energy"],
        }
    )

    vars["Non-Energy|Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Non-Energy|Crops"],
        }
    )

    vars["Non-Energy|Crops|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Non-Energy|Crops|Cereals"],
        }
    )

    vars["Non-Energy|Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Non-Energy|Livestock"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_forest_dem(units_for_vol, units_for_vol_yr):
    """Landuse: Forest Demand.

    Land-use related forest demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    dfs = []

    vars = {}

    vars["Energy plantation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Energy plantation"],
        }
    )

    vars["Energy plantation|Energy use"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Energy plantation|Energy use"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_for_vol))

    vars = {}

    vars["Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Roundwood"],
        }
    )

    vars["Roundwood|Industrial Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Roundwood|Industrial Roundwood"],
        }
    )

    vars["Roundwood|Industrial Roundwood|Energy Use"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Roundwood|Industrial Roundwood|Energy Use"],
        }
    )

    vars["Roundwood|Industrial Roundwood|Material Use"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": [
                "Forestry Demand|Roundwood|Industrial Roundwood|Material Use"
            ],
        }
    )

    vars["Roundwood|Wood Fuel"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Roundwood|Wood Fuel"],
        }
    )

    vars["Semi-Finished|Chemical Pulp"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Semi-finished|Chemical pulp"],
        }
    )

    vars["Semi-Finished|Fiberboard"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Semi-finished|Fiberboard"],
        }
    )

    vars["Semi-Finished|Mechanical Pulp"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Semi-finished|Mechanical pulp"],
        }
    )

    vars["Semi-Finished|Other Wood Products"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Semi-finished|Other wood products"],
        }
    )

    vars["Semi-Finished|Plywood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Semi-finished|Plywood"],
        }
    )

    vars["Semi-Finished|Sawnwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Semi-finished|Sawnwood"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_for_vol_yr))

    return pd.concat(dfs, sort=True)


@_register
def retr_forest_prd(units_for_vol, units_for_vol_yr, units_for_dm_yr):
    """Landuse: Forest Production.

    Land-use related forest production.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    dfs = []

    vars = {}

    vars["Energy plantation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Energy plantation"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_for_vol))

    vars = {}

    vars["Forest Residues"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Forest Residues"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_for_dm_yr))

    vars = {}

    vars["Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood"],
        }
    )

    vars["Roundwood|Industrial Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood|Industrial Roundwood"],
        }
    )

    vars["Roundwood|Wood Fuel"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood|Wood Fuel"],
        }
    )

    vars["Semi-Finished|Chemical Pulp"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Semi-finished|Chemical pulp"],
        }
    )

    vars["Semi-Finished|Fiberboard"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Semi-finished|Fiberboard"],
        }
    )

    vars["Semi-Finished|Mechanical Pulp"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Semi-finished|Mechanical pulp"],
        }
    )

    vars["Semi-Finished|Other Wood Products"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Semi-finished|Other wood products"],
        }
    )

    vars["Semi-Finished|Plywood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Semi-finished|Plywood"],
        }
    )

    vars["Semi-Finished|Sawnwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Semi-finished|Sawnwood"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_for_vol_yr))

    return pd.concat(dfs, sort=True)


@_register
def retr_lnd_cvr(units):
    """Landuse: Land-Cover.

    Land-use cover.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Total"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Land Cover"]}
    )

    vars["Cropland"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland"],
        }
    )

    vars["Cropland|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Cereals"],
        }
    )

    vars["Cropland|Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Energy Crops"],
        }
    )

    vars["Cropland|Irrigated"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Irrigated"],
        }
    )

    vars["Cropland|Non-Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Non-Energy Crops"],
        }
    )

    vars["Cropland|Oil Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Oil Crops"],
        }
    )

    vars["Cropland|Other Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Other Crops"],
        }
    )

    vars["Cropland|Rainfed"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Rainfed"],
        }
    )

    vars["Cropland|Sugar Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Sugar Crops"],
        }
    )

    vars["Forest"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest"],
        }
    )

    vars["Forest|Planted"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Planted"],
        }
    )

    vars["Forest|Planted|Natural"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Planted|Natural"],
        }
    )

    vars["Forest|Planted|Natural|Re/Afforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Planted|Natural|Re/Afforestation"],
        }
    )

    vars["Forest|Planted|Plantation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Planted|Plantation"],
        }
    )

    vars["Forest|Planted|Plantation|Re/Afforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Planted|Plantation|Re/Afforestation"],
        }
    )

    vars["Forest|Planted|Plantation|Timber"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Planted|Plantation|Timber"],
        }
    )

    vars["Forest|Natural"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Natural"],
        }
    )

    vars["Forest|Afforestation and Reforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Afforestation and Reforestation"],
        }
    )

    vars["Forest|Forest old"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Forest old"],
        }
    )

    vars["Forest|Forestry"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Forestry"],
        }
    )

    vars["Forest|Managed"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Managed"],
        }
    )

    vars["Forest|Natural Forest"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Natural Forest"],
        }
    )

    vars["Forest|Primary"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Primary"],
        }
    )

    vars["Forest|Secondary"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Secondary"],
        }
    )

    vars["Other Land"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Other Land"],
        }
    )

    vars["Other Land|Other Natural Land"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Other Land|Other Natural Land"],
        }
    )

    vars["Other Land|Other Natural Land|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Other Land|Other Natural Land|Other"],
        }
    )

    vars["Other Natural"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Other Natural"],
        }
    )

    vars["Other Natural|Primary Non-Forest"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Other Natural|Primary Non-Forest"],
        }
    )

    vars["Other Natural|Restored"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Other Natural|Restored"],
        }
    )

    vars["Pasture"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Pasture"],
        }
    )

    vars["Protected"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Protected"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_yield(units, units_timber):
    """Landuse: Crop yield.

    Land-use related crop-yield.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    dfs = []

    # Process and weight cereals
    vars = {}
    vars["Cereals"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Cereals"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Cereals"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Cotton"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Cotton"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Cotton"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Maize"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Maize"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Maize"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Other coarse grains"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Other coarse grains"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Other coarse grains"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Other Oilseeds"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Other Oilseeds"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Other Oilseeds"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Palm oil"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Palm oil"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Palm oil"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Rice"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Rice"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Rice"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Roots and tubers"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Roots and tubers"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Roots and tubers"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Soybean"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Soybean"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Soybean"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Sugar cane"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Sugar cane"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Sugar cane"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Wheat"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Wheat"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Wheat"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    vars = {}

    vars["Pulses"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Pulses"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Energy Crops
    vars = {}
    vars["Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Energy Crops"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Energy Crops"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Energy Crops
    vars = {}
    vars["Cropland|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Cropland|Cereals"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Cereals"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Energy Crops
    vars = {}
    vars["Cropland|Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Cropland|Energy Crops"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Energy Crops"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Energy Crops
    vars = {}
    vars["Cropland|Oil Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Cropland|Oil Crops"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Oilcrops"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Energy Crops
    vars = {}
    vars["Cropland|Other Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Cropland|Other Crops"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Energy Crops
    vars = {}
    vars["Cropland|Sugar Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Cropland|Sugar Crops"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Sugarcrops"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Non-Energy Crops
    vars = {}
    vars["Non-Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Non-Energy Crops"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Non-Energy Crops"],
        }
    )
    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Oilcrops
    vars = {}
    vars["Oil Crops"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Oilcrops"]}
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Oilcrops"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Sugarcrops
    vars = {}
    vars["Sugar Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Sugarcrops"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland|Sugarcrops"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units, param="weighted_avg", weighted_by=pp_utils.sum_reg(cropland)
        )
    )

    # Process and weight Sugarcrops
    vars = {}
    vars["Forestry|Timber Production"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Forestry|Timber Production"],
        }
    )

    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Planted|Plantation|Timber"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars,
            units_timber,
            param="weighted_avg",
            weighted_by=pp_utils.sum_reg(cropland),
        )
    )

    return pd.concat(dfs, sort=True)


@_register
def retr_fertilizer_use(units_nitrogen, units_phosphorus):
    """Landuse: Fertilizer usage.

    Land-use related fertilizer usage.
    Based on land-use emulator.

    Parameters
    ----------

    units_nitrogen : str
        Units to which nitrogen should be converted.
    units_phosphorus : str
        Units to which phosphorus should be converted.
    """

    dfs = []
    vars = {}

    vars["Nitrogen"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Fertilizer Use|Nitrogen"],
        }
    )
    dfs.append(pp_utils.make_outputdf(vars, units_nitrogen))

    vars = {}
    vars["Phosphorus"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Fertilizer Use|Phosphorus"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_phosphorus))
    return pd.concat(dfs, sort=True)


@_register
def retr_fertilizer_int(units_nitrogen, units_phosphorus):
    """Landuse: Fertilizer usage.

    Land-use related fertilizer usage.
    Based on land-use emulator.

    Parameters
    ----------

    units_nitrogen : str
        Units to which nitrogen should be converted.
    units_phosphorus : str
        Units to which phosphorus should be converted.
    """

    # This variable is only required for deriving the global value via weighted
    # average.
    cropland = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Cropland"],
        }
    )

    dfs = []
    vars = {}

    vars["Nitrogen|Intensity"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Fertilizer|Nitrogen|Intensity"],
        }
    )
    dfs.append(
        pp_utils.make_outputdf(
            vars, units_nitrogen, param="weighted_avg", weighted_by=cropland
        )
    )

    vars = {}
    vars["Phosphorus|Intensity"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Fertilizer|Phosphorus|Intensity"],
        }
    )

    dfs.append(
        pp_utils.make_outputdf(
            vars, units_phosphorus, param="weighted_avg", weighted_by=cropland
        )
    )
    return pd.concat(dfs, sort=True)


@_register
def retr_food_waste(units_DM, units_cap):
    """Landuse: Food waste.

    Land-use related food demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    dfs = []

    vars = {}

    vars["Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Crops|Other|Waste"],
        }
    )

    vars["Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Energy|Livestock|Other|Waste"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_DM))

    vars = {}

    vars["Food Waste [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Waste [per capita]"],
        }
    )

    pop = pp.act("Population")

    dfs.append(
        pp_utils.make_outputdf(vars, units_cap, param="weighted_avg", weighted_by=pop)
    )

    return pd.concat(dfs, sort=True)


@_register
def retr_food_dem(units):
    """Landuse: Food demand.

    Land-use related food demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Total"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Demand"],
        }
    )

    vars["Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Demand|Crops"],
        }
    )

    vars["Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Demand|Livestock"],
        }
    )

    # This variable is only required for deriving the global value via weighted
    # average.
    pop = pp.act("Population")
    df = pp_utils.make_outputdf(vars, units, param="weighted_avg", weighted_by=pop)
    return df


@_register
def retr_frst_dem(units):
    """Landuse: Forestry product demand.

    Land-use related forestry demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Roundwood"],
        }
    )

    vars["Roundwood|Industrial Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Roundwood|Industrial Roundwood"],
        }
    )

    vars["Roundwood|Wood Fuel"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Demand|Roundwood|Wood Fuel"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_frst_prd(units_residues, units_roundwood):
    """Landuse: Forestry product production.

    Land-use related forestry production.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    dfs = []

    vars = {}
    vars["Forest Residues"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Forest Residues"],
        }
    )
    dfs.append(pp_utils.make_outputdf(vars, units_residues))

    vars = {}
    vars["Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood"],
        }
    )

    vars["Roundwood|Industrial Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood|Industrial Roundwood"],
        }
    )

    vars["Roundwood|Wood Fuel"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood|Wood Fuel"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_roundwood))
    return pd.concat(dfs, sort=True)


@_register
def retr_fgases(
    units_SF6,
    conv_SF6,
    units_CF4,
    conv_CF4,
    units_fgas,
):
    """Emissions: F-Gases.

    Accounting for CF4, SF6 and Total F-Gases.

    Parameters
    ----------

    units_SF6 : str
        Units to which SF6 variables should be converted.
    conv_SF6 : number
        Conversion factor for SF6.
    units_CF4 : str
        Units to which CF4 variables should be converted.
    conv_CF4 : number
        Conversion factor for CF4.
    units_fgas : str
        Units to which F-Gases variables should be converted.
    """

    dfs = []

    vars = {}
    vars["SF6"] = pp.act("SF6_TCE") * conv_SF6
    dfs.append(pp_utils.make_outputdf(vars, units_SF6))

    vars = {}
    vars["CF4"] = pp.act("CF4_TCE") * conv_CF4
    dfs.append(pp_utils.make_outputdf(vars, units_CF4))

    vars = {}
    vars["F-Gases"] = (
        pp.act("CF4_TCE") * 2015.45
        + pp.act("SF6_TCE") * 6218.0
        + (pp.act("HFC_TCE") + pp.act("HFCo_TCE")) * 390.0
    ) * mu["conv_c2co2"]
    dfs.append(pp_utils.make_outputdf(vars, units_fgas))

    return pd.concat(dfs, sort=True)


@_register
def retr_pop(units):
    """Drivers: Population.

    Population total, as well as urban/rural split.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}
    vars["Total"] = pp.act("Population")
    vars["Risk of Hunger"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Population|Risk of Hunger"],
        }
    )

    # Popultion entry into SolWaPOPLink is equal to share of urban population
    # hence the act_rel will return absolue urban population.
    vars["Urban"] = pp.act("Population_Urban")
    vars["Rural"] = pp.act("Population_Rural")

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_demands_input(units):
    """Energy: Useful Energy input.

    Calculates the input across all commodities per demand category.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}
    vars["Input|Feedstocks"] = pp.inp(
        tec=["foil_fs", "loil_fs", "methanol_fs", "ethanol_fs", "gas_fs", "coal_fs"], units = units
    )

    vars["Input|Industrial Thermal"] = pp.inp(
        tec=[
            "foil_i",
            "loil_i",
            "meth_i",
            "eth_i",
            "gas_i",
            "coal_i",
            "biomass_i",
            "elec_i",
            "hp_el_i",
            "hp_gas_i",
            "heat_i",
            "h2_i",
        ],
        units = units,
    ) + pp.out(tec = "solar_i", units = units)

    vars["Input|Industrial Specific"] = pp.inp(
        tec=["sp_liq_I", "sp_meth_I", "sp_eth_I", "sp_el_I", "h2_fc_I", "sp_el_I_RT"], units = units
    ) + pp.out(tec = "solar_pv_I", units = units)

    vars["Input|RC Thermal"] = pp.inp(
        tec=[
            "foil_rc",
            "loil_rc",
            "meth_rc",
            "eth_rc",
            "gas_rc",
            "coal_rc",
            "biomass_rc",
            "elec_rc",
            "hp_el_rc",
            "hp_gas_rc",
            "heat_rc",
            "h2_rc",
        ],
        units = units,
    ) + pp.out(tec = "solar_rc", units = units)

    vars["Input|RC Specific"] = (
        pp.inp(tec=["sp_el_RC", "h2_fc_RC", "sp_el_RC_RT"], units = units)
        + pp.out(tec = "solar_pv_RC", units = units)
        + (
            -1.0
            * pp.out(
                tec = "h2_fc_trp",
                units = units,
                outfilter={"level": ["final"], "commodity": ["electr"]},
            )
        )
    )

    vars["Input|Transport"] = pp.inp(
        tec = [
            "foil_trp",
            "loil_trp",
            "meth_ic_trp",
            "meth_fc_trp",
            "eth_ic_trp",
            "eth_fc_trp",
            "gas_trp",
            "coal_trp",
            "elec_trp",
            "h2_fc_trp",
        ],
        units = units,
    )

    vars["Input|Shipping"] = pp.inp(
        tec = [
            "foil_bunker",
            "loil_bunker",
            "meth_bunker",
            "eth_bunker",
            "LNG_bunker",
            "LH2_bunker",
        ], units = units,
    )

    vars["Input|Non-Commercial Biomass"] = pp.inp(tec = ["biomass_nc"], units = units)

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_demands_output(units):
    """Energy: Useful Energy output.

    Calculates the useful energy per demand category.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Feedstocks"] = pp.dem("i_feed", units)
    vars["Industrial Thermal"] = pp.dem("i_therm", units)
    vars["Industrial Specific"] = pp.dem("i_spec", units)
    vars["RC Thermal"] = pp.dem("rc_therm", units)
    vars["RC Specific"] = pp.dem("rc_spec", units)
    vars["Transport"] = pp.dem("transport", units)
    vars["Shipping"] = pp.dem("shipping", units)
    vars["Non-Commercial Biomass"] = pp.dem("non-comm", units)

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_price(
    units_CPrc_co2,
    units_CPrc_co2_outp,
    units_energy,
    units_energy_outp,
    units_CPrc_c,
    conv_CPrc_co2_to_c,
    units_agri,
):
    """Price: Prices for Emissions, Energy and Land-Use.

    Parameters
    ----------

    units_CPrc_co2 : str
        Units to which carbon price (in CO2 equivalent) should be converted.
    units_CPrc_co2_outp : str
        Units in which carbon price (in CO2 equivalent) should be reported in.
    units_energy : str
        Units to which energy variables should be converted.
    units_energy_outp : str
        Units to which variables should be converted.
    units_CPrc_c : str
        Units to which carbon price (in carbon equivalent) should be converted.
    conv_CPrc_co2_to_c : number
        Conversion factor for carbon price from CO2 to C.
    units_agri : str
        Units to which agricultural variables should be converted.
    """

    dfs = []

    # ----------------------
    # Calculate Carbon Price
    # ----------------------

    vars = {}
    vars["Carbon"] = pp.cprc(units_CPrc_co2)
    dfs.append(pp_utils.make_outputdf(vars, units_CPrc_co2_outp, param="max"))

    # -------------------------------------------------
    # Calculate Final Energy Prices incl. carbon prices
    # -------------------------------------------------

    # Carbon price must also be multiplied by factor as retrieved
    # prices are already converted to $/GJ

    _cbprc = pp.cprc(units_CPrc_c) * conv_CPrc_co2_to_c
    vars = {}
    # Final Energy prices are reported including the carbon price
    vars["Final Energy|Residential|Electricity"] = pp.eneprc(
        enefilter={"commodity": ["electr"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy|Residential|Gases|Natural Gas"] = pp.eneprc(
        enefilter={"commodity": ["gas"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy|Residential|Liquids|Biomass"] = pp.eneprc(
        enefilter={"commodity": ["ethanol"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy|Residential|Liquids|Oil"] = pp.eneprc(
        enefilter={"commodity": ["lightoil"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy|Residential|Solids|Biomass"] = pp.eneprc(
        enefilter={"commodity": ["biomass"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy|Residential|Solids|Coal"] = pp.eneprc(
        enefilter={"commodity": ["coal"], "level": ["final"]}, units=units_energy
    )

    # -------------------------------------------------
    # Calculate Final Energy Prices excl. carbon prices
    # -------------------------------------------------

    vars["Final Energy wo carbon price|Residential|Electricity"] = pp.eneprc(
        enefilter={"commodity": ["electr"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy wo carbon price|Residential|Gases|Natural Gas"] = (
        pp.eneprc(
            enefilter={"commodity": ["gas"], "level": ["final"]}, units=units_energy
        )
        - _cbprc * mu["crbcnt_gas"]
    )

    vars["Final Energy wo carbon price|Residential|Liquids|Biomass"] = pp.eneprc(
        enefilter={"commodity": ["ethanol"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy wo carbon price|Residential|Liquids|Oil"] = (
        pp.eneprc(
            enefilter={"commodity": ["lightoil"], "level": ["final"]},
            units=units_energy,
        )
        - _cbprc * mu["crbcnt_oil"]
    )

    vars["Final Energy wo carbon price|Residential|Solids|Biomass"] = pp.eneprc(
        enefilter={"commodity": ["biomass"], "level": ["final"]}, units=units_energy
    )

    vars["Final Energy wo carbon price|Residential|Solids|Coal"] = (
        pp.eneprc(
            enefilter={"commodity": ["coal"], "level": ["final"]}, units=units_energy
        )
        - _cbprc * mu["crbcnt_coal"]
    )

    # ---------------------------------------------------
    # Calculate Primary Energy Prices excl. carbon prices
    # ---------------------------------------------------

    # Primary energy prices are reported excluding the carbon price, hence the
    # subtraction of the carbonprice * the carbon content of the fuel

    vars["Primary Energy w carbon price|Biomass"] = pp.eneprc(
        enefilter={"commodity": ["biomass"], "level": ["primary"]}, units=units_energy
    )

    vars["Primary Energy|Biomass"] = vars["Primary Energy w carbon price|Biomass"]

    # -----------------------------------------------------
    # Calculate Secondary Energy Prices excl. carbon prices
    # -----------------------------------------------------

    # Secondary energy prices are reported excluding the carbon price, hence the
    # subtraction of the carbonprice * the carbon content of the fuel

    #    vars["Secondary Energy|Gases|Natural Gas"] =pp.eneprc(
    #        enefilter={"commodity": [""],
    #                  "level": ["secondary"]},
    #        units=units_energy)

    #    vars["Secondary Energy|Liquids"] = pp.eneprc(
    #        enefilter={"commodity": [""],
    #                   "level": ["secondary"]},
    #        units=units_energy)

    vars["Secondary Energy|Liquids"] = pp_utils._make_zero()

    #    vars["Secondary Energy|Solids|Biomass"] = pp.eneprc(
    #        enefilter={"commodity": [""],
    #                   "level": ["secondary"]},
    #        units=units_energy)

    #    vars["Secondary Energy|Solids|Coal"] = pp.eneprc(
    #        enefilter={"commodity": [""],
    #                   "level": ["secondary"]},
    #        units=units_energy) - _cbprc * crbcnt_coal

    # Secondary energy prices are reported including the carbon price
    vars["Secondary Energy w carbon price|Liquids"] = pp_utils._make_zero()

    # Agricultural price for energy crops - same units as energy
    vars["Agriculture|Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Price|Agriculture|Energy Crops"],
        },
        units=units_energy,
    )

    dfs.append(pp_utils.make_outputdf(vars, units_energy_outp, param="max"))

    # ------------------------------------------------------
    # Calculate Energy Prices for variables where the GLOBAL
    # value variable differs from regional value variable
    # ------------------------------------------------------

    vars = {}

    # Primary energy prices are reported excluding the carbon price, hence the
    # subtraction of the carbonprice * the carbon content of the fuel

    # Coal uses primary-coal for regions and import-coal for GLB
    vars["Primary Energy w carbon price|Coal"] = pd.concat(
        [
            pp_utils.rem_glb(
                pp.eneprc(
                    enefilter={"commodity": ["coal"], "level": ["primary"]},
                    units=units_energy,
                )
            ),
            pp_utils.rem_reg(
                pp.eneprc(
                    enefilter={"commodity": ["coal"], "level": ["import"]},
                    units=units_energy,
                )
            ),
        ],
        sort=True,
    )

    vars["Primary Energy|Coal"] = vars["Primary Energy w carbon price|Coal"].subtract(
        pp_utils.rem_glb(_cbprc) * mu["crbcnt_coal"], fill_value=0
    )

    # Gas uses primary-gas for regions and import-LNG for GLB
    # Note: GLOBAL region uses LNG prices
    vars["Primary Energy w carbon price|Gas"] = pd.concat(
        [
            pp_utils.rem_glb(
                pp.eneprc(
                    enefilter={"commodity": ["gas"], "level": ["primary"]},
                    units=units_energy,
                )
            ),
            pp_utils.rem_reg(
                pp.eneprc(
                    enefilter={"commodity": ["LNG"], "level": ["import"]},
                    units=units_energy,
                )
            ),
        ],
        sort=True,
    )

    vars["Primary Energy|Gas"] = vars["Primary Energy w carbon price|Gas"].subtract(
        pp_utils.rem_glb(_cbprc) * mu["crbcnt_gas"], fill_value=0
    )

    # Oil uses primary-crudeoil for regions and import-crudeoil for GLB
    vars["Primary Energy w carbon price|Oil"] = pd.concat(
        [
            pp_utils.rem_glb(
                pp.eneprc(
                    enefilter={"commodity": ["crudeoil"], "level": ["secondary"]},
                    units=units_energy,
                )
            ),
            pp_utils.rem_reg(
                pp.eneprc(
                    enefilter={"commodity": ["crudeoil"], "level": ["import"]},
                    units=units_energy,
                )
            ),
        ],
        sort=True,
    )

    vars["Primary Energy|Oil"] = vars["Primary Energy w carbon price|Oil"].subtract(
        pp_utils.rem_glb(_cbprc) * mu["crbcnt_oil"], fill_value=0
    )

    # Secondary energy prices are reported including the carbon price
    # Oil uses secondary-lightoil for regions and import-lightoil for GLB
    vars["Secondary Energy w carbon price|Liquids|Oil"] = pd.concat(
        [
            pp_utils.rem_glb(
                pp.eneprc(
                    enefilter={"commodity": ["lightoil"], "level": ["secondary"]},
                    units=units_energy,
                )
            ),
            pp_utils.rem_reg(
                pp.eneprc(
                    enefilter={"commodity": ["lightoil"], "level": ["import"]},
                    units=units_energy,
                )
            ),
        ],
        sort=True,
    )

    vars["Secondary Energy|Liquids|Oil"] = vars[
        "Secondary Energy w carbon price|Liquids|Oil"
    ].subtract(pp_utils.rem_glb(_cbprc) * mu["crbcnt_oil"], fill_value=0)

    # Biomass uses secondary-ethanol for regions and import-ethanol for GLB
    vars["Secondary Energy w carbon price|Liquids|Biomass"] = pd.concat(
        [
            pp_utils.rem_glb(
                pp.eneprc(
                    enefilter={"commodity": ["ethanol"], "level": ["primary"]},
                    units=units_energy,
                )
            ),
            pp_utils.rem_reg(
                pp.eneprc(
                    enefilter={"commodity": ["ethanol"], "level": ["import"]},
                    units=units_energy,
                )
            ),
        ],
        sort=True,
    )

    # Carbon-price is NOT subtracted
    vars["Secondary Energy|Liquids|Biomass"] = vars[
        "Secondary Energy w carbon price|Liquids|Biomass"
    ]

    # Hydrogen uses secondary-hydrogen for regions and import-lh2 for GLB
    vars["Secondary Energy w carbon price|Hydrogen"] = pd.concat(
        [
            pp_utils.rem_glb(
                pp.eneprc(
                    enefilter={"commodity": ["hydrogen"], "level": ["secondary"]},
                    units=units_energy,
                )
            ),
            pp_utils.rem_reg(
                pp.eneprc(
                    enefilter={"commodity": ["lh2"], "level": ["import"]},
                    units=units_energy,
                )
            ),
        ],
        sort=True,
    )

    # Carbon-price is NOT subtracted
    vars["Secondary Energy|Hydrogen"] = vars["Secondary Energy w carbon price|Hydrogen"]

    dfs.append(pp_utils.make_outputdf(vars, units_energy_outp, glb=False))

    # ------------------------------------------------------
    # Calculate Energy Prices for variables where the GLOBAL
    # value is set to ZERO
    # ------------------------------------------------------

    vars = {}

    vars["Secondary Energy|Electricity"] = pp.eneprc(
        enefilter={"commodity": ["electr"], "level": ["secondary"]}, units=units_energy
    )

    vars["Secondary Energy w carbon price|Electricity"] = pp.eneprc(
        enefilter={"commodity": ["electr"], "level": ["secondary"]}, units=units_energy
    )

    dfs.append(pp_utils.make_outputdf(vars, units_energy_outp, glb=False))

    # ----------------------------------------------------
    # Calculate Agricultural Prices where the global price
    # is derived using a formula provided by GLOBIOM
    # ----------------------------------------------------

    vars = {}

    # Define the technology which represents the quantities used to derive
    # sums accross regions values.
    scale_tec = "Agricultural Production|Crops"

    vars["Agriculture|Food Products [Index]"] = pp.retrieve_lu_price(
        "Price|Agriculture|Food Products [Index]", scale_tec, y0=2020
    )
    vars["Agriculture|Food Products|Crops [Index]"] = pp.retrieve_lu_price(
        "Price|Agriculture|Food Products|Crops [Index]", scale_tec, y0=2020
    )

    scale_tec = "Agricultural Demand|Crops|Maize"

    vars["Agriculture|Food Products|Crops|Maize [Index]"] = pp.retrieve_lu_price(
        "Price|Agriculture|Food Products|Crops|Maize [Index]", scale_tec, y0=2020
    )

    scale_tec = "Agricultural Demand|Crops|Rice"

    vars["Agriculture|Food Products|Crops|Rice [Index]"] = pp.retrieve_lu_price(
        "Price|Agriculture|Food Products|Crops|Rice [Index]", scale_tec, y0=2020
    )

    scale_tec = "Agricultural Demand|Crops|Soybean"

    vars["Agriculture|Food Products|Crops|Soybean [Index]"] = pp.retrieve_lu_price(
        "Price|Agriculture|Food Products|Crops|Soybean [Index]", scale_tec, y0=2020
    )

    scale_tec = "Agricultural Demand|Crops|Wheat"

    vars["Agriculture|Food Products|Crops|Wheat [Index]"] = pp.retrieve_lu_price(
        "Price|Agriculture|Food Products|Crops|Wheat [Index]", scale_tec, y0=2020
    )

    scale_tec = "Forestry Production|Semi-finished|Chemical pulp"

    vars["Forestry|Semi-Finished|Chemical Pulp [Index]"] = pp.retrieve_lu_price(
        "Price|Forestry|Semi-Finished|Chemical Pulp [Index]", scale_tec, y0=2020
    )

    scale_tec = "Forestry Production|Semi-finished|Fiberboard"

    vars["Forestry|Semi-Finished|Fiberboard [Index]"] = pp.retrieve_lu_price(
        "Price|Forestry|Semi-Finished|Fiberboard [Index]", scale_tec, y0=2020
    )

    scale_tec = "Forestry Production|Semi-finished|Mechanical pulp"

    vars["Forestry|Semi-Finished|Mechanical Pulp [Index]"] = pp.retrieve_lu_price(
        "Price|Forestry|Semi-Finished|Mechanical Pulp [Index]", scale_tec, y0=2020
    )

    scale_tec = "Forestry Production|Semi-finished|Plywood"

    vars["Forestry|Semi-Finished|Plywood [Index]"] = pp.retrieve_lu_price(
        "Price|Forestry|Semi-Finished|Plywood [Index]", scale_tec, y0=2020
    )

    scale_tec = "Forestry Production|Semi-finished|Sawnwood"

    vars["Forestry|Semi-Finished|Sawnwood [Index]"] = pp.retrieve_lu_price(
        "Price|Forestry|Semi-Finished|Sawnwood [Index]", scale_tec, y0=2020
    )

    scale_tec = "Agricultural Production|Livestock"

    vars["Agriculture|Food Products|Livestock [Index]"] = pp.retrieve_lu_price(
        "Price|Agriculture|Food Products|Livestock [Index]", scale_tec, y0=2020
    )
    dfs.append(pp_utils.make_outputdf(vars, units_agri, glb=False))

    return pd.concat(dfs, sort=True)


@_register
def retr_globiom_feedback(
    units_emi_CH4,
    units_emi_CO2,
    units_emi_N2O,
    units_ene,
    units_ene_prc,
    units_CPrc_co2,
    units_CPrc_co2_outp,
    units_gdp,
    conv_usd,
):
    dfs = []
    vars = {}
    vars["Emissions|CH4|AFOLU"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CH4|AFOLU"],
        }
    )
    dfs.append(pp_utils.make_outputdf(vars, units_emi_CH4))

    vars = {}
    vars["Emissions|CO2|AFOLU"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU"],
        }
    )
    dfs.append(pp_utils.make_outputdf(vars, units_emi_CO2))

    vars = {}
    vars["Emissions|N2O|AFOLU"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|N2O|AFOLU"],
        }
    )
    dfs.append(pp_utils.make_outputdf(vars, units_emi_N2O))

    vars = {}
    vars["Primary Energy|Biomass"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Primary Energy|Biomass"],
        }
    )
    vars["Primary Energy|Biomass|Energy Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Primary Energy|Biomass|Energy Crops"],
        }
    )

    vars["Primary Energy|Biomass|1st Generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Primary Energy|Biomass|1st Generation"],
        }
    )

    vars["Primary Energy|Biomass|Fuelwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Primary Energy|Biomass|Fuelwood"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_ene))

    # Both the carbon price and the GDP should be reported in US$2005
    vars = {}
    vars["Price|Carbon"] = pp.cprc(units_CPrc_co2)
    vars["Price|Carbon_emulator"] = (
        pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Price|Carbon"],
            },
            units=units_CPrc_co2,
        )
        * 1
        / conv_usd
    )
    dfs.append(pp_utils.make_outputdf(vars, units_CPrc_co2_outp, param="max"))

    vars = {}
    MERtoPPP = pp.par_MERtoPPP()

    # The historic values are filled with the earliest
    # available data (constant). Global values set to zero.
    MERtoPPP = MERtoPPP.replace(0, np.nan).fillna(method="backfill", axis=1).fillna(0)

    if MERtoPPP.sum().sum() == 0:
        vars["GDP|PPP"] = pp.par_PPP()
    else:
        vars["GDP|PPP"] = pp.var_gdp() * MERtoPPP
    vars["GDP|PPP_2017"] = vars["GDP|PPP"] * 1.124
    dfs.append(pp_utils.make_outputdf(vars, units_gdp))

    # Provide "original" biomass price, without any of the additional adjustments
    # too include the abatement of lu-emissions
    vars = {}
    vars["Price|Primary Energy|Biomass"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Price|Primary Energy|Biomass"],
        }
    )
    dfs.append(pp_utils.make_outputdf(vars, units_ene_prc, glb=False))

    return pd.concat(dfs, sort=True)


@_register
def retr_kyoto(units):
    """Emissions: Kyoto Gases.

    Kyoto emissions (all GHG gases).

    Historic values are read from a file which exclude land-use
    emissions. Land-use emissions are added based on values from
    the emulator corresponding variables.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Kyoto Gases"] = pp.emiss("TCE", "all") * mu["conv_c2co2"]

    if run_history == "True":
        # Read historic data from file
        if kyoto_hist_data is not False:
            df_hist = pd.read_csv(kyoto_hist_data).set_index("Region")
            df_hist = df_hist.rename(columns={i: int(i) for i in df_hist.columns})

            # There is no need to add the historic emissions for other lu related
            # emissions, as these remain unchanged in the historic time-period.

            df_new_lu_hist = pp.land_out(
                lu_out_filter={"level": ["land_use_reporting"], "commodity": ["TCE"]},
                units="Mt CO2eq/yr",
            )

            df_hist = df_new_lu_hist.add(df_hist, fill_value=0)
            vars["Kyoto Gases"] = vars["Kyoto Gases"] + df_hist
        # Derive from historic activity
        else:
            # Retrieve FFI related CO2 emissions (convert from MtC to MtCO2)
            co2 = pp.act(["CO2_TCE", "CO2s_TCE", "CO2t_TCE"]) * mu["conv_c2co2"]
            # Retrieve FFI related CH4 emissions
            ch4 = (
                pp.act(["CH4_TCE", "CH4n_TCE", "CH4o_TCE", "CH4s_TCE"])
                * 6.82
                * mu["conv_c2co2"]
            )
            # Retrieve FFI related N2O emissions
            n2o = (
                pp.act(["N2O_TCE", "N2On_TCE", "N2Oo_TCE", "N2Os_TCE"])
                * 81.27
                * mu["conv_c2co2"]
            )

            # Retrieve F-gases: CF4+SF6+HFCs (convert to MtCO2)
            fgases = (
                pp.act("CF4_TCE") * 2015.45
                + pp.act("SF6_TCE") * 6218.0
                + (pp.act("HFC_TCE") + pp.act("HFCo_TCE")) * 390.0
            ) * mu["conv_c2co2"]

            # Retrieve land-use related GHGs: CO2+CH4+N2O
            land_use = pp.land_out(
                lu_out_filter={"level": ["land_use_reporting"], "commodity": ["TCE"]},
                units="Mt CO2eq/yr",
            )

            vars["Kyoto Gases"] += co2 + ch4 + n2o + fgases + land_use

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_macro(units, conv_usd):
    """Macro: GDP and Consumption.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    conv_usd : number
        Conversion factor to convert native to output units.
    """

    vars = {}

    MERtoPPP = pp.par_MERtoPPP()

    # The historic values are filled with the earliest
    # available data (constant). Global values set to zero.
    MERtoPPP = MERtoPPP.replace(0, np.nan).fillna(method="backfill", axis=1).fillna(0)

    vars["MER"] = pp.var_gdp() * conv_usd

    if MERtoPPP.sum().sum() == 0:
        vars["PPP"] = pp.par_PPP() * conv_usd
    else:
        vars["PPP"] = vars["MER"] * MERtoPPP

    vars["Consumption"] = (
        pp.var_consumption() * conv_usd * 1000
    )  # Convert trillion to billion

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_cost(units, conv_usd):
    """Other: Cost nodal.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    conv_usd : number
        Conversion factor to convert native to output units.
    """

    vars = {}

    vars["Cost Nodal Net"] = pp.var_cost() * conv_usd / 1000

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_othemi(var, units):
    """Emissions: Non-CO2/F-Gas emissions.

    Parameters
    ----------

    var : str
        Emissions species to be reported.
    units : str
        Units to which variables should be converted.
    """

    vars = {}

    # ---------------------
    # Agriculture (Table 1)
    # ---------------------

    # -----------------------------
    # 1.1 Agriculture Waste Bruning
    # -----------------------------

    if var in ["BCA", "OCA"]:
        helper_var = var.replace("A", "")
    elif var == "SO2":
        helper_var = "Sulfur"
    else:
        helper_var = var

    AgricultureWasteBurning = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": [f"Emissions|{helper_var}|AFOLU|Agricultural Waste Burning"],
        },
        units=units,
    )

    vars["AFOLU|Agricultural Waste Burning"] = AgricultureWasteBurning

    # ---------------------
    # 1.2 Other Agriculture
    # ---------------------

    if var in "CH4":
        vars["AFOLU|Agriculture|Livestock|Enteric Fermentation"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": [
                    "Emissions|CH4|AFOLU|Agriculture|Livestock|" "Enteric Fermentation"
                ],
            }
        )

        vars["AFOLU|Agriculture|Livestock|Manure Management"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": [
                    "Emissions|CH4|AFOLU|Agriculture|Livestock|" "Manure Management"
                ],
            }
        )

        vars["AFOLU|Agriculture|Livestock"] = (
            vars["AFOLU|Agriculture|Livestock|Enteric Fermentation"]
            + vars["AFOLU|Agriculture|Livestock|Manure Management"]
        )

        vars["AFOLU|Agriculture|Rice"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|CH4|AFOLU|Agriculture|Rice"],
            }
        )

        Agriculture = (
            vars["AFOLU|Agriculture|Livestock|Enteric Fermentation"]
            + vars["AFOLU|Agriculture|Livestock|Manure Management"]
            + vars["AFOLU|Agriculture|Rice"]
        )

    elif var in "NH3":
        vars["AFOLU|Agriculture|Livestock|Manure Management"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["NH3_ManureEM"],
            },
            units=units,
        )

        vars["AFOLU|Agriculture|Livestock"] = vars[
            "AFOLU|Agriculture|Livestock|Manure Management"
        ]

        vars["AFOLU|Agriculture|Rice"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["NH3_RiceEM"],
            },
            units=units,
        )

        vars["AFOLU|Agriculture|Managed Soils"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["NH3_SoilEM"],
            },
            units=units,
        )

        Agriculture = (
            vars["AFOLU|Agriculture|Livestock|Manure Management"]
            + vars["AFOLU|Agriculture|Rice"]
            + vars["AFOLU|Agriculture|Managed Soils"]
        )

    elif var in "NOx":
        vars["AFOLU|Agriculture|Managed Soils"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["NOx_SoilEM"],
            },
            units=units,
        )

        Agriculture = vars["AFOLU|Agriculture|Managed Soils"]

    elif var in "N2O":
        #        vars["AFOLU|Land|Land Use and Land-Use Change"] = pp.land_out(
        #            lu_out_filter={
        #                "level": ["land_use_reporting"],
        #                "commodity": ["Emissions|N2O|AFOLU|Land|Grassland Pastures"],
        #            },
        #            units=units,
        #        )

        vars["AFOLU|Agriculture|Managed Soils"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|N2O|AFOLU|Agriculture|Managed Soils"],
            }
        )

        vars["AFOLU|Agriculture|Livestock|Manure Management"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": [
                    "Emissions|N2O|AFOLU|Agriculture|" "Livestock|Manure Management"
                ],
            }
        )

        vars["AFOLU|Agriculture|Livestock"] = vars[
            "AFOLU|Agriculture|Livestock|Manure Management"
        ]

        Agriculture = (
            vars["AFOLU|Agriculture|Managed Soils"]
            + vars["AFOLU|Agriculture|Livestock|Manure Management"]
        )

    else:
        Agriculture = pp_utils._make_zero()

    vars["AFOLU|Agriculture"] = Agriculture

    # ---------------------------
    # Grassland Burning (Table 2)
    # ---------------------------

    GrasslandBurning = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": [f"Emissions|{helper_var}|AFOLU|Fires|Grassland Burning"],
        },
        units=units,
    )

    vars["AFOLU|Land|Fires|Grassland Burning"] = GrasslandBurning

    # ------------------------
    # Forest Burning (Table 3)
    # ------------------------

    ForestBurning = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": [f"Emissions|{helper_var}|AFOLU|Fires|Forest Burning"],
        },
        units=units,
    )

    vars["AFOLU|Land|Fires|Forest Burning"] = ForestBurning

    # ------------------------
    # Peat Burning (Table 3)
    # ------------------------

    ForestBurning = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": [f"Emissions|{helper_var}|AFOLU|Fires|Peat Burning"],
        },
        units=units,
    )

    vars["AFOLU|Land|Fires|Peat Burning"] = ForestBurning

    # ------------------
    # Aircraft (Table 4)
    # ------------------

    Aircraft = pp.emi(
        tec="aviation_Emission",
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Demand|Bunkers|International Aviation"] = Aircraft

    # -----------------------------------------
    # Electricity and heat production (Table 5)
    # -----------------------------------------

    Heat = pp.emi(
        tec=["bio_hpl", "coal_hpl", "foil_hpl", "gas_hpl"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Supply|Heat"] = Heat

    Powergeneration = pp.emi(
        tec=[
            "bio_istig",
            "bio_ppl",
            "coal_adv",
            "coal_ppl_u",
            "igcc",
            "coal_ppl",
            "foil_ppl",
            "gas_cc",
            "gas_ppl",
            "loil_cc",
            "loil_ppl",
            "oil_ppl",
            "coal_adv_ccs",
            "igcc_ccs",
            "gas_cc_ccs",
            "gas_ct",
            "bio_istig_ccs",
        ],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Supply|Electricity"] = Powergeneration

    gases_extraction = pp.emi(
        tec=[
            "gas_extr_1",
            "gas_extr_2",
            "gas_extr_3",
            "gas_extr_4",
            "gas_extr_5",
            "gas_extr_6",
            "gas_extr_7",
            "gas_extr_8",
        ],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_extraction_fugitive = pp.emi(
        tec=["flaring_CO2"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_transportation = pp.emi(
        tec=["gas_t_d_ch4", "gas_t_d"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_coal = pp.emi(
        tec=["coal_gas"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_hydrogen = pp.emi(
        tec=["h2_smr", "h2_smr_ccs", "h2_coal", "h2_coal_ccs", "h2_bio", "h2_bio_ccs"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_extraction = pp.emi(
        tec=[
            "oil_extr_1_ch4",
            "oil_extr_1",
            "oil_extr_2_ch4",
            "oil_extr_2",
            "oil_extr_3_ch4",
            "oil_extr_3",
            "oil_extr_4_ch4",
            "oil_extr_4",
            "oil_extr_5",
            "oil_extr_6",
            "oil_extr_7",
            "oil_extr_8",
        ],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_transportation = pp.emi(
        tec=["loil_t_d", "foil_t_d", "meth_t_d", "eth_t_d"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_oil = pp.emi(
        tec=["ref_lol", "ref_hil"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_gas = pp.emi(
        tec=["meth_ng", "meth_ng_ccs"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_coal = pp.emi(
        tec=["meth_coal", "meth_coal_ccs", "syn_liq", "syn_liq_ccs"],
        units= "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_biomass = pp.emi(
        tec=["eth_bio", "eth_bio_ccs", "liq_bio", "liq_bio_ccs"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    solid_extraction = pp.emi(
        tec=["coal_extr_ch4", "coal_extr", "lignite_extr"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    solid_transportation = pp.emi(
        tec=["coal_t_d", "biomass_t_d"],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    # --------------------------------------------
    # Allocation of supply emissions to categories
    # --------------------------------------------

    # Supply emissions for CH4, NH3, VOC are all allocated to fugitive emissions.
    # The remainder are combustion related.
    if var in ["CH4", "NH3", "VOC"]:
        vars["Energy|Supply|Gases|Biomass|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Biomass|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Coal|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Coal|Fugitive"] = gases_coal
        vars["Energy|Supply|Gases|Extraction|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Extraction|Fugitive"] = (
            gases_extraction_fugitive + gases_extraction
        )
        vars["Energy|Supply|Gases|Hydrogen|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Hydrogen|Fugitive"] = gases_hydrogen
        vars["Energy|Supply|Gases|Natural Gas|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Natural Gas|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Transportation|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Transportation|Fugitive"] = gases_transportation

        vars["Energy|Supply|Liquids|Biomass|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Biomass|Fugitive"] = liquids_biomass
        vars["Energy|Supply|Liquids|Coal|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Coal|Fugitive"] = liquids_coal
        vars["Energy|Supply|Liquids|Extraction|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Extraction|Fugitive"] = liquids_extraction
        vars["Energy|Supply|Liquids|Natural Gas|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Natural Gas|Fugitive"] = liquids_gas
        vars["Energy|Supply|Liquids|Oil|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Oil|Fugitive"] = liquids_oil
        vars["Energy|Supply|Liquids|Transportation|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Transportation|Fugitive"] = liquids_transportation

        vars["Energy|Supply|Other|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Other|Fugitive"] = pp_utils._make_zero()

        vars["Energy|Supply|Solids|Biomass|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Biomass|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Coal|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Coal|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Extraction|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Extraction|Fugitive"] = solid_extraction
        vars["Energy|Supply|Solids|Transportation|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Transportation|Fugitive"] = solid_transportation

    else:
        vars["Energy|Supply|Gases|Biomass|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Biomass|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Coal|Combustion"] = gases_coal
        vars["Energy|Supply|Gases|Coal|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Extraction|Combustion"] = gases_extraction
        vars["Energy|Supply|Gases|Extraction|Fugitive"] = gases_extraction_fugitive
        vars["Energy|Supply|Gases|Hydrogen|Combustion"] = gases_hydrogen
        vars["Energy|Supply|Gases|Hydrogen|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Natural Gas|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Natural Gas|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Gases|Transportation|Combustion"] = gases_transportation
        vars["Energy|Supply|Gases|Transportation|Fugitive"] = pp_utils._make_zero()

        vars["Energy|Supply|Liquids|Biomass|Combustion"] = liquids_biomass
        vars["Energy|Supply|Liquids|Biomass|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Coal|Combustion"] = liquids_coal
        vars["Energy|Supply|Liquids|Coal|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Extraction|Combustion"] = liquids_extraction
        vars["Energy|Supply|Liquids|Extraction|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Natural Gas|Combustion"] = liquids_gas
        vars["Energy|Supply|Liquids|Natural Gas|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Oil|Combustion"] = liquids_oil
        vars["Energy|Supply|Liquids|Oil|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Liquids|Transportation|Combustion"] = liquids_transportation
        vars["Energy|Supply|Liquids|Transportation|Fugitive"] = pp_utils._make_zero()

        vars["Energy|Supply|Other|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Other|Fugitive"] = pp_utils._make_zero()

        vars["Energy|Supply|Solids|Biomass|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Biomass|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Coal|Combustion"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Coal|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Extraction|Combustion"] = solid_extraction
        vars["Energy|Supply|Solids|Extraction|Fugitive"] = pp_utils._make_zero()
        vars["Energy|Supply|Solids|Transportation|Combustion"] = solid_transportation
        vars["Energy|Supply|Solids|Transportation|Fugitive"] = pp_utils._make_zero()

    # -------------------------------
    # Industrial Combustion (Table 7)
    # -------------------------------

    SpecificInd = pp.emi(
        tec=[
            "sp_coal_I",
            "sp_el_I",
            "sp_el_I_RT",
            "sp_liq_I",
            "sp_meth_I",
            "sp_eth_I",
            "solar_pv_I",
            "h2_fc_I",
            "sp_h2_I",
        ],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    ThermalInd = pp.emi(
        tec=[
            "coal_i",
            "foil_i",
            "loil_i",
            "gas_i",
            "meth_i",
            "eth_i",
            "h2_i",
            "biomass_i",
            "elec_i",
            "heat_i",
            "hp_el_i",
            "hp_gas_i",
            "solar_i",
        ],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    addvarInd = pp_utils._make_zero()

    IndustrialCombustion = SpecificInd + ThermalInd + addvarInd

    vars["Energy|Demand|Industry"] = IndustrialCombustion

    # --------------------------------------------
    # Industrial process and product use (Table 8)
    # --------------------------------------------

    if var in ["BCA", "OCA", "SO2", "NH3"]:
        NonEnergyInd = pp.emi(
            tec=f"{var}_IndNonEnergyEM",
            units="GWa",
            emifilter={"relation": [f"{var}_Emission"]},
            emission_units=units,
        )

    elif var in ["CO", "NOx", "VOC"]:
        NonEnergyInd = pp.emi(
            tec=f"{var}_IndNonEnergyEM",
            units="GWa",    
            emifilter={"relation": [f"{var}_nonenergy"]},
            emission_units=units,
        )

    elif var in ["CH4"]:
        NonEnergyInd = pp.emi(
            tec=f"{var}_IndNonEnergyEM",
            units="GWa",
            emifilter={"relation": [f"{var}_new_Emission"]},
            emission_units=units,
        )

    else:
        NonEnergyInd = pp_utils._make_zero()

    addvarNEIND = pp_utils._make_zero()

    if var == "N2O":
        addvarNEIND = addvarNEIND + pp.emi(
            tec=[
                "N2On_nitric",
                "N2On_adipic",
                "adipic_thermal",
                "nitric_catalytic1",
                "nitric_catalytic2",
                "nitric_catalytic3",
                "nitric_catalytic4",
                "nitric_catalytic5",
                "nitric_catalytic6",
            ],
            units="GWa",
            emifilter={"relation": [f"{var}_nonenergy"]},
            emission_units=units,
        )

    vars["Industrial Processes"] = NonEnergyInd + addvarNEIND

    # --------------------------------
    # International shipping (Table 9)
    # --------------------------------

    if var == "NH3":
        Bunker = pp_utils._make_zero()
    else:
        Bunker = pp.emi(
            tec=[
                "foil_bunker",
                "loil_bunker",
                "eth_bunker",
                "meth_bunker",
                "LNG_bunker",
                "LH2_bunker",
            ],
            units="GWa",
            emifilter={"relation": [f"{var}_Emission_bunkers"]},
            emission_units=units,
        )

    vars["Energy|Demand|Bunkers|International Shipping"] = Bunker

    # -------------------------------------------------
    # Residential, Commercial, Other - Other (Table 11)
    # -------------------------------------------------

    if var == "N2O":
        ResComOth = pp.emi(
            tec=f"{var}_OtherAgSo",
            units="GWa",
            emifilter={"relation": [f"{var}other"]},
            emission_units=units,
        ) + pp.emi(
            tec=f"{var}_ONonAgSo",
            units="GWa",
            emifilter={"relation": [f"{var}other"]},
            emission_units=units,
        )

    else:
        ResComOth = pp_utils._make_zero()

    vars["Energy|Demand|AFOFI"] = ResComOth

    # -------------------------------------------------------------------
    # Residential, Commercial, Other - Residential, Commercial (Table 12)
    # -------------------------------------------------------------------

    ResComSpec = pp.emi(
        tec=["sp_el_RC", "solar_pv_RC", "h2_fc_RC", "sp_el_RC_RT"],
        units= "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    ResComTherm = pp.emi(
        tec=[
            "coal_rc",
            "foil_rc",
            "loil_rc",
            "gas_rc",
            "elec_rc",
            "biomass_rc",
            "heat_rc",
            "meth_rc",
            "eth_rc",
            "h2_rc",
            "hp_el_rc",
            "hp_gas_rc",
            "solar_rc",
        ],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    ResComNC = pp.emi(
        tec="biomass_nc",
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    OtherSC = pp.emi(
        tec="other_sc",
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    addvarRC = pp_utils._make_zero()

    vars["Energy|Demand|Residential and Commercial"] = (
        ResComSpec + ResComTherm + ResComNC + OtherSC + addvarRC
    )

    # ------------------------------
    # Road transportation (Table 13)
    # ------------------------------

    Transport = pp.emi(
        tec=[
            "coal_trp",
            "foil_trp",
            "loil_trp",
            "gas_trp",
            "elec_trp",
            "meth_ic_trp",
            "eth_ic_trp",
            "meth_fc_trp",
            "eth_fc_trp",
            "h2_fc_trp",
        ],
        units="GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"] = Transport

    # ----------------------------------------------
    # Solvents production and application (Table 14)
    # ----------------------------------------------

    if var == "VOC":
        Solvents = pp.emi(
            tec=f"{var}_SolventEM",
            units="GWa",
            emifilter={"relation": [f"{var}_nonenergy"]},
            emission_units=units,
        )

    else:
        Solvents = pp_utils._make_zero()

    vars["Product Use|Solvents"] = Solvents

    # ----------------
    # Waste (Table 15)
    # ----------------

    if var == "N2O":
        Waste = pp.emi(
            tec=f"{var}_Human",
            units="GWa",
            emifilter={"relation": [f"{var}other"]},
            emission_units=units,
        )

    elif var == "CH4":
        Waste = pp.emi(
            tec=["CH4_WasteBurnEM", "CH4_DomWasteWa", "CH4_IndWasteWa"],
            units="GWa",
            emifilter={"relation": ["CH4_new_Emission"]},
            emission_units=units,
        ) + pp.emi(
            tec=[
                "CH4n_landfills",
                "landfill_digester1",
                "landfill_compost1",
                "landfill_mechbio",
                "landfill_heatprdn",
                "landfill_direct1",
                "landfill_ele",
                "landfill_flaring",
            ],
            units="GWa",
            emifilter={"relation": ["CH4_nonenergy"]},
            emission_units=units,
        )

    elif var in ["VOC", "CO", "NOx"]:
        Waste = pp.emi(
            tec=f"{var}_WasteBurnEM",
            units="GWa",
            emifilter={"relation": [f"{var}_nonenergy"]},
            emission_units=units,
        )

    elif var in ["BCA", "OCA", "SO2"]:
        Waste = pp.emi(
            tec=f"{var}_WasteBurnEM",
            units="GWa",
            emifilter={"relation": [f"{var}_Emission"]},
            emission_units=units,
        )

    else:
        Waste = pp_utils._make_zero()

    vars["Waste"] = Waste

    # ---------------------------------------------------------------------------
    # Allocation of supply to special aggregates & demand emissions to categories
    # ---------------------------------------------------------------------------

    if var in ["NH3", "VOC"]:
        # Special Aggregates which cannot be treated generically
        vars["Energy|Supply|Combustion"] = (
            vars["Energy|Supply|Gases|Biomass|Combustion"]
            + vars["Energy|Supply|Gases|Coal|Combustion"]
            + vars["Energy|Supply|Gases|Extraction|Combustion"]
            + vars["Energy|Supply|Gases|Hydrogen|Combustion"]
            + vars["Energy|Supply|Gases|Natural Gas|Combustion"]
            + vars["Energy|Supply|Gases|Transportation|Combustion"]
            + vars["Energy|Supply|Liquids|Biomass|Combustion"]
            + vars["Energy|Supply|Liquids|Coal|Combustion"]
            + vars["Energy|Supply|Liquids|Extraction|Combustion"]
            + vars["Energy|Supply|Liquids|Natural Gas|Combustion"]
            + vars["Energy|Supply|Liquids|Oil|Combustion"]
            + vars["Energy|Supply|Liquids|Transportation|Combustion"]
            + vars["Energy|Supply|Other|Combustion"]
            + vars["Energy|Supply|Solids|Biomass|Combustion"]
            + vars["Energy|Supply|Solids|Coal|Combustion"]
            + vars["Energy|Supply|Solids|Extraction|Combustion"]
            + vars["Energy|Supply|Solids|Transportation|Combustion"]
        )

        vars["Energy|Combustion"] = vars["Energy|Supply|Combustion"]

        vars["Energy|Supply|Fugitive"] = (
            vars["Energy|Supply|Heat"]
            + vars["Energy|Supply|Electricity"]
            + vars["Energy|Supply|Gases|Biomass|Fugitive"]
            + vars["Energy|Supply|Gases|Coal|Fugitive"]
            + vars["Energy|Supply|Gases|Extraction|Fugitive"]
            + vars["Energy|Supply|Gases|Hydrogen|Fugitive"]
            + vars["Energy|Supply|Gases|Natural Gas|Fugitive"]
            + vars["Energy|Supply|Gases|Transportation|Fugitive"]
            + vars["Energy|Supply|Liquids|Biomass|Fugitive"]
            + vars["Energy|Supply|Liquids|Coal|Fugitive"]
            + vars["Energy|Supply|Liquids|Extraction|Fugitive"]
            + vars["Energy|Supply|Liquids|Natural Gas|Fugitive"]
            + vars["Energy|Supply|Liquids|Oil|Fugitive"]
            + vars["Energy|Supply|Liquids|Transportation|Fugitive"]
            + vars["Energy|Supply|Other|Fugitive"]
            + vars["Energy|Supply|Solids|Biomass|Fugitive"]
            + vars["Energy|Supply|Solids|Coal|Fugitive"]
            + vars["Energy|Supply|Solids|Extraction|Fugitive"]
            + vars["Energy|Supply|Solids|Transportation|Fugitive"]
        )

        vars["Energy|Fugitive"] = (
            vars["Energy|Supply|Fugitive"]
            + vars["Energy|Demand|Bunkers|International Shipping"]
            + vars["Energy|Demand|AFOFI"]
            + vars["Energy|Demand|Industry"]
            + vars["Energy|Demand|Residential and Commercial"]
            + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
            + vars["Energy|Demand|Bunkers|International Aviation"]
        )

    elif var in ["CH4"]:
        # Special Aggregates which cannot be treated generically
        vars["Energy|Supply|Combustion"] = (
            vars["Energy|Supply|Gases|Biomass|Combustion"]
            + vars["Energy|Supply|Gases|Coal|Combustion"]
            + vars["Energy|Supply|Gases|Extraction|Combustion"]
            + vars["Energy|Supply|Gases|Hydrogen|Combustion"]
            + vars["Energy|Supply|Gases|Natural Gas|Combustion"]
            + vars["Energy|Supply|Gases|Transportation|Combustion"]
            + vars["Energy|Supply|Liquids|Biomass|Combustion"]
            + vars["Energy|Supply|Liquids|Coal|Combustion"]
            + vars["Energy|Supply|Liquids|Extraction|Combustion"]
            + vars["Energy|Supply|Liquids|Natural Gas|Combustion"]
            + vars["Energy|Supply|Liquids|Oil|Combustion"]
            + vars["Energy|Supply|Liquids|Transportation|Combustion"]
            + vars["Energy|Supply|Other|Combustion"]
            + vars["Energy|Supply|Solids|Biomass|Combustion"]
            + vars["Energy|Supply|Solids|Coal|Combustion"]
            + vars["Energy|Supply|Solids|Extraction|Combustion"]
            + vars["Energy|Supply|Solids|Transportation|Combustion"]
        )

        vars["Energy|Combustion"] = vars["Energy|Supply|Combustion"] + ResComNC

        vars["Energy|Supply|Fugitive"] = (
            vars["Energy|Supply|Heat"]
            + vars["Energy|Supply|Electricity"]
            + vars["Energy|Supply|Gases|Biomass|Fugitive"]
            + vars["Energy|Supply|Gases|Coal|Fugitive"]
            + vars["Energy|Supply|Gases|Extraction|Fugitive"]
            + vars["Energy|Supply|Gases|Hydrogen|Fugitive"]
            + vars["Energy|Supply|Gases|Natural Gas|Fugitive"]
            + vars["Energy|Supply|Gases|Transportation|Fugitive"]
            + vars["Energy|Supply|Liquids|Biomass|Fugitive"]
            + vars["Energy|Supply|Liquids|Coal|Fugitive"]
            + vars["Energy|Supply|Liquids|Extraction|Fugitive"]
            + vars["Energy|Supply|Liquids|Natural Gas|Fugitive"]
            + vars["Energy|Supply|Liquids|Oil|Fugitive"]
            + vars["Energy|Supply|Liquids|Transportation|Fugitive"]
            + vars["Energy|Supply|Other|Fugitive"]
            + vars["Energy|Supply|Solids|Biomass|Fugitive"]
            + vars["Energy|Supply|Solids|Coal|Fugitive"]
            + vars["Energy|Supply|Solids|Extraction|Fugitive"]
            + vars["Energy|Supply|Solids|Transportation|Fugitive"]
        )

        vars["Energy|Fugitive"] = (
            vars["Energy|Supply|Fugitive"]
            + vars["Energy|Demand|Bunkers|International Shipping"]
            + vars["Energy|Demand|AFOFI"]
            + vars["Energy|Demand|Industry"]
            + vars["Energy|Demand|Residential and Commercial"]
            - ResComNC
            + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
            + vars["Energy|Demand|Bunkers|International Aviation"]
        )

    else:
        # Special Aggregates which cannot be treated generically
        vars["Energy|Supply|Combustion"] = (
            vars["Energy|Supply|Heat"]
            + vars["Energy|Supply|Electricity"]
            + vars["Energy|Supply|Gases|Biomass|Combustion"]
            + vars["Energy|Supply|Gases|Coal|Combustion"]
            + vars["Energy|Supply|Gases|Extraction|Combustion"]
            + vars["Energy|Supply|Gases|Hydrogen|Combustion"]
            + vars["Energy|Supply|Gases|Natural Gas|Combustion"]
            + vars["Energy|Supply|Gases|Transportation|Combustion"]
            + vars["Energy|Supply|Liquids|Biomass|Combustion"]
            + vars["Energy|Supply|Liquids|Coal|Combustion"]
            + vars["Energy|Supply|Liquids|Extraction|Combustion"]
            + vars["Energy|Supply|Liquids|Natural Gas|Combustion"]
            + vars["Energy|Supply|Liquids|Oil|Combustion"]
            + vars["Energy|Supply|Liquids|Transportation|Combustion"]
            + vars["Energy|Supply|Other|Combustion"]
            + vars["Energy|Supply|Solids|Biomass|Combustion"]
            + vars["Energy|Supply|Solids|Coal|Combustion"]
            + vars["Energy|Supply|Solids|Extraction|Combustion"]
            + vars["Energy|Supply|Solids|Transportation|Combustion"]
        )

        vars["Energy|Combustion"] = (
            vars["Energy|Supply|Combustion"]
            + vars["Energy|Demand|Bunkers|International Shipping"]
            + vars["Energy|Demand|AFOFI"]
            + vars["Energy|Demand|Industry"]
            + vars["Energy|Demand|Residential and Commercial"]
            + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
            + vars["Energy|Demand|Bunkers|International Aviation"]
        )

        vars["Energy|Supply|Fugitive"] = (
            vars["Energy|Supply|Gases|Biomass|Fugitive"]
            + vars["Energy|Supply|Gases|Coal|Fugitive"]
            + vars["Energy|Supply|Gases|Extraction|Fugitive"]
            + vars["Energy|Supply|Gases|Hydrogen|Fugitive"]
            + vars["Energy|Supply|Gases|Natural Gas|Fugitive"]
            + vars["Energy|Supply|Gases|Transportation|Fugitive"]
            + vars["Energy|Supply|Liquids|Biomass|Fugitive"]
            + vars["Energy|Supply|Liquids|Coal|Fugitive"]
            + vars["Energy|Supply|Liquids|Extraction|Fugitive"]
            + vars["Energy|Supply|Liquids|Natural Gas|Fugitive"]
            + vars["Energy|Supply|Liquids|Oil|Fugitive"]
            + vars["Energy|Supply|Liquids|Transportation|Fugitive"]
            + vars["Energy|Supply|Other|Fugitive"]
            + vars["Energy|Supply|Solids|Biomass|Fugitive"]
            + vars["Energy|Supply|Solids|Coal|Fugitive"]
            + vars["Energy|Supply|Solids|Extraction|Fugitive"]
            + vars["Energy|Supply|Solids|Transportation|Fugitive"]
        )

        vars["Energy|Fugitive"] = vars["Energy|Supply|Fugitive"]

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_CO2_CCS(units_emi, units_ene):
    """Carbon sequestration.

    Energy and land-use related carbon seuqestration.

    Parameters
    ----------

    units_emi : str
        Units to which emission variables should be converted.
    units_ene : str
        Units to which energy variables should be converted.
    """

    vars = {}

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    # Biogas share calculation
    _Biogas = pp.out(tec = "gas_bio", units = units_ene)

    _gas_inp_tecs = [
        "gas_ppl",
        "gas_cc",
        "gas_cc_ccs",
        "gas_ct",
        "gas_hpl",
        "meth_ng",
        "meth_ng_ccs",
        "h2_smr",
        "h2_smr_ccs",
        "gas_t_d",
        "gas_t_d_ch4",
    ]

    _totgas = pp.inp(tec = _gas_inp_tecs, units = units_ene, inpfilter={"commodity": ["gas"]})

    _BGas_share = (_Biogas / _totgas).fillna(0)

    # Calulation of CCS components

    _CCS_coal_elec = -1.0 * pp.emi(
        tec=["c_ppl_co2scr", "coal_adv_ccs", "igcc_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_liq = -1.0 * pp.emi(
        tec=["syn_liq_ccs", "meth_coal_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_hydrogen = -1.0 * pp.emi(
        tec="h2_coal_ccs",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_cement = -1.0 * pp.emi(
        tec="cement_co2scr",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_elec = -1.0 * pp.emi(
        tec=["bio_ppl_co2scr", "bio_istig_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_liq = -1.0 * pp.emi(
        tec=["eth_bio_ccs", "liq_bio_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_hydrogen = -1.0 * pp.emi(
        tec="h2_bio_ccs",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_elec = (
        -1.0
        * pp.emi(
            tec=["g_ppl_co2scr", "gas_cc_ccs"],
            units="GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    _CCS_gas_liq = (
        -1.0
        * pp.emi(
            tec="meth_ng_ccs",
            units="GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    _CCS_gas_hydrogen = (
        -1.0
        * pp.emi(
            tec="h2_smr_ccs",
            units="GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    vars["CCS|Fossil|Energy|Supply|Electricity"] = _CCS_coal_elec + _CCS_gas_elec

    vars["CCS|Fossil|Energy|Supply|Liquids"] = _CCS_coal_liq + _CCS_gas_liq

    vars["CCS|Fossil|Energy|Supply|Hydrogen"] = _CCS_coal_hydrogen + _CCS_gas_hydrogen

    vars["CCS|Biomass|Energy|Supply|Electricity"] = (
        _CCS_bio_elec + _CCS_gas_elec * _BGas_share
    )

    vars["CCS|Biomass|Energy|Supply|Liquids"] = (
        _CCS_bio_liq + _CCS_gas_liq * _BGas_share
    )

    vars["CCS|Biomass|Energy|Supply|Hydrogen"] = (
        _CCS_bio_hydrogen + _CCS_gas_hydrogen * _BGas_share
    )

    vars["CCS|Industrial Processes"] = _CCS_cement

    # Land Use
    vars["Land Use|Afforestation"] = -pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Afforestation"],
        }
    )

    # For several of the landuse related variables, values can be both negative and positive.
    # Therefore, filtering is applied for all negative values which are then mutliplied by -1,
    # as sequestration is reported as a positive number.

    _seq_biochar = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Biochar"],
        }
    )
    vars["Land Use|Agriculture|Biochar"] = _seq_biochar[_seq_biochar < 0].fillna(0) * -1

    _seq_agri_silvo = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Silvopasture"],
        }
    )

    vars["Land Use|Agriculture|Silvopasture"] = (
        _seq_agri_silvo[_seq_agri_silvo < 0].fillna(0) * -1
    )

    _seq_agri_soil_crp = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Soil Carbon|Cropland"],
        }
    )

    vars["Land Use|Agriculture|Soil Carbon|Cropland"] = (
        _seq_agri_soil_crp[_seq_agri_soil_crp < 0].fillna(0) * -1
    )

    _seq_agri_soil_pst = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Soil Carbon|Pasture"],
        }
    )

    vars["Land Use|Agriculture|Soil Carbon|Pasture"] = (
        _seq_agri_soil_pst[_seq_agri_soil_pst < 0].fillna(0) * -1
    )

    _seq_frst_mgt = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Forest Management"],
        }
    )

    vars["Land Use|Forest Management"] = _seq_frst_mgt[_seq_frst_mgt < 0].fillna(0) * -1

    _seq_oth = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Other"],
        }
    )

    vars["Land Use|Other"] = _seq_oth[_seq_oth < 0].fillna(0) * -1

    _seq_oth_luc = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Other LUC"],
        }
    )

    vars["Land Use|Other LUC"] = _seq_oth_luc[_seq_oth_luc < 0].fillna(0) * -1

    df = pp_utils.make_outputdf(vars, units_emi)
    return df


@_register
def retr_hfc(hfc_lst):
    """Emissions: HFCs.

    Energy and lnad-use related carbon seuqestration.

    Parameters
    ----------

    hfc_lst : dict
        Dictionary {<HFC species>: [<active (boolean)>,
                                    <fil file suffix (number)>,
                                    <output unit (str)>].
    """

    dfs = []
    _HFC_refAC = pp.act_rel(
        [
            "solar_pv_RC",
            "sp_el_RC",
            "sp_el_RC_RT",
            "back_rc2",
            "h2_fc_RC",
            "leak_repair",
            "ammonia_secloop",
            "refrigerant_recovery",
        ],
        relfilter={"relation": ["HFC_Emission"]},
    )

    _HFC_mobVac = pp.act_rel(
        [
            "gas_trp",
            "elec_trp",
            "foil_trp",
            "loil_trp",
            "meth_ic_trp",
            "meth_fc_trp",
            "eth_ic_trp",
            "mvac_co2",
            "eth_fc_trp",
            "h2_fc_trp",
        ],
        relfilter={"relation": ["HFC_Emission"]},
    )

    _HFC_foam = pp.act_rel(
        [
            "coal_rc",
            "foil_rc",
            "loil_rc",
            "gas_rc",
            "biomass_rc",
            "elec_rc",
            "heat_rc",
            "meth_rc",
            "h2_rc",
            "hp_el_rc",
            "hp_gas_rc",
            "h2_fc_RC",
            "solar_rc",
            "back_rc",
            "eth_rc",
            "repl_hc",
        ],
        relfilter={"relation": ["HFC_Emission"]},
    )

    _HFC_Solvent = pp.act_rel("HFC_Solvents", relfilter={"relation": ["HFCequivOther"]})

    _HFC_FireExtinguishers = pp.act_rel(
        "HFC_FireExt", relfilter={"relation": ["HFCequivOther"]}
    )

    _HFC_AerosolNonMDI = pp.act_rel(
        "HFC_AerosolNonMDI", relfilter={"relation": ["HFCequivOther"]}
    )

    # -----------------------------
    # Calculate Total in 134a equiv
    # -----------------------------

    for hfc in hfc_lst:
        vars = {}
        run = hfc_lst[hfc][0]
        fil = hfc_lst[hfc][1]
        units = hfc_lst[hfc][2]

        if run is False:
            continue
        if run == "empty":
            vars[hfc] = pp_utils._make_zero()
        elif hfc == "Total":
            vars[hfc] = (
                _HFC_refAC
                + _HFC_mobVac
                + _HFC_foam
                + _HFC_Solvent
                + _HFC_FireExtinguishers
                + _HFC_AerosolNonMDI
            )
        else:
            vars[hfc] = (
                pp_utils.fil(_HFC_refAC, "HFC_fac", f"refAC{fil}", units)
                + pp_utils.fil(_HFC_foam, "HFC_fac", f"foam{fil}", units)
                + pp_utils.fil(_HFC_FireExtinguishers, "HFC_fac", f"Fire{fil}", units)
                + pp_utils.fil(_HFC_AerosolNonMDI, "HFC_fac", f"ANMDI{fil}", units)
                + pp_utils.fil(_HFC_mobVac, "HFC_fac", f"mVac{fil}", units)
                + pp_utils.fil(_HFC_Solvent, "HFC_fac", f"Solv{fil}", units)
            )

        dfs.append(pp_utils.make_outputdf(vars, units))

    return pd.concat(dfs, sort=True)


@_register
def retr_CO2emi(units_emi, units_ene_mdl):
    """Emissions: CO2.

    Parameters
    ----------

    units_emi : str
        Units to which emission variables should be converted.
    units_ene_mdl : str
        Native units of energy in the model.
    """

    dfs = []

    vars = {}

    if run_history != "True":
        group = ["Region", "Mode", "Vintage"]
    else:
        group = ["Region"]

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    _inp_nonccs_gas_tecs = (
        pp.inp(
            tec = [
                "gas_rc",
                "hp_gas_rc",
                "gas_i",
                "hp_gas_i",
                "gas_trp",
                "gas_fs",
                "gas_ppl",
                "gas_ct",
                "gas_cc",
                "gas_hpl",
            ], units = units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        + pp.inp(
            tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        - pp.out(
            tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl, outfilter={"commodity": ["gas"]}
        )
    )

    _inp_all_gas_tecs = _inp_nonccs_gas_tecs + pp.inp(
        tec = ["gas_cc_ccs", "meth_ng", "meth_ng_ccs", "h2_smr", "h2_smr_ccs"], units = units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out(tec = "gas_cc", units = units_ene_mdl) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units_ene_mdl)).fillna(0)

    _gas_ppl_shr = (pp.out(tec = "gas_ppl", units = units_ene_mdl) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units_ene_mdl)).fillna(0)

    _inp_nonccs_gas_tecs_wo_CCSRETRO = (
        _inp_nonccs_gas_tecs
        - _pe_wCCSretro(
            "gas_cc",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units_ene_mdl,
            share=_gas_cc_shr,
        )
        - _pe_wCCSretro(
            "gas_ppl",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units_ene_mdl,
            share=_gas_ppl_shr,
        )
    )

    # Helping variables required in units of Emissions
    _Biogas_tot_abs = pp.out("gas_bio")
    _Biogas_tot = _Biogas_tot_abs * mu["crbcnt_gas"] * mu["conv_c2co2"]
    _Biogas_el = _Biogas_tot * (
        pp.inp(
            tec = ["gas_ppl", "gas_ct", "gas_cc", "gas_cc_ccs"], units = units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_heat = _Biogas_tot * (
        pp.inp(tec = "gas_hpl", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_liquids_gas_comb = _Biogas_tot * (
        pp.inp(
            tec = ["meth_ng", "meth_ng_ccs"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_gases_h2_comb = _Biogas_tot * (
        pp.inp(
            tec = ["h2_smr", "h2_smr_ccs"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_ind = _Biogas_tot * (
        pp.inp(tec = ["gas_i", "hp_gas_i"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_fs = _Biogas_tot * (
        pp.inp(tec = "gas_fs", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_res = _Biogas_tot * (
        pp.inp(tec = ["gas_rc", "hp_gas_rc"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_trp = _Biogas_tot * (
        pp.inp(tec = "gas_trp", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_td = _Biogas_tot * (
        (
            pp.inp(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                outfilter={"commodity": ["gas"]},
            )
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # For deriving the distribution of hydrogen, the total gas use must exclude hydrogen
    # gas use in feedstocks. The relation `gas_mix_lim` ensures that blended hydrogen
    # can only make up 50% of the gas used by `gas_t_d` and `gas_t_d_ch4`. `gas_fs`
    # is excluded by also writing into the relation.
    _H2_inp_nonccs_gas_tecs_wo_CCSRETRO = _inp_nonccs_gas_tecs_wo_CCSRETRO - pp.inp(
        tec = ["gas_fs"], units = units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    _Hydrogen_tot = pp.emi(
        tec = "h2_mix", units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Hydrogen_el = _Hydrogen_tot * (
        (
            pp.inp(
                tec = ["gas_ppl", "gas_ct", "gas_cc"], units = units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - _pe_wCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units_ene_mdl,
                share=_gas_cc_shr,
            )
            - _pe_wCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units_ene_mdl,
                share=_gas_ppl_shr,
            )
        )
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_heat = _Hydrogen_tot * (
        pp.inp(tec = "gas_hpl", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_ind = _Hydrogen_tot * (
        pp.inp(tec = ["gas_i", "hp_gas_i"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # gas_fs is not able to use hydrogen, hence this is removed.
    # _Hydrogen_fs = _Hydrogen_tot * (
    #     pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
    #     / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    # ).fillna(0)

    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(tec = ["gas_rc", "hp_gas_rc"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_trp = _Hydrogen_tot * (
        pp.inp(tec = "gas_trp", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_td = _Hydrogen_tot * (
        (
            pp.inp(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                outfilter={"commodity": ["gas"]},
            )
        )
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _SE_Elec_gen = pp.emi(
        tec=[
            "coal_ppl_u",
            "coal_ppl",
            "coal_adv",
            "coal_adv_ccs",
            "igcc",
            "igcc_ccs",
            "foil_ppl",
            "loil_ppl",
            "loil_cc",
            "oil_ppl",
            "gas_ppl",
            "gas_cc",
            "gas_cc_ccs",
            "gas_ct",
            "bio_istig",
            "g_ppl_co2scr",
            "c_ppl_co2scr",
            "bio_ppl_co2scr",
            "bio_istig_ccs",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _SE_Elec_gen_woBECCS = (
        _SE_Elec_gen
        - pp.emi(
            tec=["bio_istig_ccs", "bio_ppl_co2scr"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            tec=["g_ppl_co2scr", "gas_cc_ccs"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        * (_Biogas_tot_abs / _inp_all_gas_tecs)
    )

    _SE_District_heat = pp.emi(
        tec=["coal_hpl", "foil_hpl", "gas_hpl"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _FE_Feedstocks = pp.emi(
        tec=["coal_fs", "foil_fs", "loil_fs", "gas_fs", "methanol_fs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_feedstocks"]},
        emission_units=units_emi,
    )

    _FE_Res_com = pp.emi(
        tec=["coal_rc", "foil_rc", "loil_rc", "gas_rc", "meth_rc", "hp_gas_rc"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_r_c"]},
        emission_units=units_emi,
    )

    _FE_Industry = pp.emi(
        tec=[
            "gas_i",
            "hp_gas_i",
            "loil_i",
            "meth_i",
            "coal_i",
            "foil_i",
            "sp_liq_I",
            "sp_meth_I",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _FE_Transport = pp.emi(
        tec=["gas_trp", "loil_trp", "meth_fc_trp", "meth_ic_trp", "coal_trp", "foil_trp"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_trp"]},
        emission_units=units_emi,
    )

    _FE_total = _FE_Feedstocks + _FE_Res_com + _FE_Industry + _FE_Transport

    _Other_gases_extr_comb = pp.emi(
        tec=[
            "gas_extr_1",
            "gas_extr_2",
            "gas_extr_3",
            "gas_extr_4",
            "gas_extr_5",
            "gas_extr_6",
            "gas_extr_7",
            "gas_extr_8",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_extr_fug = pp.emi(
        tec="flaring_CO2",
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calculated from CO2_TCE and doesnt include trade
    _Other_gases_trans_comb_trade = pp.emi(
        trade_tec = ['gas_piped_exp', 'LNG_shipped_exp'],
        units = units_ene_mdl,
        emifilter = {"relation": ["CO2_trade"]},
        emission_units = units_emi,
    )

    _Other_gases_trans_comb = pp.emi(
        tec=["gas_t_d", "gas_t_d_ch4"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_coal_comb = pp.emi(
        tec=["coal_gas"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    #    _Other_gases_biomass_comb = pp.emi(["gas_bio"], units_ene_mdl,
    #                                       emifilter={"relation": ["CO2_cc"]},
    #                                       emission_units=units_emi)

    _Other_gases_h2_comb = pp.emi(
        tec=["h2_smr", "h2_coal", "h2_bio", "h2_coal_ccs", "h2_smr_ccs", "h2_bio_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_h2_comb_woBECCS = (
        _Other_gases_h2_comb
        - pp.emi(
            tec=["h2_bio_ccs"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            tec=["h2_smr_ccs"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        * (_Biogas_tot_abs / _inp_all_gas_tecs)
    )

    _Other_gases_total = (
        _Other_gases_extr_comb
        + _Other_gases_extr_fug
        + _Other_gases_trans_comb
        + _Other_gases_coal_comb
        + _Other_gases_h2_comb
    )

    _Other_gases_total_woBECCS = (
        _Other_gases_extr_comb
        + _Other_gases_trans_comb
        + _Other_gases_coal_comb
        + _Other_gases_h2_comb_woBECCS
    )
    # Fugitive is not included in the total used to redistribute the difference
    # + _Other_gases_extr_fug

    _Other_liquids_extr_comb = pp.emi(
        tec=[
            "oil_extr_1",
            "oil_extr_2",
            "oil_extr_3",
            "oil_extr_4",
            "oil_extr_1_ch4",
            "oil_extr_2_ch4",
            "oil_extr_3_ch4",
            "oil_extr_4_ch4",
            "oil_extr_5",
            "oil_extr_6",
            "oil_extr_7",
            "oil_extr_8",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calcualted from CO2_TCE and doesnt include trade
    _Other_liquids_trans_comb_trade = pp.emi(
        tec=["foil_trd", "loil_trd", "oil_trd", "meth_trd"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_liquids_trans_comb = pp.emi(
        tec=["foil_t_d", "loil_t_d", "meth_t_d"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_oil_comb = pp.emi(
        tec=["ref_lol", "ref_hil"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb = pp.emi(
        tec=["meth_ng", "meth_ng_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb_woBECCS = _Other_liquids_gas_comb - pp.emi(
        tec=["meth_ng_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    ) * (_Biogas_tot_abs / _inp_all_gas_tecs)

    _Other_liquids_coal_comb = pp.emi(
        tec=["meth_coal", "syn_liq", "meth_coal_ccs", "syn_liq_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb = pp.emi(
        tec=["eth_bio", "liq_bio", "eth_bio_ccs", "liq_bio_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb_woBECCS = _Other_liquids_biomass_comb - pp.emi(
        tec=["eth_bio_ccs", "liq_bio_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_total = (
        _Other_liquids_extr_comb
        + _Other_liquids_trans_comb
        + _Other_liquids_oil_comb
        + _Other_liquids_gas_comb
        + _Other_liquids_coal_comb
        + _Other_liquids_biomass_comb
    )

    _Other_liquids_total_woBECCS = (
        _Other_liquids_extr_comb
        + _Other_liquids_trans_comb
        + _Other_liquids_oil_comb
        + _Other_liquids_gas_comb_woBECCS
        + _Other_liquids_coal_comb
        + _Other_liquids_biomass_comb_woBECCS
    )

    _Other_solids_coal_trans_comb = pp.emi(
        tec="coal_t_d",
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_solids_total = _Other_solids_coal_trans_comb

    _Cement1 = pp.emi(
        tec=["cement_CO2", "cement_co2scr"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Total = (
        _SE_Elec_gen
        + _SE_District_heat
        + _FE_total
        + _Other_gases_total
        + _Other_liquids_total
        + _Other_solids_total
        - _Biogas_tot
        + _Hydrogen_tot
        + _Cement1
        # + _Trade_losses
    )
    # GLOBIOM with the new lu implementation, LU_CO2 no longer writes
    # into _CO2_tce1 (CO2_TCE), as these have emission factors only,
    # and therefore do not write into CO2_TCE
    # + Landuse AFOLU)

    _Total_wo_BECCS = (
        abs(_SE_District_heat - _Biogas_heat + _Hydrogen_heat)
        + abs(_SE_Elec_gen_woBECCS - _Biogas_el + _Hydrogen_el)
        + abs(
            _Other_gases_total_woBECCS
            - _Biogas_gases_h2_comb
            - _Biogas_td
            + _Hydrogen_td
        )
        + abs(_Other_liquids_total_woBECCS - _Biogas_liquids_gas_comb)
        + abs(_Other_solids_total)
    )

    _CO2_tce1 = pp.emi(
        tec="CO2_TCE",
        units = units_ene_mdl,
        emifilter={"relation": ["TCE_Emission"]},
        emission_units=units_emi,
    )

    _Diff1 = _CO2_tce1 - _Total
    print("The difference between top-down and bottom-up accounting.")
    _Diff1.to_excel("_Diff.xlsx")

    if run_history == "True" and lu_hist_data is not False:
        df_hist = pd.read_csv(lu_hist_data).set_index("Region")
        df_hist = df_hist.rename(columns={i: int(i) for i in df_hist.columns})
        _Diff1 = _Diff1 - df_hist

    # -----
    # AFOLU
    # -----

    vars["AFOLU"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU"],
        },
        units=units_emi,
    )

    vars["AFOLU|Afforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Afforestation"],
        },
        units=units_emi,
    )

    vars["AFOLU|Agriculture"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture"],
        },
        units=units_emi,
    )

    vars["AFOLU|Agriculture|Biochar"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Biochar"],
        },
        units=units_emi,
    )

    vars["AFOLU|Agriculture|Silvopasture"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Silvopasture"],
        },
        units=units_emi,
    )

    vars["AFOLU|Agriculture|Soil Carbon|Cropland"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Soil Carbon|Cropland"],
        },
        units=units_emi,
    )

    vars["AFOLU|Agriculture|Soil Carbon|Pasture"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Agriculture|Soil Carbon|Pasture"],
        },
        units=units_emi,
    )

    vars["AFOLU|Deforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Deforestation"],
        },
        units=units_emi,
    )

    vars["AFOLU|Forest Management"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Forest Management"],
        },
        units=units_emi,
    )

    # This variable is used to determine the compensation quantity of GHGs
    # from land-use to ensure that the there are non non-convexities.
    vars["AFOLU|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Other"],
        },
        units=units_emi,
    )
    vars["AFOLU|Other LUC"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Other LUC"],
        },
        units=units_emi,
    )

    vars["AFOLU|Positive"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Positive"],
        },
        units=units_emi,
    )

    vars["AFOLU|Negative"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Negative"],
        },
        units=units_emi,
    )

    # ------------------
    # Aircraft (Table 4)
    # ------------------

    Aircraft = pp_utils._make_zero()
    vars["Energy|Demand|Bunkers|International Aviation"] = Aircraft

    # -----------------------------------------
    # Electricity and heat production (Table 5)
    # -----------------------------------------

    vars["Energy|Supply|Heat"] = (
        _SE_District_heat
        - _Biogas_heat
        + _Hydrogen_heat
        + _Diff1
        * (
            abs(_SE_District_heat - _Biogas_heat + _Hydrogen_heat) / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Supply|Electricity"] = (
        _SE_Elec_gen
        - _Biogas_el
        + _Hydrogen_el
        + _Diff1
        * (
            abs(_SE_Elec_gen_woBECCS - _Biogas_el + _Hydrogen_el) / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Supply|Gases|Biomass|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Gases|Biomass|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Gases|Coal|Combustion"] = _Other_gases_coal_comb + _Diff1 * (
        abs(_Other_gases_coal_comb) / _Total_wo_BECCS
    ).fillna(0)

    vars["Energy|Supply|Gases|Coal|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Gases|Extraction|Combustion"] = (
        _Other_gases_extr_comb
        + _Diff1 * (abs(_Other_gases_extr_comb) / _Total_wo_BECCS).fillna(0)
    )

    # _Diff1 is not disctributed across the variable
    vars["Energy|Supply|Gases|Extraction|Fugitive"] = _Other_gases_extr_fug
    vars["Energy|Supply|Gases|Hydrogen|Combustion"] = (
        _Other_gases_h2_comb
        - _Biogas_gases_h2_comb
        + _Diff1
        * (
            abs(_Other_gases_h2_comb_woBECCS - _Biogas_gases_h2_comb) / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Supply|Gases|Hydrogen|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Gases|Natural Gas|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Gases|Natural Gas|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Gases|Transportation|Combustion"] = (
        _Other_gases_trans_comb
        - _Biogas_td
        + _Hydrogen_td
        + _Diff1
        * (
            abs(_Other_gases_trans_comb - _Biogas_td + _Hydrogen_td) / _Total_wo_BECCS
        ).fillna(0)
        + _Other_gases_trans_comb_trade
    )

    vars["Energy|Supply|Gases|Transportation|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Biomass|Combustion"] = (
        _Other_liquids_biomass_comb
        + _Diff1
        * (abs(_Other_liquids_biomass_comb_woBECCS) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Liquids|Biomass|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Coal|Combustion"] = (
        _Other_liquids_coal_comb
        + _Diff1 * (abs(_Other_liquids_coal_comb) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Liquids|Coal|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Extraction|Combustion"] = (
        _Other_liquids_extr_comb
        + _Diff1 * (abs(_Other_liquids_extr_comb) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Liquids|Extraction|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Natural Gas|Combustion"] = (
        _Other_liquids_gas_comb
        - _Biogas_liquids_gas_comb
        + _Diff1
        * (
            abs(_Other_liquids_gas_comb_woBECCS - _Biogas_liquids_gas_comb)
            / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Supply|Liquids|Natural Gas|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Oil|Combustion"] = _Other_liquids_oil_comb + _Diff1 * (
        abs(_Other_liquids_oil_comb) / _Total_wo_BECCS
    ).fillna(0)

    vars["Energy|Supply|Liquids|Oil|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Transportation|Combustion"] = (
        _Other_liquids_trans_comb
        + _Diff1 * (abs(_Other_liquids_trans_comb) / _Total_wo_BECCS).fillna(0)
        + _Other_liquids_trans_comb_trade
    )

    vars["Energy|Supply|Liquids|Transportation|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Other|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Other|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Biomass|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Biomass|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Coal|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Coal|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Solids|Extraction|Combustion"] = (
        _Other_solids_coal_trans_comb
        + _Diff1 * (abs(_Other_solids_coal_trans_comb) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Solids|Extraction|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Transportation|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Transportation|Fugitive"] = pp_utils._make_zero()

    # -------------------------------
    # Industrial Combustion (Table 7)
    # -------------------------------

    IndustrialCombustion = _FE_Industry - _Biogas_ind + _Hydrogen_ind

    vars["Energy|Demand|Industry"] = IndustrialCombustion

    # ---------------------
    # Industrial Feedstocks
    # ---------------------

    vars["Energy|Demand|Other Sector"] = _FE_Feedstocks - _Biogas_fs  # + _Hydrogen_fs

    # --------------------------------------------
    # Industrial process and product use (Table 8)
    # --------------------------------------------

    Cement = _Cement1
    vars["Industrial Processes"] = Cement

    # --------------------------------
    # International shipping (Table 9)
    # --------------------------------

    Bunker = pp.act("CO2s_TCE") * mu["conv_c2co2"]
    vars["Energy|Demand|Bunkers|International Shipping"] = Bunker

    # -------------------------------------------------
    # Residential, Commercial, Other - Other (Table 11)
    # -------------------------------------------------

    ResComOth = pp_utils._make_zero()
    vars["Energy|Demand|AFOFI"] = ResComOth

    # -------------------------------------------------------------------
    # Residential, Commercial, Other - Residential, Commercial (Table 12)
    # -------------------------------------------------------------------

    Res_Com = _FE_Res_com - _Biogas_res + _Hydrogen_res
    vars["Energy|Demand|Residential and Commercial"] = Res_Com

    # ------------------------------
    # Road transportation (Table 13)
    # ------------------------------

    Transport = _FE_Transport - _Biogas_trp + _Hydrogen_trp
    # vars["Energy|Demand|Transportation|Road"] = Transport
    vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"] = Transport

    # ----------------------------------------------
    # Solvents production and application (Table 14)
    # ----------------------------------------------

    Solvents = pp_utils._make_zero()
    vars["Product Use|Solvents"] = Solvents

    # ----------------
    # Waste (Table 15)
    # ----------------

    Waste = pp_utils._make_zero()
    vars["Waste"] = Waste

    # Special Aggregates which cannot be treated generically
    vars["Energy|Supply|Combustion"] = (
        vars["Energy|Supply|Heat"]
        + vars["Energy|Supply|Electricity"]
        + vars["Energy|Supply|Gases|Biomass|Combustion"]
        + vars["Energy|Supply|Gases|Coal|Combustion"]
        + vars["Energy|Supply|Gases|Extraction|Combustion"]
        + vars["Energy|Supply|Gases|Hydrogen|Combustion"]
        + vars["Energy|Supply|Gases|Natural Gas|Combustion"]
        + vars["Energy|Supply|Gases|Transportation|Combustion"]
        + vars["Energy|Supply|Liquids|Biomass|Combustion"]
        + vars["Energy|Supply|Liquids|Coal|Combustion"]
        + vars["Energy|Supply|Liquids|Extraction|Combustion"]
        + vars["Energy|Supply|Liquids|Natural Gas|Combustion"]
        + vars["Energy|Supply|Liquids|Oil|Combustion"]
        + vars["Energy|Supply|Liquids|Transportation|Combustion"]
        + vars["Energy|Supply|Other|Combustion"]
        + vars["Energy|Supply|Solids|Biomass|Combustion"]
        + vars["Energy|Supply|Solids|Coal|Combustion"]
        + vars["Energy|Supply|Solids|Extraction|Combustion"]
        + vars["Energy|Supply|Solids|Transportation|Combustion"]
    )

    vars["Energy|Combustion"] = (
        vars["Energy|Supply|Combustion"]
        + vars["Energy|Demand|Bunkers|International Shipping"]
        + vars["Energy|Demand|AFOFI"]
        + vars["Energy|Demand|Industry"]
        + vars["Energy|Demand|Residential and Commercial"]
        + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
        + vars["Energy|Demand|Bunkers|International Aviation"]
    )

    vars["Energy|Supply|Fugitive"] = (
        vars["Energy|Supply|Gases|Biomass|Fugitive"]
        + vars["Energy|Supply|Gases|Coal|Fugitive"]
        + vars["Energy|Supply|Gases|Extraction|Fugitive"]
        + vars["Energy|Supply|Gases|Hydrogen|Fugitive"]
        + vars["Energy|Supply|Gases|Natural Gas|Fugitive"]
        + vars["Energy|Supply|Gases|Transportation|Fugitive"]
        + vars["Energy|Supply|Liquids|Biomass|Fugitive"]
        + vars["Energy|Supply|Liquids|Coal|Fugitive"]
        + vars["Energy|Supply|Liquids|Extraction|Fugitive"]
        + vars["Energy|Supply|Liquids|Natural Gas|Fugitive"]
        + vars["Energy|Supply|Liquids|Oil|Fugitive"]
        + vars["Energy|Supply|Liquids|Transportation|Fugitive"]
        + vars["Energy|Supply|Other|Fugitive"]
        + vars["Energy|Supply|Solids|Biomass|Fugitive"]
        + vars["Energy|Supply|Solids|Coal|Fugitive"]
        + vars["Energy|Supply|Solids|Extraction|Fugitive"]
        + vars["Energy|Supply|Solids|Transportation|Fugitive"]
    )

    vars["Energy|Fugitive"] = vars["Energy|Supply|Fugitive"]

    dfs.append(pp_utils.make_outputdf(vars, units_emi))
    vars = {}

    # Additional reporting to account for emission differences and accounting issues
    vars["Difference|Statistical"] = _Diff1
    vars["Difference|Stock|Coal"] = pp.emi(
        trade_tec = ['coal_shipped_exp', 'coal_shipped_imp'],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    ) + pp.emi(
        tec=["coal_trd"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )
    # Coal cannot be used for bunkers
    #    + pp.emi(["coal_bunker"], units_ene_mdl,
    #             emifilter={"relation": ["CO2_shipping"]},
    #             emission_units=units_emi)

    vars["Difference|Stock|Methanol"] = (
        pp.emi(
            tec = ['meth_exp', 'meth_imp'],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["meth_bunker"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["meth_trd"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Fueloil"] = (
        pp.emi(
            trade_tec = ['foil_shipped_exp', 'foil_shipped_imp',
                         'foil_piped_exp', 'foil_piped_imp'],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["foil_bunker"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["foil_trd"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Lightoil"] = (
        pp.emi(
            trade_tec = ['loil_shipped_exp', 'loil_shipped_imp'
                         'loil_piped_exp', 'loil_piped_imp'],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["loil_bunker"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["loil_trd"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Crudeoil"] = pp.emi(
        trade_tec = ['crudeoil_shipped_exp', 'crudeoil_shipped_imp',
                     'crudeoil_piped_exp', 'crudeoil_piped_imp'],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    ) + pp.emi(
        tec=["oil_trd"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )
    #    + pp.emi(["oil_bunker"], units_ene_mdl,
    #             emifilter={"relation": ["CO2_shipping"]},
    #             emission_units=units_emi)

    vars["Difference|Stock|LNG"] = (
        pp.emi(
            trade_tec = ['LNG_shipped_exp', 'LNG_shipped_imp'],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["LNG_bunker"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            tec=["LNG_trd"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Natural Gas"] = pp.emi(
        trade_tec = ['gas_piped_exp', 'gas_piped_imp'],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    dfs.append(pp_utils.make_outputdf(vars, units_emi, glb=False))
    vars = {}

    vars["Difference|Stock|Activity|Coal"] = (
        pp.inp(trade_tec=["coal_shipped_imp"], units=units_ene_mdl, inpfilter={"commodity": ["coal"]})
        - pp.inp(trade_tec=["coal_shipped_exp"], units=units_ene_mdl, inpfilter={"commodity": ["coal"]})
        + pp.inp(tec = ["coal_trd"], units = units_ene_mdl)
        - pp.out(tec = ["coal_trd"], units = units_ene_mdl)
    )
    #   + pp.inp(["coal_bunker"], units_ene_mdl)

    vars["Difference|Stock|Activity|Methanol"] = (
        pp.inp(tec=["meth_imp"], units=units_ene_mdl)
        - pp.inp(tec=["meth_exp"], units=units_ene_mdl)
        + pp.inp(tec = ["meth_bunker"], units = units_ene_mdl)
        + pp.inp(tec = ["meth_trd"], units = units_ene_mdl)
        - pp.out(tec = ["meth_trd"], units = units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Fueloil"] = (
        pp.inp(trade_tec=["foil_shipped_imp", "foil_piped_imp"], units=units_ene_mdl, inpfilter={"commodity": ["fueloil"]})
        - pp.inp(trade_tec=["foil_shipped_exp", "foil_piped_exp"], units=units_ene_mdl, inpfilter={"commodity": ["fueloil"]})
        + pp.inp(tec = ["foil_bunker"], units = units_ene_mdl)
        + pp.inp(tec = ["foil_trd"], units = units_ene_mdl)
        - pp.out(tec = ["foil_trd"], units = units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Lightoil"] = (
        pp.inp(trade_tec=["loil_shipped_imp", "loil_piped_imp"], units=units_ene_mdl, inpfilter={"commodity": ["lightoil"]})
        - pp.inp(trade_tec=["loil_shipped_exp", "loil_piped_exp"], units=units_ene_mdl, inpfilter={"commodity": ["lightoil"]})
        + pp.inp(tec = ["loil_bunker"], units = units_ene_mdl)
        + pp.inp(tec = ["loil_trd"], units = units_ene_mdl)
        - pp.out(tec = ["loil_trd"], units = units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Crudeoil"] = (
        pp.inp(trade_tec=["crudeoil_shipped_imp", "crudeoil_piped_imp"], units=units_ene_mdl, inpfilter={"commodity": ["crudeoil"]})
        - pp.inp(trade_tec=["crudeoil_shipped_exp", "crudeoil_piped_exp"], units=units_ene_mdl, inpfilter={"commodity": ["crudeoil"]})
        + pp.inp(tec = ["oil_trd"], units = units_ene_mdl)
        - pp.out(tec = ["oil_trd"], units = units_ene_mdl)
    )
    #   + pp.inp(["oil_bunker"], units_ene_mdl)

    vars["Difference|Stock|Activity|LNG"] = (
        pp.inp(trade_tec=["LNG_shipped_imp"], units=units_ene_mdl, inpfilter={"commodity": ["LNG"]})
        - pp.inp(trade_tec=["LNG_shipped_exp"], units=units_ene_mdl, inpfilter={"commodity": ["LNG"]})
        + pp.inp(tec = ["LNG_bunker"], units = units_ene_mdl)
        + pp.inp(tec = ["LNG_trd"], units = units_ene_mdl)
        - pp.out(tec = ["LNG_trd"], units = units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Natural Gas"] = pp.inp(
        trade_tec=["gas_piped_imp"], units=units_ene_mdl, inpfilter={"commodity": ["gas"]}
    ) - pp.inp(
        trade_tec=["gas_piped_exp"], units=units_ene_mdl, inpfilter={"commodity": ["gas"]}
    )

    dfs.append(pp_utils.make_outputdf(vars, units_ene_mdl, glb=False))
    return pd.concat(dfs, sort=True)


@_register
def retr_GROSSCO2emi(units_emi, units_ene_mdl):
    """Emissions: CO2.

    Parameters
    ----------

    units_emi : str
        Units to which emission variables should be converted.
    units_ene_mdl : str
        Native units of energy in the model.
    """

    dfs = []

    vars = {}

    if run_history != "True":
        group = ["Region", "Mode", "Vintage"]
    else:
        group = ["Region"]

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    _inp_nonccs_gas_tecs = (
        pp.inp(
            tec = [
                "gas_rc",
                "hp_gas_rc",
                "gas_i",
                "hp_gas_i",
                "gas_trp",
                "gas_fs",
                "gas_ppl",
                "gas_ct",
                "gas_cc",
                "gas_hpl",
            ],
            units = units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        + pp.inp(
            tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        - pp.out(
            tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl, outfilter={"commodity": ["gas"]}
        )
    )

    _inp_all_gas_tecs = _inp_nonccs_gas_tecs + pp.inp(
        tec = ["gas_cc_ccs", "meth_ng", "meth_ng_ccs", "h2_smr", "h2_smr_ccs"], units = units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out(tec = "gas_cc", units = units_ene_mdl) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units_ene_mdl)).fillna(0)

    _gas_ppl_shr = (pp.out(tec = "gas_ppl", units = units_ene_mdl) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units_ene_mdl)).fillna(0)

    _inp_nonccs_gas_tecs_wo_CCSRETRO = (
        _inp_nonccs_gas_tecs
        - _pe_wCCSretro(
            "gas_cc",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units_ene_mdl,
            share=_gas_cc_shr,
        )
        - _pe_wCCSretro(
            "gas_ppl",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units_ene_mdl,
            share=_gas_ppl_shr,
        )
    )

    # Helping variables required in units of Emissions
    _Biogas_tot_abs = pp.out("gas_bio")
    _Biogas_tot = _Biogas_tot_abs * mu["crbcnt_gas"] * mu["conv_c2co2"]
    _Biogas_el = _Biogas_tot * (
        pp.inp(
            tec = ["gas_ppl", "gas_ct", "gas_cc", "gas_cc_ccs"], units = units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_heat = _Biogas_tot * (
        pp.inp(tec = "gas_hpl", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_liquids_gas_comb = _Biogas_tot * (
        pp.inp(
            tec = ["meth_ng", "meth_ng_ccs"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_gases_h2_comb = _Biogas_tot * (
        pp.inp(
            tec = ["h2_smr", "h2_smr_ccs"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_ind = _Biogas_tot * (
        pp.inp(tec = ["gas_i", "hp_gas_i"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_fs = _Biogas_tot * (
        pp.inp(tec = "gas_fs", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_res = _Biogas_tot * (
        pp.inp(tec = ["gas_rc", "hp_gas_rc"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_trp = _Biogas_tot * (
        pp.inp(tec = "gas_trp", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_td = _Biogas_tot * (
        (
            pp.inp(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                outfilter={"commodity": ["gas"]},
            )
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # For deriving the distribution of hydrogen, the total gas use must exclude hydrogen
    # gas use in feedstocks. The relation `gas_mix_lim` ensures that blended hydrogen
    # can only make up 50% of the gas used by `gas_t_d` and `gas_t_d_ch4`. `gas_fs`
    # is excluded by also writing into the relation.
    _H2_inp_nonccs_gas_tecs_wo_CCSRETRO = _inp_nonccs_gas_tecs_wo_CCSRETRO - pp.inp(
        tec = ["gas_fs"], units = units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    _Hydrogen_tot = pp.emi(
        tec = "h2_mix", units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Hydrogen_el = _Hydrogen_tot * (
        (
            pp.inp(
                tec = ["gas_ppl", "gas_ct", "gas_cc"], units = units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - _pe_wCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units_ene_mdl,
                share=_gas_cc_shr,
            )
            - _pe_wCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units_ene_mdl,
                share=_gas_ppl_shr,
            )
        )
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_heat = _Hydrogen_tot * (
        pp.inp(tec = "gas_hpl", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_ind = _Hydrogen_tot * (
        pp.inp(tec = ["gas_i", "hp_gas_i"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # gas_fs is not able to use hydrogen, hence this is removed.
    # _Hydrogen_fs = _Hydrogen_tot * (
    #     pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
    #     / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    # ).fillna(0)

    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(tec = ["gas_rc", "hp_gas_rc"], units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_trp = _Hydrogen_tot * (
        pp.inp(tec = "gas_trp", units = units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_td = _Hydrogen_tot * (
        (
            pp.inp(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                tec = ["gas_t_d", "gas_t_d_ch4"], units = units_ene_mdl,
                outfilter={"commodity": ["gas"]},
            )
        )
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _SE_Elec_gen_wBECCS = pp.emi(
        tec=[
            "coal_ppl_u",
            "coal_ppl",
            "coal_adv",
            "coal_adv_ccs",
            "igcc",
            "igcc_ccs",
            "foil_ppl",
            "loil_ppl",
            "loil_cc",
            "oil_ppl",
            "gas_ppl",
            "gas_cc",
            "gas_cc_ccs",
            "gas_ct",
            "bio_istig",
            "g_ppl_co2scr",
            "c_ppl_co2scr",
            "bio_ppl_co2scr",
            "bio_istig_ccs",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _SE_Elec_gen = (
        _SE_Elec_gen_wBECCS
        - pp.emi(
            tec=["bio_istig_ccs", "bio_ppl_co2scr"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            tec=["g_ppl_co2scr", "gas_cc_ccs"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        * (_Biogas_tot_abs / _inp_all_gas_tecs)
    )

    _SE_District_heat = pp.emi(
        tec=["coal_hpl", "foil_hpl", "gas_hpl"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _FE_Feedstocks = pp.emi(
        tec=["coal_fs", "foil_fs", "loil_fs", "gas_fs", "methanol_fs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_feedstocks"]},
        emission_units=units_emi,
    )

    _FE_Res_com = pp.emi(
        tec=["coal_rc", "foil_rc", "loil_rc", "gas_rc", "meth_rc", "hp_gas_rc"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_r_c"]},
        emission_units=units_emi,
    )

    _FE_Industry = pp.emi(
        tec=[
            "gas_i",
            "hp_gas_i",
            "loil_i",
            "meth_i",
            "coal_i",
            "foil_i",
            "sp_liq_I",
            "sp_meth_I",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _FE_Transport = pp.emi(
        tec=["gas_trp", "loil_trp", "meth_fc_trp", "meth_ic_trp", "coal_trp", "foil_trp"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_trp"]},
        emission_units=units_emi,
    )

    _FE_total = _FE_Feedstocks + _FE_Res_com + _FE_Industry + _FE_Transport

    _Other_gases_extr_comb = pp.emi(
        tec=[
            "gas_extr_1",
            "gas_extr_2",
            "gas_extr_3",
            "gas_extr_4",
            "gas_extr_5",
            "gas_extr_6",
            "gas_extr_7",
            "gas_extr_8",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_extr_fug = pp.emi(
        tec="flaring_CO2",
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calculated from CO2_TCE and doesnt include trade
    _Other_gases_trans_comb_trade = pp.emi(
        trade_tec = ['LNG_trd', 'gas_piped_exp'],
        units=units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_gases_trans_comb = pp.emi(
        tec=["gas_t_d", "gas_t_d_ch4"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_coal_comb = pp.emi(
        tec=["coal_gas"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    #    _Other_gases_biomass_comb = pp.emi(["gas_bio"], units_ene_mdl,
    #                                       emifilter={"relation": ["CO2_cc"]},
    #                                       emission_units=units_emi)

    _Other_gases_h2_comb_wBECCS = pp.emi(
        tec=["h2_smr", "h2_coal", "h2_bio", "h2_coal_ccs", "h2_smr_ccs", "h2_bio_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_h2_comb = (
        _Other_gases_h2_comb_wBECCS
        - pp.emi(
            tec=["h2_bio_ccs"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            tec=["h2_smr_ccs"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        * (_Biogas_tot_abs / _inp_all_gas_tecs)
    )

    _Other_gases_total_wBECCS = (
        _Other_gases_extr_comb
        + _Other_gases_extr_fug
        + _Other_gases_trans_comb
        + _Other_gases_coal_comb
        + _Other_gases_h2_comb_wBECCS
    )

    _Other_gases_total = (
        _Other_gases_extr_comb
        + _Other_gases_trans_comb
        + _Other_gases_coal_comb
        + _Other_gases_h2_comb
    )
    # Fugitive is not included in the total used to redistribute the difference
    # + _Other_gases_extr_fug

    _Other_liquids_extr_comb = pp.emi(
        tec=[
            "oil_extr_1",
            "oil_extr_2",
            "oil_extr_3",
            "oil_extr_4",
            "oil_extr_1_ch4",
            "oil_extr_2_ch4",
            "oil_extr_3_ch4",
            "oil_extr_4_ch4",
            "oil_extr_5",
            "oil_extr_6",
            "oil_extr_7",
            "oil_extr_8",
        ],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calcualted from CO2_TCE and doesnt include trade
    _Other_liquids_trans_comb_trade = pp.emi(
        tec=["foil_trd", "loil_trd", "oil_trd", "meth_trd"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_liquids_trans_comb = pp.emi(
        tec=["foil_t_d", "loil_t_d", "meth_t_d"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_oil_comb = pp.emi(
        tec=["ref_lol", "ref_hil"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb_wBECCS = pp.emi(
        tec=["meth_ng", "meth_ng_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb = _Other_liquids_gas_comb_wBECCS - pp.emi(
        tec=["meth_ng_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    ) * (_Biogas_tot_abs / _inp_all_gas_tecs)

    _Other_liquids_coal_comb = pp.emi(
        tec=["meth_coal", "syn_liq", "meth_coal_ccs", "syn_liq_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb_wBECCS = pp.emi(
        tec=["eth_bio", "liq_bio", "eth_bio_ccs", "liq_bio_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb = _Other_liquids_biomass_comb_wBECCS - pp.emi(
        tec=["eth_bio_ccs", "liq_bio_ccs"],
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_total_wBECCS = (
        _Other_liquids_extr_comb
        + _Other_liquids_trans_comb
        + _Other_liquids_oil_comb
        + _Other_liquids_gas_comb_wBECCS
        + _Other_liquids_coal_comb
        + _Other_liquids_biomass_comb_wBECCS
    )

    _Other_liquids_total = (
        _Other_liquids_extr_comb
        + _Other_liquids_trans_comb
        + _Other_liquids_oil_comb
        + _Other_liquids_gas_comb
        + _Other_liquids_coal_comb
        + _Other_liquids_biomass_comb
    )

    _Other_solids_coal_trans_comb = pp.emi(
        tec="coal_t_d",
        units = units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_solids_total = _Other_solids_coal_trans_comb

    _Cement1 = (
        pp.emi(
            tec=["cement_CO2", "cement_co2scr"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        # GROSS EMISSION modification
        - pp.emi(
            tec=["cement_co2scr"],
            units = units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
    )

    _Total = (
        _SE_Elec_gen
        + _SE_District_heat
        + _FE_total
        + _Other_gases_total
        + _Other_liquids_total
        + _Other_solids_total
        - _Biogas_tot
        + _Hydrogen_tot
        + _Cement1
        # + _Trade_losses
    )
    # GLOBIOM with the new lu implementation, LU_CO2 no longer writes
    # into _CO2_tce1 (CO2_TCE), as these have emission factors only,
    # and therefore do not write into CO2_TCE
    # + Landuse AFOLU)

    _Total_wo_BECCS = (
        abs(_SE_District_heat - _Biogas_heat + _Hydrogen_heat)
        + abs(_SE_Elec_gen - _Biogas_el + _Hydrogen_el)
        + abs(_Other_gases_total - _Biogas_gases_h2_comb - _Biogas_td + _Hydrogen_td)
        + abs(_Other_liquids_total - _Biogas_liquids_gas_comb)
        + abs(_Other_solids_total)
    )

    _CO2_tce1 = pp.emi(
        tec="CO2_TCE",
        units = units_ene_mdl,
        emifilter={"relation": ["TCE_Emission"]},
        emission_units=units_emi,
    )

    _Diff1 = (
        # Total CO2 Emissions
        _CO2_tce1
        # Subtract all emissions which have make up the total
        - _Total
        + abs(_SE_Elec_gen_wBECCS - _SE_Elec_gen)
        + abs(_Other_gases_h2_comb_wBECCS - _Other_gases_h2_comb)
        + abs(_Other_gases_total_wBECCS - _Other_gases_total)
        + abs(_Other_liquids_gas_comb_wBECCS - _Other_liquids_gas_comb)
        + abs(_Other_liquids_biomass_comb_wBECCS - _Other_liquids_biomass_comb)
    )
    print("The difference between top-down and bottom-up accounting.")
    _Diff1.to_excel("_Gross_Diff.xlsx")

    if run_history == "True" and lu_hist_data is not False:
        df_hist = pd.read_csv(lu_hist_data).set_index("Region")
        df_hist = df_hist.rename(columns={i: int(i) for i in df_hist.columns})
        _Diff1 = _Diff1 - df_hist

    # -----
    # AFOLU
    # -----

    vars["AFOLU"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Gross Emissions|CO2|AFOLU"],
        },
        units=units_emi,
    )

    # ------------------
    # Aircraft (Table 4)
    # ------------------

    Aircraft = pp_utils._make_zero()
    vars["Energy|Demand|Bunkers|International Aviation"] = Aircraft

    # -----------------------------------------
    # Electricity and heat production (Table 5)
    # -----------------------------------------

    vars["Energy|Supply|Heat"] = (
        _SE_District_heat
        - _Biogas_heat
        + _Hydrogen_heat
        + _Diff1
        * (
            abs(_SE_District_heat - _Biogas_heat + _Hydrogen_heat) / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Supply|Electricity"] = (
        _SE_Elec_gen
        - _Biogas_el
        + _Hydrogen_el
        + _Diff1
        * (abs(_SE_Elec_gen - _Biogas_el + _Hydrogen_el) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Gases|Biomass|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Gases|Biomass|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Gases|Coal|Combustion"] = _Other_gases_coal_comb + _Diff1 * (
        abs(_Other_gases_coal_comb) / _Total_wo_BECCS
    ).fillna(0)

    vars["Energy|Supply|Gases|Coal|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Gases|Extraction|Combustion"] = (
        _Other_gases_extr_comb
        + _Diff1 * (abs(_Other_gases_extr_comb) / _Total_wo_BECCS).fillna(0)
    )

    # _Diff1 is not disctributed across the variable
    vars["Energy|Supply|Gases|Extraction|Fugitive"] = _Other_gases_extr_fug
    vars["Energy|Supply|Gases|Hydrogen|Combustion"] = (
        _Other_gases_h2_comb
        - _Biogas_gases_h2_comb
        + _Diff1
        * (abs(_Other_gases_h2_comb - _Biogas_gases_h2_comb) / _Total_wo_BECCS).fillna(
            0
        )
    )

    vars["Energy|Supply|Gases|Hydrogen|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Gases|Natural Gas|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Gases|Natural Gas|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Gases|Transportation|Combustion"] = (
        _Other_gases_trans_comb
        - _Biogas_td
        + _Hydrogen_td
        + _Diff1
        * (
            abs(_Other_gases_trans_comb - _Biogas_td + _Hydrogen_td) / _Total_wo_BECCS
        ).fillna(0)
        + _Other_gases_trans_comb_trade
    )

    vars["Energy|Supply|Gases|Transportation|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Biomass|Combustion"] = (
        _Other_liquids_biomass_comb
        + _Diff1 * (abs(_Other_liquids_biomass_comb) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Liquids|Biomass|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Coal|Combustion"] = (
        _Other_liquids_coal_comb
        + _Diff1 * (abs(_Other_liquids_coal_comb) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Liquids|Coal|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Extraction|Combustion"] = (
        _Other_liquids_extr_comb
        + _Diff1 * (abs(_Other_liquids_extr_comb) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Liquids|Extraction|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Natural Gas|Combustion"] = (
        _Other_liquids_gas_comb
        - _Biogas_liquids_gas_comb
        + _Diff1
        * (
            abs(_Other_liquids_gas_comb - _Biogas_liquids_gas_comb) / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Supply|Liquids|Natural Gas|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Oil|Combustion"] = _Other_liquids_oil_comb + _Diff1 * (
        abs(_Other_liquids_oil_comb) / _Total_wo_BECCS
    ).fillna(0)

    vars["Energy|Supply|Liquids|Oil|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Liquids|Transportation|Combustion"] = (
        _Other_liquids_trans_comb
        + _Diff1 * (abs(_Other_liquids_trans_comb) / _Total_wo_BECCS).fillna(0)
        + _Other_liquids_trans_comb_trade
    )

    vars["Energy|Supply|Liquids|Transportation|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Other|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Other|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Biomass|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Biomass|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Coal|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Coal|Fugitive"] = pp_utils._make_zero()

    vars["Energy|Supply|Solids|Extraction|Combustion"] = (
        _Other_solids_coal_trans_comb
        + _Diff1 * (abs(_Other_solids_coal_trans_comb) / _Total_wo_BECCS).fillna(0)
    )

    vars["Energy|Supply|Solids|Extraction|Fugitive"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Transportation|Combustion"] = pp_utils._make_zero()
    vars["Energy|Supply|Solids|Transportation|Fugitive"] = pp_utils._make_zero()

    # -------------------------------
    # Industrial Combustion (Table 7)
    # -------------------------------

    IndustrialCombustion = _FE_Industry - _Biogas_ind + _Hydrogen_ind

    vars["Energy|Demand|Industry"] = IndustrialCombustion

    # ---------------------
    # Industrial Feedstocks
    # ---------------------

    vars["Energy|Demand|Other Sector"] = _FE_Feedstocks - _Biogas_fs  # + _Hydrogen_fs

    # --------------------------------------------
    # Industrial process and product use (Table 8)
    # --------------------------------------------

    Cement = _Cement1
    vars["Industrial Processes"] = Cement

    # --------------------------------
    # International shipping (Table 9)
    # --------------------------------

    Bunker = pp.act("CO2s_TCE") * mu["conv_c2co2"]
    vars["Energy|Demand|Bunkers|International Shipping"] = Bunker

    # -------------------------------------------------
    # Residential, Commercial, Other - Other (Table 11)
    # -------------------------------------------------

    ResComOth = pp_utils._make_zero()
    vars["Energy|Demand|AFOFI"] = ResComOth

    # -------------------------------------------------------------------
    # Residential, Commercial, Other - Residential, Commercial (Table 12)
    # -------------------------------------------------------------------

    Res_Com = _FE_Res_com - _Biogas_res + _Hydrogen_res
    vars["Energy|Demand|Residential and Commercial"] = Res_Com

    # ------------------------------
    # Road transportation (Table 13)
    # ------------------------------

    Transport = _FE_Transport - _Biogas_trp + _Hydrogen_trp
    # vars["Energy|Demand|Transportation|Road"] = Transport
    vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"] = Transport

    # ----------------------------------------------
    # Solvents production and application (Table 14)
    # ----------------------------------------------

    Solvents = pp_utils._make_zero()
    vars["Product Use|Solvents"] = Solvents

    # ----------------
    # Waste (Table 15)
    # ----------------

    Waste = pp_utils._make_zero()
    vars["Waste"] = Waste

    # Special Aggregates which cannot be treated generically
    vars["Energy|Supply|Combustion"] = (
        vars["Energy|Supply|Heat"]
        + vars["Energy|Supply|Electricity"]
        + vars["Energy|Supply|Gases|Biomass|Combustion"]
        + vars["Energy|Supply|Gases|Coal|Combustion"]
        + vars["Energy|Supply|Gases|Extraction|Combustion"]
        + vars["Energy|Supply|Gases|Hydrogen|Combustion"]
        + vars["Energy|Supply|Gases|Natural Gas|Combustion"]
        + vars["Energy|Supply|Gases|Transportation|Combustion"]
        + vars["Energy|Supply|Liquids|Biomass|Combustion"]
        + vars["Energy|Supply|Liquids|Coal|Combustion"]
        + vars["Energy|Supply|Liquids|Extraction|Combustion"]
        + vars["Energy|Supply|Liquids|Natural Gas|Combustion"]
        + vars["Energy|Supply|Liquids|Oil|Combustion"]
        + vars["Energy|Supply|Liquids|Transportation|Combustion"]
        + vars["Energy|Supply|Other|Combustion"]
        + vars["Energy|Supply|Solids|Biomass|Combustion"]
        + vars["Energy|Supply|Solids|Coal|Combustion"]
        + vars["Energy|Supply|Solids|Extraction|Combustion"]
        + vars["Energy|Supply|Solids|Transportation|Combustion"]
    )

    vars["Energy|Combustion"] = (
        vars["Energy|Supply|Combustion"]
        + vars["Energy|Demand|Bunkers|International Shipping"]
        + vars["Energy|Demand|AFOFI"]
        + vars["Energy|Demand|Industry"]
        + vars["Energy|Demand|Residential and Commercial"]
        + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
        + vars["Energy|Demand|Bunkers|International Aviation"]
    )

    vars["Energy|Supply|Fugitive"] = (
        vars["Energy|Supply|Gases|Biomass|Fugitive"]
        + vars["Energy|Supply|Gases|Coal|Fugitive"]
        + vars["Energy|Supply|Gases|Extraction|Fugitive"]
        + vars["Energy|Supply|Gases|Hydrogen|Fugitive"]
        + vars["Energy|Supply|Gases|Natural Gas|Fugitive"]
        + vars["Energy|Supply|Gases|Transportation|Fugitive"]
        + vars["Energy|Supply|Liquids|Biomass|Fugitive"]
        + vars["Energy|Supply|Liquids|Coal|Fugitive"]
        + vars["Energy|Supply|Liquids|Extraction|Fugitive"]
        + vars["Energy|Supply|Liquids|Natural Gas|Fugitive"]
        + vars["Energy|Supply|Liquids|Oil|Fugitive"]
        + vars["Energy|Supply|Liquids|Transportation|Fugitive"]
        + vars["Energy|Supply|Other|Fugitive"]
        + vars["Energy|Supply|Solids|Biomass|Fugitive"]
        + vars["Energy|Supply|Solids|Coal|Fugitive"]
        + vars["Energy|Supply|Solids|Extraction|Fugitive"]
        + vars["Energy|Supply|Solids|Transportation|Fugitive"]
    )

    vars["Energy|Fugitive"] = vars["Energy|Supply|Fugitive"]

    dfs.append(pp_utils.make_outputdf(vars, units_emi))
    return pd.concat(dfs, sort=True)


@_register
def retr_SE_district_heat(units):
    """Energy: Secondary Energy heat.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Biomass"] = pp.out("bio_hpl", units)
    vars["Geothermal"] = pp.out("geo_hpl", units)
    vars["Coal"] = pp.out("coal_hpl", units)
    vars["Oil"] = pp.out("foil_hpl", units)
    vars["Gas"] = pp.out("gas_hpl", units)
    Passout_turbine = pp.out("po_turbine", units)
    vars["Other"] = Passout_turbine

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_SE_synfuels(units):
    """Energy: Secondary Energy synthetic fuels.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Liquids|Oil"] = (
        pp.out(
            "ref_lol",
            units,
            outfilter={"level": ["secondary"], "commodity": ["lightoil"]},
        )
        + pp.out(
            "ref_lol",
            units,
            outfilter={"level": ["secondary"], "commodity": ["fueloil"]},
        )
        + pp.out(
            "ref_hil",
            units,
            outfilter={"level": ["secondary"], "commodity": ["lightoil"]},
        )
        + pp.out(
            "ref_hil",
            units,
            outfilter={"level": ["secondary"], "commodity": ["fueloil"]},
        )
    )

    vars["Liquids|Biomass|w/o CCS"] = pp.out(
        ["eth_bio", "liq_bio"],
        units,
        outfilter={"level": ["primary"], "commodity": ["ethanol"]},
    )

    vars["Liquids|Biomass|w/ CCS"] = pp.out(
        ["eth_bio_ccs", "liq_bio_ccs"],
        units,
        outfilter={"level": ["primary"], "commodity": ["ethanol"]},
    )

    vars["Liquids|Coal|w/o CCS"] = pp.out(
        ["meth_coal", "syn_liq"],
        units,
        outfilter={"level": ["primary"], "commodity": ["methanol", "lightoil"]},
    )

    vars["Liquids|Coal|w/ CCS"] = pp.out(
        ["meth_coal_ccs", "syn_liq_ccs"],
        units,
        outfilter={"level": ["primary"], "commodity": ["methanol", "lightoil"]},
    )

    vars["Liquids|Gas|w/o CCS"] = pp.out(
        "meth_ng", units, outfilter={"level": ["primary"], "commodity": ["methanol"]}
    )

    vars["Liquids|Gas|w/ CCS"] = pp.out(
        "meth_ng_ccs",
        units,
        outfilter={"level": ["primary"], "commodity": ["methanol"]},
    )

    vars["Hydrogen|Coal|w/o CCS"] = pp.out(
        "h2_coal", units, outfilter={"level": ["secondary"], "commodity": ["hydrogen"]}
    )

    vars["Hydrogen|Coal|w/ CCS"] = pp.out(
        "h2_coal_ccs",
        units,
        outfilter={"level": ["secondary"], "commodity": ["hydrogen"]},
    )

    vars["Hydrogen|Gas|w/o CCS"] = pp.out(
        "h2_smr", units, outfilter={"level": ["secondary"], "commodity": ["hydrogen"]}
    )

    vars["Hydrogen|Gas|w/ CCS"] = pp.out(
        "h2_smr_ccs",
        units,
        outfilter={"level": ["secondary"], "commodity": ["hydrogen"]},
    )

    vars["Hydrogen|Biomass|w/o CCS"] = pp.out(
        "h2_bio", units, outfilter={"level": ["secondary"], "commodity": ["hydrogen"]}
    )

    vars["Hydrogen|Biomass|w/ CCS"] = pp.out(
        "h2_bio_ccs",
        units,
        outfilter={"level": ["secondary"], "commodity": ["hydrogen"]},
    )

    vars["Hydrogen|Electricity"] = pp.out(
        "h2_elec", units, outfilter={"level": ["secondary"], "commodity": ["hydrogen"]}
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_SE_gases(units):
    """Energy: Secondary Energy gases.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Natural Gas"] = pp.out(tec = ["gas_bal", "LNG_regas"], trade_tec = ["gas_piped_imp"], units=units)
    vars["Coal"] = pp.out(tec = "coal_gas", units = units)
    vars["Biomass"] = pp.out(tec = "gas_bio", units = units)
    vars["Other"] = pp.inp(tec = "h2_mix", units = units)

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_SE_solids(units):
    """Energy: Secondary Energy solids.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    BiomassIND = pp.inp(tec = "biomass_i", units = units)
    BiomassNC = pp.inp(tec = "biomass_nc", units = units)
    BiomassRC = pp.inp(tec = "biomass_rc", units = units)
    vars["Biomass"] = BiomassNC + BiomassIND + BiomassRC

    CoalIND = pp.inp(tec = ["coal_i", "coal_fs"], units = units)
    CoalRC = pp.inp(tec = "coal_rc", units = units)
    CoalTRP = pp.inp(tec = "coal_trp", units = units)
    vars["Coal"] = CoalIND + CoalRC + CoalTRP

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_SE_elecgen(units):
    """Energy: Secondary Energy electricity generation.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    _Cogen = pp.inp(tec = "po_turbine", units = units, inpfilter={"commodity": ["electr"]})

    # Calculate the possible potential for po-turbine usage
    _Potential = pp.act_rel(
        [
            "coal_ppl_u",
            "coal_ppl",
            "coal_adv",
            "coal_adv_ccs",
            "foil_ppl",
            "loil_ppl",
            "loil_cc",
            "gas_ppl",
            "gas_ct",
            "gas_cc",
            "gas_cc_ccs",
            "bio_ppl",
            "bio_istig",
            "bio_istig_ccs",
            "igcc",
            "igcc_ccs",
            "nuc_lc",
            "nuc_hc",
            "geo_ppl",
        ],
        relfilter={"relation": ["pass_out_trb"]},
        units=units,
    )

    _Biogas = pp.out("gas_bio", units)

    _gas_inp_tecs = [
        "gas_ppl",
        "gas_cc",
        "gas_cc_ccs",
        "gas_ct",
        "gas_hpl",
        "meth_ng",
        "meth_ng_ccs",
        "h2_smr",
        "h2_smr_ccs",
        "gas_t_d",
        "gas_t_d_ch4",
    ]

    _totgas = pp.inp(tec = _gas_inp_tecs, units = units, inpfilter={"commodity": ["gas"]})

    _Frac = (_Cogen / _Potential).fillna(0)

    _BGas_share = (_Biogas / _totgas).fillna(0)

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out(tec = "gas_cc", units = units) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units)).fillna(0)

    _gas_ppl_shr = (pp.out(tec = "gas_ppl", units = units) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units)).fillna(0)

    # ----------------------------------------
    # Electricity use for cooling technologies
    # ----------------------------------------
    inpfilter = {"level": ["secondary"], "commodity": ["electr"]}
    # Bioenergy Cooling Technologies
    _el_cool_bio_istig = pp.inp(
        tec = [
            "bio_istig__cl_fresh",
            "bio_istig__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    _el_cool_bio_istig_ccs = pp.inp(
        tec = [
            "bio_istig_ccs__cl_fresh",
            "bio_istig_ccs__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    _el_cool_bio_ppl = pp.inp(
        tec = [
            "bio_ppl__cl_fresh",
            "bio_ppl__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    # Geothermal cooling technologies
    _el_cool_geo_ppl = pp.inp(
        tec = [
            "geo_ppl__cl_fresh",
            "geo_ppl__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    # Light oil cooling technologies
    _el_cool_loil_cc = pp.inp(
        tec = [
            "loil_cc__cl_fresh",
            "loil_cc__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    _el_cool_loil_ppl = pp.inp(
        tec = [
            "loil_ppl__cl_fresh",
            "loil_ppl__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    # Coal Cooling Technologies
    _el_cool_coal_adv_ccs = pp.inp(
        tec = [
            "coal_adv_ccs__cl_fresh",
            "coal_adv_ccs__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )
    _el_cool_igcc_ccs = pp.inp(
        tec = [
            "igcc_ccs__cl_fresh",
            "igcc_ccs__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    _el_cool_igcc = pp.inp(
        tec = [
            "igcc__cl_fresh",
            "igcc__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    _el_cool_coal_ppl = pp.inp(
        tec = [
            "coal_ppl__cl_fresh",
            "coal_ppl__air",
            "coal_ppl_u__cl_fresh",
            "coal_ppl_u__air",
            "coal_adv__cl_fresh",
            "coal_adv__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    # Fossil Fuel Cooling Technologies
    _el_cool_foil_ppl = pp.inp(
        tec = [
            "foil_ppl__cl_fresh",
            "foil_ppl__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    # Gas Cooling Technologies
    _el_cool_gas_cc_ccs = pp.inp(
        tec = [
            "gas_cc_ccs__cl_fresh",
            "gas_cc_ccs__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    _el_cool_gas_ppl = pp.inp(
        tec = [
            "gas_ppl__cl_fresh",
            "gas_ppl__air",
            "gas_cc__cl_fresh",
            "gas_cc__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    # Nuclear Cooling Technologies
    _el_cool_nuc = pp.inp(
        tec = ["nuc_hc__cl_fresh", "nuc_hc__air", "nuc_lc__cl_fresh", "nuc_lc__air"], units = units,
        inpfilter=inpfilter,
    )

    # CSP (Concentrated Solar Power) Cooling Technologies
    _el_cool_csp_sm1 = pp.inp(
        tec = [
            # Base resources
            "csp_sm1_res__cl_fresh",
            "csp_sm1_res__air",
            # Variants 1 through 7
            "csp_sm1_res1__cl_fresh",
            "csp_sm1_res1__air",
            "csp_sm1_res2__cl_fresh",
            "csp_sm1_res2__air",
            "csp_sm1_res3__cl_fresh",
            "csp_sm1_res3__air",
            "csp_sm1_res4__cl_fresh",
            "csp_sm1_res4__air",
            "csp_sm1_res5__cl_fresh",
            "csp_sm1_res5__air",
            "csp_sm1_res6__cl_fresh",
            "csp_sm1_res6__air",
            "csp_sm1_res7__cl_fresh",
            "csp_sm1_res7__air",
            "csp_sm1_res_hist_2010__cl_fresh",
            "csp_sm1_res_hist_2010__air",
            "csp_sm1_res_hist_2015__cl_fresh",
            "csp_sm1_res_hist_2015__air",
            "csp_sm1_res_hist_2020_cl_fresh",
            "csp_sm1_res_hist_2020__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    _el_cool_csp_sm3 = pp.inp(
        tec = [
            # Base resources
            "csp_sm3_res__cl_fresh",
            "csp_sm3_res__air",
            # Variants 1 through 7
            "csp_sm3_res1__cl_fresh",
            "csp_sm3_res1__air",
            "csp_sm3_res2__cl_fresh",
            "csp_sm3_res2__air",
            "csp_sm3_res3__cl_fresh",
            "csp_sm3_res3__air",
            "csp_sm3_res4__cl_fresh",
            "csp_sm3_res4__air",
            "csp_sm3_res5__cl_fresh",
            "csp_sm3_res5__air",
            "csp_sm3_res6__cl_fresh",
            "csp_sm3_res6__air",
            "csp_sm3_res7__cl_fresh",
            "csp_sm3_res7__air",
        ],
        units = units,
        inpfilter=inpfilter,
    )

    # --------------------------------
    # Electricity generation from coal
    # --------------------------------

    vars["Coal|w/o CCS"] = (
        _se_elec_woCCSretro(
            "coal_ppl", "c_ppl_co2scr", units, _Frac, outfilter={"commodity": "electr"}
        )
        + _se_elec_po_turb("coal_adv", units, _Frac, outfilter={"commodity": "electr"})
        + _se_elec_po_turb(
            "coal_ppl_u", units, _Frac, outfilter={"commodity": "electr"}
        )
        + pp.out(
            ["meth_coal", "h2_coal", "syn_liq"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        - _el_cool_coal_ppl
        - _el_cool_igcc
    )

    vars["Coal|w/ CCS"] = (
        _se_elec_wCCSretro("coal_ppl", "c_ppl_co2scr", units, _Frac)
        + _se_elec_po_turb(
            "coal_adv_ccs", units, _Frac, outfilter={"commodity": "electr"}
        )
        + _se_elec_po_turb("igcc_ccs", units, _Frac, outfilter={"commodity": "electr"})
        + pp.out(
            ["meth_coal_ccs", "h2_coal_ccs", "syn_liq_ccs"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        - _el_cool_coal_adv_ccs
        - _el_cool_igcc_ccs
    )

    # -------------------------------
    # Electricity generation from gas
    # -------------------------------

    _Gas_woCCS = (
        _se_elec_woCCSretro(
            "gas_ppl",
            "g_ppl_co2scr",
            units,
            _Frac,
            share=_gas_ppl_shr,
            outfilter={"commodity": "electr"},
        )
        + _se_elec_po_turb("gas_ct", units, _Frac, outfilter={"commodity": "electr"})
        + _se_elec_woCCSretro(
            "gas_cc",
            "g_ppl_co2scr",
            units,
            _Frac,
            share=_gas_cc_shr,
            outfilter={"commodity": "electr"},
        )
        + pp.out(
            ["h2_smr"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        - _el_cool_gas_ppl
    )

    vars["Gas|w/o CCS"] = _Gas_woCCS * (1 - _BGas_share)

    _Gas_wCCS = (
        _se_elec_wCCSretro("gas_ppl", "g_ppl_co2scr", units, _Frac, share=_gas_ppl_shr)
        + _se_elec_wCCSretro("gas_cc", "g_ppl_co2scr", units, _Frac, share=_gas_cc_shr)
        + _se_elec_po_turb(
            "gas_cc_ccs", units, _Frac, outfilter={"commodity": "electr"}
        )
        + pp.out(
            ["h2_smr_ccs"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        - _el_cool_gas_cc_ccs
    )

    vars["Gas|w/ CCS"] = _Gas_wCCS * (1 - _BGas_share)

    # -------------------------------
    # Electricity generation from oil
    # -------------------------------

    vars["Oil|w/o CCS"] = (
        _se_elec_po_turb("foil_ppl", units, _Frac, outfilter={"commodity": "electr"})
        + _se_elec_po_turb("loil_ppl", units, _Frac, outfilter={"commodity": "electr"})
        + _se_elec_po_turb("oil_ppl", units, _Frac, outfilter={"commodity": "electr"})
        + _se_elec_po_turb("loil_cc", units, _Frac, outfilter={"commodity": "electr"})
        - _el_cool_foil_ppl
        - _el_cool_loil_ppl
        - _el_cool_loil_cc
    )

    # -----------------------------------
    # Electricity generation from biomass
    # -----------------------------------

    vars["Biomass|w/o CCS"] = (
        _se_elec_woCCSretro(
            "bio_ppl", "bio_ppl_co2scr", units, _Frac, outfilter={"commodity": "electr"}
        )
        + _se_elec_po_turb("bio_istig", units, _Frac, outfilter={"commodity": "electr"})
        + pp.out(
            ["h2_bio", "eth_bio", "liq_bio"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        + _Gas_woCCS * _BGas_share
        - _el_cool_bio_ppl
        - _el_cool_bio_istig
    )

    # eth bio CCS is set to 0 because OFR doesnt know what to do with eff = 0.
    # normally it is replaced by 1, but this doesnt work in this case!
    vars["Biomass|w/ CCS"] = (
        _se_elec_wCCSretro("bio_ppl", "bio_ppl_co2scr", units, _Frac)
        + _se_elec_po_turb(
            "bio_istig_ccs", units, _Frac, outfilter={"commodity": "electr"}
        )
        + pp.out(
            ["h2_bio_ccs", "eth_bio_ccs", "liq_bio_ccs"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        + _Gas_wCCS * _BGas_share
        - _el_cool_bio_istig_ccs
    )

    # -------------------
    # Electricity storage
    # -------------------

    # _Storage_output = pp.out("stor_ppl", units)
    # _Storage_input = (pp.inp("stor_ppl", units) / _Storage_output).fillna(0)

    # ------------------------------------
    # Operating reserve for solar and wind
    # ------------------------------------

    # _SolarPV_OR = pp.act_rel(["solar_cv1", "solar_cv2",
    #                           "solar_cv3", "solar_cv4"],
    #                          relfilter={"relation": ["oper_res"]}, units=units)

    # _Wind_OR = pp.act_rel(["wind_cv1", "wind_cv2",
    #                        "wind_cv3", "wind_cv4"],
    #                       relfilter={"relation": ["oper_res"]}, units=units)

    # --------------------------
    # Solar PV related variables
    # --------------------------

    _SolarPV_curt_inp = pp.inp(
        tec = ["solar_curtailment1", "solar_curtailment2", "solar_curtailment3"], units = units,
        inpfilter={"commodity": ["electr"]},
    )

    _SolarPV_raw = (
        pp.out(
            tec = [
                "solar_res1",
                "solar_res2",
                "solar_res3",
                "solar_res4",
                "solar_res5",
                "solar_res6",
                "solar_res7",
                "solar_res8",
                "solar_res_hist_2000",
                "solar_res_hist_2005",
                "solar_res_hist_2010",
                "solar_res_hist_2015",
                "solar_res_hist_2020",
                "solar_res_hist_2025",
            ],
            units = units,
        )
        - _SolarPV_curt_inp
    )

    _SolarPV_onsite = pp.out(
        tec = [
            "solar_res_rt_hist_2000",
            "solar_res_rt_hist_2005",
            "solar_res_rt_hist_2010",
            "solar_res_rt_hist_2015",
            "solar_res_rt_hist_2020",
            "solar_res_rt_hist_2025",
            "solar_res_RT1",
            "solar_res_RT2",
            "solar_res_RT3",
            "solar_res_RT4",
            "solar_res_RT5",
            "solar_res_RT6",
            "solar_res_RT7",
            "solar_res_RT8",
        ],
        units = units,
    )

    SolarPV = _SolarPV_raw + _SolarPV_onsite

    # _SolarPV_woStorage_total = _SolarPV_raw - _Storage_input *\
    #     (_SolarPV_OR / (_Wind_OR + _SolarPV_OR)).fillna(0)

    # _SolarPV_woStorage = _SolarPV_woStorage_total + _SolarPV_onsite
    # _SolarPV_wStorage = _Storage_output *\
    #     (_SolarPV_OR / (_Wind_OR + _SolarPV_OR)).fillna(0)

    vars["Solar|PV"] = _SolarPV_raw + _SolarPV_onsite
    vars["Solar|PV|Commercial"] = _SolarPV_raw
    vars["Solar|PV|Residential"] = _SolarPV_onsite
    vars["Solar|PV|Curtailment"] = _SolarPV_curt_inp

    # ---------------------
    # CSP related variables
    # ---------------------

    CSP = (
        pp.out(
            tec = [
                "csp_sm1_res",
                "csp_sm1_res1",
                "csp_sm1_res2",
                "csp_sm1_res3",
                "csp_sm1_res4",
                "csp_sm1_res5",
                "csp_sm1_res6",
                "csp_sm1_res7",
                "csp_sm1_res_hist_2000",
                "csp_sm1_res_hist_2005",
                "csp_sm1_res_hist_2010",
                "csp_sm1_res_hist_2015",
                "csp_sm1_res_hist_2020",
                "csp_sm1_res_hist_2025",
                "csp_sm3_res",
                "csp_sm3_res1",
                "csp_sm3_res2",
                "csp_sm3_res3",
                "csp_sm3_res4",
                "csp_sm3_res5",
                "csp_sm3_res6",
                "csp_sm3_res7",
            ],
            units = units,
        )
        - _el_cool_csp_sm1
        - _el_cool_csp_sm3
    )

    vars["Solar|CSP"] = CSP

    # ----------------------
    # Wind related variables
    # ----------------------

    _Wind_res_act = pp.out(
        tec = [
            "wind_res1",
            "wind_res2",
            "wind_res3",
            "wind_res4",
            "wind_res_hist_2000",
            "wind_res_hist_2005",
            "wind_res_hist_2010",
            "wind_res_hist_2015",
            "wind_res_hist_2020",
            "wind_res_hist_2025",
        ],
        units = units,
    )

    _Wind_ref_act = pp.out(
        tec = [
            "wind_ref1",
            "wind_ref2",
            "wind_ref3",
            "wind_ref4",
            "wind_ref5",
            "wind_ref_hist_2000",
            "wind_ref_hist_2005",
            "wind_ref_hist_2010",
            "wind_ref_hist_2015",
            "wind_ref_hist_2020",
            "wind_ref_hist_2025",
        ],
        units = units,
    )

    _Wind_curt_inp = pp.inp(
        tec = ["wind_curtailment1", "wind_curtailment2", "wind_curtailment3"], units = units,
        inpfilter={"commodity": ["electr"]},
    )

    _Wind_onshore = _Wind_res_act - _Wind_curt_inp * (
        _Wind_res_act / (_Wind_res_act + _Wind_ref_act)
    ).fillna(0)

    _Wind_offshore = _Wind_ref_act - _Wind_curt_inp * (
        _Wind_ref_act / (_Wind_res_act + _Wind_ref_act)
    ).fillna(0)

    # _Wind = _Wind_onshore + _Wind_offshore

    # _Wind_woStorage = _Wind - (_Storage_input * _Wind_OR /
    #                            (_Wind_OR + _SolarPV_OR)).fillna(0)
    # _Wind_woStorage_onshore = _Wind_woStorage *\
    #     (_Wind_onshore / _Wind).fillna(0)\
    # _Wind_woStorage_offshore = _Wind_woStorage *\
    #     (_Wind_offshore / _Wind).fillna(0)
    # _Wind_wStorage = _Storage_output *\
    #     (_Wind_OR / (_Wind_OR + _SolarPV_OR)).fillna(0)
    # _Wind_wStorage_onshore = _Wind_wStorage * (_Wind_onshore / _Wind).fillna(0)
    # _Wind_wStorage_offshore = _Wind_wStorage *\
    #     (_Wind_offshore / _Wind).fillna(0)

    vars["Wind|Onshore"] = _Wind_onshore
    vars["Wind|Offshore"] = _Wind_offshore
    vars["Wind|Curtailment"] = _Wind_curt_inp

    vars["Hydro"] = pp.out(["hydro_lc", "hydro_hc"], units)

    vars["Geothermal"] = (
        _se_elec_po_turb("geo_ppl", units, _Frac, outfilter={"commodity": "electr"})
        - _el_cool_geo_ppl
    )

    vars["Nuclear"] = (
        _se_elec_po_turb(
            "nuc_lc",
            units,
            _Frac,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        + _se_elec_po_turb(
            "nuc_hc",
            units,
            _Frac,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        + _se_elec_po_turb(
            "nuc_fbr",
            units,
            _Frac,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        - _el_cool_nuc
    )

    # comment["Other"] = "includes electricity production from fuel"
    #     " cells on the final energy level"

    vars["Other"] = (
        pp.inp(
            tec = "h2_fc_trp", units = units, inpfilter={"level": ["final"], "commodity": ["electr"]}
        )
        + pp.inp(
            tec = "h2_fc_I", units = units, inpfilter={"level": ["final"], "commodity": ["electr"]}
        )
        + pp.inp(
            tec = "h2_fc_RC", units = units, inpfilter={"level": ["final"], "commodity": ["electr"]}
        )
    )

    # ------------------------------------------------
    # Additonal reporting for GAINS diagnostic linkage
    # ------------------------------------------------

    vars["Storage Losses"] = pp.inp(
        tec = "stor_ppl", units = units, inpfilter={"commodity": ["electr"]}
    )

    vars["Transmission Losses"] = pp.inp(
        tec = "elec_t_d", units = units, inpfilter={"commodity": ["electr"]}
    ) - pp.out(tec = "elec_t_d", units = units)

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_pe(units, method=None):
    """Energy: Primary Energy.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    method : str
        If supplied, the direct equivalent will be calculated.
        Otherwise "substiution" will apply IEA conversion factors
        for heat/electricity for renewables and nuclear.
    """

    vars = {}

    if run_history != "True":
        group = ["Region", "Mode", "Vintage"]
    else:
        group = ["Region"]

    if method == "substitution":
        elec_factor = 0.35
        # hydrogen_factor = 0.6
        heat_factor = 0.8

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    _Cogen = pp.inp(tec = "po_turbine", units = units, inpfilter={"commodity": ["electr"]})

    _Potential = pp.act_rel(
        tec = [
            "coal_ppl_u",
            "coal_ppl",
            "coal_adv",
            "coal_adv_ccs",
            "foil_ppl",
            "loil_ppl",
            "loil_cc",
            "gas_ppl",
            "gas_ct",
            "gas_cc",
            "gas_cc_ccs",
            "bio_ppl",
            "bio_istig",
            "bio_istig_ccs",
            "igcc",
            "igcc_ccs",
            "nuc_lc",
            "nuc_hc",
            "geo_ppl",
        ],
        relfilter={"relation": ["pass_out_trb"]},
        units=units,
    )

    _Biogas = pp.out(tec = "gas_bio", units = units)
    _H2gas = pp.out(tec = "h2_mix", units = units)
    _Coalgas = pp.out(tec = "coal_gas", units = units)

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out(tec = "gas_cc", units = units) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units)).fillna(0)

    _gas_ppl_shr = (pp.out(tec = "gas_ppl", units = units) / pp.out(tec = ["gas_cc", "gas_ppl"], units = units)).fillna(0)

    # Example of a set
    _gas_inp_tecs = [
        "gas_ppl",
        "gas_cc",
        "gas_cc_ccs",
        "gas_ct",
        "gas_hpl",
        "meth_ng",
        "meth_ng_ccs",
        "h2_smr",
        "h2_smr_ccs",
        "gas_t_d",
        "gas_t_d_ch4",
    ]

    _totgas = pp.inp(tec = _gas_inp_tecs, units = units, inpfilter={"commodity": ["gas"]})
    _Frac = (_Cogen / _Potential).fillna(0)
    _BGas_share = (_Biogas / _totgas).fillna(0)
    _SynGas_share = ((_Biogas + _H2gas + _Coalgas) / _totgas).fillna(0)

    # -------------------
    # Primary Energy Coal
    # -------------------

    vars["Coal"] = (
        pp.inp(
            tec = ["coal_extr_ch4", "coal_extr", "lignite_extr"], trade_tec = ["coal_shipped_imp"], units=units,
            inpfilter={"commodity": ["coal", "lignite"]},
        )
        - pp.inp(trade_tec=["coal_shipped_exp"], units=units, inpfilter={"commodity": ["coal"]}
        )
        + pp.inp(tec = ["meth_bunker"], units = units)
    )

    # Note OFR 20180412: Correction inserted. scrubber output per vintage cannot
    # be divided by powerplant eff per vintage. In most cases this does work except
    # if the powerplant doesnt exist anymore, or if vintaging is switched on.
    # In either case, the efficiency with which the scrubber output is divided
    # is a weighted average based on the powerplant activity
    vars["Coal|w/ CCS"] = _pe_wCCSretro(
        "coal_ppl",
        "c_ppl_co2scr",
        group,
        inpfilter={"level": ["secondary"], "commodity": ["coal"]},
        units=units,
    ) + pp.inp(
        tec = ["igcc_ccs", "coal_adv_ccs", "meth_coal_ccs", "syn_liq_ccs", "h2_coal_ccs"], units = units,
        inpfilter={"commodity": ["coal"]},
    )

    vars["Coal|w/o CCS"] = vars["Coal"] - vars["Coal|w/ CCS"]

    # ------------------
    # Primary Energy Oil
    # ------------------

    vars["Oil|w/o CCS"] = (
        pp.inp(
            tec = [
                "oil_extr_1_ch4",
                "oil_extr_2_ch4",
                "oil_extr_3_ch4",
                "oil_extr_1",
                "oil_extr_2",
                "oil_extr_3",
                "oil_extr_4_ch4",
                "oil_extr_4",
                "oil_extr_5",
                "oil_extr_6",
                "oil_extr_7",
                "oil_extr_8",
            ],
            units = units,
            inpfilter={"level": ["resource"]},
        )
        + pp.inp(trade_tec=["crudeoil_shipped_imp", "foil_shipped_imp", "loil_shipped_imp",
                            "crudeoil_piped_imp", "foil_piped_imp", "loil_piped_imp"], units=units,
                            inpfilter={"commodity": ["crudeoil", "fueloil", "lightoil"]})
        - pp.inp(trade_tec=["crudeoil_shipped_exp", "foil_shipped_exp", "loil_shipped_exp",
                            "crudeoil_piped_exp", "foil_piped_exp", "loil_piped_exp"], units=units,
                            inpfilter={"commodity": ["crudeoil", "fueloil", "lightoil"]})
        + pp.inp(tec = ["foil_bunker", "loil_bunker"], units = units)
    )

    # ------------------
    # Primary Energy Gas
    # ------------------

    vars["Gas"] = (
        pp.inp(
            tec = [
                "gas_extr_1",
                "gas_extr_2",
                "gas_extr_3",
                "gas_extr_4",
                "gas_extr_5",
                "gas_extr_6",
                "gas_extr_7",
                "gas_extr_8",
            ],
            units = units,
            inpfilter={"level": ["resource"]},
        )
        + pp.inp(trade_tec=["LNG_shipped_imp", "gas_piped_imp"], units=units)
        - pp.inp(
            trade_tec=["LNG_shipped_exp", "gas_piped_exp"], units=units,
            inpfilter={"commodity": ["LNG", "gas"]}
        )
        + pp.inp(tec = ["LNG_bunker"], units = units)
        + pp.out(
            tec = ["oil_extr_1_ch4", "oil_extr_2_ch4", "oil_extr_3_ch4", "oil_extr_4_ch4"],
            units = units,
            outfilter={"commodity": ["gas"]},
        )
    )

    # Note OFR 20180412: Correction inserted. scrubber output per vintage cannot
    # be divided by powerplant eff per vintage. In most cases this does work except
    # if the powerplant doesnt exist anymore, or if vintaging is switched on.
    # In either case, the efficiency with which the scrubber output is divided
    # is a weighted average based on the powerplant activity
    _Gas_wCCS = (
        _pe_wCCSretro(
            "gas_cc",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units,
            share=_gas_cc_shr,
        )
        + _pe_wCCSretro(
            "gas_ppl",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units,
            share=_gas_ppl_shr,
        )
        + pp.inp(tec = ["gas_cc_ccs", "h2_smr_ccs"], units = units, inpfilter={"commodity": ["gas"]})
    )

    vars["Gas|w/ CCS"] = _Gas_wCCS * (1 - _BGas_share)

    vars["Gas|w/o CCS"] = vars["Gas"] - vars["Gas|w/ CCS"]

    # ----------------------
    # Primary Energy Nuclear
    # ----------------------

    if method == "substitution":
        vars["Nuclear"] = (
            _se_elec_po_turb(
                "nuc_lc",
                units,
                _Frac,
                outfilter={"level": ["secondary"], "commodity": ["electr"]},
            )
            / elec_factor
            + _se_elec_po_turb(
                "nuc_hc",
                units,
                _Frac,
                outfilter={"level": ["secondary"], "commodity": ["electr"]},
            )
            / elec_factor
            + _se_heat_po_turb(
                "nuc_lc",
                units,
                _Frac,
                outfilter={"level": ["secondary"], "commodity": ["electr"]},
            )
            / heat_factor
            + _se_heat_po_turb(
                "nuc_hc",
                units,
                _Frac,
                outfilter={"level": ["secondary"], "commodity": ["electr"]},
            )
            / heat_factor
        )
    else:
        vars["Nuclear"] = pp.out(
            ["nuc_lc", "nuc_hc"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )

    # ----------------------
    # Primary Energy Biomass
    # ----------------------

    # The individual technologies need to be listed instead of using the output
    # of "land_use_biomass, because other wise the historical activty is not
    # trackable.
    vars["Biomass"] = (
        pp.inp(
            tec = [
                "bio_hpl",
                "bio_istig",
                "bio_istig_ccs",
                "bio_ppl",
                "biomass_nc",
                "biomass_t_d",
                "eth_bio",
                "eth_bio_ccs",
                "gas_bio",
                "h2_bio",
                "h2_bio_ccs",
                "liq_bio",
                "liq_bio_ccs",
            ],
            units = units,
            inpfilter={"commodity": ["biomass"]},
        )
        # Biomass import/exports are excluded as the input of all technologies
        # is used therefore this already includes traded biomass.
        # + pp.inp(["biomass_imp"], units)
        # - pp.inp(["biomass_exp"], units)
        + pp.inp(tec = ["eth_bunker"], units = units)
    )

    # Note OFR 20180412: Correction inserted. scrubber output per vintage cannot
    # be divided by powerplant eff per vintage. In most cases this does work except
    # if the powerplant doesnt exist anymore, or if vintaging is switched on.
    # In either case, the efficiency with which the scrubber output is divided
    # is a weighted average based on the powerplant activity
    vars["Biomass|w/ CCS"] = (
        _pe_wCCSretro(
            "bio_ppl",
            "bio_ppl_co2scr",
            group,
            inpfilter={"level": ["primary"], "commodity": ["biomass"]},
            units=units,
        )
        + pp.inp(tec = "bio_istig_ccs", units = units, inpfilter={"commodity": ["biomass"]})
        + pp.inp(
            tec = ["h2_bio_ccs", "eth_bio_ccs", "liq_bio_ccs"],
            units = units,
            inpfilter={"commodity": ["biomass"]},
        )
        + _Gas_wCCS * _BGas_share
    )

    # Note OFR: eth_bunker should be excluded because biomass from GLOBIOM
    # already includes the total biomass production per region.
    vars["Biomass|w/o CCS"] = vars["Biomass"] - vars["Biomass|w/ CCS"]
    # + pp.inp(["eth_bunker"], units)

    # -------------------
    # Primary Energy Wind
    # -------------------

    vars["Wind"] = pp.out(
        tec = [
            "wind_res1",
            "wind_res2",
            "wind_res3",
            "wind_res4",
            "wind_ref1",
            "wind_ref2",
            "wind_ref3",
            "wind_ref4",
            "wind_ref5",
            "wind_res_hist_2000",
            "wind_res_hist_2005",
            "wind_res_hist_2010",
            "wind_res_hist_2015",
            "wind_res_hist_2020",
            "wind_res_hist_2025",
            "wind_ref_hist_2000",
            "wind_ref_hist_2005",
            "wind_ref_hist_2010",
            "wind_ref_hist_2015",
            "wind_ref_hist_2020",
            "wind_ref_hist_2025",
        ],
        units = units,
    ) - pp.inp(
        tec = ["wind_curtailment1", "wind_curtailment2", "wind_curtailment3"], units = units,
        inpfilter={"commodity": ["electr"]},
    )

    if method == "substitution":
        vars["Wind"] /= elec_factor

    # --------------------
    # Primary Energy Hydro
    # --------------------

    vars["Hydro"] = pp.out(tec = ["hydro_hc", "hydro_lc"], units = units)

    if method == "substitution":
        vars["Hydro"] /= elec_factor

    # --------------------
    # Primary Energy Ocean
    # --------------------

    vars["Ocean"] = pp_utils._make_zero()

    # ---------------------------------
    # Primary Energy Solar (PV and CSP)
    # ---------------------------------

    _pv_elec = pp.out(
        tec = [
            "solar_res1",
            "solar_res2",
            "solar_res3",
            "solar_res4",
            "solar_res5",
            "solar_res6",
            "solar_res7",
            "solar_res8",
            "solar_pv_RC",
            "solar_pv_I",
            "solar_res_hist_2000",
            "solar_res_hist_2005",
            "solar_res_hist_2010",
            "solar_res_hist_2015",
            "solar_res_hist_2020",
            "solar_res_hist_2025",
            "solar_res_rt_hist_2000",
            "solar_res_rt_hist_2005",
            "solar_res_rt_hist_2010",
            "solar_res_rt_hist_2015",
            "solar_res_rt_hist_2020",
            "solar_res_rt_hist_2025",
            "solar_res_RT1",
            "solar_res_RT2",
            "solar_res_RT3",
            "solar_res_RT4",
            "solar_res_RT5",
            "solar_res_RT6",
            "solar_res_RT7",
            "solar_res_RT8",
        ],
        units = units,
    )

    _solar_curtailment_elec = pp.inp(
        tec = ["solar_curtailment1", "solar_curtailment2", "solar_curtailment3"], units = units,
        inpfilter={"commodity": ["electr"]},
    )

    _solar_heat = pp.out(tec = ["solar_rc", "solar_i"], units = units)

    _csp_elec = pp.out(
        tec = [
            "csp_sm1_res",
            "csp_sm1_res1",
            "csp_sm1_res2",
            "csp_sm1_res3",
            "csp_sm1_res4",
            "csp_sm1_res5",
            "csp_sm1_res6",
            "csp_sm1_res7",
            "csp_sm1_res_hist_2000",
            "csp_sm1_res_hist_2005",
            "csp_sm1_res_hist_2010",
            "csp_sm1_res_hist_2015",
            "csp_sm1_res_hist_2020",
            "csp_sm1_res_hist_2025",
            "csp_sm3_res",
            "csp_sm3_res1",
            "csp_sm3_res2",
            "csp_sm3_res3",
            "csp_sm3_res4",
            "csp_sm3_res5",
            "csp_sm3_res6",
            "csp_sm3_res7",
        ],
        units = units,
    )

    if method == "substitution":
        vars["Solar"] = (
            (_pv_elec - _solar_curtailment_elec + _csp_elec) / elec_factor
        ).fillna(0) + (_solar_heat / heat_factor).fillna(0)
    else:
        vars["Solar"] = _pv_elec - _solar_curtailment_elec + _csp_elec + _solar_heat

    # -------------------------
    # Primary Energy Geothermal
    # -------------------------

    _geothermal_elec = _se_elec_po_turb(
        "geo_ppl", units, _Frac, outfilter={"commodity": "electr"}
    )
    _geothermal_heat = _se_heat_po_turb(
        "geo_ppl", units, _Frac, outfilter={"commodity": "electr"}
    ) + pp.out(tec = ["geo_hpl"], units = units)

    if method == "substitution":
        vars["Geothermal"] = (_geothermal_elec / elec_factor).fillna(0) + (
            _geothermal_heat / heat_factor
        ).fillna(0)
    else:
        vars["Geothermal|Electricity|w/o CCS"] = _geothermal_elec

        vars["Geothermal|Heat"] = _geothermal_heat

    # Primary Energy - SE Trade
    vars["Secondary Energy Trade"] = pp.out(
        tec = ["meth_imp"], trade_tec = ["lh2_shipped_imp", "eth_shipped_imp", "elec_imp"], units=units,
    ) - pp.inp(
        tec = ["meth_exp"], trade_tec = ["lh2_shipped_exp", "eth_shipped_exp", "elec_exp"], units=units,
        inpfilter={"commodity": ["methanol", "lh2",
                                 "electr_africa", 'electr_america', 'electr_asia',
                                 'electr_eurasia', 'electr_eur_afr', 'electr_asia_afr']}
    )

    # --------------------
    # Priamry Energy Other
    # --------------------

    vars["Other"] = pp.inp(tec = "LH2_bunker", units = units)

    if method != "substitution":
        # ------------------------------------------------
        # Primary Energy Electricity from coal without CCS
        # ------------------------------------------------

        vars["Coal|Electricity|w/o CCS|Hardcoal Subcritical w/o FGD/DeNOx"] = (
            _pe_elec_po_turb(
                "coal_ppl_u",
                group,
                units,
                _Frac,
                inpfilter={"commodity": ["coal"]},
                outfilter={"commodity": "electr"},
            )
        )

        vars["Coal|Electricity|w/o CCS|Hardcoal Subcritical w/ FGD/DeNOx"] = (
            _pe_elec_woCCSretro(
                "coal_ppl",
                "c_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["coal"]},
                units=units,
                _Frac=_Frac,
            )
        )

        vars["Coal|Electricity|w/o CCS|Hardcoal Supercritical"] = _pe_elec_po_turb(
            "coal_adv",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["coal"]},
            outfilter={"commodity": "electr"},
        )

        vars["Coal|Electricity|w/o CCS|Hardcoal IGCC"] = _pe_elec_po_turb(
            "igcc",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["coal"]},
            outfilter={"commodity": "electr"},
            units=units,
            _Frac=_Frac,
        )

        # ---------------------------------------------
        # Primary Energy Electricity from coal with CCS
        # ---------------------------------------------

        vars["Coal|Electricity|w/ CCS|Hardcoal Subcritical w/ FGD/DeNOx"] = (
            _pe_elec_wCCSretro(
                "coal_ppl",
                "c_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["coal"]},
                units=units,
                _Frac=_Frac,
            )
        )

        vars["Coal|Electricity|w/ CCS|Hardcoal Supercritical"] = _pe_elec_po_turb(
            "coal_adv_ccs",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["coal"]},
            outfilter={"commodity": "electr"},
        )

        vars["Coal|Electricity|w/ CCS|Hardcoal IGCC"] = _pe_elec_po_turb(
            "igcc_ccs",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["coal"]},
            outfilter={"commodity": "electr"},
        )

        # -----------------------------------------------
        # Primary Energy Electricity from gas without CCS
        # -----------------------------------------------

        vars["Gas|Electricity|w/o CCS|Natural Gas Steam Cycle"] = (
            _pe_elec_woCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_ppl_shr,
            )
        ) * (1 - _BGas_share)

        vars["Gas|Electricity|w/o CCS|Natural Gas Combustion Turbine"] = (
            _pe_elec_po_turb(
                "gas_ct",
                group,
                units,
                _Frac,
                inpfilter={"commodity": ["gas"]},
                outfilter={"commodity": "electr"},
            )
        ) * (1 - _BGas_share)

        vars["Gas|Electricity|w/o CCS|Natural Gas Combined Cycle"] = (
            _pe_elec_woCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
        ) * (1 - _BGas_share)

        # -----------------------------------------------
        # Primary Energy Electricity from gas with CCS
        # -----------------------------------------------

        vars["Gas|Electricity|w/ CCS|Natural Gas Steam Cycle"] = (
            _pe_elec_wCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
        ) * (1 - _BGas_share)

        vars["Gas|Electricity|w/ CCS|Natural Gas Combined Cycle"] = (
            _pe_elec_wCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
            + _pe_elec_po_turb(
                "gas_cc_ccs",
                group,
                units,
                _Frac,
                inpfilter={"commodity": ["gas"]},
                outfilter={"commodity": "electr"},
            )
        ) * (1 - _BGas_share)

        # -----------------------------------------------
        # Primary Energy Electricity from oil without CCS
        # -----------------------------------------------

        vars["Oil|Electricity|w/o CCS|Crude Oil Steam Cycle"] = _pe_elec_po_turb(
            "oil_ppl",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["crudeoil"]},
            outfilter={"commodity": "electr"},
        )

        vars["Oil|Electricity|w/o CCS|Heavy Fuel Oil Steam Cycle"] = _pe_elec_po_turb(
            "foil_ppl",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["fueloil"]},
            outfilter={"commodity": "electr"},
        )

        vars["Oil|Electricity|w/o CCS|Light Fuel Oil Combined Cycle"] = (
            _pe_elec_po_turb(
                "loil_cc",
                group,
                units,
                _Frac,
                inpfilter={"commodity": ["lightoil"]},
                outfilter={"commodity": "electr"},
            )
        )

        vars["Oil|Electricity|w/o CCS|Light Fuel Oil Steam Cycle"] = _pe_elec_po_turb(
            "loil_ppl",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["lightoil"]},
            outfilter={"commodity": "electr"},
        )

        vars["Oil|Electricity|w/ CCS"] = pp_utils._make_zero()

        # ---------------------------------------------------
        # Primary Energy Electricity from biomass without CCS
        # ---------------------------------------------------

        vars["Biomass|Electricity|w/o CCS|Biomass Steam Cycle"] = _pe_elec_woCCSretro(
            "bio_ppl",
            "bio_ppl_co2scr",
            group,
            inpfilter={"level": ["primary"], "commodity": ["biomass"]},
            units=units,
            _Frac=_Frac,
        )

        vars["Biomass|Electricity|w/o CCS|Biomass IGCC"] = _pe_elec_po_turb(
            "bio_istig",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["biomass"]},
            outfilter={"commodity": "electr"},
        )

        vars["Biomass|Electricity|w/o CCS|Biogas Steam Cycle"] = (
            _pe_elec_woCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_ppl_shr,
            )
        ) * _BGas_share

        vars["Biomass|Electricity|w/o CCS|Biogas Combustion Turbine"] = (
            _pe_elec_po_turb(
                "gas_ct",
                group,
                units,
                _Frac,
                inpfilter={"commodity": ["gas"]},
                outfilter={"commodity": "electr"},
            )
        ) * _BGas_share

        vars["Biomass|Electricity|w/o CCS|Biogas Combined Cycle"] = (
            _pe_elec_woCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
        ) * _BGas_share

        # ------------------------------------------------
        # Primary Energy Electricity from biomass with CCS
        # ------------------------------------------------

        vars["Biomass|Electricity|w/ CCS|Biomass Steam Cycle"] = _pe_elec_wCCSretro(
            "bio_ppl",
            "bio_ppl_co2scr",
            group,
            inpfilter={"level": ["primary"], "commodity": ["biomass"]},
            units=units,
            _Frac=_Frac,
        )

        vars["Biomass|Electricity|w/ CCS|Biomass IGCC"] = _pe_elec_po_turb(
            "bio_istig_ccs",
            group,
            units,
            _Frac,
            inpfilter={"commodity": ["biomass"]},
            outfilter={"commodity": "electr"},
        )

        vars["Biomass|Electricity|w/ CCS|Biogas Steam Cycle"] = (
            _pe_elec_wCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
        ) * _BGas_share

        vars["Biomass|Electricity|w/ CCS|Biogas Combined Cycle"] = (
            _pe_elec_wCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
            + _pe_elec_po_turb(
                "gas_cc_ccs",
                group,
                units,
                _Frac,
                inpfilter={"commodity": ["gas"]},
                outfilter={"commodity": "electr"},
            )
        ) * _BGas_share

        # -----------------------------------
        # Primary Energy from biomass (other)
        # -----------------------------------

        vars["Biomass|1st Generation"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|1st Generation"],
            }
        )

        vars["Biomass|1st Generation|Biodiesel"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|1st Generation|Biodiesel"],
            }
        )

        vars["Biomass|1st Generation|Bioethanol"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|1st Generation|Bioethanol"],
            }
        )

        vars["Biomass|Energy Crops"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Energy Crops"],
            }
        )

        vars["Biomass|Fuelwood"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Fuelwood"],
            }
        )

        vars["Biomass|Modern"] = pp.land_out(
            lu_out_filter={"level": ["land_use"], "commodity": ["bioenergy"]},
            units=units,
        ) - pp.inp(tec = "biomass_nc", units = units)

        vars["Biomass|Other"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Other"],
            }
        )

        vars["Biomass|Residues"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Residues"],
            }
        )

        vars["Biomass|Residues|Crops"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Residues|Crops"],
            }
        )

        vars["Biomass|Residues|Forest industry"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Residues|Forest industry"],
            }
        )

        vars["Biomass|Residues|Logging"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Residues|Logging"],
            }
        )

        vars["Biomass|Roundwood harvest"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Roundwood harvest"],
            }
        )

        vars["Biomass|Traditional"] = pp.inp(tec = "biomass_nc", units = units)

        # ----------------------------------------------------
        # Additonal reporting for GAINS diagnostic for biomass
        # ----------------------------------------------------

        vars["Biomass|Gases"] = pp.inp(
            tec = ["gas_bio"], units = units, inpfilter={"commodity": ["biomass"]}
        )

        vars["Biomass|Heat"] = pp.inp(tec = "bio_hpl", units = units)

        vars["Biomass|Hydrogen|w/o CCS"] = pp.inp(
            tec = ["h2_bio"], units = units, inpfilter={"commodity": ["biomass"]}
        )

        vars["Biomass|Hydrogen|w/ CCS"] = pp.inp(
            tec = ["h2_bio_ccs"], units = units, inpfilter={"commodity": ["biomass"]}
        )

        vars["Biomass|Liquids|w/o CCS"] = pp.inp(
            tec = ["eth_bio", "liq_bio"], units = units,
            inpfilter={"commodity": ["biomass"]},
        )

        vars["Biomass|Liquids|w/ CCS"] = pp.inp(
            tec = ["eth_bio_ccs", "liq_bio_ccs"], units = units,
            inpfilter={"commodity": ["biomass"]},
        )

        vars["Biomass|Solids"] = pp.inp(
            tec = ["biomass_t_d"], units = units,
            inpfilter={"commodity": ["biomass"]},
        )

        # Will only be accounted for at the global level
        vars["Biomass|Trade"] = pp.inp(
            tec = ["biomass_trd"], units = units, inpfilter={"commodity": ["biomass"]}
        ) - pp.out(tec = ["biomass_trd"], units = units, outfilter={"commodity": ["biomass"]})

        _pe_biomass_check = vars["Biomass"] - (
            +vars["Biomass|Electricity|w/o CCS|Biomass Steam Cycle"]
            + vars["Biomass|Electricity|w/o CCS|Biomass IGCC"]
            + vars["Biomass|Electricity|w/o CCS|Biogas Steam Cycle"]
            + vars["Biomass|Electricity|w/o CCS|Biogas Combustion Turbine"]
            + vars["Biomass|Electricity|w/o CCS|Biogas Combined Cycle"]
            + vars["Biomass|Electricity|w/ CCS|Biomass Steam Cycle"]
            + vars["Biomass|Electricity|w/ CCS|Biomass IGCC"]
            + vars["Biomass|Electricity|w/ CCS|Biogas Steam Cycle"]
            + vars["Biomass|Electricity|w/ CCS|Biogas Combined Cycle"]
            + vars["Biomass|Gases"]
            + vars["Biomass|Heat"]
            + vars["Biomass|Hydrogen|w/o CCS"]
            + vars["Biomass|Hydrogen|w/ CCS"]
            + vars["Biomass|Liquids|w/o CCS"]
            + vars["Biomass|Liquids|w/ CCS"]
            + vars["Biomass|Solids"]
            + vars["Biomass|Trade"]
        )

        _pe_biomass_check.to_excel("_pe_biomass_check.xlsx")

        # -------------------------------------------------
        # Additonal reporting for GAINS diagnostic for coal
        # -------------------------------------------------

        vars["Coal|Bunker"] = pp.inp(
            tec = ["meth_bunker"], units = units,
            inpfilter={"commodity": ["methanol"]},
        )

        vars["Coal|Extraction"] = pp.inp(
            tec = ["coal_extr", "lignite_extr"], units = units,
            inpfilter={"commodity": ["coal", "lignite"]},
        ) - pp.out(
            tec = ["coal_extr", "lignite_extr"], units = units, outfilter={"commodity": ["coal"]}
        )

        vars["Coal|Gases"] = pp.inp(
            tec = ["coal_gas"], units = units, inpfilter={"commodity": ["coal"]}
        )

        vars["Coal|Heat"] = pp.inp(tec = "coal_hpl", units = units)

        vars["Coal|Hydrogen|w/o CCS"] = pp.inp(
            tec = ["h2_coal"], units = units, inpfilter={"commodity": ["coal"]}
        )

        vars["Coal|Hydrogen|w/ CCS"] = pp.inp(
            tec = ["h2_coal_ccs"], units = units, inpfilter={"commodity": ["coal"]}
        )

        vars["Coal|Liquids|w/o CCS"] = pp.inp(
            tec = ["syn_liq", "meth_coal"], units = units,
            inpfilter={"commodity": ["coal"]},
        )

        vars["Coal|Liquids|w/ CCS"] = pp.inp(
            tec = ["syn_liq_ccs", "meth_coal_ccs"], units = units,
            inpfilter={"commodity": ["coal"]},
        )

        vars["Coal|Solids"] = pp.inp(
            tec = [
                "coal_t_d",
            ], units = units,
            inpfilter={"commodity": ["coal"]},
        )

        # Will only be accounted for at the global level
        vars["Coal|Trade"] = pp.inp(
            tec = ["coal_trd"], units = units, inpfilter={"commodity": ["coal"]}
        ) - pp.out(tec = ["coal_trd"], units = units, outfilter={"commodity": ["coal"]})

        _pe_coal_check = vars["Coal"] - (
            +vars["Coal|Bunker"]
            + vars["Coal|Electricity|w/o CCS|Hardcoal Subcritical w/o FGD/DeNOx"]
            + vars["Coal|Electricity|w/o CCS|Hardcoal Subcritical w/ FGD/DeNOx"]
            + vars["Coal|Electricity|w/o CCS|Hardcoal Supercritical"]
            + vars["Coal|Electricity|w/o CCS|Hardcoal IGCC"]
            + vars["Coal|Electricity|w/ CCS|Hardcoal Subcritical w/ FGD/DeNOx"]
            + vars["Coal|Electricity|w/ CCS|Hardcoal Supercritical"]
            + vars["Coal|Electricity|w/ CCS|Hardcoal IGCC"]
            + vars["Coal|Extraction"]
            + vars["Coal|Gases"]
            + vars["Coal|Heat"]
            + vars["Coal|Hydrogen|w/o CCS"]
            + vars["Coal|Hydrogen|w/ CCS"]
            + vars["Coal|Liquids|w/o CCS"]
            + vars["Coal|Liquids|w/ CCS"]
            + vars["Coal|Solids"]
            + vars["Coal|Trade"]
        )

        _pe_coal_check.to_excel("_pe_coal_check.xlsx")

        # ------------------------------------------------
        # Additonal reporting for GAINS diagnostic for gas
        # ------------------------------------------------

        vars["Gas|Bunkers"] = pp.inp(
            tec = ["LNG_bunker"], units = units, inpfilter={"commodity": ["LNG"]}
        )

        vars["Gas|Extraction"] = pp.inp(
            tec = [
                "gas_extr_1",
                "gas_extr_2",
                "gas_extr_3",
                "gas_extr_4",
                "gas_extr_5",
                "gas_extr_6",
                "gas_extr_7",
            ],
            units = units,
            inpfilter={
                "commodity": ["gas_1", "gas_2", "gas_3", "gas_4", "gas_5", "gas_6"]
            },
        ) - pp.out(
            tec = [
                "gas_extr_1",
                "gas_extr_2",
                "gas_extr_3",
                "gas_extr_4",
                "gas_extr_5",
                "gas_extr_6",
                "gas_extr_7",
            ],
            units = units,
            outfilter={"commodity": ["gas"]},
        )

        vars["Gas|Gases"] = pp.inp(
            tec = ["gas_t_d", "gas_t_d_ch4"],
            units = units,
            inpfilter={"commodity": ["gas"]},
        ) * (1 - _SynGas_share)

        vars["Gas|Heat"] = pp.inp(tec = "gas_hpl", units = units)

        vars["Gas|Hydrogen|w/o CCS"] = pp.inp(
            tec = ["h2_smr"], units = units, inpfilter={"commodity": ["gas"]}
        )

        vars["Gas|Hydrogen|w/ CCS"] = pp.inp(
            tec = ["h2_smr_ccs"], units = units, inpfilter={"commodity": ["gas"]}
        )

        vars["Gas|Liquids|w/o CCS"] = pp.inp(
            tec = ["meth_ng"], units = units, inpfilter={"commodity": ["gas"]}
        )

        vars["Gas|Liquids|w/ CCS"] = pp.inp(
            tec = ["meth_ng_ccs"], units = units, inpfilter={"commodity": ["gas"]}
        )

        vars["Gas|Solids"] = pp_utils._make_zero()

        vars["Gas|Trade"] = (
            pp.inp(
                trade_tec=["gas_piped_exp"], units=units,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                trade_tec=["gas_piped_exp"], units=units,
            )
            + pp.inp(tec = ["LNG_trd"], units = units, inpfilter={"commodity": ["LNG"]})
            - pp.out(tec = ["LNG_trd"], units = units, outfilter={"commodity": ["LNG"]})
        )

        _pe_gas_check = vars["Gas"] - (
            +vars["Gas|Bunkers"]
            + vars["Gas|Electricity|w/o CCS|Natural Gas Steam Cycle"]
            + vars["Gas|Electricity|w/o CCS|Natural Gas Combustion Turbine"]
            + vars["Gas|Electricity|w/o CCS|Natural Gas Combined Cycle"]
            + vars["Gas|Electricity|w/ CCS|Natural Gas Steam Cycle"]
            + vars["Gas|Electricity|w/ CCS|Natural Gas Combined Cycle"]
            + vars["Gas|Extraction"]
            + vars["Gas|Gases"]
            + vars["Gas|Heat"]
            + vars["Gas|Hydrogen|w/o CCS"]
            + vars["Gas|Hydrogen|w/ CCS"]
            + vars["Gas|Liquids|w/o CCS"]
            + vars["Gas|Liquids|w/ CCS"]
            + vars["Gas|Solids"]
            + vars["Gas|Trade"]
        )
        _pe_gas_check.to_excel("_pe_gas_check.xlsx")

        # ------------------------------------------------
        # Additonal reporting for GAINS diagnostic for oil
        # ------------------------------------------------

        vars["Oil|Bunker"] = pp.inp(
            tec = ["foil_bunker", "loil_bunker"], units = units,
            inpfilter={"commodity": ["lightoil", "fueloil"]},
        )

        # Extraction losses
        vars["Oil|Extraction"] = pp.inp(
            tec = [
                "oil_extr_1",
                "oil_extr_1_ch4",
                "oil_extr_2",
                "oil_extr_2_ch4",
                "oil_extr_3",
                "oil_extr_3_ch4",
                "oil_extr_4",
                "oil_extr_4_ch4",
                "oil_extr_5",
                "oil_extr_6",
                "oil_extr_7",
            ],
            units = units,
            inpfilter={
                "commodity": [
                    "crude_1",
                    "crude_2",
                    "crude_3",
                    "crude_4",
                    "crude_5",
                    "crude_6",
                    "crude_7",
                ]
            },
        ) - pp.out(
            tec = [
                "oil_extr_1",
                "oil_extr_1_ch4",
                "oil_extr_2",
                "oil_extr_2_ch4",
                "oil_extr_3",
                "oil_extr_3_ch4",
                "oil_extr_4",
                "oil_extr_4_ch4",
                "oil_extr_5",
                "oil_extr_6",
                "oil_extr_7",
            ],
            units = units,
            outfilter={"commodity": ["crudeoil"]},
        )

        vars["Oil|Gases"] = pp_utils._make_zero()

        vars["Oil|Heat"] = pp.inp(tec = "foil_hpl", units = units)

        vars["Oil|Hydrogen"] = pp_utils._make_zero()

        vars["Oil|Liquids"] = pp.inp(
            tec = ["loil_t_d", "foil_t_d"], units = units,
            inpfilter={"commodity": ["fueloil", "lightoil"]},
        )

        # Conversion losses
        vars["Oil|Refining"] = pp.inp(
            tec = ["ref_hil", "ref_lol"], units = units, inpfilter={"commodity": ["crudeoil"]}
        ) - pp.out(
            tec = ["ref_hil", "ref_lol"], units = units,
            outfilter={
                "commodity": [
                    "fueloil",
                    "lightoil",
                ]
            },
        )

        vars["Oil|Solids"] = pp_utils._make_zero()

        # Trade losses
        vars["Oil|Trade"] = pp.inp(
            tec = ["loil_trd", "foil_trade", "oil_trade"], units = units,
            inpfilter={"commodity": ["lightoil", "fueloil", "crudeoil"]},
        ) - pp.out(
            tec = ["loil_trd", "foil_trade", "oil_trade"], units = units,
            outfilter={"commodity": ["lightoil", "fueloil", "crudeoil"]},
        )

        _pe_oil_check = vars["Oil|w/o CCS"] - (
            +vars["Oil|Bunker"]
            + vars["Oil|Electricity|w/o CCS|Crude Oil Steam Cycle"]
            + vars["Oil|Electricity|w/o CCS|Heavy Fuel Oil Steam Cycle"]
            + vars["Oil|Electricity|w/o CCS|Light Fuel Oil Combined Cycle"]
            + vars["Oil|Electricity|w/o CCS|Light Fuel Oil Steam Cycle"]
            + vars["Oil|Extraction"]
            + vars["Oil|Gases"]
            + vars["Oil|Heat"]
            + vars["Oil|Liquids"]
            + vars["Oil|Refining"]
            + vars["Oil|Solids"]
            + vars["Oil|Trade"]
        )

        _pe_oil_check.to_excel("_pe_oil_check.xlsx")

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_ppl_capparameters(prmfunc, units):
    """Technology results: Capacity related.

    Parameters
    ----------

    prmfunc : str
        Name of function in `postprocess.py` to be used.
        Options
        - `pp.tic` to retrieve total installed capacity.
        - `pp.nic` to retrieve new installed capacity.
        - `pp.cumcap` to retrieve cumualtive installed capacity.
    units : str
        Units to which variables should be converted.
    """

    cumulative = True if prmfunc == "pp.cumcap" else False

    prmfunc = eval(prmfunc)
    vars = {}

    # -------------------------------
    # Calculation of helper variables
    # -------------------------------

    # Calculate share of activity for later calculating distribution
    # of gas-co2-scrubber capacity.
    # Because a powerplant doesnt necessaarily need to have activity in the years
    # when capacity installed for the scrubber, it is assumed that gas_cc "fills"
    # the gap, hence NANs are replaced with 1
    if run_history != "True":
        group = ["Region", "Vintage"]
    else:
        group = ["Region"]
    _gas_cc_shr = pp.out("gas_cc", group=group) / pp.out(
        tec = ["gas_cc", "gas_ppl"], group=group
    )
    _gas_ppl_shr = pp.out("gas_ppl", group=group) / pp.out(
        tec = ["gas_cc", "gas_ppl"], group=group
    )

    # Before filling NANs, if the values are being calculated for cumulative vraibles,
    # then the forward fill needs to be done prior to filling NANs, or else the "0"
    # cannot be preserved where there really is no activity.
    if cumulative:
        _gas_cc_shr = _gas_cc_shr.ffill(axis=1)
        _gas_ppl_shr = _gas_ppl_shr.ffill(axis=1)

    _gas_cc_shr = _gas_cc_shr.fillna(1)
    _gas_ppl_shr = _gas_ppl_shr.fillna(0)

    # ------------------------------
    # Start of calcualting variables
    # ------------------------------

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Biomass|w/ CCS|1"] = prmfunc("bio_istig_ccs")

    # ["Note": "Steam cycle, scrubber retrofit only"]
    # The scrubber retrofit is reported accounting for the efficiency.
    # The MESSAGEV efficiency was .75; elec-input was .25
    # With the normlazing to output, the electricity input is now .333 (=.25/.75).
    # The scrubber efficiency has been "lost" and hence using the current
    # efficiency .66 (1.-.33), we underestimate the scrubber-capacity.
    # The same applies to other scrubbers below.
    vars["Electricity|Biomass|w/ CCS|2"] = _calc_scrubber_capacity(
        prmfunc, "bio_ppl_co2scr", group, cumulative=cumulative
    )

    # ["Note": "IGCC"]
    vars["Electricity|Biomass|w/o CCS|1"] = prmfunc("bio_istig")

    # ["Note": "Steam cycle"]
    bio_wo_ccs2 = (prmfunc("bio_ppl") - prmfunc("bio_ppl_co2scr")).fillna(0)
    # Ensure that all negative values are 0. This can occur when the
    # scrubber instaallations dont match the yeaar in which powerplant
    # capaacities are installed.
    bio_wo_ccs2[bio_wo_ccs2 < 0] = 0
    vars["Electricity|Biomass|w/o CCS|2"] = bio_wo_ccs2

    # ["Note": "IGCC with CCS"]
    # The scrubber retrofit is reported accounting for the efficiency.
    # The MESSAGEV efficiency was .85; elec-input was .15
    vars["Electricity|Coal|w/ CCS|1"] = (
        prmfunc("igcc_ccs")
        + _calc_scrubber_capacity(prmfunc, "igcc_co2_scr", group, cumulative=cumulative)
    ).fillna(0)

    # ["Note": "Steam cycle super critical with CCS"]
    vars["Electricity|Coal|w/ CCS|2"] = prmfunc("coal_adv_ccs")

    # ["Note": "Steam cycle sub critical Filtered, scrubber retrofit only"]
    # The scrubber retrofit is reported accounting for the efficiency.
    # The MESSAGEV efficiency was .75; elec-input was .25
    vars["Electricity|Coal|w/ CCS|3"] = (
        prmfunc("c_ppl_co2scr")
        + _calc_scrubber_capacity(prmfunc, "c_ppl_co2scr", group, cumulative=cumulative)
    ).fillna(0)

    # ["Note": "IGCC"]
    vars["Electricity|Coal|w/o CCS|1"] = prmfunc("igcc")

    # ["Note": "Steam cycle super critical"]
    vars["Electricity|Coal|w/o CCS|2"] = prmfunc("coal_adv")

    # ["Note": "Steam cycle sub critical Filtered"]
    coal_wo_ccs3 = (prmfunc("coal_ppl") - prmfunc("c_ppl_co2scr")).fillna(0)
    # Ensure that all negative values are 0. This can occur when the
    # scrubber instaallations dont match the yeaar in which powerplant
    # capaacities are installed.
    coal_wo_ccs3[coal_wo_ccs3 < 0] = 0
    vars["Electricity|Coal|w/o CCS|3"] = coal_wo_ccs3

    # ["Note": "Steam cycle sub critical unfiltered"]
    vars["Electricity|Coal|w/o CCS|4"] = prmfunc("coal_ppl_u")

    # ["Note": "Combined cycle with CCS"]
    # The scrubber retrofit is reported accounting for the efficiency.
    # The MESSAGEV efficiency was .87; elec-input was .13
    vars["Electricity|Gas|w/ CCS|1"] = (
        prmfunc("gas_cc_ccs")
        + _calc_scrubber_capacity(
            prmfunc, "g_ppl_co2scr", group, share=_gas_cc_shr, cumulative=cumulative
        )
    ).fillna(0)

    vars["Electricity|Gas|w/ CCS|2"] = _calc_scrubber_capacity(
        prmfunc, "g_ppl_co2scr", group, share=_gas_ppl_shr, cumulative=cumulative
    )

    # ["Note": "Combined cycle"]
    gas_wo_ccs1 = (
        prmfunc("gas_cc")
        - _calc_scrubber_capacity(
            prmfunc,
            "g_ppl_co2scr",
            group,
            share=_gas_cc_shr,
            cumulative=cumulative,
            efficiency=False,
        )
    ).fillna(0)
    # Ensure that all negative values are 0. This can occur when the
    # scrubber instaallations dont match the yeaar in which powerplant
    # capaacities are installed.
    gas_wo_ccs1[gas_wo_ccs1 < 0] = 0
    vars["Electricity|Gas|w/o CCS|1"] = gas_wo_ccs1

    # ["Note": "Steam cycle sub cricitcal"]
    gas_wo_ccs2 = (
        prmfunc("gas_ppl")
        - _calc_scrubber_capacity(
            prmfunc,
            "g_ppl_co2scr",
            group,
            share=_gas_ppl_shr,
            cumulative=cumulative,
            efficiency=False,
        )
    ).fillna(0)
    # Ensure that all negative values are 0. This can occur when the
    # scrubber instaallations dont match the yeaar in which powerplant
    # capaacities are installed.
    gas_wo_ccs2[gas_wo_ccs2 < 0] = 0
    vars["Electricity|Gas|w/o CCS|2"] = gas_wo_ccs2

    # ["Note": "Combustion turbine"]
    vars["Electricity|Gas|w/o CCS|3"] = prmfunc("gas_ct")
    vars["Electricity|Geothermal"] = prmfunc("geo_ppl")

    # ["Note": "Lowcost hydropower"]
    vars["Electricity|Hydro|1"] = prmfunc("hydro_lc")

    # ["Note": "Highcost hydropower"]
    vars["Electricity|Hydro|2"] = prmfunc("hydro_hc")

    # ["Note": "Generation I.-II."]
    vars["Electricity|Nuclear|1"] = prmfunc("nuc_lc")

    # ["Note": "Generation III.-IV."]
    vars["Electricity|Nuclear|2"] = prmfunc("nuc_hc")
    vars["Electricity|Oil|w/ CCS"] = pp_utils._make_zero()

    # ["Note": "Combined cycle light oil"]
    vars["Electricity|Oil|w/o CCS|1"] = prmfunc("loil_cc")

    # ["Note": "Steam cycle light oil"]
    vars["Electricity|Oil|w/o CCS|2"] = prmfunc("loil_ppl")

    # ["Note": "Steam cycle fuel oil"]
    vars["Electricity|Oil|w/o CCS|3"] = prmfunc("foil_ppl")

    vars["Electricity|Solar|CSP|1"] = prmfunc("csp_sm1_ppl")
    vars["Electricity|Solar|CSP|2"] = prmfunc("csp_sm3_ppl")
    vars["Electricity|Solar|PV|Commercial"] = prmfunc(
        [
            "solar_res1",
            "solar_res2",
            "solar_res3",
            "solar_res4",
            "solar_res5",
            "solar_res6",
            "solar_res7",
            "solar_res8",
            "solar_res_hist_2000",
            "solar_res_hist_2005",
            "solar_res_hist_2010",
            "solar_res_hist_2015",
            "solar_res_hist_2020",
            "solar_res_hist_2025",
        ]
    )
    vars["Electricity|Solar|PV|Residential"] = prmfunc(
        [
            "solar_res_rt_hist_2000",
            "solar_res_rt_hist_2005",
            "solar_res_rt_hist_2010",
            "solar_res_rt_hist_2015",
            "solar_res_rt_hist_2020",
            "solar_res_rt_hist_2025",
            "solar_res_RT1",
            "solar_res_RT2",
            "solar_res_RT3",
            "solar_res_RT4",
            "solar_res_RT5",
            "solar_res_RT6",
            "solar_res_RT7",
            "solar_res_RT8",
        ]
    )
    vars["Electricity|Solar|PV"] = (
        vars["Electricity|Solar|PV|Residential"]
        + vars["Electricity|Solar|PV|Commercial"]
    )
    vars["Electricity|Wind|Offshore"] = prmfunc(
        [
            "wind_ref1",
            "wind_ref2",
            "wind_ref3",
            "wind_ref4",
            "wind_ref5",
            "wind_ref_hist_2000",
            "wind_ref_hist_2005",
            "wind_ref_hist_2010",
            "wind_ref_hist_2015",
            "wind_ref_hist_2020",
            "wind_ref_hist_2025",
        ]
    )
    vars["Electricity|Wind|Onshore"] = prmfunc(
        [
            "wind_res1",
            "wind_res2",
            "wind_res3",
            "wind_res4",
            "wind_res_hist_2000",
            "wind_res_hist_2005",
            "wind_res_hist_2010",
            "wind_res_hist_2015",
            "wind_res_hist_2020",
            "wind_res_hist_2025",
        ]
    )
    vars["Electricity|Storage"] = prmfunc("stor_ppl")
    vars["Gases|Biomass|w/o CCS"] = prmfunc("gas_bio")
    vars["Gases|Coal|w/o CCS"] = prmfunc("coal_gas")
    vars["Hydrogen|Biomass|w/ CCS"] = prmfunc("h2_bio_ccs")
    vars["Hydrogen|Biomass|w/o CCS"] = prmfunc("h2_bio")
    vars["Hydrogen|Coal|w/ CCS"] = prmfunc("h2_coal_ccs")
    vars["Hydrogen|Coal|w/o CCS"] = prmfunc("h2_coal")
    vars["Hydrogen|Electricity"] = prmfunc("h2_elec")
    vars["Hydrogen|Gas|w/ CCS"] = prmfunc("h2_smr_ccs")
    vars["Hydrogen|Gas|w/o CCS"] = prmfunc("h2_smr")

    # ["Note": "Ethanol synthesis via biomass gasification with CCS"]
    vars["Liquids|Biomass|w/ CCS|1"] = prmfunc("eth_bio_ccs")

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Biomass|w/ CCS|2"] = prmfunc("liq_bio_ccs")

    # ["Note": "Ethanol synthesis via biomass gasification"]
    vars["Liquids|Biomass|w/o CCS|1"] = prmfunc("eth_bio")

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Biomass|w/o CCS|2"] = prmfunc("liq_bio")

    # ["Note": "Methanol synthesis via coal gasification with CCS"]
    vars["Liquids|Coal|w/ CCS|1"] = prmfunc("meth_coal_ccs")

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Coal|w/ CCS|2"] = prmfunc("syn_liq_ccs")

    # ["Note": "Methanol synthesis via coal gasification"]
    vars["Liquids|Coal|w/o CCS|1"] = prmfunc("meth_coal")

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Coal|w/o CCS|2"] = prmfunc("syn_liq")
    vars["Liquids|Gas|w/ CCS"] = prmfunc("meth_ng_ccs")
    vars["Liquids|Gas|w/o CCS"] = prmfunc("meth_ng")

    # ["Note": "Refinery low yield"]
    vars["Liquids|Oil|w/o CCS|1"] = prmfunc("ref_lol")

    # ["Note": "Refinery high yield"]
    vars["Liquids|Oil|w/o CCS|2"] = prmfunc("ref_hil")

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_ppl_parameters(prmfunc, units):
    """Technology Inputs: Investment costs and technical lifetime.

    Parameters
    ----------

    prmfunc : str
        Name of function in `postprocess.py` to be used.
        Options
        - `pp.inv_cost` to retrieve technology investment costs.
        - `pp.pll` to retrieve technical lifetime of technologies.
    units : str
        Units to which variables should be converted.
    """

    prmfunc = eval(prmfunc)
    vars = {}

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Biomass|w/ CCS|1"] = prmfunc("bio_istig_ccs", units=units)

    # ["Note": "IGCC"]
    vars["Electricity|Biomass|w/o CCS|1"] = prmfunc("bio_istig", units=units)

    # ["Note": "Steam cycle"]
    vars["Electricity|Biomass|w/o CCS|2"] = prmfunc("bio_ppl", units=units)

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Coal|w/ CCS|1"] = prmfunc("igcc_ccs", units=units)

    # ["Note": "Steam cycle super critical with CCS"]
    vars["Electricity|Coal|w/ CCS|2"] = prmfunc("coal_adv_ccs", units=units)

    # ["Note": "IGCC"]
    vars["Electricity|Coal|w/o CCS|1"] = prmfunc("igcc", units=units)

    # ["Note": "Steam cycle super critical"]
    vars["Electricity|Coal|w/o CCS|2"] = prmfunc("coal_adv", units=units)

    # ["Note": "Steam cycle sub critical Filtered"]
    vars["Electricity|Coal|w/o CCS|3"] = prmfunc("coal_ppl", units=units)

    # ["Note": "Steam cycle sub critical unfiltered"]
    vars["Electricity|Coal|w/o CCS|4"] = prmfunc("coal_ppl_u", units=units)

    # ["Note": "Combined cycle with CCS"]
    vars["Electricity|Gas|w/ CCS|1"] = prmfunc("gas_cc_ccs", units=units)

    # ["Note": "Combined cycle"]
    vars["Electricity|Gas|w/o CCS|1"] = prmfunc("gas_cc", units=units)

    # ["Note": "Steam cycle sub cricitcal"]
    vars["Electricity|Gas|w/o CCS|2"] = prmfunc("gas_ppl", units=units)

    # ["Note": "Combustion turbine"]
    vars["Electricity|Gas|w/o CCS|3"] = prmfunc("gas_ct", units=units)
    vars["Electricity|Geothermal"] = prmfunc("geo_ppl", units=units)

    # ["Note": "Lowcost hydropower"]
    vars["Electricity|Hydro|1"] = prmfunc("hydro_lc", units=units)

    # ["Note": "Highcost hydropower"]
    vars["Electricity|Hydro"] = vars["Electricity|Hydro|2"] = prmfunc(
        "hydro_hc", units=units
    )

    # ["Note": "Generation I.-II."]
    vars["Electricity|Nuclear|1"] = prmfunc("nuc_lc", units=units)

    # ["Note": "Generation III.-IV."]
    vars["Electricity|Nuclear"] = vars["Electricity|Nuclear|2"] = prmfunc(
        "nuc_hc", units=units
    )

    # ["Note": "Combined cycle light oil"]
    vars["Electricity|Oil|w/o CCS"] = vars["Electricity|Oil|w/o CCS|1"] = prmfunc(
        "loil_cc", units=units
    )

    # ["Note": "Steam cycle light oil"]
    vars["Electricity|Oil|w/o CCS|2"] = prmfunc("loil_ppl", units=units)

    # ["Note": "Steam cycle fuel oil"]
    vars["Electricity|Oil|w/o CCS|3"] = prmfunc("foil_ppl", units=units)
    vars["Electricity|Solar|CSP|1"] = prmfunc("csp_sm1_ppl", units=units)
    vars["Electricity|Solar|CSP|2"] = prmfunc("csp_sm3_ppl", units=units)
    vars["Electricity|Solar|PV|Utility"] = prmfunc("solar_res1", units=units)
    vars["Electricity|Solar|PV|Residential"] = prmfunc("solar_res_RT1", units=units)
    vars["Electricity|Wind|Offshore"] = prmfunc("wind_ref1", units=units)
    vars["Electricity|Wind|Onshore"] = prmfunc("wind_res1", units=units)
    vars["Electricity|Storage"] = prmfunc("stor_ppl", units=units)
    vars["Gases|Biomass|w/o CCS"] = prmfunc("gas_bio", units=units)
    vars["Gases|Coal|w/o CCS"] = prmfunc("coal_gas", units=units)
    vars["Hydrogen|Biomass|w/ CCS"] = prmfunc("h2_bio_ccs", units=units)
    vars["Hydrogen|Biomass|w/o CCS"] = prmfunc("h2_bio", units=units)
    vars["Hydrogen|Coal|w/ CCS"] = prmfunc("h2_coal_ccs", units=units)
    vars["Hydrogen|Coal|w/o CCS"] = prmfunc("h2_coal", units=units)
    vars["Hydrogen|Electricity"] = prmfunc("h2_elec", units=units)
    vars["Hydrogen|Gas|w/ CCS"] = prmfunc("h2_smr_ccs", units=units)
    vars["Hydrogen|Gas|w/o CCS"] = prmfunc("h2_smr", units=units)

    # ["Note": "Ethanol synthesis via biomass gasification with CCS"]
    vars["Liquids|Biomass|w/ CCS|1"] = prmfunc("eth_bio_ccs", units=units)

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Biomass|w/ CCS"] = vars["Liquids|Biomass|w/ CCS|2"] = prmfunc(
        "liq_bio_ccs", units=units
    )

    # ["Note": "Ethanol synthesis via biomass gasification"]
    vars["Liquids|Biomass|w/o CCS|1"] = prmfunc("eth_bio", units=units)

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Biomass|w/o CCS"] = vars["Liquids|Biomass|w/o CCS|2"] = prmfunc(
        "liq_bio", units=units
    )

    # ["Note": "Methanol synthesis via coal gasification with CCS"]
    vars["Liquids|Coal|w/ CCS|1"] = prmfunc("meth_coal_ccs", units=units)

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Coal|w/ CCS"] = vars["Liquids|Coal|w/ CCS|2"] = prmfunc(
        "syn_liq_ccs", units=units
    )

    # ["Note": "Methanol synthesis via coal gasification"]
    vars["Liquids|Coal|w/o CCS|1"] = prmfunc("meth_coal", units=units)

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Coal|w/o CCS"] = vars["Liquids|Coal|w/o CCS|2"] = prmfunc(
        "syn_liq", units=units
    )

    vars["Liquids|Gas|w/ CCS"] = prmfunc("meth_ng_ccs", units=units)
    vars["Liquids|Gas|w/o CCS"] = prmfunc("meth_ng", units=units)

    # ["Note": "Refinery low yield"]
    vars["Liquids|Oil|w/o CCS|1"] = prmfunc("ref_lol", units=units)

    # ["Note": "Refinery high yield"]
    vars["Liquids|Oil|w/o CCS"] = vars["Liquids|Oil|w/o CCS|2"] = prmfunc(
        "ref_hil", units=units
    )

    df = pp_utils.make_outputdf(vars, units, glb=False)
    return df


@_register
def retr_ppl_opcost_parameters(prmfunc, units):
    """Technology Inputs: Fixed and variable costs.

    Parameters
    ----------

    prmfunc : str
        Name of function in `postprocess.py` to be used.
        Options
        - `pp.fom` to retrieve technology fixed operating and maintenance costs.
        - `pp.vom` to retrieve technology variable operating and maintenance costs.
    units : str
        Units to which variables should be converted.
    """

    prmfunc = eval(prmfunc)

    vars = {}

    group = ["Region"]
    formatting = "reporting"

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Biomass|w/ CCS|1"] = prmfunc(
        "bio_istig_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "IGCC"]
    vars["Electricity|Biomass|w/o CCS|1"] = prmfunc(
        "bio_istig", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle"]
    vars["Electricity|Biomass|w/o CCS|2"] = prmfunc(
        "bio_ppl", units=units, group=group, formatting=formatting
    )

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Coal|w/ CCS|1"] = prmfunc(
        "igcc_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle super critical with CCS"]
    vars["Electricity|Coal|w/ CCS|2"] = prmfunc(
        "coal_adv_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "IGCC"]
    vars["Electricity|Coal|w/o CCS|1"] = prmfunc(
        "igcc", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle super critical"]
    vars["Electricity|Coal|w/o CCS|2"] = prmfunc(
        "coal_adv", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle sub critical Filtered"]
    vars["Electricity|Coal|w/o CCS|3"] = prmfunc(
        "coal_ppl", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle sub critical unfiltered"]
    vars["Electricity|Coal|w/o CCS|4"] = prmfunc(
        "coal_ppl_u", units=units, group=group, formatting=formatting
    )

    # ["Note": "Combined cycle with CCS"]
    vars["Electricity|Gas|w/ CCS|1"] = prmfunc(
        "gas_cc_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "Combined cycle"]
    vars["Electricity|Gas|w/o CCS|1"] = prmfunc(
        "gas_cc", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle sub cricitcal"]
    vars["Electricity|Gas|w/o CCS|2"] = prmfunc(
        "gas_ppl", units=units, group=group, formatting=formatting
    )

    # ["Note": "Combustion turbine"]
    vars["Electricity|Gas|w/o CCS|3"] = prmfunc(
        "gas_ct", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Geothermal"] = prmfunc(
        "geo_ppl", units=units, group=group, formatting=formatting
    )

    # ["Note": "Lowcost hydropower"]
    vars["Electricity|Hydro|1"] = prmfunc(
        "hydro_lc", units=units, group=group, formatting=formatting
    )

    # ["Note": "Highcost hydropower"]
    vars["Electricity|Hydro"] = vars["Electricity|Hydro|2"] = prmfunc(
        "hydro_hc", units=units, group=group, formatting=formatting
    )

    # ["Note": "Generation I.-II."]
    vars["Electricity|Nuclear|1"] = prmfunc(
        "nuc_lc", units=units, group=group, formatting=formatting
    )

    # ["Note": "Generation III.-IV."]
    vars["Electricity|Nuclear"] = vars["Electricity|Nuclear|2"] = prmfunc(
        "nuc_hc", units=units, group=group, formatting=formatting
    )

    # ["Note": "Combined cycle light oil"]
    vars["Electricity|Oil|w/o CCS"] = vars["Electricity|Oil|w/o CCS|1"] = prmfunc(
        "loil_cc", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle light oil"]
    vars["Electricity|Oil|w/o CCS|2"] = prmfunc(
        "loil_ppl", units=units, group=group, formatting=formatting
    )

    # ["Note": "Steam cycle fuel oil"]
    vars["Electricity|Oil|w/o CCS|3"] = prmfunc(
        "foil_ppl", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Solar|CSP|1"] = prmfunc(
        "csp_sm1_ppl", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Solar|CSP|2"] = prmfunc(
        "csp_sm3_ppl", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Solar|PV|Utility"] = prmfunc(
        "solar_res1", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Solar|PV|Residential"] = prmfunc(
        "solar_res_RT1", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Wind|Offshore"] = prmfunc(
        "wind_ref1", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Wind|Onshore"] = prmfunc(
        "wind_res1", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Storage"] = prmfunc(
        "stor_ppl", units=units, group=group, formatting=formatting
    )

    vars["Gases|Biomass|w/o CCS"] = prmfunc(
        "gas_bio", units=units, group=group, formatting=formatting
    )

    vars["Gases|Coal|w/o CCS"] = prmfunc(
        "coal_gas", units=units, group=group, formatting=formatting
    )

    vars["Hydrogen|Biomass|w/ CCS"] = prmfunc(
        "h2_bio_ccs", units=units, group=group, formatting=formatting
    )

    vars["Hydrogen|Biomass|w/o CCS"] = prmfunc(
        "h2_bio", units=units, group=group, formatting=formatting
    )

    vars["Hydrogen|Coal|w/ CCS"] = prmfunc(
        "h2_coal_ccs", units=units, group=group, formatting=formatting
    )

    vars["Hydrogen|Coal|w/o CCS"] = prmfunc(
        "h2_coal", units=units, group=group, formatting=formatting
    )

    vars["Hydrogen|Electricity"] = prmfunc(
        "h2_elec", units=units, group=group, formatting=formatting
    )

    vars["Hydrogen|Gas|w/ CCS"] = prmfunc(
        "h2_smr_ccs", units=units, group=group, formatting=formatting
    )

    vars["Hydrogen|Gas|w/o CCS"] = prmfunc(
        "h2_smr", units=units, group=group, formatting=formatting
    )

    # ["Note": "Ethanol synthesis via biomass gasification with CCS"]
    vars["Liquids|Biomass|w/ CCS|1"] = prmfunc(
        "eth_bio_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Biomass|w/ CCS"] = vars["Liquids|Biomass|w/ CCS|2"] = prmfunc(
        "liq_bio_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "Ethanol synthesis via biomass gasification"]
    vars["Liquids|Biomass|w/o CCS|1"] = prmfunc(
        "eth_bio", units=units, group=group, formatting=formatting
    )

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Biomass|w/o CCS"] = vars["Liquids|Biomass|w/o CCS|2"] = prmfunc(
        "liq_bio", units=units, group=group, formatting=formatting
    )

    # ["Note": "Methanol synthesis via coal gasification with CCS"]
    vars["Liquids|Coal|w/ CCS|1"] = prmfunc(
        "meth_coal_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Coal|w/ CCS"] = vars["Liquids|Coal|w/ CCS|2"] = prmfunc(
        "syn_liq_ccs", units=units, group=group, formatting=formatting
    )

    # ["Note": "Methanol synthesis via coal gasification"]
    vars["Liquids|Coal|w/o CCS|1"] = prmfunc(
        "meth_coal", units=units, group=group, formatting=formatting
    )

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Coal|w/o CCS"] = vars["Liquids|Coal|w/o CCS|2"] = prmfunc(
        "syn_liq", units=units, group=group, formatting=formatting
    )

    vars["Liquids|Gas|w/ CCS"] = prmfunc(
        "meth_ng_ccs", units=units, group=group, formatting=formatting
    )

    vars["Liquids|Gas|w/o CCS"] = prmfunc(
        "meth_ng", units=units, group=group, formatting=formatting
    )

    # ["Note": "Refinery low yield"]
    vars["Liquids|Oil|w/o CCS|1"] = prmfunc(
        "ref_lol", units=units, group=group, formatting=formatting
    )

    # ["Note": "Refinery high yield"]
    vars["Liquids|Oil|w/o CCS"] = vars["Liquids|Oil|w/o CCS|2"] = prmfunc(
        "ref_hil", units=units, group=group, formatting=formatting
    )

    df = pp_utils.make_outputdf(vars, units, glb=False)
    return df


@_register
def retr_eff_parameters(units):
    """Technology Inputs: Efficiency.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    # ----------------------
    # Electricity generation
    # ----------------------

    outfilter = {"level": ["secondary"], "commodity": ["electr"]}

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Biomass|w/ CCS|1"] = pp.eff(
        "bio_istig_ccs",
        inpfilter={"commodity": ["biomass"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "IGCC"]
    vars["Electricity|Biomass|w/o CCS|1"] = pp.eff(
        "bio_istig",
        inpfilter={"commodity": ["biomass"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle"]
    vars["Electricity|Biomass|w/o CCS|2"] = pp.eff(
        "bio_ppl",
        inpfilter={"commodity": ["biomass"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Coal|w/ CCS|1"] = pp.eff(
        "igcc_ccs",
        inpfilter={"commodity": ["coal"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle super critical with CCS"]
    vars["Electricity|Coal|w/ CCS|2"] = pp.eff(
        "coal_adv_ccs",
        inpfilter={"commodity": ["coal"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "IGCC"]
    vars["Electricity|Coal|w/o CCS|1"] = pp.eff(
        "igcc",
        inpfilter={"commodity": ["coal"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle super critical"]
    vars["Electricity|Coal|w/o CCS|2"] = pp.eff(
        "coal_adv",
        inpfilter={"commodity": ["coal"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle sub critical Filtered"]
    vars["Electricity|Coal|w/o CCS|3"] = pp.eff(
        "coal_ppl",
        inpfilter={"commodity": ["coal"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle sub critical unfiltered"]
    vars["Electricity|Coal|w/o CCS|4"] = pp.eff(
        "coal_ppl_u",
        inpfilter={"commodity": ["coal"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Combined cycle with CCS"]
    vars["Electricity|Gas|w/ CCS|1"] = pp.eff(
        "gas_cc_ccs",
        inpfilter={"commodity": ["gas"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Combined cycle"]
    vars["Electricity|Gas|w/o CCS|1"] = pp.eff(
        "gas_cc",
        inpfilter={"commodity": ["gas"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle sub cricitcal"]
    vars["Electricity|Gas|w/o CCS|2"] = pp.eff(
        "gas_ppl",
        inpfilter={"commodity": ["gas"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Combustion turbine"]
    vars["Electricity|Gas|w/o CCS|3"] = pp.eff(
        "gas_ct",
        inpfilter={"commodity": ["gas"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Electricity|Geothermal"] = pp_utils._make_zero()
    vars["Electricity|Hydro|1"] = pp_utils._make_zero()
    vars["Electricity|Hydro"] = vars["Electricity|Hydro|2"] = pp_utils._make_zero()
    vars["Electricity|Nuclear|1"] = pp_utils._make_zero()
    vars["Electricity|Nuclear"] = vars["Electricity|Nuclear|2"] = pp_utils._make_zero()

    # ["Note": "Combined cycle light oil"]
    vars["Electricity|Oil|w/o CCS"] = vars["Electricity|Oil|w/o CCS|1"] = pp.eff(
        "loil_cc",
        inpfilter={"commodity": ["lightoil"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle light oil"]
    vars["Electricity|Oil|w/o CCS|2"] = pp.eff(
        "loil_ppl",
        inpfilter={"commodity": ["lightoil"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Steam cycle fuel oil"]
    vars["Electricity|Oil|w/o CCS|3"] = pp.eff(
        "foil_ppl",
        inpfilter={"commodity": ["fueloil"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Electricity|Solar|CSP|1"] = pp_utils._make_zero()
    vars["Electricity|Solar|CSP|2"] = pp_utils._make_zero()
    vars["Electricity|Solar|PV"] = pp_utils._make_zero()
    vars["Electricity|Wind|Offshore"] = pp_utils._make_zero()
    vars["Electricity|Wind|Onshore"] = pp_utils._make_zero()

    # --------------
    # Gas production
    # --------------

    outfilter = {"level": ["secondary"], "commodity": ["gas"]}

    vars["Gases|Biomass|w/o CCS"] = pp.eff(
        "gas_bio",
        inpfilter={"commodity": ["biomass", "electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Gases|Coal|w/o CCS"] = pp.eff(
        "coal_gas",
        inpfilter={"commodity": ["coal"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # -------------------
    # Hydrogen production
    # -------------------

    outfilter = {"level": ["secondary"], "commodity": ["hydrogen"]}

    vars["Hydrogen|Biomass|w/ CCS"] = pp.eff(
        "h2_bio_ccs",
        inpfilter={"commodity": ["biomass", "electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Hydrogen|Biomass|w/o CCS"] = pp.eff(
        "h2_bio",
        inpfilter={"commodity": ["biomass", "electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Hydrogen|Coal|w/ CCS"] = pp.eff(
        "h2_coal_ccs",
        inpfilter={"commodity": ["coal", "electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Hydrogen|Coal|w/o CCS"] = pp.eff(
        "h2_coal",
        inpfilter={"commodity": ["coal", "electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Hydrogen|Electricity"] = pp.eff(
        "h2_elec",
        inpfilter={"commodity": ["electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Hydrogen|Gas|w/ CCS"] = pp.eff(
        "h2_smr_ccs",
        inpfilter={"commodity": ["gas", "electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    vars["Hydrogen|Gas|w/o CCS"] = pp.eff(
        "h2_smr",
        inpfilter={"commodity": ["gas", "electr"]},
        outfilter=outfilter,
        formatting="reporting",
    )

    # ----------------------
    # Bio-ethanol production
    # ----------------------

    outfilter = {"level": ["primary"], "commodity": ["ethanol"]}
    inpfilter = {"commodity": ["biomass", "electr"]}

    # ["Note": "Ethanol synthesis via biomass gasification with CCS"]
    vars["Liquids|Biomass|w/ CCS|1"] = pp.eff(
        "eth_bio_ccs", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Biomass|w/ CCS"] = vars["Liquids|Biomass|w/ CCS|2"] = pp.eff(
        "liq_bio_ccs", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    # ["Note": "Ethanol synthesis via biomass gasification"]
    vars["Liquids|Biomass|w/o CCS|1"] = pp.eff(
        "eth_bio", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Biomass|w/o CCS"] = vars["Liquids|Biomass|w/o CCS|2"] = pp.eff(
        "liq_bio", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    # ------------------
    # Synfuel production
    # ------------------

    inpfilter = {"level": ["secondary"], "commodity": ["coal"]}
    outfilter = {"level": ["secondary"], "commodity": ["lightoil", "electr"]}

    # ["Note": "FTL (Fischer Tropsch Liquids) with CCS"]
    vars["Liquids|Coal|w/ CCS"] = vars["Liquids|Coal|w/ CCS|2"] = pp.eff(
        "syn_liq_ccs", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    # ["Note": "FTL (Fischer Tropsch Liquids)"]
    vars["Liquids|Coal|w/o CCS"] = vars["Liquids|Coal|w/o CCS|2"] = pp.eff(
        "syn_liq", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    outfilter = {"level": ["primary"], "commodity": ["methanol"]}

    # ["Note": "Methanol synthesis via coal gasification with CCS"]
    vars["Liquids|Coal|w/ CCS|1"] = pp.eff(
        "meth_coal_ccs",
        inpfilter=inpfilter,
        outfilter=outfilter,
        formatting="reporting",
    )

    # ["Note": "Methanol synthesis via coal gasification"]
    vars["Liquids|Coal|w/o CCS|1"] = pp.eff(
        "meth_coal", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    inpfilter = {"level": ["secondary"], "commodity": ["gas"]}

    vars["Liquids|Gas|w/ CCS"] = pp.eff(
        "meth_ng_ccs", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    vars["Liquids|Gas|w/o CCS"] = pp.eff(
        "meth_ng", inpfilter=inpfilter, outfilter=outfilter, formatting="reporting"
    )

    # ["Note": "Refinery low yield"]
    vars["Liquids|Oil|w/o CCS|1"] = pp.eff(
        "ref_lol", inpfilter={"commodity": ["crudeoil"]}, formatting="reporting"
    )

    # ["Note": "Refinery high yield"]
    vars["Liquids|Oil|w/o CCS"] = vars["Liquids|Oil|w/o CCS|2"] = pp.eff(
        "ref_hil", inpfilter={"commodity": ["crudeoil"]}, formatting="reporting"
    )

    # As the unit is "%", values retrieved from the database must be mutplied by 100.
    # For all years where the technologies are not included in the model, the
    # efficiency is set to 0 (this includes global numbers)
    tmp = {}
    for v in vars.keys():
        tmp[v] = round(vars[v] * 100, 2)
    vars = tmp

    df = pp_utils.make_outputdf(vars, units, glb=False)
    return df


@_register
def retr_fe(units):
    """Energy: Final Energy.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    # ----------------------------------------------------
    # Calculate share of hydrogen in FE
    # based on method applied for allocating CO2 emissions
    # ----------------------------------------------------

    if run_history != "True":
        group = ["Region", "Mode", "Vintage"]
    else:
        group = ["Region"]

    _inp_nonccs_gas_tecs = (
        pp.inp(
            tec = [
                "gas_rc",
                "hp_gas_rc",
                "gas_i",
                "hp_gas_i",
                "gas_trp",
                # "gas_fs",
                "gas_ppl",
                "gas_ct",
                "gas_cc",
                "gas_hpl",
            ],
            units = units,
            inpfilter={"commodity": ["gas"]},
        )
        + pp.inp(tec = ["gas_t_d", "gas_t_d_ch4"], units = units, inpfilter={"commodity": ["gas"]})
        - pp.out(tec = ["gas_t_d", "gas_t_d_ch4"], units = units, outfilter={"commodity": ["gas"]})
    )

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out(tec = "gas_cc") / pp.out(tec = ["gas_cc", "gas_ppl"])).fillna(0)

    _gas_ppl_shr = (pp.out(tec = "gas_ppl") / pp.out(tec = ["gas_cc", "gas_ppl"])).fillna(0)

    _inp_nonccs_gas_tecs_wo_CCSRETRO = (
        _inp_nonccs_gas_tecs
        - _pe_wCCSretro(
            "gas_cc",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units,
            share=_gas_cc_shr,
        )
        - _pe_wCCSretro(
            "gas_ppl",
            "g_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units,
            share=_gas_ppl_shr,
        )
    )

    _Hydrogen_tot = pp.out(
        tec = "h2_mix", units = units,
    )

    _Hydrogen_ind = _Hydrogen_tot * (
        pp.inp(tec = ["gas_i", "hp_gas_i"], units = units, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    #    _Hydrogen_fs = _Hydrogen_tot * (
    #        pp.inp("gas_fs", units, inpfilter={"commodity": ["gas"]})
    #        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    #    ).fillna(0)

    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(tec = ["gas_rc", "hp_gas_rc"], units = units, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_trp = _Hydrogen_tot * (
        pp.inp(tec = "gas_trp", units = units, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # ---------------
    # Industry sector
    # ---------------

    BiomassIND = pp.inp(tec = "biomass_i", units = units)
    OilIND = pp.inp(tec = ["foil_i", "sp_liq_I", "loil_i"], units = units)

    MethIND = pp.inp(tec = ["sp_meth_I", "meth_i"], units = units)
    EthIND = pp.inp(tec = ["eth_i", "sp_eth_I"], units = units)

    GasIND = (
        pp.inp(
            tec = ["gas_i", "hp_gas_i"], units = units,
            inpfilter={"level": ["final"], "commodity": ["gas"]},
        )
        - _Hydrogen_ind
    )

    CoalIND = pp.inp(tec = ["coal_i"], units = units)

    # Comment OFR: In next variable - Added electricity requirements for gas
    # heat-pumps
    ElecIND = pp.inp(tec = ["sp_el_I", "elec_i", "hp_el_i"], units = units) + pp.inp(
        tec = "hp_gas_i", units = units, inpfilter={"level": ["final"], "commodity": ["electr"]}
    )

    OnsitePVIND = pp.out(tec = ["solar_pv_I"], units = units) + pp.inp(tec = ["sp_el_I_RT"], units = units)
    DheatIND = pp.inp(tec = "heat_i", units = units)
    SolThermIND = pp.out(tec = "solar_i", units = units)
    H2IND = pp.inp(tec = ["h2_i", "h2_fc_I"], units = units) + _Hydrogen_ind

    vars["Industry|Electricity"] = ElecIND + OnsitePVIND
    vars["Industry|Electricity|Solar"] = OnsitePVIND
    vars["Industry|Gases"] = GasIND
    vars["Industry|Heat"] = DheatIND
    vars["Industry|Hydrogen"] = H2IND
    vars["Industry|Liquids|Biomass"] = EthIND
    vars["Industry|Liquids|Coal"] = MethIND
    vars["Industry|Liquids|Gas"] = pp_utils._make_zero()
    vars["Industry|Liquids|Oil"] = OilIND
    vars["Industry|Other"] = SolThermIND
    vars["Industry|Solids|Coal"] = CoalIND
    vars["Industry|Solids|Biomass"] = BiomassIND

    # ---------------------------------
    # Residential and commercial sector
    # ---------------------------------

    BiomassRC = pp.inp(tec = "biomass_rc", units = units)
    OilRC = pp.inp(tec = ["foil_rc", "loil_rc"], units = units)
    MethRC = pp.inp(tec = "meth_rc", units = units)
    EthRC = pp.inp(tec = "eth_rc", units = units)

    GasRC = (
        pp.inp(
            tec = ["gas_rc", "hp_gas_rc"], units = units,
            inpfilter={"level": ["final"], "commodity": ["gas"]},
        )
        - _Hydrogen_res
    )

    CoalRC = pp.inp(tec = "coal_rc", units = units)
    ElecRC = pp.inp(tec = ["sp_el_RC"], units = units)

    # Comment OFR: In next variable - Added electricity requirements for gas
    # heat-pumps
    ElecHpRC = pp.inp(tec = "hp_el_rc", units = units) + pp.inp(
        tec = "hp_gas_rc", units = units, inpfilter={"level": ["final"], "commodity": ["electr"]}
    )

    ElecDirRC = pp.inp(tec = ["elec_rc"], units = units)

    ElecFcTRP = -1.0 * pp.out(
        tec = "h2_fc_trp", units = units, outfilter={"level": ["final"], "commodity": ["electr"]}
    )

    OnsitePVRC = pp.out(tec = ["solar_pv_RC"], units = units) + pp.inp(tec = ["sp_el_RC_RT"], units = units)
    DheatRC = pp.inp(tec = "heat_rc", units = units)
    SolThermRC = pp.out(tec = "solar_rc", units = units)

    H2RC = (
        pp.inp(
            tec = ["h2_rc", "h2_fc_RC", "h2_fc_trp"], units = units,
            inpfilter={"level": ["final"], "commodity": ["hydrogen"]},
        )
        + _Hydrogen_res
    )

    BiomassNC = pp.inp(tec = "biomass_nc", units = units)

    # Comment OFR: In next variable - Includes other electricity consumption
    # as well (ElecHpRC + ElecDirRC + ElecFcTRP)
    vars["Residential and Commercial|Electricity"] = (
        ElecRC + OnsitePVRC + ElecHpRC + ElecDirRC + ElecFcTRP
    )

    vars["Residential and Commercial|Electricity|Solar"] = OnsitePVRC
    vars["Residential and Commercial|Gases"] = GasRC
    vars["Residential and Commercial|Heat"] = DheatRC
    vars["Residential and Commercial|Hydrogen"] = H2RC
    vars["Residential and Commercial|Other"] = SolThermRC
    vars["Residential and Commercial|Liquids|Biomass"] = EthRC
    vars["Residential and Commercial|Liquids|Coal"] = MethRC
    vars["Residential and Commercial|Liquids|Gas"] = pp_utils._make_zero()
    vars["Residential and Commercial|Liquids|Oil"] = OilRC
    vars["Residential and Commercial|Solids|Coal"] = CoalRC
    vars["Residential and Commercial|Solids|Biomass"] = BiomassRC + BiomassNC
    vars["Residential and Commercial|Solids|Biomass|Traditional"] = BiomassNC

    # --------------
    # Transportation
    # --------------

    OilTRP = pp.inp(tec = ["foil_trp", "loil_trp"], units = units)
    OilTRP_shipping_foil = pp.inp(tec = ["foil_bunker"], units = units)
    OilTRP_shipping_loil = pp.inp(tec = ["loil_bunker"], units = units)
    MethTRP = pp.inp(tec = ["meth_ic_trp", "meth_fc_trp"], units = units)
    MethTRP_shipping = pp.inp(tec = ["meth_bunker"], units = units)
    EthTRP = pp.inp(tec = ["eth_ic_trp", "eth_fc_trp"], units = units)
    EthTRP_shipping = pp.inp(tec = ["eth_bunker"], units = units)
    GasTRP = pp.inp(tec = ["gas_trp"], units = units) - _Hydrogen_trp
    GasTRP_shipping = pp.inp(tec = ["LNG_bunker"], units = units)
    CoalTRP = pp.inp(tec = "coal_trp", units = units)
    ElecTRP = pp.inp(tec = "elec_trp", units = units)

    H2TRP = (
        pp.inp(tec = "h2_fc_trp", units = units, inpfilter={"level": ["final"], "commodity": ["lh2"]})
        + _Hydrogen_trp
    )

    H2TRP_shipping = pp.inp(tec = "LH2_bunker", units = units)

    # Transportation

    vars["Transportation|Electricity"] = ElecTRP
    vars["Transportation|Gases"] = GasTRP
    vars["Transportation|Hydrogen"] = H2TRP
    vars["Transportation|Liquids|Biomass"] = EthTRP
    vars["Transportation|Liquids|Coal"] = MethTRP
    vars["Transportation|Liquids|Oil"] = OilTRP
    vars["Transportation|Liquids|Gas"] = pp_utils._make_zero()
    vars["Transportation|Other"] = CoalTRP

    # Transportation (w/ bunkers)

    vars["Transportation (w/ bunkers)|Electricity"] = ElecTRP
    vars["Transportation (w/ bunkers)|Gases"] = GasTRP + GasTRP_shipping
    vars["Transportation (w/ bunkers)|Hydrogen"] = H2TRP + H2TRP_shipping
    vars["Transportation (w/ bunkers)|Liquids|Biomass"] = EthTRP + EthTRP_shipping
    vars["Transportation (w/ bunkers)|Liquids|Coal"] = MethTRP + MethTRP_shipping
    vars["Transportation (w/ bunkers)|Liquids|Oil"] = (
        OilTRP + OilTRP_shipping_foil + OilTRP_shipping_loil
    )
    vars["Transportation (w/ bunkers)|Liquids|Gas"] = pp_utils._make_zero()
    vars["Transportation (w/ bunkers)|Other"] = CoalTRP

    # Bunkers

    vars["Bunkers|Electricity"] = pp_utils._make_zero()
    vars["Bunkers|Gases"] = GasTRP_shipping
    vars["Bunkers|Hydrogen"] = H2TRP_shipping
    vars["Bunkers|Liquids|Biomass"] = EthTRP_shipping
    vars["Bunkers|Liquids|Coal"] = MethTRP_shipping

    vars["Bunkers|Liquids|Oil"] = OilTRP_shipping_foil + OilTRP_shipping_loil
    vars["Bunkers|Liquids|Gas"] = pp_utils._make_zero()

    vars["Bunkers|Other"] = pp_utils._make_zero()

    # ---------------------------
    # Non-Energy Use (feedstocks)
    # ---------------------------

    vars["Non-Energy Use|Liquids|Biomass"] = pp.inp(
        tec = ["ethanol_fs"], units = units, inpfilter={"commodity": ["ethanol"]}
    )

    # Note OFR: this category includes both coal from a solid and liquid
    # (methanol) source
    vars["Non-Energy Use|Solids|Coal"] = pp.inp(
        tec = ["coal_fs"], units = units, inpfilter={"commodity": ["coal"]}
    )
    vars["Non-Energy Use|Liquids|Coal"] = pp.inp(
        tec = ["methanol_fs"], units = units, inpfilter={"commodity": ["methanol"]}
    )

    # Note OFR: this can include biogas, natural gas and hydrogen which is mixed
    # further upstream
    vars["Non-Energy Use|Gases"] = pp.inp(
        tec = ["gas_fs"], units = units, inpfilter={"commodity": ["gas"]}
    )

    vars["Non-Energy Use|Liquids|Oil"] = pp.inp(
        tec = ["foil_fs", "loil_fs"], units = units, inpfilter={"commodity": ["fueloil", "lightoil"]}
    )

    vars["Geothermal"] = pp_utils._make_zero()

    # Note OFR: Currently, the split required to run GAINS between
    #           Commercial and Residential is not supported.
    # Note OFR: Currently, the split required to run GAINS to differentiate
    #           between fraight and Passenger Transport is not supported.

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_trade(
    units_energy,
    units_CPrc_co2,
    units_emi_val,
    units_emi_vol,
    units_agri_vol,
    units_for_vol,
):
    """Energy: Trade.

    Energy and emission trade between regions.

    Parameters
    ----------
    units_energy : str
        Units to which energy variables should be converted.
    units_CPrc_co2 : str
        Units to which carbon price (in CO2 equivalent) should be converted.
    units_emi_val : str
        Units to which value based emission variables should be converted.
    units_emi_vol : str
        Units to which volume based emission variables should be converted.
    """

    dfs = []
    vars = {}

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    # Retrieve regional export quantities
    _FOil_exp_reg = pp.inp(trade_tec=["foil_shipped_exp", "foil_piped_exp"], units=units_energy, inpfilter={"commodity": ["fueloil"]})
    _LOil_exp_reg = pp.inp(trade_tec=["loil_shipped_exp", "loil_piped_exp"], units=units_energy, inpfilter={"commodity": ["lightoil"]})
    _BTL_exp_reg = pp.inp(trade_tec=["eth_shipped_exp"], units=units_energy, inpfilter={"commodity": ["ethanol"]})
    _FTL_exp_reg = pp.inp(tec=["meth_exp"], units=units_energy, inpfilter={"commodity": ["methanol"]})
    _LNG_exp_reg = pp.inp(trade_tec=["LNG_shipped_exp"], units=units_energy, inpfilter={"commodity": ["LNG"]})
    _H2_exp_reg = pp.inp(trade_tec=["lh2_shipped_exp"], units=units_energy, inpfilter={"commodity": ["lh2"]})

    # Calculate net trade
    _Biomass_total = pp.inp(trade_tec=["biomass_shipped_imp"], units=units_energy, inpfilter={"commodity": ["biomass"]}) - pp.inp(
        trade_tec=["biomass_shipped_exp"], units=units_energy, inpfilter={"commodity": ["biomass"]}
    )

    _Coal_total = pp.inp(trade_tec=["coal_shipped_imp"], units=units_energy, inpfilter={"commodity": ["coal"]}) - pp.inp(
        trade_tec=["coal_shipped_exp"], units=units_energy, inpfilter={"commodity": ["coal"]}
    )

    _CrudeOil_total = pp.inp(trade_tec=["crudeoil_shipped_imp", "crudeoil_piped_imp"], units=units_energy, inpfilter={"commodity": ["crudeoil"]}) - pp.inp(
        trade_tec=["crudeoil_shipped_exp", "crudeoil_piped_exp"], units=units_energy, inpfilter={"commodity": ["crudeoil"]}
    )

    _foilprod_total = pp.inp(trade_tec=["foil_shipped_imp", "foil_piped_imp"], units=units_energy, inpfilter={"commodity": ["fueloil"]}) - pp.inp(
        trade_tec=["foil_shipped_exp", "foil_piped_exp"], units=units_energy, inpfilter={"commodity": ["fueloil"]}
    )
    _loilprod_total = pp.inp(trade_tec=["loil_shipped_imp", "loil_piped_imp"], units=units_energy, inpfilter={"commodity": ["lightoil"]}) - pp.inp(
        trade_tec=["loil_shipped_exp", "loil_piped_exp"], units=units_energy, inpfilter={"commodity": ["lightoil"]}
    )

    _Gas_total = pp.inp(trade_tec=["LNG_shipped_imp", "gas_piped_imp"], units=units_energy, inpfilter={"commodity": ["LNG", "gas"]}) - pp.inp(
        trade_tec=["LNG_shipped_exp", "gas_piped_exp"],
        units=units_energy,
        inpfilter={"commodity": ["LNG", "gas"]}
    )

    _Elec_total = pp.inp(
        trade_tec=["elec_imp"], units=units_energy,
    ) - pp.inp(
        trade_tec=["elec_exp"], units=units_energy,
    )

    _H2_total = pp.inp(trade_tec=["lh2_shipped_imp"], units=units_energy, inpfilter={"commodity": ["lh2"]}) - pp.inp(
        trade_tec=["lh2_shipped_exp"], units=units_energy, inpfilter={"commodity": ["lh2"]}
    )

    _BTL_total = pp.inp(trade_tec=["eth_shipped_imp"], units=units_energy, inpfilter={"commodity": ["ethanol"]}) - pp.inp(
        trade_tec=["eth_shipped_exp"], units=units_energy, inpfilter={"commodity": ["ethanol"]}
    )

    _FTL_total = pp.inp(tec=["meth_imp"], units=units_energy, inpfilter={"commodity": ["methanol"]}) - pp.inp(
        tec=["meth_exp"], units=units_energy, inpfilter={"commodity": ["methanol"]}
    )

    # Fuel consumption by international shipping in glb region
    # function aggr_glb sets all region values to the value region "World"
    _FOil_ship = pp_utils.aggr_glb(pp.inp(tec = "foil_bunker", units = units_energy))
    _LOil_ship = pp_utils.aggr_glb(pp.inp(tec = "loil_bunker", units = units_energy))
    _BTL_ship = pp_utils.aggr_glb(pp.inp(tec = "eth_bunker", units = units_energy))
    _FTL_ship = pp_utils.aggr_glb(pp.inp(tec = "meth_bunker", units = units_energy))
    _LNG_ship = pp_utils.aggr_glb(pp.inp(tec = "LNG_bunker", units = units_energy))
    _H2_ship = pp_utils.aggr_glb(pp.inp(tec = "LH2_bunker", units = units_energy))

    # Exports to glb region for all regions jointly
    # function aggr_reg aggregates values over all regions != World (!aggr
    # nam+lam+weu+eeu+fsu+mea+afr+cpa+sas+pas+pao)
    _FOil_exp_glb = pp_utils.aggr_reg(pp.inp(trade_tec=["foil_shipped_exp", "foil_piped_exp"], units=units_energy, inpfilter={"commodity": ["fueloil"]}))
    _LOil_exp_glb = pp_utils.aggr_reg(pp.inp(trade_tec=["loil_shipped_exp", "loil_piped_exp"], units=units_energy, inpfilter={"commodity": ["lightoil"]}))
    _BTL_exp_glb = pp_utils.aggr_reg(pp.inp(trade_tec=["eth_shipped_exp"], units=units_energy, inpfilter={"commodity": ["ethanol"]}))
    _FTL_exp_glb = pp_utils.aggr_reg(pp.inp(tec=["meth_exp"], units=units_energy, inpfilter={"commodity": ["methanol"]}))
    _LNG_exp_glb = pp_utils.aggr_reg(pp.inp(trade_tec=["LNG_shipped_exp"], units=units_energy, inpfilter={"commodity": ["LNG"]}))
    _H2_exp_glb = pp_utils.aggr_reg(pp.inp(trade_tec=["lh2_shipped_exp"], units=units_energy, inpfilter={"commodity": ["lh2"]}))

    # Share of international shipping fuel demand of region in tit file
    _FOil_ship_reg = _FOil_ship * (_FOil_exp_reg / _FOil_exp_glb).fillna(0)
    _LOil_ship_reg = _LOil_ship * (_LOil_exp_reg / _LOil_exp_glb).fillna(0)
    _BTL_ship_reg = _BTL_ship * (_BTL_exp_reg / _BTL_exp_glb).fillna(0)
    _FTL_ship_reg = _FTL_ship * (_FTL_exp_reg / _FTL_exp_glb).fillna(0)
    _LNG_ship_reg = _LNG_ship * (_LNG_exp_reg / _LNG_exp_glb).fillna(0)
    _H2_ship_reg = _H2_ship * (_H2_exp_reg / _H2_exp_glb).fillna(0)

    # ----------------------------------------------------
    # Net trade (excluding use for international shipping)
    # ----------------------------------------------------

    vars["Primary Energy|Biomass|Volume"] = -1 * _Biomass_total
    vars["Primary Energy|Coal|Volume"] = -1 * _Coal_total
    vars["Primary Energy|Oil|Volume"] = -1 * _CrudeOil_total
    vars["Primary Energy|Gas|Volume"] = -1 * (_Gas_total + _LNG_ship_reg)
    vars["Secondary Energy|Electricity|Volume"] = -1 * _Elec_total
    vars["Secondary Energy|Hydrogen|Volume"] = -1 * (_H2_total + _H2_ship_reg)

    vars["Secondary Energy|Liquids|Biomass|Volume"] = -1 * (_BTL_total + _BTL_ship_reg)

    vars["Secondary Energy|Liquids|Coal|Volume"] = -1 * (_FTL_total + _FTL_ship_reg)

    vars["Secondary Energy|Liquids|Oil|Volume|Fuel Oil"] = -1 * (
        _foilprod_total + _FOil_ship_reg
    )
    vars["Secondary Energy|Liquids|Oil|Volume|Light Oil"] = -1 * (
        _loilprod_total + _LOil_ship_reg
    )

    # -----------
    # Gross trade
    # -----------

    vars["Gross Export|Primary Energy|Biomass|Volume"] = pp.inp(
        trade_tec=["biomass_shipped_exp"], units=units_energy, inpfilter={"commodity": ["biomass"]}
    )

    vars["Gross Export|Primary Energy|Coal|Volume"] = pp.inp(
        trade_tec=["coal_shipped_exp"], units=units_energy, inpfilter={"commodity": ["coal"]}
    )

    vars["Gross Export|Primary Energy|Oil|Volume"] = pp.inp(
        trade_tec=["crudeoil_shipped_exp", "crudeoil_piped_exp"], units=units_energy, inpfilter={"commodity": ["crudeoil"]}
    )

    vars["Gross Export|Primary Energy|Gas|Volume"] = pp.inp(
        trade_tec=["LNG_shipped_exp", "gas_piped_exp"], units=units_energy, inpfilter={"commodity": ["LNG", "gas"]}
    )

    vars["Gross Export|Secondary Energy|Electricity|Volume"] = pp.inp(
        trade_tec=["elec_exp"], units=units_energy, inpfilter={"commodity": ["electr_africa", 'electr_america', 'electr_asia',
                                 'electr_eurasia', 'electr_eur_afr', 'electr_asia_afr']}
    )

    vars["Gross Export|Secondary Energy|Liquids|Biomass|Volume"] = pp.inp(
        trade_tec=["eth_shipped_exp"], units=units_energy, inpfilter={"commodity": ["ethanol"]}
    )

    vars["Gross Export|Secondary Energy|Liquids|Coal|Volume"] = pp.inp(
        tec=["meth_exp"], units=units_energy, inpfilter={"commodity": ["methanol"]}
    )

    vars["Gross Export|Secondary Energy|Liquids|Oil|Volume"] = pp.inp(
        trade_tec=["foil_shipped_exp", "loil_shipped_exp",
                   "foil_piped_exp", "loil_piped_exp"], units=units_energy, inpfilter={"commodity": ["fueloil", "lightoil"]}
    )

    vars["Gross Import|Primary Energy|Biomass|Volume"] = pp.inp(
        trade_tec=["biomass_shipped_imp"], units=units_energy, inpfilter={"commodity": ["biomass"]}
    )

    vars["Gross Import|Primary Energy|Coal|Volume"] = pp.inp(
        trade_tec=["coal_shipped_imp"], units=units_energy, inpfilter={"commodity": ["coal"]}
    )

    vars["Gross Import|Primary Energy|Oil|Volume"] = pp.inp(
        trade_tec=["crudeoil_shipped_imp", "crudeoil_piped_imp"], units=units_energy, inpfilter={"commodity": ["crudeoil"]}
    )

    vars["Gross Import|Primary Energy|Gas|Volume"] = pp.inp(
        trade_tec=["LNG_shipped_imp", "gas_piped_imp"], units=units_energy, inpfilter={"commodity": ["LNG", "gas"]}
    )

    vars["Gross Import|Secondary Energy|Electricity|Volume"] = pp.inp(
        trade_tec=["elec_imp"], units=units_energy
    )

    vars["Gross Import|Secondary Energy|Liquids|Biomass|Volume"] = pp.inp(
        trade_tec=["eth_shipped_imp"], units=units_energy, inpfilter={"commodity": ["ethanol"]}
    )

    vars["Gross Import|Secondary Energy|Liquids|Coal|Volume"] = pp.inp(
        tec=["meth_imp"], units=units_energy, inpfilter={"commodity": ["methanol"]}
    )

    vars["Gross Import|Secondary Energy|Liquids|Oil|Volume"] = pp.inp(
        trade_tec=["foil_shipped_imp", "loil_shipped_imp",
                   "foil_piped_imp", "loil_piped_imp"], units=units_energy, inpfilter={"commodity": ["fueloil", "lightoil"]}
    )

    dfs.append(pp_utils.make_outputdf(vars, units_energy))

    cprc = pp.cprc(units_CPrc_co2)

    # Emissions Trading
    vars = {}
    vars["Emissions Allowances|Value"] = pp_utils._make_zero()
    vars["Emissions|Value|Carbon|Absolute"] = pp_utils._make_zero()
    vars["Emissions|Value|Carbon|Exports"] = (
        pp.out(tec = "certificate_sell") * mu["conv_c2co2"] * cprc / 1000
    )
    vars["Emissions|Value|Carbon|Imports"] = (
        pp.inp(tec = "certificate_buy") * mu["conv_c2co2"] * cprc / 1000
    )
    vars["Emissions|Value|Carbon|Net Exports"] = (
        vars["Emissions|Value|Carbon|Exports"] - vars["Emissions|Value|Carbon|Imports"]
    )

    dfs.append(pp_utils.make_outputdf(vars, units_emi_val))

    vars = {}
    vars["Emissions Allowances|Volume"] = pp_utils._make_zero()
    vars["Emissions|Volume|Carbon|Absolute"] = pp_utils._make_zero()
    vars["Emissions|Volume|Carbon|Exports"] = (
        pp.out(tec = "certificate_sell") * mu["conv_c2co2"]
    )
    vars["Emissions|Volume|Carbon|Imports"] = (
        pp.inp(tec = "certificate_buy") * mu["conv_c2co2"]
    )
    vars["Emissions|Volume|Carbon|Net Exports"] = (
        vars["Emissions|Volume|Carbon|Exports"]
        - vars["Emissions|Volume|Carbon|Imports"]
    )

    dfs.append(pp_utils.make_outputdf(vars, units_emi_vol))

    vars = {}

    vars["Agriculture"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture"],
        }
    )

    vars["Agriculture|Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Crops"],
        }
    )

    vars["Agriculture|Crops|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Crops|Cereals"],
        }
    )

    vars["Agriculture|Crops|Oil Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Crops|Oil Crops"],
        }
    )

    vars["Agriculture|Crops|Other Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Crops|Other Crops"],
        }
    )

    vars["Agriculture|Crops|Sugar Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Crops|Sugar Crops"],
        }
    )

    vars["Agriculture|Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Livestock"],
        }
    )

    vars["Agriculture|Livestock|Dairy"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Livestock|Dairy"],
        }
    )

    vars["Agriculture|Livestock|Non-Ruminant"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Livestock|Non-Ruminant"],
        }
    )

    vars["Agriculture|Livestock|Ruminant"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Agriculture|Livestock|Ruminant"],
        }
    )

    vars["Forestry|Semi-Finished|Chemical Pulp"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Semi-Finished|Chemical Pulp"],
        }
    )

    vars["Forestry|Semi-Finished|Mechanical Pulp"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Semi-Finished|Mechanical Pulp"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_agri_vol))

    vars = {}

    vars["Forestry|Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Roundwood"],
        }
    )

    vars["Forestry|Roundwood|Industrial Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Roundwood|Industrial Roundwood"],
        }
    )

    vars["Forestry|Roundwood|Wood Fuel"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Roundwood|Wood Fuel"],
        }
    )

    vars["Forestry|Semi-Finished|Fiberboard"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Semi-Finished|Fiberboard"],
        }
    )

    vars["Forestry|Semi-Finished|Other Wood Products"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Semi-Finished|Other Wood Products"],
        }
    )

    vars["Forestry|Semi-Finished|Plywood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Semi-Finished|Plywood"],
        }
    )

    vars["Forestry|Semi-Finished|Sawnwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Trade|Forestry|Semi-Finished|Sawnwood"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_for_vol))

    df = pd.concat(dfs, sort=True)
    return df


@_register
def retr_supply_inv(units_energy, units_emi, units_ene_mdl):
    """Technology results: Investments.

    Investments into technologies.

    Note OFR - 20.04.2017: The following has been checked between Volker
    Krey, David McCollum and Oliver Fricko.

    There are fixed factors by which ccs technologies are multiplied which
    equate to the share of the powerplant costs which split investments into the
    share associated with the standard powerplant and the share associated
    with those investments related to CCS.

    For some extraction and synfuel technologies, a certain share of the voms and
    foms are attributed to the investments which is based on the GEA-Study
    where the derived investment costs were partly attributed to the
    voms/foms.

    Parameters
    ----------
    units_energy : str
        Units to which energy variables should be converted.
    units_emi : str
        Units to which emission variables should be converted.
    units_ene_mdl : str
        Native units of energy in the model.
    """

    vars = {}

    # ----------
    # Extraction
    # ----------

    # Note OFR 25.04.2017: All non-extraction costs for Coal, Gas and Oil
    # have been moved to "Energy|Other"

    vars["Extraction|Coal"] = pp.investment(
        tec=["coal_extr_ch4", "coal_extr", "lignite_extr"], units=units_energy
    )

    vars["Extraction|Gas|Conventional"] = (
        pp.investment(
            tec=["gas_extr_1", "gas_extr_2", "gas_extr_3", "gas_extr_4"], units=units_energy
        )
        + pp.act_vom(
            tec=["gas_extr_1", "gas_extr_2", "gas_extr_3", "gas_extr_4"], units=units_energy
        )
        * 0.5
    )

    vars["Extraction|Gas|Unconventional"] = (
        pp.investment(
            tec=["gas_extr_5", "gas_extr_6", "gas_extr_7", "gas_extr_8"], units=units_energy
        )
        + pp.act_vom(
            tec=["gas_extr_5", "gas_extr_6", "gas_extr_7", "gas_extr_8"], units=units_energy
        )
        * 0.5
    )

    # Note OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under "Liquids|Oil"

    vars["Extraction|Oil|Conventional"] = (
        pp.investment(
            tec=[
                "oil_extr_1",
                "oil_extr_2",
                "oil_extr_3",
                "oil_extr_1_ch4",
                "oil_extr_2_ch4",
                "oil_extr_3_ch4",
            ],
            units=units_energy,
        )
        + pp.act_vom(
            tec=[
                "oil_extr_1",
                "oil_extr_2",
                "oil_extr_3",
                "oil_extr_1_ch4",
                "oil_extr_2_ch4",
                "oil_extr_3_ch4",
            ],
            units=units_energy,
        )
        * 0.5
    )

    vars["Extraction|Oil|Unconventional"] = (
        pp.investment(
            tec=[
                "oil_extr_4",
                "oil_extr_4_ch4",
                "oil_extr_5",
                "oil_extr_6",
                "oil_extr_7",
                "oil_extr_8",
            ],
            units=units_energy,
        )
        + pp.act_vom(
            tec=[
                "oil_extr_4",
                "oil_extr_4_ch4",
                "oil_extr_5",
                "oil_extr_6",
                "oil_extr_7",
                "oil_extr_8",
            ],
            units=units_energy,
        )
        * 0.5
    )

    # As no mode is specified, u5-reproc will account for all 3 modes.
    vars["Extraction|Uranium"] = (
        pp.investment(
            tec=["uran2u5", "Uran_extr", "u5-reproc", "plutonium_prod"], units=units_energy
        )
        + pp.act_vom(tec=["uran2u5", "Uran_extr"], units=units_energy)
        + pp.act_vom(tec=["u5-reproc"], actfilter={"mode": ["M1"]}, units=units_energy)
        + pp.tic_fom(tec=["uran2u5", "Uran_extr", "u5-reproc"], units=units_energy)
    )

    # ----------------
    # Electricity - Cooling
    # ----------------

    # Bioenergy Cooling Technologies
    _cool_bio_istig = pp.investment(
        tec=[
            "bio_istig__ot_fresh",
            "bio_istig__cl_fresh",
            "bio_istig__ot_saline",
            "bio_istig__air",
        ],
        units=units_energy,
    )

    _cool_bio_istig_ccs = pp.investment(
        tec=[
            "bio_istig_ccs__ot_fresh",
            "bio_istig_ccs__cl_fresh",
            "bio_istig_ccs__ot_saline",
            "bio_istig_ccs__air",
        ],
        units=units_energy,
    )

    _cool_bio_ppl = pp.investment(
        tec=[
            "bio_ppl__ot_fresh",
            "bio_ppl__cl_fresh",
            "bio_ppl__ot_saline",
            "bio_ppl__air",
        ],
        units=units_energy,
    )

    # Geothermal cooling technologies
    _cool_geo_ppl = pp.investment(
        tec=[
            "geo_ppl__ot_fresh",
            "geo_ppl__cl_fresh",
            "geo_ppl__ot_saline",
            "geo_ppl__air",
        ],
        units=units_energy,
    )

    # Light oil cooling technologies
    _cool_loil_cc = pp.investment(
        tec=[
            "loil_cc__ot_fresh",
            "loil_cc__cl_fresh",
            "loil_cc__ot_saline",
            "loil_cc__air",
        ],
        units=units_energy,
    )

    _cool_loil_ppl = pp.investment(
        tec=[
            "loil_ppl__ot_fresh",
            "loil_ppl__cl_fresh",
            "loil_ppl__ot_saline",
            "loil_ppl__air",
        ],
        units=units_energy,
    )

    # Coal Cooling Technologies
    _cool_coal_adv_ccs = pp.investment(
        tec=[
            "coal_adv_ccs__ot_fresh",
            "coal_adv_ccs__cl_fresh",
            "coal_adv_ccs__ot_saline",
            "coal_adv_ccs__air",
        ],
        units=units_energy,
    )
    _cool_igcc_ccs = pp.investment(
        tec=[
            "igcc_ccs__ot_fresh",
            "igcc_ccs__cl_fresh",
            "igcc_ccs__ot_saline",
            "igcc_ccs__air",
        ],
        units=units_energy,
    )

    _cool_igcc = pp.investment(
        tec=[
            "igcc__ot_fresh",
            "igcc__cl_fresh",
            "igcc__ot_saline",
            "igcc__air",
        ],
        units=units_energy,
    )

    _cool_coal_ppl = pp.investment(
        tec=[
            "coal_ppl__ot_fresh",
            "coal_ppl__cl_fresh",
            "coal_ppl__ot_saline",
            "coal_ppl__air",
            "coal_ppl_u__ot_fresh",
            "coal_ppl_u__cl_fresh",
            "coal_ppl_u__ot_saline",
            "coal_ppl_u__air",
            "coal_adv__ot_fresh",
            "coal_adv__cl_fresh",
            "coal_adv__ot_saline",
            "coal_adv__air",
        ],
        units=units_energy,
    )

    # Fossil Fuel Cooling Technologies
    _cool_foil_ppl = pp.investment(
        tec=[
            "foil_ppl__ot_fresh",
            "foil_ppl__cl_fresh",
            "foil_ppl__ot_saline",
            "foil_ppl__air",
        ],
        units=units_energy,
    )

    # Gas Cooling Technologies
    _cool_gas_cc_ccs = pp.investment(
        tec=[
            "gas_cc_ccs__ot_fresh",
            "gas_cc_ccs__cl_fresh",
            "gas_cc_ccs__ot_saline",
            "gas_cc_ccs__air",
        ],
        units=units_energy,
    )

    _cool_gas_ppl = pp.investment(
        tec=[
            "gas_ppl__ot_fresh",
            "gas_ppl__cl_fresh",
            "gas_ppl__ot_saline",
            "gas_ppl__air",
            "gas_cc__ot_fresh",
            "gas_cc__cl_fresh",
            "gas_cc__ot_saline",
            "gas_cc__air",
        ],
        units=units_energy,
    )

    # Nuclear Cooling Technologies
    _cool_nuc = pp.investment(
        tec=[
            "nuc_hc__ot_fresh",
            "nuc_hc__cl_fresh",
            "nuc_hc__ot_saline",
            "nuc_hc__air",
            "nuc_lc__ot_fresh",
            "nuc_lc__cl_fresh",
            "nuc_lc__ot_saline",
            "nuc_lc__air",
        ],
        units=units_energy,
    )

    # CSP (Concentrated Solar Power) Cooling Technologies
    _cool_csp_sm1 = pp.investment(
        tec=[
            # Base resources
            "csp_sm1_res__ot_fresh",
            "csp_sm1_res__cl_fresh",
            "csp_sm1_res__ot_saline",
            "csp_sm1_res__air",
            # Variants 1 through 7
            "csp_sm1_res1__ot_fresh",
            "csp_sm1_res1__cl_fresh",
            "csp_sm1_res1__ot_saline",
            "csp_sm1_res1__air",
            "csp_sm1_res2__ot_fresh",
            "csp_sm1_res2__cl_fresh",
            "csp_sm1_res2__ot_saline",
            "csp_sm1_res2__air",
            "csp_sm1_res3__ot_fresh",
            "csp_sm1_res3__cl_fresh",
            "csp_sm1_res3__ot_saline",
            "csp_sm1_res3__air",
            "csp_sm1_res4__ot_fresh",
            "csp_sm1_res4__cl_fresh",
            "csp_sm1_res4__ot_saline",
            "csp_sm1_res4__air",
            "csp_sm1_res5__ot_fresh",
            "csp_sm1_res5__cl_fresh",
            "csp_sm1_res5__ot_saline",
            "csp_sm1_res5__air",
            "csp_sm1_res6__ot_fresh",
            "csp_sm1_res6__cl_fresh",
            "csp_sm1_res6__ot_saline",
            "csp_sm1_res6__air",
            "csp_sm1_res7__ot_fresh",
            "csp_sm1_res7__cl_fresh",
            "csp_sm1_res7__ot_saline",
            "csp_sm1_res7__air",
            "csp_sm1_res_hist_2010__ot_fresh",
            "csp_sm1_res_hist_2010__cl_fresh",
            "csp_sm1_res_hist_2010__ot_saline",
            "csp_sm1_res_hist_2010__air",
            "csp_sm1_res_hist_2015__ot_fresh",
            "csp_sm1_res_hist_2015__cl_fresh",
            "csp_sm1_res_hist_2015__ot_saline",
            "csp_sm1_res_hist_2015__air",
            "csp_sm1_res_hist_2020__ot_fresh",
            "csp_sm1_res_hist_2020__cl_fresh",
            "csp_sm1_res_hist_2020__ot_saline",
            "csp_sm1_res_hist_2020__air",
        ],
        units=units_energy,
    )

    _cool_csp_sm3 = pp.investment(
        tec=[
            # Base resources
            "csp_sm3_res__ot_fresh",
            "csp_sm3_res__cl_fresh",
            "csp_sm3_res__ot_saline",
            "csp_sm3_res__air",
            # Variants 1 through 7
            "csp_sm3_res1__ot_fresh",
            "csp_sm3_res1__cl_fresh",
            "csp_sm3_res1__ot_saline",
            "csp_sm3_res1__air",
            "csp_sm3_res2__ot_fresh",
            "csp_sm3_res2__cl_fresh",
            "csp_sm3_res2__ot_saline",
            "csp_sm3_res2__air",
            "csp_sm3_res3__ot_fresh",
            "csp_sm3_res3__cl_fresh",
            "csp_sm3_res3__ot_saline",
            "csp_sm3_res3__air",
            "csp_sm3_res4__ot_fresh",
            "csp_sm3_res4__cl_fresh",
            "csp_sm3_res4__ot_saline",
            "csp_sm3_res4__air",
            "csp_sm3_res5__ot_fresh",
            "csp_sm3_res5__cl_fresh",
            "csp_sm3_res5__ot_saline",
            "csp_sm3_res5__air",
            "csp_sm3_res6__ot_fresh",
            "csp_sm3_res6__cl_fresh",
            "csp_sm3_res6__ot_saline",
            "csp_sm3_res6__air",
            "csp_sm3_res7__ot_fresh",
            "csp_sm3_res7__cl_fresh",
            "csp_sm3_res7__ot_saline",
            "csp_sm3_res7__air",
        ],
        units=units_energy,
    )

    # ---------------------
    # Electricity - Fossils
    # ---------------------

    vars["Electricity|Coal|w/ CCS"] = (
        pp.investment(tec=["c_ppl_co2scr"], units=units_energy)
        + pp.investment(tec=["coal_adv_ccs"], units=units_energy) * 0.25
        + pp.investment(tec=["igcc_ccs"], units=units_energy) * 0.31
        + _cool_coal_adv_ccs * 0.25
        + _cool_igcc_ccs * 0.31
    )

    vars["Electricity|Coal|w/o CCS"] = (
        pp.investment(tec=["coal_ppl", "coal_ppl_u", "coal_adv"], units=units_energy)
        + pp.investment(tec=["coal_adv_ccs"], units=units_energy) * 0.75
        + pp.investment(tec=["igcc"], units=units_energy)
        + pp.investment(tec=["igcc_ccs"], units=units_energy) * 0.69
        + _cool_coal_ppl
        + _cool_coal_adv_ccs * 0.75
        + _cool_igcc
        + _cool_igcc_ccs * 0.69
    )

    vars["Electricity|Gas|w/ CCS"] = (
        pp.investment(tec=["g_ppl_co2scr"], units=units_energy)
        + pp.investment(tec=["gas_cc_ccs"], units=units_energy) * 0.53
        + _cool_gas_cc_ccs * 0.53
    )

    vars["Electricity|Gas|w/o CCS"] = (
        pp.investment(tec=["gas_cc", "gas_ct", "gas_ppl"], units=units_energy)
        + pp.investment(tec=["gas_cc_ccs"], units=units_energy) * 0.47
        + _cool_gas_ppl
        + _cool_gas_cc_ccs * 0.47
    )

    vars["Electricity|Oil|w/o CCS"] = (
        pp.investment(
            tec=["foil_ppl", "loil_ppl", "oil_ppl", "loil_cc"], units=units_energy
        )
        + _cool_foil_ppl
        + _cool_loil_ppl
        + _cool_loil_cc
    )

    # ------------------------
    # Electricity - Renewables
    # ------------------------

    vars["Electricity|Biomass|w/ CCS"] = (
        pp.investment(tec=["bio_ppl_co2scr"], units=units_energy)
        + pp.investment(tec=["bio_istig_ccs"], units=units_energy) * 0.31
        + _cool_bio_istig_ccs * 0.31
    )

    vars["Electricity|Biomass|w/o CCS"] = (
        pp.investment(tec=["bio_ppl", "bio_istig"], units=units_energy)
        + pp.investment(tec=["bio_istig_ccs"], units=units_energy) * 0.69
        + _cool_bio_ppl
        + _cool_bio_istig
        + _cool_bio_istig_ccs * 0.69
    )

    vars["Electricity|Geothermal"] = (
        pp.investment(tec=["geo_ppl"], units=units_energy) + _cool_geo_ppl
    )

    vars["Electricity|Hydro"] = pp.investment(
        tec=["hydro_hc", "hydro_lc"], units=units_energy
    )
    vars["Electricity|Other"] = pp.investment(
        tec=["h2_fc_I", "h2_fc_RC"], units=units_energy
    )

    _solar_pv_elec = pp.investment(
        tec=[
            "solar_pv_ppl",
            "solar_pv_I",
            "solar_pv_RC",
            "solar_res1",
            "solar_res2",
            "solar_res3",
            "solar_res4",
            "solar_res5",
            "solar_res6",
            "solar_res7",
            "solar_res8",
            "solar_res_hist_2000",
            "solar_res_hist_2005",
            "solar_res_hist_2010",
            "solar_res_hist_2015",
            "solar_res_hist_2020",
            "solar_res_hist_2025",
        ],
        units=units_energy,
    )

    _solar_th_elec = (
        pp.investment(tec=["csp_sm1_ppl", "csp_sm3_ppl"], units=units_energy)
        + _cool_csp_sm1
        + _cool_csp_sm3
    )

    vars["Electricity|Solar|PV|Commercial"] = _solar_pv_elec
    vars["Electricity|Solar|PV|Residential"] = pp.investment(
        tec=[
            "solar_res_rt_hist_2000",
            "solar_res_rt_hist_2005",
            "solar_res_rt_hist_2010",
            "solar_res_rt_hist_2015",
            "solar_res_rt_hist_2020",
            "solar_res_rt_hist_2025",
            "solar_res_RT1",
            "solar_res_RT2",
            "solar_res_RT3",
            "solar_res_RT4",
            "solar_res_RT5",
            "solar_res_RT6",
            "solar_res_RT7",
            "solar_res_RT8",
        ],
        units=units_energy,
    )
    vars["Electricity|Solar|PV"] = (
        vars["Electricity|Solar|PV|Commercial"]
        + vars["Electricity|Solar|PV|Residential"]
    )
    vars["Electricity|Solar|CSP"] = _solar_th_elec
    vars["Electricity|Wind|Onshore"] = pp.investment(
        tec=[
            "wind_ppl",
            "wind_res1",
            "wind_res2",
            "wind_res3",
            "wind_res4",
            "wind_res_hist_2000",
            "wind_res_hist_2005",
            "wind_res_hist_2010",
            "wind_res_hist_2015",
            "wind_res_hist_2020",
            "wind_res_hist_2025",
        ],
        units=units_energy,
    )
    vars["Electricity|Wind|Offshore"] = pp.investment(
        tec=[
            "wind_ppf",
            "wind_ref1",
            "wind_ref2",
            "wind_ref3",
            "wind_ref4",
            "wind_ref5",
            "wind_ref_hist_2000",
            "wind_ref_hist_2005",
            "wind_ref_hist_2010",
            "wind_ref_hist_2015",
            "wind_ref_hist_2020",
            "wind_ref_hist_2025",
        ],
        units=units_energy,
    )

    # -------------------
    # Electricity Nuclear
    # -------------------

    vars["Electricity|Nuclear"] = (
        pp.investment(tec=["nuc_hc", "nuc_lc"], units=units_energy) + _cool_nuc
    )

    # --------------------------------------------------
    # Electricity Storage, transmission and distribution
    # --------------------------------------------------

    vars["Electricity|Electricity Storage"] = pp.investment(
        tec=["stor_ppl"], units=units_energy
    )

    vars["Electricity|Transmission and Distribution"] = (
        pp.investment(
            tec = ['elec_t_d'],
            trade_tec = ['elec_exp', 'elec_imp'],
            units=units_energy,
        )
        + pp.act_vom(
            tec = ['elec_t_d'],
            trade_tec = ['elec_exp', 'elec_imp'],
            units=units_energy,
        )
        * 0.5
        + pp.tic_fom(
            tec = ['elec_t_d'],
            trade_tec = ['elec_exp', 'elec_imp'],
            units=units_energy,
        )
        * 0.5
    )

    # ------------------------------------------
    # CO2 Storage, transmission and distribution
    # ------------------------------------------

    _CCS_coal_elec = -1 * pp.emi(
        tec=["c_ppl_co2scr", "coal_adv_ccs", "igcc_ccs", "cement_co2scr"],
        units=units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_synf = -1 * pp.emi(
        tec=["syn_liq_ccs", "h2_coal_ccs", "meth_coal_ccs"],
        units=units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_elec = -1 * pp.emi(
        tec=["g_ppl_co2scr", "gas_cc_ccs"],
        units=units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_synf = -1 * pp.emi(
        tec=["h2_smr_ccs", "meth_ng_ccs"],
        units=units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_elec = -1 * pp.emi(
        tec=["bio_ppl_co2scr", "bio_istig_ccs"],
        units=units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_synf = -1 * pp.emi(
        tec=["eth_bio_ccs", "liq_bio_ccs", "h2_bio_ccs"],
        units=units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _Biogas_use_tot = pp.out(tec=["gas_bio"], units=units_energy)

    _Gas_use_tot = pp.inp(
        tec=[
            "gas_ppl",
            "gas_cc",
            "gas_cc_ccs",
            "gas_ct",
            "gas_hpl",
            "meth_ng",
            "meth_ng_ccs",
            "h2_smr",
            "h2_smr_ccs",
            "gas_rc",
            "hp_gas_rc",
            "gas_i",
            "hp_gas_i",
            "gas_trp",
            "gas_fs",
        ]
    )

    _Biogas_share = (_Biogas_use_tot / _Gas_use_tot).fillna(0)

    _CCS_Foss = (
        _CCS_coal_elec
        + _CCS_coal_synf
        + _CCS_gas_elec * (1 - _Biogas_share)
        + _CCS_gas_synf * (1 - _Biogas_share)
    )

    _CCS_Bio = (
        _CCS_bio_elec + _CCS_bio_synf - (_CCS_gas_elec + _CCS_gas_synf) * _Biogas_share
    )

    _CCS_coal_elec_shr = (_CCS_coal_elec / _CCS_Foss).fillna(0)
    _CCS_coal_synf_shr = (_CCS_coal_synf / _CCS_Foss).fillna(0)
    _CCS_gas_elec_shr = (_CCS_gas_elec / _CCS_Foss).fillna(0)
    _CCS_gas_synf_shr = (_CCS_gas_synf / _CCS_Foss).fillna(0)
    _CCS_bio_elec_shr = (_CCS_bio_elec / _CCS_Bio).fillna(0)
    _CCS_bio_synf_shr = (_CCS_bio_synf / _CCS_Bio).fillna(0)

    CO2_trans_dist_elec = (
        pp.act_vom(tec="co2_tr_dis", units=units_energy) * 0.5 * _CCS_coal_elec_shr
        + pp.act_vom(tec="co2_tr_dis", units=units_energy) * 0.5 * _CCS_gas_elec_shr
        + pp.act_vom(tec="bco2_tr_dis", units=units_energy) * 0.5 * _CCS_bio_elec_shr
    )

    CO2_trans_dist_synf = (
        pp.act_vom(tec="co2_tr_dis", units=units_energy) * 0.5 * _CCS_coal_synf_shr
        + pp.act_vom(tec="co2_tr_dis", units=units_energy) * 0.5 * _CCS_gas_synf_shr
        + pp.act_vom(tec="bco2_tr_dis", units=units_energy) * 0.5 * _CCS_bio_synf_shr
    )

    vars["CO2 Transport and Storage"] = CO2_trans_dist_elec + CO2_trans_dist_synf

    # ----
    # Heat
    # ----

    vars["Heat"] = pp.investment(
        tec=["coal_hpl", "foil_hpl", "gas_hpl", "bio_hpl", "heat_t_d", "po_turbine"],
        units=units_energy,
    )

    # -------------------------
    # Synthetic fuel production
    # -------------------------

    # Note OFR 25.04.2017: XXX_synf_ccs has been split into hydrogen and
    # liquids. The shares then add up to 1, but the variables are kept
    # separate in order to preserve the split between CCS and non-CCS

    _Coal_synf_ccs_liq = (
        pp.investment(tec=["meth_coal_ccs"], units=units_energy) * 0.02
        + pp.investment(tec=["syn_liq_ccs"], units=units_energy) * 0.01
    )

    _Gas_synf_ccs_liq = pp.investment(tec=["meth_ng_ccs"], units=units_energy) * 0.08

    _Bio_synf_ccs_liq = (
        pp.investment(tec=["eth_bio_ccs"], units=units_energy) * 0.34
        + pp.investment(tec=["liq_bio_ccs"], units=units_energy) * 0.02
    )

    _Coal_synf_ccs_h2 = pp.investment(tec=["h2_coal_ccs"], units=units_energy) * 0.03
    _Gas_synf_ccs_h2 = pp.investment(tec=["h2_smr_ccs"], units=units_energy) * 0.17
    _Bio_synf_ccs_h2 = pp.investment(tec=["h2_bio_ccs"], units=units_energy) * 0.02

    # Note OFR 25.04.2017: "coal_gas" have been moved to "other"
    vars["Liquids|Coal and Gas"] = (
        pp.investment(tec=["meth_coal", "syn_liq", "meth_ng"], units=units_energy)
        + pp.investment(tec=["meth_ng_ccs"], units=units_energy) * 0.92
        + pp.investment(tec=["meth_coal_ccs"], units=units_energy) * 0.98
        + pp.investment(tec=["syn_liq_ccs"], units=units_energy) * 0.99
        + _Coal_synf_ccs_liq
        + _Gas_synf_ccs_liq
    )

    # Note OFR 25.04.2017: "gas_bio" has been moved to "other"
    vars["Liquids|Biomass"] = (
        pp.investment(tec=["eth_bio", "liq_bio"], units=units_energy)
        + pp.investment(tec=["liq_bio_ccs"], units=units_energy) * 0.98
        + pp.investment(tec=["eth_bio_ccs"], units=units_energy) * 0.66
        + _Bio_synf_ccs_liq
    )

    # Note OFR 25.04.2017: "transport, import and exports costs related to
    # liquids are only included in the total"
    _Synfuel_other = pp.investment(
        tec = ['meth_t_d', 'eth_t_d', 'meth_exp', 'meth_imp'],
        trade_tec = ['eth_shipped_exp', 'eth_shipped_imp'],
        units=units_energy,
    )

    vars["Liquids|Oil"] = pp.investment(
        tec=["ref_lol", "ref_hil"], units=units_energy
    ) + pp.tic_fom(tec=["ref_hil", "ref_lol"], units=units_energy)

    vars["Liquids"] = (
        vars["Liquids|Coal and Gas"]
        + vars["Liquids|Biomass"]
        + vars["Liquids|Oil"]
        + _Synfuel_other
    )

    # --------
    # Hydrogen
    # --------

    vars["Hydrogen|Fossil"] = (
        pp.investment(tec=["h2_coal", "h2_smr"], units=units_energy)
        + pp.investment(tec=["h2_coal_ccs"], units=units_energy) * 0.97
        + pp.investment(tec=["h2_smr_ccs"], units=units_energy) * 0.83
        + _Coal_synf_ccs_h2
        + _Gas_synf_ccs_h2
    )

    vars["Hydrogen|Renewable"] = (
        pp.investment(tec=["h2_bio"], units=units_energy)
        + pp.investment(tec=["h2_bio_ccs"], units=units_energy) * 0.98
        + _Bio_synf_ccs_h2
    )

    vars["Hydrogen|Other"] = (
        pp.investment(
            tec = ['h2_elec', 'h2_liq', 'h2_t_d',
                   'lh2_bal', 'lh2_regas', 'lh2_t_d'],
            trade_tec = ['lh2_shipped_exp', 'lh2_shipped_imp'],
            units=units_energy,
        )
        + pp.act_vom(tec=["h2_mix"], units=units_energy) * 0.5
    )

    # -----
    # Other
    # -----

    # All questionable variables from extraction that are not related directly
    # to extraction should be moved to Other
    # Note OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under "Liquids|Oil"

    vars["Other|Liquids|Oil|Transmission and Distribution"] = pp.investment(
        tec=["foil_t_d", "loil_t_d"], units=units_energy
    )

    vars["Other|Liquids|Oil|Other"] = pp.investment(
        tec = ['loil_std', 'oil_bal'],
        trade_tec = ['foil_shipped_exp', 'foil_shipped_imp',
                     'foil_piped_exp', 'foil_piped_imp',
                     'loil_shipped_exp', 'loil_shipped_imp',
                     'loil_piped_exp', 'loil_piped_imp',
                     'crudeoil_shipped_exp', 'crudeoil_shipped_imp',
                     'crudeoil_piped_exp', 'crudeoil_piped_imp'],
        units=units_energy,
    )

    vars["Other|Gases|Transmission and Distribution"] = pp.investment(
        tec=["gas_t_d", "gas_t_d_ch4"], units=units_energy
    )

    vars["Other|Gases|Production"] = pp.investment(
        tec=["gas_bio", "coal_gas"], units=units_energy
    )

    vars["Other|Gases|Other"] = pp.investment(
        tec = ['LNG_bal', 'LNG_prod', 'LNG_regas',
               'gas_bal', 'gas_std', 'gas_sto'],
        trade_tec = ['LNG_shipped_exp', 'LNG_shipped_imp',
                     'gas_piped_exp', 'gas_piped_imp'],
        units=units_energy,
    )

    vars["Other|Solids|Coal|Transmission and Distribution"] = (
        pp.investment(
            tec=[
                "coal_t_d",
            ],
            units=units_energy,
        )
        + pp.act_vom(
            tec=[
                "coal_t_d",
            ],
            units=units_energy,
        )
        * 0.5
    )

    vars["Other|Solids|Coal|Other"] = pp.investment(
        tec = ['coal_bal', 'coal_std'],
        trade_tec = ['coal_shipped_exp', 'coal_shipped_imp'],
        units=units_energy
    )

    vars["Other|Solids|Biomass|Transmission and Distribution"] = pp.investment(
        tec=["biomass_t_d"], units=units_energy
    )

    vars["Other|Other"] = pp_utils._make_zero()

    df = pp_utils.make_outputdf(vars, units_energy)
    return df


@_register
def retr_water_use(units, method):
    """Water: Withdrawal or Consumption.

    Parameters
    ----------
    units : str
        Units to which variables should be converted.
    method : str
        `withdrawal` will calculate the water withdrawn, while
        `consumption` will calculate the `withdrawal` and subtrace the
        water quantities returned.
    """

    vars = {}

    if run_history != "True":
        group = ["Region", "Mode", "Vintage"]
    else:
        group = ["Region"]

    #    # --------------------------------
    #    # Calculation of helping variables
    #    # --------------------------------
    #
    #    _Cogen = pp.inp("po_turbine", inpfilter={"commodity": ["electr"]})
    #
    #    _Potential = pp.act_rel(
    #        [
    #            "coal_ppl_u",
    #            "coal_ppl",
    #            "coal_adv",
    #            "coal_adv_ccs",
    #            "foil_ppl",
    #            "loil_ppl",
    #            "loil_cc",
    #            "gas_ppl",
    #            "gas_ct",
    #            "gas_cc",
    #            "gas_cc_ccs",
    #            "bio_ppl",
    #            "bio_istig",
    #            "bio_istig_ccs",
    #            "igcc",
    #            "igcc_ccs",
    #            "nuc_lc",
    #            "nuc_hc",
    #            "geo_ppl",
    #        ],
    #        relfilter={"relation": ["pass_out_trb"]},
    #    )
    #
    #    _Biogas = pp.out("gas_bio")
    #
    #    _gas_inp_tecs = [
    #        "gas_ppl",
    #        "gas_cc",
    #        "gas_cc_ccs",
    #        "gas_ct",
    #        "gas_hpl",
    #        "meth_ng",
    #        "meth_ng_ccs",
    #        "h2_smr",
    #        "h2_smr_ccs",
    #        "gas_t_d",
    #        "gas_t_d_ch4",
    #    ]
    #
    #    _totgas = pp.inp(_gas_inp_tecs, inpfilter={"commodity": ["gas"]})
    #    _Frac = (_Cogen / _Potential).fillna(0)
    #    _BGas_share = (_Biogas / _totgas).fillna(0)
    #
    #    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    #    _gas_cc_shr = (pp.out("gas_cc") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)
    #
    #    _gas_ppl_shr = (pp.out("gas_ppl") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)
    #
    #    inpfilter = {"level": ["water_supply"], "commodity": ["freshwater"]}
    #    outfilter = {"level": ["secondary"], "commodity": ["electr"]}
    #    emiffilter = {"emission": ["fresh_return"]}
    #
    #    # --------------------
    #    # Once Through Cooling
    #    # --------------------
    #
    #    _gas_ot_fresh_wo_CCS = pp.inp(
    #        ["gas_cc__ot_fresh", "gas_ppl__ot_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _gas_ot_fresh_w_CCS = pp.inp(["gas_cc_ccs__ot_fresh"], units, inpfilter=inpfilter)
    #
    #    _coal_ot_fresh_wo_CCS = pp.inp(
    #        [
    #            "coal_adv__ot_fresh",
    #            "coal_ppl__ot_fresh",
    #            "coal_ppl_u__ot_fresh",
    #            "igcc__ot_fresh",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _coal_ot_fresh_w_CCS = pp.inp(
    #        ["coal_adv_ccs__ot_fresh", "igcc_ccs__ot_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _oil_ot_fresh_wo_CCS = pp.inp(
    #        ["foil_ppl__ot_fresh", "loil_cc__ot_fresh", "loil_ppl__ot_fresh"],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _bio_ot_fresh_wo_CCS = pp.inp(
    #        ["bio_istig__ot_fresh", "bio_ppl__ot_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _bio_ot_fresh_w_CCS = pp.inp(
    #        ["bio_istig_ccs__ot_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _geo_ot_fresh_wo_CCS = pp.inp(["geo_ppl__ot_fresh"], units, inpfilter=inpfilter)
    #
    #    _nuc_ot_fresh_wo_CCS = pp.inp(
    #        ["nuc_hc__ot_fresh", "nuc_lc__ot_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _solar_ot_fresh_wo_CCS = pp.inp(
    #        [
    #            "csp_sm1_res__ot_fresh",
    #            "csp_sm1_res1__ot_fresh",
    #            "csp_sm1_res2__ot_fresh",
    #            "csp_sm1_res3__ot_fresh",
    #            "csp_sm1_res4__ot_fresh",
    #            "csp_sm1_res5__ot_fresh",
    #            "csp_sm1_res5__cl_fresh",
    #            "csp_sm1_res6__ot_fresh",
    #            "csp_sm1_res7__ot_fresh",
    #            "csp_sm3_res__ot_fresh",
    #            "csp_sm3_res1__ot_fresh",
    #            "csp_sm3_res2__ot_fresh",
    #            "csp_sm3_res3__ot_fresh",
    #            "csp_sm3_res4__ot_fresh",
    #            "csp_sm3_res5__ot_fresh",
    #            "csp_sm3_res6__ot_fresh",
    #            "csp_sm3_res7__ot_fresh",
    #            "csp_sm1_res_hist_2010__ot_fresh",
    #            "csp_sm1_res_hist_2015__ot_fresh",
    #            "csp_sm1_res_hist_2020__ot_fresh",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    if method == "consumption":
    #        _gas_ot_fresh_wo_CCS -= pp.act_emif(
    #            ["gas_cc__ot_fresh", "gas_ppl__ot_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _gas_ot_fresh_w_CCS -= pp.act_emif(
    #            ["gas_cc_ccs__ot_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _coal_ot_fresh_wo_CCS -= pp.act_emif(
    #            [
    #                "coal_adv__ot_fresh",
    #                "coal_ppl__ot_fresh",
    #                "coal_ppl_u__ot_fresh",
    #                "igcc__ot_fresh",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _coal_ot_fresh_w_CCS -= pp.act_emif(
    #            ["coal_adv_ccs__ot_fresh", "igcc_ccs__ot_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _oil_ot_fresh_wo_CCS -= pp.act_emif(
    #            ["foil_ppl__ot_fresh", "loil_cc__ot_fresh", "loil_ppl__ot_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _bio_ot_fresh_wo_CCS -= pp.act_emif(
    #            ["bio_istig__ot_fresh", "bio_ppl__ot_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _bio_ot_fresh_w_CCS -= pp.act_emif(
    #            ["bio_istig_ccs__ot_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _geo_ot_fresh_wo_CCS -= pp.act_emif(
    #            ["geo_ppl__ot_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _nuc_ot_fresh_wo_CCS -= pp.act_emif(
    #            ["nuc_hc__ot_fresh", "nuc_lc__ot_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _solar_ot_fresh_wo_CCS -= pp.act_emif(
    #            [
    #                "csp_sm1_res__ot_fresh",
    #                "csp_sm1_res1__ot_fresh",
    #                "csp_sm1_res2__ot_fresh",
    #                "csp_sm1_res3__ot_fresh",
    #                "csp_sm1_res4__ot_fresh",
    #                "csp_sm1_res5__ot_fresh",
    #                "csp_sm1_res5__cl_fresh",
    #                "csp_sm1_res6__ot_fresh",
    #                "csp_sm1_res7__ot_fresh",
    #                "csp_sm3_res__ot_fresh",
    #                "csp_sm3_res1__ot_fresh",
    #                "csp_sm3_res2__ot_fresh",
    #                "csp_sm3_res3__ot_fresh",
    #                "csp_sm3_res4__ot_fresh",
    #                "csp_sm3_res5__ot_fresh",
    #                "csp_sm3_res6__ot_fresh",
    #                "csp_sm3_res7__ot_fresh",
    #                "csp_sm1_res_hist_2010__ot_fresh",
    #                "csp_sm1_res_hist_2015__ot_fresh",
    #                "csp_sm1_res_hist_2020__ot_fresh",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #    # -----------
    #    # Closed Loop
    #    # -----------
    #
    #    _gas_cl_fresh_wo_CCS = pp.inp(
    #        ["gas_cc__cl_fresh", "gas_ppl__cl_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _gas_cl_fresh_w_CCS = pp.inp(["gas_cc_ccs__cl_fresh"], units, inpfilter=inpfilter)
    #
    #    _coal_cl_fresh_wo_CCS = pp.inp(
    #        [
    #            "coal_adv__cl_fresh",
    #            "coal_ppl__cl_fresh",
    #            "coal_ppl_u__cl_fresh",
    #            "igcc__cl_fresh",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _coal_cl_fresh_w_CCS = pp.inp(
    #        ["coal_adv_ccs__cl_fresh", "igcc_ccs__cl_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _oil_cl_fresh_wo_CCS = pp.inp(
    #        ["foil_ppl__cl_fresh", "loil_cc__cl_fresh", "loil_ppl__cl_fresh"],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _bio_cl_fresh_wo_CCS = pp.inp(
    #        ["bio_istig__cl_fresh", "bio_ppl__cl_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _bio_cl_fresh_w_CCS = pp.inp(
    #        ["bio_istig_ccs__cl_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _geo_cl_fresh_wo_CCS = pp.inp(["geo_ppl__cl_fresh"], units, inpfilter=inpfilter)
    #
    #    _nuc_cl_fresh_wo_CCS = pp.inp(
    #        ["nuc_hc__cl_fresh", "nuc_lc__cl_fresh"], units, inpfilter=inpfilter
    #    )
    #
    #    _solar_cl_fresh_wo_CCS = pp.inp(
    #        [
    #            "csp_sm1_res__cl_fresh",
    #            "csp_sm1_res1__cl_fresh",
    #            "csp_sm1_res2__cl_fresh",
    #            "csp_sm1_res3__cl_fresh",
    #            "csp_sm1_res4__cl_fresh",
    #            "csp_sm1_res5__cl_fresh",
    #            "csp_sm1_res5__cl_fresh",
    #            "csp_sm1_res6__cl_fresh",
    #            "csp_sm1_res7__cl_fresh",
    #            "csp_sm3_res__cl_fresh",
    #            "csp_sm3_res1__cl_fresh",
    #            "csp_sm3_res2__cl_fresh",
    #            "csp_sm3_res3__cl_fresh",
    #            "csp_sm3_res4__cl_fresh",
    #            "csp_sm3_res5__cl_fresh",
    #            "csp_sm3_res6__cl_fresh",
    #            "csp_sm3_res7__cl_fresh",
    #            "csp_sm1_res_hist_2010__cl_fresh",
    #            "csp_sm1_res_hist_2015__cl_fresh",
    #            "csp_sm1_res_hist_2020__cl_fresh",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    if method == "consumption":
    #        _gas_cl_fresh_wo_CCS -= pp.act_emif(
    #            ["gas_cc__cl_fresh", "gas_ppl__cl_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _gas_cl_fresh_w_CCS -= pp.act_emif(
    #            ["gas_cc_ccs__cl_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _coal_cl_fresh_wo_CCS -= pp.act_emif(
    #            [
    #                "coal_adv__cl_fresh",
    #                "coal_ppl__cl_fresh",
    #                "coal_ppl_u__cl_fresh",
    #                "igcc__cl_fresh",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _coal_cl_fresh_w_CCS -= pp.act_emif(
    #            ["coal_adv_ccs__cl_fresh", "igcc_ccs__cl_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _oil_cl_fresh_wo_CCS -= pp.act_emif(
    #            ["foil_ppl__cl_fresh", "loil_cc__cl_fresh", "loil_ppl__cl_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _bio_cl_fresh_wo_CCS -= pp.act_emif(
    #            ["bio_istig__cl_fresh", "bio_ppl__cl_fresh"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _bio_cl_fresh_w_CCS -= pp.act_emif(
    #            ["bio_istig_ccs__cl_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _geo_cl_fresh_wo_CCS -= pp.act_emif(
    #            ["geo_ppl__cl_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _nuc_cl_fresh_wo_CCS -= pp.act_emif(
    #            ["nuc_hc__cl_fresh", "nuc_lc__cl_fresh"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _solar_cl_fresh_wo_CCS -= pp.act_emif(
    #            [
    #                "csp_sm1_res__cl_fresh",
    #                "csp_sm1_res1__cl_fresh",
    #                "csp_sm1_res2__cl_fresh",
    #                "csp_sm1_res3__cl_fresh",
    #                "csp_sm1_res4__cl_fresh",
    #                "csp_sm1_res5__cl_fresh",
    #                "csp_sm1_res5__cl_fresh",
    #                "csp_sm1_res6__cl_fresh",
    #                "csp_sm1_res7__cl_fresh",
    #                "csp_sm3_res__cl_fresh",
    #                "csp_sm3_res1__cl_fresh",
    #                "csp_sm3_res2__cl_fresh",
    #                "csp_sm3_res3__cl_fresh",
    #                "csp_sm3_res4__cl_fresh",
    #                "csp_sm3_res5__cl_fresh",
    #                "csp_sm3_res6__cl_fresh",
    #                "csp_sm3_res7__cl_fresh",
    #                "csp_sm1_res_hist_2010__cl_fresh",
    #                "csp_sm1_res_hist_2015__cl_fresh",
    #                "csp_sm1_res_hist_2020__cl_fresh",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #    # -----------------------------
    #    # Once Through Cooling (SALINE)
    #    # -----------------------------
    #    sinpfilter = {"level": ["saline_supply"], "commodity": ["saline_ppl"]}
    #
    #    _gas_ot_saline_wo_CCS = pp.inp(
    #        ["gas_cc__ot_saline", "gas_ppl__ot_saline"], units, inpfilter=sinpfilter
    #    )
    #
    #    _gas_ot_saline_w_CCS = pp.inp(
    #        ["gas_cc_ccs__ot_saline"], units, inpfilter=sinpfilter
    #    )
    #
    #    _coal_ot_saline_wo_CCS = pp.inp(
    #        [
    #            "coal_adv__ot_saline",
    #            "coal_ppl__ot_saline",
    #            "coal_ppl_u__ot_saline",
    #            "igcc__ot_saline",
    #        ],
    #        units,
    #        inpfilter=sinpfilter,
    #    )
    #
    #    _coal_ot_saline_w_CCS = pp.inp(
    #        ["coal_adv_ccs__ot_saline", "igcc_ccs__ot_saline"], units, inpfilter=sinpfilter
    #    )
    #
    #    _oil_ot_saline_wo_CCS = pp.inp(
    #        ["foil_ppl__ot_saline", "loil_cc__ot_saline", "loil_ppl__ot_saline"],
    #        units,
    #        inpfilter=sinpfilter,
    #    )
    #
    #    _bio_ot_saline_wo_CCS = pp.inp(
    #        ["bio_istig__ot_saline", "bio_ppl__ot_saline"], units, inpfilter=sinpfilter
    #    )
    #
    #    _bio_ot_saline_w_CCS = pp.inp(
    #        ["bio_istig_ccs__ot_saline"], units, inpfilter=sinpfilter
    #    )
    #
    #    _geo_ot_saline_wo_CCS = pp.inp(["geo_ppl__ot_saline"], units, inpfilter=sinpfilter)
    #
    #    _nuc_ot_saline_wo_CCS = pp.inp(
    #        ["nuc_hc__ot_saline", "nuc_lc__ot_saline"], units, inpfilter=sinpfilter
    #    )
    #
    #    _solar_ot_saline_wo_CCS = pp.inp(
    #        [
    #            "csp_sm1_res__ot_saline",
    #            "csp_sm1_res1__ot_saline",
    #            "csp_sm1_res2__ot_saline",
    #            "csp_sm1_res3__ot_saline",
    #            "csp_sm1_res4__ot_saline",
    #            "csp_sm1_res5__ot_saline",
    #            "csp_sm1_res5__ot_saline",
    #            "csp_sm1_res6__ot_saline",
    #            "csp_sm1_res7__ot_saline",
    #            "csp_sm3_res__ot_saline",
    #            "csp_sm3_res1__ot_saline",
    #            "csp_sm3_res2__ot_saline",
    #            "csp_sm3_res3__ot_saline",
    #            "csp_sm3_res4__ot_saline",
    #            "csp_sm3_res5__ot_saline",
    #            "csp_sm3_res6__ot_saline",
    #            "csp_sm3_res7__ot_saline",
    #            "csp_sm1_res_hist_2010__ot_saline",
    #            "csp_sm1_res_hist_2015__ot_saline",
    #            "csp_sm1_res_hist_2020__ot_saline",
    #        ],
    #        units,
    #        inpfilter=sinpfilter,
    #    )
    #
    #    if method == "consumption":
    #        _gas_ot_saline_wo_CCS -= pp.act_emif(
    #            ["gas_cc__ot_saline", "gas_ppl__ot_saline"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _gas_ot_saline_w_CCS -= pp.act_emif(
    #            ["gas_cc_ccs__ot_saline"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _coal_ot_saline_wo_CCS -= pp.act_emif(
    #            [
    #                "coal_adv__ot_saline",
    #                "coal_ppl__ot_saline",
    #                "coal_ppl_u__ot_saline",
    #                "igcc__ot_saline",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _coal_ot_saline_w_CCS -= pp.act_emif(
    #            ["coal_adv_ccs__ot_saline", "igcc_ccs__ot_saline"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _oil_ot_saline_wo_CCS -= pp.act_emif(
    #            ["foil_ppl__ot_saline", "loil_cc__ot_saline", "loil_ppl__ot_saline"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _bio_ot_saline_wo_CCS -= pp.act_emif(
    #            ["bio_istig__ot_saline", "bio_ppl__ot_saline"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _bio_ot_saline_w_CCS -= pp.act_emif(
    #            ["bio_istig_ccs__ot_saline"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _geo_ot_saline_wo_CCS -= pp.act_emif(
    #            ["geo_ppl__ot_saline"], units=units, emiffilter=emiffilter
    #        )
    #
    #        _nuc_ot_saline_wo_CCS -= pp.act_emif(
    #            ["nuc_hc__ot_saline", "nuc_lc__ot_saline"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        _solar_ot_saline_wo_CCS -= pp.act_emif(
    #            [
    #                "csp_sm1_res__ot_saline",
    #                "csp_sm1_res1__ot_saline",
    #                "csp_sm1_res2__ot_saline",
    #                "csp_sm1_res3__ot_saline",
    #                "csp_sm1_res4__ot_saline",
    #                "csp_sm1_res5__ot_saline",
    #                "csp_sm1_res5__ot_saline",
    #                "csp_sm1_res6__ot_saline",
    #                "csp_sm1_res7__ot_saline",
    #                "csp_sm3_res__ot_saline",
    #                "csp_sm3_res1__ot_saline",
    #                "csp_sm3_res2__ot_saline",
    #                "csp_sm3_res3__ot_saline",
    #                "csp_sm3_res4__ot_saline",
    #                "csp_sm3_res5__ot_saline",
    #                "csp_sm3_res6__ot_saline",
    #                "csp_sm3_res7__ot_saline",
    #                "csp_sm1_res_hist_2010__ot_saline",
    #                "csp_sm1_res_hist_2015__ot_saline",
    #                "csp_sm1_res_hist_2020__ot_saline",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #    # -----------
    #    # Dry cooling
    #    # -----------
    #
    #    _gas_air = pp.inp(["gas_cc__air", "gas_ppl__air"], units, inpfilter=inpfilter)
    #
    #    _coal_air = pp.inp(
    #        ["coal_adv__air", "coal_ppl__air", "coal_ppl_u__air", "igcc__air"],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _oil_air = pp.inp(
    #        ["foil_ppl__air", "loil_cc__air", "loil_ppl__air"], units, inpfilter=inpfilter
    #    )
    #
    #    _bio_air = pp.inp(["bio_istig__air", "bio_ppl__air"], units, inpfilter=inpfilter)
    #
    #    _geo_air = pp.inp(["geo_ppl__air"], units, inpfilter=inpfilter)
    #    _solar_air = pp.inp(
    #        [
    #            "csp_sm1_res__air",
    #            "csp_sm1_res1__air",
    #            "csp_sm1_res2__air",
    #            "csp_sm1_res3__air",
    #            "csp_sm1_res4__air",
    #            "csp_sm1_res5__air",
    #            "csp_sm1_res5__air",
    #            "csp_sm1_res6__air",
    #            "csp_sm1_res7__air",
    #            "csp_sm3_res__air",
    #            "csp_sm3_res1__air",
    #            "csp_sm3_res2__air",
    #            "csp_sm3_res3__air",
    #            "csp_sm3_res4__air",
    #            "csp_sm3_res5__air",
    #            "csp_sm3_res6__air",
    #            "csp_sm3_res7__air",
    #            "csp_sm1_res_hist_2010__air",
    #            "csp_sm1_res_hist_2015__air",
    #            "csp_sm1_res_hist_2020__air",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    # -----------------------
    #    # Water use for coal elec
    #    # -----------------------
    #
    #    _Cooling_ele_coal_wCCS = (
    #        _coal_ot_fresh_w_CCS + _coal_cl_fresh_w_CCS + _coal_ot_saline_w_CCS
    #    )
    #
    #    _Direct_ele_coal_wCCS = (
    #        _pe_elec_wCCSretro(
    #            "coal_ppl",
    #            "c_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #        )
    #        + _pe_elec_po_turb(
    #            "coal_adv_ccs",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _pe_elec_po_turb(
    #            "igcc_ccs",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _out_div_eff(
    #            ["meth_coal_ccs", "h2_coal_ccs", "syn_liq_ccs"], group, inpfilter, outfilter
    #        )
    #    )
    #
    #    vars["Electricity|Coal|w/ CCS"] = _Direct_ele_coal_wCCS + _Cooling_ele_coal_wCCS
    #
    #    _Cooling_ele_coal_woCCS = (
    #        _coal_ot_fresh_wo_CCS
    #        + _coal_cl_fresh_wo_CCS
    #        + _coal_ot_saline_wo_CCS
    #        + _coal_air
    #    )
    #
    #    _Direct_ele_coal_woCCS = (
    #        _pe_elec_woCCSretro(
    #            "coal_ppl",
    #            "c_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #        )
    #        + _pe_elec_po_turb(
    #            "coal_adv",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _pe_elec_po_turb(
    #            "coal_ppl_u",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _out_div_eff(["meth_coal", "h2_coal", "syn_liq"], group, inpfilter, outfilter)
    #    )
    #
    #    vars["Electricity|Coal|w/o CCS"] = _Direct_ele_coal_woCCS + _Cooling_ele_coal_woCCS
    #
    #    # -----------------------------
    #    # Water use for electricity Gas
    #    # -----------------------------
    #
    #    _Cooling_ele_gas_wCCS = (
    #        _gas_ot_fresh_w_CCS + _gas_cl_fresh_w_CCS + _gas_ot_saline_w_CCS
    #    )
    #
    #    _Direct_ele_gas_wCCS = (
    #        _pe_elec_wCCSretro(
    #            "gas_cc",
    #            "g_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #            share=_gas_cc_shr,
    #        )
    #        + _pe_elec_wCCSretro(
    #            "gas_ppl",
    #            "g_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #            share=_gas_ppl_shr,
    #        )
    #        + _pe_elec_po_turb(
    #            "gas_cc_ccs",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _out_div_eff(["h2_smr_ccs"], group, inpfilter, outfilter)
    #    )
    #
    #    vars["Electricity|Gas|w/ CCS"] = (_Direct_ele_gas_wCCS + _Cooling_ele_gas_wCCS) * (
    #        1 - _BGas_share
    #    )
    #
    #    _Cooling_ele_gas_woCCS = (
    #        _gas_ot_fresh_wo_CCS + _gas_cl_fresh_wo_CCS + _gas_ot_saline_wo_CCS + _gas_air
    #    )
    #
    #    _Direct_ele_gas_woCCS = (
    #        _pe_elec_woCCSretro(
    #            "gas_ppl",
    #            "g_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #            share=_gas_ppl_shr,
    #        )
    #        + _pe_elec_woCCSretro(
    #            "gas_cc",
    #            "g_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #            share=_gas_cc_shr,
    #        )
    #        + _pe_elec_po_turb(
    #            "gas_ct",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _out_div_eff(["h2_smr"], group, inpfilter, outfilter)
    #    )
    #
    #    vars["Electricity|Gas|w/o CCS"] = (
    #        _Direct_ele_gas_woCCS + _Cooling_ele_gas_woCCS
    #    ) * (1 - _BGas_share)
    #
    #    # -----------------------------
    #    # Water use for electricity bio
    #    # -----------------------------
    #
    #    _Cooling_ele_bio_wCCS = (
    #        _bio_ot_fresh_w_CCS + _bio_cl_fresh_w_CCS + _bio_ot_saline_w_CCS
    #    )
    #
    #    _Direct_ele_bio_wCCS = (
    #        _pe_elec_wCCSretro(
    #            "bio_ppl",
    #            "bio_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #        )
    #        + _pe_elec_po_turb(
    #            "bio_istig_ccs",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _out_div_eff(
    #            ["h2_bio_ccs", "eth_bio_ccs", "liq_bio_ccs"], group, inpfilter, outfilter
    #        )
    #    )
    #
    #    vars["Electricity|Biomass|w/ CCS"] = (
    #        _Direct_ele_bio_wCCS
    #        + _Cooling_ele_bio_wCCS
    #        + (_Direct_ele_gas_wCCS + _Cooling_ele_gas_wCCS) * _BGas_share
    #    )
    #
    #    _Cooling_ele_bio_woCCS = (
    #        _bio_ot_fresh_wo_CCS + _bio_cl_fresh_wo_CCS + _bio_ot_saline_wo_CCS + _bio_air
    #    )
    #
    #    _Direct_ele_bio_woCCS = (
    #        _pe_elec_woCCSretro(
    #            "bio_ppl",
    #            "bio_ppl_co2scr",
    #            group,
    #            inpfilter=inpfilter,
    #            units=units,
    #            _Frac=_Frac,
    #        )
    #        + _pe_elec_po_turb(
    #            "bio_istig",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _out_div_eff(["h2_bio", "eth_bio", "liq_bio"], group, inpfilter, outfilter)
    #    )
    #
    #    vars["Electricity|Biomass|w/o CCS"] = (
    #        _Direct_ele_bio_woCCS
    #        + _Cooling_ele_bio_woCCS
    #        + (_Direct_ele_gas_woCCS + _Cooling_ele_gas_woCCS) * _BGas_share
    #    )
    #
    #    # -----------------------------
    #    # Water use for electricity oil
    #    # -----------------------------
    #
    #    _Cooling_ele_oil = (
    #        _oil_ot_fresh_wo_CCS + _oil_cl_fresh_wo_CCS + _oil_ot_saline_wo_CCS + _oil_air
    #    )
    #
    #    _Direct_ele_oil = (
    #        _pe_elec_po_turb(
    #            "foil_ppl",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _pe_elec_po_turb(
    #            "loil_ppl",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _pe_elec_po_turb(
    #            "oil_ppl",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _pe_elec_po_turb(
    #            "loil_cc",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #    )
    #
    #    vars["Electricity|Oil"] = _Direct_ele_oil + _Cooling_ele_oil
    #
    #    # ------------------------------------
    #    # Water use for electricity geothermal
    #    # ------------------------------------
    #
    #    _Cooling_ele_geo = (
    #        _geo_ot_fresh_wo_CCS + _geo_cl_fresh_wo_CCS + _geo_ot_saline_wo_CCS + _geo_air
    #    )
    #
    #    _Direct_ele_geo = _pe_elec_po_turb(
    #        "geo_ppl",
    #        group,
    #        units,
    #        _Frac,
    #        inpfilter=inpfilter,
    #        outfilter={"commodity": "electr"},
    #    )
    #
    #    vars["Electricity|Geothermal"] = _Direct_ele_geo + _Cooling_ele_geo
    #
    #    # ------------------------------------
    #    # Water use for electricity hydropower
    #    # ------------------------------------
    #
    #    # Note Wenji 03.05.2017: Hydropower uses a different commodity than the
    #    # other technologies.
    #
    #    vars["Electricity|Hydro"] = pp.inp(
    #        ["hydro_lc", "hydro_hc"],
    #        units,
    #        inpfilter={"level": ["water_supply"], "commodity": ["freshwater_instream"]},
    #    )
    #
    #    # ---------------------------------
    #    # Water use for electricity nuclear
    #    # ---------------------------------
    #
    #    _Cooling_ele_nuc = (
    #        _nuc_ot_fresh_wo_CCS + _nuc_cl_fresh_wo_CCS + _nuc_ot_saline_wo_CCS
    #    )
    #
    #    _Direct_ele_nuc = (
    #        _pe_elec_po_turb(
    #            "nuc_lc",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _pe_elec_po_turb(
    #            "nuc_hc",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #        + _pe_elec_po_turb(
    #            "nuc_fbr",
    #            group,
    #            units,
    #            _Frac,
    #            inpfilter=inpfilter,
    #            outfilter={"commodity": "electr"},
    #        )
    #    )
    #
    #    vars["Electricity|Nuclear"] = _Direct_ele_nuc + _Cooling_ele_nuc
    #
    #    # ------------------------------
    #    # Water use for electricity wind
    #    # ------------------------------
    #
    #    # Note Wenji 02.05.2017: In the GDX file it seems that these technologies
    #    # have no input
    #
    #    Wind_onshore = pp.inp(
    #        [
    #            "wind_res1",
    #            "wind_res2",
    #            "wind_res3",
    #            "wind_res4",
    #            "wind_res_hist_2000",
    #            "wind_res_hist_2005",
    #            "wind_res_hist_2010",
    #            "wind_res_hist_2015",
    #            "wind_res_hist_2020",
    #            "wind_res_hist_2025",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    Wind_offshore = pp.inp(
    #        [
    #            "wind_ref1",
    #            "wind_ref2",
    #            "wind_ref3",
    #            "wind_ref4",
    #            "wind_ref5",
    #            "wind_ref_hist_2000",
    #            "wind_ref_hist_2005",
    #            "wind_ref_hist_2010",
    #            "wind_ref_hist_2015",
    #            "wind_ref_hist_2020",
    #            "wind_ref_hist_2025",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    vars["Electricity|Wind"] = Wind_onshore + Wind_offshore
    #
    #    # -------------------------------
    #    # Water use for electricity solar
    #    # -------------------------------
    #
    #    _Cooling_ele_solar_th = (
    #        _solar_ot_fresh_wo_CCS
    #        + _solar_cl_fresh_wo_CCS
    #        + _solar_ot_saline_wo_CCS
    #        + _solar_air
    #    )
    #
    #    _Direct_ele_solar_th = pp.inp(
    #        [
    #            "csp_sm1_res",
    #            "csp_sm1_res1",
    #            "csp_sm1_res2",
    #            "csp_sm1_res3",
    #            "csp_sm1_res4",
    #            "csp_sm1_res5",
    #            "csp_sm1_res6",
    #            "csp_sm1_res7",
    #            "csp_sm1_res_hist_2000",
    #            "csp_sm1_res_hist_2005",
    #            "csp_sm1_res_hist_2010",
    #            "csp_sm1_res_hist_2015",
    #            "csp_sm1_res_hist_2020",
    #            "csp_sm1_res_hist_2025",
    #            "csp_sm3_res",
    #            "csp_sm3_res1",
    #            "csp_sm3_res2",
    #            "csp_sm3_res3",
    #            "csp_sm3_res4",
    #            "csp_sm3_res5",
    #            "csp_sm3_res6",
    #            "csp_sm3_res7",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    vars["Electricity|Solar|CSP"] = _Direct_ele_solar_th + _Cooling_ele_solar_th
    #
    #    # Note Wenji 02.05.2017: In the GDX file it seems that these technologies
    #    # have no input
    #
    #    vars["Electricity|Solar|PV"] = pp.inp(
    #        [
    #            "solar_res1",
    #            "solar_res2",
    #            "solar_res3",
    #            "solar_res4",
    #            "solar_res5",
    #            "solar_res6",
    #            "solar_res7",
    #            "solar_res8",
    #            "solar_pv_I",
    #            "solar_pv_RC",
    #            "solar_res_hist_2000",
    #            "solar_res_hist_2005",
    #            "solar_res_hist_2010",
    #            "solar_res_hist_2015",
    #            "solar_res_hist_2020",
    #            "solar_res_hist_2025",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    vars["Electricity|Other"] = pp.inp(
    #        ["h2_fc_trp", "h2_fc_I", "h2_fc_RC"], units, inpfilter=inpfilter
    #    )
    #
    #    vars["Electricity|Dry Cooling"] = (
    #        _gas_air + _coal_air + _oil_air + _bio_air + _geo_air + _solar_air
    #    )
    #
    #    vars["Electricity|Once Through"] = (
    #        _gas_ot_fresh_wo_CCS
    #        + _gas_ot_fresh_w_CCS
    #        + _coal_ot_fresh_wo_CCS
    #        + _coal_ot_fresh_w_CCS
    #        + _oil_ot_fresh_wo_CCS
    #        + _bio_ot_fresh_wo_CCS
    #        + _bio_ot_fresh_w_CCS
    #        + _geo_ot_fresh_wo_CCS
    #        + _nuc_ot_fresh_wo_CCS
    #        + _solar_ot_fresh_wo_CCS
    #    )
    #
    #    vars["Electricity|Sea Cooling"] = (
    #        _gas_ot_saline_wo_CCS
    #        + _gas_ot_saline_w_CCS
    #        + _coal_ot_saline_wo_CCS
    #        + _coal_ot_saline_w_CCS
    #        + _oil_ot_saline_wo_CCS
    #        + _bio_ot_saline_wo_CCS
    #        + _bio_ot_saline_w_CCS
    #        + _geo_ot_saline_wo_CCS
    #        + _nuc_ot_saline_wo_CCS
    #        + _solar_ot_saline_wo_CCS
    #    )
    #
    #    vars["Electricity|Wet Tower"] = (
    #        _gas_cl_fresh_wo_CCS
    #        + _gas_cl_fresh_w_CCS
    #        + _coal_cl_fresh_wo_CCS
    #        + _coal_cl_fresh_w_CCS
    #        + _oil_cl_fresh_wo_CCS
    #        + _bio_cl_fresh_wo_CCS
    #        + _bio_cl_fresh_w_CCS
    #        + _geo_cl_fresh_wo_CCS
    #        + _nuc_cl_fresh_wo_CCS
    #        + _solar_cl_fresh_wo_CCS
    #    )
    #
    #    # ----------
    #    # Extraction
    #    # ----------
    #
    #    vars["Extraction|Coal"] = pp.inp(
    #        ["coal_extr", "coal_extr_ch4", "lignite_extr"], units, inpfilter=inpfilter
    #    )
    #
    #    vars["Extraction|Gas"] = pp.inp(
    #        [
    #            "gas_extr_1",
    #            "gas_extr_2",
    #            "gas_extr_3",
    #            "gas_extr_4",
    #            "gas_extr_5",
    #            "gas_extr_6",
    #            "gas_extr_7",
    #            "gas_extr_8",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    vars["Extraction|Oil"] = pp.inp(
    #        [
    #            "oil_extr_1",
    #            "oil_extr_2",
    #            "oil_extr_3",
    #            "oil_extr_4",
    #            "oil_extr_5",
    #            "oil_extr_6",
    #            "oil_extr_7",
    #            "oil_extr_8",
    #            "oil_extr_1_ch4",
    #            "oil_extr_2_ch4",
    #            "oil_extr_3_ch4",
    #            "oil_extr_4_ch4",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    vars["Extraction|Uranium"] = pp.inp("Uran_extr", units, inpfilter=inpfilter)
    #
    #    if method == "consumption":
    #        vars["Extraction|Coal"] -= pp.act_emif(
    #            ["coal_extr", "coal_extr_ch4", "lignite_extr"],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        vars["Extraction|Gas"] -= pp.act_emif(
    #            [
    #                "gas_extr_1",
    #                "gas_extr_2",
    #                "gas_extr_3",
    #                "gas_extr_4",
    #                "gas_extr_5",
    #                "gas_extr_6",
    #                "gas_extr_7",
    #                "gas_extr_8",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        vars["Extraction|Oil"] -= pp.act_emif(
    #            [
    #                "oil_extr_1",
    #                "oil_extr_2",
    #                "oil_extr_3",
    #                "oil_extr_4",
    #                "oil_extr_5",
    #                "oil_extr_6",
    #                "oil_extr_7",
    #                "oil_extr_8",
    #                "oil_extr_1_ch4",
    #                "oil_extr_2_ch4",
    #                "oil_extr_3_ch4",
    #                "oil_extr_4_ch4",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        vars["Extraction|Uranium"] -= pp.act_emif(
    #            "Uran_extr", units=units, emiffilter=emiffilter
    #        )
    #
    #    # -----
    #    # Gases
    #    # -----
    #
    #    vars["Gases|Biomass"] = pp.inp("gas_bio", units, inpfilter=inpfilter)
    #
    #    vars["Gases|Coal"] = pp.inp("coal_gas", units, inpfilter=inpfilter)
    #
    #    if method == "consumption":
    #        vars["Gases|Biomass"] -= pp.act_emif(
    #            "gas_bio", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Gases|Coal"] -= pp.act_emif(
    #            "coal_gas", units=units, emiffilter=emiffilter
    #        )
    #
    #    # ----
    #    # Heat
    #    # ----
    #
    #    _Cooling_heat_gas = pp.inp(
    #        [
    #            "gas_hpl__ot_fresh",
    #            "gas_hpl__cl_fresh",
    #            "gas_hpl__ot_saline",
    #            "gas_hpl__air",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _Direct_heat_gas = pp.inp("gas_hpl", units, inpfilter=inpfilter)
    #
    #    vars["Heat|Gas"] = _Cooling_heat_gas + _Direct_heat_gas
    #
    #    _Cooling_heat_geo = pp.inp(
    #        [
    #            "geo_hpl__ot_fresh",
    #            "geo_hpl__cl_fresh",
    #            "geo_hpl__ot_saline",
    #            "geo_hpl__air",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _Direct_heat_geo = pp.inp("geo_hpl", units, inpfilter=inpfilter)
    #
    #    vars["Heat|Geothermal"] = _Cooling_heat_geo + _Direct_heat_geo
    #
    #    _Cooling_heat_bio = pp.inp(
    #        [
    #            "bio_hpl__ot_fresh",
    #            "bio_hpl__cl_fresh",
    #            "bio_hpl__ot_saline",
    #            "bio_hpl__air",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _Direct_heat_bio = pp.inp("bio_hpl", units, inpfilter=inpfilter)
    #
    #    vars["Heat|Biomass"] = _Cooling_heat_bio + _Direct_heat_bio
    #
    #    vars["Heat|Coal"] = pp.inp("coal_hpl", units, inpfilter=inpfilter)
    #
    #    _Cooling_heat_oil = pp.inp(
    #        [
    #            "foil_hpl__ot_fresh",
    #            "foil_hpl__cl_fresh",
    #            "foil_hpl__ot_saline",
    #            "foil_hpl__air",
    #        ],
    #        units,
    #        inpfilter=inpfilter,
    #    )
    #
    #    _Direct_heat_oil = pp.inp("foil_hpl", units, inpfilter=inpfilter)
    #
    #    vars["Heat|Oil"] = _Cooling_heat_oil + _Direct_heat_oil
    #
    #    vars["Heat|Other"] = pp.inp("po_turbine", units, inpfilter=inpfilter)
    #
    #    if method == "consumption":
    #        vars["Heat|Gas"] = vars["Heat|Gas"] - pp.act_emif(
    #            [
    #                "gas_hpl__ot_fresh",
    #                "gas_hpl__cl_fresh",
    #                "gas_hpl__ot_saline",
    #                "gas_hpl__air",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        vars["Heat|Geothermal"] -= pp.act_emif(
    #            [
    #                "geo_hpl__ot_fresh",
    #                "geo_hpl__cl_fresh",
    #                "geo_hpl__ot_saline",
    #                "geo_hpl__air",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        vars["Heat|Biomass"] -= pp.act_emif(
    #            [
    #                "bio_hpl__ot_fresh",
    #                "bio_hpl__cl_fresh",
    #                "bio_hpl__ot_saline",
    #                "bio_hpl__air",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        vars["Heat|Coal"] -= pp.act_emif("coal_hpl", units=units, emiffilter=emiffilter)
    #
    #        vars["Heat|Oil"] -= pp.act_emif(
    #            [
    #                "foil_hpl__ot_fresh",
    #                "foil_hpl__cl_fresh",
    #                "foil_hpl__ot_saline",
    #                "foil_hpl__air",
    #            ],
    #            units=units,
    #            emiffilter=emiffilter,
    #        )
    #
    #        vars["Heat|Other"] -= pp.act_emif(
    #            "po_turbine", units=units, emiffilter=emiffilter
    #        )
    #
    #    # --------
    #    # Hydrogen
    #    # --------
    #
    #    hydrogen_outputfilter = {"level": ["secondary"], "commodity": ["hydrogen"]}
    #
    #    vars["Hydrogen|Biomass|w/ CCS"] = _out_div_eff(
    #        "h2_bio_ccs", group, inpfilter, hydrogen_outputfilter
    #    )
    #
    #    vars["Hydrogen|Biomass|w/o CCS"] = _out_div_eff(
    #        "h2_bio", group, inpfilter, hydrogen_outputfilter
    #    )
    #
    #    vars["Hydrogen|Coal|w/ CCS"] = _out_div_eff(
    #        "h2_coal_ccs", group, inpfilter, hydrogen_outputfilter
    #    )
    #
    #    vars["Hydrogen|Coal|w/o CCS"] = _out_div_eff(
    #        "h2_coal", group, inpfilter, hydrogen_outputfilter
    #    )
    #
    #    vars["Hydrogen|Gas|w/ CCS"] = _out_div_eff(
    #        "h2_smr_ccs", group, inpfilter, hydrogen_outputfilter
    #    )
    #
    #    vars["Hydrogen|Gas|w/o CCS"] = _out_div_eff(
    #        "h2_smr", group, inpfilter, hydrogen_outputfilter
    #    )
    #
    #    vars["Hydrogen|Electricity"] = _out_div_eff(
    #        "h2_elec", group, inpfilter, hydrogen_outputfilter
    #    )
    #
    #    if method == "consumption":
    #        vars["Hydrogen|Biomass|w/ CCS"] -= pp.act_emif(
    #            "h2_bio_ccs", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Hydrogen|Biomass|w/o CCS"] -= pp.act_emif(
    #            "h2_bio", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Hydrogen|Coal|w/ CCS"] -= pp.act_emif(
    #            "h2_coal_ccs", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Hydrogen|Coal|w/o CCS"] -= pp.act_emif(
    #            "h2_coal", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Hydrogen|Gas|w/ CCS"] -= pp.act_emif(
    #            "h2_smr_ccs", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Hydrogen|Gas|w/o CCS"] -= pp.act_emif(
    #            "h2_smr", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Hydrogen|Electricity"] -= pp.act_emif(
    #            "h2_elec", units=units, emiffilter=emiffilter
    #        )

    # --------------------
    # Irrigation water use
    # --------------------

    vars["Irrigation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Water|Withdrawal|Irrigation"],
        }
    )

    vars["Irrigation|Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Water|Withdrawal|Irrigation|Cereals"],
        }  # original unit seems to be km3, no multiplication
    )

    vars["Irrigation|Oilcrops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Water|Withdrawal|Irrigation|Oilcrops"],
        }  # original unit seems to be km3, no multiplication
    )

    vars["Irrigation|Sugarcrops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Water|Withdrawal|Irrigation|Sugarcrops"],
        }  # original unit seems to be km3, no multiplication
    )

    if method == "consumption":
        vars["Irrigation"] = pp_utils._make_zero()
        vars["Irrigation|Cereals"] = pp_utils._make_zero()
        vars["Irrigation|Oilcrops"] = pp_utils._make_zero()
        vars["Irrigation|Sugarcrops"] = pp_utils._make_zero()

    #    # -----------
    #    # Bio ethanol
    #    # -----------
    #
    #    bio_liquid_outfilter = {"level": ["primary"], "commodity": ["ethanol"]}
    #
    #    vars["Liquids|Biomass|w/ CCS"] = _out_div_eff(
    #        ["eth_bio_ccs", "liq_bio_ccs"], group, inpfilter, bio_liquid_outfilter
    #    )
    #
    #    vars["Liquids|Biomass|w/o CCS"] = _out_div_eff(
    #        ["eth_bio", "liq_bio"], group, inpfilter, bio_liquid_outfilter
    #    )
    #
    #    if method == "consumption":
    #        vars["Liquids|Biomass|w/ CCS"] -= pp.act_emif(
    #            ["eth_bio_ccs", "liq_bio_ccs"], units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Liquids|Biomass|w/o CCS"] -= pp.act_emif(
    #            ["eth_bio", "liq_bio"], units=units, emiffilter=emiffilter
    #        )
    #
    #    # ---------------------------
    #    # Synthetic liquids from coal
    #    # ---------------------------
    #
    #    # Note OFR 20210731: The filter below seems to be incorrect and hence was
    #    # corrected.
    #    # syn_liquid_outfilter = {"level": ["primary"],
    #    #                         "commodity": ["methanol"]}
    #
    #    syn_liquid_outfilter = {
    #        "level": ["secondary"],
    #        "commodity": ["methanol", "lightoil"],
    #    }
    #
    #    vars["Liquids|Coal|w/ CCS"] = _out_div_eff(
    #        ["meth_coal_ccs", "syn_liq_ccs"], group, inpfilter, syn_liquid_outfilter
    #    )
    #
    #    vars["Liquids|Coal|w/o CCS"] = _out_div_eff(
    #        ["meth_coal", "syn_liq"], group, inpfilter, syn_liquid_outfilter
    #    )
    #
    #    vars["Liquids|Gas|w/ CCS"] = _out_div_eff(
    #        ["meth_ng_ccs"], group, inpfilter, syn_liquid_outfilter
    #    )
    #
    #    vars["Liquids|Gas|w/o CCS"] = _out_div_eff(
    #        ["meth_ng"], group, inpfilter, syn_liquid_outfilter
    #    )
    #
    #    if method == "consumption":
    #        vars["Liquids|Coal|w/ CCS"] -= pp.act_emif(
    #            ["meth_coal_ccs", "syn_liq_ccs"], units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Liquids|Coal|w/o CCS"] -= pp.act_emif(
    #            ["meth_coal", "syn_liq"], units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Liquids|Gas|w/ CCS"] -= pp.act_emif(
    #            "meth_ng_ccs", units=units, emiffilter=emiffilter
    #        )
    #
    #        vars["Liquids|Gas|w/o CCS"] -= pp.act_emif(
    #            "meth_ng", units=units, emiffilter=emiffilter
    #        )
    #
    #    # ----------
    #    # Refineries
    #    # ----------
    #
    #    loil_filter = {"level": ["secondary"], "commodity": ["lightoil"]}
    #    foil_filter = {"level": ["secondary"], "commodity": ["fueloil"]}
    #
    #    vars["Liquids|Oil"] = _out_div_eff(
    #        ["ref_lol", "ref_hil"], group, inpfilter, loil_filter
    #    ) + _out_div_eff(["ref_lol", "ref_hil"], group, inpfilter, foil_filter)
    #
    #    if method == "consumption":
    #        vars["Liquids|Oil"] -= pp.act_emif(
    #            ["ref_lol", "ref_hil"], units=units, emiffilter=emiffilter
    #        )

    df = pp_utils.make_outputdf(vars, units)

    # Identify columns that contain numeric data (years)
    numeric_cols = df.select_dtypes(include=["number"]).columns
    # convert non-irrigation water from MCM to km3
    df.loc[
        ~df["Variable"].str.contains(r"Water Withdrawal\|Irrigation", regex=True),
        numeric_cols,
    ] /= 1000

    return df


@_register
def retr_GAINS():
    dfs = []

    vars = {}

    vars["AGR_ARABLE"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|AGR_ARABLE"],
        }
    )

    vars["FOREST"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|FOREST"],
        }
    )

    vars["GRASSLAND"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|GRASSLAND"],
        }
    )

    vars["RICE"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|RICE"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, "million ha"))

    vars = {}

    vars["AGR_BEEF"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|AGR_BEEF"],
        }
    )

    vars["AGR_COWS"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|AGR_COWS"],
        }
    )

    vars["AGR_OTANI"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|AGR_OTANI"],
        }
    )

    vars["AGR_PIG"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|AGR_PIG"],
        }
    )

    vars["AGR_POULT"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|AGR_POULT"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, "M animal"))

    vars = {}

    vars["FCON_Total_Nfertilizer"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Activity for GAINS|FCON_Total_Nfertilizer"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, "kt N"))

    df = pd.concat(dfs, sort=True)
    return df


@_register
def retr_food_availability(units):
    """Landuse: Food demand.

    Land-use related food demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Total"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Availability [per capita]"],
        }
    )

    vars["Crops [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Availability|Crops [per capita]"],
        }
    )

    vars["Crops|Cereals [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Availability|Crops|Cereals [per capita]"],
        }
    )

    vars["Crops|Oil Crops [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Availability|Crops|Oil Crops [per capita]"],
        }
    )

    vars["Crops|Sugar Crops [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Availability|Crops|Sugar Crops [per capita]"],
        }
    )

    vars["Livestock [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Availability|Livestock [per capita]"],
        }
    )

    # This variable is only required for deriving the global value via weighted
    # average.
    pop = pp.act("Population")
    df = pp_utils.make_outputdf(vars, units, param="weighted_avg", weighted_by=pop)
    return df


@_register
def retr_food_intake(units):
    """Landuse: Food demand.

    Land-use related food demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Total"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Intake [per capita]"],
        }
    )

    vars["Crops [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Intake|Crops [per capita]"],
        }
    )

    vars["Livestock [per capita]"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Food Intake|Livestock [per capita]"],
        }
    )

    # This variable is only required for deriving the global value via weighted
    # average.
    pop = pp.act("Population")
    df = pp_utils.make_outputdf(vars, units, param="weighted_avg", weighted_by=pop)
    return df


@_register
def retr_harvested_area(units):
    """Landuse: Food demand.

    Land-use related food demand.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Forestry"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Forestry"],
        }
    )

    vars["Rice"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Rice"],
        }
    )

    vars["Wheat"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Wheat"],
        }
    )

    vars["Maize"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Maize"],
        }
    )

    vars["Soybean"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Soybean"],
        }
    )

    vars["Cotton"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Cotton"],
        }
    )

    vars["Sugar cane"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Sugar cane"],
        }
    )

    vars["Palm oil"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Palm oil"],
        }
    )

    vars["Other coarse grains"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Other coarse grains"],
        }
    )

    vars["Other Oilseeds"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Other Oilseeds"],
        }
    )

    vars["Roots and tubers"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Roots and tubers"],
        }
    )

    vars["Pulses"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Pulses"],
        }
    )

    vars["Cereals"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Cereals"],
        }
    )

    vars["Oil Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Oil Crops"],
        }
    )

    vars["Sugar Crops "] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Sugar Crops"],
        }
    )

    vars["Other Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Harvested Area|Other Crops"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_carbon_removal(units_emi, units_ene):
    vars = {}

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    # Biogas share calculation
    _Biogas = pp.out("gas_bio", units_ene)

    _gas_inp_tecs = [
        "gas_ppl",
        "gas_cc",
        "gas_cc_ccs",
        "gas_ct",
        "gas_hpl",
        "meth_ng",
        "meth_ng_ccs",
        "h2_smr",
        "h2_smr_ccs",
        "gas_t_d",
        "gas_t_d_ch4",
    ]

    _totgas = pp.inp(_gas_inp_tecs, units_ene, inpfilter={"commodity": ["gas"]})

    _BGas_share = (_Biogas / _totgas).fillna(0)

    # Calulation of CCS components

    _CCS_bio_elec = -1.0 * pp.emi(
        tec=["bio_ppl_co2scr", "bio_istig_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_liq = -1.0 * pp.emi(
        tec=["eth_bio_ccs", "liq_bio_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_hydrogen = -1.0 * pp.emi(
        tec="h2_bio_ccs",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_elec = -1.0 * pp.emi(
        tec=["g_ppl_co2scr", "gas_cc_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_liq = -1.0 * pp.emi(
        tec="meth_ng_ccs",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_hydrogen = -1.0 * pp.emi(
        tec="h2_smr_ccs",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    vars["Geological Storage|Biomass"] = (
        _CCS_bio_elec
        + _CCS_gas_elec * _BGas_share  # Biomass|Energy|Supply|Electricity
        + _CCS_bio_liq
        + _CCS_gas_liq * _BGas_share  # Biomass|Energy|Supply|Liquids
        + _CCS_bio_hydrogen
        + _CCS_gas_hydrogen * _BGas_share  # Biomass|Energy|Supply|Hydrogen
    )

    # vars["Land Use"] = pp.land_out(
    #     lu_out_filter={
    #         "level": ["land_use_reporting"],
    #         "commodity": ["Carbon Removal|Land Use"],
    #     },
    #     units=units_emi,
    # )

    vars["Land Use|Agroforestry"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Carbon Removal|Land Use|Agroforestry"],
        },
        units=units_emi,
    )

    vars["Land Use|Biochar"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Carbon Removal|Land Use|Biochar"],
        },
        units=units_emi,
    )

    vars["Land Use|Forest Management"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Carbon Removal|Land Use|Forest Management"],
        },
        units=units_emi,
    )

    vars["Land Use|Re/Afforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Carbon Removal|Land Use|Re/Afforestation"],
        },
        units=units_emi,
    )

    vars["Land Use|Soil Carbon Management|Cropland"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Carbon Removal|Land Use|Soil Carbon Management|Cropland"],
        },
        units=units_emi,
    )

    vars["Land Use|Soil Carbon Management|Grassland"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Carbon Removal|Land Use|Soil Carbon Management|Grassland"],
        },
        units=units_emi,
    )

    vars["Land Use|Soil Carbon Management"] = (
        vars["Land Use|Soil Carbon Management|Cropland"]
        + vars["Land Use|Soil Carbon Management|Grassland"]
    )

    vars["Land Use|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": [
                "Carbon Removal|Land Use|Other LUC",
                "Carbon Removal|Land Use|Other",
            ],
        },
        units=units_emi,
    )

    vars["Land Use"] = (
        vars["Land Use|Agroforestry"]
        + vars["Land Use|Biochar"]
        + vars["Land Use|Forest Management"]
        + vars["Land Use|Soil Carbon Management"]
        + vars["Land Use|Re/Afforestation"]
        + vars["Land Use|Other"]
    )

    df = pp_utils.make_outputdf(vars, units_emi)
    return df


@_register
def retr_CO2_Capture(units_emi, units_ene):
    """Carbon sequestration.

    Energy and land-use related carbon seuqestration.

    Parameters
    ----------

    units_emi : str
        Units to which emission variables should be converted.
    units_ene : str
        Units to which energy variables should be converted.
    """

    vars = {}

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    # Biogas share calculation
    _Biogas = pp.out("gas_bio", units_ene)

    _gas_inp_tecs = [
        "gas_ppl",
        "gas_cc",
        "gas_cc_ccs",
        "gas_ct",
        "gas_hpl",
        "meth_ng",
        "meth_ng_ccs",
        "h2_smr",
        "h2_smr_ccs",
        "gas_t_d",
        "gas_t_d_ch4",
    ]

    _totgas = pp.inp(tec = _gas_inp_tecs, units = units_ene, inpfilter={"commodity": ["gas"]})

    _BGas_share = (_Biogas / _totgas).fillna(0)

    # Calulation of CCS components

    _CCS_coal_elec = -1.0 * pp.emi(
        tec=["c_ppl_co2scr", "coal_adv_ccs", "igcc_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_liq = -1.0 * pp.emi(
        tec=["syn_liq_ccs", "meth_coal_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_hydrogen = -1.0 * pp.emi(
        tec="h2_coal_ccs",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_cement = -1.0 * pp.emi(
        tec="cement_co2scr",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_elec = -1.0 * pp.emi(
        tec=["bio_ppl_co2scr", "bio_istig_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_liq = -1.0 * pp.emi(
        tec=["eth_bio_ccs", "liq_bio_ccs"],
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_hydrogen = -1.0 * pp.emi(
        tec= "h2_bio_ccs",
        units="GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_elec = (
        -1.0
        * pp.emi(
            tec=["g_ppl_co2scr", "gas_cc_ccs"],
            units="GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    _CCS_gas_liq = (
        -1.0
        * pp.emi(
            tec="meth_ng_ccs",
            units="GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    _CCS_gas_hydrogen = (
        -1.0
        * pp.emi(
            tec="h2_smr_ccs",
            units= "GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    vars["Energy|Supply|Electricity|Fossil"] = _CCS_coal_elec + _CCS_gas_elec

    vars["Energy|Supply|Liquids|Fossil"] = _CCS_coal_liq + _CCS_gas_liq

    vars["Energy|Supply|Hydrogen|Fossil"] = _CCS_coal_hydrogen + _CCS_gas_hydrogen

    vars["Energy|Supply|Electricity|Biomass"] = (
        _CCS_bio_elec + _CCS_gas_elec * _BGas_share
    )

    vars["Energy|Supply|Liquids|Biomass"] = _CCS_bio_liq + _CCS_gas_liq * _BGas_share

    vars["Energy|Supply|Hydrogen|Biomass"] = (
        _CCS_bio_hydrogen + _CCS_gas_hydrogen * _BGas_share
    )

    vars["Industrial Processes"] = _CCS_cement

    df = pp_utils.make_outputdf(vars, units_emi)
    return df


@_register
def retr_CO2_Gross_Removals(units):

    vars = {}

    vars["CO2|AFOLU|Intentional"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Gross Removals|CO2|AFOLU|Intentional"],
        }
    )

    vars["CO2|AFOLU"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Gross Removals|CO2|AFOLU"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df
