import numpy as np
import pandas as pd

from message_ix_models.report.legacy import pp_utils

pp = None
mu = None
run_history = None
urban_perc_data = None
kyoto_hist_data = None
lu_hist_data = None

func_dict = {}


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
                    tec,
                    units,
                    outfilter={"level": ["secondary"], "commodity": ["electr"]},
                )
                * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
                - (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
                * pp.out(
                    scrub_tec,
                    units,
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
                    scrub_tec,
                    units,
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
        pp.out(tec, units, outfilter=outfilter)
        * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
        - (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
        * pp.out(
            scrub_tec,
            units,
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
            scrub_tec,
            units,
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
                pp.out(tec, units, outfilter=outfilter, group=group)
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

    df = pp.out(tec, units, outfilter=outfilter) * (
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

    df = pp.out(tec, units, outfilter=outfilter) * (
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
                (pp.out(t, outfilter=outfilter, group=group))
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
        ["foil_fs", "loil_fs", "methanol_fs", "ethanol_fs", "gas_fs", "coal_fs"], units
    )

    vars["Input|Industrial Thermal"] = pp.inp(
        [
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
        units,
    ) + pp.out("solar_i", units)

    vars["Input|Industrial Specific"] = pp.inp(
        ["sp_liq_I", "sp_meth_I", "sp_eth_I", "sp_el_I", "h2_fc_I", "sp_el_I_RT"], units
    ) + pp.out("solar_pv_I", units)

    vars["Input|RC Thermal"] = pp.inp(
        [
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
        units,
    ) + pp.out("solar_rc", units)

    vars["Input|RC Specific"] = (
        pp.inp(["sp_el_RC", "h2_fc_RC", "sp_el_RC_RT"], units)
        + pp.out("solar_pv_RC", units)
        + (
            -1.0
            * pp.out(
                "h2_fc_trp",
                units,
                outfilter={"level": ["final"], "commodity": ["electr"]},
            )
        )
    )

    vars["Input|Transport"] = pp.inp(
        [
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
        units,
    )

    vars["Input|Shipping"] = pp.inp(
        [
            "foil_bunker",
            "loil_bunker",
            "meth_bunker",
            "eth_bunker",
            "LNG_bunker",
            "LH2_bunker",
        ],
        units,
    )

    vars["Input|Non-Commercial Biomass"] = pp.inp(["biomass_nc"], units)

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
        "aviation_Emission",
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Demand|Bunkers|International Aviation"] = Aircraft

    # -----------------------------------------
    # Electricity and heat production (Table 5)
    # -----------------------------------------

    Heat = pp.emi(
        ["bio_hpl", "coal_hpl", "foil_hpl", "gas_hpl"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Supply|Heat"] = Heat

    Powergeneration = pp.emi(
        [
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
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Supply|Electricity"] = Powergeneration

    gases_extraction = pp.emi(
        [
            "gas_extr_1",
            "gas_extr_2",
            "gas_extr_3",
            "gas_extr_4",
            "gas_extr_5",
            "gas_extr_6",
            "gas_extr_7",
            "gas_extr_8",
        ],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_extraction_fugitive = pp.emi(
        ["flaring_CO2"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_transportation = pp.emi(
        ["gas_t_d_ch4", "gas_t_d"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_coal = pp.emi(
        ["coal_gas"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    gases_hydrogen = pp.emi(
        ["h2_smr", "h2_smr_ccs", "h2_coal", "h2_coal_ccs", "h2_bio", "h2_bio_ccs"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_extraction = pp.emi(
        [
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
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_transportation = pp.emi(
        ["loil_t_d", "foil_t_d", "meth_t_d", "eth_t_d"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_oil = pp.emi(
        ["ref_lol", "ref_hil"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_gas = pp.emi(
        ["meth_ng", "meth_ng_ccs"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_coal = pp.emi(
        ["meth_coal", "meth_coal_ccs", "syn_liq", "syn_liq_ccs"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    liquids_biomass = pp.emi(
        ["eth_bio", "eth_bio_ccs", "liq_bio", "liq_bio_ccs"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    solid_extraction = pp.emi(
        ["coal_extr_ch4", "coal_extr", "lignite_extr"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    solid_transportation = pp.emi(
        ["coal_t_d", "biomass_t_d"],
        "GWa",
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
        [
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
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    ThermalInd = pp.emi(
        [
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
        "GWa",
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
            f"{var}_IndNonEnergyEM",
            "GWa",
            emifilter={"relation": [f"{var}_Emission"]},
            emission_units=units,
        )

    elif var in ["CO", "NOx", "VOC"]:
        NonEnergyInd = pp.emi(
            f"{var}_IndNonEnergyEM",
            "GWa",
            emifilter={"relation": [f"{var}_nonenergy"]},
            emission_units=units,
        )

    elif var in ["CH4"]:
        NonEnergyInd = pp.emi(
            f"{var}_IndNonEnergyEM",
            "GWa",
            emifilter={"relation": [f"{var}_new_Emission"]},
            emission_units=units,
        )

    else:
        NonEnergyInd = pp_utils._make_zero()

    addvarNEIND = pp_utils._make_zero()

    if var == "N2O":
        addvarNEIND = addvarNEIND + pp.emi(
            [
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
            "GWa",
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
            [
                "foil_bunker",
                "loil_bunker",
                "eth_bunker",
                "meth_bunker",
                "LNG_bunker",
                "LH2_bunker",
            ],
            "GWa",
            emifilter={"relation": [f"{var}_Emission_bunkers"]},
            emission_units=units,
        )

    vars["Energy|Demand|Bunkers|International Shipping"] = Bunker

    # -------------------------------------------------
    # Residential, Commercial, Other - Other (Table 11)
    # -------------------------------------------------

    if var == "N2O":
        ResComOth = pp.emi(
            f"{var}_OtherAgSo",
            "GWa",
            emifilter={"relation": [f"{var}other"]},
            emission_units=units,
        ) + pp.emi(
            f"{var}_ONonAgSo",
            "GWa",
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
        ["sp_el_RC", "solar_pv_RC", "h2_fc_RC", "sp_el_RC_RT"],
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    ResComTherm = pp.emi(
        [
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
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    ResComNC = pp.emi(
        "biomass_nc",
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    OtherSC = pp.emi(
        "other_sc",
        "GWa",
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
        [
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
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"] = Transport

    # ----------------------------------------------
    # Solvents production and application (Table 14)
    # ----------------------------------------------

    if var == "VOC":
        Solvents = pp.emi(
            f"{var}_SolventEM",
            "GWa",
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
            f"{var}_Human",
            "GWa",
            emifilter={"relation": [f"{var}other"]},
            emission_units=units,
        )

    elif var == "CH4":
        Waste = pp.emi(
            ["CH4_WasteBurnEM", "CH4_DomWasteWa", "CH4_IndWasteWa"],
            "GWa",
            emifilter={"relation": ["CH4_new_Emission"]},
            emission_units=units,
        ) + pp.emi(
            [
                "CH4n_landfills",
                "landfill_digester1",
                "landfill_compost1",
                "landfill_mechbio",
                "landfill_heatprdn",
                "landfill_direct1",
                "landfill_ele",
                "landfill_flaring",
            ],
            "GWa",
            emifilter={"relation": ["CH4_nonenergy"]},
            emission_units=units,
        )

    elif var in ["VOC", "CO", "NOx"]:
        Waste = pp.emi(
            f"{var}_WasteBurnEM",
            "GWa",
            emifilter={"relation": [f"{var}_nonenergy"]},
            emission_units=units,
        )

    elif var in ["BCA", "OCA", "SO2"]:
        Waste = pp.emi(
            f"{var}_WasteBurnEM",
            "GWa",
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

    _CCS_coal_elec = -1.0 * pp.emi(
        ["c_ppl_co2scr", "coal_adv_ccs", "igcc_ccs"],
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_liq = -1.0 * pp.emi(
        ["syn_liq_ccs", "meth_coal_ccs"],
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_hydrogen = -1.0 * pp.emi(
        "h2_coal_ccs",
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_cement = -1.0 * pp.emi(
        "cement_co2scr",
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_elec = -1.0 * pp.emi(
        ["bio_ppl_co2scr", "bio_istig_ccs"],
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_liq = -1.0 * pp.emi(
        ["eth_bio_ccs", "liq_bio_ccs"],
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_hydrogen = -1.0 * pp.emi(
        "h2_bio_ccs",
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_elec = (
        -1.0
        * pp.emi(
            ["g_ppl_co2scr", "gas_cc_ccs"],
            "GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    _CCS_gas_liq = (
        -1.0
        * pp.emi(
            "meth_ng_ccs",
            "GWa",
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        * (1.0 - _BGas_share)
    )

    _CCS_gas_hydrogen = (
        -1.0
        * pp.emi(
            "h2_smr_ccs",
            "GWa",
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
            [
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
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        + pp.inp(
            ["gas_t_d", "gas_t_d_ch4"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        - pp.out(
            ["gas_t_d", "gas_t_d_ch4"], units_ene_mdl, outfilter={"commodity": ["gas"]}
        )
    )

    _inp_all_gas_tecs = _inp_nonccs_gas_tecs + pp.inp(
        ["gas_cc_ccs", "meth_ng", "meth_ng_ccs", "h2_smr", "h2_smr_ccs"],
        units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out("gas_cc") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    _gas_ppl_shr = (pp.out("gas_ppl") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

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
            ["gas_ppl", "gas_ct", "gas_cc", "gas_cc_ccs"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_heat = _Biogas_tot * (
        pp.inp("gas_hpl", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_liquids_gas_comb = _Biogas_tot * (
        pp.inp(
            ["meth_ng", "meth_ng_ccs"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_gases_h2_comb = _Biogas_tot * (
        pp.inp(
            ["h2_smr", "h2_smr_ccs"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_ind = _Biogas_tot * (
        pp.inp(["gas_i", "hp_gas_i"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_fs = _Biogas_tot * (
        pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_res = _Biogas_tot * (
        pp.inp(["gas_rc", "hp_gas_rc"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_trp = _Biogas_tot * (
        pp.inp("gas_trp", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_td = _Biogas_tot * (
        (
            pp.inp(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
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
        ["gas_fs"],
        units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    _Hydrogen_tot = pp.emi(
        "h2_mix",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Hydrogen_el = _Hydrogen_tot * (
        (
            pp.inp(
                ["gas_ppl", "gas_ct", "gas_cc"],
                units_ene_mdl,
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
        pp.inp("gas_hpl", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_ind = _Hydrogen_tot * (
        pp.inp(["gas_i", "hp_gas_i"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # gas_fs is not able to use hydrogen, hence this is removed.
    # _Hydrogen_fs = _Hydrogen_tot * (
    #     pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
    #     / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    # ).fillna(0)

    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(["gas_rc", "hp_gas_rc"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_trp = _Hydrogen_tot * (
        pp.inp("gas_trp", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_td = _Hydrogen_tot * (
        (
            pp.inp(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
                outfilter={"commodity": ["gas"]},
            )
        )
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _SE_Elec_gen = pp.emi(
        [
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
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _SE_Elec_gen_woBECCS = (
        _SE_Elec_gen
        - pp.emi(
            ["bio_istig_ccs", "bio_ppl_co2scr"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            ["g_ppl_co2scr", "gas_cc_ccs"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        * (_Biogas_tot_abs / _inp_all_gas_tecs)
    )

    _SE_District_heat = pp.emi(
        ["coal_hpl", "foil_hpl", "gas_hpl"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _FE_Feedstocks = pp.emi(
        ["coal_fs", "foil_fs", "loil_fs", "gas_fs", "methanol_fs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_feedstocks"]},
        emission_units=units_emi,
    )

    _FE_Res_com = pp.emi(
        ["coal_rc", "foil_rc", "loil_rc", "gas_rc", "meth_rc", "hp_gas_rc"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_r_c"]},
        emission_units=units_emi,
    )

    _FE_Industry = pp.emi(
        [
            "gas_i",
            "hp_gas_i",
            "loil_i",
            "meth_i",
            "coal_i",
            "foil_i",
            "sp_liq_I",
            "sp_meth_I",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _FE_Transport = pp.emi(
        ["gas_trp", "loil_trp", "meth_fc_trp", "meth_ic_trp", "coal_trp", "foil_trp"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trp"]},
        emission_units=units_emi,
    )

    _FE_total = _FE_Feedstocks + _FE_Res_com + _FE_Industry + _FE_Transport

    _Other_gases_extr_comb = pp.emi(
        [
            "gas_extr_1",
            "gas_extr_2",
            "gas_extr_3",
            "gas_extr_4",
            "gas_extr_5",
            "gas_extr_6",
            "gas_extr_7",
            "gas_extr_8",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_extr_fug = pp.emi(
        "flaring_CO2",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calculated from CO2_TCE and doesnt include trade
    _Other_gases_trans_comb_trade = pp.emi(
        [
            "LNG_trd",
            "gas_exp_afr",
            "gas_exp_cpa",
            "gas_exp_eeu",
            "gas_exp_nam",
            "gas_exp_pao",
            "gas_exp_sas",
            "gas_exp_weu",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_gases_trans_comb = pp.emi(
        ["gas_t_d", "gas_t_d_ch4"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_coal_comb = pp.emi(
        ["coal_gas"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    #    _Other_gases_biomass_comb = pp.emi(["gas_bio"], units_ene_mdl,
    #                                       emifilter={"relation": ["CO2_cc"]},
    #                                       emission_units=units_emi)

    _Other_gases_h2_comb = pp.emi(
        ["h2_smr", "h2_coal", "h2_bio", "h2_coal_ccs", "h2_smr_ccs", "h2_bio_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_h2_comb_woBECCS = (
        _Other_gases_h2_comb
        - pp.emi(
            ["h2_bio_ccs"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            ["h2_smr_ccs"],
            units_ene_mdl,
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
        [
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
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calcualted from CO2_TCE and doesnt include trade
    _Other_liquids_trans_comb_trade = pp.emi(
        ["foil_trd", "loil_trd", "oil_trd", "meth_trd"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_liquids_trans_comb = pp.emi(
        ["foil_t_d", "loil_t_d", "meth_t_d"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_oil_comb = pp.emi(
        ["ref_lol", "ref_hil"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb = pp.emi(
        ["meth_ng", "meth_ng_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb_woBECCS = _Other_liquids_gas_comb - pp.emi(
        ["meth_ng_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    ) * (_Biogas_tot_abs / _inp_all_gas_tecs)

    _Other_liquids_coal_comb = pp.emi(
        ["meth_coal", "syn_liq", "meth_coal_ccs", "syn_liq_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb = pp.emi(
        ["eth_bio", "liq_bio", "eth_bio_ccs", "liq_bio_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb_woBECCS = _Other_liquids_biomass_comb - pp.emi(
        ["eth_bio_ccs", "liq_bio_ccs"],
        units_ene_mdl,
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
        "coal_t_d",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_solids_total = _Other_solids_coal_trans_comb

    _Cement1 = pp.emi(
        ["cement_CO2", "cement_co2scr"],
        units_ene_mdl,
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
        "CO2_TCE",
        units_ene_mdl,
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
        ["coal_imp", "coal_exp"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    ) + pp.emi(
        ["coal_trd"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )
    # Coal cannot be used for bunkers
    #    + pp.emi(["coal_bunker"], units_ene_mdl,
    #             emifilter={"relation": ["CO2_shipping"]},
    #             emission_units=units_emi)

    vars["Difference|Stock|Methanol"] = (
        pp.emi(
            ["meth_imp", "meth_exp"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["meth_bunker"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["meth_trd"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Fueloil"] = (
        pp.emi(
            ["foil_imp", "foil_exp"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["foil_bunker"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["foil_trd"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Lightoil"] = (
        pp.emi(
            ["loil_imp", "loil_exp"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["loil_bunker"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["loil_trd"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Crudeoil"] = pp.emi(
        ["oil_imp", "oil_exp"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    ) + pp.emi(
        ["oil_trd"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )
    #    + pp.emi(["oil_bunker"], units_ene_mdl,
    #             emifilter={"relation": ["CO2_shipping"]},
    #             emission_units=units_emi)

    vars["Difference|Stock|LNG"] = (
        pp.emi(
            ["LNG_imp", "LNG_exp"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["LNG_bunker"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_shipping"]},
            emission_units=units_emi,
        )
        + pp.emi(
            ["LNG_trd"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_trade"]},
            emission_units=units_emi,
        )
    )

    vars["Difference|Stock|Natural Gas"] = pp.emi(
        [
            "gas_imp",
            "gas_exp_nam",
            "gas_exp_weu",
            "gas_exp_eeu",
            "gas_exp_pao",
            "gas_exp_cpa",
            "gas_exp_sas",
            "gas_exp_afr",
            "gas_exp_scs",
            "gas_exp_cas",
            "gas_exp_ubm",
            "gas_exp_rus",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    dfs.append(pp_utils.make_outputdf(vars, units_emi, glb=False))
    vars = {}

    vars["Difference|Stock|Activity|Coal"] = (
        pp.inp(["coal_imp"], units_ene_mdl)
        - pp.inp(["coal_exp"], units_ene_mdl)
        + pp.inp(["coal_trd"], units_ene_mdl)
        - pp.out(["coal_trd"], units_ene_mdl)
    )
    #   + pp.inp(["coal_bunker"], units_ene_mdl)

    vars["Difference|Stock|Activity|Methanol"] = (
        pp.inp(["meth_imp"], units_ene_mdl)
        - pp.inp(["meth_exp"], units_ene_mdl)
        + pp.inp(["meth_bunker"], units_ene_mdl)
        + pp.inp(["meth_trd"], units_ene_mdl)
        - pp.out(["meth_trd"], units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Fueloil"] = (
        pp.inp(["foil_imp"], units_ene_mdl)
        - pp.inp(["foil_exp"], units_ene_mdl)
        + pp.inp(["foil_bunker"], units_ene_mdl)
        + pp.inp(["foil_trd"], units_ene_mdl)
        - pp.out(["foil_trd"], units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Lightoil"] = (
        pp.inp(["loil_imp"], units_ene_mdl)
        - pp.inp(["loil_exp"], units_ene_mdl)
        + pp.inp(["loil_bunker"], units_ene_mdl)
        + pp.inp(["loil_trd"], units_ene_mdl)
        - pp.out(["loil_trd"], units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Crudeoil"] = (
        pp.inp(["oil_imp"], units_ene_mdl)
        - pp.inp(["oil_exp"], units_ene_mdl)
        + pp.inp(["oil_trd"], units_ene_mdl)
        - pp.out(["oil_trd"], units_ene_mdl)
    )
    #   + pp.inp(["oil_bunker"], units_ene_mdl)

    vars["Difference|Stock|Activity|LNG"] = (
        pp.inp(["LNG_imp"], units_ene_mdl)
        - pp.inp(["LNG_exp"], units_ene_mdl)
        + pp.inp(["LNG_bunker"], units_ene_mdl)
        + pp.inp(["LNG_trd"], units_ene_mdl)
        - pp.out(["LNG_trd"], units_ene_mdl)
    )

    vars["Difference|Stock|Activity|Natural Gas"] = pp.inp(
        ["gas_imp"], units_ene_mdl
    ) - pp.inp(
        [
            "gas_exp_nam",
            "gas_exp_weu",
            "gas_exp_eeu",
            "gas_exp_pao",
            "gas_exp_cpa",
            "gas_exp_sas",
            "gas_exp_afr",
            "gas_exp_scs",
            "gas_exp_cas",
            "gas_exp_ubm",
            "gas_exp_rus",
        ],
        units_ene_mdl,
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
            [
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
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        + pp.inp(
            ["gas_t_d", "gas_t_d_ch4"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        - pp.out(
            ["gas_t_d", "gas_t_d_ch4"], units_ene_mdl, outfilter={"commodity": ["gas"]}
        )
    )

    _inp_all_gas_tecs = _inp_nonccs_gas_tecs + pp.inp(
        ["gas_cc_ccs", "meth_ng", "meth_ng_ccs", "h2_smr", "h2_smr_ccs"],
        units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out("gas_cc") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    _gas_ppl_shr = (pp.out("gas_ppl") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

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
            ["gas_ppl", "gas_ct", "gas_cc", "gas_cc_ccs"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_heat = _Biogas_tot * (
        pp.inp("gas_hpl", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_liquids_gas_comb = _Biogas_tot * (
        pp.inp(
            ["meth_ng", "meth_ng_ccs"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_gases_h2_comb = _Biogas_tot * (
        pp.inp(
            ["h2_smr", "h2_smr_ccs"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_ind = _Biogas_tot * (
        pp.inp(["gas_i", "hp_gas_i"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_fs = _Biogas_tot * (
        pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_res = _Biogas_tot * (
        pp.inp(["gas_rc", "hp_gas_rc"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_trp = _Biogas_tot * (
        pp.inp("gas_trp", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_td = _Biogas_tot * (
        (
            pp.inp(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
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
        ["gas_fs"],
        units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    _Hydrogen_tot = pp.emi(
        "h2_mix",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Hydrogen_el = _Hydrogen_tot * (
        (
            pp.inp(
                ["gas_ppl", "gas_ct", "gas_cc"],
                units_ene_mdl,
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
        pp.inp("gas_hpl", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_ind = _Hydrogen_tot * (
        pp.inp(["gas_i", "hp_gas_i"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # gas_fs is not able to use hydrogen, hence this is removed.
    # _Hydrogen_fs = _Hydrogen_tot * (
    #     pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
    #     / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    # ).fillna(0)

    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(["gas_rc", "hp_gas_rc"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_trp = _Hydrogen_tot * (
        pp.inp("gas_trp", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_td = _Hydrogen_tot * (
        (
            pp.inp(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
                inpfilter={"commodity": ["gas"]},
            )
            - pp.out(
                ["gas_t_d", "gas_t_d_ch4"],
                units_ene_mdl,
                outfilter={"commodity": ["gas"]},
            )
        )
        / _H2_inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _SE_Elec_gen_wBECCS = pp.emi(
        [
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
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _SE_Elec_gen = (
        _SE_Elec_gen_wBECCS
        - pp.emi(
            ["bio_istig_ccs", "bio_ppl_co2scr"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            ["g_ppl_co2scr", "gas_cc_ccs"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        * (_Biogas_tot_abs / _inp_all_gas_tecs)
    )

    _SE_District_heat = pp.emi(
        ["coal_hpl", "foil_hpl", "gas_hpl"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _FE_Feedstocks = pp.emi(
        ["coal_fs", "foil_fs", "loil_fs", "gas_fs", "methanol_fs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_feedstocks"]},
        emission_units=units_emi,
    )

    _FE_Res_com = pp.emi(
        ["coal_rc", "foil_rc", "loil_rc", "gas_rc", "meth_rc", "hp_gas_rc"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_r_c"]},
        emission_units=units_emi,
    )

    _FE_Industry = pp.emi(
        [
            "gas_i",
            "hp_gas_i",
            "loil_i",
            "meth_i",
            "coal_i",
            "foil_i",
            "sp_liq_I",
            "sp_meth_I",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _FE_Transport = pp.emi(
        ["gas_trp", "loil_trp", "meth_fc_trp", "meth_ic_trp", "coal_trp", "foil_trp"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trp"]},
        emission_units=units_emi,
    )

    _FE_total = _FE_Feedstocks + _FE_Res_com + _FE_Industry + _FE_Transport

    _Other_gases_extr_comb = pp.emi(
        [
            "gas_extr_1",
            "gas_extr_2",
            "gas_extr_3",
            "gas_extr_4",
            "gas_extr_5",
            "gas_extr_6",
            "gas_extr_7",
            "gas_extr_8",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_extr_fug = pp.emi(
        "flaring_CO2",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calculated from CO2_TCE and doesnt include trade
    _Other_gases_trans_comb_trade = pp.emi(
        [
            "LNG_trd",
            "gas_exp_afr",
            "gas_exp_cpa",
            "gas_exp_eeu",
            "gas_exp_nam",
            "gas_exp_pao",
            "gas_exp_sas",
            "gas_exp_weu",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_gases_trans_comb = pp.emi(
        ["gas_t_d", "gas_t_d_ch4"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_coal_comb = pp.emi(
        ["coal_gas"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    #    _Other_gases_biomass_comb = pp.emi(["gas_bio"], units_ene_mdl,
    #                                       emifilter={"relation": ["CO2_cc"]},
    #                                       emission_units=units_emi)

    _Other_gases_h2_comb_wBECCS = pp.emi(
        ["h2_smr", "h2_coal", "h2_bio", "h2_coal_ccs", "h2_smr_ccs", "h2_bio_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_gases_h2_comb = (
        _Other_gases_h2_comb_wBECCS
        - pp.emi(
            ["h2_bio_ccs"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            ["h2_smr_ccs"],
            units_ene_mdl,
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
        [
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
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calcualted from CO2_TCE and doesnt include trade
    _Other_liquids_trans_comb_trade = pp.emi(
        ["foil_trd", "loil_trd", "oil_trd", "meth_trd"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_liquids_trans_comb = pp.emi(
        ["foil_t_d", "loil_t_d", "meth_t_d"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_oil_comb = pp.emi(
        ["ref_lol", "ref_hil"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb_wBECCS = pp.emi(
        ["meth_ng", "meth_ng_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_gas_comb = _Other_liquids_gas_comb_wBECCS - pp.emi(
        ["meth_ng_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    ) * (_Biogas_tot_abs / _inp_all_gas_tecs)

    _Other_liquids_coal_comb = pp.emi(
        ["meth_coal", "syn_liq", "meth_coal_ccs", "syn_liq_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb_wBECCS = pp.emi(
        ["eth_bio", "liq_bio", "eth_bio_ccs", "liq_bio_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_liquids_biomass_comb = _Other_liquids_biomass_comb_wBECCS - pp.emi(
        ["eth_bio_ccs", "liq_bio_ccs"],
        units_ene_mdl,
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
        "coal_t_d",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_solids_total = _Other_solids_coal_trans_comb

    _Cement1 = (
        pp.emi(
            ["cement_CO2", "cement_co2scr"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        # GROSS EMISSION modification
        - pp.emi(
            ["cement_co2scr"],
            units_ene_mdl,
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
        "CO2_TCE",
        units_ene_mdl,
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


