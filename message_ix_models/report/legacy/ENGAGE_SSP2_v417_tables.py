import pandas as pd

from . import pp_utils

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
        pp.out(scrub_tec, units)
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
    ).fillna(0)

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
            pp.out(tec, units)
            * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
            - (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
            * pp.out(scrub_tec, units)
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
    ).fillna(0)

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
            (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
            * pp.out(scrub_tec, units)
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
    ).fillna(0)

    return df


def _se_elec_woCCSretro(tec, scrub_tec, units, _Frac, share=1):
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
    """

    df = (
        pp.out(tec, units)
        * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
        - (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
        * pp.out(scrub_tec, units)
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
        * pp.out(scrub_tec, units)
        * share
    )

    return df


def _pe_elec_po_turb(tec, group, units, _Frac, inpfilter):
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
    """

    df = pp_utils.ppgroup(
        (
            (
                pp.out(tec, units, group=group)
                * (1.0 - pp.rel(tec, relfilter={"relation": ["pass_out_trb"]}) * _Frac)
            )
            / pp.eff(tec, inpfilter=inpfilter, group=group)
        ).fillna(0)
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
                / pp.eff(t, inpfilter=inpfilter, group=group)
            )
        )
    df = pd.concat(dfs, sort=True)

    return df.groupby(df.index.name).sum()


# -------------------
# Reporting functions
# -------------------


@_register
def retr_globiom(units_ghg, units_co2, units_energy, units_volume, units_area):
    """GLOBIOM: Additional variables specific to the emulator.

    This section reports additional GLOBIOM specific
    variables that are not included in other parts
    of the reporting.

    Note that this data is for IIASA internal purposes only.

    Parameters
    ----------

    units_ghg : str
        Units to which non-CO2 GHG-variables should be converted.
    units_co2 : str
        Units to which CO2 variables should be converted.
    units_energy : str
        Units to which energy variables should be converted.
    units_volume : str
        Units to which volume based variable should be converted.
    units_area : str
        Units to which are based variable should be converted.
    """

    dfs = []

    # ------------------------------
    # Section: Non-CO2 GHG emissions
    # ------------------------------

    vars = {}
    vars["Emissions|CH4 Emissions Total"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Agri_CH4"]},
        units=units_ghg,
    )

    vars["Emissions|N20 Emissions Total"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Agri_N2O"]},
        units=units_ghg,
    )

    vars["Emissions|GHG Emissions Total"] = (
        pp.land_out(
            lu_out_filter={"level": ["land_use_reporting"], "commodity": ["LU_CO2"]},
            units=units_ghg,
        )
        + vars["Emissions|N20 Emissions Total"]
        + vars["Emissions|CH4 Emissions Total"]
    )

    dfs.append(pp_utils.make_outputdf(vars, units_ghg))

    # ----------------------
    # Section: CO2 emissions
    # ----------------------

    vars = {}
    vars["Emissions|Afforestation CO2 G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Aff_CO2_G4M"]},
        units=units_co2,
    )

    vars["Emissions|CO2 Emissions|BIO00"] = pp_utils._make_zero()
    vars["Emissions|CO2 Emissions|BIO01"] = pp_utils._make_zero()
    vars["Emissions|CO2 Emissions|BIO02"] = pp_utils._make_zero()
    vars["Emissions|CO2 Emissions|BIO03"] = pp_utils._make_zero()
    vars["Emissions|CO2 Emissions|BIO04"] = pp_utils._make_zero()
    vars["Emissions|CO2 Emissions|BIO05"] = pp_utils._make_zero()
    vars["Emissions|CO2 Emissions|BIO0N"] = pp_utils._make_zero()

    vars["Emissions|CO2 Emissions"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["LU_CO2"]},
        units=units_co2,
    )

    vars["Emissions|Deforestation CO2 GLO"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Def_CO2_GLO"]},
        units=units_co2,
    )

    vars["Emissions|Deforestation CO2 G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Def_CO2_G4M"]},
        units=units_co2,
    )

    vars["Emissions|Forest Management CO2 G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Fmg_CO2_G4M"]},
        units=units_co2,
    )

    vars["Emissions|Olc CO2 Globiom"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Olc_CO2_GLO"]},
        units=units_co2,
    )

    vars["Emissions|Total CO2 G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Tot_CO2_G4M"]},
        units=units_co2,
    )

    dfs.append(pp_utils.make_outputdf(vars, units_co2))

    # ---------------
    # Section: Energy
    # ---------------

    vars = {}
    vars["Primary Energy|Biomass|BIO00"] = pp_utils._make_zero()
    vars["Primary Energy|Biomass|BIO01"] = pp_utils._make_zero()
    vars["Primary Energy|Biomass|BIO02"] = pp_utils._make_zero()
    vars["Primary Energy|Biomass|BIO03"] = pp_utils._make_zero()
    vars["Primary Energy|Biomass|BIO04"] = pp_utils._make_zero()
    vars["Primary Energy|Biomass|BIO05"] = pp_utils._make_zero()
    vars["Primary Energy|Biomass|BIO0N"] = pp_utils._make_zero()

    vars["Primary Energy|Biomass"] = pp.land_out(
        lu_out_filter={"level": ["land_use"], "commodity": ["bioenergy"]},
        units=units_energy,
    )

    vars["Primary Energy|Forest Biomass GLO"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["ForestBiomass"]}
    )

    vars["Primary Energy|Forest Biomass G4M"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["ForestBiomass_G4M"],
        }
    )

    vars["Primary Energy|Fuel Wood GLO"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["FuelWood"]}
    )

    vars["Primary Energy|Fuel Wood G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["FuelWood_G4M"]}
    )

    vars["Primary Energy|Other Solid Non Commercial"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["OthSolidNonComm"],
        }
    )

    vars["Primary Energy|Plantation Biomass"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["PlantationBiomass"],
        }
    )

    vars["Primary Energy|Sawmill Residues GLO"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["SawmillResidues"],
        }
    )

    vars["Primary Energy|Sawmill Residues G4M"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["SawmillResidues_G4M"],
        }
    )

    vars["Primary Energy|Solid Exogenous GLO"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["SolidExogenous"]}
    )

    vars["Primary Energy|Solid Exogenous G4M"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["SolidExogenous_G4M"],
        }
    )

    vars["Primary Energy|Solid Total GLO"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["SolidTotal"]}
    )

    vars["Primary Energy|Solid Total G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["SolidTotal_G4M"]}
    )

    vars["Final Energy|Liquid Total"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["LiquidTotal"]}
    )

    dfs.append(pp_utils.make_outputdf(vars, units_energy))

    # -------------
    # Section: Wood
    # -------------

    vars = {}
    vars["Wood|Forest Harvest Deforestation G4M"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["ForestHarvestDF_G4M"],
        }
    )

    vars["Wood|Forest Harvest Forest Management G4M"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["ForestHarvestFM_G4M"],
        }
    )

    vars["Wood|Forest Harvest Total G4M"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["ForestHarvestTot_G4M"],
        }
    )

    vars["Wood|Plantation Harvest GLO"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["PlantationHarvest_GLO"],
        }
    )

    vars["Wood|Timber Industry GLO"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["TimberIndust"]}
    )

    vars["Wood|Timber Industry G4M"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["TimberIndust_G4M"],
        }
    )

    dfs.append(pp_utils.make_outputdf(vars, units_volume))

    # -------------------
    # Section: Land cover
    # -------------------

    vars = {}
    vars["Land Cover|New Forest G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["NewFor_G4M"]}
    )

    vars["Land Cover|Old Forest G4M"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["OldFor_G4M"]}
    )

    dfs.append(pp_utils.make_outputdf(vars, units_area))
    return pd.concat(dfs, sort=True)


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

    vars["Energy|Crops|1st generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Bioenergy|1st generation"],
        }
    )

    vars["Energy|Crops|2nd generation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Bioenergy|2nd generation"],
        }
    )

    vars["Non-Energy|Crops|Feed"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Feed|Crops"],
        }
    )

    vars["Non-Energy|Crops|Food"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Food|Crops"],
        }
    )

    vars["Non-Energy|Crops|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Food|Crops"],
        }
    )

    vars["Non-Energy|Livestock|Food"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Food|Livestock"],
        }
    )

    vars["Non-Energy|Livestock|Other"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Demand|Non-Food|Livestock"],
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

    vars["Energy|Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Energy Crops"],
        }
    )

    vars["Non-Energy|Crops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Non-Energy Crops"],
        }
    )

    vars["Non-Energy|Livestock"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Agricultural Production|Livestock"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


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

    vars["Forest"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest"],
        }
    )

    vars["Forest|Afforestation and Reforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Afforestation and" " Reforestation"],
        }
    )

    vars["Forest|Natural Forest"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Forest|Natural Forest"],
        }
    )

    vars["Other Land"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Other Natural Land"],
        }
    )

    vars["Pasture"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Land Cover|Pasture"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


@_register
def retr_yield(units):
    """Landuse: Crop yield.

    Land-use related crop-yield.
    Based on land-use emulator.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """

    vars = {}

    vars["Cereal"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Cereal"]}
    )

    vars["Oilcrops"] = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Yield|Oilcrops"]}
    )

    vars["Sugarcrops"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Yield|Sugarcrops"],
        }
    )

    df = pp_utils.make_outputdf(vars, units, param="mean")
    return df


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

    # Unit must be converted by factor .001 as they seem to be
    # by a factor of 1000 too large.
    vars = {}
    vars["Phosphorus"] = (
        pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Fertilizer Use|Phosphorus"],
            }
        )
        * 0.001
    )

    dfs.append(pp_utils.make_outputdf(vars, units_phosphorus))
    return pd.concat(dfs, sort=True)


@_register
def retr_frst_prd(units):
    """Landuse: Forestry product production.

    Land-use related forestry production.
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
            "commodity": ["Forestry Production|Roundwood"],
        }
    )

    vars["Roundwood|Industrial Roundwood"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood|Industrial" " Roundwood"],
        }
    )

    vars["Roundwood|Wood Fuel"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Forestry Production|Roundwood|Wood Fuel"],
        }
    )

    df = pp_utils.make_outputdf(vars, units)
    return df


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

    df_urban_perc = pd.read_csv(urban_perc_data).set_index("Region") / 100
    df_urban_perc = df_urban_perc.rename(
        columns={i: int(i) for i in df_urban_perc.columns}
    )

    vars["Urban"] = vars["Total"].multiply(df_urban_perc, fill_value=0)

    vars["Rural"] = vars["Total"] - vars["Urban"]

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
    scale_tec = "Agricultural Production|Non-Energy Crops"

    vars["Agriculture|Non-Energy Crops and Livestock|Index"] = pp.retrieve_lu_price(
        "Price|Agriculture|Non-Energy Crops and Livestock|Index", scale_tec
    )
    vars["Agriculture|Non-Energy Crops|Index"] = pp.retrieve_lu_price(
        "Price|Agriculture|Non-Energy Crops|Index", scale_tec
    )
    dfs.append(pp_utils.make_outputdf(vars, units_agri, glb=False))

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
        df_hist = pd.read_csv(kyoto_hist_data).set_index("Region")
        df_hist = df_hist.rename(columns={i: int(i) for i in df_hist.columns})

        # There is no need to add the historic emissions for other lu related
        # emissions, as these remain unchanged in the historic time-period.

        df_new_lu_hist = (
            pp.land_out(
                lu_out_filter={
                    "level": ["land_use_reporting"],
                    "commodity": ["LU_CO2"],
                },
                units="Mt CO2eq/yr",
            )
            + pp.land_out(
                lu_out_filter={
                    "level": ["land_use_reporting"],
                    "commodity": ["Agri_CH4"],
                },
                units="Mt CO2eq/yr",
            )
            + pp.land_out(
                lu_out_filter={
                    "level": ["land_use_reporting"],
                    "commodity": ["Agri_N2O_calc"],
                },
                units="Mt CO2eq/yr",
            )
        )

        df_hist = df_new_lu_hist.add(df_hist, fill_value=0)
        vars["Kyoto Gases"] = vars["Kyoto Gases"] + df_hist

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

    if var in ["BCA", "OCA", "SO2", "NH3"]:
        # HACK: Variable activity is in kt so GWa -> MWa will * by .001
        AgricultureWasteBurning = pp.emi(
            f"{var}_AgWasteEM",
            "GWa",
            emifilter={"relation": [f"{var}_Emission"]},
            emission_units=units,
        )

    elif var in ["CO", "NOx", "VOC"]:
        AgricultureWasteBurning = pp.emi(
            f"{var}_AgWasteEM",
            "GWa",
            emifilter={"relation": [f"{var}_nonenergy"]},
            emission_units=units,
        )

    elif var in ["CH4"]:
        AgricultureWasteBurning = pp.emi(
            f"{var}_AgWasteEM",
            "GWa",
            emifilter={"relation": [f"{var}_new_Emission"]},
            emission_units=units,
        )
    else:
        AgricultureWasteBurning = pp_utils._make_zero()

    vars["AFOLU|Biomass Burning"] = AgricultureWasteBurning

    # ---------------------
    # 1.2 Other Agriculture
    # ---------------------

    if var in "CH4":
        vars["AFOLU|Agriculture|Livestock|Enteric Fermentation"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": [
                    "Emissions|CH4|Land Use|Agriculture|Enteric" " Fermentation"
                ],
            }
        )

        vars["AFOLU|Agriculture|Livestock|Manure Management"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|CH4|Land Use|Agriculture|AWM"],
            }
        )

        vars["AFOLU|Agriculture|Livestock"] = (
            vars["AFOLU|Agriculture|Livestock|Enteric Fermentation"]
            + vars["AFOLU|Agriculture|Livestock|Manure Management"]
        )

        vars["AFOLU|Agriculture|Rice"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|CH4|Land Use|Agriculture|Rice"],
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
        vars["AFOLU|Land|Grassland Pastures"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|N2O|Land Use|Agriculture|" "Pasture"],
            }
        )

        vars["AFOLU|Agriculture|Managed Soils"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|N2O|Land Use|Agriculture|" "Cropland Soils"],
            }
        )

        vars["AFOLU|Agriculture|Livestock|Manure Management"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|N2O|Land Use|Agriculture|" "AWM"],
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

    if var in ["BCA", "OCA", "SO2", "NH3", "CO", "NOx", "VOC", "CH4"]:
        GrasslandBurning = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": [f"{var}_SavanBurnEM"],
            },
            units=units,
        )

    else:
        GrasslandBurning = pp_utils._make_zero()

    vars["AFOLU|Land|Grassland Burning"] = GrasslandBurning

    # ------------------------
    # Forest Burning (Table 3)
    # ------------------------

    if var in ["BCA", "OCA", "SO2", "NH3", "CO", "NOx", "VOC", "CH4"]:
        ForestBurning = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": [f"{var}_LandUseChangeEM"],
            },
            units=units,
        )
    else:
        ForestBurning = pp_utils._make_zero()

    vars["AFOLU|Land|Forest Burning"] = ForestBurning

    # ------------------
    # Aircraft (Table 4)
    # ------------------

    Aircraft = pp.emi(
        "aviation_Emission",
        "GWa",
        emifilter={"relation": [f"{var}_Emission"]},
        emission_units=units,
    )

    vars["Energy|Demand|Transportation|Aviation|International"] = Aircraft

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
            "SO2_scrub_ppl",
            "coal_adv_ccs",
            "igcc_ccs",
            "gas_cc_ccs",
            "gas_ct",
            "gas_htfc",
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
        ["ref_lol", "ref_hil", "SO2_scrub_ref"],
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

    if var == "SO2":
        # SO2_Scrubber // SO2_IndNonEnergyEM // SO2_coal_t/d
        addvarInd = addvarInd + pp.emi(
            ["SO2_scrub_ind", "coal_t_d-in-SO2", "coal_t_d-in-06p"],
            "GWa",
            emifilter={"relation": [f"{var}_Emission"]},
            emission_units=units,
        )

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

    vars["Energy|Demand|Transportation|Shipping|International"] = Bunker

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
        ["sp_el_RC", "solar_pv_RC", "h2_fc_RC"],
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

    if var == "SO2":
        addvarRC = addvarRC + pp.emi(
            ["coal_t_d-rc-SO2", "coal_t_d-rc-06p"],
            "GWa",
            emifilter={"relation": [f"{var}_Emission"]},
            emission_units=units,
        )

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
            + vars["Energy|Demand|Transportation|Shipping|International"]
            + vars["Energy|Demand|AFOFI"]
            + vars["Energy|Demand|Industry"]
            + vars["Energy|Demand|Residential and Commercial"]
            + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
            + vars["Energy|Demand|Transportation|Aviation|International"]
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
            + vars["Energy|Demand|Transportation|Shipping|International"]
            + vars["Energy|Demand|AFOFI"]
            + vars["Energy|Demand|Industry"]
            + vars["Energy|Demand|Residential and Commercial"]
            - ResComNC
            + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
            + vars["Energy|Demand|Transportation|Aviation|International"]
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
            + vars["Energy|Demand|Transportation|Shipping|International"]
            + vars["Energy|Demand|AFOFI"]
            + vars["Energy|Demand|Industry"]
            + vars["Energy|Demand|Residential and Commercial"]
            + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
            + vars["Energy|Demand|Transportation|Aviation|International"]
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
        "gas_htfc",
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
        ["c_ppl_co2scr", "coal_adv_ccs", "igcc_ccs", "igcc_co2scr"],
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

    vars["Land Use"] = -pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|Land Use|Negative"],
        }
    )

    vars["Land Use|Afforestation"] = -pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["Aff_CO2_G4M"]},
        units=units_emi,
    )

    df = pp_utils.make_outputdf(vars, units_emi)
    return df


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
                "gas_htfc",
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
        - _pe_wCCSretro(
            "gas_htfc",
            "gfc_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units_ene_mdl,
        )
    )

    # Helping variables required in units of Emissions
    _Biogas_tot_abs = pp.out("gas_bio")
    _Biogas_tot = _Biogas_tot_abs * mu["crbcnt_gas"] * mu["conv_c2co2"]
    _Biogas_el = _Biogas_tot * (
        pp.inp(
            ["gas_ppl", "gas_ct", "gas_cc", "gas_cc_ccs", "gas_htfc"],
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

    _Hydrogen_tot = pp.emi(
        "h2_mix",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Hydrogen_el = _Hydrogen_tot * (
        (
            pp.inp(
                ["gas_ppl", "gas_ct", "gas_cc", "gas_htfc"],
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
            - _pe_wCCSretro(
                "gas_htfc",
                "gfc_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units_ene_mdl,
            )
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_heat = _Hydrogen_tot * (
        pp.inp("gas_hpl", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_ind = _Hydrogen_tot * (
        pp.inp(["gas_i", "hp_gas_i"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_fs = _Hydrogen_tot * (
        pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(["gas_rc", "hp_gas_rc"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_trp = _Hydrogen_tot * (
        pp.inp("gas_trp", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
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
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
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
            "gas_htfc",
            "bio_istig",
            "g_ppl_co2scr",
            "c_ppl_co2scr",
            "bio_ppl_co2scr",
            "igcc_co2scr",
            "gfc_co2scr",
            "cfc_co2scr",
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
            emifilter={"relation": ["CO2_Emission"]},
            emission_units=units_emi,
        )
        - pp.emi(
            ["g_ppl_co2scr", "gas_cc_ccs"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_Emission"]},
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
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    # Note that this is not included in the total because
    # the diff is only calcualted from CO2_TCE and doesnt include trade
    _Other_gases_trans_comb_trade = pp.emi(
        ["LNG_trd"],
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
        ["foil_trd", "loil_trd", "oil_trd", "meth_trd", "eth_trd"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    _Other_liquids_trans_comb = pp.emi(
        ["foil_t_d", "loil_t_d", "meth_t_d", "eth_t_d"],
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
        emifilter={"relation": ["CO2_Emission"]},
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
    )
    # GLOBIOM with the new lu implementation, LU_CO2 no longer writes
    # into _CO2_tce1 (CO2_TCE), as these have emission factors only,
    # and therefore do not write into CO2_TCE
    # + _CO2_GLOBIOM)

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

    _CO2_GLOBIOM = pp.land_out(
        lu_out_filter={"level": ["land_use_reporting"], "commodity": ["LU_CO2"]},
        units=units_emi,
    )

    _Diff1 = _CO2_tce1 - _Total

    if run_history == "True":
        df_hist = pd.read_csv(lu_hist_data).set_index("Region")
        df_hist = df_hist.rename(columns={i: int(i) for i in df_hist.columns})
        _Diff1 = _Diff1 - df_hist

    # ---------------------
    # Agriculture (Table 1)
    # ---------------------

    AgricultureWasteBurning = pp_utils._make_zero()
    vars["AFOLU|Biomass Burning"] = AgricultureWasteBurning
    Agriculture = pp_utils._make_zero()
    vars["AFOLU|Agriculture"] = Agriculture

    # ---------------------------
    # Grassland Burning (Table 2)
    # ---------------------------

    GrasslandBurning = pp_utils._make_zero()
    vars["AFOLU|Land|Grassland Burning"] = GrasslandBurning

    # ------------------------
    # Forest Burning (Table 3)
    # ------------------------

    ForestBurning = pp_utils._make_zero()
    vars["AFOLU|Land|Forest Burning"] = ForestBurning

    vars["AFOLU"] = _CO2_GLOBIOM

    # ------------------
    # Aircraft (Table 4)
    # ------------------

    Aircraft = pp_utils._make_zero()
    vars["Energy|Demand|Transportation|Aviation|International"] = Aircraft

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

    vars[
        "Energy|Supply|Gases|Extraction|Combustion"
    ] = _Other_gases_extr_comb + _Diff1 * (
        abs(_Other_gases_extr_comb) / _Total_wo_BECCS
    ).fillna(
        0
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

    vars[
        "Energy|Supply|Liquids|Biomass|Combustion"
    ] = _Other_liquids_biomass_comb + _Diff1 * (
        abs(_Other_liquids_biomass_comb_woBECCS) / _Total_wo_BECCS
    ).fillna(
        0
    )

    vars["Energy|Supply|Liquids|Biomass|Fugitive"] = pp_utils._make_zero()

    vars[
        "Energy|Supply|Liquids|Coal|Combustion"
    ] = _Other_liquids_coal_comb + _Diff1 * (
        abs(_Other_liquids_coal_comb) / _Total_wo_BECCS
    ).fillna(
        0
    )

    vars["Energy|Supply|Liquids|Coal|Fugitive"] = pp_utils._make_zero()

    vars[
        "Energy|Supply|Liquids|Extraction|Combustion"
    ] = _Other_liquids_extr_comb + _Diff1 * (
        abs(_Other_liquids_extr_comb) / _Total_wo_BECCS
    ).fillna(
        0
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

    vars[
        "Energy|Supply|Solids|Extraction|Combustion"
    ] = _Other_solids_coal_trans_comb + _Diff1 * (
        abs(_Other_solids_coal_trans_comb) / _Total_wo_BECCS
    ).fillna(
        0
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

    vars["Energy|Demand|Other Sector"] = _FE_Feedstocks - _Biogas_fs + _Hydrogen_fs

    # --------------------------------------------
    # Industrial process and product use (Table 8)
    # --------------------------------------------

    Cement = _Cement1
    vars["Industrial Processes"] = Cement

    # --------------------------------
    # International shipping (Table 9)
    # --------------------------------

    _Bunker1 = pp.act("CO2s_TCE") * mu["conv_c2co2"]
    Bunker = _Bunker1
    vars["Energy|Demand|Transportation|Shipping|International"] = Bunker

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
        + vars["Energy|Demand|Transportation|Shipping|International"]
        + vars["Energy|Demand|AFOFI"]
        + vars["Energy|Demand|Industry"]
        + vars["Energy|Demand|Residential and Commercial"]
        + vars["Energy|Demand|Transportation|Road Rail and Domestic Shipping"]
        + vars["Energy|Demand|Transportation|Aviation|International"]
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

    _Cogen = pp.inp("po_turbine", units, inpfilter={"commodity": ["electr"]})

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
            "gas_htfc",
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
    _H2gas = pp.out("h2_mix", units)
    _Coalgas = pp.out("coal_gas", units)

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out("gas_cc") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    _gas_ppl_shr = (pp.out("gas_ppl") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    # Example of a set
    _gas_inp_tecs = [
        "gas_ppl",
        "gas_cc",
        "gas_cc_ccs",
        "gas_ct",
        "gas_htfc",
        "gas_hpl",
        "meth_ng",
        "meth_ng_ccs",
        "h2_smr",
        "h2_smr_ccs",
        "gas_t_d",
        "gas_t_d_ch4",
    ]

    _totgas = pp.inp(_gas_inp_tecs, units, inpfilter={"commodity": ["gas"]})
    _Frac = (_Cogen / _Potential).fillna(0)
    _BGas_share = (_Biogas / _totgas).fillna(0)
    _SynGas_share = ((_Biogas + _H2gas + _Coalgas) / _totgas).fillna(0)

    # -------------------
    # Primary Energy Coal
    # -------------------

    vars["Coal"] = (
        pp.inp(
            ["coal_extr_ch4", "coal_extr", "lignite_extr", "coal_imp"],
            units,
            inpfilter={"commodity": ["coal", "lignite"]},
        )
        - pp.inp("coal_exp", units)
        + pp.inp(["meth_bunker"], units)
    )

    # Note OFR 20180412: Correction inserted. scrubber output per vintage cannot
    # be divided by powerplant eff per vintage. In most cases this does work except
    # if the powerplant doesnt exist anymore, or if vintaging is switched on.
    # In either case, the efficiency with which the scrubber output is divided
    # is a weighted average based on the powerplant activity
    vars["Coal|w/ CCS"] = (
        _pe_wCCSretro(
            "coal_ppl",
            "c_ppl_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["coal"]},
            units=units,
        )
        + _pe_wCCSretro(
            "igcc",
            "igcc_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["coal"]},
            units=units,
        )
        + pp.inp(
            ["igcc_ccs", "coal_adv_ccs", "meth_coal_ccs", "syn_liq_ccs", "h2_coal_ccs"],
            units,
            inpfilter={"commodity": ["coal"]},
        )
    )

    vars["Coal|w/o CCS"] = vars["Coal"] - vars["Coal|w/ CCS"]

    # ------------------
    # Primary Energy Oil
    # ------------------

    vars["Oil|w/o CCS"] = (
        pp.inp(
            [
                "oil_extr_1_ch4",
                "oil_extr_2_ch4",
                "oil_extr_3_ch4",
                "oil_extr_1",
                "oil_extr_2",
                "oil_extr_3",
            ],
            units,
            inpfilter={"level": ["resource"]},
        )
        + (
            pp.out(
                [
                    "oil_extr_4_ch4",
                    "oil_extr_4",
                    "oil_extr_5",
                    "oil_extr_6",
                    "oil_extr_7",
                    "oil_extr_8",
                ],
                units,
            )
            / 0.9
        ).fillna(0)
        + pp.inp(["oil_imp", "foil_imp", "loil_imp"], units)
        - pp.inp(["oil_exp", "foil_exp", "loil_exp"], units)
        + pp.inp(["foil_bunker", "loil_bunker"], units)
    )

    # ------------------
    # Primary Energy Gas
    # ------------------

    vars["Gas"] = (
        pp.inp(
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
            units,
            inpfilter={"level": ["resource"]},
        )
        + pp.inp(["LNG_imp", "gas_imp"], units)
        - pp.inp(
            [
                "LNG_exp",
                "gas_exp_nam",
                "gas_exp_weu",
                "gas_exp_eeu",
                "gas_exp_pao",
                "gas_exp_cpa",
                "gas_exp_afr",
                "gas_exp_sas",
                "gas_exp_pas",
                "gas_exp_scs",
                "gas_exp_cas",
                "gas_exp_ubm",
                "gas_exp_rus",
            ],
            units,
        )
        + pp.inp(["LNG_bunker"], units)
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
        + _pe_wCCSretro(
            "gas_htfc",
            "gfc_co2scr",
            group,
            inpfilter={"level": ["secondary"], "commodity": ["gas"]},
            units=units,
        )
        + pp.inp(["gas_cc_ccs", "h2_smr_ccs"], units, inpfilter={"commodity": ["gas"]})
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
            + _se_elec_po_turb(
                "nuc_fbr",
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
            + _se_heat_po_turb(
                "nuc_fbr",
                units,
                _Frac,
                outfilter={"level": ["secondary"], "commodity": ["electr"]},
            )
            / heat_factor
        )
    else:
        vars["Nuclear"] = pp.out(
            ["nuc_lc", "nuc_hc", "nuc_fbr"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )

    # ----------------------
    # Primary Energy Biomass
    # ----------------------

    vars["Biomass"] = pp.out(
        ["biomass_imp", "land_use_biomass"], units, outfilter={"commodity": ["biomass"]}
    ) - pp.inp(["biomass_exp"], units, inpfilter={"commodity": ["biomass"]})

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
        + pp.inp("bio_istig_ccs", units, inpfilter={"commodity": ["biomass"]})
        + pp.inp(
            ["h2_bio_ccs", "eth_bio_ccs", "liq_bio_ccs"],
            units,
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
        [
            "wind_res1",
            "wind_res2",
            "wind_res3",
            "wind_res4",
            "wind_ref1",
            "wind_ref2",
            "wind_ref3",
            "wind_ref4",
            "wind_ref5",
            "wind_res_hist_2005",
            "wind_res_hist_2010",
            "wind_res_hist_2015",
            "wind_res_hist_2020",
            "wind_ref_hist_2005",
            "wind_ref_hist_2010",
            "wind_ref_hist_2015",
            "wind_ref_hist_2020",
        ],
        units,
    ) - pp.inp(
        ["wind_curtailment1", "wind_curtailment2", "wind_curtailment3"],
        units,
        inpfilter={"commodity": ["electr"]},
    )

    if method == "substitution":
        vars["Wind"] /= elec_factor

    # --------------------
    # Primary Energy Hydro
    # --------------------

    vars["Hydro"] = pp.out(["hydro_hc", "hydro_lc"], units)

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
        [
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
            "solar_res_hist_2005",
            "solar_res_hist_2010",
            "solar_res_hist_2015",
            "solar_res_hist_2020",
        ],
        units,
    )

    _solar_curtailment_elec = pp.inp(
        ["solar_curtailment1", "solar_curtailment2", "solar_curtailment3"],
        units,
        inpfilter={"commodity": ["electr"]},
    )

    _solar_heat = pp.out(["solar_rc", "solar_i"], units)

    _csp_elec = pp.out(
        [
            "csp_sm1_res",
            "csp_sm1_res1",
            "csp_sm1_res2",
            "csp_sm1_res3",
            "csp_sm1_res4",
            "csp_sm1_res5",
            "csp_sm1_res6",
            "csp_sm1_res7",
            "csp_res_hist_2005",
            "csp_res_hist_2010",
            "csp_res_hist_2015",
            "csp_res_hist_2020",
            "csp_sm3_res",
            "csp_sm3_res1",
            "csp_sm3_res2",
            "csp_sm3_res3",
            "csp_sm3_res4",
            "csp_sm3_res5",
            "csp_sm3_res6",
            "csp_sm3_res7",
        ],
        units,
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

    _geothermal_elec = _se_elec_po_turb("geo_ppl", units, _Frac)
    _geothermal_heat = _se_heat_po_turb("geo_ppl", units, _Frac) + pp.out(
        ["geo_hpl"], units
    )

    if method == "substitution":
        vars["Geothermal"] = (_geothermal_elec / elec_factor).fillna(0) + (
            _geothermal_heat / heat_factor
        ).fillna(0)
    else:
        vars["Geothermal"] = _geothermal_elec + _geothermal_heat

    # Primary Energy - SE Trade
    vars["Secondary Energy Trade"] = pp.out(
        [
            "meth_imp",
            "lh2_imp",
            "eth_imp",
            "elec_imp",
            "elec_imp_africa",
            "elec_imp_america",
            "elec_imp_asia",
            "elec_imp_eurasia",
            "elec_imp_eur_afr",
            "elec_imp_asia_afr",
        ],
        units,
    ) - pp.inp(
        [
            "meth_exp",
            "lh2_exp",
            "eth_exp",
            "elec_exp",
            "elec_exp_africa",
            "elec_exp_america",
            "elec_exp_asia",
            "elec_exp_eurasia",
            "elec_exp_eur_afr",
            "elec_exp_asia_afr",
        ],
        units,
    )

    # --------------------
    # Priamry Energy Other
    # --------------------

    vars["Other"] = pp.inp("LH2_bunker", units)

    if method != "substitution":

        # ------------------------------------
        # Primary Energy Electricity from coal
        # ------------------------------------

        vars["Coal|Electricity|w/o CCS"] = (
            _pe_elec_woCCSretro(
                "coal_ppl",
                "c_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["coal"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_woCCSretro(
                "igcc",
                "igcc_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["coal"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_po_turb(
                "coal_adv", group, units, _Frac, inpfilter={"commodity": ["coal"]}
            )
            + _pe_elec_po_turb(
                "coal_ppl_u", group, units, _Frac, inpfilter={"commodity": ["coal"]}
            )
        )

        vars["Coal|Electricity|w/ CCS"] = (
            _pe_elec_wCCSretro(
                "coal_ppl",
                "c_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["coal"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_wCCSretro(
                "igcc",
                "igcc_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["coal"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_po_turb(
                "coal_adv_ccs", group, units, _Frac, inpfilter={"commodity": ["coal"]}
            )
            + _pe_elec_po_turb(
                "igcc_ccs", group, units, _Frac, inpfilter={"commodity": ["coal"]}
            )
        )

        # -----------------------------------
        # Primary Energy Electricity from gas
        # -----------------------------------

        _ElecGas_woCCS = (
            _pe_elec_woCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_ppl_shr,
            )
            + _pe_elec_woCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
            + _pe_elec_woCCSretro(
                "gas_htfc",
                "gfc_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_po_turb(
                "gas_ct", group, units, _Frac, inpfilter={"commodity": ["gas"]}
            )
        )

        vars["Gas|Electricity|w/o CCS"] = _ElecGas_woCCS * (1 - _BGas_share)

        _ElecGas_wCCS = (
            _pe_elec_wCCSretro(
                "gas_cc",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_cc_shr,
            )
            + _pe_elec_wCCSretro(
                "gas_ppl",
                "g_ppl_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
                share=_gas_ppl_shr,
            )
            + _pe_elec_wCCSretro(
                "gas_htfc",
                "gfc_co2scr",
                group,
                inpfilter={"level": ["secondary"], "commodity": ["gas"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_po_turb(
                "gas_cc_ccs", group, units, _Frac, inpfilter={"commodity": ["gas"]}
            )
        )

        vars["Gas|Electricity|w/ CCS"] = _ElecGas_wCCS * (1 - _BGas_share)

        # -----------------------------------
        # Primary Energy Electricity from oil
        # -----------------------------------

        vars["Oil|Electricity|w/o CCS"] = (
            _pe_elec_po_turb(
                "foil_ppl", group, units, _Frac, inpfilter={"commodity": ["fueloil"]}
            )
            + _pe_elec_po_turb(
                "loil_ppl", group, units, _Frac, inpfilter={"commodity": ["lightoil"]}
            )
            + _pe_elec_po_turb(
                "oil_ppl", group, units, _Frac, inpfilter={"commodity": ["crudeoil"]}
            )
            + _pe_elec_po_turb(
                "loil_cc", group, units, _Frac, inpfilter={"commodity": ["lightoil"]}
            )
        )

        vars["Oil|Electricity|w/ CCS"] = pp_utils._make_zero()

        # ---------------------------------------
        # Primary Energy Electricity from biomass
        # ---------------------------------------

        vars["Biomass|Electricity|w/o CCS"] = (
            _pe_elec_woCCSretro(
                "bio_ppl",
                "bio_ppl_co2scr",
                group,
                inpfilter={"level": ["primary"], "commodity": ["biomass"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_po_turb(
                "bio_istig", group, units, _Frac, inpfilter={"commodity": ["biomass"]}
            )
            + _ElecGas_woCCS * _BGas_share
        )

        vars["Biomass|Electricity|w/ CCS"] = (
            _pe_elec_wCCSretro(
                "bio_ppl",
                "bio_ppl_co2scr",
                group,
                inpfilter={"level": ["primary"], "commodity": ["biomass"]},
                units=units,
                _Frac=_Frac,
            )
            + _pe_elec_po_turb(
                "bio_istig_ccs",
                group,
                units,
                _Frac,
                inpfilter={"commodity": ["biomass"]},
            )
            + _ElecGas_wCCS * _BGas_share
        )

        # -----------------------------------
        # Primary Energy from biomass (other)
        # -----------------------------------

        vars["Biomass|Modern"] = pp.land_out(
            lu_out_filter={"level": ["land_use"], "commodity": ["bioenergy"]},
            units=units,
        ) - pp.inp("biomass_nc", units)

        vars["Biomass|Residues"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["SawmillResidues_G4M"],
            }
        )

        vars["Biomass|Traditional"] = pp.inp("biomass_nc", units)

        vars["Biomass|Energy Crops"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["PlantationBiomass"],
            }
        )

        vars["Biomass|1st Generation"] = (
            pp.land_out(
                lu_out_filter={
                    "level": ["land_use_reporting"],
                    "commodity": ["Biodiesel_G1"],
                }
            )
            + pp.land_out(
                lu_out_filter={
                    "level": ["land_use_reporting"],
                    "commodity": ["Bioethanol_G1"],
                }
            )
        ) / 0.41

        # ----------------------------------------------------
        # Additonal reporting for GAINS diagnostic for biomass
        # ----------------------------------------------------

        vars["Biomass|Gases"] = pp.inp(
            ["gas_bio"], units, inpfilter={"commodity": ["biomass"]}
        )

        vars["Biomass|Hydrogen"] = pp.inp(
            ["h2_bio", "h2_bio_ccs"], units, inpfilter={"commodity": ["biomass"]}
        )

        vars["Biomass|Liquids"] = pp.inp(
            ["eth_bio", "eth_bio_ccs", "liq_bio", "liq_bio_ccs"],
            units,
            inpfilter={"commodity": ["biomass"]},
        )

        vars["Biomass|Solids"] = pp.inp(
            ["biomass_t_d"], units, inpfilter={"commodity": ["biomass"]}
        )

        # -------------------------------------------------
        # Additonal reporting for GAINS diagnostic for coal
        # -------------------------------------------------

        vars["Coal|Gases"] = pp.inp(
            ["coal_gas"], units, inpfilter={"commodity": ["coal"]}
        )

        vars["Coal|Hydrogen"] = pp.inp(
            ["h2_coal", "h2_coal_ccs"], units, inpfilter={"commodity": ["coal"]}
        )

        vars["Coal|Liquids"] = pp.inp(
            ["syn_liq", "syn_liq_ccs", "meth_coal", "meth_coal_ccs"],
            units,
            inpfilter={"commodity": ["coal"]},
        )

        vars["Coal|Solids"] = pp.inp(
            [
                "coal_t_d-rc-06p",
                "coal_t_d-in-06p",
                "coal_t_d-in-SO2",
                "coal_t_d-rc-SO2",
                "coal_t_d",
            ],
            units,
            inpfilter={"commodity": ["coal"]},
        )

        # ------------------------------------------------
        # Additonal reporting for GAINS diagnostic for gas
        # ------------------------------------------------

        vars["Gas|Gases"] = pp.inp(
            ["gas_t_d", "gas_t_d_ch4"], units, inpfilter={"commodity": ["gas"]}
        ) * (1 - _SynGas_share)

        vars["Gas|Hydrogen"] = pp.inp(
            ["h2_smr", "h2_smr_ccs"], units, inpfilter={"commodity": ["gas"]}
        )

        vars["Gas|Liquids"] = pp.inp(
            ["meth_ng", "meth_ng_ccs"], units, inpfilter={"commodity": ["gas"]}
        )

        vars["Gas|Solids"] = pp_utils._make_zero()

        # ------------------------------------------------
        # Additonal reporting for GAINS diagnostic for oil
        # ------------------------------------------------

        vars["Oil|Gases"] = pp_utils._make_zero()
        vars["Oil|Hydrogen"] = pp_utils._make_zero()
        vars["Oil|Liquids"] = pp.inp(
            ["loil_t_d", "foil_t_d"],
            units,
            inpfilter={"commodity": ["fueloil", "lightoil"]},
        )

        vars["Oil|Solids"] = pp_utils._make_zero()

    df = pp_utils.make_outputdf(vars, units)
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

    # --------------------------------
    # Calculation of helping variables
    # --------------------------------

    _Cogen = pp.inp("po_turbine", inpfilter={"commodity": ["electr"]})

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
            "gas_htfc",
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
    )

    _Biogas = pp.out("gas_bio")

    _gas_inp_tecs = [
        "gas_ppl",
        "gas_cc",
        "gas_cc_ccs",
        "gas_ct",
        "gas_htfc",
        "gas_hpl",
        "meth_ng",
        "meth_ng_ccs",
        "h2_smr",
        "h2_smr_ccs",
        "gas_t_d",
        "gas_t_d_ch4",
    ]

    _totgas = pp.inp(_gas_inp_tecs, inpfilter={"commodity": ["gas"]})
    _Frac = (_Cogen / _Potential).fillna(0)
    _BGas_share = (_Biogas / _totgas).fillna(0)

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out("gas_cc") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    _gas_ppl_shr = (pp.out("gas_ppl") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    inpfilter = {"level": ["water_supply"], "commodity": ["freshwater_supply"]}
    outfilter = {"level": ["secondary"], "commodity": ["electr"]}
    emiffilter = {"emission": ["fresh_wastewater"]}

    # --------------------
    # Once Through Cooling
    # --------------------

    _gas_ot_fresh_wo_CCS = pp.inp(
        ["gas_cc__ot_fresh", "gas_ppl__ot_fresh"], units, inpfilter=inpfilter
    )

    _gas_ot_fresh_w_CCS = pp.inp(["gas_cc_ccs__ot_fresh"], units, inpfilter=inpfilter)

    _coal_ot_fresh_wo_CCS = pp.inp(
        [
            "coal_adv__ot_fresh",
            "coal_ppl__ot_fresh",
            "coal_ppl_u__ot_fresh",
            "igcc__ot_fresh",
        ],
        units,
        inpfilter=inpfilter,
    )

    _coal_ot_fresh_w_CCS = pp.inp(
        ["coal_adv_ccs__ot_fresh", "igcc_ccs__ot_fresh"], units, inpfilter=inpfilter
    )

    _oil_ot_fresh_wo_CCS = pp.inp(
        ["foil_ppl__ot_fresh", "loil_cc__ot_fresh", "loil_ppl__ot_fresh"],
        units,
        inpfilter=inpfilter,
    )

    _bio_ot_fresh_wo_CCS = pp.inp(
        ["bio_istig__ot_fresh", "bio_ppl__ot_fresh"], units, inpfilter=inpfilter
    )

    _bio_ot_fresh_w_CCS = pp.inp(
        ["bio_istig_ccs__ot_fresh"], units, inpfilter=inpfilter
    )

    _geo_ot_fresh_wo_CCS = pp.inp(["geo_ppl__ot_fresh"], units, inpfilter=inpfilter)

    _nuc_ot_fresh_wo_CCS = pp.inp(
        ["nuc_hc__ot_fresh", "nuc_lc__ot_fresh"], units, inpfilter=inpfilter
    )

    _solar_ot_fresh_wo_CCS = pp.inp(
        "solar_th_ppl__ot_fresh", units, inpfilter=inpfilter
    )

    if method == "consumption":
        _gas_ot_fresh_wo_CCS -= pp.act_emif(
            ["gas_cc__ot_fresh", "gas_ppl__ot_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _gas_ot_fresh_w_CCS -= pp.act_emif(
            ["gas_cc_ccs__ot_fresh"], units=units, emiffilter=emiffilter
        )

        _coal_ot_fresh_wo_CCS -= pp.act_emif(
            [
                "coal_adv__ot_fresh",
                "coal_ppl__ot_fresh",
                "coal_ppl_u__ot_fresh",
                "igcc__ot_fresh",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        _coal_ot_fresh_w_CCS -= pp.act_emif(
            ["coal_adv_ccs__ot_fresh", "igcc_ccs__ot_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _oil_ot_fresh_wo_CCS -= pp.act_emif(
            ["foil_ppl__ot_fresh", "loil_cc__ot_fresh", "loil_ppl__ot_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _bio_ot_fresh_wo_CCS -= pp.act_emif(
            ["bio_istig__ot_fresh", "bio_ppl__ot_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _bio_ot_fresh_w_CCS -= pp.act_emif(
            ["bio_istig_ccs__ot_fresh"], units=units, emiffilter=emiffilter
        )

        _geo_ot_fresh_wo_CCS -= pp.act_emif(
            ["geo_ppl__ot_fresh"], units=units, emiffilter=emiffilter
        )

        _nuc_ot_fresh_wo_CCS -= pp.act_emif(
            ["nuc_hc__ot_fresh", "nuc_lc__ot_fresh"], units=units, emiffilter=emiffilter
        )

        _solar_ot_fresh_wo_CCS -= pp.act_emif(
            ["solar_th_ppl__ot_fresh"], units=units, emiffilter=emiffilter
        )

    # -----------
    # Closed Loop
    # -----------

    _gas_cl_fresh_wo_CCS = pp.inp(
        ["gas_cc__cl_fresh", "gas_ppl__cl_fresh"], units, inpfilter=inpfilter
    )

    _gas_cl_fresh_w_CCS = pp.inp(["gas_cc_ccs__cl_fresh"], units, inpfilter=inpfilter)

    _coal_cl_fresh_wo_CCS = pp.inp(
        [
            "coal_adv__cl_fresh",
            "coal_ppl__cl_fresh",
            "coal_ppl_u__cl_fresh",
            "igcc__cl_fresh",
        ],
        units,
        inpfilter=inpfilter,
    )

    _coal_cl_fresh_w_CCS = pp.inp(
        ["coal_adv_ccs__cl_fresh", "igcc_ccs__cl_fresh"], units, inpfilter=inpfilter
    )

    _oil_cl_fresh_wo_CCS = pp.inp(
        ["foil_ppl__cl_fresh", "loil_cc__cl_fresh", "loil_ppl__cl_fresh"],
        units,
        inpfilter=inpfilter,
    )

    _bio_cl_fresh_wo_CCS = pp.inp(
        ["bio_istig__cl_fresh", "bio_ppl__cl_fresh"], units, inpfilter=inpfilter
    )

    _bio_cl_fresh_w_CCS = pp.inp(
        ["bio_istig_ccs__cl_fresh"], units, inpfilter=inpfilter
    )

    _geo_cl_fresh_wo_CCS = pp.inp(["geo_ppl__cl_fresh"], units, inpfilter=inpfilter)

    _nuc_cl_fresh_wo_CCS = pp.inp(
        ["nuc_hc__cl_fresh", "nuc_lc__cl_fresh"], units, inpfilter=inpfilter
    )

    _solar_cl_fresh_wo_CCS = pp.inp(
        "solar_th_ppl__cl_fresh", units, inpfilter=inpfilter
    )

    if method == "consumption":
        _gas_cl_fresh_wo_CCS -= pp.act_emif(
            ["gas_cc__cl_fresh", "gas_ppl__cl_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _gas_cl_fresh_w_CCS -= pp.act_emif(
            ["gas_cc_ccs__cl_fresh"], units=units, emiffilter=emiffilter
        )

        _coal_cl_fresh_wo_CCS -= pp.act_emif(
            [
                "coal_adv__cl_fresh",
                "coal_ppl__cl_fresh",
                "coal_ppl_u__cl_fresh",
                "igcc__cl_fresh",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        _coal_cl_fresh_w_CCS -= pp.act_emif(
            ["coal_adv_ccs__cl_fresh", "igcc_ccs__cl_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _oil_cl_fresh_wo_CCS -= pp.act_emif(
            ["foil_ppl__cl_fresh", "loil_cc__cl_fresh", "loil_ppl__cl_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _bio_cl_fresh_wo_CCS -= pp.act_emif(
            ["bio_istig__cl_fresh", "bio_ppl__cl_fresh"],
            units=units,
            emiffilter=emiffilter,
        )

        _bio_cl_fresh_w_CCS -= pp.act_emif(
            ["bio_istig_ccs__cl_fresh"], units=units, emiffilter=emiffilter
        )

        _geo_cl_fresh_wo_CCS -= pp.act_emif(
            ["geo_ppl__cl_fresh"], units=units, emiffilter=emiffilter
        )

        _nuc_cl_fresh_wo_CCS -= pp.act_emif(
            ["nuc_hc__cl_fresh", "nuc_lc__cl_fresh"], units=units, emiffilter=emiffilter
        )

        _solar_cl_fresh_wo_CCS -= pp.act_emif(
            ["solar_th_ppl__cl_fresh"], units=units, emiffilter=emiffilter
        )

    # -----------------------------
    # Once Through Cooling (SALINE)
    # -----------------------------
    sinpfilter = {"level": ["water_supply"], "commodity": ["saline_supply_ppl"]}

    _gas_ot_saline_wo_CCS = pp.inp(
        ["gas_cc__ot_saline", "gas_ppl__ot_saline"], units, inpfilter=sinpfilter
    )

    _gas_ot_saline_w_CCS = pp.inp(
        ["gas_cc_ccs__ot_saline"], units, inpfilter=sinpfilter
    )

    _coal_ot_saline_wo_CCS = pp.inp(
        [
            "coal_adv__ot_saline",
            "coal_ppl__ot_saline",
            "coal_ppl_u__ot_saline",
            "igcc__ot_saline",
        ],
        units,
        inpfilter=sinpfilter,
    )

    _coal_ot_saline_w_CCS = pp.inp(
        ["coal_adv_ccs__ot_saline", "igcc_ccs__ot_saline"], units, inpfilter=sinpfilter
    )

    _oil_ot_saline_wo_CCS = pp.inp(
        ["foil_ppl__ot_saline", "loil_cc__ot_saline", "loil_ppl__ot_saline"],
        units,
        inpfilter=sinpfilter,
    )

    _bio_ot_saline_wo_CCS = pp.inp(
        ["bio_istig__ot_saline", "bio_ppl__ot_saline"], units, inpfilter=sinpfilter
    )

    _bio_ot_saline_w_CCS = pp.inp(
        ["bio_istig_ccs__ot_saline"], units, inpfilter=sinpfilter
    )

    _geo_ot_saline_wo_CCS = pp.inp(["geo_ppl__ot_saline"], units, inpfilter=sinpfilter)

    _nuc_ot_saline_wo_CCS = pp.inp(
        ["nuc_hc__ot_saline", "nuc_lc__ot_saline"], units, inpfilter=sinpfilter
    )

    _solar_ot_saline_wo_CCS = pp.inp(
        "solar_th_ppl__ot_saline", units, inpfilter=sinpfilter
    )

    if method == "consumption":
        _gas_ot_saline_wo_CCS -= pp.act_emif(
            ["gas_cc__ot_saline", "gas_ppl__ot_saline"],
            units=units,
            emiffilter=emiffilter,
        )

        _gas_ot_saline_w_CCS -= pp.act_emif(
            ["gas_cc_ccs__ot_saline"], units=units, emiffilter=emiffilter
        )

        _coal_ot_saline_wo_CCS -= pp.act_emif(
            [
                "coal_adv__ot_saline",
                "coal_ppl__ot_saline",
                "coal_ppl_u__ot_saline",
                "igcc__ot_saline",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        _coal_ot_saline_w_CCS -= pp.act_emif(
            ["coal_adv_ccs__ot_saline", "igcc_ccs__ot_saline"],
            units=units,
            emiffilter=emiffilter,
        )

        _oil_ot_saline_wo_CCS -= pp.act_emif(
            ["foil_ppl__ot_saline", "loil_cc__ot_saline", "loil_ppl__ot_saline"],
            units=units,
            emiffilter=emiffilter,
        )

        _bio_ot_saline_wo_CCS -= pp.act_emif(
            ["bio_istig__ot_saline", "bio_ppl__ot_saline"],
            units=units,
            emiffilter=emiffilter,
        )

        _bio_ot_saline_w_CCS -= pp.act_emif(
            ["bio_istig_ccs__ot_saline"], units=units, emiffilter=emiffilter
        )

        _geo_ot_saline_wo_CCS -= pp.act_emif(
            ["geo_ppl__ot_saline"], units=units, emiffilter=emiffilter
        )

        _nuc_ot_saline_wo_CCS -= pp.act_emif(
            ["nuc_hc__ot_saline", "nuc_lc__ot_saline"],
            units=units,
            emiffilter=emiffilter,
        )

        _solar_ot_saline_wo_CCS -= pp.act_emif(
            ["solar_th_ppl__ot_saline"], units=units, emiffilter=emiffilter
        )

    # -----------
    # Dry cooling
    # -----------

    _gas_air = pp.inp(["gas_cc__air", "gas_ppl__air"], units, inpfilter=inpfilter)

    _coal_air = pp.inp(
        ["coal_adv__air", "coal_ppl__air", "coal_ppl_u__air", "igcc__air"],
        units,
        inpfilter=inpfilter,
    )

    _oil_air = pp.inp(
        ["foil_ppl__air", "loil_cc__air", "loil_ppl__air"], units, inpfilter=inpfilter
    )

    _bio_air = pp.inp(["bio_istig__air", "bio_ppl__air"], units, inpfilter=inpfilter)

    _geo_air = pp.inp(["geo_ppl__air"], units, inpfilter=inpfilter)
    _solar_air = pp.inp("solar_th_ppl__air", units, inpfilter=inpfilter)

    # -----------------------
    # Water use for coal elec
    # -----------------------

    _Cooling_ele_coal_wCCS = (
        _coal_ot_fresh_w_CCS + _coal_cl_fresh_w_CCS + _coal_ot_saline_w_CCS
    )

    _Direct_ele_coal_wCCS = (
        _pe_elec_wCCSretro(
            "coal_ppl",
            "c_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
        )
        + _pe_elec_wCCSretro(
            "igcc", "igcc_co2scr", group, inpfilter=inpfilter, units=units, _Frac=_Frac
        )
        + _pe_elec_po_turb("coal_adv_ccs", group, units, _Frac, inpfilter=inpfilter)
        + _pe_elec_po_turb("igcc_ccs", group, units, _Frac, inpfilter=inpfilter)
        + _out_div_eff(
            ["meth_coal_ccs", "h2_coal_ccs", "syn_liq_ccs"], group, inpfilter, outfilter
        )
    )

    vars["Electricity|Coal|w/ CCS"] = _Direct_ele_coal_wCCS + _Cooling_ele_coal_wCCS

    _Cooling_ele_coal_woCCS = (
        _coal_ot_fresh_wo_CCS
        + _coal_cl_fresh_wo_CCS
        + _coal_ot_saline_wo_CCS
        + _coal_air
    )

    _Direct_ele_coal_woCCS = (
        _pe_elec_woCCSretro(
            "coal_ppl",
            "c_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
        )
        + _pe_elec_woCCSretro(
            "igcc", "igcc_co2scr", group, inpfilter=inpfilter, units=units, _Frac=_Frac
        )
        + _pe_elec_po_turb("coal_adv", group, units, _Frac, inpfilter=inpfilter)
        + _pe_elec_po_turb("coal_ppl_u", group, units, _Frac, inpfilter=inpfilter)
        + _out_div_eff(["meth_coal", "h2_coal", "syn_liq"], group, inpfilter, outfilter)
    )

    vars["Electricity|Coal|w/o CCS"] = _Direct_ele_coal_woCCS + _Cooling_ele_coal_woCCS

    # -----------------------------
    # Water use for electricity Gas
    # -----------------------------

    _Cooling_ele_gas_wCCS = (
        _gas_ot_fresh_w_CCS + _gas_cl_fresh_w_CCS + _gas_ot_saline_w_CCS
    )

    _Direct_ele_gas_wCCS = (
        _pe_elec_wCCSretro(
            "gas_cc",
            "g_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
            share=_gas_cc_shr,
        )
        + _pe_elec_wCCSretro(
            "gas_ppl",
            "g_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
            share=_gas_ppl_shr,
        )
        + _pe_elec_wCCSretro(
            "gas_htfc",
            "gfc_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
        )
        + _pe_elec_po_turb("gas_cc_ccs", group, units, _Frac, inpfilter=inpfilter)
        + _out_div_eff(["h2_smr_ccs"], group, inpfilter, outfilter)
    )

    vars["Electricity|Gas|w/ CCS"] = (_Direct_ele_gas_wCCS + _Cooling_ele_gas_wCCS) * (
        1 - _BGas_share
    )

    _Cooling_ele_gas_woCCS = (
        _gas_ot_fresh_wo_CCS + _gas_cl_fresh_wo_CCS + _gas_ot_saline_wo_CCS + _gas_air
    )

    _Direct_ele_gas_woCCS = (
        _pe_elec_woCCSretro(
            "gas_ppl",
            "g_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
            share=_gas_ppl_shr,
        )
        + _pe_elec_woCCSretro(
            "gas_cc",
            "g_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
            share=_gas_cc_shr,
        )
        + _pe_elec_woCCSretro(
            "gas_htfc",
            "gfc_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
        )
        + _pe_elec_po_turb("gas_ct", group, units, _Frac, inpfilter=inpfilter)
        + _out_div_eff(["h2_smr"], group, inpfilter, outfilter)
    )

    vars["Electricity|Gas|w/o CCS"] = (
        _Direct_ele_gas_woCCS + _Cooling_ele_gas_woCCS
    ) * (1 - _BGas_share)

    # -----------------------------
    # Water use for electricity bio
    # -----------------------------

    _Cooling_ele_bio_wCCS = (
        _bio_ot_fresh_w_CCS + _bio_cl_fresh_w_CCS + _bio_ot_saline_w_CCS
    )

    _Direct_ele_bio_wCCS = (
        _pe_elec_wCCSretro(
            "bio_ppl",
            "bio_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
        )
        + _pe_elec_po_turb("bio_istig_ccs", group, units, _Frac, inpfilter=inpfilter)
        + _out_div_eff(
            ["h2_bio_ccs", "eth_bio_ccs", "liq_bio_ccs"], group, inpfilter, outfilter
        )
    )

    vars["Electricity|Biomass|w/ CCS"] = (
        _Direct_ele_bio_wCCS
        + _Cooling_ele_bio_wCCS
        + (_Direct_ele_gas_wCCS + _Cooling_ele_gas_wCCS) * _BGas_share
    )

    _Cooling_ele_bio_woCCS = (
        _bio_ot_fresh_wo_CCS + _bio_cl_fresh_wo_CCS + _bio_ot_saline_wo_CCS + _bio_air
    )

    _Direct_ele_bio_woCCS = (
        _pe_elec_woCCSretro(
            "bio_ppl",
            "bio_ppl_co2scr",
            group,
            inpfilter=inpfilter,
            units=units,
            _Frac=_Frac,
        )
        + _pe_elec_po_turb("bio_istig", group, units, _Frac, inpfilter=inpfilter)
        + _out_div_eff(["h2_bio", "eth_bio", "liq_bio"], group, inpfilter, outfilter)
    )

    vars["Electricity|Biomass|w/o CCS"] = (
        _Direct_ele_bio_woCCS
        + _Cooling_ele_bio_woCCS
        + (_Direct_ele_gas_woCCS + _Cooling_ele_gas_woCCS) * _BGas_share
    )

    # -----------------------------
    # Water use for electricity oil
    # -----------------------------

    _Cooling_ele_oil = (
        _oil_ot_fresh_wo_CCS + _oil_cl_fresh_wo_CCS + _oil_ot_saline_wo_CCS + _oil_air
    )

    _Direct_ele_oil = (
        _pe_elec_po_turb("foil_ppl", group, units, _Frac, inpfilter=inpfilter)
        + _pe_elec_po_turb("loil_ppl", group, units, _Frac, inpfilter=inpfilter)
        + _pe_elec_po_turb("oil_ppl", group, units, _Frac, inpfilter=inpfilter)
        + _pe_elec_po_turb("loil_cc", group, units, _Frac, inpfilter=inpfilter)
    )

    vars["Electricity|Oil"] = _Direct_ele_oil + _Cooling_ele_oil

    # ------------------------------------
    # Water use for electricity geothermal
    # ------------------------------------

    _Cooling_ele_geo = (
        _geo_ot_fresh_wo_CCS + _geo_cl_fresh_wo_CCS + _geo_ot_saline_wo_CCS + _geo_air
    )

    _Direct_ele_geo = _pe_elec_po_turb(
        "geo_ppl", group, units, _Frac, inpfilter=inpfilter
    )

    vars["Electricity|Geothermal"] = _Direct_ele_geo + _Cooling_ele_geo

    # ------------------------------------
    # Water use for electricity hydropower
    # ------------------------------------

    # Note Wenji 03.05.2017: Hydropower uses a different commodity than the
    # other technologies.

    vars["Electricity|Hydro"] = pp.inp(
        ["hydro_lc", "hydro_hc"],
        units,
        inpfilter={"level": ["water_supply"], "commodity": ["freshwater_instream"]},
    )

    # ---------------------------------
    # Water use for electricity nuclear
    # ---------------------------------

    _Cooling_ele_nuc = (
        _nuc_ot_fresh_wo_CCS + _nuc_cl_fresh_wo_CCS + _nuc_ot_saline_wo_CCS
    )

    _Direct_ele_nuc = (
        _pe_elec_po_turb("nuc_lc", group, units, _Frac, inpfilter=inpfilter)
        + _pe_elec_po_turb("nuc_hc", group, units, _Frac, inpfilter=inpfilter)
        + _pe_elec_po_turb("nuc_fbr", group, units, _Frac, inpfilter=inpfilter)
    )

    vars["Electricity|Nuclear"] = _Direct_ele_nuc + _Cooling_ele_nuc

    # ------------------------------
    # Water use for electricity wind
    # ------------------------------

    # Note Wenji 02.05.2017: In the GDX file it seems that these technologies
    # have no input

    Wind_onshore = pp.inp(
        [
            "wind_res1",
            "wind_res2",
            "wind_res3",
            "wind_res4",
            "wind_res_hist_2005",
            "wind_res_hist_2010",
            "wind_res_hist_2015",
            "wind_res_hist_2020",
        ],
        units,
        inpfilter=inpfilter,
    )

    Wind_offshore = pp.inp(
        [
            "wind_ref1",
            "wind_ref2",
            "wind_ref3",
            "wind_ref4",
            "wind_ref5",
            "wind_ref_hist_2005",
            "wind_ref_hist_2010",
            "wind_ref_hist_2015",
            "wind_ref_hist_2020",
        ],
        units,
        inpfilter=inpfilter,
    )

    vars["Electricity|Wind"] = Wind_onshore + Wind_offshore

    # -------------------------------
    # Water use for electricity solar
    # -------------------------------

    _Cooling_ele_solar_th = (
        _solar_ot_fresh_wo_CCS
        + _solar_cl_fresh_wo_CCS
        + _solar_ot_saline_wo_CCS
        + _solar_air
    )

    _Direct_ele_solar_th = pp.inp(
        [
            "solar_th_ppl",
            "csp_sm1_res",
            "csp_sm1_res1",
            "csp_sm1_res2",
            "csp_sm1_res3",
            "csp_sm1_res4",
            "csp_sm1_res5",
            "csp_sm1_res6",
            "csp_sm1_res7",
            "csp_res_hist_2005",
            "csp_res_hist_2010",
            "csp_res_hist_2015",
            "csp_res_hist_2020",
            "csp_sm3_res",
            "csp_sm3_res1",
            "csp_sm3_res2",
            "csp_sm3_res3",
            "csp_sm3_res4",
            "csp_sm3_res5",
            "csp_sm3_res6",
            "csp_sm3_res7",
        ],
        units,
        inpfilter=inpfilter,
    )

    vars["Electricity|Solar|CSP"] = _Direct_ele_solar_th + _Cooling_ele_solar_th

    # Note Wenji 02.05.2017: In the GDX file it seems that these technologies
    # have no input

    vars["Electricity|Solar|PV"] = pp.inp(
        [
            "solar_res1",
            "solar_res2",
            "solar_res3",
            "solar_res4",
            "solar_res5",
            "solar_res6",
            "solar_res7",
            "solar_res8",
            "solar_pv_I",
            "solar_pv_RC",
            "solar_res_hist_2005",
            "solar_res_hist_2010",
            "solar_res_hist_2015",
            "solar_res_hist_2020",
        ],
        units,
        inpfilter=inpfilter,
    )

    vars["Electricity|Other"] = pp.inp(
        ["h2_fc_trp", "h2_fc_I", "h2_fc_RC"], units, inpfilter=inpfilter
    )

    vars["Electricity|Dry Cooling"] = (
        _gas_air + _coal_air + _oil_air + _bio_air + _geo_air + _solar_air
    )

    vars["Electricity|Once Through"] = (
        _gas_ot_fresh_wo_CCS
        + _gas_ot_fresh_w_CCS
        + _coal_ot_fresh_wo_CCS
        + _coal_ot_fresh_w_CCS
        + _oil_ot_fresh_wo_CCS
        + _bio_ot_fresh_wo_CCS
        + _bio_ot_fresh_w_CCS
        + _geo_ot_fresh_wo_CCS
        + _nuc_ot_fresh_wo_CCS
        + _solar_ot_fresh_wo_CCS
    )

    vars["Electricity|Sea Cooling"] = (
        _gas_ot_saline_wo_CCS
        + _gas_ot_saline_w_CCS
        + _coal_ot_saline_wo_CCS
        + _coal_ot_saline_w_CCS
        + _oil_ot_saline_wo_CCS
        + _bio_ot_saline_wo_CCS
        + _bio_ot_saline_w_CCS
        + _geo_ot_saline_wo_CCS
        + _nuc_ot_saline_wo_CCS
        + _solar_ot_saline_wo_CCS
    )

    vars["Electricity|Wet Tower"] = (
        _gas_cl_fresh_wo_CCS
        + _gas_cl_fresh_w_CCS
        + _coal_cl_fresh_wo_CCS
        + _coal_cl_fresh_w_CCS
        + _oil_cl_fresh_wo_CCS
        + _bio_cl_fresh_wo_CCS
        + _bio_cl_fresh_w_CCS
        + _geo_cl_fresh_wo_CCS
        + _nuc_cl_fresh_wo_CCS
        + _solar_cl_fresh_wo_CCS
    )

    # ----------
    # Extraction
    # ----------

    vars["Extraction|Coal"] = pp.inp(
        ["coal_extr", "coal_extr_ch4", "lignite_extr"], units, inpfilter=inpfilter
    )

    vars["Extraction|Gas"] = pp.inp(
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
        units,
        inpfilter=inpfilter,
    )

    vars["Extraction|Oil"] = pp.inp(
        [
            "oil_extr_1",
            "oil_extr_2",
            "oil_extr_3",
            "oil_extr_4",
            "oil_extr_5",
            "oil_extr_6",
            "oil_extr_7",
            "oil_extr_8",
            "oil_extr_1_ch4",
            "oil_extr_2_ch4",
            "oil_extr_3_ch4",
            "oil_extr_4_ch4",
        ],
        units,
        inpfilter=inpfilter,
    )

    vars["Extraction|Uranium"] = pp.inp("Uran_extr", units, inpfilter=inpfilter)

    if method == "consumption":
        vars["Extraction|Coal"] -= pp.act_emif(
            ["coal_extr", "coal_extr_ch4", "lignite_extr"],
            units=units,
            emiffilter=emiffilter,
        )

        vars["Extraction|Gas"] -= pp.act_emif(
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
            units=units,
            emiffilter=emiffilter,
        )

        vars["Extraction|Oil"] -= pp.act_emif(
            [
                "oil_extr_1",
                "oil_extr_2",
                "oil_extr_3",
                "oil_extr_4",
                "oil_extr_5",
                "oil_extr_6",
                "oil_extr_7",
                "oil_extr_8",
                "oil_extr_1_ch4",
                "oil_extr_2_ch4",
                "oil_extr_3_ch4",
                "oil_extr_4_ch4",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        vars["Extraction|Uranium"] -= pp.act_emif(
            "Uran_extr", units=units, emiffilter=emiffilter
        )

    # -----
    # Gases
    # -----

    vars["Gases|Biomass"] = pp.inp("gas_bio", units, inpfilter=inpfilter)

    vars["Gases|Coal"] = pp.inp("coal_gas", units, inpfilter=inpfilter)

    if method == "consumption":
        vars["Gases|Biomass"] -= pp.act_emif(
            "gas_bio", units=units, emiffilter=emiffilter
        )

        vars["Gases|Coal"] -= pp.act_emif(
            "coal_gas", units=units, emiffilter=emiffilter
        )

    # ----
    # Heat
    # ----

    _Cooling_heat_gas = pp.inp(
        [
            "gas_hpl__ot_fresh",
            "gas_hpl__cl_fresh",
            "gas_hpl__ot_saline",
            "gas_hpl__air",
        ],
        units,
        inpfilter=inpfilter,
    )

    _Direct_heat_gas = pp.inp("gas_hpl", units, inpfilter=inpfilter)

    vars["Heat|Gas"] = _Cooling_heat_gas + _Direct_heat_gas

    _Cooling_heat_geo = pp.inp(
        [
            "geo_hpl__ot_fresh",
            "geo_hpl__cl_fresh",
            "geo_hpl__ot_saline",
            "geo_hpl__air",
        ],
        units,
        inpfilter=inpfilter,
    )

    _Direct_heat_geo = pp.inp("geo_hpl", units, inpfilter=inpfilter)

    vars["Heat|Geothermal"] = _Cooling_heat_geo + _Direct_heat_geo

    _Cooling_heat_bio = pp.inp(
        [
            "bio_hpl__ot_fresh",
            "bio_hpl__cl_fresh",
            "bio_hpl__ot_saline",
            "bio_hpl__air",
        ],
        units,
        inpfilter=inpfilter,
    )

    _Direct_heat_bio = pp.inp("bio_hpl", units, inpfilter=inpfilter)

    vars["Heat|Biomass"] = _Cooling_heat_bio + _Direct_heat_bio

    vars["Heat|Coal"] = pp.inp("coal_hpl", units, inpfilter=inpfilter)

    _Cooling_heat_oil = pp.inp(
        [
            "foil_hpl__ot_fresh",
            "foil_hpl__cl_fresh",
            "foil_hpl__ot_saline",
            "foil_hpl__air",
        ],
        units,
        inpfilter=inpfilter,
    )

    _Direct_heat_oil = pp.inp("foil_hpl", units, inpfilter=inpfilter)

    vars["Heat|Oil"] = _Cooling_heat_oil + _Direct_heat_oil

    vars["Heat|Other"] = pp.inp("po_turbine", units, inpfilter=inpfilter)

    if method == "consumption":
        vars["Heat|Gas"] = vars["Heat|Gas"] - pp.act_emif(
            [
                "gas_hpl__ot_fresh",
                "gas_hpl__cl_fresh",
                "gas_hpl__ot_saline",
                "gas_hpl__air",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        vars["Heat|Geothermal"] -= pp.act_emif(
            [
                "geo_hpl__ot_fresh",
                "geo_hpl__cl_fresh",
                "geo_hpl__ot_saline",
                "geo_hpl__air",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        vars["Heat|Biomass"] -= pp.act_emif(
            [
                "bio_hpl__ot_fresh",
                "bio_hpl__cl_fresh",
                "bio_hpl__ot_saline",
                "bio_hpl__air",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        vars["Heat|Coal"] -= pp.act_emif("coal_hpl", units=units, emiffilter=emiffilter)

        vars["Heat|Oil"] -= pp.act_emif(
            [
                "foil_hpl__ot_fresh",
                "foil_hpl__cl_fresh",
                "foil_hpl__ot_saline",
                "foil_hpl__air",
            ],
            units=units,
            emiffilter=emiffilter,
        )

        vars["Heat|Other"] -= pp.act_emif(
            "po_turbine", units=units, emiffilter=emiffilter
        )

    # --------
    # Hydrogen
    # --------

    hydrogen_outputfilter = {"level": ["secondary"], "commodity": ["hydrogen"]}

    vars["Hydrogen|Biomass|w/ CCS"] = _out_div_eff(
        "h2_bio_ccs", group, inpfilter, hydrogen_outputfilter
    )

    vars["Hydrogen|Biomass|w/o CCS"] = _out_div_eff(
        "h2_bio", group, inpfilter, hydrogen_outputfilter
    )

    vars["Hydrogen|Coal|w/ CCS"] = _out_div_eff(
        "h2_coal_ccs", group, inpfilter, hydrogen_outputfilter
    )

    vars["Hydrogen|Coal|w/o CCS"] = _out_div_eff(
        "h2_coal", group, inpfilter, hydrogen_outputfilter
    )

    vars["Hydrogen|Gas|w/ CCS"] = _out_div_eff(
        "h2_smr_ccs", group, inpfilter, hydrogen_outputfilter
    )

    vars["Hydrogen|Gas|w/o CCS"] = _out_div_eff(
        "h2_smr", group, inpfilter, hydrogen_outputfilter
    )

    vars["Hydrogen|Electricity"] = _out_div_eff(
        "h2_elec", group, inpfilter, hydrogen_outputfilter
    )

    if method == "consumption":
        vars["Hydrogen|Biomass|w/ CCS"] -= pp.act_emif(
            "h2_bio_ccs", units=units, emiffilter=emiffilter
        )

        vars["Hydrogen|Biomass|w/o CCS"] -= pp.act_emif(
            "h2_bio", units=units, emiffilter=emiffilter
        )

        vars["Hydrogen|Coal|w/ CCS"] -= pp.act_emif(
            "h2_coal_ccs", units=units, emiffilter=emiffilter
        )

        vars["Hydrogen|Coal|w/o CCS"] -= pp.act_emif(
            "h2_coal", units=units, emiffilter=emiffilter
        )

        vars["Hydrogen|Gas|w/ CCS"] -= pp.act_emif(
            "h2_smr_ccs", units=units, emiffilter=emiffilter
        )

        vars["Hydrogen|Gas|w/o CCS"] -= pp.act_emif(
            "h2_smr", units=units, emiffilter=emiffilter
        )

        vars["Hydrogen|Electricity"] -= pp.act_emif(
            "h2_elec", units=units, emiffilter=emiffilter
        )

    # --------------------
    # Irrigation water use
    # --------------------

    vars["Irrigation"] = (
        pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Water|Withdrawal|Irrigation"],
            }
        )
        * 0.001
    )
    if method == "consumption":
        vars["Irrigation"] = pp_utils._make_zero()

    # -----------
    # Bio ethanol
    # -----------

    bio_liquid_outfilter = {"level": ["primary"], "commodity": ["ethanol"]}

    vars["Liquids|Biomass|w/ CCS"] = _out_div_eff(
        ["eth_bio_ccs", "liq_bio_ccs"], group, inpfilter, bio_liquid_outfilter
    )

    vars["Liquids|Biomass|w/o CCS"] = _out_div_eff(
        ["eth_bio", "liq_bio"], group, inpfilter, bio_liquid_outfilter
    )

    if method == "consumption":
        vars["Liquids|Biomass|w/ CCS"] -= pp.act_emif(
            ["eth_bio_ccs", "liq_bio_ccs"], units=units, emiffilter=emiffilter
        )

        vars["Liquids|Biomass|w/o CCS"] -= pp.act_emif(
            ["eth_bio", "liq_bio"], units=units, emiffilter=emiffilter
        )

    # ---------------------------
    # Synthetic liquids from coal
    # ---------------------------

    # Note OFR 20210731: The filter below seems to be incorrect and hence was
    # corrected.
    # syn_liquid_outfilter = {"level": ["primary"],
    #                         "commodity": ["methanol"]}

    syn_liquid_outfilter = {
        "level": ["secondary"],
        "commodity": ["methanol", "lightoil"],
    }

    vars["Liquids|Coal|w/ CCS"] = _out_div_eff(
        ["meth_coal_ccs", "syn_liq_ccs"], group, inpfilter, syn_liquid_outfilter
    )

    vars["Liquids|Coal|w/o CCS"] = _out_div_eff(
        ["meth_coal", "syn_liq"], group, inpfilter, syn_liquid_outfilter
    )

    vars["Liquids|Gas|w/ CCS"] = _out_div_eff(
        ["meth_ng_ccs"], group, inpfilter, syn_liquid_outfilter
    )

    vars["Liquids|Gas|w/o CCS"] = _out_div_eff(
        ["meth_ng"], group, inpfilter, syn_liquid_outfilter
    )

    if method == "consumption":
        vars["Liquids|Coal|w/ CCS"] -= pp.act_emif(
            ["meth_coal_ccs", "syn_liq_ccs"], units=units, emiffilter=emiffilter
        )

        vars["Liquids|Coal|w/o CCS"] -= pp.act_emif(
            ["meth_coal", "syn_liq"], units=units, emiffilter=emiffilter
        )

        vars["Liquids|Gas|w/ CCS"] -= pp.act_emif(
            "meth_ng_ccs", units=units, emiffilter=emiffilter
        )

        vars["Liquids|Gas|w/o CCS"] -= pp.act_emif(
            "meth_ng", units=units, emiffilter=emiffilter
        )

    # ----------
    # Refineries
    # ----------

    loil_filter = {"level": ["secondary"], "commodity": ["lightoil"]}
    foil_filter = {"level": ["secondary"], "commodity": ["fueloil"]}

    vars["Liquids|Oil"] = _out_div_eff(
        ["ref_lol", "ref_hil"], group, inpfilter, loil_filter
    ) + _out_div_eff(["ref_lol", "ref_hil"], group, inpfilter, foil_filter)

    if method == "consumption":
        vars["Liquids|Oil"] -= pp.act_emif(
            ["ref_lol", "ref_hil"], units=units, emiffilter=emiffilter
        )

    df = pp_utils.make_outputdf(vars, units)
    return df
