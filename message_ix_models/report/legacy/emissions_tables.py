import numpy as np
import pandas as pd

from . import pp_utils

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
            (
                pp.out(
                    tec,
                    units,
                    outfilter={"level": ["secondary"], "commodity": ["electr"]},
                )
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
                / pp.eff(t, inpfilter=inpfilter, outfilter=outfilter, group=group)
            )
        )
    df = pd.concat(dfs, sort=True)

    return df.groupby(df.index.name).sum()


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

    vars["PPP"] = vars["MER"] * MERtoPPP

    vars["Consumption"] = (
        pp.var_consumption() * conv_usd * 1000
    )  # Convert trillion to billion

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
    elif var in ["N2O"]:
        AgricultureWasteBurning = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|N2O|AFOLU|Biomass Burning"],
            }
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
        vars["AFOLU|Land|Grassland Pastures"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|N2O|AFOLU|Land|Grassland Pastures"],
            }
        )

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

    if var in ["BCA", "OCA", "SO2", "NH3", "CO", "NOx", "VOC", "CH4"]:
        GrasslandBurning = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": [f"{var}_SavanBurnEM"],
            },
            units=units,
        )
    elif var in ["N2O"]:
        GrasslandBurning = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Emissions|N2O|AFOLU|Land|Grassland Burning"],
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
            "commodity": ["Emissions|CO2|AFOLU|Negative"],
        }
    )

    vars["Land Use|Afforestation"] = -pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Afforestation"],
        }
    )

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

    vars["AFOLU|Negative"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Negative"],
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

    #    vars["AFOLU|Soil Carbon"] = pp.land_out(
    #        lu_out_filter={"level": ["land_use_reporting"],
    #                       "commodity": ["Emissions|CO2|AFOLU|Soil Carbon"]},
    #        units=units_emi)

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

    dfs.append(pp_utils.make_outputdf(vars, units_emi, glb=True))
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

    dfs.append(pp_utils.make_outputdf(vars, units_ene_mdl, glb=True))
    return pd.concat(dfs, sort=True)
