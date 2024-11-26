import pandas as pd

import message_data.tools.post_processing.pp_utils as pp_utils

pp = None
mu = None
run_history = None
urban_perc_data = None
kyoto_hist_data = None
lu_hist_data = None

# Dictionary where all functions defined in the script are stored.
func_dict = {}  # type: dict


def return_func_dict():
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

    _Cogen = pp.inp("po_turbine", units, inpfilter={"commodity": ["electr"]})

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

    # Calculate shares for ppl feeding into g_ppl_co2scr (gas_cc and gas_ppl)
    _gas_cc_shr = (pp.out("gas_cc") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    _gas_ppl_shr = (pp.out("gas_ppl") / pp.out(["gas_cc", "gas_ppl"])).fillna(0)

    # --------------------------------
    # Electricity generation from coal
    # --------------------------------

    vars["Coal|w/o CCS"] = (
        _se_elec_woCCSretro("coal_ppl", "c_ppl_co2scr", units, _Frac)
        + _se_elec_po_turb("coal_adv", units, _Frac)
        + _se_elec_po_turb("coal_ppl_u", units, _Frac)
        + _se_elec_woCCSretro("igcc", "igcc_co2scr", units, _Frac)
        + pp.out(
            ["meth_coal", "h2_coal", "syn_liq"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
    )

    vars["Coal|w/ CCS"] = (
        _se_elec_wCCSretro("coal_ppl", "c_ppl_co2scr", units, _Frac)
        + _se_elec_po_turb("coal_adv_ccs", units, _Frac)
        + _se_elec_wCCSretro("igcc", "igcc_co2scr", units, _Frac)
        + _se_elec_po_turb("igcc_ccs", units, _Frac)
        + pp.out(
            ["meth_coal_ccs", "h2_coal_ccs", "syn_liq_ccs"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
    )

    # -------------------------------
    # Electricity generation from gas
    # -------------------------------

    _Gas_woCCS = (
        _se_elec_woCCSretro("gas_ppl", "g_ppl_co2scr", units, _Frac, share=_gas_ppl_shr)
        + _se_elec_po_turb("gas_ct", units, _Frac)
        + _se_elec_woCCSretro("gas_cc", "g_ppl_co2scr", units, _Frac, share=_gas_cc_shr)
        + _se_elec_woCCSretro("gas_htfc", "gfc_co2scr", units, _Frac)
        + pp.out(
            ["h2_smr"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
    )

    vars["Gas|w/o CCS"] = _Gas_woCCS * (1 - _BGas_share)

    _Gas_wCCS = (
        _se_elec_wCCSretro("gas_ppl", "g_ppl_co2scr", units, _Frac, share=_gas_ppl_shr)
        + _se_elec_wCCSretro("gas_cc", "g_ppl_co2scr", units, _Frac, share=_gas_cc_shr)
        + _se_elec_wCCSretro("gas_htfc", "gfc_co2scr", units, _Frac)
        + _se_elec_po_turb("gas_cc_ccs", units, _Frac)
        + pp.out(
            ["h2_smr_ccs"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
    )

    vars["Gas|w/ CCS"] = _Gas_wCCS * (1 - _BGas_share)

    # -------------------------------
    # Electricity generation from oil
    # -------------------------------

    vars["Oil|w/o CCS"] = (
        _se_elec_po_turb("foil_ppl", units, _Frac)
        + _se_elec_po_turb("loil_ppl", units, _Frac)
        + _se_elec_po_turb("oil_ppl", units, _Frac)
        + _se_elec_po_turb("loil_cc", units, _Frac)
    )

    # -----------------------------------
    # Electricity generation from biomass
    # -----------------------------------

    vars["Biomass|w/o CCS"] = (
        _se_elec_woCCSretro("bio_ppl", "bio_ppl_co2scr", units, _Frac)
        + _se_elec_po_turb("bio_istig", units, _Frac)
        + pp.out(
            ["h2_bio", "eth_bio", "liq_bio"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        + _Gas_woCCS * _BGas_share
    )

    # eth bio CCS is set to 0 because OFR doesnt know what to do with eff = 0.
    # normally it is replaced by 1, but this doesnt work in this case!
    vars["Biomass|w/ CCS"] = (
        _se_elec_wCCSretro("bio_ppl", "bio_ppl_co2scr", units, _Frac)
        + _se_elec_po_turb("bio_istig_ccs", units, _Frac)
        + pp.out(
            ["h2_bio_ccs", "eth_bio_ccs", "liq_bio_ccs"],
            units,
            outfilter={"level": ["secondary"], "commodity": ["electr"]},
        )
        + _Gas_wCCS * _BGas_share
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
        ["solar_curtailment1", "solar_curtailment2", "solar_curtailment3"],
        units,
        inpfilter={"commodity": ["electr"]},
    )

    _SolarPV_raw = (
        pp.out(
            [
                "solar_res1",
                "solar_res2",
                "solar_res3",
                "solar_res4",
                "solar_res5",
                "solar_res6",
                "solar_res7",
                "solar_res8",
                "solar_res_hist_2005",
                "solar_res_hist_2010",
                "solar_res_hist_2015",
                "solar_res_hist_2020",
            ],
            units,
        )
        - _SolarPV_curt_inp
    )

    _SolarPV_onsite = pp.out(["solar_pv_I", "solar_pv_RC"], units)

    SolarPV = _SolarPV_raw + _SolarPV_onsite

    # _SolarPV_woStorage_total = _SolarPV_raw - _Storage_input *\
    #     (_SolarPV_OR / (_Wind_OR + _SolarPV_OR)).fillna(0)

    # _SolarPV_woStorage = _SolarPV_woStorage_total + _SolarPV_onsite
    # _SolarPV_wStorage = _Storage_output *\
    #     (_SolarPV_OR / (_Wind_OR + _SolarPV_OR)).fillna(0)

    vars["Solar|PV"] = SolarPV
    vars["Solar|PV|Curtailment"] = _SolarPV_curt_inp

    # ---------------------
    # CSP related variables
    # ---------------------

    CSP = pp.out(
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

    vars["Solar|CSP"] = CSP

    # ----------------------
    # Wind related variables
    # ----------------------

    _Wind_res_act = pp.out(
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
    )

    _Wind_ref_act = pp.out(
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
    )

    _Wind_curt_inp = pp.inp(
        ["wind_curtailment1", "wind_curtailment2", "wind_curtailment3"],
        units,
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

    vars["Hydro"] = pp.out(
        [
            "hydro_1",
            "hydro_2",
            "hydro_3",
            "hydro_4",
            "hydro_5",
            "hydro_6",
            "hydro_7",
            "hydro_8",
        ],
        units,
    )

    vars["Geothermal"] = _se_elec_po_turb("geo_ppl", units, _Frac)

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
    )

    # comment["Other"] = "includes electricity production from fuel"
    #     " cells on the final energy level"

    vars["Other"] = (
        pp.inp(
            "h2_fc_trp", units, inpfilter={"level": ["final"], "commodity": ["electr"]}
        )
        + pp.inp(
            "h2_fc_I", units, inpfilter={"level": ["final"], "commodity": ["electr"]}
        )
        + pp.inp(
            "h2_fc_RC", units, inpfilter={"level": ["final"], "commodity": ["electr"]}
        )
    )

    # ------------------------------------------------
    # Additonal reporting for GAINS diagnostic linkage
    # ------------------------------------------------

    vars["Storage Losses"] = pp.inp(
        "stor_ppl", units, inpfilter={"commodity": ["electr"]}
    )

    vars["Transmission Losses"] = pp.inp(
        "elec_t_d", units, inpfilter={"commodity": ["electr"]}
    ) - pp.out("elec_t_d", units)

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

    vars["Biomass"] = pp.land_out(
        lu_out_filter={"level": ["land_use"], "commodity": ["bioenergy"]}, units=units
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

    vars["Hydro"] = pp.out(
        [
            "hydro_1",
            "hydro_2",
            "hydro_3",
            "hydro_4",
            "hydro_5",
            "hydro_6",
            "hydro_7",
            "hydro_8",
        ],
        units,
    )

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

        vars["Biomass|1st Generation"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|" "1st Generation"],
            }
        )

        vars["Biomass|1st Generation|Biodiesel"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|" "1st Generation|Biodiesel"],
            }
        )

        vars["Biomass|1st Generation|Bioethanol"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|" "1st Generation|Bioethanol"],
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
        ) - pp.inp("biomass_nc", units)

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

        vars["Biomass|Residues|Forest industry"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Residues|" "Forest industry"],
            }
        )

        vars["Biomass|Residues|Logging"] = pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Primary Energy|Biomass|Residues|" "Logging"],
            }
        )

        vars["Biomass|Traditional"] = pp.inp("biomass_nc", units)

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

    prmfunc = eval(prmfunc)
    vars = {}

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Biomass|w/ CCS|1"] = prmfunc("bio_istig_ccs")

    # ["Note": "IGCC"]
    vars["Electricity|Biomass|w/o CCS|1"] = prmfunc("bio_istig")

    # ["Note": "Steam cycle"]
    vars["Electricity|Biomass|w/o CCS|2"] = prmfunc("bio_ppl")

    # ["Note": "IGCC with CCS"]
    vars["Electricity|Coal|w/ CCS|1"] = prmfunc("igcc_ccs")

    # ["Note": "Steam cycle super critical with CCS"]
    vars["Electricity|Coal|w/ CCS|2"] = prmfunc("coal_adv_ccs")

    # ["Note": "IGCC"]
    vars["Electricity|Coal|w/o CCS|1"] = prmfunc("igcc")

    # ["Note": "Steam cycle super critical"]
    vars["Electricity|Coal|w/o CCS|2"] = prmfunc("coal_adv")

    # ["Note": "Steam cycle sub critical Filtered"]
    vars["Electricity|Coal|w/o CCS|3"] = prmfunc("coal_ppl")

    # ["Note": "Steam cycle sub critical unfiltered"]
    vars["Electricity|Coal|w/o CCS|4"] = prmfunc("coal_ppl_u")

    # ["Note": "Combined cycle with CCS"]
    vars["Electricity|Gas|w/ CCS|1"] = prmfunc("gas_cc_ccs")

    # ["Note": "Combined cycle"]
    vars["Electricity|Gas|w/o CCS|1"] = prmfunc("gas_cc")

    # ["Note": "Steam cycle sub cricitcal"]
    vars["Electricity|Gas|w/o CCS|2"] = prmfunc("gas_ppl")

    # ["Note": "Combustion turbine"]
    vars["Electricity|Gas|w/o CCS|3"] = prmfunc("gas_ct")
    vars["Electricity|Geothermal"] = prmfunc("geo_ppl")

    # ["Note": "Cost hydropower 1-8"]
    vars["Electricity|Hydro|1"] = prmfunc("hydro_1")
    vars["Electricity|Hydro|2"] = prmfunc("hydro_2")
    vars["Electricity|Hydro|3"] = prmfunc("hydro_3")
    vars["Electricity|Hydro|4"] = prmfunc("hydro_4")
    vars["Electricity|Hydro|5"] = prmfunc("hydro_5")
    vars["Electricity|Hydro|6"] = prmfunc("hydro_6")
    vars["Electricity|Hydro|7"] = prmfunc("hydro_7")
    vars["Electricity|Hydro|8"] = prmfunc("hydro_8")

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
    vars["Electricity|Solar|PV"] = prmfunc("solar_pv_ppl")
    vars["Electricity|Wind|Offshore"] = prmfunc("wind_ppf")
    vars["Electricity|Wind|Onshore"] = prmfunc("wind_ppl")
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

    # ["Note": "Cst hydropower 1-8"]
    vars["Electricity|Hydro|1"] = prmfunc("hydro_1", units=units)
    vars["Electricity|Hydro|2"] = prmfunc("hydro_2", units=units)
    vars["Electricity|Hydro"] = vars["Electricity|Hydro|3"] = prmfunc(
        "hydro_3", units=units
    )
    vars["Electricity|Hydro|4"] = prmfunc("hydro_4", units=units)
    vars["Electricity|Hydro|5"] = prmfunc("hydro_5", units=units)
    vars["Electricity|Hydro|6"] = prmfunc("hydro_6", units=units)
    vars["Electricity|Hydro|7"] = prmfunc("hydro_7", units=units)
    vars["Electricity|Hydro|8"] = prmfunc("hydro_8", units=units)

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
    vars["Electricity|Solar|PV"] = prmfunc("solar_pv_ppl", units=units)
    vars["Electricity|Wind|Offshore"] = prmfunc("wind_ppf", units=units)
    vars["Electricity|Wind|Onshore"] = prmfunc("wind_ppl", units=units)
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

    # ["Note": "Cst hydropower 1-8"]
    vars["Electricity|Hydro|1"] = prmfunc(
        "hydro_1", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Hydro|2"] = prmfunc(
        "hydro_2", units=units, group=group, formatting=formatting
    )
    vars["Electricity|Hydro"] = vars["Electricity|Hydro|3"] = prmfunc(
        "hydro_3", units=units, group=group, formatting=formatting
    )
    vars["Electricity|Hydro|4"] = prmfunc(
        "hydro_4", units=units, group=group, formatting=formatting
    )
    vars["Electricity|Hydro|5"] = prmfunc(
        "hydro_5", units=units, group=group, formatting=formatting
    )
    vars["Electricity|Hydro|6"] = prmfunc(
        "hydro_6", units=units, group=group, formatting=formatting
    )
    vars["Electricity|Hydro|7"] = prmfunc(
        "hydro_7", units=units, group=group, formatting=formatting
    )
    vars["Electricity|Hydro|8"] = prmfunc(
        "hydro_8", units=units, group=group, formatting=formatting
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

    vars["Electricity|Solar|PV"] = prmfunc(
        "solar_pv_ppl", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Wind|Offshore"] = prmfunc(
        "wind_ppf", units=units, group=group, formatting=formatting
    )

    vars["Electricity|Wind|Onshore"] = prmfunc(
        "wind_ppl", units=units, group=group, formatting=formatting
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
        ["coal_extr_ch4", "coal_extr", "lignite_extr"], units=units_energy
    )

    vars["Extraction|Gas|Conventional"] = (
        pp.investment(
            ["gas_extr_1", "gas_extr_2", "gas_extr_3", "gas_extr_4"], units=units_energy
        )
        + pp.act_vom(
            ["gas_extr_1", "gas_extr_2", "gas_extr_3", "gas_extr_4"], units=units_energy
        )
        * 0.5
    )

    vars["Extraction|Gas|Unconventional"] = (
        pp.investment(
            ["gas_extr_5", "gas_extr_6", "gas_extr_7", "gas_extr_8"], units=units_energy
        )
        + pp.act_vom(
            ["gas_extr_5", "gas_extr_6", "gas_extr_7", "gas_extr_8"], units=units_energy
        )
        * 0.5
    )

    # Note OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under "Liquids|Oil"

    vars["Extraction|Oil|Conventional"] = (
        pp.investment(
            [
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
            [
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
            [
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
            [
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
            ["uran2u5", "Uran_extr", "u5-reproc", "plutonium_prod"], units=units_energy
        )
        + pp.act_vom(["uran2u5", "Uran_extr"], units=units_energy)
        + pp.act_vom(["u5-reproc"], actfilter={"mode": ["M1"]}, units=units_energy)
        + pp.tic_fom(["uran2u5", "Uran_extr", "u5-reproc"], units=units_energy)
    )

    # ---------------------
    # Electricity - Fossils
    # ---------------------

    vars["Electricity|Coal|w/ CCS"] = (
        pp.investment(["c_ppl_co2scr", "cfc_co2scr"], units=units_energy)
        + pp.investment("coal_adv_ccs", units=units_energy) * 0.25
        + pp.investment("igcc_ccs", units=units_energy) * 0.31
    )

    vars["Electricity|Coal|w/o CCS"] = (
        pp.investment(["coal_ppl", "coal_ppl_u", "coal_adv"], units=units_energy)
        + pp.investment("coal_adv_ccs", units=units_energy) * 0.75
        + pp.investment("igcc", units=units_energy)
        + pp.investment("igcc_ccs", units=units_energy) * 0.69
    )

    vars["Electricity|Gas|w/ CCS"] = (
        pp.investment(["g_ppl_co2scr", "gfc_co2scr"], units=units_energy)
        + pp.investment("gas_cc_ccs", units=units_energy) * 0.53
    )

    vars["Electricity|Gas|w/o CCS"] = (
        pp.investment(["gas_cc", "gas_ct", "gas_ppl"], units=units_energy)
        + pp.investment("gas_cc_ccs", units=units_energy) * 0.47
    )

    vars["Electricity|Oil|w/o CCS"] = pp.investment(
        ["foil_ppl", "loil_ppl", "oil_ppl", "loil_cc"], units=units_energy
    )

    # ------------------------
    # Electricity - Renewables
    # ------------------------

    vars["Electricity|Biomass|w/ CCS"] = (
        pp.investment("bio_ppl_co2scr", units=units_energy)
        + pp.investment("bio_istig_ccs", units=units_energy) * 0.31
    )

    vars["Electricity|Biomass|w/o CCS"] = (
        pp.investment(["bio_ppl", "bio_istig"], units=units_energy)
        + pp.investment("bio_istig_ccs", units=units_energy) * 0.69
    )

    vars["Electricity|Geothermal"] = pp.investment("geo_ppl", units=units_energy)

    vars["Electricity|Hydro"] = pp.investment(
        [
            "hydro_1",
            "hydro_2",
            "hydro_3",
            "hydro_4",
            "hydro_5",
            "hydro_6",
            "hydro_7",
            "hydro_8",
        ],
        units=units_energy,
    )
    vars["Electricity|Other"] = pp.investment(
        ["h2_fc_I", "h2_fc_RC"], units=units_energy
    )

    _solar_pv_elec = pp.investment(
        ["solar_pv_ppl", "solar_pv_I", "solar_pv_RC"], units=units_energy
    )

    _solar_th_elec = pp.investment(["csp_sm1_ppl", "csp_sm3_ppl"], units=units_energy)

    vars["Electricity|Solar|PV"] = _solar_pv_elec
    vars["Electricity|Solar|CSP"] = _solar_th_elec
    vars["Electricity|Wind|Onshore"] = pp.investment(["wind_ppl"], units=units_energy)
    vars["Electricity|Wind|Offshore"] = pp.investment(["wind_ppf"], units=units_energy)

    # -------------------
    # Electricity Nuclear
    # -------------------

    vars["Electricity|Nuclear"] = pp.investment(
        ["nuc_hc", "nuc_lc"], units=units_energy
    )

    # --------------------------------------------------
    # Electricity Storage, transmission and distribution
    # --------------------------------------------------

    vars["Electricity|Electricity Storage"] = pp.investment(
        "stor_ppl", units=units_energy
    )

    vars["Electricity|Transmission and Distribution"] = (
        pp.investment(
            [
                "elec_t_d",
                "elec_exp",
                "elec_exp_africa",
                "elec_exp_america",
                "elec_exp_asia",
                "elec_exp_eurasia",
                "elec_exp_eur_afr",
                "elec_exp_asia_afr",
                "elec_imp",
                "elec_imp_africa",
                "elec_imp_america",
                "elec_imp_asia",
                "elec_imp_eurasia",
                "elec_imp_eur_afr",
                "elec_imp_asia_afr",
            ],
            units=units_energy,
        )
        + pp.act_vom(
            [
                "elec_t_d",
                "elec_exp",
                "elec_exp_africa",
                "elec_exp_america",
                "elec_exp_asia",
                "elec_exp_eurasia",
                "elec_exp_eur_afr",
                "elec_exp_asia_afr",
                "elec_imp",
                "elec_imp_africa",
                "elec_imp_america",
                "elec_imp_asia",
                "elec_imp_eurasia",
                "elec_imp_eur_afr",
                "elec_imp_asia_afr",
            ],
            units=units_energy,
        )
        * 0.5
        + pp.tic_fom(
            [
                "elec_t_d",
                "elec_exp",
                "elec_exp_africa",
                "elec_exp_america",
                "elec_exp_asia",
                "elec_exp_eurasia",
                "elec_exp_eur_afr",
                "elec_exp_asia_afr",
                "elec_imp",
                "elec_imp_africa",
                "elec_imp_america",
                "elec_imp_asia",
                "elec_imp_eurasia",
                "elec_imp_eur_afr",
                "elec_imp_asia_afr",
            ],
            units=units_energy,
        )
        * 0.5
    )

    # ------------------------------------------
    # CO2 Storage, transmission and distribution
    # ------------------------------------------

    _CCS_coal_elec = -1 * pp.emi(
        ["c_ppl_co2scr", "coal_adv_ccs", "igcc_ccs", "cement_co2scr"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_coal_synf = -1 * pp.emi(
        ["syn_liq_ccs", "h2_coal_ccs", "meth_coal_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_elec = -1 * pp.emi(
        ["g_ppl_co2scr", "gas_cc_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_gas_synf = -1 * pp.emi(
        ["h2_smr_ccs", "meth_ng_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_elec = -1 * pp.emi(
        ["bio_ppl_co2scr", "bio_istig_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_bio_synf = -1 * pp.emi(
        ["eth_bio_ccs", "liq_bio_ccs", "h2_bio_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _Biogas_use_tot = pp.out("gas_bio")

    _Gas_use_tot = pp.inp(
        [
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
        pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_coal_elec_shr
        + pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_gas_elec_shr
        + pp.act_vom("bco2_tr_dis", units=units_energy) * 0.5 * _CCS_bio_elec_shr
    )

    CO2_trans_dist_synf = (
        pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_coal_synf_shr
        + pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_gas_synf_shr
        + pp.act_vom("bco2_tr_dis", units=units_energy) * 0.5 * _CCS_bio_synf_shr
    )

    vars["CO2 Transport and Storage"] = CO2_trans_dist_elec + CO2_trans_dist_synf

    # ----
    # Heat
    # ----

    vars["Heat"] = pp.investment(
        ["coal_hpl", "foil_hpl", "gas_hpl", "bio_hpl", "heat_t_d", "po_turbine"],
        units=units_energy,
    )

    # -------------------------
    # Synthetic fuel production
    # -------------------------

    # Note OFR 25.04.2017: XXX_synf_ccs has been split into hydrogen and
    # liquids. The shares then add up to 1, but the variables are kept
    # separate in order to preserve the split between CCS and non-CCS

    _Coal_synf_ccs_liq = (
        pp.investment("meth_coal_ccs", units=units_energy) * 0.02
        + pp.investment("syn_liq_ccs", units=units_energy) * 0.01
    )

    _Gas_synf_ccs_liq = pp.investment("meth_ng_ccs", units=units_energy) * 0.08

    _Bio_synf_ccs_liq = (
        pp.investment("eth_bio_ccs", units=units_energy) * 0.34
        + pp.investment("liq_bio_ccs", units=units_energy) * 0.02
    )

    _Coal_synf_ccs_h2 = pp.investment("h2_coal_ccs", units=units_energy) * 0.03
    _Gas_synf_ccs_h2 = pp.investment("h2_smr_ccs", units=units_energy) * 0.17
    _Bio_synf_ccs_h2 = pp.investment("h2_bio_ccs", units=units_energy) * 0.02

    # Note OFR 25.04.2017: "coal_gas" have been moved to "other"
    vars["Liquids|Coal and Gas"] = (
        pp.investment(["meth_coal", "syn_liq", "meth_ng"], units=units_energy)
        + pp.investment("meth_ng_ccs", units=units_energy) * 0.92
        + pp.investment("meth_coal_ccs", units=units_energy) * 0.98
        + pp.investment("syn_liq_ccs", units=units_energy) * 0.99
        + _Coal_synf_ccs_liq
        + _Gas_synf_ccs_liq
    )

    # Note OFR 25.04.2017: "gas_bio" has been moved to "other"
    vars["Liquids|Biomass"] = (
        pp.investment(["eth_bio", "liq_bio"], units=units_energy)
        + pp.investment("liq_bio_ccs", units=units_energy) * 0.98
        + pp.investment("eth_bio_ccs", units=units_energy) * 0.66
        + _Bio_synf_ccs_liq
    )

    # Note OFR 25.04.2017: "transport, import and exports costs related to
    # liquids are only included in the total"
    _Synfuel_other = pp.investment(
        [
            "meth_exp",
            "meth_imp",
            "meth_t_d",
            "meth_bal",
            "eth_exp",
            "eth_imp",
            "eth_t_d",
            "eth_bal",
            "SO2_scrub_synf",
        ],
        units=units_energy,
    )

    vars["Liquids|Oil"] = pp.investment(
        ["ref_lol", "ref_hil"], units=units_energy
    ) + pp.tic_fom(["ref_hil", "ref_lol"], units=units_energy)

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
        pp.investment(["h2_coal", "h2_smr"], units=units_energy)
        + pp.investment("h2_coal_ccs", units=units_energy) * 0.97
        + pp.investment("h2_smr_ccs", units=units_energy) * 0.83
        + _Coal_synf_ccs_h2
        + _Gas_synf_ccs_h2
    )

    vars["Hydrogen|Renewable"] = (
        pp.investment("h2_bio", units=units_energy)
        + pp.investment("h2_bio_ccs", units=units_energy) * 0.98
        + _Bio_synf_ccs_h2
    )

    vars["Hydrogen|Other"] = (
        pp.investment(
            [
                "h2_elec",
                "h2_liq",
                "h2_t_d",
                "lh2_exp",
                "lh2_imp",
                "lh2_bal",
                "lh2_regas",
                "lh2_t_d",
            ],
            units=units_energy,
        )
        + pp.act_vom("h2_mix", units=units_energy) * 0.5
    )

    # -----
    # Other
    # -----

    # All questionable variables from extraction that are not related directly
    # to extraction should be moved to Other
    # Note OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under "Liquids|Oil"

    vars["Other|Liquids|Oil|Transmission and Distribution"] = pp.investment(
        ["foil_t_d", "loil_t_d"], units=units_energy
    )

    vars["Other|Liquids|Oil|Other"] = pp.investment(
        [
            "foil_exp",
            "loil_exp",
            "oil_exp",
            "oil_imp",
            "foil_imp",
            "loil_imp",
            "loil_std",
            "oil_bal",
            "loil_sto",
        ],
        units=units_energy,
    )

    vars["Other|Gases|Transmission and Distribution"] = pp.investment(
        ["gas_t_d", "gas_t_d_ch4"], units=units_energy
    )

    vars["Other|Gases|Production"] = pp.investment(
        ["gas_bio", "coal_gas"], units=units_energy
    )

    vars["Other|Gases|Other"] = pp.investment(
        [
            "LNG_bal",
            "LNG_prod",
            "LNG_regas",
            "LNG_exp",
            "LNG_imp",
            "gas_bal",
            "gas_std",
            "gas_sto",
            "gas_exp_eeu",
            "gas_exp_nam",
            "gas_exp_pao",
            "gas_exp_weu",
            "gas_exp_cpa",
            "gas_exp_afr",
            "gas_exp_sas",
            "gas_exp_scs",
            "gas_exp_cas",
            "gas_exp_ubm",
            "gas_exp_rus",
            "gas_imp",
        ],
        units=units_energy,
    )

    vars["Other|Solids|Coal|Transmission and Distribution"] = (
        pp.investment(
            [
                "coal_t_d",
                "coal_t_d-rc-SO2",
                "coal_t_d-rc-06p",
                "coal_t_d-in-SO2",
                "coal_t_d-in-06p",
            ],
            units=units_energy,
        )
        + pp.act_vom(
            [
                "coal_t_d-rc-SO2",
                "coal_t_d-rc-06p",
                "coal_t_d-in-SO2",
                "coal_t_d-in-06p",
                "coal_t_d",
            ],
            units=units_energy,
        )
        * 0.5
    )

    vars["Other|Solids|Coal|Other"] = pp.investment(
        ["coal_exp", "coal_imp", "coal_bal", "coal_std"], units=units_energy
    )

    vars["Other|Solids|Biomass|Transmission and Distribution"] = pp.investment(
        "biomass_t_d", units=units_energy
    )

    vars["Other|Other"] = pp.investment(
        ["SO2_scrub_ref"], units=units_energy
    ) * 0.5 + pp.investment(["SO2_scrub_ind"], units=units_energy)

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
        [
            "hydro_1",
            "hydro_2",
            "hydro_3",
            "hydro_4",
            "hydro_5",
            "hydro_6",
            "hydro_7",
            "hydro_8",
        ],
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

    vars["Irrigation|Cereals"] = (
        pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Water|Withdrawal|Irrigation|Cereals"],
            }
        )
        * 0.001
    )

    vars["Irrigation|Oilcrops"] = (
        pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Water|Withdrawal|Irrigation|Oilcrops"],
            }
        )
        * 0.001
    )

    vars["Irrigation|Sugarcrops"] = (
        pp.land_out(
            lu_out_filter={
                "level": ["land_use_reporting"],
                "commodity": ["Water|Withdrawal|Irrigation|Sugarcrops"],
            }
        )
        * 0.001
    )

    if method == "consumption":
        vars["Irrigation"] = pp_utils._make_zero()
        vars["Irrigation|Cereals"] = pp_utils._make_zero()
        vars["Irrigation|Oilcrops"] = pp_utils._make_zero()
        vars["Irrigation|Sugarcrops"] = pp_utils._make_zero()

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
