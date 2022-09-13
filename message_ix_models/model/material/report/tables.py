import logging
import pandas as pd

from message_data.tools.post_processing import default_tables
from message_data.tools.post_processing.default_tables import (
    TECHS,
    rc_techs,
    _pe_wCCSretro,
)
import message_data.tools.post_processing.pp_utils as pp_utils

log = logging.getLogger(__name__)

pp = None
mu = None
run_history = None
urban_perc_data = None
kyoto_hist_data = None
lu_hist_data = None

# Dictionary where all functions defined in the script are stored.
func_dict = {}  # type: dict


def return_func_dict():
    """Return functions described in script."""

    # This is a crude hack: when iamc_report_hackathon.report(), per config, finds and
    # calls this function to retrieve the list of functions in the file, we override the
    # lists of technologies used by functions defined in .default_tables.
    # FIXME(PNK) read these from proper hierarchical technology code lists; store on a
    #            Context object.
    TECHS.update(_TECHS)

    # Invoke similar code associated with MESSAGEix-Buildings
    from message_data.model.buildings.report import configure_legacy_reporting

    configure_legacy_reporting(TECHS)

    log.info(f"Configured legacy reporting for -BM model variants:\n{TECHS = }")

    return func_dict


#: Lists of technologies for materials variant.
_TECHS = {
    "cement with ccs": ["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"],
    "industry with ccs": [
        "biomass_NH3_ccs",
        "coal_NH3_ccs",
        "fueloil_NH3_ccs",
        "gas_NH3_ccs",
    ],
    "gas extra": [
        "furnace_gas_aluminum",
        "furnace_gas_petro",
        "furnace_gas_cement",
        "furnace_gas_refining",
        "hp_gas_aluminum",
        "hp_gas_petro",
        "hp_gas_refining",
    ],
    "refining": [
        "atm_distillation_ref",
        "catalytic_cracking_ref",
        "catalytic_reforming_ref",
        "coking_ref",
        "dheat_refining",
        "fc_h2_refining",
        "furnace_biomass_refining",
        "furnace_coal_refining",
        "furnace_coke_refining",
        "furnace_elec_refining",
        "furnace_ethanol_refining",
        "furnace_foil_refining",
        "furnace_gas_refining",
        "furnace_h2_refining",
        "furnace_loil_refining",
        "furnace_methanol_refining",
        "hp_elec_refining",
        "hp_gas_refining",
        "hydro_cracking_ref",
        "hydrotreating_ref",
        "solar_refining",
        "vacuum_distillation_ref",
        "visbreaker_ref",
    ],
    "se solids biomass extra": [
        "furnace_biomass_aluminum",
        "furnace_biomass_cement",
        "furnace_biomass_petro",
        "furnace_biomass_refining",
        "furnace_biomass_steel",
    ],
    "se solids coal extra": [
        "furnace_coal_aluminum",
        "furnace_coal_petro",
        "furnace_coal_cement",
        "furnace_coal_refining",
    ],
    "synfuels oil": [
        (
            "agg_ref",
            "out",
            dict(level=["secondary"], commodity=["lightoil", "fueloil"]),
        ),
        (
            "steam_cracker_petro",
            "inp",
            dict(level=["final"], commodity=["atm_gasoil", "naphtha", "vacuum_gasoil"]),
        ),
    ],
}


def _register(func):
    """Function to register reporting functions.

    Parameters
    ----------

    func : str
        Function name
    """

    func_dict[func.__name__] = func
    return func


# -------------------
# Reporting functions
# -------------------


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
                "gas_i",
                "hp_gas_i",
                "gas_trp",
                "gas_fs",
                "gas_ppl",
                "gas_ct",
                "gas_cc",
                "gas_htfc",
                "gas_hpl",
            ]
            + TECHS["rc gas"]
            + TECHS["gas extra"],
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
        pp.inp(
            [
                "gas_i",
                "hp_gas_i",
            ]
            + TECHS["gas extra"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_fs = _Biogas_tot * (
        pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    _Biogas_res = _Biogas_tot * (
        pp.inp(TECHS["rc gas"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
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
        pp.inp(
            [
                "gas_i",
                "hp_gas_i",
            ]
            + TECHS["gas extra"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_fs = _Hydrogen_tot * (
        pp.inp("gas_fs", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(TECHS["rc gas"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
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
        [
            "coal_fs",
            "foil_fs",
            "loil_fs",
            "gas_fs",
            "methanol_fs",
            "steam_cracker_petro",
            "gas_processing_petro",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_feedstocks"]},
        emission_units=units_emi,
    )

    _FE_Res_com = pp.emi(
        rc_techs("coal foil gas loil meth"),
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
        [
            "furnace_coke_refining",
            "furnace_coal_refining",
            "furnace_biomass_refining",
            "furnace_gas_refining",
            "furnace_loil_refining",
            "furnace_foil_refining",
            "furnace_methanol_refining",
            "atm_distillation_ref",
            "vacuum_distillation_ref",
            "catalytic_cracking_ref",
            "visbreaker_ref",
            "coking_ref",
            "catalytic_reforming_ref",
            "hydro_cracking_ref",
        ],
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
        [
            "clinker_dry_cement",
            "clinker_wet_cement",
        ]
        + TECHS["cement with ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _Cement2 = pp.emi(
        [
            "DUMMY_limestone_supply_cement",
            "furnace_biomass_cement",
            "furnace_coal_cement",
            "furnace_foil_cement",
            "furnace_gas_cement",
            "furnace_loil_cement",
            "furnace_methanol_cement",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _Aluminum1 = pp.emi(
        ["soderberg_aluminum", "prebake_aluminum", "vertical_stud"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _Aluminum2 = pp.emi(
        [
            "furnace_biomass_aluminum",
            "furnace_coal_aluminum",
            "furnace_foil_aluminum",
            "furnace_gas_aluminum",
            "furnace_loil_aluminum",
            "furnace_methanol_aluminum",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _Steel = pp.emi(
        ["DUMMY_coal_supply", "DUMMY_gas_supply", "DUMMY_limestone_supply_steel"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _Chemicals = pp.emi(
        [
            "furnace_biomass_petro",
            "furnace_coal_petro",
            "furnace_coke_petro",
            "furnace_foil_petro",
            "furnace_gas_petro",
            "furnace_loil_petro",
            "furnace_methanol_petro",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    _Ammonia = pp.emi(
        ["biomass_NH3", "gas_NH3", "coal_NH3", "fueloil_NH3"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Ammonia_ccs = pp.emi(
        ["biomass_NH3_ccs", "gas_NH3_ccs", "coal_NH3_ccs", "fueloil_NH3_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    # FIXME unused; clarify purpose or remove
    _Total = (  # noqa: F841
        _SE_Elec_gen
        + _SE_District_heat
        + _FE_total
        + _Other_gases_total
        + _Other_liquids_total
        + _Other_solids_total
        - _Biogas_tot
        + _Hydrogen_tot
        + _Cement1
        + _Cement2
        + _Aluminum1
        + _Aluminum2
        + _Steel
        + _Chemicals
        + _Ammonia
        + _Ammonia_ccs
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

    # FIXME unused; clarify purpose or remove
    _CO2_tce1 = pp.emi(  # noqa: F841
        "CO2_TCE",
        units_ene_mdl,
        emifilter={"relation": ["TCE_Emission"]},
        emission_units=units_emi,
    )

    _CO2_GLOBIOM = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU"],
        },
        units=units_emi,
    )

    # _Diff1 = _CO2_tce1 - _Total
    _Diff1 = pp_utils._make_zero()

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
    vars["AFOLU|Afforestation"] = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU|Afforestation"],
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
def retr_SE_solids(units):
    """Energy: Secondary Energy solids.

    Parameters
    ----------

    units : str
        Units to which variables should be converted.
    """
    # Call the function in default_tables()
    base = default_tables.retr_SE_solids(units)

    # Extra retrieval requiring special filters
    vars = dict(
        Coal=pp.inp(
            [
                "bf_steel",
                "cokeoven_steel",
                "furnace_coal_steel",
                "sinter_steel",
            ],
            units,
            inpfilter={"commodity": ["coal"], "level": ["final"]},
        )
    )
    df = pp_utils.make_outputdf(vars, units)

    # Combine with `base`; sum at matching indices
    return (
        pd.concat([base, df])
        .groupby(["Model", "Scenario", "Variable", "Unit", "Region"], as_index=False)
        .sum()
    )
