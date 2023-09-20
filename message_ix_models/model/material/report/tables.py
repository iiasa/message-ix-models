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

    log.debug(f"Configured legacy reporting for -BM model variants:\n{TECHS = }")

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
            "agg_ref",
            units,
            outfilter={"level": ["secondary"], "commodity": ["lightoil"]},
        )
        + pp.out(
            "agg_ref",
            units,
            outfilter={"level": ["secondary"], "commodity": ["fueloil"]},
        )
        + pp.inp(
            "steam_cracker_petro",
            units,
            inpfilter={
                "level": ["final"],
                "commodity": ["atm_gasoil", "vacuum_gasoil", "naphtha"],
            },
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
        outfilter={"level": ["primary"], "commodity": ["methanol"]},
    )

    vars["Liquids|Coal|w/ CCS"] = pp.out(
        ["meth_coal_ccs", "syn_liq_ccs"],
        units,
        outfilter={"level": ["primary"], "commodity": ["methanol"]},
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
        ["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"],
        "GWa",
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    _CCS_ammonia = -1.0 * pp.emi(
        ['biomass_NH3_ccs', 'gas_NH3_ccs', 'coal_NH3_ccs', 'fueloil_NH3_ccs'],
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

    vars["CCS|Industrial Processes"] = _CCS_cement + _CCS_ammonia

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

    # ----------------------------------------------------------------------------------
    # Step 1. Calculate variables used for distribution of hydrogen and biogas related
    #        emissions
    # ----------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------
    # Step 1.1. Calculate total got HYDROGEN distribution
    #
    # Calculate all non-ccs gas use which is used for the distribution of HYDROGEN
    # related emissions which need to be subtracted from various sectors.
    # ----------------------------------------------------------------------------------

    _inp_nonccs_gas_tecs = (
        pp.inp(
            [
                "gas_cc",  # _Biogas_el, _Hydrogen_el
                "gas_ct",  # _Biogas_el, _Hydrogen_el
                "gas_fs",  # _Biogas_fs, _Hydrogen_fs
                "gas_hpl",  # _Biogas_heat, _Hydrogen_heat
                "gas_htfc",  # _Biogas_el, _Hydrogen_el
                "gas_i",  # _Biogas_ind_rest, _Hydrogen_ind_rest
                "gas_ppl",  # _Biogas_el, _Hydrogen_el
                "gas_trp",  # _Biogas_trp, _Hydrogen_trp
                "hp_gas_i",  # _Biogas_ind_rest, _Hydrogen_ind_rest
            ]
            + TECHS["rc gas"]  # _Biogas_res, _Hydrogen_res
            + TECHS["gas extra"],  # See above
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        + pp.inp(
            ["gas_t_d", "gas_t_d_ch4"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )  # _Biogas_td, _Hydrogen_td
        - pp.out(
            ["gas_t_d", "gas_t_d_ch4"], units_ene_mdl, outfilter={"commodity": ["gas"]}
        )  # _Biogas_td, _Hydrogen_td
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

    # ----------------------------------------------------------------------------------
    # Step 1.2. Calculate total for BIOGAS distribution
    #
    # Calculate all gas use which is used to for the distribution of BIOGAS related
    # emissions which need to be subtracted from various sectors.
    # ----------------------------------------------------------------------------------

    _inp_all_gas_tecs = _inp_nonccs_gas_tecs + pp.inp(
        [
            "gas_cc_ccs",  # _Biogas_el, no-hydrogen via hlim
            "meth_ng",  # _Biogas_liquids_gas_comb, no-hydrogen via ?
            "meth_ng_ccs",  # _Biogas_liquids_gas_comb, no-hydrogen via ?
            "h2_smr",  # _Biogas_gases_h2_comb, no-hydrogen via ?
            "h2_smr_ccs",  # _Biogas_gases_h2_comb, no-hydrogen via hlim
            "gas_NH3_ccs"  # _Biogas_fs, no-hydrogen needs entry to hlim entry
            # equivalent to CO2 entry (-)
        ],
        units_ene_mdl,
        inpfilter={"commodity": ["gas"]},
    )

    # ----------------------------------------------------------------------------------
    # Step 2. Distribute BIOGAS emissions
    #
    # - Calculate biogas-emission attribution to sector
    # - Based on: sector_specfific_gas_consumption / total_gas_use * total_biogas_use
    # - These emissions will later be subtracted from the respective sectors.
    # ----------------------------------------------------------------------------------

    # Retrieve total bio-gas quantity procuded
    _Biogas_tot_abs = pp.out("gas_bio")

    # Multiply quantity by emission factor
    _Biogas_tot = _Biogas_tot_abs * mu["crbcnt_gas"] * mu["conv_c2co2"]

    # Variable: "Energy|Supply|Electricity"
    _Biogas_el = _Biogas_tot * (
        pp.inp(
            ["gas_ppl", "gas_ct", "gas_cc", "gas_cc_ccs", "gas_htfc"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Supply|Heat"
    _Biogas_heat = _Biogas_tot * (
        pp.inp("gas_hpl", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Supply|Liquids|Natural Gas|Combustion"
    _Biogas_liquids_gas_comb = _Biogas_tot * (
        pp.inp(
            ["meth_ng", "meth_ng_ccs"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Supply|Gases|Hydrogen|Combustion"
    _Biogas_gases_h2_comb = _Biogas_tot * (
        pp.inp(
            ["h2_smr", "h2_smr_ccs"], units_ene_mdl, inpfilter={"commodity": ["gas"]}
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Supply|Gases|Transportation|Combustion"
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

    # Variable: "Energy|Demand|Industry|Other Sector"
    _Biogas_ind_rest = _Biogas_tot * (
        pp.inp(
            [
                "gas_i",
                "hp_gas_i",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Steel"
    _Biogas_ind_steel = _Biogas_tot * (
        pp.inp(
            ["dri_steel", "eaf_steel", "furnace_gas_steel", "hp_gas_steel"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Non-Metallic Minerals|Cement"
    _Biogas_ind_cement = _Biogas_tot * (
        pp.inp(
            [
                "furnace_gas_cement",
                "hp_gas_cement",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Non-Ferrous Metals|Aluminium"
    _Biogas_ind_alu = _Biogas_tot * (
        pp.inp(
            [
                "furnace_gas_aluminum",
                "hp_gas_aluminum",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Chemicals|High Value Chemicals"
    _Biogas_ind_petro = _Biogas_tot * (
        pp.inp(
            [
                "furnace_gas_petro",
                "gas_processing_petro",
                "hp_gas_petro",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Chemicals|Ammonia"
    _Biogas_ind_ammonia = _Biogas_tot * (
        pp.inp(
            ["gas_NH3", "gas_NH3_ccs"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Other Sector"
    _Biogas_fs = _Biogas_tot * (
        pp.inp(
            ["gas_fs"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Supply|Liquids|Oil|Combustion"
    _Biogas_ref = _Biogas_tot * (
        pp.inp(
            ["furnace_gas_refining", "hp_gas_refining"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Residential and Commercial"
    _Biogas_res = _Biogas_tot * (
        pp.inp(TECHS["rc gas"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    # Variable: "Energy|Demand|Transportation|Road Rail and Domestic Shipping"
    _Biogas_trp = _Biogas_tot * (
        pp.inp("gas_trp", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_all_gas_tecs
    ).fillna(0)

    # Finally check to ensure that the sum of all the varaibles onto which biogas
    # emissions are distributed on, matches the total.
    # FIXME this variable is assigned but never used
    check_biogas = _Biogas_tot - (  # noqa: F841
        _Biogas_el
        + _Biogas_heat
        + _Biogas_liquids_gas_comb
        + _Biogas_gases_h2_comb
        + _Biogas_td
        + _Biogas_ind_rest
        + _Biogas_ind_steel
        + _Biogas_ind_cement
        + _Biogas_ind_alu
        + _Biogas_ind_petro
        + _Biogas_ind_ammonia
        + _Biogas_fs
        + _Biogas_ref
        + _Biogas_res
        + _Biogas_trp
    )

    # ----------------------------------------------------------------------------------
    # Step 3. Distribute HYDROGEN emissions
    #
    # - Calculate hydrogen-emissions for each sector.
    # - Based on: sector_specific_gas_consumption / total_NONCCS_gas_use
    #             * total hydrogen-emissions
    # - All CCS excluded as hydrogen cannot be used in CCS applications, hence also
    #   division by _inp_nonccs_gas_tecs_wo_CCSRETRO.
    # - Emissions that will later be added to the respective sectors, as the
    #   emission-entry of `h2_mix` is negative, hence the derived values are negative.
    # - All FE-technologies listed here, must have an entry into the relation
    #   `h2mix_direct`
    # ----------------------------------------------------------------------------------

    # Retrieve total hydrogen emissions
    # Note, these are negative!
    _Hydrogen_tot = pp.emi(
        "h2_mix",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Electricity"
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

    # Variable: "Energy|Supply|Heat"
    _Hydrogen_heat = _Hydrogen_tot * (
        pp.inp("gas_hpl", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Supply|Gases|Transportation|Combustion"
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

    # Variable: "Energy|Demand|Industry|Other Sector"
    _Hydrogen_ind_rest = _Hydrogen_tot * (
        pp.inp(
            [
                "gas_i",
                "hp_gas_i",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Steel"
    _Hydrogen_ind_steel = _Hydrogen_tot * (
        pp.inp(
            ["dri_steel", "eaf_steel", "furnace_gas_steel", "hp_gas_steel"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Non-Metallic Minerals|Cement"
    _Hydrogen_ind_cement = _Hydrogen_tot * (
        pp.inp(
            [
                "furnace_gas_cement",
                "hp_gas_cement",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Non-Ferrous Metals|Aluminium"
    _Hydrogen_ind_alu = _Hydrogen_tot * (
        pp.inp(
            [
                "furnace_gas_aluminum",
                "hp_gas_aluminum",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Chemicals|High Value Chemicals"
    _Hydrogen_ind_petro = _Hydrogen_tot * (
        pp.inp(
            [
                "furnace_gas_petro",
                "gas_processing_petro",
                "hp_gas_petro",
            ],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Industry|Chemicals|Ammonia"
    _Hydrogen_ind_ammonia = _Hydrogen_tot * (
        pp.inp(
            ["gas_NH3"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Other Sector"
    _Hydrogen_fs = _Hydrogen_tot * (
        pp.inp(
            ["gas_fs"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Supply|Liquids|Oil|Combustion"
    _Hydrogen_ref = _Hydrogen_tot * (
        pp.inp(
            ["furnace_gas_refining", "hp_gas_refining"],
            units_ene_mdl,
            inpfilter={"commodity": ["gas"]},
        )
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Residential and Commercial"
    _Hydrogen_res = _Hydrogen_tot * (
        pp.inp(TECHS["rc gas"], units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Variable: "Energy|Demand|Transportation|Road Rail and Domestic Shipping"
    _Hydrogen_trp = _Hydrogen_tot * (
        pp.inp("gas_trp", units_ene_mdl, inpfilter={"commodity": ["gas"]})
        / _inp_nonccs_gas_tecs_wo_CCSRETRO
    ).fillna(0)

    # Finally check to ensure that the sum of all the variables onto which hydrogen
    # emissions are distributed on, matches the total.
    # FIXME this variable is assigned but never used
    check_hydrogen = _Hydrogen_tot - (  # noqa: F841
        _Hydrogen_el
        + _Hydrogen_heat
        + _Hydrogen_td
        + _Hydrogen_ind_rest
        + _Hydrogen_ind_steel
        + _Hydrogen_ind_cement
        + _Hydrogen_ind_alu
        + _Hydrogen_ind_petro
        + _Hydrogen_ind_ammonia
        + _Hydrogen_fs
        + _Hydrogen_ref
        + _Hydrogen_res
        + _Hydrogen_trp
    )

    # ----------------------------------------------------------------------------------
    # Step 4. Calculate total sector emissions
    #
    # - For all biomass-CCS or gas-CCS based technologies, total emissions with their
    #   contributions to emission abatement need to be calculated.
    # ----------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------
    # Step 4.1 Energy Supply - Heat and Electricity
    # ----------------------------------------------------------------------------------

    # Variable: "Energy|Supply|Electricity"
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

    # Variable: "Energy|Supply|Electricity"
    # Used for reallocation of Diff.
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

    # Variable: "Energy|Supply|Heat"
    _SE_District_heat = pp.emi(
        ["coal_hpl", "foil_hpl", "gas_hpl"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # ----------------------------------------------------------------------------------
    # Step 4.2 Final Energy
    # ----------------------------------------------------------------------------------

    # Variable: "Energy|Demand|Industry|Other Sector"
    _FE_Industry_rest = pp.emi(
        [
            "coal_i",
            "foil_i",
            "gas_i",
            "hp_gas_i",
            "loil_i",
            "meth_i",
            "sp_liq_I",
            "sp_meth_I",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Industry|Steel"
    _FE_Industry_steel = pp.emi(
        ["DUMMY_coal_supply", "DUMMY_gas_supply", "DUMMY_limestone_supply_steel"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Industry|Non-Metallic Minerals|Cement"
    _FE_Industry_cement = pp.emi(
        [
            "DUMMY_limestone_supply_cement",
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

    # Variable: "Industrial Processes|Non-Metallic Minerals|Cement"
    _FE_IndustryProcess_alu = pp.emi(
        ["prebake_aluminum", "soderberg_aluminum", "vertical_stud"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Industry|Non-Ferrous Metals|Aluminium"
    _FE_Industry_alu = pp.emi(
        [
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

    #  Variable: "Industrial Processes|Non-Ferrous Metals|Aluminium"
    _FE_IndustryProcess_cement = pp.emi(
        [
            "clinker_dry_ccs_cement",
            "clinker_dry_cement",
            "clinker_wet_ccs_cement",
            "clinker_wet_cement",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_Emission"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Industry|Chemicals|High Value Chemicals"
    _FE_Industry_petro = pp.emi(
        [
            "furnace_coal_petro",
            "furnace_coke_petro",
            "furnace_foil_petro",
            "furnace_gas_petro",
            "furnace_loil_petro",
            "furnace_methanol_petro",
            "gas_processing_petro",
            "steam_cracker_petro",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_ind"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Industry|Chemicals|Ammonia"
    _FE_Industry_ammonia = pp.emi(
        [
            "biomass_NH3_ccs",
            "biomass_NH3",
            "coal_NH3_ccs",
            "coal_NH3",
            "fueloil_NH3_ccs",
            "fueloil_NH3",
            "gas_NH3_ccs",
            "gas_NH3",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Industry|Chemicals|Ammonia"
    # Used for reallocation of Diff. - this is an exception. Normally the Diff. is only
    # reallocated across SE-conversion technologies, which is where the NH3 technologies
    # are located despite being assigned to `Demand|Industry` emissions.
    _FE_Industry_ammonia_woBECCS = (
        _FE_Industry_ammonia
        - pp.emi(
            ["biomass_NH3_ccs"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        - pp.emi(
            ["gas_NH3_ccs"],
            units_ene_mdl,
            emifilter={"relation": ["CO2_cc"]},
            emission_units=units_emi,
        )
        * (_Biogas_tot_abs / _inp_all_gas_tecs)
    )

    # Variable: "Energy|Demand|Other Sector"
    _FE_Feedstocks = pp.emi(
        [
            "coal_fs",
            "foil_fs",
            "loil_fs",
            "gas_fs",
            "methanol_fs",
        ],
        units_ene_mdl,
        emifilter={"relation": ["CO2_feedstocks"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Residential and Commercial"
    _FE_Res_com = pp.emi(
        rc_techs("coal foil gas loil meth"),
        units_ene_mdl,
        emifilter={"relation": ["CO2_r_c"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Demand|Transportation|Road Rail and Domestic Shipping"
    _FE_Transport = pp.emi(
        ["gas_trp", "loil_trp", "meth_fc_trp", "meth_ic_trp", "coal_trp", "foil_trp"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trp"]},
        emission_units=units_emi,
    )

    _FE_Industry = (
        _FE_Industry_rest
        + _FE_Industry_steel
        + _FE_IndustryProcess_cement
        + _FE_Industry_cement
        + _FE_IndustryProcess_alu
        + _FE_Industry_alu
        + _FE_Industry_petro
        + _FE_Industry_ammonia
    )
    _FE_total = _FE_Feedstocks + _FE_Res_com + _FE_Industry + _FE_Transport

    # ----------------------------------------------------------------------------------
    # Step 4.3 Energy Supply - Gases
    # ----------------------------------------------------------------------------------

    # Variable: "Energy|Supply|Gases|Extraction|Combustion"
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

    # Variable: "Energy|Supply|Gases|Extraction|Fugitive"
    _Other_gases_extr_fug = pp.emi(
        "flaring_CO2",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Gases|Transportation|Combustion"
    # Note that this is not included in the total because the diff is only calculated
    # from CO2_TCE and doesn't include trade.
    _Other_gases_trans_comb_trade = pp.emi(
        ["LNG_trd"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Gases|Transportation|Combustion"
    _Other_gases_trans_comb = pp.emi(
        ["gas_t_d", "gas_t_d_ch4"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Gases|Coal|Combustion"
    _Other_gases_coal_comb = pp.emi(
        ["coal_gas"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    #    _Other_gases_biomass_comb = pp.emi(["gas_bio"], units_ene_mdl,
    #                                       emifilter={"relation": ["CO2_cc"]},
    #                                       emission_units=units_emi)

    # Varpible: "Energy|Supply|Gases|Hydrogen|Combustion"
    _Other_gases_h2_comb = pp.emi(
        ["h2_smr", "h2_coal", "h2_bio", "h2_coal_ccs", "h2_smr_ccs", "h2_bio_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Gases|Hydrogen|Combustion"
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

    # Summation of all gases to be added to total
    _Other_gases_total = (
        _Other_gases_extr_comb
        + _Other_gases_extr_fug
        + _Other_gases_trans_comb
        + _Other_gases_coal_comb
        + _Other_gases_h2_comb
    )

    # Summation of all gases without BECCS used to reallocate the difference between
    # top-down and bottom up accounting.
    _Other_gases_total_woBECCS = (
        _Other_gases_extr_comb
        + _Other_gases_trans_comb
        + _Other_gases_coal_comb
        + _Other_gases_h2_comb_woBECCS
    )
    # Fugitive is not included in the total used to redistribute the difference
    # + _Other_gases_extr_fug

    # ----------------------------------------------------------------------------------
    # Step 4.4 Energy Supply - Liquids
    # ----------------------------------------------------------------------------------

    # Variable: "Energy|Supply|Liquids|Extraction|Combustion"
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

    # Variable: "Energy|Supply|Liquids|Transportation|Combustion"
    # Note that this is not included in the total because the diff is only calculated
    # from CO2_TCE and doesn't include trade.
    _Other_liquids_trans_comb_trade = pp.emi(
        ["foil_trd", "loil_trd", "oil_trd", "meth_trd", "eth_trd"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_trade"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Liquids|Transportation|Combustion"
    _Other_liquids_trans_comb = pp.emi(
        ["foil_t_d", "loil_t_d", "meth_t_d", "eth_t_d"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Liquids|Oil|Combustion"
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

    # Variable: "Energy|Supply|Liquids|Natural Gas|Combustion"
    _Other_liquids_gas_comb = pp.emi(
        ["meth_ng", "meth_ng_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Liquids|Natural Gas|Combustion"
    # For Diff allocation
    _Other_liquids_gas_comb_woBECCS = _Other_liquids_gas_comb - pp.emi(
        ["meth_ng_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    ) * (_Biogas_tot_abs / _inp_all_gas_tecs)

    # Variable: "Energy|Supply|Liquids|Coal|Combustion"
    _Other_liquids_coal_comb = pp.emi(
        ["meth_coal", "syn_liq", "meth_coal_ccs", "syn_liq_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Liquids|Biomass|Combustion"
    _Other_liquids_biomass_comb = pp.emi(
        ["eth_bio", "liq_bio", "eth_bio_ccs", "liq_bio_ccs"],
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    # Variable: "Energy|Supply|Liquids|Biomass|Combustion"
    # For Diff allocation
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

    # ----------------------------------------------------------------------------------
    # Step 4.5 Energy Supply - Solids
    # ----------------------------------------------------------------------------------

    # Variable: "Energy|Supply|Solids|Extraction|Combustion"
    _Other_solids_coal_trans_comb = pp.emi(
        "coal_t_d",
        units_ene_mdl,
        emifilter={"relation": ["CO2_cc"]},
        emission_units=units_emi,
    )

    _Other_solids_total = _Other_solids_coal_trans_comb

    # ----------------------------------------------------------------------------------
    # Step 4.6 Land-use
    # ----------------------------------------------------------------------------------

    # Variable: "AFOLU"
    _CO2_GLOBIOM = pp.land_out(
        lu_out_filter={
            "level": ["land_use_reporting"],
            "commodity": ["Emissions|CO2|AFOLU"],
        },
        units=units_emi,
    )

    # ----------------------------------------------------------------------------------
    # Step 5. Calculate Difference in top-down vs bottom-up
    #
    # TODO What about _FE_Feedstocks_woBECCS? Should they be included in
    # _Total_wo_BECCS?
    # ----------------------------------------------------------------------------------

    _Total = (  # noqa: F841 due to modified calculation of _Diff1, below
        _SE_Elec_gen
        + _SE_District_heat
        + _FE_total
        + _Other_gases_total
        + _Other_liquids_total
        + _Other_solids_total
        - _Biogas_tot
        + _Hydrogen_tot
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
        # Exception made for ammonia as it is as SecondaryEnergy level not at final.
        + abs(
            _FE_Industry_ammonia_woBECCS - _Biogas_ind_ammonia + _Hydrogen_ind_ammonia
        )
    )

    _CO2_tce1 = pp.emi(  # noqa: F841 due to modified calculation of _Diff1, below
        "CO2_TCE",
        units_ene_mdl,
        emifilter={"relation": ["TCE_Emission"]},
        emission_units=units_emi,
    )

    # This was temporarily zero, since the difference was still not zero.
    # _Diff1 = _CO2_tce1 - _Total
    _Diff1 = pp_utils._make_zero()

    if run_history == "True":
        df_hist = pd.read_csv(lu_hist_data).set_index("Region")
        df_hist = df_hist.rename(columns={i: int(i) for i in df_hist.columns})
        _Diff1 = _Diff1 - df_hist

    # ----------------------------------------------------------------------------------
    # Step 6. Assign values to IAMC Reporting Variables
    # ----------------------------------------------------------------------------------

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

    # ----------------------------------------------------------------------------------
    # Aircraft
    # ----------------------------------------------------------------------------------

    Aircraft = pp_utils._make_zero()
    vars["Energy|Demand|Transportation|Aviation|International"] = Aircraft

    # ----------------------------------------------------------------------------------
    # Energy Supply - Heat and Electricity
    # ----------------------------------------------------------------------------------

    vars["Energy|Supply|Electricity"] = (
        _SE_Elec_gen
        - _Biogas_el
        + _Hydrogen_el
        + _Diff1
        * (
            abs(_SE_Elec_gen_woBECCS - _Biogas_el + _Hydrogen_el) / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Supply|Heat"] = (
        _SE_District_heat
        - _Biogas_heat
        + _Hydrogen_heat
        + _Diff1
        * (
            abs(_SE_District_heat - _Biogas_heat + _Hydrogen_heat) / _Total_wo_BECCS
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

    vars["Energy|Supply|Liquids|Oil|Combustion"] = (
        _Other_liquids_oil_comb
        - _Biogas_ref
        + _Hydrogen_ref
        + _Diff1
        * (
            abs(_Other_liquids_oil_comb - _Biogas_ref + _Hydrogen_ref) / _Total_wo_BECCS
        ).fillna(0)
    )

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

    # ----------------------------------------------------------------------------------
    # Final Energy - Industrial Combustion
    # ----------------------------------------------------------------------------------

    vars["Energy|Demand|Industry|Other Sector"] = (
        _FE_Industry_rest - _Biogas_ind_rest + _Hydrogen_ind_rest
    )
    vars["Energy|Demand|Industry|Steel"] = (
        _FE_Industry_steel - _Biogas_ind_steel + _Hydrogen_ind_steel
    )
    vars["Energy|Demand|Industry|Non-Metallic Minerals|Cement"] = (
        _FE_Industry_cement - _Biogas_ind_cement + _Hydrogen_ind_cement
    )
    vars["Energy|Demand|Industry|Non-Metallic Minerals"] = (
        _FE_Industry_cement - _Biogas_ind_cement + _Hydrogen_ind_cement
    )
    vars["Energy|Demand|Industry|Non-Ferrous Metals|Aluminium"] = (
        _FE_Industry_alu - _Biogas_ind_alu + _Hydrogen_ind_alu
    )
    vars["Energy|Demand|Industry|Non-Ferrous Metals"] = (
        _FE_Industry_alu - _Biogas_ind_alu + _Hydrogen_ind_alu
    )
    vars["Energy|Demand|Industry|Chemicals|High Value Chemicals"] = (
        _FE_Industry_petro - _Biogas_ind_petro + _Hydrogen_ind_petro
    )
    vars["Energy|Demand|Industry|Chemicals|Ammonia"] = (
        _FE_Industry_ammonia
        - _Biogas_ind_ammonia
        + _Hydrogen_ind_ammonia
        + _Diff1
        * (
            abs(
                _FE_Industry_ammonia_woBECCS
                - _Biogas_ind_ammonia
                + _Hydrogen_ind_ammonia
            )
            / _Total_wo_BECCS
        ).fillna(0)
    )

    vars["Energy|Demand|Industry|Chemicals"] = (
    vars["Energy|Demand|Industry|Chemicals|High Value Chemicals"] +
    vars["Energy|Demand|Industry|Chemicals|Ammonia"] )

    # Intermediate total, used below in calculation of "Energy|Combustion"
    vars["Energy|Demand|Industry"] = (
        vars["Energy|Demand|Industry|Other Sector"]
        + vars["Energy|Demand|Industry|Steel"]
        + vars["Energy|Demand|Industry|Non-Metallic Minerals|Cement"]
        + vars["Energy|Demand|Industry|Non-Ferrous Metals|Aluminium"]
        + vars["Energy|Demand|Industry|Chemicals"]
    )

    # ---------------------
    # Industrial Feedstocks
    # ---------------------

    vars["Energy|Demand|Other Sector"] = _FE_Feedstocks - _Biogas_fs + _Hydrogen_fs

    # --------------------------------------------
    # Industrial process and product use (Table 8)
    # --------------------------------------------

    vars[
        "Industrial Processes|Non-Ferrous Metals|Aluminium"
    ] = _FE_IndustryProcess_alu
    vars["Industrial Processes|Non-Metallic Minerals|Cement"] = _FE_IndustryProcess_cement

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

@_register
def retr_supply_inv(units_energy,
                    units_emi,
                    units_ene_mdl):
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

    vars["Extraction|Coal"] = pp.investment(["coal_extr_ch4", "coal_extr",
                                             "lignite_extr"], units=units_energy)

    vars["Extraction|Gas|Conventional"] =\
        pp.investment(["gas_extr_1", "gas_extr_2",
                       "gas_extr_3", "gas_extr_4"], units=units_energy) +\
        pp.act_vom(["gas_extr_1", "gas_extr_2",
                    "gas_extr_3", "gas_extr_4"], units=units_energy) * .5

    vars["Extraction|Gas|Unconventional"] =\
        pp.investment(["gas_extr_5", "gas_extr_6",
                       "gas_extr_7", "gas_extr_8"], units=units_energy) +\
        pp.act_vom(["gas_extr_5", "gas_extr_6",
                    "gas_extr_7", "gas_extr_8"], units=units_energy) * .5

    # Note OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under "Liquids|Oil"

    vars["Extraction|Oil|Conventional"] =\
        pp.investment(["oil_extr_1", "oil_extr_2", "oil_extr_3",
                       "oil_extr_1_ch4", "oil_extr_2_ch4", "oil_extr_3_ch4"],
                      units=units_energy) +\
        pp.act_vom(["oil_extr_1", "oil_extr_2", "oil_extr_3",
                    "oil_extr_1_ch4", "oil_extr_2_ch4", "oil_extr_3_ch4"],
                   units=units_energy) * .5

    vars["Extraction|Oil|Unconventional"] =\
        pp.investment(["oil_extr_4", "oil_extr_4_ch4", "oil_extr_5",
                       "oil_extr_6", "oil_extr_7", "oil_extr_8"],
                      units=units_energy) +\
        pp.act_vom(["oil_extr_4", "oil_extr_4_ch4", "oil_extr_5",
                    "oil_extr_6", "oil_extr_7", "oil_extr_8"], units=units_energy) * .5

    # As no mode is specified, u5-reproc will account for all 3 modes.
    vars["Extraction|Uranium"] =\
        pp.investment(["uran2u5", "Uran_extr",
                       "u5-reproc", "plutonium_prod"], units=units_energy) +\
        pp.act_vom(["uran2u5", "Uran_extr"], units=units_energy) +\
        pp.act_vom(["u5-reproc"], actfilter={"mode": ["M1"]}, units=units_energy) +\
        pp.tic_fom(["uran2u5", "Uran_extr", "u5-reproc"], units=units_energy)

    # ---------------------
    # Electricity - Fossils
    # ---------------------

    vars["Electricity|Coal|w/ CCS"] =\
        pp.investment(["c_ppl_co2scr", "cfc_co2scr"], units=units_energy) +\
        pp.investment("coal_adv_ccs", units=units_energy) * 0.25 +\
        pp.investment("igcc_ccs", units=units_energy) * 0.31

    vars["Electricity|Coal|w/o CCS"] =\
        pp.investment(["coal_ppl", "coal_ppl_u", "coal_adv"], units=units_energy) +\
        pp.investment("coal_adv_ccs", units=units_energy) * 0.75 +\
        pp.investment("igcc", units=units_energy) +\
        pp.investment("igcc_ccs", units=units_energy) * 0.69

    vars["Electricity|Gas|w/ CCS"] =\
        pp.investment(["g_ppl_co2scr", "gfc_co2scr"], units=units_energy) +\
        pp.investment("gas_cc_ccs", units=units_energy) * 0.53

    vars["Electricity|Gas|w/o CCS"] =\
        pp.investment(["gas_cc", "gas_ct", "gas_ppl"], units=units_energy) +\
        pp.investment("gas_cc_ccs", units=units_energy) * 0.47

    vars["Electricity|Oil|w/o CCS"] =\
        pp.investment(["foil_ppl", "loil_ppl", "oil_ppl", "loil_cc"],
                      units=units_energy)

    # ------------------------
    # Electricity - Renewables
    # ------------------------

    vars["Electricity|Biomass|w/ CCS"] =\
        pp.investment("bio_ppl_co2scr", units=units_energy) +\
        pp.investment("bio_istig_ccs", units=units_energy) * 0.31

    vars["Electricity|Biomass|w/o CCS"] =\
        pp.investment(["bio_ppl", "bio_istig"], units=units_energy) +\
        pp.investment("bio_istig_ccs", units=units_energy) * 0.69

    vars["Electricity|Geothermal"] = pp.investment("geo_ppl", units=units_energy)

    vars["Electricity|Hydro"] = pp.investment(["hydro_hc", "hydro_lc"],
                                              units=units_energy)
    vars["Electricity|Other"] = pp.investment(["h2_fc_I", "h2_fc_RC"],
                                              units=units_energy)

    _solar_pv_elec = pp.investment(["solar_pv_ppl", "solar_pv_I",
                                    "solar_pv_RC"], units=units_energy)

    _solar_th_elec = pp.investment(["csp_sm1_ppl", "csp_sm3_ppl"], units=units_energy)

    vars["Electricity|Solar|PV"] = _solar_pv_elec
    vars["Electricity|Solar|CSP"] = _solar_th_elec
    vars["Electricity|Wind|Onshore"] = pp.investment(["wind_ppl"], units=units_energy)
    vars["Electricity|Wind|Offshore"] = pp.investment(["wind_ppf"], units=units_energy)

    # -------------------
    # Electricity Nuclear
    # -------------------

    vars["Electricity|Nuclear"] = pp.investment(["nuc_hc", "nuc_lc"],
                                                units=units_energy)

    # --------------------------------------------------
    # Electricity Storage, transmission and distribution
    # --------------------------------------------------

    vars["Electricity|Electricity Storage"] = pp.investment("stor_ppl",
                                                            units=units_energy)

    vars["Electricity|Transmission and Distribution"] =\
        pp.investment(["elec_t_d",
                       "elec_exp", "elec_exp_africa",
                       "elec_exp_america", "elec_exp_asia",
                       "elec_exp_eurasia", "elec_exp_eur_afr",
                       "elec_exp_asia_afr",
                       "elec_imp", "elec_imp_africa",
                       "elec_imp_america", "elec_imp_asia",
                       "elec_imp_eurasia", "elec_imp_eur_afr",
                       "elec_imp_asia_afr"], units=units_energy) +\
        pp.act_vom(["elec_t_d",
                    "elec_exp", "elec_exp_africa",
                    "elec_exp_america", "elec_exp_asia",
                    "elec_exp_eurasia", "elec_exp_eur_afr",
                    "elec_exp_asia_afr",
                    "elec_imp", "elec_imp_africa",
                    "elec_imp_america", "elec_imp_asia",
                    "elec_imp_eurasia", "elec_imp_eur_afr",
                    "elec_imp_asia_afr"], units=units_energy) * .5 +\
        pp.tic_fom(["elec_t_d",
                    "elec_exp", "elec_exp_africa",
                    "elec_exp_america", "elec_exp_asia",
                    "elec_exp_eurasia", "elec_exp_eur_afr",
                    "elec_exp_asia_afr",
                    "elec_imp", "elec_imp_africa",
                    "elec_imp_america", "elec_imp_asia",
                    "elec_imp_eurasia", "elec_imp_eur_afr",
                    "elec_imp_asia_afr"], units=units_energy) * .5

    # ------------------------------------------
    # CO2 Storage, transmission and distribution
    # ------------------------------------------

    _CCS_coal_elec = -1 *\
        pp.emi(["c_ppl_co2scr", "coal_adv_ccs",
                "igcc_ccs", "clinker_dry_ccs_cement",'clinker_wet_ccs_cement'], units_ene_mdl,
               emifilter={"relation": ["CO2_Emission"]},
               emission_units=units_emi)

    _CCS_coal_synf = -1 *\
        pp.emi(["syn_liq_ccs", "h2_coal_ccs",
                "meth_coal_ccs"], units_ene_mdl,
               emifilter={"relation": ["CO2_Emission"]},
               emission_units=units_emi)

    _CCS_gas_elec = -1 *\
        pp.emi(["g_ppl_co2scr", "gas_cc_ccs"], units_ene_mdl,
               emifilter={"relation": ["CO2_Emission"]},
               emission_units=units_emi)

    _CCS_gas_synf = -1 *\
        pp.emi(["h2_smr_ccs", "meth_ng_ccs"], units_ene_mdl,
               emifilter={"relation": ["CO2_Emission"]},
               emission_units=units_emi)

    _CCS_bio_elec = -1 *\
        pp.emi(["bio_ppl_co2scr", "bio_istig_ccs"], units_ene_mdl,
               emifilter={"relation": ["CO2_Emission"]},
               emission_units=units_emi)

    _CCS_bio_synf = -1 *\
        pp.emi(["eth_bio_ccs", "liq_bio_ccs",
                "h2_bio_ccs"], units_ene_mdl,
               emifilter={"relation": ["CO2_Emission"]},
               emission_units=units_emi)

    _Biogas_use_tot = pp.out("gas_bio")

    _Gas_use_tot = pp.inp(["gas_ppl", "gas_cc", "gas_cc_ccs",
                           "gas_ct", "gas_htfc", "gas_hpl",
                           "meth_ng", "meth_ng_ccs", "h2_smr",
                           "h2_smr_ccs", "gas_rc", "hp_gas_rc",
                           "gas_i", "hp_gas_i",
                           'furnace_gas_aluminum',
                           'furnace_gas_petro',
                           'furnace_gas_cement',
                           'furnace_gas_refining',
                           "hp_gas_i",
                           'hp_gas_aluminum',
                           'hp_gas_petro',
                           'hp_gas_refining',
                           "gas_trp",
                           "gas_fs"])

    _Biogas_share = (_Biogas_use_tot / _Gas_use_tot).fillna(0)

    _CCS_Foss = _CCS_coal_elec +\
        _CCS_coal_synf +\
        _CCS_gas_elec * (1 - _Biogas_share) +\
        _CCS_gas_synf * (1 - _Biogas_share)

    _CCS_Bio = _CCS_bio_elec +\
        _CCS_bio_synf -\
        (_CCS_gas_elec + _CCS_gas_synf) * _Biogas_share

    _CCS_coal_elec_shr = (_CCS_coal_elec / _CCS_Foss).fillna(0)
    _CCS_coal_synf_shr = (_CCS_coal_synf / _CCS_Foss).fillna(0)
    _CCS_gas_elec_shr = (_CCS_gas_elec / _CCS_Foss).fillna(0)
    _CCS_gas_synf_shr = (_CCS_gas_synf / _CCS_Foss).fillna(0)
    _CCS_bio_elec_shr = (_CCS_bio_elec / _CCS_Bio).fillna(0)
    _CCS_bio_synf_shr = (_CCS_bio_synf / _CCS_Bio).fillna(0)

    CO2_trans_dist_elec =\
        pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_coal_elec_shr +\
        pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_gas_elec_shr +\
        pp.act_vom("bco2_tr_dis", units=units_energy) * 0.5 * _CCS_bio_elec_shr

    CO2_trans_dist_synf =\
        pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_coal_synf_shr +\
        pp.act_vom("co2_tr_dis", units=units_energy) * 0.5 * _CCS_gas_synf_shr +\
        pp.act_vom("bco2_tr_dis", units=units_energy) * 0.5 * _CCS_bio_synf_shr

    vars["CO2 Transport and Storage"] = CO2_trans_dist_elec + CO2_trans_dist_synf

    # ----
    # Heat
    # ----

    vars["Heat"] = pp.investment(["coal_hpl", "foil_hpl", "gas_hpl",
                                  "bio_hpl", "heat_t_d", "po_turbine"],
                                 units=units_energy)

    # -------------------------
    # Synthetic fuel production
    # -------------------------

    # Note OFR 25.04.2017: XXX_synf_ccs has been split into hydrogen and
    # liquids. The shares then add up to 1, but the variables are kept
    # separate in order to preserve the split between CCS and non-CCS

    _Coal_synf_ccs_liq = pp.investment("meth_coal_ccs", units=units_energy) * 0.02 +\
        pp.investment("syn_liq_ccs", units=units_energy) * 0.01

    _Gas_synf_ccs_liq = pp.investment("meth_ng_ccs", units=units_energy) * 0.08

    _Bio_synf_ccs_liq = pp.investment("eth_bio_ccs", units=units_energy) * 0.34 + \
        pp.investment("liq_bio_ccs", units=units_energy) * 0.02

    _Coal_synf_ccs_h2 = pp.investment("h2_coal_ccs", units=units_energy) * 0.03
    _Gas_synf_ccs_h2 = pp.investment("h2_smr_ccs", units=units_energy) * 0.17
    _Bio_synf_ccs_h2 = pp.investment("h2_bio_ccs", units=units_energy) * 0.02

    # Note OFR 25.04.2017: "coal_gas" have been moved to "other"
    vars["Liquids|Coal and Gas"] =\
        pp.investment(["meth_coal", "syn_liq", "meth_ng"], units=units_energy) +\
        pp.investment("meth_ng_ccs", units=units_energy) * 0.92 +\
        pp.investment("meth_coal_ccs", units=units_energy) * 0.98 +\
        pp.investment("syn_liq_ccs", units=units_energy) * 0.99 +\
        _Coal_synf_ccs_liq +\
        _Gas_synf_ccs_liq

    # Note OFR 25.04.2017: "gas_bio" has been moved to "other"
    vars["Liquids|Biomass"] =\
        pp.investment(["eth_bio", "liq_bio"], units=units_energy) +\
        pp.investment("liq_bio_ccs", units=units_energy) * 0.98 +\
        pp.investment("eth_bio_ccs", units=units_energy) * 0.66 + _Bio_synf_ccs_liq

    # Note OFR 25.04.2017: "transport, import and exports costs related to
    # liquids are only included in the total"
    _Synfuel_other = pp.investment(["meth_exp", "meth_imp", "meth_t_d",
                                    "meth_bal", "eth_exp", "eth_imp",
                                    "eth_t_d", "eth_bal", "SO2_scrub_synf"],
                                   units=units_energy)

    vars["Liquids|Oil"] = pp.investment(['furnace_coke_refining',
                                          'furnace_coal_refining',
                                          'furnace_foil_refining',
                                          'furnace_loil_refining',
                                          'furnace_ethanol_refining',
                                          'furnace_biomass_refining',
                                          'furnace_methanol_refining',
                                          'furnace_gas_refining',
                                          'furnace_elec_refining',
                                          'furnace_h2_refining',
                                          'hp_gas_refining',
                                          'hp_elec_refining',
                                          'fc_h2_refining',
                                          'solar_refining',
                                          'dheat_refining',
                                          'atm_distillation_ref',
                                          'vacuum_distillation_ref',
                                          'hydrotreating_ref',
                                          'catalytic_cracking_ref',
                                          'visbreaker_ref',
                                          'coking_ref',
                                          'catalytic_reforming_ref',
                                          'hydro_cracking_ref'
    ], units=units_energy) +\
        pp.tic_fom(['furnace_coke_refining',
                  'furnace_coal_refining',
                  'furnace_foil_refining',
                  'furnace_loil_refining',
                  'furnace_ethanol_refining',
                  'furnace_biomass_refining',
                  'furnace_methanol_refining',
                  'furnace_gas_refining',
                  'furnace_elec_refining',
                  'furnace_h2_refining',
                  'hp_gas_refining',
                  'hp_elec_refining',
                  'fc_h2_refining',
                  'solar_refining',
                  'dheat_refining',
                  'atm_distillation_ref',
                  'vacuum_distillation_ref',
                  'hydrotreating_ref',
                  'catalytic_cracking_ref',
                  'visbreaker_ref',
                  'coking_ref',
                  'catalytic_reforming_ref',
                  'hydro_cracking_ref'], units=units_energy)

    vars["Liquids"] = vars["Liquids|Coal and Gas"] +\
        vars["Liquids|Biomass"] +\
        vars["Liquids|Oil"] +\
        _Synfuel_other

    # --------
    # Hydrogen
    # --------

    vars["Hydrogen|Fossil"] =\
        pp.investment(["h2_coal", "h2_smr"], units=units_energy) +\
        pp.investment("h2_coal_ccs", units=units_energy) * 0.97 +\
        pp.investment("h2_smr_ccs", units=units_energy) * 0.83 +\
        _Coal_synf_ccs_h2 +\
        _Gas_synf_ccs_h2

    vars["Hydrogen|Renewable"] = pp.investment("h2_bio", units=units_energy) +\
        pp.investment("h2_bio_ccs", units=units_energy) * 0.98 +\
        _Bio_synf_ccs_h2

    vars["Hydrogen|Other"] = pp.investment(["h2_elec", "h2_liq", "h2_t_d",
                                            "lh2_exp", "lh2_imp", "lh2_bal",
                                            "lh2_regas", "lh2_t_d"],
                                           units=units_energy) +\
        pp.act_vom("h2_mix", units=units_energy) * 0.5

    # -----
    # Other
    # -----

    # All questionable variables from extraction that are not related directly
    # to extraction should be moved to Other
    # Note OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under "Liquids|Oil"

    vars["Other|Liquids|Oil|Transmission and Distribution"] =\
        pp.investment(["foil_t_d", "loil_t_d"], units=units_energy)

    vars["Other|Liquids|Oil|Other"] =\
        pp.investment(["foil_exp", "loil_exp", "oil_exp",
                       "oil_imp", "foil_imp", "loil_imp",
                       "loil_std", "oil_bal", "loil_sto"], units=units_energy)

    vars["Other|Gases|Transmission and Distribution"] =\
        pp.investment(["gas_t_d", "gas_t_d_ch4"], units=units_energy)

    vars["Other|Gases|Production"] =\
        pp.investment(["gas_bio", "coal_gas"], units=units_energy)

    vars["Other|Gases|Other"] =\
        pp.investment(["LNG_bal", "LNG_prod", "LNG_regas",
                       "LNG_exp", "LNG_imp", "gas_bal",
                       "gas_std", "gas_sto", "gas_exp_eeu",
                       "gas_exp_nam", "gas_exp_pao", "gas_exp_weu",
                       "gas_exp_cpa", "gas_exp_afr", "gas_exp_sas",
                       "gas_exp_scs", "gas_exp_cas", "gas_exp_ubm",
                       "gas_exp_rus", "gas_imp"], units=units_energy)

    vars["Other|Solids|Coal|Transmission and Distribution"] =\
        pp.investment(["coal_t_d", "coal_t_d-rc-SO2", "coal_t_d-rc-06p",
                       "coal_t_d-in-SO2", "coal_t_d-in-06p"], units=units_energy) +\
        pp.act_vom(["coal_t_d-rc-SO2", "coal_t_d-rc-06p", "coal_t_d-in-SO2",
                    "coal_t_d-in-06p", "coal_t_d"], units=units_energy) * 0.5

    vars["Other|Solids|Coal|Other"] =\
        pp.investment(["coal_exp", "coal_imp",
                       "coal_bal", "coal_std"], units=units_energy)

    vars["Other|Solids|Biomass|Transmission and Distribution"] =\
        pp.investment("biomass_t_d", units=units_energy)

    vars["Other|Other"] =\
        pp.investment(["SO2_scrub_ref"], units=units_energy) * 0.5 +\
        pp.investment(["SO2_scrub_ind"], units=units_energy)

    df = pp_utils.make_outputdf(vars, units_energy)
    return df
