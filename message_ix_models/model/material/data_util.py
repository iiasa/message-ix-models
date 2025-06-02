import os
from collections.abc import Mapping
from functools import lru_cache
from typing import TYPE_CHECKING, Literal, Optional

import message_ix
import numpy as np
import pandas as pd
from genno import Computer
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import (
    read_yaml_file,
    remove_from_list_if_exists,
)
from message_ix_models.model.structure import get_region_codes
from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context


def add_macro_materials(
    scen: message_ix.Scenario, filename: str, check_converge: bool = False
) -> message_ix.Scenario:
    """
    Prepare data for MACRO calibration by reading data from xlsx file

    Parameters
    ----------
    scen: message_ix.Scenario
        Scenario to be calibrated
    filename: str
        name of xlsx calibration data file
    check_converge: bool
        parameter passed to MACRO calibration function
    Returns
    -------
    message_ix.Scenario
        MACRO-calibrated Scenario instance
    """

    # Making a dictionary from the MACRO Excel file
    xls = pd.ExcelFile(package_data_path("material", "macro", filename))
    data = {}
    for s in xls.sheet_names:
        data[s] = xls.parse(s)

    # # Load the new GDP values
    # df_gdp = load_GDP_COVID()
    #
    # # substitute the gdp_calibrate
    # parname = "gdp_calibrate"
    #
    # # keep the historical GDP to pass the GDP check at add_macro()
    # df_gdphist = data[parname]
    # df_gdphist = df_gdphist.loc[df_gdphist.year < info.y0]
    # data[parname] = pd.concat(
    #     [df_gdphist, df_gdp.loc[df_gdp.year >= info.y0]], ignore_index=True
    # )

    # Calibration
    scen = scen.add_macro(data, check_convergence=check_converge)

    return scen


def modify_baseyear_bounds(scen: message_ix.Scenario) -> None:
    # TODO: instead of removing bounds, bounds should be updated with IEA data
    scen.check_out()
    th_tecs_to_keep = ["solar_i", "biomass_i"]
    for substr in ["up", "lo"]:
        df = scen.par(f"bound_activity_{substr}")
        scen.remove_par(
            f"bound_activity_{substr}",
            df[(df["technology"].str.endswith("_fs")) & (df["year_act"] == 2020)],
        )
        scen.remove_par(
            f"bound_activity_{substr}",
            df[
                (df["technology"].str.endswith("_i"))
                & (df["year_act"] == 2020)
                & ~(df["technology"].isin(th_tecs_to_keep))
            ],
        )
        scen.remove_par(
            f"bound_activity_{substr}",
            df[(df["technology"].str.endswith("_I")) & (df["year_act"] == 2020)],
        )
    scen.commit(comment="remove base year industry tec bounds")


@lru_cache
def get_region_map() -> Mapping[str, str]:
    """Construct a mapping from "COUNTRY" IDs to regions (nodes in the "R12" codelist).

    These "COUNTRY" IDs are produced by a certain script for processing the IEA
    Extended World Energy Balances; this script is *not* in :mod:`message_ix_models`;
    i.e. it is *not* the same as :mod:`.tools.iea.web`. They include some ISO 3166-1
    alpha-3 codes, but also other values like "GREENLAND" (instead of "GRL"), "KOSOVO",
    and "IIASA_SAS".

    This function reads the `material-region` annotation on items in the R12 node
    codelist, expecting a list of strings. Of these:

    - The special value "*" is interpreted to mean "include the IDs all of the child
      nodes of this node (i.e. their ISO 3166-1 alpha-3 codes) in the mapping".
    - All other values are mapped directly.

    The return value is cached for reuse.

    Returns
    -------
    dict
        Mapping from e.g. "KOSOVO" to e.g. "R12_EEU".
    """
    result = {}

    # - Load the R12 node codelist.
    # - Iterate over codes that are regions (i.e. omit World and the ISO 3166-1 alpha-3
    #   codes for individual countries within regions)
    for node in get_region_codes("R12"):
        # - Retrieve the "material-region" annotation and eval() it as a Python list.
        # - Iterate over each value in this list.
        for value in node.eval_annotation(id="material-region"):
            # Update (expand) the mapping
            if value == "*":  # Special value → map every child node's ID to the node ID
                result.update({child.id: node.id for child in node.child})
            else:  # Any other value → map it to the node ID
                result.update({value: node.id})

    return result


def map_iea_db_to_msg_regs(df_iea: pd.DataFrame) -> pd.DataFrame:
    """Add a "REGION" column to `df_iea`.

    Parameters
    ----------
    df_iea
        Data frame containing the IEA energy balances data set. This **must** have a
        "COUNTRY" column.

    Returns
    -------
    pandas.DataFrame
        with added column "REGION" containing node IDs according to
        :func:`get_region_map`.
    """
    # - Duplicate the "COUNTRY" column to "REGION".
    # - Replace the "REGION" values using the mapping.
    return df_iea.eval("REGION = COUNTRY").replace({"REGION": get_region_map()})


def read_iea_tec_map(tec_map_fname: str) -> pd.DataFrame:
    """
    reads mapping file and returns relevant columns needed for technology mapping

    Parameters
    ----------
    tec_map_fname
        name of mapping file used to map IEA flows and products
        to existing MESSAGEix technologies
    Returns
    -------
    pd.DataFrame
        returns df with mapped technologies
    """
    MAP = pd.read_csv(package_data_path("material", "iea_mappings", tec_map_fname))

    MAP = pd.concat([MAP, MAP["IEA flow"].str.split(", ", expand=True)], axis=1)
    MAP = (
        MAP.melt(
            value_vars=MAP.columns[-13:],
            value_name="FLOW",
            id_vars=["technology", "IEA product"],
        )
        .dropna()
        .drop("variable", axis=1)
    )
    MAP = pd.concat([MAP, MAP["IEA product"].str.split(", ", expand=True)], axis=1)
    MAP = (
        MAP.melt(
            value_vars=MAP.columns[-19:],
            value_name="PRODUCT",
            id_vars=["technology", "FLOW"],
        )
        .dropna()
        .drop("variable", axis=1)
    )
    MAP = MAP.drop_duplicates()
    return MAP


def add_emission_accounting(scen: message_ix.Scenario) -> None:
    """

    Parameters
    ----------
    scen
    """
    # (1) ******* Add non-CO2 gases to the relevant relations. ********
    # This is done by multiplying the input values and emission_factor
    # per year,region and technology for furnace technologies.

    tec_list_residual = scen.par("emission_factor")["technology"].unique()
    tec_list_input = scen.par("input")["technology"].unique()

    # The technology list to retrieve the input values for furnaces
    tec_list_input = [
        i for i in tec_list_input if (("furnace" in i) | ("hp_gas_" in i))
    ]
    # tec_list_input.remove("hp_gas_i")
    # tec_list_input.remove("hp_gas_rc")

    # The technology list to retreive the emission_factors
    tec_list_residual = [
        i
        for i in tec_list_residual
        if (
            (
                ("biomass_i" in i)
                | ("coal_i" in i)
                | ("foil_i" in i)
                | ("gas_i" in i)
                | ("hp_gas_i" in i)
                | ("loil_i" in i)
                | ("meth_i" in i)
            )
            & ("imp" not in i)
            & ("trp" not in i)
        )
    ]

    # Retrieve the input values
    input_df = scen.par("input", filters={"technology": tec_list_input})
    input_df.drop(
        ["node_origin", "commodity", "level", "time", "time_origin", "unit"],
        axis=1,
        inplace=True,
    )
    input_df.drop_duplicates(inplace=True)
    input_df = input_df[input_df["year_act"] >= 2020]

    # Retrieve the emission factors

    emission_df = scen.par("emission_factor", filters={"technology": tec_list_residual})
    emission_df.drop(["unit", "mode"], axis=1, inplace=True)
    emission_df = emission_df[emission_df["year_act"] >= 2020]
    emission_df.drop_duplicates(inplace=True)

    # Mapping to multiply the emission_factor with the corresponding
    # input values from new indsutry technologies

    dic = {
        "foil_i": [
            "furnace_foil_steel",
            "furnace_foil_aluminum",
            "furnace_foil_cement",
            "furnace_foil_petro",
            "furnace_foil_refining",
        ],
        "biomass_i": [
            "furnace_biomass_steel",
            "furnace_biomass_aluminum",
            "furnace_biomass_cement",
            "furnace_biomass_petro",
            "furnace_biomass_refining",
        ],
        "coal_i": [
            "furnace_coal_steel",
            "furnace_coal_aluminum",
            "furnace_coal_cement",
            "furnace_coal_petro",
            "furnace_coal_refining",
            "furnace_coke_petro",
            "furnace_coke_refining",
        ],
        "loil_i": [
            "furnace_loil_steel",
            "furnace_loil_aluminum",
            "furnace_loil_cement",
            "furnace_loil_petro",
            "furnace_loil_refining",
        ],
        "gas_i": [
            "furnace_gas_steel",
            "furnace_gas_aluminum",
            "furnace_gas_cement",
            "furnace_gas_petro",
            "furnace_gas_refining",
        ],
        "meth_i": [
            "furnace_methanol_steel",
            "furnace_methanol_aluminum",
            "furnace_methanol_cement",
            "furnace_methanol_petro",
            "furnace_methanol_refining",
        ],
        "hp_gas_i": [
            "hp_gas_steel",
            "hp_gas_aluminum",
            "hp_gas_cement",
            "hp_gas_petro",
            "hp_gas_refining",
        ],
    }

    # Create an empty dataframe
    df_non_co2_emissions = pd.DataFrame()

    # Find the technology, year_act, year_vtg, emission, node_loc combination
    emissions = [e for e in emission_df["emission"].unique()]
    remove_from_list_if_exists("CO2_industry", emissions)
    remove_from_list_if_exists("CO2_res_com", emissions)
    # emissions.remove("CO2_industry")
    # emissions.remove("CO2_res_com")

    for t in emission_df["technology"].unique():
        for e in emissions:
            # This should be a dataframe
            emission_df_filt = emission_df.loc[
                ((emission_df["technology"] == t) & (emission_df["emission"] == e))
            ]
            # Filter the technologies that we need the input value
            # This should be a dataframe
            input_df_filt = input_df[input_df["technology"].isin(dic[t])]
            if (emission_df_filt.empty) | (input_df_filt.empty):
                continue
            else:
                df_merged = pd.merge(
                    emission_df_filt,
                    input_df_filt,
                    on=["year_act", "year_vtg", "node_loc"],
                )
                df_merged["value"] = df_merged["value_x"] * df_merged["value_y"]
                df_merged.drop(
                    ["technology_x", "value_x", "value_y", "year_vtg", "emission"],
                    axis=1,
                    inplace=True,
                )
                df_merged.rename(columns={"technology_y": "technology"}, inplace=True)
                relation_name = e + "_Emission"
                df_merged["relation"] = relation_name
                df_merged["node_rel"] = df_merged["node_loc"]
                df_merged["year_rel"] = df_merged["year_act"]
                df_merged["unit"] = "???"
                df_non_co2_emissions = pd.concat([df_non_co2_emissions, df_merged])

        scen.check_out()
        scen.add_par("relation_activity", df_non_co2_emissions)
        scen.commit("Non-CO2 Emissions accounting for industry technologies added.")

    # ***** (2) Add the CO2 emission factors to CO2_Emission relation. ******
    # We dont need to add ammonia/fertilier production here. Because there are
    # no extra process emissions that need to be accounted in emissions relation.
    # CCS negative emission_factor are added to this relation in gen_data_ammonia.py.
    # Emissions from refining sector are categorized as 'CO2_transformation'.

    tec_list = scen.par("emission_factor")["technology"].unique()
    tec_list_materials = [
        i
        for i in tec_list
        if (
            ("steel" in i)
            | ("aluminum" in i)
            | ("petro" in i)
            | ("cement" in i)
            | ("ref" in i)
        )
    ]
    for elem in ["refrigerant_recovery", "replacement_so2", "SO2_scrub_ref"]:
        remove_from_list_if_exists(elem, tec_list_materials)
    # tec_list_materials.remove("refrigerant_recovery")
    # tec_list_materials.remove("replacement_so2")
    # tec_list_materials.remove("SO2_scrub_ref")
    emission_factors = scen.par(
        "emission_factor", filters={"technology": tec_list_materials, "emission": "CO2"}
    )
    # Note: Emission for CO2 MtC/ACT.
    relation_activity = emission_factors.assign(
        relation=lambda x: (x["emission"] + "_Emission")
    )
    relation_activity["node_rel"] = relation_activity["node_loc"]
    relation_activity.drop(["year_vtg", "emission"], axis=1, inplace=True)
    relation_activity["year_rel"] = relation_activity["year_act"]
    relation_activity_co2 = relation_activity[
        (relation_activity["relation"] != "PM2p5_Emission")
        & (relation_activity["relation"] != "CO2_industry_Emission")
        & (relation_activity["relation"] != "CO2_transformation_Emission")
    ]

    # ***** (3) Add thermal industry technologies to CO2_ind relation ******

    relation_activity_furnaces = scen.par(
        "emission_factor",
        filters={"emission": "CO2_industry", "technology": tec_list_materials},
    )
    relation_activity_furnaces["relation"] = "CO2_ind"
    relation_activity_furnaces["node_rel"] = relation_activity_furnaces["node_loc"]
    relation_activity_furnaces.drop(["year_vtg", "emission"], axis=1, inplace=True)
    relation_activity_furnaces["year_rel"] = relation_activity_furnaces["year_act"]
    relation_activity_furnaces = relation_activity_furnaces[
        ~relation_activity_furnaces["technology"].str.contains("_refining")
    ]

    # ***** (4) Add steel energy input technologies to CO2_ind relation ****

    relation_activity_steel = scen.par(
        "emission_factor",
        filters={
            "emission": "CO2_industry",
            "technology": ["DUMMY_coal_supply", "DUMMY_gas_supply"],
        },
    )
    relation_activity_steel["relation"] = "CO2_ind"
    relation_activity_steel["node_rel"] = relation_activity_steel["node_loc"]
    relation_activity_steel.drop(["year_vtg", "emission"], axis=1, inplace=True)
    relation_activity_steel["year_rel"] = relation_activity_steel["year_act"]

    # ***** (5) Add refinery technologies to CO2_cc ******

    relation_activity_ref = scen.par(
        "emission_factor",
        filters={"emission": "CO2_transformation", "technology": tec_list_materials},
    )
    relation_activity_ref["relation"] = "CO2_cc"
    relation_activity_ref["node_rel"] = relation_activity_ref["node_loc"]
    relation_activity_ref.drop(["year_vtg", "emission"], axis=1, inplace=True)
    relation_activity_ref["year_rel"] = relation_activity_ref["year_act"]

    scen.check_out()
    scen.add_par("relation_activity", relation_activity_co2)
    scen.add_par("relation_activity", relation_activity_furnaces)
    scen.add_par("relation_activity", relation_activity_steel)
    scen.add_par("relation_activity", relation_activity_ref)
    scen.commit("Emissions accounting for industry technologies added.")

    # ***** (6) Add feedstock using technologies to CO2_feedstocks *****
    nodes = scen.par("relation_activity", filters={"relation": "CO2_feedstocks"})[
        "node_rel"
    ].unique()
    years = scen.par("relation_activity", filters={"relation": "CO2_feedstocks"})[
        "year_rel"
    ].unique()

    for n in nodes:
        for t in ["steam_cracker_petro", "gas_processing_petro"]:
            for m in ["atm_gasoil", "vacuum_gasoil", "naphtha"]:
                if t == "steam_cracker_petro":
                    if m == "vacuum_gasoil":
                        # fueloil emission factor * input
                        val = 0.665 * 1.339
                    elif m == "atm_gasoil":
                        val = 0.665 * 1.435
                    else:
                        val = 0.665 * 1.537442922

                    co2_feedstocks = pd.DataFrame(
                        {
                            "relation": "CO2_feedstocks",
                            "node_rel": n,
                            "year_rel": years,
                            "node_loc": n,
                            "technology": t,
                            "year_act": years,
                            "mode": m,
                            "value": val,
                            "unit": "t",
                        }
                    )
                else:
                    # gas emission factor * gas input
                    val = 0.482 * 1.331811263

                    co2_feedstocks = pd.DataFrame(
                        {
                            "relation": "CO2_feedstocks",
                            "node_rel": n,
                            "year_rel": years,
                            "node_loc": n,
                            "technology": t,
                            "year_act": years,
                            "mode": "M1",
                            "value": val,
                            "unit": "t",
                        }
                    )
                scen.check_out()
                scen.add_par("relation_activity", co2_feedstocks)
                scen.commit("co2_feedstocks updated")

    # **** (7) Correct CF4 Emission relations *****
    # Remove transport related technologies from CF4_Emissions

    scen.check_out()

    CF4_trp_Emissions = scen.par(
        "relation_activity", filters={"relation": "CF4_Emission"}
    )
    list_tec_trp = [
        cf4_emi
        for cf4_emi in CF4_trp_Emissions["technology"].unique()
        if "trp" in cf4_emi
    ]
    CF4_trp_Emissions = CF4_trp_Emissions[
        CF4_trp_Emissions["technology"].isin(list_tec_trp)
    ]

    scen.remove_par("relation_activity", CF4_trp_Emissions)

    # Remove transport related technologies from CF4_alm_red and add aluminum tecs.

    CF4_red = scen.par("relation_activity", filters={"relation": "CF4_alm_red"})
    list_tec_trp = [
        cf4_emi for cf4_emi in CF4_red["technology"].unique() if "trp" in cf4_emi
    ]
    CF4_red = CF4_red[CF4_red["technology"].isin(list_tec_trp)]

    scen.remove_par("relation_activity", CF4_red)

    CF4_red_add = scen.par(
        "emission_factor",
        filters={
            "technology": ["soderberg_aluminum", "prebake_aluminum"],
            "emission": "CF4",
        },
    )
    CF4_red_add.drop(["year_vtg", "emission"], axis=1, inplace=True)
    CF4_red_add["unit"] = "???"
    CF4_red_add["year_rel"] = CF4_red_add["year_act"]
    CF4_red_add["node_rel"] = CF4_red_add["node_loc"]
    CF4_red_add["relation"] = "CF4_Emission"
    scen.add_par("relation_activity", CF4_red_add)

    CF4_red_add["relation"] = "CF4_alm_red"
    CF4_red_add["value"] *= 1000
    scen.add_par("relation_activity", CF4_red_add)
    scen.commit("CF4 relations corrected.")

    # copy CO2_cc values to CO2_industry for conventional methanol tecs
    # scen.check_out()
    # meth_arr = ["meth_ng", "meth_coal", "meth_coal_ccs", "meth_ng_ccs"]
    # df = scen.par("relation_activity",
    # filters={"relation": "CO2_cc", "technology": meth_arr})
    # df = df.rename({"year_rel": "year_vtg"}, axis=1)
    # values = dict(zip(df["technology"], df["value"]))
    #
    # df_em = scen.par("emission_factor",
    # filters={"emission": "CO2_transformation", "technology": meth_arr})
    # for i in meth_arr:
    #     df_em.loc[df_em["technology"] == i, "value"] = values[i]
    # df_em["emission"] = "CO2_industry"
    #
    # scen.add_par("emission_factor", df_em)
    # scen.commit("add methanol CO2_industry")


def add_elec_lowerbound_2020(scen: message_ix.Scenario) -> None:
    # To avoid zero i_spec prices only for R12_CHN, add the below section.
    # read input parameters for relevant technology/commodity combinations for
    # converting betwen final and useful energy

    input_residual_electricity = scen.par(
        "input",
        filters={"technology": "sp_el_I", "year_vtg": "2020", "year_act": "2020"},
    )

    # read processed final energy data from IEA extended energy balances
    # that is aggregated to MESSAGEix regions, fuels and (industry) sectors

    final = pd.read_csv(
        package_data_path("material", "other", "residual_industry_2019.csv")
    )

    # downselect needed fuels and sectors
    final_residual_electricity = final.query(
        'MESSAGE_fuel=="electr" & MESSAGE_sector=="industry_residual"'
    )

    # join final energy data from IEA energy balances and input coefficients
    # from final-to-useful technologies from MESSAGEix
    bound_residual_electricity = pd.merge(
        input_residual_electricity,
        final_residual_electricity,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    # derive useful energy values by dividing final energy by
    # input coefficient from final-to-useful technologies
    bound_residual_electricity["value"] = (
        bound_residual_electricity["Value"] / bound_residual_electricity["value"]
    )

    # downselect dataframe columns for MESSAGEix parameters
    bound_residual_electricity = bound_residual_electricity.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )
    # rename columns if necessary
    bound_residual_electricity.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    # Decrease 20% to aviod zero prices (the issue continiues otherwise)
    bound_residual_electricity["value"] = bound_residual_electricity["value"] * 0.8
    bound_residual_electricity = bound_residual_electricity[
        bound_residual_electricity["node_loc"] == "R12_CHN"
    ]

    scen.check_out()

    # add parameter dataframes to ixmp
    scen.add_par("bound_activity_lo", bound_residual_electricity)

    # Remove the previous bounds
    remove_par_lo = scen.par(
        "growth_activity_lo",
        filters={"technology": "sp_el_I", "year_act": 2020, "node_loc": "R12_CHN"},
    )
    scen.remove_par("growth_activity_lo", remove_par_lo)

    scen.commit("added lower bound for activity of residual electricity technologies")


def add_coal_lowerbound_2020(sc: message_ix.Scenario) -> None:
    """Set lower bounds for coal and i_spec as a calibration for 2020"""

    final_resid = pd.read_csv(
        package_data_path("material", "other", "residual_industry_2019.csv")
    )

    # read input parameters for relevant technology/commodity combinations
    # for converting betwen final and useful energy
    input_residual_coal = sc.par(
        "input",
        filters={"technology": "coal_i", "year_vtg": "2020", "year_act": "2020"},
    )
    input_cement_coal = sc.par(
        "input",
        filters={
            "technology": "furnace_coal_cement",
            "year_vtg": "2020",
            "year_act": "2020",
            "mode": "high_temp",
        },
    )
    input_residual_electricity = sc.par(
        "input",
        filters={"technology": "sp_el_I", "year_vtg": "2020", "year_act": "2020"},
    )

    # downselect needed fuels and sectors
    final_residual_coal = final_resid.query(
        'MESSAGE_fuel=="coal" & MESSAGE_sector=="industry_residual"'
    )
    final_cement_coal = final_resid.query(
        'MESSAGE_fuel=="coal" & MESSAGE_sector=="cement"'
    )
    final_residual_electricity = final_resid.query(
        'MESSAGE_fuel=="electr" & MESSAGE_sector=="industry_residual"'
    )

    # join final energy data from IEA energy balances and input
    # coefficients from final-to-useful technologies from MESSAGEix
    bound_coal = pd.merge(
        input_residual_coal,
        final_residual_coal,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )
    bound_cement_coal = pd.merge(
        input_cement_coal,
        final_cement_coal,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )
    bound_residual_electricity = pd.merge(
        input_residual_electricity,
        final_residual_electricity,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    # derive useful energy values by dividing final energy
    # by input coefficient from final-to-useful technologies
    bound_coal["value"] = bound_coal["Value"] / bound_coal["value"]
    bound_cement_coal["value"] = bound_cement_coal["Value"] / bound_cement_coal["value"]
    bound_residual_electricity["value"] = (
        bound_residual_electricity["Value"] / bound_residual_electricity["value"]
    )

    # downselect dataframe columns for MESSAGEix parameters
    bound_coal = bound_coal.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )
    bound_cement_coal = bound_cement_coal.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )
    bound_residual_electricity = bound_residual_electricity.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )

    # rename columns if necessary
    bound_coal.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]
    bound_cement_coal.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]
    bound_residual_electricity.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    # (Artificially) lower bounds when i_spec act is too close
    # to the bounds (avoid 0-price for macro calibration)
    more = ["R12_MEA", "R12_EEU", "R12_SAS", "R12_PAS"]
    # import pdb; pdb.set_trace()
    bound_residual_electricity.loc[
        bound_residual_electricity.node_loc.isin(["R12_PAO"]), "value"
    ] *= 0.80
    bound_residual_electricity.loc[
        bound_residual_electricity.node_loc.isin(more), "value"
    ] *= 0.85

    sc.check_out()

    # add parameter dataframes to ixmp
    sc.add_par("bound_activity_lo", bound_coal)
    # sc.add_par("bound_activity_lo", bound_cement_coal)
    sc.add_par("bound_activity_lo", bound_residual_electricity)

    # commit scenario to ixmp backend
    sc.commit(
        "added lower bound for activity of residual industrial coal"
        "and cement coal furnace technologies and "
        "adjusted 2020 residual industrial electricity demand"
    )


def add_cement_bounds_2020(sc: message_ix.Scenario) -> None:
    """Set lower and upper bounds for gas and oil as a calibration for 2020"""

    final_resid = pd.read_csv(
        package_data_path("material", "other", "residual_industry_2019.csv")
    )

    input_cement_foil = sc.par(
        "input",
        filters={
            "technology": "furnace_foil_cement",
            "year_vtg": "2020",
            "year_act": "2020",
            "mode": "high_temp",
        },
    )

    input_cement_loil = sc.par(
        "input",
        filters={
            "technology": "furnace_loil_cement",
            "year_vtg": "2020",
            "year_act": "2020",
            "mode": "high_temp",
        },
    )

    input_cement_gas = sc.par(
        "input",
        filters={
            "technology": "furnace_gas_cement",
            "year_vtg": "2020",
            "year_act": "2020",
            "mode": "high_temp",
        },
    )

    input_cement_biomass = sc.par(
        "input",
        filters={
            "technology": "furnace_biomass_cement",
            "year_vtg": "2020",
            "year_act": "2020",
            "mode": "high_temp",
        },
    )

    input_cement_coal = sc.par(
        "input",
        filters={
            "technology": "furnace_coal_cement",
            "year_vtg": "2020",
            "year_act": "2020",
            "mode": "high_temp",
        },
    )

    # downselect needed fuels and sectors
    final_cement_foil = final_resid.query(
        'MESSAGE_fuel=="foil" & MESSAGE_sector=="cement"'
    )

    final_cement_loil = final_resid.query(
        'MESSAGE_fuel=="loil" & MESSAGE_sector=="cement"'
    )

    final_cement_gas = final_resid.query(
        'MESSAGE_fuel=="gas" & MESSAGE_sector=="cement"'
    )

    final_cement_biomass = final_resid.query(
        'MESSAGE_fuel=="biomass" & MESSAGE_sector=="cement"'
    )

    final_cement_coal = final_resid.query(
        'MESSAGE_fuel=="coal" & MESSAGE_sector=="cement"'
    )

    # join final energy data from IEA energy balances and input coefficients
    # from final-to-useful technologies from MESSAGEix
    bound_cement_loil = pd.merge(
        input_cement_loil,
        final_cement_loil,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    bound_cement_foil = pd.merge(
        input_cement_foil,
        final_cement_foil,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    bound_cement_gas = pd.merge(
        input_cement_gas,
        final_cement_gas,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    bound_cement_biomass = pd.merge(
        input_cement_biomass,
        final_cement_biomass,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    bound_cement_coal = pd.merge(
        input_cement_coal,
        final_cement_coal,
        left_on="node_loc",
        right_on="MESSAGE_region",
        how="inner",
    )

    # derive useful energy values by dividing final energy
    # by input coefficient from final-to-useful technologies
    bound_cement_loil["value"] = bound_cement_loil["Value"] / bound_cement_loil["value"]
    bound_cement_foil["value"] = bound_cement_foil["Value"] / bound_cement_foil["value"]
    bound_cement_gas["value"] = bound_cement_gas["Value"] / bound_cement_gas["value"]
    bound_cement_biomass["value"] = (
        bound_cement_biomass["Value"] / bound_cement_biomass["value"]
    )
    bound_cement_coal["value"] = bound_cement_coal["Value"] / bound_cement_coal["value"]

    # downselect dataframe columns for MESSAGEix parameters
    bound_cement_loil = bound_cement_loil.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )

    bound_cement_foil = bound_cement_foil.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )

    bound_cement_gas = bound_cement_gas.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )

    bound_cement_biomass = bound_cement_biomass.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )

    bound_cement_coal = bound_cement_coal.filter(
        items=["node_loc", "technology", "year_act", "mode", "time", "value", "unit_x"]
    )

    # rename columns if necessary
    bound_cement_loil.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    bound_cement_foil.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    bound_cement_gas.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    bound_cement_biomass.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    bound_cement_coal.columns = [
        "node_loc",
        "technology",
        "year_act",
        "mode",
        "time",
        "value",
        "unit",
    ]

    sc.check_out()
    nodes = bound_cement_loil["node_loc"].values
    years = bound_cement_loil["year_act"].values

    # add parameter dataframes to ixmp
    sc.add_par("bound_activity_up", bound_cement_loil)
    sc.add_par("bound_activity_up", bound_cement_foil)
    # sc.add_par("bound_activity_lo", bound_cement_gas)
    sc.add_par("bound_activity_up", bound_cement_gas)
    sc.add_par("bound_activity_up", bound_cement_biomass)
    sc.add_par("bound_activity_up", bound_cement_coal)

    for n in nodes:
        bound_cement_meth = pd.DataFrame(
            {
                "node_loc": n,
                "technology": "furnace_methanol_cement",
                "year_act": years,
                "mode": "high_temp",
                "time": "year",
                "value": 0,
                "unit": "???",
            }
        )

        sc.add_par("bound_activity_lo", bound_cement_meth)
        sc.add_par("bound_activity_up", bound_cement_meth)

    for n in nodes:
        bound_cement_eth = pd.DataFrame(
            {
                "node_loc": n,
                "technology": "furnace_ethanol_cement",
                "year_act": years,
                "mode": "high_temp",
                "time": "year",
                "value": 0,
                "unit": "???",
            }
        )

        sc.add_par("bound_activity_lo", bound_cement_eth)
        sc.add_par("bound_activity_up", bound_cement_eth)

    # commit scenario to ixmp backend
    sc.commit("added lower and upper bound for fuels for cement 2020.")


def read_sector_data(
    scenario: message_ix.Scenario, sectname: str, ssp: Optional[str], filename: str
) -> pd.DataFrame:
    """Read sector data for industry with `sectname`.

    Parameters
    ----------
    scenario :
    sectname :
        Name of industry sector.
    ssp :
        If sector data should be read from an SSP-specific file.
    filename :
        Name of input file with suffix.

    Returns
    -------
    pd.DataFrame
    """
    # Read in technology-specific parameters from input xlsx
    # Now used for steel and cement, which are in one file

    import numpy as np

    s_info = ScenarioInfo(scenario)

    if "R12_CHN" in s_info.N:
        sheet_n = sectname + "_R12"
    else:
        sheet_n = sectname + "_R11"

    if filename.endswith(".csv"):
        data_df = pd.read_csv(
            package_data_path("material", sectname, filename), comment="#"
        )
    else:
        data_df = pd.read_excel(
            package_data_path("material", sectname, ssp, filename),
            sheet_name=sheet_n,
        )

    # Clean the data
    data_df = data_df[
        [
            "Region",
            "Technology",
            "Parameter",
            "Level",
            "Commodity",
            "Mode",
            "Species",
            "Units",
            "Value",
        ]
    ].replace(np.nan, "", regex=True)

    # Combine columns and remove ''
    list_series = (
        data_df[["Parameter", "Commodity", "Level", "Mode"]]
        .apply(list, axis=1)
        .apply(lambda x: list(filter(lambda a: a != "", x)))
    )
    list_ef = data_df[["Parameter", "Species", "Mode"]].apply(list, axis=1)

    data_df["parameter"] = list_series.str.join("|")
    data_df.loc[data_df["Parameter"] == "emission_factor", "parameter"] = (
        list_ef.str.join("|")
    )

    data_df = data_df.drop(["Parameter", "Level", "Commodity", "Mode"], axis=1)
    data_df = data_df.drop(data_df[data_df.Value == ""].index)

    data_df.columns = data_df.columns.str.lower()

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_df


def add_ccs_technologies(scen: message_ix.Scenario) -> None:
    """Adds the relevant CCS technologies to the co2_trans_disp and bco2_trans_disp
    relations

    Parameters
    ----------
    scen: message_ix.Scenario
        Scenario instance to add CCS emission factor parametrization to
    """

    # The relation coefficients for CO2_Emision and bco2_trans_disp and
    # co2_trans_disp are both MtC. The emission factor for CCS add_ccs_technologies
    # are specified in MtC as well.
    bco2_trans_relation = scen.par(
        "emission_factor", filters={"technology": "biomass_NH3_ccs", "emission": "CO2"}
    )
    co2_trans_relation = scen.par(
        "emission_factor",
        filters={
            "technology": [
                "clinker_dry_ccs_cement",
                "clinker_wet_ccs_cement",
                "gas_NH3_ccs",
                "coal_NH3_ccs",
                "fueloil_NH3_ccs",
                "bf_ccs_steel",
                "dri_gas_ccs_steel",
            ],
            "emission": "CO2",
        },
    )

    bco2_trans_relation.drop(["year_vtg", "emission", "unit"], axis=1, inplace=True)
    bco2_trans_relation["relation"] = "bco2_trans_disp"
    bco2_trans_relation["node_rel"] = bco2_trans_relation["node_loc"]
    bco2_trans_relation["year_rel"] = bco2_trans_relation["year_act"]
    bco2_trans_relation["unit"] = "???"

    co2_trans_relation.drop(["year_vtg", "emission", "unit"], axis=1, inplace=True)
    co2_trans_relation["relation"] = "co2_trans_disp"
    co2_trans_relation["node_rel"] = co2_trans_relation["node_loc"]
    co2_trans_relation["year_rel"] = co2_trans_relation["year_act"]
    co2_trans_relation["unit"] = "???"

    scen.check_out()
    scen.add_par("relation_activity", bco2_trans_relation)
    scen.add_par("relation_activity", co2_trans_relation)
    scen.commit("New CCS technologies added to the CO2 accounting relations.")


def read_timeseries(
    scenario: message_ix.Scenario, material: str, ssp: Optional[str], filename: str
) -> pd.DataFrame:
    """Read ‘time-series’ data from a material-specific `filename`.

    Read "timeseries" type data from a sector-specific input file to data frame and
    format as MESSAGE parameter data.

    Parameters
    ----------
    scenario :
        Scenario used to get structural information like model regions and years.
    material :
        Name of material folder (**‘sector’**) where `filename` is located.
    ssp :
        If timeseries is available for different SSPs, the respective file is selected.
    filename :
        Name of data file including :file:`.csv` or :file:`.xlsx` suffix.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the timeseries data for MESSAGEix parameters
    """
    # Ensure config is loaded, get the context
    s_info = ScenarioInfo(scenario)

    # if context.scenario_info['scenario'] == 'NPi400':
    #     sheet_name="timeseries_NPi400"
    # else:
    #     sheet_name = "timeseries"

    if "R12_CHN" in s_info.N:
        sheet_n = "timeseries_R12"
    else:
        sheet_n = "timeseries_R11"

    material = f"{material}/{ssp}" if ssp else material
    # Read the file

    if filename.endswith(".csv"):
        df = pd.read_csv(package_data_path("material", material, filename))

        # Function to convert string to integer if it is a digit
        def convert_if_digit(col_name):
            return int(col_name) if col_name.isdigit() else col_name

        # Apply the function to the DataFrame column names
        df = df.rename(columns=convert_if_digit)
    else:
        df = pd.read_excel(
            package_data_path("material", material, filename), sheet_name=sheet_n
        )

    import numbers

    # Take only existing years in the data
    datayears = [x for x in list(df) if isinstance(x, numbers.Number)]

    df = pd.melt(
        df,
        id_vars=[
            "parameter",
            "region",
            "technology",
            "mode",
            "units",
            "commodity",
            "level",
        ],
        value_vars=datayears,
        var_name="year",
    )

    df = df.drop(df[np.isnan(df.value)].index)
    return df


def read_rel(
    scenario: message_ix.Scenario, material: str, ssp: str or None, filename: str
) -> pd.DataFrame:
    """
    Read relation_* type parameter data for specific industry

    Parameters
    ----------
    ssp: str
        if relations are available for different SSPs, the respective file is selected
    scenario:
        scenario used to get structural information like
    material: str
        name of material folder where xlsx is located
    filename:
        name of xlsx file

    Returns
    -------
    pd.DataFrame
        DataFrame containing relation_* parameter data
    """
    # Ensure config is loaded, get the context

    s_info = ScenarioInfo(scenario)

    if "R12_CHN" in s_info.N:
        sheet_n = "relations_R12"
    else:
        sheet_n = "relations_R11"
    material = f"{material}/{ssp}" if ssp else material
    # Read the file
    if filename.endswith(".csv"):
        data_rel = pd.read_csv(package_data_path("material", material, filename))
    else:
        data_rel = pd.read_excel(
            package_data_path("material", material, filename), sheet_name=sheet_n
        )
    return data_rel


def gen_te_projections(
    scen: message_ix.Scenario,
    ssp: Literal["all", "LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"] = "SSP2",
    method: Literal["constant", "convergence", "gdp"] = "convergence",
    ref_reg: str = "R12_NAM",
    module="materials",
    reduction_year=2100,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calls message_ix_models.tools.costs with config for MESSAGEix-Materials
    and return inv_cost and fix_cost projections for energy and materials
    technologies

    Parameters
    ----------
    scen: message_ix.Scenario
        Scenario instance is required to get technology set
    ssp: str
        SSP to use for projection assumptions
    method: str
        method to use for cost convergence over time
    ref_reg: str
        reference region to use for regional cost differentiation

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        tuple with "inv_cost" and "fix_cost" DataFrames
    """
    model_tec_set = list(scen.set("technology"))
    cfg = Config(
        module=module,
        ref_region=ref_reg,
        method=method,
        format="message",
        scenario=ssp,
        reduction_year=reduction_year,
    )
    out_materials = create_cost_projections(cfg)
    fix_cost = (
        out_materials["fix_cost"]
        .drop_duplicates()
        .drop(["scenario_version", "scenario"], axis=1)
    )
    fix_cost = fix_cost[fix_cost["technology"].isin(model_tec_set)]

    inv_cost = (
        out_materials["inv_cost"]
        .drop_duplicates()
        .drop(["scenario_version", "scenario"], axis=1)
    )
    inv_cost = inv_cost[inv_cost["technology"].isin(model_tec_set)]
    return inv_cost, fix_cost


def get_ssp_soc_eco_data(
    context: "Context", model: str, measure: str, tec: str
) -> pd.DataFrame:
    """
    Function to update scenario GDP and POP timeseries to SSP 3.0
    and format to MESSAGEix "bound_activity_*" DataFrame

    Parameters
    ----------
    context: Context
        context used to prepare genno.Computer
    model:
        model name of projections to read
    measure:
        Indicator to read (GDP or Population)
    tec:
        name to use for "technology" column
    Returns
    -------
    pd.DataFrame
        DataFrame with SSP indicator data in "bound_activity_*" parameter
        format
    """
    from message_ix_models.project.ssp.data import SSPUpdate

    c = Computer()
    keys = SSPUpdate.add_tasks(
        c, context=context, release="3.1", measure=measure, ssp_id="2"
    )
    return (
        c.get(keys[0])
        .to_dataframe()
        .reset_index()
        .rename(columns={"n": "node_loc", "y": "year_act"})
        .assign(mode="P", technology=tec, time="year", unit="GWa")
    )


def add_elec_i_ini_act(scenario: message_ix.Scenario) -> None:
    """
    Adds initial_activity_up parameter for "elec_i" technology by copying
    value from "hp_el_i" technology

    Parameters
    ----------
    scenario: message_ix.Scenario
        Scenario where "elec_i" should be updated
    """
    par = "initial_activity_up"
    df_el = scenario.par(par, filters={"technology": "hp_el_i"})
    df_el["technology"] = "elec_i"
    scenario.check_out()
    scenario.add_par(par, df_el)
    scenario.commit("add initial_activity_up for elec_i")
    return


def calculate_ini_new_cap(
    df_demand: pd.DataFrame, technology: str, material: str, ssp: str
) -> pd.DataFrame:
    """Derive ``initial_new_capacity_up`` data for CCS from projected cement demand.

    Parameters
    ----------
    df_demand :
        DataFrame containing "demand" MESSAGEix parametrization.
    technology :
        Name of CCS technology to be parametrized.
    material :
        Name of the material/industry sector.

    Returns
    -------
    pd.DataFrame
        Formatted to "initial_new_capacity_up" columns.
    """

    SCALER = {
        ("LED", "cement"): 0.001,
        ("SSP1", "cement"): 0.001,
        ("SSP2", "cement"): 0.001,
        ("SSP3", "cement"): 0.0008,
        ("SSP4", "cement"): 0.002,
        ("SSP5", "cement"): 0.002,
        ("LED", "steel"): 0.002,
        ("SSP1", "steel"): 0.002,
        ("SSP2", "steel"): 0.002,
        ("SSP3", "steel"): 0.001,
        ("SSP4", "steel"): 0.003,
        ("SSP5", "steel"): 0.003,
    }
    scalar = SCALER[(ssp, material)]
    CLINKER_RATIO = 0.72 if material == "cement" else 1

    tmp = (
        df_demand.eval("value = value * @CLINKER_RATIO * @scalar")
        .rename(columns={"node": "node_loc", "year": "year_vtg"})
        .assign(technology=technology)
    )

    return make_df("initial_new_capacity_up", **tmp)

    del scalar, CLINKER_RATIO  # pragma: no cover — quiet lint error F821 above


def calibrate_t_d_tecs(scenario: "Scenario"):
    # ------------------------------------------------------
    # Revise t_d historical_activity and 2020 activity bounds
    # ------------------------------------------------------

    # The activity of the t_d technologies is adjusted to fit
    # with the historical_activity/bda_lo calibration for all
    # technologies which take from the output commodity/level.

    tecs = [t for t in scenario.set("technology").tolist() if t.find("t_d") >= 0]
    td = (
        scenario.par("output", filters={"technology": tecs})[
            ["node_loc", "technology", "commodity", "level"]
        ]
        .drop_duplicates()
        .reset_index()
        .drop("index", axis=1)
    )
    update_df = pd.DataFrame()
    for i in td.index:
        try:
            row = td.iloc[i]
        except Exception:
            here = 1  # noqa: F841 —for debugging?
        # Retrieve input of techologies which take from the commodity/level.
        inp = scenario.par(
            "input",
            filters={
                "node_loc": row.node_loc,
                "commodity": row.commodity,
                "level": row.level,
            },
        )

        # Retrieve historical and calibrated activity as bound_activity_lo
        # and combine the two.
        histact = scenario.par(
            "historical_activity",
            filters={
                "node_loc": row.node_loc,
                "technology": inp.technology.unique().tolist(),
            },
        )
        act = scenario.par(
            "bound_activity_lo",
            filters={
                "node_loc": row.node_loc,
                "technology": inp.technology.unique().tolist(),
                "year_act": 2020,
            },
        )

        act = (
            pd.concat([histact, act])
            .sort_values(by=["node_loc", "technology", "year_act"])
            .set_index(["node_loc", "technology", "year_act"])
        )

        # Multiply the activity data with the input efficiencies to derive total energy
        # requirements
        act["eff"] = (
            inp.loc[
                (inp.year_vtg == inp.year_act)
                & (inp.node_loc == row.node_loc)
                & (inp.technology.isin(act.reset_index().technology.unique().tolist()))
                & (inp.year_act.isin(act.reset_index().year_act.unique().tolist()))
            ]
            .set_index(["node_loc", "technology", "year_act"])
            .value
        )

        # Backfill any missing values
        act["eff"] = act["eff"].bfill()

        act["fe"] = act["eff"] * act["value"]

        # Aggregate the final energy data and assign the t_d technology name
        df = act.copy()
        df = (
            df.reset_index()
            .groupby(["node_loc", "year_act"])
            .sum(numeric_only=True)[["fe"]]
            .rename(columns={"fe": "value"})
            .assign(unit="GWa", technology=row.technology, mode="M1", time="year")
            .reset_index()
        )

        update_df = pd.concat([update_df, df])

    update_df = update_df.reset_index().drop("index", axis=1)

    # Make a manual adjusment to "gas_t_d_ch4".
    # Because is has the same output as "gas_t_d", it will also get the
    # the same historical_acitivty, therefore it is set to zero.
    update_df.loc[update_df.technology == "gas_t_d_ch4", "value"] = 0

    # Add data as historical_activity and bound_activity_lo/up (*1.005)
    with scenario.transact("Calibrate t_d technology"):
        hist_df = update_df.copy()
        hist_df = hist_df.loc[hist_df.year_act < scenario.firstmodelyear]
        scenario.add_par("historical_activity", hist_df)

        scenario.add_par("bound_activity_lo", update_df)
        update_df.value *= 1.005
        scenario.add_par("bound_activity_up", update_df)


def add_water_par_data(scenario: "Scenario") -> None:
    """Adds water supply technologies that are required for the Materials build

    Parameters
    ----------
    scenario: .Scenario
        instance to check for water technologies and add if missing
    """
    scenario.check_out()
    water_dict = pd.read_excel(
        package_data_path("material", "other", "water_tec_pars.xlsx"),
        sheet_name=None,
    )
    for par in water_dict.keys():
        scenario.add_par(par, water_dict[par])
    scenario.commit("add missing water tecs")


def calibrate_for_SSPs(scenario: "Scenario") -> None:
    """Adjust technologies activity bounds and growth constraints to avoid base year
    infeasibilities in year 2020. Specifically developed for the SSP_dev scenarios,
    where most technology activities are fixed in 2020.

    Parameters
    ----------
    scenario: .Scenario
        instance to apply parameter changes to
    """
    add_elec_i_ini_act(scenario)

    # prohibit electric clinker kilns in first decade
    common = {
        "technology": "furnace_elec_cement",
        "mode": ["high_temp", "low_temp"],
        "time": "year",
        "value": 0,
        "unit": "GWa",
        "year_act": [2020, 2025],
    }
    s_info = ScenarioInfo(scenario)
    scenario.check_out()
    scenario.add_par(
        "bound_activity_up",
        make_df("bound_activity_up", **common).pipe(
            broadcast, node_loc=nodes_ex_world(s_info.N)
        ),
    )
    scenario.commit("add bound for thermal electr use in cement")

    # relax 2020 growth constraint for RCPA to avoid infeasibility
    scenario.check_out()
    df = scenario.par(
        "growth_activity_up", filters={"node_loc": "R12_RCPA", "year_act": 2020}
    )
    df = df[
        (df["technology"].str.endswith("_i")) | (df["technology"].str.endswith("_I"))
    ]
    df["value"] = 5
    scenario.add_par("growth_activity_up", df)
    scenario.commit("remove growth constraints in RCPA industry")

    # remove sp_eth_I historical activity, which is most likely from old scenario runs
    df = scenario.par("historical_activity", filters={"technology": "sp_eth_I"})
    scenario.check_out()
    scenario.remove_par("historical_activity", df)
    scenario.commit("remove sp_eth_I hist act from 2015")

    # correct wrong Viet Nam IEA numbers
    df = scenario.par(
        "historical_activity",
        filters={"node_loc": "R12_RCPA", "technology": "sp_el_I", "year_act": 2015},
    )
    df["value"] = 7
    scenario.check_out()
    scenario.add_par("historical_activity", df)
    scenario.commit("increase wrong RCPA hist act in 2015")

    df = scenario.par(
        "initial_activity_up",
        filters={
            "node_loc": "R12_RCPA",
            "year_act": 2020,
            "technology": "loil_i",
        },
    )
    df["value"] = 0.058
    scenario.check_out()
    scenario.add_par("initial_activity_up", df)
    scenario.commit("add loil_i ini act for RCPA 2020")

    df = scenario.par(
        "growth_activity_lo",
        filters={
            "node_loc": "R12_FSU",
            "year_act": 2020,
            "technology": "loil_i",
        },
    )
    df["value"] = -5
    scenario.check_out()
    scenario.add_par("growth_activity_lo", df)
    scenario.commit("fix loil_i gro lo for FSU 2020")

    for bound in ["up", "lo"]:
        par = f"bound_activity_{bound}"
        df = scenario.par(par, filters={"year_act": 2020})
        scenario.check_out()
        scenario.remove_par(
            f"bound_activity_{bound}", df[df["technology"].str.contains("t_d")]
        )
        scenario.commit("remove t_d 2020 bounds")

        df = scenario.par(par, filters={"technology": "elec_i"})
        df["value"] = 0
        scenario.check_out()
        scenario.add_par(par, df)
        scenario.commit("set elec_i bounds 2020 to 0")

    df = scenario.par("historical_activity", filters={"technology": "elec_i"})
    scenario.check_out()
    scenario.remove_par("historical_activity", df)
    scenario.commit("remove elec_i hist act")
    df = scenario.par("historical_activity", filters={"technology": "elec_i"})
    scenario.check_out()
    scenario.remove_par("historical_activity", df)
    scenario.commit("remove elec_i hist act")

    df = scenario.par(
        "growth_activity_lo",
        filters={
            "node_loc": ["R12_CHN"],
            "year_act": 2020,
            "technology": "coal_i",
        },
    )
    df["value"] = -0.1
    scenario.check_out()
    scenario.add_par("growth_activity_lo", df)
    scenario.commit("decrease gro lo for coal_ i CHN 2020")

    df = scenario.par(
        "growth_activity_lo",
        filters={
            "node_loc": ["R12_LAM"],
            "year_act": 2020,
            "technology": "gas_i",
        },
    )
    df["value"] = -0.04
    scenario.check_out()
    scenario.add_par("growth_activity_lo", df)
    scenario.commit("decrease gro lo for gas_i LAM 2020")

    df = scenario.par(
        "growth_activity_lo",
        filters={
            "node_loc": ["R12_RCPA"],
            "year_act": 2020,
            "technology": "foil_i",
        },
    )
    df["value"] = -0.1
    scenario.check_out()
    scenario.add_par("growth_activity_lo", df)
    scenario.commit("decrease gro lo for foil_i RCPA 2020")

    df = scenario.par(
        "growth_activity_up",
        filters={
            "node_loc": ["R12_CHN"],
            "year_act": 2020,
            "technology": ["loil_i", "gas_i"],
        },
    )
    df["value"] *= 4
    scenario.check_out()
    scenario.add_par("growth_activity_up", df)
    scenario.commit("increase gro up for loil_i/gas_i CHN 2020")

    df = scenario.par(
        "growth_activity_up",
        filters={
            "node_loc": ["R12_CHN"],
            "year_act": 2020,
            "technology": ["loil_i", "gas_i", "heat_i"],
        },
    )
    df["value"] = 5
    scenario.check_out()
    scenario.add_par("growth_activity_up", df)
    scenario.commit("increase gro up for loil_i/gas_i/heat_i CHN 2020")

    return


def gen_plastics_emission_factors(
    info, species: Literal["methanol", "HVCs", "ethanol"]
) -> dict[str, pd.DataFrame]:
    """Generate "CO2_Emission" relation parameter that
    represents stored carbon in produced plastics.
    The calculation considers:
    * carbon content of feedstocks,
    * the share that is converted to plastics
    * the end-of-life treatment (i.e. incineration, landfill, etc.)

    Values are negative since they need to be deducted
    from top-down accounting, which assumes that all extracted
    carbonaceous resources are released as carbon emissions.
    (Which is not correct for carbon used in long-lived products)

    Parameters
    ----------
    species:
        feedstock species to generate relation for
    info: ScenarioInfo

    Returns
    -------
    Dict[str, pd.DataFrame]
    """

    tec_species_map = {"methanol": "meth_ind_fs", "HVCs": "production_HVC"}

    carbon_pars = read_yaml_file(
        package_data_path(
            "material", "petrochemicals", "chemicals_carbon_parameters.yaml"
        )
    )
    # TODO: move EOL parameters to a different file to disassociate from methanol model
    end_of_life_pars = pd.read_excel(
        package_data_path("material", "methanol", "methanol_sensitivity_pars.xlsx"),
        sheet_name="Sheet1",
        dtype=object,
    )
    seq_carbon = {
        k: (v["carbon mass"] / v["molar mass"]) * v["plastics use"]
        for k, v in carbon_pars[species].items()
    }
    end_of_life_pars = end_of_life_pars.set_index("par").to_dict()["value"]
    common = {
        "unit": "???",
        "relation": "CO2_Emission",
        "mode": seq_carbon.keys(),
        "technology": tec_species_map[species],
    }
    co2_emi_rel = make_df("relation_activity", **common).drop(columns="value")
    co2_emi_rel = co2_emi_rel.merge(
        pd.Series(seq_carbon, name="value").to_frame().reset_index(),
        left_on="mode",
        right_on="index",
    ).drop(columns="index")

    years = info.Y
    co2_emi_rel = co2_emi_rel.pipe(broadcast, year_act=years)
    co2_emi_rel["year_rel"] = co2_emi_rel["year_act"]

    co2_emi_rel = co2_emi_rel.pipe(broadcast, node_loc=nodes_ex_world(info.N)).pipe(
        same_node
    )

    def apply_eol_factor(row, pars):
        if row["year_act"] < pars["incin_trend_end"]:
            share = pars["incin_rate"] + pars["incin_trend"] * (row["year_act"] - 2020)
        else:
            share = 0.5
        return row["value"] * (1 - share)

    co2_emi_rel["value"] = co2_emi_rel.apply(
        lambda x: apply_eol_factor(x, end_of_life_pars), axis=1
    ).mul(-1)
    return {"relation_activity": co2_emi_rel}


def gen_chemicals_co2_ind_factors(
    info, species: Literal["methanol", "HVCs"]
) -> dict[str, pd.DataFrame]:
    """Generate "CO2_ind" relation parameter that
    represents carbon in chemical feedstocks that is oxidized in the
    short-term (=during the model horizon) in downstream products.
    This happens either through natural oxidation or combustion as the
    end-of-life treatment of plastics.

    The calculation considers:
    * carbon content of feedstocks,
    * the share that is converted to oxidizable chemicals
    * the end-of-life treatment shares (i.e. incineration, landfill, etc.)

    Values are positive since they need to be added
    to bottom-up emission accounting.

    Parameters
    ----------
    species:
        feedstock species to generate relation for
    info: ScenarioInfo

    Returns
    -------
    Dict[str, pd.DataFrame]
    """

    tec_species_map = {
        "methanol": "meth_ind_fs",
        "HVCs": "production_HVC",
        "ethanol": "ethanol_to_ethylene_petro",
    }

    carbon_pars = read_yaml_file(
        package_data_path(
            "material", "petrochemicals", "chemicals_carbon_parameters.yaml"
        )
    )
    # TODO: move EOL parameters to a different file to disassociate from methanol model
    end_of_life_pars = pd.read_excel(
        package_data_path("material", "methanol", "methanol_sensitivity_pars.xlsx"),
        sheet_name="Sheet1",
        dtype=object,
    )
    temporary_sequestered = {
        k: (v["carbon mass"] / v["molar mass"]) * (1 - v["plastics use"])
        for k, v in carbon_pars[species].items()
    }
    embodied_carbon_plastics = {
        k: (v["carbon mass"] / v["molar mass"]) * v["plastics use"]
        for k, v in carbon_pars[species].items()
    }
    end_of_life_pars = end_of_life_pars.set_index("par").to_dict()["value"]
    common = {
        "unit": "???",
        "relation": "CO2_ind",
        "mode": embodied_carbon_plastics.keys(),
        "technology": tec_species_map[species],
    }
    co2_emi_rel = make_df("relation_activity", **common).drop(columns="value")
    co2_emi_rel = co2_emi_rel.merge(
        pd.Series(embodied_carbon_plastics, name="value").to_frame().reset_index(),
        left_on="mode",
        right_on="index",
    ).drop(columns="index")

    years = info.Y
    co2_emi_rel = co2_emi_rel.pipe(broadcast, year_act=years)
    co2_emi_rel["year_rel"] = co2_emi_rel["year_act"]

    co2_emi_rel = co2_emi_rel.pipe(broadcast, node_loc=nodes_ex_world(info.N)).pipe(
        same_node
    )

    def apply_eol_factor(row, pars):
        if row["year_act"] < pars["incin_trend_end"]:
            share = pars["incin_rate"] + pars["incin_trend"] * (row["year_act"] - 2020)
        else:
            share = 0.5
        return row["value"] * share

    co2_emi_rel["value"] = co2_emi_rel.apply(
        lambda x: apply_eol_factor(x, end_of_life_pars), axis=1
    )

    def add_non_combustion_oxidation(row):
        return temporary_sequestered[row["mode"]] + row["value"]

    co2_emi_rel["value"] = co2_emi_rel.apply(
        lambda x: add_non_combustion_oxidation(x), axis=1
    )
    return {"relation_activity": co2_emi_rel}


def gen_ethanol_to_ethylene_emi_factor(info) -> dict[str, pd.DataFrame]:
    """Generate "CO2_ind" relation parameter that
    represents carbon in chemical feedstocks that is oxidized in the
    short-term (=during the model horizon) in downstream products.
    This happens either through natural oxidation or combustion as the
    end-of-life treatment of plastics.

    The calculation considers:
    * carbon content of feedstocks,
    * the share that is converted to oxidizable chemicals
    * the end-of-life treatment shares (i.e. incineration, landfill, etc.)

    Values are positive since they need to be added
    to bottom-up emission accounting.

    Parameters
    ----------
    info: ScenarioInfo

    Returns
    -------
    Dict[str, pd.DataFrame]
    """

    carbon_pars = read_yaml_file(
        package_data_path(
            "material", "petrochemicals", "chemicals_carbon_parameters.yaml"
        )
    )
    embodied_carbon_plastics = {
        k: (v["carbon mass"] / v["molar mass"]) * v["plastics use"]
        for k, v in carbon_pars["ethanol"].items()
    }
    common = {
        "unit": "???",
        "relation": "CO2_ind",
        "mode": embodied_carbon_plastics.keys(),
        "technology": "ethanol_to_ethylene_petro",
    }
    co2_emi_rel = make_df("relation_activity", **common).drop(columns="value")
    co2_emi_rel = co2_emi_rel.merge(
        pd.Series(embodied_carbon_plastics, name="value").to_frame().reset_index(),
        left_on="mode",
        right_on="index",
    ).drop(columns="index")

    years = info.Y
    co2_emi_rel = co2_emi_rel.pipe(broadcast, year_act=years)
    co2_emi_rel["year_rel"] = co2_emi_rel["year_act"]

    co2_emi_rel = co2_emi_rel.pipe(broadcast, node_loc=nodes_ex_world(info.N)).pipe(
        same_node
    )
    return {"relation_activity": co2_emi_rel}
