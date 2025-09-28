from collections.abc import Mapping
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

import message_ix
import numpy as np
import pandas as pd
from genno import Computer
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import (
    read_yaml_file,
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
    from message_ix_models.types import ParameterData


def add_macro_materials(
    scen: message_ix.Scenario, filename: str, check_converge: bool = False
) -> message_ix.Scenario:
    """Prepare data for MACRO calibration by reading data from xlsx file.

    Parameters
    ----------
    scen
        Scenario to be calibrated
    filename
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
    """Reads mapping file and returns relevant columns needed for technology mapping.

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


def add_cement_ccs_co2_tr_relation(scen: message_ix.Scenario) -> None:
    """Adds CCS technologies to the `co2_trans_disp` and `bco2_trans_disp` relations.

    Parameters
    ----------
    scen
        Scenario instance to add CCS emission factor parametrization to
    """

    # The relation coefficients for CO2_Emision and bco2_trans_disp and
    # co2_trans_disp are both MtC. The emission factor for CCS add_ccs_technologies
    # are specified in MtC as well.
    co2_trans_relation = scen.par(
        "emission_factor",
        filters={
            "technology": [
                "clinker_dry_ccs_cement",
                "clinker_wet_ccs_cement",
            ],
            "emission": "CO2",
        },
    )

    co2_trans_relation.drop(["year_vtg", "emission", "unit"], axis=1, inplace=True)
    co2_trans_relation["relation"] = "co2_trans_disp"
    co2_trans_relation["node_rel"] = co2_trans_relation["node_loc"]
    co2_trans_relation["year_rel"] = co2_trans_relation["year_act"]
    co2_trans_relation["unit"] = "???"

    with scen.transact("New CCS technologies added to the CO2 accounting relations."):
        scen.add_par("relation_activity", co2_trans_relation)


def gen_emi_rel_data(
    s_info: ScenarioInfo,
    material: Literal["aluminum", "steel", "cement", "petrochemicals"],
) -> "ParameterData":
    df = pd.read_csv(
        package_data_path("material", material, "emission_factors.csv"), comment="#"
    )
    df = (
        make_df(
            "relation_activity",
            **df,
        )
        .pipe(broadcast, year_act=s_info.Y, node_loc=nodes_ex_world(s_info.N))
        .assign(year_rel=lambda x: x["year_act"], unit="Mt C/yr")
        .pipe(same_node)
    )
    df = df[df["value"] != 0]
    return {"relation_activity": df}


def read_sector_data(
    scenario: message_ix.Scenario, sectname: str, ssp: str | None, filename: str
) -> pd.DataFrame:
    """Read sector data for industry with `sectname`.

    Parameters
    ----------
    scenario
        Scenario used to get structural information like model regions and years.
    sectname
        Name of industry sector.
    ssp
        If sector data should be read from an SSP-specific file.
    filename
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


def read_timeseries(
    scenario: message_ix.Scenario, material: str, ssp: str | None, filename: str
) -> pd.DataFrame:
    """Read ‘time-series’ data from a material-specific `filename`.

    Read "timeseries" type data from a sector-specific input file to data frame and
    format as MESSAGE parameter data.

    Parameters
    ----------
    scenario
        Scenario used to get structural information like model regions and years.
    material
        Name of material folder (**‘sector’**) where `filename` is located.
    ssp
        If timeseries is available for different SSPs, the respective file is selected.
    filename
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
        def convert_if_digit(col_name: str | int) -> str | int:
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
    scenario: message_ix.Scenario, material: str, ssp: str | None, filename: str
) -> pd.DataFrame:
    """
    Read ``relation_*`` type parameter data for specific industry

    Parameters
    ----------
    ssp
        if relations are available for different SSPs, the respective file is selected
    scenario
        scenario used to get structural information like
    material
        name of material folder where xlsx is located
    filename
        name of xlsx file

    Returns
    -------
    pd.DataFrame
        DataFrame containing ``relation_*`` parameter data
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
    """Generate cost parameter data for scenario technology set.

    Calls :mod:`message_ix_models.tools.costs` with config for MESSAGEix-Materials
    and return ``inv_cost`` and ``fix_cost`` projections for energy and industry
    technologies.

    Parameters
    ----------
    scen
        Scenario instance is required to get technology set.
    ssp
        SSP to use for projection assumptions.
    method
        method to use for cost convergence over time.
    ref_reg
        reference region to use for regional cost differentiation.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        tuple with ``inv_cost`` and ``fix_cost`` data
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
    """Generate GDP and POP data of SSP 3.0 in ``bound_activity_*`` format.

    Parameters
    ----------
    context
        context used to prepare genno.Computer
    model
        model name of projections to read
    measure
        Indicator to read (GDP or Population)
    tec
        name to use for "technology" column

    Returns
    -------
    pd.DataFrame
        DataFrame with SSP indicator data in ``bound_activity_*`` parameter format.
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


def calculate_ini_new_cap(
    df_demand: pd.DataFrame, technology: str, material: str, ssp: str
) -> pd.DataFrame:
    """Derive ``initial_new_capacity_up`` data for CCS from projected cement demand.

    Parameters
    ----------
    df_demand
        DataFrame containing "demand" MESSAGEix parametrization.
    technology
        Name of CCS technology to be parametrized.
    material
        Name of the material/industry sector.

    Returns
    -------
    pd.DataFrame
        ``initial_new_capacity_up`` parameter data.
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


def add_water_par_data(scenario: "Scenario") -> None:
    """Adds water supply technologies that are required for the Materials build.

    Parameters
    ----------
    scenario
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


def gen_plastics_emission_factors(
    info, species: Literal["methanol", "HVCs", "ethanol"]
) -> "ParameterData":
    """Generate "CO2_Emission" relation parameter to account stored carbon in plastics.

    The calculation considers:

    * carbon content of feedstocks,
    * the share that is converted to plastics
    * the end-of-life treatment (i.e. incineration, landfill, etc.)

    *NOTE:
    Values are negative since they need to be deducted from top-down accounting, which
    assumes that all extracted carbonaceous resources are released as carbon emissions.
    (Which would not be correct for carbon used in long-lived products)*

    Parameters
    ----------
    species
        feedstock species to generate relation for
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
) -> "ParameterData":
    """Generate `CO2_ind` ``relation_activity`` values for chemical production.

    The values represent the carbon in chemical feedstocks which is oxidized in the
    short-term (i.e. during the model horizon) in downstream products. Oxidation either
    through natural oxidation or combustion as the end-of-life treatment of plastics.

    The calculation considers:

    * carbon content of feedstocks,
    * the share that is converted to oxidizable chemicals
    * the end-of-life treatment shares (i.e. incineration, landfill, etc.)

    *NOTE: Values are positive since they need to be added to bottom-up emission
    accounting.*

    Parameters
    ----------
    species:
        feedstock species to generate relation for
    """

    tec_species_map = {
        "methanol": "meth_ind_fs",
        "HVCs": "production_HVC",
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


def gen_ethanol_to_ethylene_emi_factor(info: ScenarioInfo) -> "ParameterData":
    """Generate `CO2_ind` ``relation_activity`` values for `ethanol_to_ethylene_petro`.

    The values represent the carbon in chemical feedstocks which is oxidized in the
    short-term (i.e. during the model horizon) in downstream products. Oxidation either
    through natural oxidation or combustion as the end-of-life treatment of plastics.

     The calculation considers:

     * carbon content of feedstocks,
     * the share that is converted to oxidizable chemicals
     * the end-of-life treatment shares (i.e. incineration, landfill, etc.)

     *NOTE: Values are positive since they are added to bottom-up CO2 accounting.*
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
