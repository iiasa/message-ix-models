import logging
from collections.abc import Mapping
from functools import lru_cache, partial

import pandas as pd
from sdmx.model.v21 import Code

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model import build
from message_ix_models.model.structure import get_codes
from message_ix_models.util import broadcast, package_data_path

from .utils import read_config

log = logging.getLogger(__name__)


def cat_tec_cooling(context: Context) -> tuple[pd.DataFrame, list[str]]:
    """
    Categorize cooling technologies based on predefined types and match them with
    parent technologies present in the scenario.

    This function extracts cooling technology data from a CSV file, filters them
    based on parent technologies available in the scenario, and categorizes each
    cooling technology into a predefined type. It also retrieves a list of unique
    region nodes from the scenario parameter data.

    Parameters
    ----------
    context : Context
        Provides access to the current scenario and configuration.

    Returns
    -------
    tuple[pd.DataFrame, list[str]]
        - cat_tec: A DataFrame with columns:
            - 'type_tec': Cooling technology category.
            - 'tec': Name of the cooling technology.
        - regions_df: A list of unique region nodes from the scenario.
    """
    # Define cooling type categories and their corresponding strings
    cooling_types = {
        "share_cooling_ot_fresh_tot": ["ot_fresh", "cl_fresh", "air", "ot_saline"],
        "share_cooling_cl_fresh_tot": ["ot_fresh", "cl_fresh", "air", "ot_saline"],
        "share_cooling_air_tot": ["ot_fresh", "cl_fresh", "air", "ot_saline"],
        "share_cooling_ot_saline_tot": ["ot_fresh", "cl_fresh", "air", "ot_saline"],
        "share_cooling_ot_fresh_share": ["ot_fresh"],
        "share_cooling_cl_fresh_share": ["cl_fresh"],
        "share_cooling_air_share": ["air"],
        "share_cooling_ot_saline_share": ["ot_saline"],
    }

    FILE = "tech_water_performance_ssp_msg.csv"
    path = package_data_path("water", "ppl_cooling_tech", FILE)
    df = pd.read_csv(path)
    cooling_df = df.loc[df["technology_group"] == "cooling"].copy(deep=True)
    # Separate a column for parent technologies of respective cooling
    # techs
    cooling_df["parent_tech"] = (
        cooling_df["technology_name"]
        .apply(lambda x: pd.Series(str(x).split("__")))
        .drop(columns=1)
    )
    # Extract unique technologies
    sc = context.get_scenario()
    #  get df = sc.par("input") for technollgies in cooling_df(parent_tach)
    df = sc.par("input", filters={"technology": cooling_df["parent_tech"].unique()})
    missing_tec = cooling_df["parent_tech"][
        ~cooling_df["parent_tech"].isin(df["technology"])
    ]
    # some techs only have output, like csp
    ref_output = sc.par("output", {"technology": missing_tec})
    ref_output.columns = df.columns
    # merge ref_input and ref_output
    df = pd.concat([df, ref_output])
    parent_tech_sc = df["technology"].unique()
    regions_df = df["node_loc"].unique().tolist()

    # Assertion check for valid data
    assert (
        len(parent_tech_sc) > 0
    ), "No matching parent technologies found in the scenario."
    assert len(regions_df) > 0, "No unique nodes (regions) found in the scenario."

    # not filter cooling_tec with only parent_tech matching parent_tech_sc
    cooling_df = cooling_df.loc[cooling_df["parent_tech"].isin(parent_tech_sc)].copy()
    unique_technologies = cooling_df["technology_name"].unique()

    # Create a list to store rows for the cat_tec DataFrame
    cat_tec_rows = []

    # Iterate through unique technologies
    for tech in unique_technologies:
        for type_tec, keywords in cooling_types.items():
            for keyword in keywords:
                if keyword in tech:
                    # Add a row to the cat_tec list with type_tec and technology
                    cat_tec_rows.append({"type_tec": type_tec, "tec": tech})

    # Create the cat_tec DataFrame
    cat_tec = pd.DataFrame(cat_tec_rows)

    return cat_tec, regions_df


def get_spec(context: Context) -> Mapping[str, ScenarioInfo]:
    """Return the specification for nexus implementation

    Parameters
    ----------
    context : .Context
        The key ``regions`` determines the regional aggregation used.
    """

    context = read_config()

    require = ScenarioInfo()
    remove = ScenarioInfo()
    add = ScenarioInfo()

    # cooling data included by default
    # Merge technology.yaml with set.yaml
    context["water set"]["cooling"]["technology"]["add"] = context["water technology"][
        "cooling"
    ]
    # Update the ScenarioInfo objects with required and new set elements
    for set_name, config in context["water set"]["cooling"].items():
        # Required elements
        require.set[set_name].extend(config.get("require", []))

        # Elements to remove
        remove.set[set_name].extend(config.get("remove", []))

        # Elements to add
        add.set[set_name].extend(config.get("add", []))

    if context.nexus_set == "nexus":
        # Merge technology.yaml with set.yaml
        context["water set"]["nexus"]["technology"]["add"] = context[
            "water technology"
        ]["nexus"]
        # Update the ScenarioInfo objects with required and new set elements
        for set_name, config in context["water set"]["nexus"].items():
            # Required elements
            require.set[set_name].extend(config.get("require", []))

            # Elements to remove
            remove.set[set_name].extend(config.get("remove", []))

            # Elements to add
            add.set[set_name].extend(config.get("add", []))

        # The set of required nodes varies according to context.regions
        n_codes = get_codes(f"node/{context.regions}")
        nodes = list(map(str, n_codes[n_codes.index(Code(id="World"))].child))
        require.set["node"].extend(nodes)

        # Share commodity for groundwater
        results = {}
        df_node = context.all_nodes
        n = len(df_node.values)

        d = {
            "shares": ["share_low_lim_GWat"] * n,
            "node_share": df_node,
            "node": df_node,
            "type_tec": ["share_low_lim_GWat_share"] * n,
            "mode": ["M1"] * n,
            "commodity": ["groundwater_basin"] * n,
            "level": ["water_avail_basin"] * n,
        }

        df_share = pd.DataFrame(data=d)
        df_list = df_share.values.tolist()
        results["map_shares_commodity_share"] = df_list

        d = {
            "shares": ["share_low_lim_GWat"] * n,
            "node_share": df_node,
            "node": df_node,
            "type_tec": ["share_low_lim_GWat_total"] * n,
            "mode": ["M1"] * n,
            "commodity": ["surfacewater_basin"] * n,
            "level": ["water_avail_basin"] * n,
        }

        df_share = pd.DataFrame(data=d)

        d2 = {
            "shares": ["share_low_lim_GWat"] * n,
            "node_share": df_node,
            "node": df_node,
            "type_tec": ["share_low_lim_GWat_total"] * n,
            "mode": ["M1"] * n,
            "commodity": ["groundwater_basin"] * n,
            "level": ["water_avail_basin"] * n,
        }

        df_share2 = pd.DataFrame(data=d2)

        df_share = pd.concat([df_share, df_share2])
        df_list = df_share.values.tolist()

        results["map_shares_commodity_total"] = df_list

        for set_name, config in results.items():
            # Sets  to add
            add.set[set_name].extend(config)

        results = {}

        # Share commodity for urban water recycling
        d = {
            "shares": ["share_wat_recycle"] * n,
            "node_share": df_node,
            "node": df_node,
            "type_tec": ["share_wat_recycle_share"] * n,
            "mode": ["M1"] * n,
            "commodity": ["urban_collected_wst"] * n,
            "level": ["water_treat"] * n,
        }

        df_share = pd.DataFrame(data=d)
        df_list = df_share.values.tolist()
        results["map_shares_commodity_share"] = df_list

        d = {
            "shares": ["share_wat_recycle"] * n,
            "node_share": df_node,
            "node": df_node,
            "type_tec": ["share_wat_recycle_total"] * n,
            "mode": ["M1"] * n,
            "commodity": ["urban_collected_wst"] * n,
            "level": ["water_treat"] * n,
        }

        df_share = pd.DataFrame(data=d)

        d2 = {
            "shares": ["share_wat_recycle"] * n,
            "node_share": df_node,
            "node": df_node,
            "type_tec": ["share_wat_recycle_total"] * n,
            "mode": ["M1"] * n,
            "commodity": ["urban_collected_wst"] * n,
            "level": ["water_treat"] * n,
        }

        df_share2 = pd.DataFrame(data=d2)

        df_share = pd.concat([df_share, df_share2])
        df_list = df_share.values.tolist()

        results["map_shares_commodity_total"] = df_list

        for set_name, config in results.items():
            # Sets  to add
            add.set[set_name].extend(config)

    # for both cooling and nexus add share contraints for cooling technologies
    # cat_tec

    results = {}
    cat_tec, nodes_cooling = cat_tec_cooling(context)
    results["cat_tec"] = cat_tec.values.tolist()
    n = len(nodes_cooling)
    # Share commodity for urban water recycling
    shares_cool = [
        "share_cooling_ot_fresh",
        "share_cooling_cl_fresh",
        "share_cooling_air",
        "share_cooling_ot_saline",
    ]
    commodity_cool = ["ot_fresh", "cl_fresh", "air", "ot_saline"]

    type_tec_share = [
        "share_cooling_ot_fresh_share",
        "share_cooling_cl_fresh_share",
        "share_cooling_air_share",
        "share_cooling_ot_saline_share",
    ]
    df_share = pd.DataFrame(
        {
            "shares": shares_cool,
            "node_share": [None] * len(shares_cool),  # Placeholder for node_share
            "node": [None] * len(shares_cool),  # Placeholder for node
            "type_tec": type_tec_share,
            "mode": "M1",  # Repeat mode
            "commodity": commodity_cool,
            "level": "share",  # Repeat level
        }
    ).pipe(broadcast, node_share=nodes_cooling)

    df_share["node"] = df_share["node_share"]
    # re order columns like this:
    # ['shares', 'node_share', 'node', 'type_tec', 'mode', 'commodity', 'level']
    df_share = df_share[
        ["shares", "node_share", "node", "type_tec", "mode", "commodity", "level"]
    ]

    df_list = df_share.values.tolist()
    results["map_shares_commodity_share"] = df_list
    # for total
    type_tec_tot = [
        "share_cooling_ot_fresh_tot",
        "share_cooling_cl_fresh_tot",
        "share_cooling_air_tot",
        "share_cooling_ot_saline_tot",
    ]
    df_share = pd.DataFrame(
        {
            "shares": shares_cool,
            "node_share": [None] * len(shares_cool),  # Placeholder for node_share
            "node": [None] * len(shares_cool),  # Placeholder for node
            "type_tec": type_tec_tot,
            "mode": "M1",  # Repeat mode
            "commodity": [None] * len(shares_cool),
            "level": "share",  # Repeat level
        }
    ).pipe(broadcast, node_share=nodes_cooling, commodity=commodity_cool)

    df_share["node"] = df_share["node_share"]
    # re order columns like this:
    # ['shares', 'node_share', 'node', 'type_tec', 'mode', 'commodity', 'level']
    df_share = df_share[
        ["shares", "node_share", "node", "type_tec", "mode", "commodity", "level"]
    ]

    df_list = df_share.values.tolist()

    results["map_shares_commodity_total"] = df_list

    for set_name, config in results.items():
        # Sets to add
        add.set[set_name].extend(config)

    return dict(require=require, remove=remove, add=add)


@lru_cache()
def generate_set_elements(set_name, match=None):
    codes = read_config()["water set"][set_name].get("add", [])

    hierarchical = set_name in {"technology"}

    results = []
    for code in codes:
        if match and code.id != match:
            continue
        elif hierarchical:
            results.extend(code)

    return results


def map_basin(context: Context) -> Mapping[str, ScenarioInfo]:
    """Return specification for mapping basins to regions

    The basins are spatially consolidated from HydroSHEDS basins delineation
    database.This delineation is then intersected with MESSAGE regions to form new
    water sector regions for the nexus module.
    The nomenclature for basin names is <basin_id>|<MESSAGEregion> such as R1|AFR
    """
    context = read_config()

    add = ScenarioInfo()

    require = ScenarioInfo()

    remove = ScenarioInfo()

    # define an empty dictionary
    results = {}
    # read csv file for basin names and region mapping
    # reading basin_delineation
    FILE = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE)

    df = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df["node"] = "B" + df["BCU_name"].astype(str)
    df["mode"] = "M" + df["BCU_name"].astype(str)
    df["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df["REGION"].astype(str)
    )

    results["node"] = df["node"]
    results["mode"] = df["mode"]
    # map nodes as per dimensions
    df1 = pd.DataFrame({"node_parent": df["region"], "node": df["node"]})
    df2 = pd.DataFrame({"node_parent": df["node"], "node": df["node"]})
    frame = [df1, df2]
    df_node = pd.concat(frame)
    nodes = df_node.values.tolist()

    results["map_node"] = nodes

    context.all_nodes = df["node"]

    for set_name, config in results.items():
        # Sets to add
        add.set[set_name].extend(config)

    return dict(require=require, remove=remove, add=add)


def main(context: Context, scenario, **options):
    """Set up MESSAGEix-Nexus on `scenario`.

    See also
    --------
    add_data
    apply_spec
    get_spec
    """
    from .data import add_data

    log.info("Set up MESSAGEix-Nexus")

    if context.nexus_set == "nexus":
        # Add water balance
        spec = map_basin(context)

        # Apply the structural changes AND add the data
        build.apply_spec(scenario, spec, **options)

    # Core water structure
    spec1 = get_spec(context)

    # Apply the structural changes AND add the data
    build.apply_spec(scenario, spec1, partial(add_data, context=context), **options)

    # Uncomment to dump for debugging
    # scenario.to_excel('debug.xlsx')
