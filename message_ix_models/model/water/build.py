import logging
from functools import lru_cache, partial
from typing import Mapping

import pandas as pd
from sdmx.model.v21 import Code

from message_ix_models import ScenarioInfo
from message_ix_models.model import build
from message_ix_models.model.structure import get_codes
from message_ix_models.util import private_data_path

from .utils import read_config

log = logging.getLogger(__name__)


def get_spec(context) -> Mapping[str, ScenarioInfo]:
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

        df_share = df_share.append(df_share2)
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

        df_share = df_share.append(df_share2)
        df_list = df_share.values.tolist()

        results["map_shares_commodity_total"] = df_list

        for set_name, config in results.items():
            # Sets  to add
            add.set[set_name].extend(config)

    elif context.nexus_set == "cooling":
        # Merge technology.yaml with set.yaml
        context["water set"]["cooling"]["technology"]["add"] = context[
            "water technology"
        ]["cooling"]
        # Update the ScenarioInfo objects with required and new set elements
        for set_name, config in context["water set"]["cooling"].items():
            # Required elements
            require.set[set_name].extend(config.get("require", []))

            # Elements to remove
            remove.set[set_name].extend(config.get("remove", []))

            # Elements to add
            add.set[set_name].extend(config.get("add", []))

    return dict(require=require, remove=remove, add=add)

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


def map_basin(context) -> Mapping[str, ScenarioInfo]:
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
    PATH = private_data_path("water", "delineation", FILE)

    df = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df["node"] = "B" + df["BCU_name"].astype(str)
    df["mode"] = "M" + df["BCU_name"].astype(str)
    if context.type_reg == "country":
        df["region"] = context.map_ISO_c[context.regions]
    else:
        df["region"] = f"{context.regions}_" + df["REGION"].astype(str)

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
        # Sets  to add
        add.set[set_name].extend(config)

    return dict(require=require, remove=remove, add=add)


def main(context, scenario, **options):
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
