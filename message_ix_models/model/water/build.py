import logging
from functools import lru_cache, partial
from typing import Mapping

import pandas as pd
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

    # Update the ScenarioInfo objects with required and new set elements
    for set_name, config in context["water set"].items():
        # Required elements
        require.set[set_name].extend(config.get("require", []))

        # Elements to remove
        remove.set[set_name].extend(config.get("remove", []))

        # Elements to add
        add.set[set_name].extend(config.get("add", []))

    # The set of required nodes varies according to context.regions
    nodes = get_codes(f"node/{context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    require.set["node"].extend(nodes)

    # require.set["node"].extend(nn)
    # create a mapping ISO code : region name, for other scripts
    # only needed for 1-country models
    if context.type_reg == "country":
        map_ISO_c = {context.regions: nodes[0]}
        context.map_ISO_c = map_ISO_c
        log.info(f"mapping {context.map_ISO_c[context.regions]}")
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
    """Info to be added
    """
    context = read_config()

    add = ScenarioInfo()

    require = ScenarioInfo()

    remove = ScenarioInfo()

    # define an empty dictionary
    results = {}
    # read csv file for basin names and region mapping
    path = private_data_path("water", "delineation", "basins_by_region_simpl_R11.csv")
    df = pd.read_csv(path)
    # Assigning proper nomenclature
    df["node"] = "B" + df["BCU_name"].astype(str)
    df["mode"] = "M" + df["BCU_name"].astype(str)
    df["region"] = "R11_" + df["REGION"].astype(str)
    results["node"] = df["node"]
    results["mode"] = df["mode"]
    # map nodes as per dimensions
    df1 = pd.DataFrame({"node_parent": df["region"], "node": df["node"]})
    df2 = pd.DataFrame({"node_parent": df["node"], "node": df["node"]})
    frame = [df1, df2]
    df_node = pd.concat(frame)
    nodes = df_node.values.tolist()

    results["map_node"] = nodes

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

    if context.nexus_set == 'nexus':
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
