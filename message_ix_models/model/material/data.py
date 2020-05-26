import numpy as np
import pandas as pd

from message_data.tools import ScenarioInfo, broadcast, make_df, same_node

from .util import read_config


def gen_data(scenario, dry_run=False):
    """Generate data for materials representation of nitrogen fertilizers."""
    # Load configuration
    config = read_config()["material"]["set"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # List of dataframes, concatenated together at end
    output = []

    # NH3 production processes
    common = dict(
        year_vtg=s_info.Y,
        commodity="NH3",
        level="material_interim",
        # TODO fill in remaining dimensions
        mode="all",
    )

    # Iterate over new technologies, using the configuration
    for t in config["technology"]["add"]:
        # TODO describe what this is. In SetupNitrogenBase.py, the values are
        #      read for technology == "solar_i" but are not altered before
        #      being re-added to the scenario.
        df = (
            make_df("output", technology=t, **common)
            .pipe(broadcast, node_loc=s_info.N)
            .pipe(same_node)
        )
        output.append(df)

        # Heat output
        output.append(
            df.assign(
                commodity="d_heat",
                level="secondary",
                value=np.nan,  # LoadParams.output_heat[t]
            )
        )

    result = dict(output=pd.concat(output))

    # Temporary: return nothing, since the data frames are incomplete
    return dict()
    # return result
