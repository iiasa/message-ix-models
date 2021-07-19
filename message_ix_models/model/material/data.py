from collections import defaultdict
import logging

import pandas as pd
from message_ix import make_df
from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast, same_node

from .util import read_config


log = logging.getLogger(__name__)


def gen_data(scenario, dry_run=False):
    """Generate data for materials representation of nitrogen fertilizers.

    .. note:: This code is only partially translated from
       :file:`SetupNitrogenBase.py`.
    """
    # Load configuration
    config = read_config()["material"]["set"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data = read_data()

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # NH3 production processes
    common = dict(
        year_vtg=s_info.Y,
        commodity="NH3",
        level="material_interim",
        # TODO fill in remaining dimensions
        mode="all",
        time="year",
        time_dest="year",
        time_origin="year",
    )

    # Iterate over new technologies, using the configuration
    for t in config["technology"]["add"]:
        # Output of NH3: same efficiency for all technologies
        # TODO the output commodity and level are different for
        #      t=NH3_to_N_fertil; use 'if' statements to fill in.
        df = (
            make_df("output", technology=t, value=1, unit='t', **common)
            .pipe(broadcast, node_loc=s_info.N)
            .pipe(same_node)
        )
        results["output"].append(df)

        # Heat output

        # Retrieve the scalar value from the data
        row = data.loc[("output_heat", t.id), :]
        log.info(f"Use {repr(row.Variable)} for heat output of {t}")

        # Store a modified data frame
        results["output"].append(
            df.assign(commodity="d_heat", level="secondary", value=row[2010])
        )

    # TODO add other variables from SetupNitrogenBase.py

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    # Temporary: return nothing, since the data frames are incomplete
    return dict()
    # return result


def read_data():
    """Read and clean data from :file:`n-fertilizer_techno-economic.xlsx`."""
    # Ensure config is loaded, get the context
    context = read_config()

    # Shorter access to sets configuration
    sets = context["material"]["set"]

    # Read the file
    data = pd.read_excel(
        context.get_path("material", "n-fertilizer_techno-economic.xlsx"),
        sheet_name="Sheet1",
        engine="openpyxl",
    )

    # Prepare contents for the "parameter" and "technology" columns
    # FIXME put these in the file itself to avoid ambiguity/error

    # "Variable" column contains different values selected to match each of
    # these parameters, per technology
    params = [
        "inv_cost",
        "fix_cost",
        "var_cost",
        "technical_lifetime",
        "input_fuel",
        "input_elec",
        "input_water",
        "output_NH3",
        "output_water",
        "output_heat",
        "emissions",
        "capacity_factor",
    ]

    param_values = []
    tech_values = []
    for t in sets["technology"]["add"]:
        param_values.extend(params)
        tech_values.extend([t.id] * len(params))

    # Clean the data
    data = (
        # Insert "technology" and "parameter" columns
        data.assign(technology=tech_values, parameter=param_values)
        # Drop columns that don't contain useful information
        .drop(["Model", "Scenario", "Region"], axis=1)
        # Set the data frame index for selection
        .set_index(["parameter", "technology"])
    )

    # TODO convert units for some parameters, per LoadParams.py

    return data
