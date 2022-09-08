import logging
import re
from itertools import product

import message_ix
import message_ix_models.util as mutil
import numpy as np
import pandas as pd
from message_ix_models import ScenarioInfo
from message_ix_models.util import nodes_ex_world

# from message_data.projects.ngfs.util import add_macro_COVID  # Unused

log = logging.getLogger(__name__)

#: Commodities for the buildings sector.
BUILD_COMMODITIES = [
    "resid_floor_construction",  # floorspace to be constructed
    "resid_floor_demolition",  # floorspace to be demolished
    "comm_floor_construction",  # floorspace to be constructed
    "comm_floor_demolition",  # floorspace to be demolished
    # TODO Need to harmonize on the commodity names (remove the material name)
]

#: Technologies for the buildings sector.
BUILD_TECHS = [
    # technology providing residential floorspace activity
    "construction_resid_build",
    "demolition_resid_build",
    # technology providing commercial floorspace activity
    "construction_comm_build",
    "demolition_comm_build",
]

#: Commodity names to be converted for use in MESSAGEix-Materials.
BUILD_COMM_CONVERT = [
    "resid_mat_int_scrap_steel",
    "resid_mat_int_scrap_aluminum",
    "resid_mat_int_scrap_cement",
    "resid_mat_int_demand_steel",
    "resid_mat_int_demand_aluminum",
    "resid_mat_int_demand_cement",
    "comm_mat_int_scrap_steel",
    "comm_mat_int_scrap_aluminum",
    "comm_mat_int_scrap_cement",
    "comm_mat_int_demand_steel",
    "comm_mat_int_demand_aluminum",
    "comm_mat_int_demand_cement",
]

#: Types of materials.
MATERIALS = ["steel", "cement", "aluminum"]


def add_bio_backstop(scen):
    """Fill the gap between the biomass demands & potential to avoid infeasibility.

    .. todo:: Replace this with proper & complete use of the current
       :mod:`message_data.tools.utilities.add_globiom`.

       This function simplified from a version in the MESSAGE_Buildings/util/ directory,
       itself modified from an old/outdated (before 2022-03) version of
       :mod:`.add_globiom`.

       See https://iiasa-ece.slack.com/archives/C03M5NX9X0D/p1659623091532079 for
       discussion.
    """
    scen.check_out()

    # Add a new technology
    scen.add_set("technology", "bio_backstop")

    # Retrieve technology for which will be used to create the backstop
    filters = {"technology": "elec_rc", "node_loc": "R12_NAM"}

    for node, par in product(["R12_AFR", "R12_SAS"], ["output", "var_cost"]):
        values = dict(technology="bio_backstop", node_loc=node)

        if par == "output":
            values.update(commodity="biomass", node_dest=node, level="primary")
        elif par == "var_cost":
            values.update(value=1e5)

        data = scen.par(par, filters=filters).assign(**values)
        # print(df)
        scen.add_par(par, data)

    scen.commit("Add biomass dummy")


def get_prices(s: message_ix.Scenario) -> pd.DataFrame:
    """Retrieve PRICE_COMMODITY for certain quantities; excluding _GLB node."""
    result = s.var(
        "PRICE_COMMODITY",
        filters={
            "level": "final",
            "commodity": ["biomass", "coal", "lightoil", "gas", "electr", "d_heat"],
        },
    )
    return result[~result["node"].str.endswith("_GLB")]


# FIXME(PNK) Too complex; McCabe complexity of 17 > 14 for the rest of message_data
def setup_scenario(  # noqa: C901
    scenario: message_ix.Scenario,
    demand: pd.DataFrame,
    prices: pd.DataFrame,
    sturm_scenarios: pd.DataFrame,
    comm_sturm_scenarios: pd.DataFrame,
    first_iteration: bool,
):
    """Set up the structure and data for MESSAGE_Buildings on `scenario`.

    Parameters
    ----------
    scenario
        Scenario to set up.
    info
        Information about `scenario`.
    """
    info = ScenarioInfo(scenario)

    if BUILD_COMMODITIES[0] in info.set["commodity"]:
        # Scenario already set up; do nothing
        return

    scenario.check_out()

    from utils.rc_afofi import return_PERC_AFOFI  # type: ignore

    # Add floorspace unit
    scenario.platform.add_unit("Mm2/y", "mil. square meters by year")

    # Add new commodities and technologies
    # TODO use message_ix_model.build.apply_spec() pattern, like materials & transport
    scenario.add_set("commodity", BUILD_COMMODITIES)
    scenario.add_set("technology", BUILD_TECHS)

    # Find emissions in relation activity
    emiss_rel = list(
        filter(
            lambda rel: "Emission" in rel,
            scenario.par("relation_activity").relation.unique(),
        )
    )

    # Create new demands and techs for AFOFI based on percentages between 2010 and 2015
    dd_replace = scenario.par(
        "demand", filters={"commodity": ["rc_spec", "rc_therm"], "year": info.Y}
    )
    perc_afofi_therm, perc_afofi_spec = return_PERC_AFOFI()
    afofi_dd = dd_replace.copy(True)
    for reg in perc_afofi_therm.index:
        # Boolean mask for rows matching this `reg`
        mask = afofi_dd["node"].str.endswith(reg)

        # NB(PNK) This could probably be simplified using groupby()
        afofi_dd.loc[mask & (afofi_dd.commodity == "rc_therm"), "value"] = (
            afofi_dd.loc[mask & (afofi_dd.commodity == "rc_therm"), "value"]
            * perc_afofi_therm.loc[reg][0]
        )
        afofi_dd.loc[mask & (afofi_dd.commodity == "rc_spec"), "value"] = (
            afofi_dd.loc[mask & (afofi_dd.commodity == "rc_spec"), "value"]
            * perc_afofi_spec.loc[reg][0]
        )

    afofi_dd["commodity"] = afofi_dd.commodity.str.replace("rc", "afofi")
    scenario.add_set("commodity", afofi_dd.commodity.unique())
    scenario.add_par("demand", afofi_dd)

    rc_techs = scenario.par("output", filters={"commodity": ["rc_therm", "rc_spec"]})[
        "technology"
    ].unique()

    for tech_orig in rc_techs:
        # Filters for retrieving data
        filters = dict(filters={"technology": tech_orig})

        # Derived name of new technology
        tech_new = tech_orig.replace("rc", "afofi").replace("RC", "AFOFI")
        scenario.add_set("technology", tech_new)

        # Copy data for input, capacity_factor, and emission_factor
        for name in ("input", "capacity_factor", "emission_factor"):
            scenario.add_par(
                name, scenario.par(name, **filters).assign(technology=tech_new)
            )

        # Replace commodity name in output
        name = "output"
        scenario.add_par(
            name,
            scenario.par(name, **filters).assign(
                technology=tech_new,
                commodity=lambda df: df["commodity"].str.replace("rc", "afofi"),
            ),
        )

        # Only copy relation_activity data for emiss_rel
        name = "relation_activity"
        filters["filters"].update(relation=emiss_rel)
        scenario.add_par(
            name, scenario.par(name, **filters).assign(technology=tech_new)
        )

    # Set model demands for rc_therm and rc_spec to zero
    dd_replace["value"] = 0
    scenario.add_par("demand", dd_replace)

    # Create new input/output for building material intensities
    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
        mode="M1",
        year_vtg=info.Y,
        year_act=info.Y,
    )

    # Iterate over `BUILD_COMM_CONVERT` and nodes (excluding World and *_GLB)
    for c, n in product(BUILD_COMM_CONVERT, nodes_ex_world(info.N)):
        rc, *_, typ, comm = c.split("_")  # First, second-to-last, and last entries

        common.update(node_loc=n, node_origin=n, node_dest=n)

        # Select data for (rc, c, n)
        df_mat = (sturm_scenarios if rc == "resid" else comm_sturm_scenarios).query(
            f"commodity == '{c}' and node == '{n}'"
        )

        if typ == "demand":
            tec = f"construction_{rc}_build"
            # Need to take care of 2110 by appending the last value
            df_demand = mutil.make_io(
                (comm, "demand", "t"),
                (f"{rc}_floor_construction", "demand", "t"),
                efficiency=pd.concat([df_mat.value, df_mat.value.tail(1)]),
                technology=tec,
                **common,
            )
            scenario.add_par("input", df_demand["input"])
            scenario.add_par("output", df_demand["output"])
        elif typ == "scrap":
            tec = f"demolition_{rc}_build"
            # Need to take care of 2110 by appending the last value
            df_scrap = mutil.make_io(
                (comm, "end_of_life", "t"),  # will be flipped to output
                (f"{rc}_floor_demolition", "demand", "t"),
                efficiency=pd.concat([df_mat.value, df_mat.value.tail(1)]),
                technology=tec,
                **common,
            )
            # Flip input to output (no input for demolition)
            df_temp = df_scrap["input"].rename(
                columns={"node_origin": "node_dest", "time_origin": "time_dest"}
            )
            scenario.add_par("output", df_temp)
            scenario.add_par("output", df_scrap["output"])

    # Subtract building material demand from existing demands in scenario
    for rc in ["resid", "comm"]:
        # Don't do this for commercial demands in the first iteration
        if rc == "comm" and first_iteration:
            continue

        df_out = (
            sturm_scenarios.copy(True)
            if rc == "resid"
            else comm_sturm_scenarios.copy(True)
        )
        df = df_out[
            df_out.commodity.str.fullmatch(f"{rc}_mat_demand_(cement|steel|aluminum)")
        ]  # .copy(True)
        df["commodity"] = df.apply(lambda x: x.commodity.split("_")[-1], axis=1)
        df = df.rename(columns={"value": f"demand_{rc}_const"}).drop(
            columns=["level", "time", "unit"]
        )
        # df = df.stack()
        mat_demand = (
            scenario.par("demand", {"level": "demand"})
            .join(
                df.set_index(["node", "year", "commodity"]),
                on=["node", "year", "commodity"],
                how="left",
            )
            .dropna()
        )
        mat_demand["value"] = np.maximum(
            mat_demand["value"] - mat_demand[f"demand_{rc}_const"], 0
        )
        scenario.add_par("demand", mat_demand.drop(columns=f"demand_{rc}_const"))

    # Create new technologies for building energy

    # Mapping from commodity to base model's *_rc technology
    rc_tech_fuel = {"lightoil": "loil_rc", "electr": "elec_rc", "d_heat": "heat_rc"}

    # Add for fuels above
    for fuel in prices["commodity"].unique():
        # Find the original rc technology for the fuel
        tech_orig = rc_tech_fuel.get(fuel, f"{fuel}_rc")

        # Remove lower bound in activity for older, now unused rc techs to allow them to
        # reach zero
        filters = dict(filters={"technology": tech_orig, "year_act": info.Y})
        for name, value in (
            ("bound_activity_lo", 0.0),
            ("growth_activity_lo", -1.0),
            ("soft_activity_lo", 0.0),
        ):
            scenario.add_par(name, scenario.par(name, **filters).assign(value=value))

        # Create the technologies for the new commodities
        for commodity in filter(
            re.compile(f"[_-]{fuel}").search, demand["commodity"].unique()
        ):

            # Fix for lightoil gas included
            if "lightoil-gas" in commodity:
                tech_new = f"{fuel}_lg_" + commodity.replace("_lightoil-gas", "")
            else:
                tech_new = f"{fuel}_" + commodity.replace(f"_{fuel}", "")

            # commented: for debugging
            # print(f"{fuel = }", f"{commodity = }", f"{tech_new = }", sep="\n")

            # Add new commodities and technologies
            scenario.add_set("commodity", commodity)
            scenario.add_set("technology", tech_new)

            # Modify data
            for name, filters, extra in (
                ("input", {}, dict(value=1.0)),
                ("output", {}, dict(commodity=commodity, value=1.0)),
                ("capacity_factor", {}, {}),
                ("emission_factor", {}, {}),
                ("relation_activity", dict(relation=emiss_rel), {}),
            ):
                filters["technology"] = tech_orig
                scenario.add_par(
                    name,
                    scenario.par(name, filters=filters).assign(
                        technology=tech_new, **extra
                    ),
                )

    scenario.commit("message_data.model.buildings.setup_scenario()")
    scenario.set_as_default()
