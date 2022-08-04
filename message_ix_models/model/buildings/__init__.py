from itertools import product

import message_ix
import message_ix_models.util as mutil
import numpy as np
import pandas as pd
from message_ix_models import ScenarioInfo

# from message_data.projects.ngfs.util import add_macro_COVID  # Unused

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
    info: ScenarioInfo,
    demand: pd.DataFrame,
    prices: pd.DataFrame,
    sturm_scenarios: pd.DataFrame,
    comm_sturm_scenarios: pd.DataFrame,
    first_iteration: bool,
):
    """Set up the structure and data for MESSAGE_Buildings on `scenario`."""
    if BUILD_COMMODITIES[0] in info.set["commodity"]:
        # Scenario already set up; do notihing
        return

    from utils import rc_afofi  # type: ignore

    nodes = info.N
    years_model = info.Y

    # Add floorspace unit
    scenario.platform.add_unit("Mm2/y", "mil. square meters by year")

    # Add new commodities and technologies
    scenario.add_set("commodity", BUILD_COMMODITIES)
    scenario.add_set("technology", BUILD_TECHS)

    # Find emissions in relation activity
    emiss_rel = list(
        filter(
            lambda rel: "Emission" in rel,
            scenario.par("relation_activity").relation.unique(),
        )
    )

    # Create new demands and techs for AFOFI
    # based on percentages between 2010 and 2015
    # (see rc_afofi.py in utils)
    dd_replace = scenario.par(
        "demand",
        filters={"commodity": ["rc_spec", "rc_therm"], "year": years_model},
    )
    [perc_afofi_therm, perc_afofi_spec] = rc_afofi.return_PERC_AFOFI()
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
        tech_new = tech_orig.replace("rc", "afofi")

        if "RC" in tech_orig:
            tech_new = tech_orig.replace("RC", "AFOFI")

        filters = dict(filters={"technology": tech_orig})
        for name in ("input", "capacity_factor", "emission_factor"):
            scenario.add_par(
                name, scenario.par(name, **filters).assign(technology=tech_new)
            )

        afofi_out = scenario.par("output", **filters).assign(
            technology=tech_new,
            commodity=lambda df: df["commodity"].str.replace("rc", "afofi"),
        )

        afofi_rel = scenario.par(
            "relation_activity",
            filters={"technology": tech_orig, "relation": emiss_rel},
        ).assign(technology=tech_new)

        scenario.add_set("technology", tech_new)
        scenario.add_par("output", afofi_out)
        scenario.add_par("relation_activity", afofi_rel)

    # Set model demands for rc_therm and rc_spec to zero
    dd_replace["value"] = 0
    scenario.add_par("demand", dd_replace)

    # Create new input/output for building material intensities
    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
        mode="M1",
        year_vtg=years_model,
        year_act=years_model,
    )

    # Iterate over `BUILD_COMM_CONVERT` and  nodes (excluding World and *_GLB)
    for c, n in product(
        BUILD_COMM_CONVERT, filter(lambda n: "World" not in n and "GLB" not in n, nodes)
    ):
        comm = c.split("_")[-1]
        typ = c.split("_")[-2]
        rc = c.split("_")[-5]  # "resid" or "comm"

        common.update(node_loc=n, node_origin=n, node_dest=n)

        if rc == "resid":
            df_mat = sturm_scenarios.loc[
                (sturm_scenarios["commodity"] == c) & (sturm_scenarios["node"] == n)
            ]
        elif rc == "comm" and first_iteration:
            df_mat = comm_sturm_scenarios.loc[
                (comm_sturm_scenarios["commodity"] == c)
                & (comm_sturm_scenarios["node"] == n)
            ]

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
    rc_tech_fuel = pd.DataFrame(
        {
            "fuel": ["biomass", "coal", "lightoil", "gas", "electr", "d_heat"],
            "technology": [
                "biomass_rc",
                "coal_rc",
                "loil_rc",
                "gas_rc",
                "elec_rc",
                "heat_rc",
            ],
        }
    )

    # Add for fuels above
    for fuel in prices["commodity"].unique():
        # Find the original rc technology for the fuel
        tech_orig = rc_tech_fuel.loc[rc_tech_fuel["fuel"] == fuel, "technology"].values[
            0
        ]

        # Remove lower bound in activity for older, now unused
        # rc techs to allow them to reach zero
        filters = dict(filters={"technology": tech_orig, "year_act": years_model})
        for constraint, value in (
            ("bound_activity", 0.0),
            ("growth_activity", -1.0),
            ("soft_activity", 0.0),
        ):
            name = f"{constraint}_lo"
            scenario.add_par(name, scenario.par(name, **filters).assign(value=value))

        # Create the technologies for the new commodities
        for commodity in filter(
            lambda com: f"_{fuel}" in com or f"-{fuel}" in com,
            demand["commodity"].unique(),
        ):

            # Fix for lightoil gas included
            if "lightoil-gas" in commodity:
                tech_new = fuel + "_lg_" + commodity.replace("_lightoil-gas", "")
            else:
                tech_new = fuel + "_" + commodity.replace("_" + fuel, "")

            filters = dict(filters={"technology": tech_orig})
            build_in = scenario.par("input", **filters).assign(
                technology=tech_new, value=1.0
            )

            build_out = scenario.par("output", **filters).assign(
                technology=tech_new, commodity=commodity, value=1.0
            )

            build_cf = scenario.par("capacity_factor", **filters).assign(
                technology=tech_new
            )

            build_ef = scenario.par("emission_factor", **filters).assign(
                technology=tech_new
            )

            build_rel = scenario.par(
                "relation_activity",
                filters={"technology": tech_orig, "relation": emiss_rel},
            ).assign(technology=tech_new)

            scenario.add_set("commodity", commodity)
            scenario.add_set("technology", tech_new)
            scenario.add_par("input", build_in)
            scenario.add_par("output", build_out)
            scenario.add_par("capacity_factor", build_cf)
            scenario.add_par("emission_factor", build_ef)
            scenario.add_par("relation_activity", build_rel)
