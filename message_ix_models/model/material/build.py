import logging
from collections.abc import Mapping
from typing import Any, Optional

import message_ix
import pandas as pd

from message_ix_models import Context
from message_ix_models.model.build import apply_spec
from message_ix_models.model.material.data_aluminum import gen_data_aluminum
from message_ix_models.model.material.data_ammonia_new import gen_all_NH3_fert
from message_ix_models.model.material.data_cement import gen_data_cement
from message_ix_models.model.material.data_generic import gen_data_generic
from message_ix_models.model.material.data_methanol import gen_data_methanol
from message_ix_models.model.material.data_petro import gen_data_petro_chemicals
from message_ix_models.model.material.data_steel import gen_data_steel
from message_ix_models.model.material.data_util import (
    add_ccs_technologies,
    add_cement_bounds_2020,
    add_coal_lowerbound_2020,
    add_elec_i_ini_act,
    add_elec_lowerbound_2020,
    add_emission_accounting,
    add_new_ind_hist_act,
    modify_baseyear_bounds,
    modify_demand_and_hist_activity,
    modify_industry_demand,
)
from message_ix_models.model.material.util import path_fallback, read_config
from message_ix_models.model.structure import generate_set_elements, get_region_codes
from message_ix_models.util import (
    add_par_data,
    identify_nodes,
    load_package_data,
    package_data_path,
)
from message_ix_models.util.compat.message_data import (
    calibrate_UE_gr_to_demand,
    calibrate_UE_share_constraints,
)
from message_ix_models.util.compat.message_data import (
    manual_updates_ENGAGE_SSP2_v417_to_v418 as engage_updates,
)
from message_ix_models.util.scenarioinfo import ScenarioInfo, Spec

log = logging.getLogger(__name__)

DATA_FUNCTIONS = [
    gen_data_aluminum,
    gen_data_methanol,
    gen_all_NH3_fert,
    gen_data_generic,
    gen_data_steel,
    gen_data_cement,
    gen_data_petro_chemicals,
    # gen_data_power_sector,
]

# add as needed/implemented
SPEC_LIST = (
    "generic",
    "common",
    "steel",
    "cement",
    "aluminum",
    "petro_chemicals",
    "buildings",
    "power_sector",
    "fertilizer",
    "methanol",
)


# Try to handle multiple data input functions from different materials
def add_data(scenario: message_ix.Scenario, dry_run: bool = False) -> None:
    """Populate `scenario` with MESSAGEix-Materials data."""
    # Information about `scenario`
    for func in DATA_FUNCTIONS:
        # Generate or load the data; add to the Scenario
        log.info(f"from {func.__name__}()")
        data = func(scenario)
        data = {k: v for k, v in data.items() if not v.empty}
        add_par_data(scenario, data, dry_run=dry_run)
    log.info("done")


def build(
    context: Context,
    scenario: message_ix.Scenario,
    old_calib: bool,
    modify_existing_constraints: bool = True,
    iea_data_path: Optional[str] = None,
) -> message_ix.Scenario:
    """Set up materials accounting on `scenario`."""
    node_suffix = context.model.regions

    if node_suffix != "R12":
        raise NotImplementedError(
            "MESSAGEix-Materials is currently only supporting"
            " MESSAGEix-GLOBIOM R12 regions"
        )

    if f"{node_suffix}_GLB" not in list(scenario.platform.regions().region):
        # Required for material trade model
        # TODO Include this in the spec, while not using it as a value for `node_loc`
        scenario.platform.add_region(f"{node_suffix}_GLB", "region", "World")

    # Get the specification and apply to the base scenario
    spec = make_spec(node_suffix)
    apply_spec(scenario, spec, add_data, fast=True)  # dry_run=True

    water_dict = pd.read_excel(
        package_data_path("material", "other", "water_tec_pars.xlsx"),
        sheet_name=None,
    )
    scenario.check_out()
    for par in water_dict.keys():
        scenario.add_par(par, water_dict[par])
    scenario.commit("add missing water tecs")

    # Adjust exogenous energy demand to incorporate the endogenized sectors
    # Adjust the historical activity of the useful level industry technologies
    # Coal calibration 2020
    add_ccs_technologies(scenario)
    if old_calib:
        modify_demand_and_hist_activity(scenario)
    else:
        modify_baseyear_bounds(scenario)
        last_hist_year = scenario.par("historical_activity")["year_act"].max()
        modify_industry_demand(scenario, last_hist_year, iea_data_path)
        add_new_ind_hist_act(scenario, [last_hist_year], iea_data_path)
        add_emission_accounting(scenario)

    if modify_existing_constraints:
        calibrate_existing_constraints(scenario)

    return scenario


def calibrate_existing_constraints(scenario: message_ix.Scenario):
    if "SSP_dev" not in scenario.model:
        engage_updates._correct_balance_td_efficiencies(scenario)
        engage_updates._correct_coal_ppl_u_efficiencies(scenario)
        engage_updates._correct_td_co2cc_emissions(scenario)

    from message_ix_models import ScenarioInfo

    s_info = ScenarioInfo(scenario)
    nodes = s_info.N

    add_coal_lowerbound_2020(scenario)
    add_cement_bounds_2020(scenario)

    # Market penetration adjustments
    # NOTE: changing demand affects the market penetration
    # levels for the end-use technologies.
    # FIXME: context.ssp only works for SSP1/2/3 currently missing SSP4/5
    calibrate_UE_gr_to_demand(
        scenario,
        s_info,
        data_path=package_data_path("material"),
        ssp="SSP2",
        region=identify_nodes(scenario),
    )
    calibrate_UE_share_constraints(scenario, s_info)

    # Electricity calibration to avoid zero prices for CHN.
    if "R12_CHN" in nodes:
        add_elec_lowerbound_2020(scenario)

    df = scenario.par(
        "bound_activity_lo",
        filters={"node_loc": "R12_RCPA", "technology": "sp_el_I", "year_act": 2020},
    )
    scenario.check_out()
    scenario.remove_par("bound_activity_lo", df)
    scenario.commit("remove sp_el_I min bound on RCPA in 2020")

    add_elec_i_ini_act(scenario)

    # remove scrap constraint for aluminum recycling in base year
    df_scrap_inp = scenario.par(
        "input",
        filters={
            "technology": "secondary_aluminum",
            "commodity": "aluminum",
            "year_act": 2020,
        },
    )
    with scenario.transact():
        scenario.remove_par("input", df_scrap_inp)

    df_out_scrap = scenario.par(
        "output",
        filters={"level": "new_scrap", "commodity": "aluminum", "year_act": 2020},
    )
    with scenario.transact():
        scenario.remove_par("output", df_out_scrap)


def get_spec() -> Mapping[str, ScenarioInfo]:
    """Return the specification for materials accounting."""
    require = ScenarioInfo()
    add = ScenarioInfo()
    remove = ScenarioInfo()

    # Load configuration
    # context = Context.get_instance(-1)
    context = read_config()

    # Update the ScenarioInfo objects with required and new set elements
    for type in SPEC_LIST:
        for set_name, config in context["material"][type].items():
            # for cat_name, detail in config.items():
            # Required elements
            require.set[set_name].extend(config.get("require", []))

            # Elements to add
            add.set[set_name].extend(config.get("add", []))

            # Elements to remove
            remove.set[set_name].extend(config.get("remove", []))

    return dict(require=require, add=add, remove=remove)


def make_spec(regions: str, materials: str or None = SPEC_LIST) -> Spec:
    sets: dict[str, Any] = dict()
    materials = ["common"] if not materials else materials
    # Overrides specific to regional versions
    tmp = dict()
    # technology.yaml currently not used in Materials
    for fn in ["set.yaml"]:  # , "technology.yaml"):
        # Field name
        name = fn.split(".yaml")[0]

        # Load and store the data from the YAML file: either in a subdirectory for
        # context.model.regions, or the top-level data directory
        path = path_fallback(regions, fn).relative_to(package_data_path())
        # tmp[name] = load_private_data(*path.parts)
        tmp[name] = load_package_data(*path.parts)

    # Merge contents of technology.yaml into set.yaml
    # technology.yaml currently not used in Materials
    sets.update(tmp.pop("set"))

    s = Spec()

    # Convert some values to codes
    for material in materials:
        for set_name in sets[material]:
            if not all(
                [
                    isinstance(item, list)
                    for sublist in sets[material][set_name].values()
                    for item in sublist
                ]
            ):
                generate_set_elements(sets[material], set_name)

            # Elements to add, remove, and require
            for action in {"add", "remove", "require"}:
                s[action].set[set_name].extend(sets[material][set_name].get(action, []))
            try:
                s.add.set[f"{set_name} indexers"] = sets[material][set_name]["indexers"]
            except KeyError:
                pass

    # The set of required nodes varies according to context.model.regions
    codelist = regions
    try:
        s["require"].set["node"].extend(map(str, get_region_codes(codelist)))
    except FileNotFoundError:
        raise ValueError(
            f"Cannot get spec for MESSAGEix-Materials with regions={codelist!r}"
        ) from None

    return s
