import logging
from typing import Any

import message_ix

from message_ix_models import Context
from message_ix_models.model.build import apply_spec
from message_ix_models.model.material.data_aluminum import gen_data_aluminum
from message_ix_models.model.material.data_ammonia import gen_all_NH3_fert
from message_ix_models.model.material.data_cement import gen_data_cement
from message_ix_models.model.material.data_generic import gen_data_generic
from message_ix_models.model.material.data_methanol import gen_data_methanol
from message_ix_models.model.material.data_other_industry import gen_data_other
from message_ix_models.model.material.data_petro import gen_data_petro_chemicals
from message_ix_models.model.material.data_power_sector import gen_data_power_sector
from message_ix_models.model.material.data_steel import gen_data_steel
from message_ix_models.model.material.data_util import add_water_par_data
from message_ix_models.model.material.share_constraints import CommShareConfig
from message_ix_models.model.material.util import path_fallback
from message_ix_models.model.structure import generate_set_elements, get_region_codes
from message_ix_models.util import (
    add_par_data,
    load_package_data,
    package_data_path,
)
from message_ix_models.util.scenarioinfo import Spec

log = logging.getLogger(__name__)

DATA_FUNCTIONS = [
    gen_data_other,
    gen_data_aluminum,
    gen_data_methanol,
    gen_all_NH3_fert,
    gen_data_generic,
    gen_data_steel,
    gen_data_cement,
    gen_data_petro_chemicals,
    gen_data_power_sector,
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
    iea_data_path: str | None = None,
    power_sector: bool = False,
) -> message_ix.Scenario:
    """Build Materials model on `scenario`."""
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
    # Add remaining structure that is not supported by make_spec() e.g. .add_cat() calls
    with scenario.transact():
        CommShareConfig.from_files(scenario, "coal_residual_industry").add_to_scenario(
            scenario
        )
    # exclude power sector data if not requested
    if not power_sector:
        DATA_FUNCTIONS.pop()
    apply_spec(scenario, spec, add_data, fast=True)
    add_water_par_data(scenario)
    return scenario


def make_spec(regions: str, materials: str or None = SPEC_LIST) -> Spec:
    """Return the structural :class:`Spec` for MESSAGEix-Materials."""
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
