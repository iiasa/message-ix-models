"""Build workflow for hydrogen technologies in MESSAGE-IX scenarios.

This module provides functions to add hydrogen technologies and their
parametrization to MESSAGE-IX scenarios following the specification in set.yaml.
"""

import logging
from typing import Any, Optional

import message_ix

from message_ix_models import Context
from message_ix_models.model.build import apply_spec
from message_ix_models.model.hydrogen.data_hydrogen import gen_data_hydrogen
from message_ix_models.model.hydrogen.utils import read_config
from message_ix_models.model.structure import generate_set_elements, get_region_codes
from message_ix_models.util import add_par_data, load_package_data, package_data_path
from message_ix_models.util.scenarioinfo import Spec

log = logging.getLogger(__name__)


def make_spec(regions: str = "R12") -> Spec:
    """Create Spec for hydrogen technologies from set.yaml.

    Parameters
    ----------
    regions : str, optional
        Regional aggregation, by default "R12"

    Returns
    -------
    Spec
        Specification with required, add, and remove set elements
    """
    # Load the hydrogen set configuration
    sets: dict[str, Any] = dict()

    # Load set.yaml
    path = package_data_path("hydrogen", "set.yaml").relative_to(package_data_path())
    sets = load_package_data(*path.parts)

    # Create Spec object
    s = Spec()

    # Process the hydrogen configuration
    # The loaded YAML has structure: {'hydrogen': {'technology': {...}, ...}}
    hydrogen_outer = sets.get("hydrogen", {})
    hydrogen_config = (
        hydrogen_outer.get("hydrogen", {}) if isinstance(hydrogen_outer, dict) else {}
    )

    for set_name, set_config in hydrogen_config.items():
        if not isinstance(set_config, dict):
            continue

        # Convert values to codes if needed
        if not all(
            isinstance(item, list)
            for sublist in set_config.values()
            for item in (sublist if isinstance(sublist, list) else [sublist])
        ):
            generate_set_elements(hydrogen_config, set_name)

        # Process each action (add, remove, require)
        for action in {"add", "remove", "require"}:
            if action in set_config:
                s[action].set[set_name].extend(set_config[action])

        # Handle indexers if present
        try:
            s.add.set[f"{set_name} indexers"] = set_config["indexers"]
        except KeyError:
            pass

    # Add required nodes based on regional specification
    codelist = regions
    try:
        s["require"].set["node"].extend(map(str, get_region_codes(codelist)))
    except FileNotFoundError:
        raise ValueError(
            f"Cannot get spec for hydrogen technologies with regions={codelist!r}"
        ) from None

    return s


def add_data(scenario: message_ix.Scenario, dry_run: bool = False) -> None:
    """Populate scenario with hydrogen technology data.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario to add data to
    dry_run : bool, optional
        If True, do not actually add data to scenario, by default False
    """
    log.info("Generating hydrogen technology data...")

    # Generate all hydrogen data
    data = gen_data_hydrogen(scenario, dry_run=dry_run)

    # Filter out empty dataframes
    data = {k: v for k, v in data.items() if not v.empty}

    # Add to scenario
    log.info(f"Adding {len(data)} parameter types to scenario")
    add_par_data(scenario, data, dry_run=dry_run)

    log.info("Hydrogen technology data added successfully")


def build(
    context: Context,
    scenario: message_ix.Scenario,
    fast: bool = False,
) -> message_ix.Scenario:
    """Build hydrogen technologies onto scenario.

    This function adds hydrogen production, storage, and utilization
    technologies to a MESSAGE-IX scenario following the specification
    in set.yaml.

    Parameters
    ----------
    context : Context
        Context object with model configuration
    scenario : message_ix.Scenario
        Base scenario to build upon
    fast : bool, optional
        If True, use faster but less validated build process, by default False

    Returns
    -------
    message_ix.Scenario
        Modified scenario with hydrogen technologies

    Raises
    ------
    NotImplementedError
        If regional aggregation is not R12
    """
    # Get regional specification
    node_suffix = context.model.regions

    # Currently only support R12 regions
    if node_suffix != "R12":
        raise NotImplementedError(
            "Hydrogen technologies currently only support "
            "MESSAGE-IX-GLOBIOM R12 regions"
        )

    # Check if global region exists, add if needed
    if f"{node_suffix}_GLB" not in list(scenario.platform.regions().region):
        log.info(f"Adding global region {node_suffix}_GLB")
        scenario.platform.add_region(f"{node_suffix}_GLB", "region", "World")

    # Get the specification from set.yaml
    log.info("Creating hydrogen technology specification...")
    spec = make_spec(node_suffix)

    # Apply spec to scenario (adds/removes sets)
    log.info("Applying specification to scenario...")
    apply_spec(scenario, spec, add_data, fast=fast)

    log.info("Hydrogen technologies built successfully")
    return scenario


def get_spec() -> dict[str, Any]:
    """Return the specification for hydrogen technologies.

    This is a convenience function that returns the specification
    dictionary structure directly.

    Returns
    -------
    dict
        Dictionary with 'require', 'add', 'remove' keys containing set specifications
    """
    # Load configuration
    context = read_config()

    require = {}
    add = {}
    remove = {}

    # Process hydrogen configuration
    hydrogen_config = context.get("hydrogen", {})

    for set_name, config in hydrogen_config.items():
        if not isinstance(config, dict):
            continue

        # Required elements
        if "require" in config:
            require.setdefault(set_name, []).extend(config["require"])

        # Elements to add
        if "add" in config:
            add.setdefault(set_name, []).extend(config["add"])

        # Elements to remove
        if "remove" in config:
            remove.setdefault(set_name, []).extend(config["remove"])

    return dict(require=require, add=add, remove=remove)
