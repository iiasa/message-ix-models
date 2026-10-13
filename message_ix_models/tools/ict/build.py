import logging
from typing import TYPE_CHECKING, Any

from message_ix_models import ScenarioInfo, Spec
from message_ix_models.model.build import apply_spec
from message_ix_models.model.structure import generate_set_elements
from message_ix_models.tools.ict.data import gen_data
from message_ix_models.util import load_package_data, package_data_path

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)


def make_spec() -> Spec:
    """Return the structural :class:`Spec` for MESSAGEix-Transport.

    Configuration is loaded from :file:`…/ict/set.yaml` and
    :file:`…/ict/technology.yaml`.
    """
    sets: dict[str, Any] = dict()

    # Overrides specific to regional versions
    tmp = dict()
    for fn in ("set.yaml", "technology.yaml"):
        # Field name
        name = fn.split(".yaml")[0]

        # Load and store the data from the YAML file: either in a subdirectory for
        # context.model.regions, or the top-level data directory
        path = package_data_path("ict", fn)
        tmp[name] = load_package_data(*path.parts)

    # Merge contents of technology.yaml into set.yaml
    sets.update(tmp.pop("set"))
    sets["technology"]["add"] = tmp.pop("technology")

    s = Spec()

    # Convert some values to codes
    for set_name in sets:
        generate_set_elements(sets, set_name)

        # Elements to add, remove, and require
        for action in {"add", "remove", "require"}:
            s[action].set[set_name].extend(sets[set_name].get(action, []))
        try:
            s.add.set[f"{set_name} indexers"] = sets[set_name]["indexers"]
        except KeyError:
            pass

    return s


def build(context, scenario: "Scenario", ict_scenario: str) -> "Scenario":
    """Build ICT model on `scenario`."""
    node_suffix = context.model.regions

    if node_suffix != "R12":
        raise NotImplementedError(
            "ICT module is currently only supporting models with MESSAGEix-GLOBIOM R12"
            "regions"
        )

    context.model.ict_scenario = ict_scenario

    # Get the specification and apply to the base scenario
    spec = make_spec()
    par_data = gen_data(context, ScenarioInfo(scenario), spec)

    def data(scenario, dry_run=False):
        return par_data

    apply_spec(scenario, spec, data, fast=True)
    return scenario
