from collections.abc import Sequence
from copy import deepcopy
from itertools import chain
from typing import Any, Union

from sdmx.model.common import Code
from sdmx.model.v21 import Annotation

from message_ix_models import ScenarioInfo, Spec
from message_ix_models.model import disutility
from message_ix_models.model.structure import generate_set_elements, get_region_codes
from message_ix_models.util import load_package_data, package_data_path

from .util import region_path_fallback

#: Template for disutility technologies.
TEMPLATE = Code(
    id="{technology} usage by {group}",
    annotations=[
        Annotation(
            id="input",
            text=repr(
                dict(
                    commodity="transport vehicle {technology}",
                    level="useful",
                    unit="Gv km",
                )
            ),
        ),
        Annotation(
            id="output",
            text=repr(
                dict(commodity="transport pax {group}", level="useful", unit="Gp km")
            ),
        ),
        Annotation(id="is-disutility", text=repr(True)),
        Annotation(id="units", text="registry.Unit('passenger / vehicle')"),
    ],
)


def get_technology_groups(
    technologies: Union[Spec, ScenarioInfo, Sequence["Code"]],
) -> dict[str, list[str]]:
    """Subsets of transport technologies for aggregation, mapping, and filtering.

    Returns
    -------
    dict
        Values are lists of transport technologies (|t|) that appear in the model.
        Keys include:

          - Codes from :file:`transport/technology.yaml` with children. These can be
            modes, services, groups of either, or other groups of technologies. Children
            are processed recursively to obtain |t| elements.
          - "historical-only": includes technologies where this annotation exists and
            is set to :any:`True`.
          - "LDV usage": includes the technologies generated using :data:`TEMPLATE`.
            See :func:`make_spec`.
    """
    if isinstance(technologies, Spec):
        t_list: Sequence["Code"] = technologies.add.set["technology"]
    elif isinstance(technologies, ScenarioInfo):
        t_list = technologies.set["technology"]
    else:
        t_list = technologies

    result: dict[str, list[str]] = {"historical-only": [], "LDV usage": []}

    def _leaf_ids(node) -> list[str]:
        """Recursively collect leaf IDs."""
        return list(
            chain(*[_leaf_ids(c) if len(c.child) else (c.id,) for c in node.child])
        )

    for tech in t_list:
        if len(tech.child):
            # Code with child codes → a group of technologies → store all the leaf IDs
            result[tech.id] = _leaf_ids(tech)
        else:
            # Code without children = an individual technology → add to certain groups
            if tech.eval_annotation(id="historical-only") is True:
                result["historical-only"].append(tech.id)
            if tech.eval_annotation(id="is-disutility") is True:
                result["LDV usage"].append(tech.id)

    return result


def make_spec(regions: str) -> Spec:
    """Return the structural :class:`Spec` for MESSAGEix-Transport."""
    sets: dict[str, Any] = dict()

    # Overrides specific to regional versions
    tmp = dict()
    for fn in ("set.yaml", "technology.yaml"):
        # Field name
        name = fn.split(".yaml")[0]

        # Load and store the data from the YAML file: either in a subdirectory for
        # context.model.regions, or the top-level data directory
        path = region_path_fallback(regions, fn).relative_to(package_data_path())
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

    # node: the set of required nodes varies according to context.model.regions
    codelist = regions
    try:
        s["require"].set["node"].extend(map(str, get_region_codes(codelist)))
    except FileNotFoundError:
        raise ValueError(
            f"Cannot get spec for MESSAGEix-Transport with regions={codelist!r}"
        ) from None

    # Identify LDV technologies
    techs = s.add.set["technology"]
    LDV_techs = techs[techs.index("LDV")].child

    # Associate LDV techs with their output commodities
    for t in LDV_techs:
        output = dict(commodity=f"transport vehicle {t.id}", level="useful")
        t.annotations.append(Annotation(id="output", text=repr(output)))

    # Associate other techs with their output commodities
    for mode in "F RAIL", "F ROAD":
        parent = techs[techs.index(mode)]
        for t in parent.child:
            t.annotations.append(deepcopy(parent.get_annotation(id="output")))

    # Generate a spec for the generalized disutility formulation for LDVs
    s2 = disutility.get_spec(
        groups=s.add.set["consumer_group"], technologies=LDV_techs, template=TEMPLATE
    )

    # Merge the items to be added by the two specs
    s.add.update(s2.add)

    return s
