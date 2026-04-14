from collections import defaultdict
from collections.abc import Sequence
from copy import deepcopy
from typing import Any

from sdmx.model.common import Code
from sdmx.model.v21 import Annotation

from message_ix_models import ScenarioInfo, Spec
from message_ix_models.model import disutility
from message_ix_models.model.structure import generate_set_elements, get_region_codes
from message_ix_models.util import load_package_data, package_data_path
from message_ix_models.util.sdmx import leaf_ids

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


def _code_list(
    key: str, data: Spec | ScenarioInfo | Sequence["Code"]
) -> Sequence["Code"]:
    """Handle input for :func:`get_commodity_groups`, :func:`get_technology_groups`."""
    match data:
        case Spec():
            return data.add.set[key]
        case ScenarioInfo():
            return data.set[key]
        case _:
            return data


def get_commodity_groups(
    commodities: Spec | ScenarioInfo | Sequence["Code"],
) -> dict[str, list[str]]:
    """Subsets of transport commodities for aggregation, mapping, and filtering.

    Returns
    -------
    dict
        Values are lists of transport commodities (|c|) that appear in the model.
        Keys include:

        - "activity F": commodities measuring freight activity.
        - "activity P": commodities measuring passenger activity.
        - "disutility": the disutility commodity.
        - "vehicle activity": commodities measuring vehicle activity
        - "_T": total or all; union of the above 4 groups.
    """
    result: dict[str, list[str]] = defaultdict(list)

    for c in _code_list("commodity", commodities):
        result["_T"].append(c.id)
        if c.id == "disutility":
            result["disutility"].append(c.id)
            continue

        match c.id.split()[1:]:
            case ("vehicle", *_) | (*_, "vehicle"):
                result["vehicle activity"].append(c.id)
            case ("pax", *_):
                result["activity P"].append(c.id)
            case ("F", *_):
                result["activity F"].append(c.id)
            case _:
                raise ValueError(c)

    # Check consistency
    assert {"activity F", "activity P", "disutility", "vehicle activity", "_T"} == set(
        result
    )

    # Convert to a non-defaultdict, so incorrect lookups fail
    return dict(result)


def get_technology_groups(
    technologies: Spec | ScenarioInfo | Sequence["Code"],
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
        - "historical-only": includes technologies where this annotation exists and is
          set to :any:`True`.
        - "usage": all 'usage' pseudo-technologies that transform vehicle activity into
          freight or passenger activity.
        - "usage LDV": includes the technologies generated using :data:`TEMPLATE`. See
          :func:`make_spec`.
        - "vehicle": all vehicle technologies that transform energy inputs into vehicle
          activity.
        - "_T": total or all; the list of all technologies.
    """
    result: dict[str, list[str]] = defaultdict(list)

    for t in _code_list("technology", technologies):
        if len(t.child):
            # Code with child codes → a group of technologies → store all the leaf IDs
            techs = result[t.id] = leaf_ids(t)
            # Add to either "usage" or "vehicle" group
            result["usage" if "usage" in t.id else "vehicle"].extend(techs)
            # Add to catch-all group
            result["_T"].extend(techs)
        elif t.eval_annotation(id="historical-only") is True:
            # Code without children = an individual technology → add to certain groups
            result["historical-only"].append(t.id)
        elif t.eval_annotation(id="is-disutility") is True:
            result["usage LDV"].append(t.id)

    # Convert to a non-defaultdict, so incorrect lookups fail
    return dict(result)


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
