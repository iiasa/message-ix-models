from collections.abc import Sequence
from typing import Any, Union

from sdmx.model.common import Annotation, Code

from message_ix_models import ScenarioInfo, Spec
from message_ix_models.model import disutility
from message_ix_models.model.structure import generate_set_elements, get_region_codes
from message_ix_models.util import load_package_data, package_data_path

from .util import path_fallback

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
    """Subsets of transport technologies for aggregation and filtering."""
    if isinstance(technologies, Spec):
        t_list: Sequence["Code"] = technologies.add.set["technology"]
    elif isinstance(technologies, ScenarioInfo):
        t_list = technologies.set["technology"]
    else:
        t_list = technologies

    result: dict[str, list[str]] = {"non-ldv": []}

    # Only include those technologies with children
    for tech in filter(lambda t: len(t.child), t_list):
        result[tech.id] = list(c.id for c in tech.child)
        # Store non-LDV technologies
        if tech.id != "LDV":
            result["non-ldv"].extend(result[tech.id])

    return result


def make_spec(regions: str) -> Spec:
    sets: dict[str, Any] = dict()

    # Overrides specific to regional versions
    tmp = dict()
    for fn in ("set.yaml", "technology.yaml"):
        # Field name
        name = fn.split(".yaml")[0]

        # Load and store the data from the YAML file: either in a subdirectory for
        # context.model.regions, or the top-level data directory
        path = path_fallback(regions, fn).relative_to(package_data_path())
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

    # Generate a spec for the generalized disutility formulation for LDVs
    s2 = disutility.get_spec(
        groups=s.add.set["consumer_group"], technologies=LDV_techs, template=TEMPLATE
    )

    # Merge the items to be added by the two specs
    s.add.update(s2.add)

    return s
