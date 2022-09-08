import re
from typing import List

from message_ix_models import Context, Spec
from message_ix_models.model.structure import get_codes
from message_ix_models.util import load_private_data
from sdmx.model import Code

from message_data.tools import generate_set_elements, get_region_codes


def get_spec(context: Context) -> Spec:
    """Return the specification for MESSAGEix-Transport.

    Parameters
    ----------
    context : .Context
        The key ``regions`` determines the regional aggregation used.
    """
    load_config(context)

    s = Spec()

    for set_name, config in context["buildings set"].items():
        # Elements to add, remove, and require
        for action in {"add", "remove", "require"}:
            s[action].set[set_name].extend(config.get(action, []))

    # Generate commodities that replace corresponding rc_* in the base model
    for c in filter(lambda x: x.id.startswith("rc_"), get_codes("commodity")):
        s.add.set["commodity"].append(Code(id=c.id.replace("rc_", "afofi_")))

    # Generate technologies that replace corresponding *_rc in the base model
    expr = re.compile("_(rc|RC)$")
    for t in filter(lambda x: expr.search(x.id), get_codes("technology")):
        new_id = t.id.replace("_rc", "_afofi").replace("_RC", "_AFOFI")

        # FIXME would prefer to do the following, but .buildings.setup_scenario()
        # currently preserves capitalization
        # new_id = expr.sub("_afofi", t.id)

        s.add.set["technology"].append(Code(id=new_id))

    # The set of required nodes varies according to context.regions
    s.require.set["node"].extend(map(str, get_region_codes(context.regions)))

    return s


def get_techs(spec: Spec, commodity=None) -> List[str]:
    """Return a list of buildings technologies."""
    result = spec.add.set["technology"]
    if commodity:
        result = filter(lambda s: s.id.startswith(commodity), result)

    return sorted(map(str, result))


def load_config(context):
    if "buildings set" in context:
        return

    context["buildings set"] = load_private_data("buildings", "set.yaml")

    # Generate set elements from a product of others
    for set_name, info in context["buildings set"].items():
        generate_set_elements(context, set_name, kind="buildings")
