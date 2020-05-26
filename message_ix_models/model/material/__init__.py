from typing import Mapping

from message_data.tools import ScenarioInfo, as_codes, get_context


def get_spec() -> Mapping[str, ScenarioInfo]:
    """Return the specification for materials accounting."""
    require = ScenarioInfo()
    add = ScenarioInfo()

    # Load configuration
    context = read_config()

    # Update the ScenarioInfo objects with required and new set elements
    for set_name, config in context["material"]["set"].items():
        # Required elements
        require.set[set_name].extend(config.get("require", []))

        # Elements to add
        add.set[set_name].extend(config.get("add", []))

    return dict(require=require, remove=ScenarioInfo(), add=add)


def read_config():
    """Read configuration from material.yaml."""
    # TODO this is similar to transport.utils.read_config; make a common
    #      function so it doesn't need to be in this file.
    context = get_context()

    try:
        # Check if the configuration was already loaded
        context["material"]["set"]
    except KeyError:
        # Not yet loaded
        pass
    else:
        # Already loaded
        return context

    # Read material.yaml
    context.load_config("material")

    # Convert some values to Code objects
    for set_name, info in context["material"]["set"].items():
        try:
            info["add"] = as_codes(info["add"])
        except KeyError:
            pass

    return context
