from message_ix_models import Context
from message_ix_models.util import as_codes, load_package_data


def read_config(context=None):
    """Read configuration from material.yaml."""
    # TODO this is similar to transport.utils.read_config; make a common
    #      function so it doesn't need to be in this file.
    context = context or Context.get_instance(0)

    try:
        # Check if the configuration was already loaded
        context["material"]["set"]
    except KeyError:
        # Not yet loaded
        pass
    else:
        # Already loaded
        return context

    # Read material.yaml, store with a shorter name
    context["material"] = load_package_data("material", "config")

    # Convert some values to Code objects
    for set_name, info in context["material"]["set"].items():
        try:
            info["add"] = as_codes(info["add"])
        except KeyError:
            pass

    return context
