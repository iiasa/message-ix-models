from message_data.tools import as_codes, get_context


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
    context.load_config("material", "config")

    # Use a shorter name
    context["material"] = context["material config"]

    # Convert some values to Code objects
    for set_name, info in context["material"]["set"].items():
        try:
            info["add"] = as_codes(info["add"])
        except KeyError:
            pass

    return context
