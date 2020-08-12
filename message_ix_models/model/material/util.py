from message_ix_models import Context
from message_ix_models.util import as_codes, load_package_data


def read_config():
    """Read configuration from set.yaml."""
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

    # Read material.yaml
    context.load_config("material", "set")

    # Use a shorter name
    context["material"] = context["material set"]

    # Merge technology.yaml with set.yaml
    # context["material"]["steel"]["technology"]["add"] = (
    #     context.pop("transport technology")
    # )

    # Convert some values to Code objects
    # JM: Ask what this is
    # for set_name, info in context["material"]["common"].items():
    #     try:
    #         info["add"] = as_codes(info["add"])
    #     except KeyError:
    #         pass

    return context
