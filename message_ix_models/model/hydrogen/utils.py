from message_ix_models import Context
from message_ix_models.util import load_package_data

# Configuration files
METADATA = [
    ("hydrogen", "set"),
]


def read_config() -> Context:
    """Read configuration from set.yaml.

    Returns
    -------
    message_ix_models.Context
        Context object holding information about MESSAGEix-Hydrogen structure
    """
    context = Context.get_instance(-1)

    if "hydrogen set" in context:
        # Already loaded
        return context

    # Load material configuration
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_package_data(*_parts)

    # Use a shorter name
    context["hydrogen"] = context["hydrogen set"]
    return context