from message_ix_models import Context
from pathlib import Path
from message_ix_models.util import load_private_data

# Configuration files
METADATA = [
    # ("material", "config"),
    ("material", "set"),
    # ("material", "technology"),
]

def read_config():
    """Read configuration from set.yaml."""
    # TODO this is similar to transport.utils.read_config; make a common
    #      function so it doesn't need to be in this file.
    context = Context.get_instance(-1)

    if "material set" in context:
        # Already loaded
        return context

    # Load material configuration
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_private_data(*_parts)

    # Read material.yaml
    # context.metadata_path=Path("C:/Users/unlu/Documents/GitHub/message_data/data")
    # context.load_config("material", "set")

    # Use a shorter name
    context["material"] = context["material set"]

    # Merge technology.yaml with set.yaml
    # context["material"]["steel"]["technology"]["add"] = (
    #     context.pop("transport technology")
    # )

    return context
