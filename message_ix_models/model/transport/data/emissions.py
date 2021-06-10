from message_data.tools import load_data


def get_emissions_data(context):
    """Load emissions data from a file."""
    source = context["transport config"]["data source"]["emissions"]

    return dict(
        emission_factor=load_data(None, "transport", "emi", f"{source}-emission_factor")
    )
