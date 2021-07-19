"""Transport emissions data."""
from genno.computations import load_file

from message_data.model.transport.utils import path_fallback


def get_emissions_data(context):
    """Load emissions data from a file."""
    source = context["transport config"]["data source"]["emissions"]

    qty = load_file(path_fallback(context, "emi", f"{source}-emission_factor.csv"))

    return dict(emission_factor=qty.to_dataframe())
