"""Prepare data from the IKARUS model via GEAM_TRP_techinput.xlsx."""
from message_ix.model.transport.util import ScenarioInfo


def main(scenario):
    s_info = ScenarioInfo(scenario)
    # Open the GEAM_TRP_techinput.xls file using openpyxl

    # Open the 'updateTRPdata' sheet

    # Read values from table for e.g. "regional train electric efficient"
    # (= rail_pub)

    # Compute message_ix quantities *with* proper unit conversion:

    # - Set up a pint.UnitRegistry
    # - define all the units: EUR

    s_info.N  # list of nodes e.g. for node_loc column of parameters
    s_info.Y  # list of years e.g. for year_vtg column of parameters

    # moutp
    # capacity_factor (~plf)
    # technical_lifetime (~pll)
    # inv (~inv)
    # fixed_cost (~fom)

    # Write the resulting data to temporary files: 1 per parameter.
