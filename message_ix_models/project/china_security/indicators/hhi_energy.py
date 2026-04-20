"""
Calculate HHI by fuel and energy type
"""
import pandas as pd
import numpy as np
import ixmp
import os
import pyam

# Function to run HHI by fuel and trade type
def calculate_energy_hhi(input_data:pd.DataFrame,
                         portfolio_level:str,
                         fuel_level:int,
                         region:str,
                         use_units:str = "EJ/yr",
                         secondary_energy_type:str = None,
                         fuel_subset:list[str] = None) -> pd.DataFrame:
    """
    Calculate the HHI by fuel and energy type for a given region

    Args:
        input_data: pandas dataframe with trade data (full)
        portfolio_level: Energy level (Primary Energy or Secondary Energy)
        fuel_level: level of fuel (1 for Primary Energy, 2 for Secondary Energy)
        region: region to include
        use_units: units of energy (EJ/yr)
        secondary_energy_type: type of secondary energy (e.g. "Electricity")
        fuel_subset: list of fuels to include
    """
    # Primary or secondary energy demand (inlcuding solar and wind)
    pdf = input_data.copy()
    pdf = pdf[pdf['Unit'] == use_units]

    pdf = pdf[pdf['Variable'].str.split("|").str[0] == portfolio_level]
    pdf = pdf[pdf['Variable'].str.split("|").str.len() == fuel_level + 1]
    if fuel_level == 2:
        pdf = pdf[pdf['Variable'].str.split("|").str[fuel_level - 1] == secondary_energy_type]

    pdf = pdf[pdf['Region'] == region]

    pdf['Fuel'] = pdf['Variable'].str.split("|").str[fuel_level] # For PE and SE HHI = 1, for electricity HHI = 2
    if fuel_subset is not None:
        pdf = pdf[pdf['Fuel'].isin(fuel_subset)] # For electricity, ensure 

    pdf = pdf.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()

    # Total 
    pdf_total = pdf.groupby(["Model", "Scenario", "Unit", "Year"])["Value"].sum().reset_index()
    pdf_total.rename(columns={"Value": "Total"}, inplace=True)
    pdf = pdf.merge(pdf_total, on=["Model", "Scenario", "Unit", "Year"], how="left")

    # Calculate HHI
    pdf["HHI"] = (pdf["Value"] / pdf["Total"]) ** 2
    pdf = pdf.groupby(["Model", "Scenario", "Unit", "Year"])[f"HHI"].sum().reset_index()

    pdf = pdf.merge(pdf_total, on=["Model", "Scenario", "Unit", "Year"], how="left")

    varname = portfolio_level
    if secondary_energy_type is not None:
        varname = varname + " (" + secondary_energy_type + ")"

    pdf = pdf.rename(columns={"HHI": f"HHI of {varname}"})
    pdf = pdf.rename(columns={"Total": f"Total of {varname}"})

    return pdf
    