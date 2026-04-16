"""
Calculate HHI by fuel and trade type
"""
import pandas as pd
import numpy as np
import ixmp
import os
import pyam

# Function to run HHI by fuel and trade type
def calculate_energy_hhi(input_data:pd.DataFrame,
                        portfolio_level:str,
                        region:str,
                        use_units:str = "EJ/yr") -> pd.DataFrame:
    """
    Calculate the HHI by fuel and trade type for a given region

    Args:
        input_data: pandas dataframe with trade data (full)
        use_units: physical (EJ/yr) or monetary (billion USD_2010/yr)
        portfolio_level: Energy level (Primary Energy or Secondary Energy)
        region: list of regions to include
        use_units: physical (EJ/yr) or monetary (billion USD_2010/yr)
    """

    # Filter data
    df = input_data.copy()
    df = df[df['Unit'] == use_units]
    
    # Primary or secondary energy demand (inlcuding solar and wind)
    pdf = input_data.copy()
    pdf = pdf[pdf['Unit'] == use_units]

    pdf = pdf[pdf['Variable'].str.split("|").str[0] == portfolio_level]
    pdf = pdf[pdf['Variable'].str.split("|").str.len() == 3]
    pdf = pdf[pdf['Region'] == region]

    pdf['Fuel'] = pdf['Variable'].str.split("|").str[1]

    pdf = pdf.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()

    # Total 
    pdf_total = pdf.groupby(["Model", "Scenario", "Unit", "Year"])["Value"].sum().reset_index()
    pdf_total.rename(columns={"Value": "Total"}, inplace=True)
    pdf = pdf.merge(pdf_total, on=["Model", "Scenario", "Unit", "Year"], how="left")

    # Calculate HHI
    pdf["HHI"] = (pdf["Value"] / pdf["Total"]) ** 2
    pdf = pdf.groupby(["Model", "Scenario", "Unit", "Year"])[f"HHI of ({portfolio_level})"].sum().reset_index()

    return pdf
    