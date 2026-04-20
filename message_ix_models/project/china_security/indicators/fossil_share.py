"""
Calculate fossil fuel share of primary energy
"""
import pandas as pd
import numpy as np
import ixmp
import os
import pyam

# Function to run HHI by fuel and trade type
def calculate_fossil_share(input_data:pd.DataFrame,
                           region:str,
                           use_units:str = "EJ/yr",
                           fossil_fuels:list[str] = ["Coal", "Oil", "Gas"]) -> pd.DataFrame:
    """
    Calculate the fossil fuel share of primary energy for a given region

    Args:
        input_data: pandas dataframe with primary energy data (full)
        region: region to include
        use_units: units of energy (EJ/yr)
        fossil_fuels: list of fossil fuels to include
    """

    # Filter data
    df = input_data.copy()
    df = df[df['Unit'] == use_units]

    df = df[df['Variable'].str.split("|").str[0] == "Primary Energy"]
    df = df[df['Variable'].str.split("|").str.len() == 3]
    df = df[df['Region'] == region]

    df['Fuel'] = df['Variable'].str.split("|").str[1]

    df = df.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()

    # Total 
    df_total = df.groupby(["Model", "Scenario", "Unit", "Year"])["Value"].sum().reset_index()
    df_total.rename(columns={"Value": "Total"}, inplace=True)
    df = df.merge(df_total, on=["Model", "Scenario", "Unit", "Year"], how="left")

    # Calculate share
    df["Share"] = df["Value"] / df["Total"]

    # Differentiate fossil fuels
    df = df[df['Fuel'].isin(fossil_fuels)]
    df = df.groupby(["Model", "Scenario", "Unit", "Year"])["Share"].sum().reset_index()
    df = df.rename(columns={"Share": "Fossil Share of Primary Energy"})

    df = df.merge(df_total, on=["Model", "Scenario", "Unit", "Year"], how="outer")
    df = df.rename(columns={"Total": "Total Primary Energy"})

    return df
    