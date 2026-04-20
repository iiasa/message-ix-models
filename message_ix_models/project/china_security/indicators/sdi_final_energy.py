"""
Calculate SDI for final energy by sector
"""
import pandas as pd
import numpy as np
import ixmp
import os
import pyam

# Function to run HHI by fuel and trade type
def calculate_sector_sdi(input_data:pd.DataFrame,
                         region:str,
                         sector_list:list[str] = ["Residential and Commercial", "Transportation"]) -> pd.DataFrame:
    """
    Calculate the SDI for final energy by sector for a given region

    Args:
        input_data: pandas dataframe with trade data (full)
        sector_list: list of sectors to include
        region: list of regions to include
    """

    # Filter data
    df = input_data.copy()

    df = df[df['Variable'].str.split("|").str[0] == "Final Energy"]
    df = df[df['Variable'].str.split("|").str.len() == 3]
    df = df[df['Region'] == region]

    df['Sector'] = df['Variable'].str.split("|").str[1]
    df['Fuel'] = df['Variable'].str.split("|").str[2]

    df = df[df['Sector'].isin(sector_list)]

    df = df.groupby(["Model", "Scenario", "Sector", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()

    # Total 
    df_total = df.groupby(["Model", "Scenario", "Sector", "Unit", "Year"])["Value"].sum().reset_index()
    df_total.rename(columns={"Value": "Total"}, inplace=True)
    df = df.merge(df_total, on=["Model", "Scenario", "Sector", "Unit", "Year"], how="left")

    # Share
    df['p_i'] = df['Value'] / df['Total']
    df['SDI'] = df['p_i'] * np.log(df['p_i'])
    df = df.groupby(["Model", "Scenario", "Sector", "Unit", "Year"])["SDI"].sum().reset_index()
    df['SDI'] = df['SDI'] * -1
    df = df.rename(columns={"SDI": "SDI of Final Energy by Sector"})

    return df