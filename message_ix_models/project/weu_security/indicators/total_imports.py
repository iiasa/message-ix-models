"""
Calculate total imports
"""
import pandas as pd
import numpy as np
import ixmp
import os
import pyam

# Function to run HHI by fuel and trade type
def calculate_total_imports(input_data:pd.DataFrame,
                            portfolio:list[str],
                            region:list[str],
                            region_name:str,
                            use_units:str = "EJ/yr") -> pd.DataFrame:
    """
    Calculate the total imports for a given region

    Args:
        input_data: pandas dataframe with trade data (full)
        portfolio: list of fuels (e.g. ["Oil", "Gas"])
        region: list of regions to include
        region_name: name of the region
        use_units: physical (EJ/yr) or monetary (billion USD_2010/yr)
    """

    # Filter data
    df = input_data.copy()
    df = df[df['Unit'] == use_units]
    
    df = df[df['Variable'].str.split("|").str[0] == "Trade"]
    df = df[df['Variable'].str.contains('|'.join(portfolio))]

    df['Fuel'] = ''
    for p in portfolio:
        df['Fuel'] = np.where(df['Variable'].str.contains(p), p, df['Fuel'])

    df['exporter'] = df['Region'].str.split(">").str[0]
    df['importer'] = df['Region'].str.split(">").str[1]
    
    df = df[df['importer'].isin(region)]
    df['importer'] = region_name

    # Sum to fuel level
    df = df.groupby(["Model", "Scenario", "importer", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()

    outdf = pd.DataFrame(columns=["Model", "Scenario", "Unit", "Year"])
    for f in portfolio:
        tdf = df[df['Fuel'] == f].copy()
        tdf = tdf[['Model', 'Scenario', 'Unit', 'Year', 'Value']]
        tdf.rename(columns={"Value": f"Total Imports ({f})"}, inplace=True)
        outdf = pd.merge(outdf, tdf, on=["Model", "Scenario", "Unit", "Year"], how="outer")
        
    return outdf