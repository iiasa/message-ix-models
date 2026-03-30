"""
Calculate HHI by fuel and trade type
"""
import pandas as pd
import numpy as np
import ixmp
import os
import pyam

# Function to run HHI by fuel and trade type
def calculate_hhi(input_data:pd.DataFrame,
                  trade_type:str,
                  portfolio:str,
                  region:list[str],
                  region_name:str,
                  use_units:str = "EJ/yr",
                  total_hhi:bool = False,) -> pd.DataFrame:
    """
    Calculate the HHI by fuel and trade type for a given region

    Args:
        input_data: pandas dataframe with trade data (full)
        use_units: physical (EJ/yr) or monetary (billion USD_2010/yr)
        trade_type: Exports or Imports
        portfolio: portfolio of fuels (e.g. "Trade|Primary Energy|Oil [Volume]")
        region: list of regions to include
        region_name: name of the region
        use_units: physical (EJ/yr) or monetary (billion USD_2010/yr)
        total_hhi: whether to calculate the total HHI
    """

    # Filter data
    df = input_data.copy()
    df = df[df['Unit'] == use_units]
    
    df = df[df['Variable'].str.split("|").str[0] == "Trade"]
    df = df[df['Variable'].str.contains('|'.join(portfolio))]

    df['Fuel'] = ''
    if total_hhi == False:
        for p in portfolio:
            df['Fuel'] = np.where(df['Variable'].str.contains(p), p, df['Fuel'])
    elif total_hhi == True:
        df['Fuel'] = 'Total'

    df['exporter'] = df['Region'].str.split(">").str[0]
    df['importer'] = df['Region'].str.split(">").str[1]
    
    if trade_type == "Exports":
        df = df[df['exporter'].isin(region)]
        df['exporter'] = region_name
    elif trade_type == "Imports":
        df = df[df['importer'].isin(region)]
        df['importer'] = region_name

    # Sum to fuel level
    df = df.groupby(["Model", "Scenario", "exporter", "importer", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()

    df_total= df.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()
    df_total.rename(columns={"Value": "Total"}, inplace=True)
    df = df.merge(df_total, on=["Model", "Scenario", "Fuel", "Unit", "Year"], how="left")

    df_hhi = df.copy()
    df_hhi["HHI"] = (df_hhi["Value"] / df_hhi["Total"]) ** 2
    df_hhi = df_hhi.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["HHI"].sum().reset_index()

    outdf = pd.DataFrame(columns=["Model", "Scenario", "Unit", "Year"])
    for f in df_hhi['Fuel'].unique():
        tdf = df_hhi[df_hhi['Fuel'] == f].copy()
        tdf.rename(columns={"HHI": f"HHI ({f})"}, inplace=True)
        tdf = tdf.drop(columns=['Fuel'])
        outdf = outdf.merge(tdf, on=["Model", "Scenario", "Unit", "Year"], how="outer")
        
    return outdf