"""
Calculate energy expenditures share of GDP and energy intensity of GDP
"""
import pandas as pd
import numpy as np
import os
import pyam

# Function to run HHI by fuel and trade type
def exp_over_gdp(input_data:pd.DataFrame,
                 region:str) -> pd.DataFrame:
    """
    Calculate the energy expenditures share of GDP and energy intensity of GDP for a given region
    Args:
        input_data: pandas dataframe with trade data (full)
        region: region to include
    """

    # Filter data
    df = input_data.copy()
    df = df[df['Region'] == region]

    df_cost = df[df['Variable'] == "Cost|Cost Nodal Net"]
    df_cost = df_cost.rename(columns={"Value": "Cost Nodal Net ($B2010)"})
    df_cost = df_cost[['Model', 'Scenario', 'Year', 'Cost Nodal Net ($B2010)']]

    df_gdp = df[df['Variable'] == "GDP|PPP"]
    df_gdp = df_gdp.rename(columns={"Value": "GDP ($B2010)"})
    df_gdp = df_gdp[['Model', 'Scenario', 'Year', 'GDP ($B2010)']]

    df_pe = df[df['Variable'] == "Primary Energy"]
    df_pe = df_pe.rename(columns={"Value": "Primary Energy (EJ)"})
    df_pe = df_pe[['Model', 'Scenario', 'Year', 'Primary Energy (EJ)']]

    df_out = df_cost.merge(df_gdp, on=["Model", "Scenario", "Year"], how="outer")
    df_out['Energy Expenditures Share of GDP'] = df_out['Cost Nodal Net ($B2010)'] / df_out['GDP ($B2010)']

    df_out = df_out.merge(df_pe, on=["Model", "Scenario", "Year"], how="outer")
    df_out['Energy Intensity of GDP (MJ/USD2010)'] = (df_out['Primary Energy (EJ)'] * 1e12) / (df_out['GDP ($B2010)']*1e9)

    return df_out