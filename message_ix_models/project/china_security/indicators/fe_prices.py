"""
Extract prices
"""
import pandas as pd
import numpy as np
import os
import pyam

# Function to run HHI by fuel and trade type
def fe_prices(input_data:pd.DataFrame,
              region:str,
              price_dict: {"Price|Secondary Energy|Electricity": "Electricity",
                           "Price|Primary Energy|Gas": "Gas",
                           "Price|Primary Energy|Oil": "Oil",
                           "Price|Primary Energy|Coal": "Coal"}) -> pd.DataFrame:
    """
    Extract the final energy prices for a given region
    Args:
        input_data: pandas dataframe with trade data (full)
        region: region to include
    """

    # Filter data
    df = input_data.copy()
    df = df[df['Region'] == region]
    df = df[df['Variable'].isin(price_dict.keys())]
    df = df.rename(columns={"Value": "Price ($/GJ)"})
    df["Fuel"] = df["Variable"].map(price_dict)
    
    df = df[['Model', 'Scenario', 'Year', 'Fuel', 'Price ($/GJ)']]

    return df
