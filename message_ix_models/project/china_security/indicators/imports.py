"""
Calculate total imports
"""
import pandas as pd
import numpy as np
import ixmp
import os
import pyam

# Function to run HHI by fuel and trade type
def import_indicators(input_data:pd.DataFrame,
                      region:str,
                      portfolio:list[str],
                      portfolio_level:str,
                      use_units:str = "EJ/yr",) -> pd.DataFrame:
    """
    Calculate the total imports for a given region

    Args:
        input_data: pandas dataframe with trade data (full)
        portfolio: list of fuels (e.g. ["Oil", "Gas"])
        region: list of regions to include
        use_units: physical (EJ/yr) or monetary (billion USD_2010/yr)
    """

    # Filter data
    df = input_data.copy()
    df = df[df['Unit'] == use_units]
    
    df = df[df['Variable'].str.split("|").str[0] == "Trade"]
    df = df[df['Variable'].str.contains('|'.join(portfolio))]
    df = df[df['Variable'].str.contains(portfolio_level)]

    df['Fuel'] = ''
    for p in portfolio:
        df['Fuel'] = np.where(df['Variable'].str.contains(f"\|{p} Volume"), p, df['Fuel'])

    df['exporter'] = df['Region'].str.split(">").str[0]
    df['importer'] = df['Region'].str.split(">").str[1]
    
    imp_df = df[(df['importer'] == region) & (df['exporter'] != region)]
    exp_df = df[(df['exporter'] == region) & (df['importer'] != region)]

    # Sum to fuel level
    imp_df = imp_df.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()
    imp_df = imp_df.rename(columns={"Value": "Total Imports"})
    exp_df = exp_df.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()
    exp_df = exp_df.rename(columns={"Value": "Total Exports"})

    df = imp_df.merge(exp_df, on=["Model", "Scenario", "Fuel", "Unit", "Year"], how="outer")
    df['Total Imports'] = df['Total Imports'].fillna(0)
    df['Total Exports'] = df['Total Exports'].fillna(0)
    df['Net Imports'] = df['Total Imports'] - df['Total Exports']

    # Primary or secondary energy demand (inlcuding solar and wind)
    pdf = input_data.copy()
    pdf = pdf[pdf['Unit'] == use_units]

    pdf = pdf[pdf['Variable'].str.split("|").str[0] == portfolio_level]
    pdf = pdf[pdf['Variable'].str.split("|").str.len() == 2]
    pdf = pdf[pdf['Region'] == region]

    pdf['Fuel'] = pdf['Variable'].str.split("|").str[1]

    pdf = pdf.groupby(["Model", "Scenario", "Fuel", "Unit", "Year"])["Value"].sum().reset_index()
    pdf = pdf.rename(columns={"Value": "Energy Demand"})

    # Import dependence
    df = df.merge(pdf, on=["Model", "Scenario", "Fuel", "Unit", "Year"], how="left")
    df['Net Import Dependence'] = df['Net Imports'] / df['Energy Demand']
    df = df[['Model', 'Scenario', 'Fuel', 'Unit', 'Year', 'Net Imports', 'Net Import Dependence', "Energy Demand"]]

    # Calculate total import dependence (including all fuels)
    df_total = df.groupby(["Model", "Scenario", "Unit", "Year"])["Net Imports"].sum().reset_index()
    pdf_total = pdf.groupby(["Model", "Scenario", "Unit", "Year"])["Energy Demand"].sum().reset_index()

    df_total = df_total.merge(pdf_total, on=["Model", "Scenario", "Unit", "Year"], how="left")
    df_total['Net Import Dependence'] = df_total['Net Imports'] / df_total['Energy Demand']
    df_total = df_total[['Model', 'Scenario', 'Unit', 'Year', 'Net Imports', 'Net Import Dependence', "Energy Demand"]]
    df_total['Fuel'] = f'{portfolio_level}'
    df_total = df_total[['Model', 'Scenario', 'Fuel', 'Unit', 'Year', 'Net Imports', 'Net Import Dependence', "Energy Demand"]]

    df = pd.concat([df, df_total])

    return df