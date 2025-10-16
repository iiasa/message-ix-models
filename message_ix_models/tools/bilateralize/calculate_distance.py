"""
Calculate distances between pairs of ports
"""
import pandas as pd
import numpy as np
from itertools import combinations
import math
from message_ix_models.util import package_data_path
import os

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees) using the Haversine formula.
    Returns distance in kilometers.
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r

def calculate_port_distances(df):
    """
    Read CSV file with port data and calculate distances between all port combinations.
    
    Parameters:
    csv_file_path (str): Path to the CSV file containing Port, Latitude, Longitude columns
    
    Returns:
    pandas.DataFrame: DataFrame with columns 'Port1', 'Port2', 'Distance_km'
    """  
    # Check if required columns exist
    required_columns = ['Port', 'Latitude', 'Longitude']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Remove rows with missing coordinates
    df_clean = df.dropna(subset=['Latitude', 'Longitude'])
    
    if df_clean.empty:
        raise ValueError("No valid coordinate data found in the file")
    
    print(f"Loaded {len(df_clean)} ports with valid coordinates")
    
    # Calculate distances between all port combinations
    distances = []
    
    # Get all combinations of ports (without repetition)
    port_combinations = list(combinations(df_clean.index, 2))
    
    print(f"Calculating distances for {len(port_combinations)} port pairs...")
    
    for i, j in port_combinations:
        port1 = df_clean.iloc[i]
        port2 = df_clean.iloc[j]
        
        distance = haversine_distance(
            port1['Latitude'], port1['Longitude'],
            port2['Latitude'], port2['Longitude']
        )
        
        distances.append({
            'Port1': port1['Port'],
            'Port2': port2['Port'],
            'Distance_km': round(distance, 2)
        })
    
    # Create DataFrame with results
    outdf1 = pd.DataFrame(distances)
    
    # Concatenate other direction too
    outdf2 = outdf1.copy()
    outdf2 = outdf2.rename(columns = {'Port1': 'Port2',
                                      'Port2': 'Port1'})
    outdf = pd.concat([outdf1, outdf2])
    return outdf
        
def calculate_distance(regional_specification):
    '''
    Run distance calculation.
    
    Inputs:
        regional_specification: MESSAGE regional specification (e.g., "R12")
    Outputs:
        CSV file in data/bilateralize/distances/ that includes the 
        distances for regional specification
    '''
    # Specify the path to CSV file
    csv_path = os.path.abspath(os.path.join(os.path.dirname(package_data_path("bilateralize")),
                                            "bilateralize", "distances"))
    
    infile = pd.read_excel(os.path.join(csv_path, 'distances.xlsx'), 
                           sheet_name = 'node_ports')    
    infile = infile[infile['Regionalization'] == regional_specification]
    
    # Calculate distances
    df = calculate_port_distances(infile)
    
    # Add regions back
    for i in ['1', '2']:
        df = df.merge(infile[['Node', 'Port']], 
                      left_on = 'Port' + i, right_on = 'Port', how = 'left')
        df = df.rename(columns = {'Node': 'Node' + i})
    df = df[['Node1', 'Port1', 'Node2', 'Port2', 'Distance_km']]
        
    df.to_csv(os.path.join(csv_path, regional_specification + "_distances.csv"),
              index = False)
    
if __name__ == "__main__":
    result = calculate_distance()