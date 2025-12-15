import pandas as pd
import plotly.express as px
from message_ix_models.util import package_data_path
from itertools import combinations
from scgraph.geographs.marnet import marnet_geograph

# Read in port data
ports = pd.read_excel(package_data_path('bilateralize', 'distances', 'distances.xlsx'), sheet_name='node_ports')
ports = ports[ports['Regionalization'] == 'R12']

# Remove rows with missing coordinates
ports_clean = ports.dropna(subset=["Latitude", "Longitude"])

if ports_clean.empty:
    raise ValueError("No valid coordinate data found in the file")

# Get all combinations of ports (without repetition)
port_combinations = list(combinations(ports_clean.index, 2))

# Calculate distances between all port combinations
distances = []
for i, j in port_combinations:
        port1 = ports_clean.iloc[i]
        port2 = ports_clean.iloc[j]

        distance = marnet_geograph.get_shortest_path(
            origin_node = {"latitude": port1["Latitude"], 
                           "longitude": port1["Longitude"]},
            destination_node = {"latitude": port2["Latitude"], 
                                "longitude": port2["Longitude"]},
        )

        distances.append(
            {
                "Port1": port1["Port"],
                "Port2": port2["Port"],
                "Distance_km": distance['length'],
                "Path": distance['coordinate_path']
            }
        )

# Create DataFrame with results
ports = pd.DataFrame(distances)

# Map maritime paths
import folium

# Folium does not wrap coordinates around the globe, so we need to adjust the path accordingly
# Essentially we create 5 differnt paths that offset the original path by 0, 360, -360, 720, -720 degrees
# This allows the illusion that the path wraps around the globe with folium
def adjustArcPath(path):
    for index in range(1, len(path)):
        x = path[index][1]
        prevX = path[index - 1][1]
        path[index][1] = x - (round((x - prevX)/360,0) * 360)
    return path

def modifyArcPathLong(points, amount):
    return [[i[0], i[1]+amount] for i in points]

def getCleanArcPath(path):
    path = adjustArcPath(path)
    return [
        path,
        modifyArcPathLong(path, 360),
        modifyArcPathLong(path, -360),
        modifyArcPathLong(path, 720),
        modifyArcPathLong(path, -720)
    ]

# Create a folium map
map = folium.Map([40, 180], zoom_start=2)
# Populate it with the path
folium.PolyLine(
    getCleanArcPath(ports['Path'].iloc[0]),
    color='green',
    weight=5,
    opacity=0.5,
    popup='Length (KM): ' + str(ports['Distance_km'].iloc[0])
).add_to(map)
map




# Read the crosswalk data
df = pd.read_csv(package_data_path('alps_hhi', 'maps', 'r12_crosswalk.csv'))

# Create a choropleth map with regions outlined
fig = px.choropleth(
    df,
    locations='country_ISO',
    color='R12_region',
    locationmode='ISO-3',
    title='R12 Regions Map',
    color_discrete_sequence=px.colors.qualitative.Set3,
    labels={'R12_region': 'Region'}
)

# Update layout for better visualization
fig.update_layout(
    geo=dict(
        showframe=True,
        showcoastlines=True,
        projection_type='natural earth',
        coastlinecolor='darkgray',
        showcountries=True,
        countrycolor='white',
        countrywidth=0.5
    ),
    height=600,
    margin=dict(l=0, r=0, t=50, b=0)
)

# Update traces to add region outlines
fig.update_traces(
    marker_line_color='black',
    marker_line_width=1.5
)

fig.show()

# Print region summary
print("\nRegion Summary:")
print(df['R12_region'].value_counts().sort_index())
print(f"\nTotal countries: {len(df)}")
print(f"Total regions: {df['R12_region'].nunique()}")