# -*- coding: utf-8 -*-
"""
Base region map (R12)
"""
import json
import requests
import folium 
import pandas as pd

from message_ix_models.util import package_data_path

def base_regions_map():
    map = folium.Map([40, 180],
                    zoom_start=2,
                    tiles='cartodbpositron')

    # Add R12 regions
    base_regions = pd.read_csv(package_data_path('alps_hhi', 'maps', 'r12_crosswalk.csv'))
    iso_to_region = dict(zip(base_regions['country_ISO'], base_regions['R12_region']))

    # Get unique regions and assign colors
    regions = sorted(base_regions['R12_region'].unique())
    colors = ['#8dd3c7', '#ffffb3', '#bebada', '#fb8072', '#80b1d3', '#fdb462',
            '#b3de69', '#fccde5', '#d9d9d9', '#bc80bd', '#ccebc5', '#ffed6f']
    region_colors = {region: colors[i % len(colors)] for i, region in enumerate(regions)}

    # Download world geometries (GeoJSON)
    url = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson'
    world_geo = requests.get(url).json()

    # Add GeoJson layer with custom styling
    def style_function(feature):
        props = feature['properties']
        # Try multiple possible ISO code fields
        iso_code = props.get('ISO3166-1-Alpha-3')
        region = iso_to_region.get(iso_code, 'Unknown')
        color = region_colors.get(region, 'lightgray')
        
        return {
            'fillColor': color,
            'color': 'black',
            'weight': 0.5,
            'fillOpacity': 0.5
        }

    # Add the GeoJson layer
    folium.GeoJson(
        world_geo,
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['name', 'ISO3166-1-Alpha-3'],
            aliases=['Country:', 'ISO Code:'],
            localize=True
        )
    ).add_to(map)

    # Add a custom legend
    legend_html = '''
    <div style="position: fixed; 
        top: 10px; right: 10px; width: 120px; 
        background-color: white; z-index:9999; font-size:12px;
        border:2px solid grey; border-radius: 5px; padding: 10px;
        max-height: 90vh; overflow-y: auto;">
        <p style="margin:0 0 10px 0; font-weight: bold; font-size:14px;">R12 Regions</p>
    '''

    for region in regions:
        color = region_colors[region]
        legend_html += f'''<p style="margin:5px 0;">
                        <span style="background-color:{color}; 
                        width:20px; height:15px; display:inline-block; 
                        border:1px solid black; margin-right:5px;"></span>
                        {region} </p>'''

    legend_html += '</div>'
    map.get_root().html.add_child(folium.Element(legend_html))

    return map