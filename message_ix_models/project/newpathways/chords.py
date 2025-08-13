import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from ipywidgets import interact, widgets
import math

def create_chord_diagram(df, selected_commodity=None, selected_year=None, top_n=15):
    """
    Create an interactive chord diagram from trade data
    
    Parameters:
    df: pandas DataFrame with columns ['exporter', 'importer', 'value', 'commodity', 'year']
    selected_commodity: str, filter for specific commodity
    selected_year: int, filter for specific year
    top_n: int, number of top countries to include
    """
    
    # Filter data
    filtered_df = df.copy()
    if selected_commodity:
        filtered_df = filtered_df[filtered_df['commodity'] == selected_commodity]
    if selected_year:
        filtered_df = filtered_df[filtered_df['year'] == selected_year]
    
    # Aggregate values by exporter-importer pairs
    trade_matrix = filtered_df.groupby(['exporter', 'importer'])['value'].sum().reset_index()
    
    # Get top countries by total trade volume
    country_totals = pd.concat([
        trade_matrix.groupby('exporter')['value'].sum().rename('total'),
        trade_matrix.groupby('importer')['value'].sum().rename('total')
    ]).groupby(level=0).sum().sort_values(ascending=False)
    
    top_countries = country_totals.head(top_n).index.tolist()
    
    # Filter matrix to include only top countries
    trade_matrix = trade_matrix[
        (trade_matrix['exporter'].isin(top_countries)) & 
        (trade_matrix['importer'].isin(top_countries))
    ]
    
    # Create adjacency matrix
    countries = sorted(list(set(trade_matrix['exporter'].tolist() + trade_matrix['importer'].tolist())))
    n_countries = len(countries)
    
    # Create mapping
    country_to_idx = {country: i for i, country in enumerate(countries)}
    
    # Initialize matrix
    matrix = np.zeros((n_countries, n_countries))
    
    # Fill matrix
    for _, row in trade_matrix.iterrows():
        exp_idx = country_to_idx[row['exporter']]
        imp_idx = country_to_idx[row['importer']]
        matrix[exp_idx][imp_idx] = row['value']
    
    # Calculate arc segments for each country based on individual trade flows
    country_arc_segments = {}
    total_all_trade = matrix.sum()
    
    for i, country in enumerate(countries):
        segments = []
        
        # Export segments - each destination gets a segment proportional to export value
        exports = []
        for j, partner in enumerate(countries):
            if i != j and matrix[i][j] > 0:
                exports.append({
                    'partner': partner,
                    'value': matrix[i][j],
                    'type': 'export',
                    'color_idx': i
                })
        
        # Import segments - each source gets a segment proportional to import value  
        imports = []
        for j, partner in enumerate(countries):
            if i != j and matrix[j][i] > 0:
                imports.append({
                    'partner': partner,
                    'value': matrix[j][i], 
                    'type': 'import',
                    'color_idx': j  # Use source country's color
                })
        
        # Calculate total trade for this country to determine overall arc size
        total_exports = sum([e['value'] for e in exports])
        total_imports = sum([i['value'] for i in imports])
        total_country_trade = total_exports + total_imports
        
        # Overall arc width proportional to total trade
        if total_country_trade > 0:
            arc_width = (total_country_trade / total_all_trade) * 2 * np.pi
            arc_width = max(0.1, arc_width)  # Minimum visibility
        else:
            arc_width = 0.1
            
        country_arc_segments[country] = {
            'exports': exports,
            'imports': imports, 
            'total_exports': total_exports,
            'total_imports': total_imports,
            'arc_width': arc_width,
            'segments': exports + imports
        }
    
    # Position countries around circle with gaps
    current_angle = 0
    gap_size = 0.05  # Small gap between countries
    
    for country in countries:
        arc_info = country_arc_segments[country]
        arc_info['start_angle'] = current_angle
        arc_info['end_angle'] = current_angle + arc_info['arc_width']
        current_angle += arc_info['arc_width'] + gap_size
    
    # Scale to fill full circle if needed
    total_used = current_angle - gap_size
    if total_used < 2 * np.pi:
        scale_factor = (2 * np.pi - len(countries) * gap_size) / (total_used - len(countries) * gap_size)
        current_angle = 0
        for country in countries:
            arc_info = country_arc_segments[country]
            scaled_width = arc_info['arc_width'] * scale_factor
            arc_info['start_angle'] = current_angle
            arc_info['end_angle'] = current_angle + scaled_width
            arc_info['arc_width'] = scaled_width
            current_angle += scaled_width + gap_size
    
    # Create traces for the chord diagram
    fig = go.Figure()
    
    # Create chord diagram with variable-width arcs
    colors = px.colors.qualitative.Set3[:n_countries]
    
    # Store connection points for chords
    connection_points = {}
    
    for i, country in enumerate(countries):
        arc_info = country_arc_segments[country]
        start_angle = arc_info['start_angle']
        end_angle = arc_info['end_angle']
        
        # Create individual segments within each country's arc
        current_seg_angle = start_angle
        connection_points[country] = {'exports': {}, 'imports': {}}
        
        # First do exports (first part of arc)
        if arc_info['total_exports'] > 0:
            for export in arc_info['exports']:
                partner = export['partner']
                value = export['value']
                # Segment width proportional to this specific trade flow
                seg_width = (value / (arc_info['total_exports'] + arc_info['total_imports'])) * arc_info['arc_width']
                seg_end = current_seg_angle + seg_width
                
                # Store connection point (middle of segment)
                connection_points[country]['exports'][partner] = current_seg_angle + seg_width/2
                
                # Create arc segment with varying width
                n_points = max(10, int(seg_width * 100))  # More points for wider segments
                seg_angles = np.linspace(current_seg_angle, seg_end, n_points)
                
                # Variable width - thicker for larger values
                max_width = 0.3  # Maximum distance from circle
                min_width = 0.05  # Minimum width for visibility
                segment_thickness = min_width + (value / matrix.max()) * (max_width - min_width)
                
                # Inner and outer curves
                inner_radius = 1.0
                outer_radius = 1.0 + segment_thickness
                
                inner_x = inner_radius * np.cos(seg_angles)
                inner_y = inner_radius * np.sin(seg_angles)
                outer_x = outer_radius * np.cos(seg_angles)
                outer_y = outer_radius * np.sin(seg_angles)
                
                # Create filled area for the segment
                fill_x = np.concatenate([inner_x, outer_x[::-1], [inner_x[0]]])
                fill_y = np.concatenate([inner_y, outer_y[::-1], [inner_y[0]]])
                
                fig.add_trace(go.Scatter(
                    x=fill_x, y=fill_y,
                    fill='toself',
                    fillcolor=colors[i],
                    line=dict(color=colors[i], width=1),
                    mode='lines',
                    showlegend=False,
                    hoverinfo='text',
                    hovertext=f'{country} → {partner}<br>Export Value: {value:,.0f}'
                ))
                
                current_seg_angle = seg_end
        
        # Then do imports (second part of arc)
        if arc_info['total_imports'] > 0:
            for imp in arc_info['imports']:
                partner = imp['partner']
                value = imp['value']
                color_idx = imp['color_idx']
                # Segment width proportional to this specific trade flow
                seg_width = (value / (arc_info['total_exports'] + arc_info['total_imports'])) * arc_info['arc_width']
                seg_end = current_seg_angle + seg_width
                
                # Store connection point
                connection_points[country]['imports'][partner] = current_seg_angle + seg_width/2
                
                # Create arc segment
                n_points = max(10, int(seg_width * 100))
                seg_angles = np.linspace(current_seg_angle, seg_end, n_points)
                
                # Variable width based on import value
                max_width = 0.3
                min_width = 0.05
                segment_thickness = min_width + (value / matrix.max()) * (max_width - min_width)
                
                inner_radius = 1.0
                outer_radius = 1.0 + segment_thickness
                
                inner_x = inner_radius * np.cos(seg_angles)
                inner_y = inner_radius * np.sin(seg_angles)
                outer_x = outer_radius * np.cos(seg_angles)
                outer_y = outer_radius * np.sin(seg_angles)
                
                fill_x = np.concatenate([inner_x, outer_x[::-1], [inner_x[0]]])
                fill_y = np.concatenate([inner_y, outer_y[::-1], [inner_y[0]]])
                
                # Use source country's color but with transparency for imports
                fig.add_trace(go.Scatter(
                    x=fill_x, y=fill_y,
                    fill='toself',
                    fillcolor=colors[color_idx],
                    line=dict(color=colors[color_idx], width=1),
                    mode='lines',
                    showlegend=False,
                    hoverinfo='text',
                    hovertext=f'{partner} → {country}<br>Import Value: {value:,.0f}'))
                
                current_seg_angle = seg_end
        
        # Add country label
        label_angle = (start_angle + end_angle) / 2
        x_pos = 1.4 * np.cos(label_angle)
        y_pos = 1.4 * np.sin(label_angle)
        
        fig.add_trace(go.Scatter(
            x=[x_pos], y=[y_pos],
            mode='text',
            text=[country],
            textfont=dict(size=12, color='black'),
            showlegend=False,
            hoverinfo='skip'
        ))
    
    # Add chords connecting the segments
    for exporter in countries:
        if exporter in connection_points:
            for partner, export_angle in connection_points[exporter]['exports'].items():
                if partner in connection_points and exporter in connection_points[partner]['imports']:
                    import_angle = connection_points[partner]['imports'][exporter]
                    
                    # Get trade value for chord thickness
                    exp_idx = country_to_idx[exporter]
                    imp_idx = country_to_idx[partner]
                    trade_value = matrix[exp_idx][imp_idx]
                    
                    # Create bezier curve for chord
                    start_x, start_y = np.cos(export_angle), np.sin(export_angle)
                    end_x, end_y = np.cos(import_angle), np.sin(import_angle)
                    
                    # Bezier curve through center
                    t = np.linspace(0, 1, 50)
                    chord_x = (1-t)**2 * start_x + 2*(1-t)*t * 0 + t**2 * end_x
                    chord_y = (1-t)**2 * start_y + 2*(1-t)*t * 0 + t**2 * end_y
                    
                    # Chord thickness based on trade value
                    line_width = max(0.5, (trade_value / matrix.max()) * 15)
                    
                    exporter_idx = country_to_idx[exporter]
                    fig.add_trace(go.Scatter(
                        x=chord_x, y=chord_y,
                        mode='lines',
                        line=dict(
                            color=colors[exporter_idx],
                            width=line_width
                        ),
                        showlegend=False,
                        hoverinfo='text',
                        hovertext=f'{exporter} → {partner}<br>Value: {trade_value:,.0f}'
                    ))
    
    # Update layout
    fig.update_layout(
        title=f'Trade Flow Chord Diagram<br>{"Commodity: " + str(selected_commodity) if selected_commodity else "All Commodities"}<br>{"Year: " + str(selected_year) if selected_year else "All Years"}',
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-1.5, 1.5]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-1.5, 1.5]),
        plot_bgcolor='white',
        width=1200,
        height=1200,
        showlegend=False
    )
    
    return fig

def save_chord_to_html(df, filename='chord_diagram.html', selected_commodity=None, selected_year=None, top_n=15):
    """
    Save chord diagram to HTML file
    
    Parameters:
    df: pandas DataFrame with trade data
    filename: str, output HTML filename
    selected_commodity: str, filter for specific commodity
    selected_year: int, filter for specific year
    top_n: int, number of top countries to include
    """
    fig = create_chord_diagram(df, selected_commodity, selected_year, top_n)
    fig.write_html(filename, include_plotlyjs='cdn')
    print(f"Chord diagram saved to {filename}")
    return filename

def create_interactive_html_dashboard(df, filename='interactive_chord_dashboard.html'):
    """
    Create an interactive HTML dashboard with multiple chord diagrams
    """
    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
    
    # Get unique values
    commodities = sorted(df['commodity'].unique().tolist())
    years = sorted(df['year'].unique().tolist())
    
    # Create subplot structure
    n_commodities = min(4, len(commodities))  # Limit to 4 for readability
    n_years = min(2, len(years))
    
    fig = make_subplots(
        rows=n_years, cols=n_commodities,
        subplot_titles=[f"{commodities[i]} ({years[j]})" 
                       for j in range(n_years) for i in range(n_commodities)],
        specs=[[{"type": "scatter"} for _ in range(n_commodities)] for _ in range(n_years)]
    )
    
    # Add individual chord diagrams
    for i, year in enumerate(years[:n_years]):
        for j, commodity in enumerate(commodities[:n_commodities]):
            # Create mini chord diagram
            chord_fig = create_chord_diagram(df, commodity, year, top_n=10)
            
            # Extract traces from chord diagram
            for trace in chord_fig.data:
                fig.add_trace(trace, row=i+1, col=j+1)
    
    # Update layout
    fig.update_layout(
        title="Interactive Trade Flow Dashboard",
        height=1000 * n_years,
        width=1000 * n_commodities,
        showlegend=False
    )
    
    # Save to HTML
    fig.write_html(filename, include_plotlyjs='cdn')
    print(f"Interactive dashboard saved to {filename}")
    return filename

def create_interactive_chord_widget(df):
    """
    Create interactive widget for chord diagram (for Jupyter notebooks)
    """
    # Get unique values for filters
    commodities = ['All'] + sorted(df['commodity'].unique().tolist())
    years = ['All'] + sorted(df['year'].unique().tolist())
    
    @interact(
        commodity=widgets.Dropdown(options=commodities, value='All', description='Commodity:'),
        year=widgets.Dropdown(options=years, value='All', description='Year:'),
        top_n=widgets.IntSlider(min=5, max=30, value=15, description='Top N Countries:')
    )
    def update_chord(commodity, year, top_n):
        selected_commodity = None if commodity == 'All' else commodity
        selected_year = None if year == 'All' else year
        
        fig = create_chord_diagram(df, selected_commodity, selected_year, top_n)
        fig.show()

# Example usage and sample data generator
def generate_sample_data():
    """Generate sample trade data for demonstration"""
    np.random.seed(42)
    
    countries = ['USA', 'China', 'Germany', 'Japan', 'UK', 'France', 'India', 'Italy', 
                'Brazil', 'Canada', 'Russia', 'Mexico', 'Spain', 'Australia', 'Netherlands']
    commodities = ['Electronics', 'Machinery', 'Textiles', 'Agriculture', 'Chemicals', 'Metals']
    years = [2020, 2021, 2022, 2023]
    
    data = []
    for year in years:
        for commodity in commodities:
            for exporter in countries:
                for importer in countries:
                    if exporter != importer and np.random.random() > 0.7:  # 30% chance of trade
                        value = np.random.exponential(1000) * np.random.uniform(0.1, 10)
                        data.append({
                            'exporter': exporter,
                            'importer': importer,
                            'value': value,
                            'commodity': commodity,
                            'year': year
                        })
    
    return pd.DataFrame(data)

# Main execution
sankeydf = sankeydf.rename(columns = {'fuel': 'commodity'})

save_chord_to_html(sankeydf,
                   os.path.join(data_path, "diagnostics", "chord.html"),
                   selected_commodity = 'Shipped LNG',
                   selected_year = 2030)


