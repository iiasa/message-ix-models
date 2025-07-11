import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np

class InteractiveSankey:
    def __init__(self, df):
        """
        Initialize the InteractiveSankey with a pandas DataFrame.
        
        Expected columns: year, exporter, importer, fuel, value
        """
        self.df = df.copy()
        self.filtered_df = df.copy()
        
        # Validate required columns
        required_cols = ['year', 'exporter', 'importer', 'fuel', 'value']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
    
    def filter_data(self, year=None, fuel=None):
        """Filter the data based on year and/or fuel."""
        self.filtered_df = self.df.copy()
        
        if year is not None and year != 'all':
            self.filtered_df = self.filtered_df[self.filtered_df['year'] == year]
        
        if fuel is not None and fuel != 'all':
            self.filtered_df = self.filtered_df[self.filtered_df['fuel'] == fuel]
        
        return self.filtered_df
    
    def prepare_sankey_data(self):
        """Prepare data for Sankey diagram."""
        # Aggregate data by exporter-importer-fuel
        agg_df = self.filtered_df.groupby(['exporter', 'importer', 'fuel'])['value'].sum().reset_index()
        
        # Create unique nodes
        exporters = agg_df['exporter'].unique()
        importers = agg_df['importer'].unique()
        all_countries = list(set(list(exporters) + list(importers)))
        
        # Create node mapping
        node_map = {country: idx for idx, country in enumerate(all_countries)}
        
        # Prepare links
        source = [node_map[exp] for exp in agg_df['exporter']]
        target = [node_map[imp] for imp in agg_df['importer']]
        value = agg_df['value'].tolist()
        
        # Create colors for links based on fuel
        tech_colors = px.colors.qualitative.Vivid
        tech_list = agg_df['importer'].unique()
        tech_color_map = {tech: tech_colors[i % len(tech_colors)] for i, tech in enumerate(tech_list)}
        
        link_colors = [tech_color_map[tech] for tech in agg_df['importer']]
        
        # Create hover text for links
        hover_text = [
            f"<b>{exp} → {imp}</b><br>" +
            f"fuel: {tech}<br>" +
            f"Value: {val:,.0f} GWa"
            for exp, imp, tech, val in zip(agg_df['exporter'], agg_df['importer'], 
                                         agg_df['fuel'], agg_df['value'])
        ]
        
        return {
            'nodes': all_countries,
            'source': source,
            'target': target,
            'value': value,
            'link_colors': link_colors,
            'hover_text': hover_text,
            'tech_data': agg_df
        }
    
    def create_sankey(self, title=None, width=1000, height=600):
        """Create the Sankey diagram."""
        sankey_data = self.prepare_sankey_data()
        
        # Create figure
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=sankey_data['nodes'],
                color="lightblue"
            ),
            link=dict(
                source=sankey_data['source'],
                target=sankey_data['target'],
                value=sankey_data['value'],
                color=[color.replace('rgb', 'rgba').replace(')', ', 0.6)') for color in sankey_data['link_colors']],
                hovertemplate='%{customdata}<extra></extra>',
                customdata=sankey_data['hover_text']
            )
        )])
        
        # Update layout
        if title is None:
            title = f"Trade Flow Analysis ({len(self.filtered_df)} flows)"
        
        fig.update_layout(
            title_text=title,
            font_size=12,
            width=width,
            height=height,
            paper_bgcolor='white',
            plot_bgcolor='white'
        )
        
        return fig
    
    def create_dashboard(self):
        """Create a complete dashboard with interactive filters and statistics."""
        # Get unique values for filters
        years = ['All'] + sorted([str(year) for year in self.df['year'].unique()])
        technologies = ['All'] + sorted(self.df['fuel'].unique())
        
        # Create initial filtered data
        current_data = self.df.copy()
        
        # Create the main Sankey figure
        sankey_data = self._prepare_sankey_data_for_df(current_data)
        
        # Create Sankey figure
        fig = go.Figure()
        
        # Add Sankey diagram
        fig.add_trace(
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=sankey_data['nodes'],
                    color="lightblue"
                ),
                link=dict(
                    source=sankey_data['source'],
                    target=sankey_data['target'],
                    value=sankey_data['value'],
                    color=[color.replace('rgb', 'rgba').replace(')', ', 0.6)') for color in sankey_data['link_colors']],
                    hovertemplate='%{customdata}<extra></extra>',
                    customdata=sankey_data['hover_text']
                )
            )
        )
        
        # Create dropdown buttons for filters
        dropdown_buttons = []
        
        # Create buttons for each year-fuel combination
        for year in years:
            for tech in technologies:
                # Apply filters
                if year == 'All' and tech == 'All':
                    filtered_data = self.df.copy()
                elif year == 'All':
                    filtered_data = self.df[self.df['fuel'] == tech]
                elif tech == 'All':
                    filtered_data = self.df[self.df['year'] == int(year)]
                else:
                    filtered_data = self.df[(self.df['year'] == int(year)) & (self.df['fuel'] == tech)]
                
                # Prepare Sankey data for this combination
                combo_sankey_data = self._prepare_sankey_data_for_df(filtered_data)
                
                # Create button label
                if year == 'All' and tech == 'All':
                    label = "All Data"
                elif year == 'All':
                    label = f"All Years - {tech}"
                elif tech == 'All':
                    label = f"{year} - All Technologies"
                else:
                    label = f"{year} - {tech}"
                
                # Add button
                dropdown_buttons.append(
                    dict(
                        label=label,
                        method="restyle",
                        args=[{
                            "node.label": [combo_sankey_data['nodes']],
                            "link.source": [combo_sankey_data['source']],
                            "link.target": [combo_sankey_data['target']],
                            "link.value": [combo_sankey_data['value']],
                            "link.color": [[color.replace('rgb', 'rgba').replace(')', ', 0.6)') for color in combo_sankey_data['link_colors']]],
                            "link.customdata": [combo_sankey_data['hover_text']]
                        }]
                    )
                )
        
        # Update layout with dropdown menu
        fig.update_layout(
            title_text="Interactive Trade Flow Dashboard",
            font_size=12,
            width=1200,
            height=700,
            updatemenus=[
                dict(
                    buttons=dropdown_buttons,
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.1,
                    xanchor="left",
                    y=1.02,
                    yanchor="top",
                    type="dropdown"
                )
            ],
            annotations=[
                dict(text="Filter:", showarrow=False, x=0.05, y=1.02, yref="paper", align="left")
            ]
        )
        
        return fig
    
    def create_multi_chart_dashboard(self):
        """Create a dashboard with multiple separate charts (alternative approach)."""
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go
        
        # Create subplots for non-Sankey charts
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('fuel Distribution', 'Top Exporters by Value',
                          'Top Importers by Value', 'Summary Statistics'),
            specs=[
                [{"type": "pie"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "table"}]
            ],
            vertical_spacing=0.1,
            horizontal_spacing=0.1
        )
        
        # Add fuel pie chart
        tech_summary = self.filtered_df.groupby('fuel')['value'].sum().reset_index()
        if not tech_summary.empty:
            fig.add_trace(
                go.Pie(
                    labels=tech_summary['fuel'],
                    values=tech_summary['value'],
                    name="fuel Distribution"
                ),
                row=1, col=1
            )
        
        # Add top exporters bar chart
        exp_summary = self.filtered_df.groupby('exporter')['value'].sum().reset_index().sort_values('value', ascending=False).head(10)
        if not exp_summary.empty:
            fig.add_trace(
                go.Bar(
                    x=exp_summary['exporter'],
                    y=exp_summary['value'],
                    name="Top Exporters",
                    marker_color='lightcoral'
                ),
                row=1, col=2
            )
        
        # Add top importers bar chart
        imp_summary = self.filtered_df.groupby('importer')['value'].sum().reset_index().sort_values('value', ascending=False).head(10)
        if not imp_summary.empty:
            fig.add_trace(
                go.Bar(
                    x=imp_summary['importer'],
                    y=imp_summary['value'],
                    name="Top Importers",
                    marker_color='lightgreen'
                ),
                row=2, col=1
            )
        
        # Add summary statistics table
        stats = self._get_summary_stats_for_df(self.filtered_df)
        fig.add_trace(
            go.Table(
                header=dict(values=['Metric', 'Value']),
                cells=dict(values=[
                    ['Total Value', 'Total Flows', 'Countries', 'Exporters', 'Importers', 'Technologies'],
                    [f"{stats['total_value']:,.0f} GWa", stats['total_flows'], stats['unique_countries'], 
                     stats['unique_exporters'], stats['unique_importers'], stats['unique_technologies']]
                ])
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text="Trade Flow Analytics Dashboard",
            showlegend=False,
            height=800,
            width=1200
        )
        
        return fig
    
    def _prepare_sankey_data_for_df(self, df):
        """Helper method to prepare Sankey data for a specific DataFrame."""
        if df.empty:
            return {
                'nodes': ['No Data'],
                'source': [],
                'target': [],
                'value': [],
                'link_colors': [],
                'hover_text': []
            }
        
        # Aggregate data by exporter-importer-fuel
        agg_df = df.groupby(['exporter', 'importer', 'fuel'])['value'].sum().reset_index()
        
        # Create unique nodes
        exporters = agg_df['exporter'].unique()
        importers = agg_df['importer'].unique()
        all_countries = list(set(list(exporters) + list(importers)))
        
        # Create node mapping
        node_map = {country: idx for idx, country in enumerate(all_countries)}
        
        # Prepare links
        source = [node_map[exp] for exp in agg_df['exporter']]
        target = [node_map[imp] for imp in agg_df['importer']]
        value = agg_df['value'].tolist()
        
        # Create colors for links based on fuel
        tech_colors = px.colors.qualitative.Vivid
        tech_list = agg_df['importer'].unique()
        tech_color_map = {tech: tech_colors[i % len(tech_colors)] for i, tech in enumerate(tech_list)}
        
        link_colors = [tech_color_map[tech] for tech in agg_df['importer']]
        
        # Create hover text for links
        hover_text = [
            f"<b>{exp} → {imp}</b><br>" +
            f"fuel: {tech}<br>" +
            f"Value: {val:,.0f} GWa"
            for exp, imp, tech, val in zip(agg_df['exporter'], agg_df['importer'], 
                                         agg_df['fuel'], agg_df['value'])
        ]
        
        return {
            'nodes': all_countries,
            'source': source,
            'target': target,
            'value': value,
            'link_colors': link_colors,
            'hover_text': hover_text
        }
    
    def _get_summary_stats_for_df(self, df):
        """Helper method to get summary statistics for a specific DataFrame."""
        if df.empty:
            return {
                'total_value': 0,
                'total_flows': 0,
                'unique_exporters': 0,
                'unique_importers': 0,
                'unique_technologies': 0,
                'unique_countries': 0
            }
        
        return {
            'total_value': df['value'].sum(),
            'total_flows': len(df),
            'unique_exporters': df['exporter'].nunique(),
            'unique_importers': df['importer'].nunique(),
            'unique_technologies': df['fuel'].nunique(),
            'unique_countries': len(set(df['exporter'].unique().tolist() + 
                                     df['importer'].unique().tolist()))
        }
    
    def get_summary_stats(self):
        """Get summary statistics for the filtered data."""
        return {
            'total_value': self.filtered_df['value'].sum(),
            'total_flows': len(self.filtered_df),
            'unique_exporters': self.filtered_df['exporter'].nunique(),
            'unique_importers': self.filtered_df['importer'].nunique(),
            'unique_technologies': self.filtered_df['fuel'].nunique(),
            'unique_countries': len(set(self.filtered_df['exporter'].unique().tolist() + 
                                     self.filtered_df['importer'].unique().tolist()))
        }
    
    def print_summary(self):
        """Print a summary of the current filtered data."""
        stats = self.get_summary_stats()
        print(f"=== Trade Flow Summary ===")
        print(f"Total Value: {stats['total_value']:,.0f}GWa")
        print(f"Total Flows: {stats['total_flows']}")
        print(f"Unique Countries: {stats['unique_countries']}")
        print(f"Unique Exporters: {stats['unique_exporters']}")
        print(f"Unique Importers: {stats['unique_importers']}")
        print(f"Unique Technologies: {stats['unique_technologies']}")

