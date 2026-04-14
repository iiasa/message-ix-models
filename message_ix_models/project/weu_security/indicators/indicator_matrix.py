# Combine legacy reporting output for development of energy security matrix

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import numpy as np
import yaml
import matplotlib.colors as mcolors

from message_ix_models.project.weu_security.indicators.regional_hhi import calculate_hhi
from message_ix_models.project.weu_security.indicators.total_imports import calculate_total_imports
from message_ix_models.project.weu_security.indicators.import_dependence import calculate_import_dependence

# Import configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)

# Import and combine legacy report outputs
tracked_scenarios = ['SSP2', 'SSP2_NAM30EJ', 'SSP2_MEACON_1.0',
                     'FSU2100', 'FSU2100_MEACON_1.0', 'FSU2100_NAM30EJ',
                     'INDC2030', 'INDC2030_NAM30EJ', 'INDC2030_MEACON_1.0',
                     'INDC2030_FSU2100', 'INDC2030_FSU2100_MEACON_1.0', 'INDC2030_FSU2100_NAM30EJ']

base_df = pd.DataFrame()
for scenario in tracked_scenarios:
    indf = pd.read_csv(package_data_path('weu_security', 'reporting', 'output', f'weu_security_{scenario}.csv'))
    base_df = pd.concat([base_df, indf])

# Make long
year_cols = ['2030', '2035', '2040', '2045', '2050', '2055', '2060',
             '2070', '2080', '2090', '2100', '2110']
base_df = base_df.melt(
    id_vars=['Model', 'Scenario', 'Variable', 'Unit', 'Region'],
    value_vars=year_cols,
    var_name='Year',
    value_name='Value'
)
base_df['Year'] = base_df['Year'].astype(int)

# Region names need to be updated
base_df['Region'] = np.where(base_df['Region'].str.contains('>') == False, "R12_" + base_df['Region'], base_df['Region'])

# Set up portfolio
matrix_portfolio = ["Biomass", "Coal", "Gas", "Oil", "Ethanol", "Fuel Oil", "Light Oil"]

# Total imports
total_imports_df = calculate_total_imports(input_data=base_df,
                                           portfolio = matrix_portfolio,
                                           region = ["R12_WEU", "R12_EEU"],
                                           region_name = "Europe")
# Import dependence
pe_import_dependence = calculate_import_dependence(input_data=base_df,
                                                   region = ["R12_WEU", "R12_EEU"],
                                                   region_name = "Europe",
                                                   portfolio = ["Biomass", "Coal", "Gas", "Oil"],
                                                   portfolio_level = "Primary Energy")

se_import_dependence = calculate_import_dependence(input_data=base_df,
                                                   region = ["R12_WEU", "R12_EEU"],
                                                   region_name = "Europe",
                                                   portfolio = ["Ethanol", "Fuel Oil", "Light Oil"],
                                                   portfolio_level = "Secondary Energy")                                                  
# Import HHI by fuel
hhi_df = calculate_hhi(input_data=base_df,
                       trade_type = "Imports",
                       portfolio = matrix_portfolio,
                       region = ["R12_WEU", "R12_EEU"],
                       region_name = "Europe")

# Combine and filter
fulldf = pd.merge(total_imports_df, pe_import_dependence, on=["Model", "Scenario", "Unit", "Year"], how="left")
fulldf = pd.merge(fulldf, se_import_dependence, on=["Model", "Scenario", "Unit", "Year"], how="left")
fulldf = pd.merge(fulldf, hhi_df, on=["Model", "Scenario", "Unit", "Year"], how="left")

# Reshape long
id_vars = ['Model', 'Scenario', 'Unit', 'Year']
value_vars = [c for c in fulldf.columns if c not in id_vars]

fulldf = fulldf.melt(
    id_vars=id_vars,
    value_vars=value_vars,
    var_name='Indicator',
    value_name='Value'
)

# Columns by scenario
outdf = pd.DataFrame(columns=['Year','Indicator'])
for scenario in tracked_scenarios:
    tdf = fulldf[fulldf['Scenario'] == scenario].copy()
    tdf = tdf.drop(columns=['Scenario'])
    tdf.rename(columns={'Value': f'{scenario}'}, inplace=True)
    tdf = tdf[['Year', 'Indicator', f'{scenario}']]
    outdf = outdf.merge(tdf, on=['Year', 'Indicator'], how='outer')

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def plot_heat_matrix(
    df: pd.DataFrame,
    year: int,
    indicators: list[str],
    scenario_order: list[str],
    title: str | None = None,
    cmap: str = "YlOrRd",
    figsize: tuple = (10, 6),
    fmt: str = ".2f",
    annot: bool = True,
) -> plt.Figure:
    """
    Plot a heat matrix for a given year.
      - Rows    : scenarios (ordered by scenario_order)
      - Columns : indicators (ordered by indicators)
      - Color   : scaled independently per indicator column

    Parameters
    ----------
    df             : DataFrame with columns [Year, Indicator, <scenario_col>, ...]
    year           : The year to filter on.
    indicators     : Ordered list of indicator names (become columns).
    scenario_order : Ordered list of scenario column names (become rows).
    title          : Optional plot title. Defaults to "Heat Matrix — {year}".
    cmap           : Matplotlib colormap name.
    figsize        : Figure size tuple (width, height).
    fmt            : Number format string for cell annotations (e.g. ".2f", ".0%").
    annot          : Whether to annotate cells with values.

    Returns
    -------
    fig : matplotlib Figure
    """
    # --- Validate inputs -------------------------------------------------------
    missing_scenarios = [s for s in scenario_order if s not in df.columns]
    if missing_scenarios:
        raise ValueError(f"Scenario columns not found in DataFrame: {missing_scenarios}")

    missing_indicators = [i for i in indicators if i not in df["Indicator"].values]
    if missing_indicators:
        raise ValueError(f"Indicators not found in DataFrame: {missing_indicators}")

    # --- Filter & pivot --------------------------------------------------------
    mask = (df["Year"] == year) & (df["Indicator"].isin(indicators))
    subset = df.loc[mask].copy()

    if subset.empty:
        raise ValueError(f"No data found for year={year} and the given indicators.")

    # Pivot: rows = scenarios, columns = indicators
    matrix = (
        subset
        .set_index("Indicator")[scenario_order]  # select scenario columns
        .reindex(indicators)                      # order indicators
        .T                                        # transpose → scenarios as rows
        .reindex(scenario_order)                  # order scenario rows
    )

    data = matrix.values.astype(float)
    n_scenarios, n_indicators = data.shape

    # --- Normalize per indicator column ----------------------------------------
    norm_data = np.zeros_like(data)
    for j in range(n_indicators):
        col = data[:, j]
        col_min, col_max = np.nanmin(col), np.nanmax(col)
        if col_max > col_min:
            norm_data[:, j] = (col - col_min) / (col_max - col_min)
        else:
            norm_data[:, j] = 0.5  # all values identical → mid-color

    # --- Plot ------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=figsize)

    cmap_obj = plt.get_cmap(cmap)
    ax.imshow(norm_data, cmap=cmap_obj, aspect="auto", vmin=0, vmax=1)

    # --- Axis labels -----------------------------------------------------------
    ax.set_xticks(range(n_indicators))
    ax.set_xticklabels(
        indicators,
        fontsize=11,
        rotation=45,
        ha="left",
        rotation_mode="anchor",
    )
    ax.set_yticks(range(n_scenarios))
    ax.set_yticklabels(scenario_order, fontsize=11)
    ax.xaxis.set_label_position("top")
    ax.xaxis.tick_top()

    # --- Cell annotations ------------------------------------------------------
    if annot:
        for r in range(n_scenarios):
            for c in range(n_indicators):
                val = data[r, c]
                if np.isnan(val):
                    text, color = "N/A", "gray"
                else:
                    text = format(val, fmt)
                    color = "white" if norm_data[r, c] > 0.6 else "black"
                ax.text(c, r, text, ha="center", va="center", fontsize=10, color=color)

    # --- Grid lines ------------------------------------------------------------
    ax.set_xticks(np.arange(-0.5, n_indicators, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, n_scenarios, 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=1.5)
    ax.tick_params(which="minor", length=0)

    # --- Title & layout --------------------------------------------------------
    ax.set_title(title or f"Heat Matrix — {year}", fontsize=13, pad=14, loc="left")
    fig.tight_layout()

    return fig

matrix_indicators = ['Energy Demand (Primary Energy)', 'Energy Demand (Oil)', 'Energy Demand (Gas)',
                     'Net Imports (Primary Energy)', 'Net Imports (Oil)', 'Net Imports (Gas)',
                     'Net Import Dependence (Primary Energy)', 'Net Import Dependence (Oil)', 'Net Import Dependence (Gas)',
                     'HHI (Oil)', 'HHI (Gas)']

plot_heat_matrix(
    df = outdf,
    year = 2060,
    indicators = matrix_indicators,
    scenario_order = tracked_scenarios,
    title = "Fuel Security Indicators — 2060",
    cmap = "coolwarm",
    figsize = (15, 6),
    fmt = ".2f",
    annot = True,
)
plt.show()