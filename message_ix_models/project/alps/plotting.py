"""Reusable plotting utilities for ALPS water analysis.

Provides composable functions for:
- Basin selection and mapping
- Timeseries plotting with uncertainty bands
- Multi-panel comparison layouts
- Representative basin visualization
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional, List, Dict, Tuple


class BasinMapper:
    """Handle basin index to name mapping for MESSAGE R12 basins."""

    def __init__(self, basin_file: Optional[Path] = None):
        """Initialize basin mapper.

        Args:
            basin_file: Path to all_basins.csv. If None, uses default location.
        """
        if basin_file is None:
            basin_file = Path("message_ix_models/data/water/infrastructure/all_basins.csv")

        self.basin_df = pd.read_csv(basin_file)
        self.basin_r12 = self.basin_df[self.basin_df['model_region'] == 'R12'].copy()
        self.basin_r12 = self.basin_r12.reset_index(drop=True)

        # Create mapping: row_index -> basin_name
        self.index_to_name = {idx: row['BASIN']
                             for idx, row in self.basin_r12.iterrows()}
        self.name_to_index = {name: idx
                             for idx, name in self.index_to_name.items()}

    def find_basin_indices(self, basin_names: List[str]) -> List[int]:
        """Find row indices for representative basins.

        Args:
            basin_names: List of basin names to search for

        Returns:
            List of row indices matching basin names
        """
        indices = []

        for basin_name in basin_names:
            found = False
            search_name = basin_name.lower()

            # Handle spelling variations
            if "brahmaputra" in search_name:
                search_name = search_name.replace("brahmaputra", "bramaputra")

            for idx, name in self.index_to_name.items():
                name_lower = name.lower()

                # Bidirectional substring match with word boundary checks for short names
                if search_name == name_lower or (search_name in name_lower or name_lower in search_name):
                    # Extra check for short names to avoid false matches (e.g., "Ob" matching "Gobi")
                    if len(search_name) <= 3:
                        if (search_name == name_lower or
                            name_lower.startswith(search_name + " ") or
                            name_lower.endswith(" " + search_name)):
                            indices.append(idx)
                            print(f"  Matched '{basin_name}' -> row {idx}: '{name}'")
                            found = True
                            break
                    else:
                        indices.append(idx)
                        print(f"  Matched '{basin_name}' -> row {idx}: '{name}'")
                        found = True
                        break

            if not found:
                print(f"  WARNING: Could not find basin '{basin_name}'")

        return indices


def load_representative_basins(basins_file: Path) -> List[str]:
    """Load representative basin names from text file.

    Args:
        basins_file: Path to file with one basin name per line

    Returns:
        List of basin names
    """
    with open(basins_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def filter_years(df: pd.DataFrame,
                 start_year: int = 2020,
                 end_year: int = 2100) -> Tuple[pd.DataFrame, np.ndarray]:
    """Filter DataFrame columns to year range.

    Args:
        df: DataFrame with year columns
        start_year: First year to include
        end_year: Last year to include

    Returns:
        Tuple of (filtered DataFrame, year array)
    """
    all_years = df.columns.astype(int)
    year_mask = (all_years >= start_year) & (all_years <= end_year)
    years = all_years[year_mask]

    return df.iloc[:, year_mask], years


def plot_timeseries_with_bands(ax: plt.Axes,
                               years: np.ndarray,
                               mean: np.ndarray,
                               lower: Optional[np.ndarray] = None,
                               upper: Optional[np.ndarray] = None,
                               label: str = 'E[X]',
                               band_label: str = 'CVaR 10-90',
                               color: str = 'blue',
                               linestyle: str = '-',
                               linewidth: float = 2.5,
                               alpha: float = 0.3,
                               zorder_line: int = 5,
                               zorder_band: int = 2):
    """Plot timeseries with optional uncertainty bands.

    Args:
        ax: Matplotlib axes
        years: Year values for x-axis
        mean: Mean values for y-axis
        lower: Lower bound for uncertainty band (optional)
        upper: Upper bound for uncertainty band (optional)
        label: Label for mean line
        band_label: Label for uncertainty band
        color: Color for line and band
        linestyle: Line style for mean
        linewidth: Width of mean line
        alpha: Transparency of uncertainty band
        zorder_line: Z-order for line
        zorder_band: Z-order for band
    """
    ax.plot(years, mean, linestyle, color=color, linewidth=linewidth,
            label=label, zorder=zorder_line)

    if lower is not None and upper is not None:
        ax.fill_between(years, lower, upper, alpha=alpha, color=color,
                       label=band_label, zorder=zorder_band)


def create_comparison_layout(n_basins: int,
                            n_columns: int = 3,
                            figsize_per_panel: Tuple[float, float] = (6, 3)) -> Tuple[plt.Figure, np.ndarray]:
    """Create figure with grid layout for multi-basin comparison.

    Args:
        n_basins: Number of basins to plot
        n_columns: Number of columns in grid
        figsize_per_panel: (width, height) per panel in inches

    Returns:
        Tuple of (figure, axes array)
    """
    n_rows = int(np.ceil(n_basins / n_columns))
    figsize = (figsize_per_panel[0] * n_columns, figsize_per_panel[1] * n_rows)

    fig, axes = plt.subplots(n_rows, n_columns, figsize=figsize)

    # Flatten axes array for easy indexing
    if n_basins == 1:
        axes = np.array([axes])
    else:
        axes = axes.flatten()

    # Hide unused subplots
    for i in range(n_basins, len(axes)):
        axes[i].axis('off')

    return fig, axes


def create_three_panel_comparison_layout(n_basins: int,
                                        figsize: Tuple[float, float] = (18, 3)) -> Tuple[plt.Figure, np.ndarray]:
    """Create figure with 3-column layout (Method1 | Method2 | Difference).

    Args:
        n_basins: Number of basins (rows)
        figsize: (width, height) per row

    Returns:
        Tuple of (figure, axes array shaped (n_basins, 3))
    """
    fig, axes = plt.subplots(n_basins, 3, figsize=(figsize[0], figsize[1] * n_basins))

    # Handle single basin case
    if n_basins == 1:
        axes = axes.reshape(1, -1)

    return fig, axes


def style_basin_subplot(ax: plt.Axes,
                       title: str,
                       ylabel: str = 'Runoff (km³/yr)',
                       xlabel: str = 'Year',
                       show_xlabel: bool = True,
                       grid: bool = True,
                       legend: bool = True,
                       legend_kwargs: Optional[Dict] = None):
    """Apply consistent styling to basin subplot.

    Args:
        ax: Axes to style
        title: Subplot title
        ylabel: Y-axis label
        xlabel: X-axis label
        show_xlabel: Whether to show x-axis label
        grid: Whether to show grid
        legend: Whether to show legend
        legend_kwargs: Keyword arguments for legend
    """
    ax.set_title(title, fontsize=11, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=10)

    if show_xlabel:
        ax.set_xlabel(xlabel, fontsize=10)

    if grid:
        ax.grid(True, alpha=0.3, linestyle='--')

    if legend:
        if legend_kwargs is None:
            legend_kwargs = {'fontsize': 8, 'loc': 'best'}
        ax.legend(**legend_kwargs)


def add_stats_textbox(ax: plt.Axes,
                     stats_text: str,
                     position: Tuple[float, float] = (0.02, 0.98),
                     va: str = 'top',
                     ha: str = 'left',
                     fontsize: int = 8):
    """Add statistics text box to subplot.

    Args:
        ax: Axes to add text to
        stats_text: Text content
        position: (x, y) position in axes coordinates
        va: Vertical alignment
        ha: Horizontal alignment
        fontsize: Font size
    """
    ax.text(position[0], position[1], stats_text,
           transform=ax.transAxes, fontsize=fontsize,
           verticalalignment=va, horizontalalignment=ha,
           bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))


def plot_basin_comparison_three_panel(
    axes_row: np.ndarray,
    years: np.ndarray,
    basin_name: str,
    method1_data: Dict[str, np.ndarray],
    method2_data: Dict[str, np.ndarray],
    method1_label: str = 'Weighted',
    method2_label: str = 'Unweighted',
    variable_name: str = 'Runoff',
    variable_unit: str = 'km³/yr',
    show_xlabel: bool = False,
    show_uncertainty: bool = True
):
    """Plot three-panel comparison for a single basin.

    Args:
        axes_row: Array of 3 axes [method1, method2, difference]
        years: Year array
        basin_name: Name of basin
        method1_data: Dict with keys 'mean', 'lower', 'upper' (numpy arrays)
        method2_data: Dict with keys 'mean', 'lower', 'upper' (numpy arrays)
        method1_label: Label for first method
        method2_label: Label for second method
        variable_name: Name of variable being plotted
        variable_unit: Unit string for y-axis
        show_xlabel: Whether to show x-axis labels
        show_uncertainty: Whether to show uncertainty bands (default: True)
    """
    # Panel 1: Method 1
    ax1 = axes_row[0]
    plot_timeseries_with_bands(
        ax1, years, method1_data['mean'],
        method1_data.get('lower') if show_uncertainty else None,
        method1_data.get('upper') if show_uncertainty else None,
        label='E[X]' if show_uncertainty else method1_label,
        band_label='CVaR 10-90',
        color='green', linewidth=2.5
    )
    style_basin_subplot(ax1, f'{basin_name} - {method1_label}',
                       ylabel=f'{variable_name} ({variable_unit})',
                       show_xlabel=show_xlabel)

    # Panel 2: Method 2
    ax2 = axes_row[1]
    plot_timeseries_with_bands(
        ax2, years, method2_data['mean'],
        method2_data.get('lower') if show_uncertainty else None,
        method2_data.get('upper') if show_uncertainty else None,
        label='E[X]' if show_uncertainty else method2_label,
        band_label='CVaR 10-90',
        color='blue', linewidth=2.5
    )
    style_basin_subplot(ax2, f'{basin_name} - {method2_label}',
                       ylabel=f'{variable_name} ({variable_unit})',
                       show_xlabel=show_xlabel)

    # Panel 3: Difference (method2 - method1, so baseline more wet = positive)
    ax3 = axes_row[2]
    diff_mean = method2_data['mean'] - method1_data['mean']
    if show_uncertainty:
        diff_lower = method2_data.get('lower', method2_data['mean']) - method1_data.get('lower', method1_data['mean'])
        diff_upper = method2_data.get('upper', method2_data['mean']) - method1_data.get('upper', method1_data['mean'])
    else:
        diff_lower = None
        diff_upper = None

    plot_timeseries_with_bands(
        ax3, years, diff_mean, diff_lower, diff_upper,
        label='Δ' if not show_uncertainty else 'ΔE[X]',
        band_label='ΔCVaR 10-90',
        color='gray', linewidth=2.5
    )
    ax3.axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.7)
    style_basin_subplot(ax3, f'{basin_name} - Difference ({method2_label} - {method1_label})',
                       ylabel=f'Difference ({variable_unit})',
                       show_xlabel=show_xlabel)

    # Add stats to difference plot
    mean_diff = np.mean(diff_mean)
    stats_text = f'Mean Δ: {mean_diff:.1f} {variable_unit}\n+ = {method2_label.lower()} over, - = {method2_label.lower()} under'
    add_stats_textbox(ax3, stats_text)


def plot_gmt_timeseries(ax: plt.Axes,
                       years: np.ndarray,
                       weighted_gmt: np.ndarray,
                       unweighted_gmt: np.ndarray,
                       percentile_10: Optional[np.ndarray] = None,
                       percentile_90: Optional[np.ndarray] = None,
                       title: str = 'Global Mean Temperature: Weighted vs Unweighted'):
    """Plot GMT timeseries comparison.

    Args:
        ax: Axes to plot on
        years: Year array
        weighted_gmt: Weighted GMT timeseries
        unweighted_gmt: Unweighted GMT timeseries
        percentile_10: 10th percentile band (optional)
        percentile_90: 90th percentile band (optional)
        title: Plot title
    """
    ax.plot(years, weighted_gmt, 'g-', linewidth=2.5, label='Weighted E[GMT]', zorder=5)
    ax.plot(years, unweighted_gmt, 'b--', linewidth=2, label='Unweighted E[GMT]', zorder=4)

    if percentile_10 is not None and percentile_90 is not None:
        ax.fill_between(years, percentile_10, percentile_90,
                       alpha=0.2, color='gray', label='10th-90th percentile', zorder=1)

    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('GSAT Anomaly (K)', fontsize=11)
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(fontsize=10, loc='upper left')

    # Add stats text
    mean_diff = np.mean(weighted_gmt - unweighted_gmt)
    add_stats_textbox(ax,
                     f'Mean difference: {mean_diff:.3f} K\n+ = weighted warmer',
                     position=(0.98, 0.02), va='bottom', ha='right')


def plot_gmt_difference(ax: plt.Axes,
                       years: np.ndarray,
                       weighted_gmt: np.ndarray,
                       unweighted_gmt: np.ndarray,
                       title: str = 'GMT Difference: Weighted - Unweighted'):
    """Plot GMT difference timeseries.

    Args:
        ax: Axes to plot on
        years: Year array
        weighted_gmt: Weighted GMT timeseries
        unweighted_gmt: Unweighted GMT timeseries
        title: Plot title
    """
    diff_gmt = weighted_gmt - unweighted_gmt

    ax.plot(years, diff_gmt, 'k-', linewidth=2.5, label='Δ GMT (Weighted - Unweighted)', zorder=5)
    ax.axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.7)
    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('Temperature Difference (K)', fontsize=11)
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(fontsize=10, loc='best')

    # Add stats text
    mean_diff = np.mean(diff_gmt)
    stats_text = f'Mean Δ: {mean_diff:.3f} K\nMax Δ: {np.max(diff_gmt):.3f} K\nMin Δ: {np.min(diff_gmt):.3f} K'
    add_stats_textbox(ax, stats_text, position=(0.98, 0.98), va='top', ha='right')


def plot_r12_regional_comparison(
    expectation_df: pd.DataFrame,
    cvar10_df: pd.DataFrame,
    cvar90_df: pd.DataFrame,
    variable_name: str = 'Capacity Factor',
    variable_unit: str = '%',
    start_year: int = 2020,
    end_year: int = 2100,
    title_suffix: str = ''
) -> Tuple[plt.Figure, np.ndarray]:
    """Plot R12 regional data in 2×6 layout.

    Args:
        expectation_df: DataFrame with expectation values (12 rows × year columns)
        cvar10_df: DataFrame with CVaR 10% values
        cvar90_df: DataFrame with CVaR 90% values
        variable_name: Name of variable being plotted
        variable_unit: Unit string for y-axis
        start_year: First year to plot
        end_year: Last year to plot
        title_suffix: Optional suffix for title

    Returns:
        Tuple of (figure, axes array)
    """
    # R12 region names in order
    r12_regions = ['AFR', 'CHN', 'EEU', 'FSU', 'LAM', 'MEA',
                   'NAM', 'PAO', 'PAS', 'RCPA', 'SAS', 'WEU']

    # Filter years
    year_cols = [str(y) for y in range(start_year, end_year + 1) if str(y) in expectation_df.columns]
    years = np.array([int(y) for y in year_cols])

    exp_filtered = expectation_df[year_cols].values
    c10_filtered = cvar10_df[year_cols].values
    c90_filtered = cvar90_df[year_cols].values

    # Create figure with 2 rows × 6 columns
    fig, axes = plt.subplots(2, 6, figsize=(24, 8))
    fig.suptitle(f'R12 Regional {variable_name}{title_suffix}', fontsize=16, fontweight='bold', y=0.995)

    # Plot each region
    for i, region in enumerate(r12_regions):
        row = i // 6
        col = i % 6
        ax = axes[row, col]

        # Extract data for this region
        mean = exp_filtered[i, :]
        lower = c10_filtered[i, :]
        upper = c90_filtered[i, :]

        # Plot timeseries with uncertainty band
        plot_timeseries_with_bands(
            ax, years, mean, lower, upper,
            label='E[X]', band_label='CVaR 10-90',
            color='steelblue', linewidth=2
        )

        # Style subplot
        show_xlabel = (row == 1)  # Only bottom row shows x-label
        ylabel = f'{variable_name} ({variable_unit})' if col == 0 else ''  # Only left column shows y-label

        style_basin_subplot(ax, region, ylabel=ylabel, show_xlabel=show_xlabel)

    plt.tight_layout()
    return fig, axes
