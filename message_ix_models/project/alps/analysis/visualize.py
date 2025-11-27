"""Basin map visualization utilities.

Creates choropleth maps of MESSAGE basin data using R12 basin shapefiles.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# Default shapefile path
DEFAULT_SHAPEFILE = Path.home() / "ISIMIP/basins_delineated/basins_by_region_simpl_R12.shp"


def parse_message_basin_code(code: str) -> tuple[int, str]:
    """Parse MESSAGE basin code into basin_id and region.

    Parameters
    ----------
    code : str
        MESSAGE basin code (e.g., 'B107|AFR')

    Returns
    -------
    tuple[int, str]
        (basin_id, region) e.g., (107, 'AFR')
    """
    parts = code.split("|")
    basin_id = int(parts[0][1:])  # Remove 'B' prefix
    region = parts[1]
    return basin_id, region


def load_basin_shapefile(shapefile_path: Optional[Path] = None):
    """Load basin shapefile with geopandas.

    Parameters
    ----------
    shapefile_path : Path, optional
        Path to shapefile. Uses DEFAULT_SHAPEFILE if not provided.

    Returns
    -------
    GeoDataFrame
        Basin geometries with BASIN_ID and REGION columns
    """
    import geopandas as gpd

    if shapefile_path is None:
        shapefile_path = DEFAULT_SHAPEFILE

    gdf = gpd.read_file(shapefile_path)
    return gdf


def join_data_to_basins(
    data: pd.DataFrame,
    gdf,
    value_col: str = "value",
) -> "gpd.GeoDataFrame":
    """Join tabular data to basin geometries.

    Parameters
    ----------
    data : pd.DataFrame
        Data with 'basin' column containing MESSAGE codes (e.g., 'B107|AFR')
        and a value column to plot
    gdf : GeoDataFrame
        Basin geometries from load_basin_shapefile()
    value_col : str
        Name of the value column in data

    Returns
    -------
    GeoDataFrame
        Basin geometries with joined data values
    """
    # Parse MESSAGE codes
    parsed = data["basin"].apply(parse_message_basin_code)
    data = data.copy()
    data["basin_id"] = [p[0] for p in parsed]
    data["region"] = [p[1] for p in parsed]

    # Aggregate if multiple entries per basin-region (take mean)
    agg_data = data.groupby(["basin_id", "region"])[value_col].mean().reset_index()

    # Join to shapefile
    merged = gdf.merge(
        agg_data,
        left_on=["BASIN_ID", "REGION"],
        right_on=["basin_id", "region"],
        how="left",
    )

    return merged


def plot_basin_map(
    gdf,
    value_col: str,
    title: str,
    cmap: str = "RdYlBu",
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    figsize: tuple = (14, 8),
    ax=None,
    cbar_label: Optional[str] = None,
    diverging: bool = True,
) -> tuple:
    """Plot choropleth map of basin values.

    Parameters
    ----------
    gdf : GeoDataFrame
        Basin geometries with value column
    value_col : str
        Column name to plot
    title : str
        Plot title
    cmap : str
        Matplotlib colormap name
    vmin, vmax : float, optional
        Color scale limits. If None and diverging=True, centers at 0.
    figsize : tuple
        Figure size
    ax : matplotlib Axes, optional
        Axes to plot on. Creates new figure if None.
    cbar_label : str, optional
        Colorbar label
    diverging : bool
        If True, center colormap at 0

    Returns
    -------
    tuple
        (fig, ax) matplotlib figure and axes
    """
    if ax is None:
        fig, ax = plt.subplots(1, 1, figsize=figsize)
    else:
        fig = ax.figure

    # Handle color scale for diverging data
    if diverging and vmin is None and vmax is None:
        max_abs = gdf[value_col].abs().max()
        vmin, vmax = -max_abs, max_abs

    # Plot basins with data
    gdf.plot(
        column=value_col,
        cmap=cmap,
        linewidth=0.3,
        edgecolor="0.5",
        legend=True,
        legend_kwds={
            "label": cbar_label or value_col,
            "orientation": "horizontal",
            "shrink": 0.6,
            "pad": 0.05,
        },
        ax=ax,
        vmin=vmin,
        vmax=vmax,
        missing_kwds={"color": "lightgrey", "label": "No data"},
    )

    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_axis_off()

    return fig, ax


def create_basin_comparison_maps(
    costs_data: Optional[pd.DataFrame],
    sw_data: Optional[pd.DataFrame],
    gw_data: Optional[pd.DataFrame],
    output_path: Path,
    shapefile_path: Optional[Path] = None,
    year: Optional[int] = None,
    scenario_label: str = "",
):
    """Create 3-panel comparison map: costs, surfacewater, groundwater.

    Parameters
    ----------
    costs_data : pd.DataFrame, optional
        Costs by basin (node column with MESSAGE codes, year columns)
    sw_data : pd.DataFrame, optional
        Surfacewater by basin
    gw_data : pd.DataFrame, optional
        Groundwater by basin
    output_path : Path
        Directory for output files
    shapefile_path : Path, optional
        Path to basin shapefile
    year : int, optional
        Year to plot. If None, uses mean across years.
    scenario_label : str
        Label for output filename
    """
    gdf = load_basin_shapefile(shapefile_path)

    # Determine which panels to create
    panels = []
    if costs_data is not None:
        panels.append(("costs", costs_data, "Costs (M USD)", "RdYlBu_r", False))
    if sw_data is not None:
        panels.append(("surfacewater", sw_data, "Surfacewater (MCM)", "RdYlBu", True))
    if gw_data is not None:
        panels.append(("groundwater", gw_data, "Groundwater (MCM)", "RdYlBu", True))

    if not panels:
        print("No data provided for plotting")
        return

    fig, axes = plt.subplots(1, len(panels), figsize=(6 * len(panels), 6))
    if len(panels) == 1:
        axes = [axes]

    for ax, (name, data, label, cmap, diverging) in zip(axes, panels):
        # Prepare data for plotting
        if isinstance(data.index, pd.Index) and "node" not in data.columns:
            # Wide format with node as index
            df = data.reset_index()
            df = df.rename(columns={"index": "node"}) if "index" in df.columns else df
        else:
            df = data.copy()

        # Get value for specified year or mean
        year_cols = [c for c in df.columns if isinstance(c, (int, float)) or (isinstance(c, str) and c.isdigit())]

        if year is not None and str(year) in [str(c) for c in year_cols]:
            year_col = [c for c in year_cols if str(c) == str(year)][0]
            plot_df = pd.DataFrame({
                "basin": df["node"] if "node" in df.columns else df.iloc[:, 0],
                "value": df[year_col],
            })
            title = f"{label} ({year})"
        else:
            # Mean across years
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            plot_df = pd.DataFrame({
                "basin": df["node"] if "node" in df.columns else df.iloc[:, 0],
                "value": df[numeric_cols].mean(axis=1),
            })
            title = f"{label} (mean)"

        # Join and plot
        merged = join_data_to_basins(plot_df, gdf, "value")
        plot_basin_map(
            merged,
            "value",
            title,
            cmap=cmap,
            ax=ax,
            cbar_label=label,
            diverging=diverging,
        )

    plt.tight_layout()

    # Save figure
    suffix = f"_{scenario_label}" if scenario_label else ""
    suffix += f"_{year}" if year else "_mean"
    outfile = output_path / f"basin_maps{suffix}.png"
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved: {outfile}")
    return outfile


def plot_metric_map(
    df: pd.DataFrame,
    output_path: Path,
    basin_col: str = "basin",
    value_col: str = "value",
    title: str = "Metric Map",
    cbar_label: str = "",
    filename: str = "metric_map.png",
    cmap: str = "RdYlBu",
    diverging: bool = True,
    shapefile_path: Optional[Path] = None,
    mask_col: Optional[str] = None,
    mask_threshold: Optional[float] = None,
    mask_op: str = "gt",
):
    """Plot choropleth map of any basin-level metric.

    Parameters
    ----------
    df : pd.DataFrame
        Data with basin codes and values
    output_path : Path
        Directory for output
    basin_col : str
        Column name containing MESSAGE basin codes (e.g., 'B107|AFR')
    value_col : str
        Column name containing values to plot
    title : str
        Plot title
    cbar_label : str
        Colorbar label
    filename : str
        Output filename
    cmap : str
        Matplotlib colormap
    diverging : bool
        If True, center colormap at 0
    shapefile_path : Path, optional
        Path to basin shapefile
    mask_col : str, optional
        Column to use for masking (e.g., 'p_value')
    mask_threshold : float, optional
        Threshold for masking
    mask_op : str
        'gt' to mask where mask_col > threshold, 'lt' for <

    Returns
    -------
    Path
        Path to saved figure
    """
    gdf = load_basin_shapefile(shapefile_path)

    # Prepare data with optional masking
    plot_df = df[[basin_col, value_col]].copy()
    plot_df.columns = ["basin", "value"]

    if mask_col is not None and mask_threshold is not None:
        if mask_op == "gt":
            mask = df[mask_col] > mask_threshold
        else:
            mask = df[mask_col] < mask_threshold
        plot_df.loc[mask, "value"] = np.nan
        n_masked = mask.sum()
    else:
        n_masked = 0

    merged = join_data_to_basins(plot_df, gdf, "value")

    fig, ax = plot_basin_map(
        merged,
        "value",
        title,
        cmap=cmap,
        cbar_label=cbar_label or value_col,
        diverging=diverging,
    )

    # Add masking note if applicable
    if n_masked > 0:
        op_str = ">" if mask_op == "gt" else "<"
        ax.text(
            0.02, 0.02,
            f"Grey: {mask_col} {op_str} {mask_threshold} ({n_masked} basins)",
            transform=ax.transAxes,
            fontsize=8,
            color="grey",
        )

    outfile = output_path / filename
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved: {outfile}")
    return outfile


def plot_monotonicity_map(
    monotonicity_df: pd.DataFrame,
    output_path: Path,
    variable: str,
    shapefile_path: Optional[Path] = None,
):
    """Plot map of Spearman rho values from monotonicity validation."""
    return plot_metric_map(
        monotonicity_df,
        output_path,
        basin_col="basin",
        value_col="spearman_rho",
        title=f"Monotonicity (Spearman rho): {variable}",
        cbar_label="Spearman rho",
        filename=f"monotonicity_map_{variable}.png",
        cmap="RdYlBu",
        diverging=True,
        shapefile_path=shapefile_path,
    )


def plot_sensitivity_map(
    sensitivity_df: pd.DataFrame,
    output_path: Path,
    variable: str,
    metric: str = "slope",
    shapefile_path: Optional[Path] = None,
    mask_insignificant: bool = True,
    p_threshold: float = 0.05,
    subtitle: Optional[str] = None,
):
    """Plot map of sensitivity metrics (slope, R², etc.)."""
    # Variable display names
    var_names = {
        "rime_qtot_mean": "Total Runoff",
        "qtot_mean": "Total Runoff",
        "rime_qr": "Groundwater Recharge",
        "qr": "Groundwater Recharge",
        "surfacewater": "Surface Water",
        "groundwater": "Groundwater",
        "costs": "Nodal Costs",
    }
    var_display = var_names.get(variable, variable)

    # Variable short names for colorbar
    var_short = {
        "rime_qtot_mean": "qtot",
        "qtot_mean": "qtot",
        "rime_qr": "qr",
        "qr": "qr",
        "surfacewater": "SW",
        "groundwater": "GW",
        "costs": "cost",
    }
    var_abbrev = var_short.get(variable, variable)

    # Auto-detect source for subtitle
    is_rime = variable.startswith("rime_") or variable in ("qtot_mean", "qr")
    is_message_cid = variable in ("surfacewater", "groundwater", "costs")
    if subtitle is None:
        if is_rime:
            subtitle = "RIME emulation of CWatM"
        elif is_message_cid:
            subtitle = "MESSAGE CID scenarios"

    # Configure based on metric
    if metric == "slope":
        cbar_label = f"d({var_abbrev})/d(GWL) [MCM/K]"
        cmap = "BrBG"
        diverging = True
    elif metric == "r_squared":
        cbar_label = "R²"
        cmap = "viridis"
        diverging = False
    else:
        cbar_label = metric
        cmap = "plasma"
        diverging = False

    # Build title with optional subtitle
    title = f"Sensitivity: GWL vs {var_display}"
    if subtitle:
        title = f"{title}\n{subtitle}"

    suffix = f"_p{str(p_threshold).replace('.', '')}" if mask_insignificant else ""

    return plot_metric_map(
        sensitivity_df,
        output_path,
        basin_col="basin",
        value_col=metric,
        title=title,
        cbar_label=cbar_label,
        filename=f"sensitivity_{metric}_{variable}{suffix}.png",
        cmap=cmap,
        diverging=diverging,
        shapefile_path=shapefile_path,
        mask_col="p_value" if mask_insignificant else None,
        mask_threshold=p_threshold if mask_insignificant else None,
        mask_op="gt",
    )
