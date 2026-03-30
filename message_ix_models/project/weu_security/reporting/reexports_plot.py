# Line graph of changes to FSU exports

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import numpy as np
import yaml
from itertools import cycle

# Import libraries for plotting
import matplotlib.ticker as mticker
from matplotlib.patches import Patch


# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors'] 
region_labels = config['region_labels']

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
from typing import Optional


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
from typing import Optional


def plot_export_changes(
    df: pd.DataFrame,
    exporter: str,
    scenario_pairs: list[tuple[str, str]],
    fuel_types: Optional[list[str]] = None,
    value_col: str = "value",
    figsize_per_panel: tuple[float, float] = (4.0, 3.5),
    palette: Optional[dict] = None,
    title: Optional[str] = None,
    value_label: str = "Export Change",
    year_col: str = "year",
    scenario_col: str = "scenario",
    fuel_col: str = "fuel_type",
    exporter_col: str = "exporter",
    importer_col: str = "importer",
) -> plt.Figure:
    """
    Produce a paneled figure of stacked bar charts showing the change in
    exports between each (scenario, reference_scenario) pair for a given exporter.

    Layout
    ------
    - Rows    : one per (scenario, reference_scenario) pair in `scenario_pairs`
    - Columns : one per fuel type (auto-detected or supplied via `fuel_types`)
    - Each panel : stacked bar chart, x = year, stacks coloured by importer,
                   values = export(scenario) − export(reference_scenario)

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: model, scenario, year, fuel_type,
        exporter, importer, value  (names overridable via kwargs).
    exporter : str
        The exporter region/country to analyse.
    scenario_pairs : list of (scenario, reference_scenario) tuples
        Each tuple defines one row of panels.
        Example: [("High RE", "Baseline"), ("Policy", "Baseline")]
    fuel_types : list[str] | None
        Ordered list of fuel types to show as columns.
        Defaults to sorted unique values in df[fuel_col].
    value_col : str
        Column holding the numeric trade value.
    figsize_per_panel : (width, height)
        Size of a single panel in inches.
    palette : dict | None
        Mapping {importer: colour}.  Auto-generated if None.
    title : str | None
        Overall figure suptitle.
    value_label : str
        Y-axis label used on every panel.
    year_col, scenario_col, fuel_col, exporter_col, importer_col : str
        Column name overrides.

    Returns
    -------
    matplotlib.figure.Figure
    """
    # ------------------------------------------------------------------ #
    # 0. Validate & prepare
    # ------------------------------------------------------------------ #
    required = {year_col, scenario_col, fuel_col, exporter_col, importer_col, value_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame is missing columns: {missing}")

    if not scenario_pairs:
        raise ValueError("`scenario_pairs` must contain at least one tuple.")

    # Subset to the chosen exporter
    df_exp = df[df[exporter_col] == exporter].copy()
    if df_exp.empty:
        raise ValueError(f"No rows found for exporter='{exporter}'.")

    # Determine fuel types & years
    fuel_types = fuel_types or sorted(df_exp[fuel_col].unique())
    years = sorted(df_exp[year_col].unique())

    # All trading partners (importers) across the full filtered dataset
    all_importers = sorted(df_exp[importer_col].unique())

    # ------------------------------------------------------------------ #
    # 1. Build colour palette for importers
    # ------------------------------------------------------------------ #
    if palette is None:
        cmap = plt.get_cmap("tab20", len(all_importers))
        palette = {imp: cmap(i) for i, imp in enumerate(all_importers)}

    # ------------------------------------------------------------------ #
    # 2. Aggregate exports per (scenario, fuel, year, importer)
    # ------------------------------------------------------------------ #
    exports = (
        df_exp
        .groupby([scenario_col, fuel_col, year_col, importer_col])[value_col]
        .sum()
        .reset_index()
    )

    # ------------------------------------------------------------------ #
    # 3. Layout
    # ------------------------------------------------------------------ #
    n_rows = len(scenario_pairs)
    n_cols = len(fuel_types)
    fig_w = figsize_per_panel[0] * n_cols
    fig_h = figsize_per_panel[1] * n_rows + (1.0 if title else 0.4)

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(fig_w, fig_h),
        squeeze=False,
        sharey="row",
    )

    fig.patch.set_facecolor("#F7F7F5")

    # ------------------------------------------------------------------ #
    # 4. Draw each panel
    # ------------------------------------------------------------------ #
    x = np.arange(len(years))
    bar_width = 0.7

    for row_idx, (scen, ref_scen) in enumerate(scenario_pairs):
        for col_idx, fuel in enumerate(fuel_types):
            ax = axes[row_idx][col_idx]
            ax.set_facecolor("#F7F7F5")

            # Filter export data for this fuel
            net_scen = exports[(exports[scenario_col] == scen) & (exports[fuel_col] == fuel)]
            net_ref  = exports[(exports[scenario_col] == ref_scen) & (exports[fuel_col] == fuel)]

            # Pivot to (year × importer), fill missing with 0
            def _pivot(d):
                return (
                    d.pivot_table(
                        index=year_col, columns=importer_col,
                        values=value_col, aggfunc="sum"
                    )
                    .reindex(index=years, columns=all_importers, fill_value=0)
                )

            piv_scen = _pivot(net_scen)
            piv_ref  = _pivot(net_ref)
            delta    = piv_scen - piv_ref   # shape: (years × importers)

            # Separate positive & negative stacks
            pos = delta.clip(lower=0)
            neg = delta.clip(upper=0)

            # Draw stacked bars
            pos_bottom = np.zeros(len(years))
            neg_bottom = np.zeros(len(years))

            for imp in all_importers:
                if imp not in delta.columns:
                    continue
                p_vals = pos[imp].values
                n_vals = neg[imp].values
                color  = palette[imp]

                if p_vals.any():
                    ax.bar(x, p_vals, bar_width, bottom=pos_bottom,
                           color=color, label=imp)
                    pos_bottom += p_vals

                if n_vals.any():
                    ax.bar(x, n_vals, bar_width, bottom=neg_bottom,
                           color=color, label=None)
                    neg_bottom += n_vals

            # Zero line
            ax.axhline(0, color="#333333", linewidth=0.8, linestyle="--", alpha=0.6)

            # Axes formatting
            ax.set_xticks(x)
            ax.set_xticklabels(
                [str(y) for y in years],
                rotation=45, ha="right", fontsize=7,
            )
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(
                lambda v, _: f"{v:,.0f}"
            ))
            ax.tick_params(axis="both", labelsize=7)
            ax.spines[["top", "right"]].set_visible(False)

            # Column headers (fuel type) — top row only
            if row_idx == 0:
                ax.set_title(fuel, fontsize=9, fontweight="bold", pad=6)

            # Row labels — first column only
            if col_idx == 0:
                row_label = f"{scen}\nvs. {ref_scen}"
                ax.set_ylabel(
                    f"{row_label}\n\n{value_label}",
                    fontsize=7.5, labelpad=6,
                )

    # ------------------------------------------------------------------ #
    # 5. Shared legend (importers)
    # ------------------------------------------------------------------ #
    legend_handles = [
        Patch(facecolor=palette[imp], label=imp)
        for imp in all_importers
        if imp in palette
    ]
    fig.legend(
        handles=legend_handles,
        title="Importer",
        loc="lower center",
        ncol=min(len(all_importers), 6),
        fontsize=7.5,
        title_fontsize=8,
        frameon=False,
        bbox_to_anchor=(0.5, 0.0),
    )

    # ------------------------------------------------------------------ #
    # 6. Overall title & layout
    # ------------------------------------------------------------------ #
    suptitle = title or f"Export Changes — Exporter: {exporter}"
    fig.suptitle(suptitle, fontsize=11, fontweight="bold", y=1.01)

    legend_rows = max(1, len(all_importers) // 6)
    fig.tight_layout(rect=[0, 0.04 * legend_rows, 1, 1])

# Import fuel supply data
indf = pd.read_csv(package_data_path('weu_security', 'reporting', 'fuel_supply_out.csv'))

df = indf[indf['supply_type'] == 'Imports']
df = df.rename(columns = {'region':'importer'})
df = df[['model', 'scenario', 'year', 'fuel_type', 'exporter', 'importer', 'value']]

df['exporter'] = np.where(df['exporter'].isin(['R12_WEU', 'R12_EEU']), 'Europe', df['exporter'])
df['importer'] = np.where(df['importer'].isin(['R12_WEU', 'R12_EEU']), 'Europe', df['importer'])

check = df[df['scenario'].isin(['FSU2040_NAM30EJ', 'FSU2040_NAM30EJ_REEX'])]
check = check[check['exporter'] == 'Europe']
check['value'] = np.where(check['scenario'] == 'FSU2040_NAM30EJ', check['value']*-1, check['value'])
check = check.groupby(['year', 'fuel_type', 'exporter', 'importer'])['value'].sum().reset_index()






df = pd.concat([df, check])
df = df.groupby(['model', 'scenario', 'year', 'fuel_type', 'exporter', 'importer'])['value'].sum().reset_index()
df = df[df['exporter'] != df['importer']]

plot_export_changes(df=df,
        exporter="Europe",
        scenario_pairs=[("FSU2040_NAM30EJ", "FSU2040_NAM30EJ_REEX")],
        fuel_types=["Crude", "Gas", "Light Oil"],
        title="Net Export Changes vs. SSP2 Reference Scenario",
    )
