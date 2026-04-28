# Fuel flows sankey

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Polygon
import numpy as np
import yaml
import matplotlib
matplotlib.use('Agg')
from collections import defaultdict

from update_scenario_names import update_scenario_names

# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors']
region_labels = config['region_labels']

# Import fuel supply data
basedf = pd.read_csv(package_data_path('weu_security', 'reporting', 'fuel_supply_out.csv'))
basedf['exporter'] = np.where(basedf['supply_type'] == 'Exports', basedf['region'], basedf['exporter'])
basedf = basedf[basedf['value'] != 0]
basedf = basedf[basedf['supply_type'].isin(['Imports'])]

basedf['transport_type'] = 'Domestic'
basedf['transport_type'] = np.where(basedf['variable'].str.contains('Pipe'), 'Pipeline', basedf['transport_type'])
basedf['transport_type'] = np.where(basedf['variable'].str.contains('Shipped'), 'Shipped', basedf['transport_type'])

basedf['region'] = np.where(basedf['region'].isin(["R12_EEU", "R12_WEU"]), "Europe", basedf['region'])
basedf['exporter'] = np.where(basedf['exporter'].isin(["R12_EEU", "R12_WEU"]), "Europe", basedf['exporter'])
basedf = basedf[basedf['exporter'] != basedf['region']]

basedf = update_scenario_names(basedf)

DEFAULT_COLORS = [
    "#534AB7", "#0F6E56", "#993C1D", "#185FA5",
    "#854F0B", "#993556", "#3B6D11", "#A32D2D",
]

DEFAULT_HATCHES = [None, "///", "...", "xxx", "---", "\\\\\\"]


def _draw_sankey_ax(
    ax, agg, flows_split, exporters, regions,
    global_total, panel_total,
    exp_totals, reg_totals,
    exp_colors, reg_colors, reg_labels,
    region_color_map, region_label_map,
    transport_hatches, add_value_labels,
):
    BAR_W = 0.12
    GAP = 0.01
    ax.set_xlim(0, 1)
    ax.axis("off")

    # ── compute bar positions (y0, height) for each node ──────────────────
    def bar_positions(nodes, totals, side_total):
        positions = {}
        cursor = 0.0
        for node in nodes:
            h = totals[node] / global_total
            positions[node] = (cursor, h)
            cursor += h + (GAP * side_total / global_total)
        return positions

    exp_pos = bar_positions(exporters, exp_totals, panel_total)
    reg_pos = bar_positions(regions,   reg_totals, panel_total)

    # ── draw exporter bars (left side) ────────────────────────────────────
    for exp in exporters:
        y0, h = exp_pos[exp]
        color = region_color_map.get(exp, exp_colors.get(exp, "#888780"))
        ax.add_patch(plt.Rectangle((0, y0), BAR_W, h,
                                   color=color, zorder=3))
        label = region_label_map.get(exp, exp)
        ax.text(BAR_W / 2, y0 + h / 2, label,
                ha="center", va="center", fontsize=16, color="black",
                zorder=4, clip_on=False)
        if add_value_labels:
            ax.text(BAR_W / 2, y0 + h / 2, f"{exp_totals[exp]:.1f}",
                    ha="center", va="center", fontsize=12, color="black", zorder=4)

    # ── draw region bars (right side) ─────────────────────────────────────
    for reg in regions:
        y0, h = reg_pos[reg]
        color = reg_colors.get(reg, "#888780")
        ax.add_patch(plt.Rectangle((1 - BAR_W, y0), BAR_W, h,
                                   color=color, zorder=3))
        label = reg_labels.get(reg, reg)
        ax.text(1 - BAR_W / 2, y0 + h / 2, label,
                ha="center", va="center", fontsize=16, color="black",
                zorder=4, clip_on=False)
        if add_value_labels:
            ax.text(1 - BAR_W / 2, y0 + h / 2, f"{reg_totals[reg]:.1f}",
                    ha="center", va="center", fontsize=12, color="black", zorder=4)

    # ── draw flow bands ────────────────────────────────────────────────────
    exp_cursor = {e: exp_pos[e][0] for e in exporters}
    reg_cursor = {r: reg_pos[r][0] for r in regions}

    transport_totals = defaultdict(float)  # initialised before the loop

    for exp, reg, transport, val in flows_split:
        if val <= 0 or exp not in exp_pos or reg not in reg_pos:
            continue
        transport_totals[transport] += val

        band_h = val / global_total
        hatch  = transport_hatches.get(transport, None)
        color  = region_color_map.get(exp, exp_colors.get(exp, "#888780"))

        x_left  = BAR_W
        x_right = 1 - BAR_W
        y_l0 = exp_cursor[exp]
        y_r0 = reg_cursor[reg]

        t = np.linspace(0, 1, 100)
        cx1, cx2 = x_left + 0.3, x_right - 0.3

        top_l = y_l0 + band_h
        top_r = y_r0 + band_h
        bx     = (1-t)**3*x_left + 3*(1-t)**2*t*cx1 + 3*(1-t)*t**2*cx2 + t**3*x_right
        by_bot = (1-t)**3*y_l0   + 3*(1-t)**2*t*y_l0  + 3*(1-t)*t**2*y_r0  + t**3*y_r0
        by_top = (1-t)**3*top_l  + 3*(1-t)**2*t*top_l + 3*(1-t)*t**2*top_r + t**3*top_r

        verts = np.column_stack([
            np.concatenate([bx, bx[::-1]]),
            np.concatenate([by_bot, by_top[::-1]]),
        ])
        poly = Polygon(verts, closed=True,
                       facecolor=color, alpha=0.45,
                       hatch=hatch,
                       edgecolor="white" if hatch else "none",
                       linewidth=0, zorder=2)
        ax.add_patch(poly)

        exp_cursor[exp] += band_h
        reg_cursor[reg] += band_h

    # ── tighten ylim to actual content ────────────────────────────────────
    all_tops = ([v + h for v, h in exp_pos.values()] +
                [v + h for v, h in reg_pos.values()])
    if all_tops:
        ax.set_ylim(-0.02, max(all_tops) + 0.02)

    return transport_totals


def plot_sankey(
    df,
    fuel_type, plot_year,
    model, scenario=None,
    exporter_col="exporter",
    region_col="region",
    value_col="value",
    transport_col="transport_type",
    hatch_map=None,
    color_map=None,
    region_color_map=None,
    region_label_map=None,
    figsize=None,
    title=None,
    title_fontweight="500",
    ax=None,
    add_value_labels=False,
    save_path=None,
):
    """
    Draw a Sankey diagram (or panel of Sankeys) of flows from exporters to
    regions, sized by value and split by transport_type.

    When scenario=None all scenarios in the filtered data are drawn as
    separate panels in a single figure (one column per scenario). When a
    single scenario string is passed, a single axes is drawn as before.

    Cross-panel synchronisation
    ---------------------------
    Each axes is given a physical height proportional to its scenario's
    panel_total / max_panel_total, so that a scenario with half the flows
    literally occupies half the figure height. Bar heights within each panel
    are also proportional to global_total so the coordinate scaling is
    consistent.

    Within-panel balance
    --------------------
    ``panel_total = max(sum_exp, sum_reg)`` is used as the common denominator
    for both sides of a panel so that the left (exporter) and right (importer)
    bar stacks always reach the same combined height, even when the raw data
    sums differ slightly.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns for exporter, region, value, and transport_type.
    fuel_type : str
        Value to filter the 'fuel_type' column on.
    plot_year : int or str
        Value to filter the 'year' column on.
    model : str
        Value to filter the 'model' column on.
    scenario : list of str or None
        Scenarios to plot, e.g. ["Base", "High"]. Each entry becomes one
        panel. Pass None to include all scenarios in the data.
    exporter_col : str
        Column name for the source / left-side nodes.
    region_col : str
        Column name for the target / right-side nodes.
    value_col : str
        Column name for the numeric flow magnitude.
    transport_col : str
        Column name for the transport type (e.g. 'pipeline', 'LNG').
    hatch_map : dict, optional
        {transport_type: hatch_pattern} e.g. {"pipeline": None, "LNG": "///"}.
    color_map : dict, optional
        {exporter_name: hex_color}. Falls back to DEFAULT_COLORS.
    region_color_map : dict, optional
        {region_name: hex_color}. Colors node bars; also used for exporter bars
        and band colors when the exporter code appears in the dict.
    region_label_map : dict, optional
        {region_name: display_label}.
    figsize : tuple or None
        Figure size. Defaults to (9 * n_scenarios, 9).
    title : str, optional
        Figure suptitle.
    ax : matplotlib.axes.Axes, optional
        Draw onto a single existing axes (only valid when scenario is not None).
    add_value_labels : bool, optional
        Whether to add value labels to the nodes. Defaults to False.
    save_path : str or Path, optional
        If given, saves the figure to this path.

    Returns
    -------
    fig, axes   (axes is a single Axes when one scenario, else an ndarray)
    """
    data = df.copy()
    data = data[data['fuel_type'] == fuel_type]
    data = data[data['year'] == plot_year]
    data = data[data['model'] == model]

    # scenario filtering
    if scenario is not None:
        scenario = [scenario] if isinstance(scenario, str) else list(scenario)
        data = data[data['scenario'].isin(scenario)]
        scenarios = scenario
    else:
        scenarios = list(data['scenario'].unique())

    # transport column
    if transport_col in data.columns:
        data[transport_col] = data[transport_col].fillna("unknown")
    else:
        data[transport_col] = "unknown"

    keep_cols = ['scenario', exporter_col, region_col, transport_col, value_col]
    data = data[keep_cols]
    data = (data.groupby(['scenario', exporter_col, region_col, transport_col])[value_col]
               .sum().reset_index())

    # ── derive shared vocabulary across all scenarios ──────────────────────
    all_exporters_ordered = list(dict.fromkeys(data[exporter_col]))
    all_regions_ordered   = list(dict.fromkeys(data[region_col]))

    transport_types = list(dict.fromkeys(data[transport_col]))
    hatch_map = hatch_map or {}
    auto_h = iter(DEFAULT_HATCHES)
    transport_hatches = {
        tt: hatch_map[tt] if tt in hatch_map else next(auto_h, "///")
        for tt in transport_types
    }

    color_map = color_map or {}
    auto_c = iter(DEFAULT_COLORS)
    exp_colors = {e: color_map.get(e) or next(auto_c, "#888780") for e in all_exporters_ordered}

    region_color_map = region_color_map or {}
    region_label_map = region_label_map or {}
    reg_colors = {r: region_color_map.get(r, "#888780") for r in all_regions_ordered}
    reg_labels = {r: region_label_map.get(r, r)         for r in all_regions_ordered}

    # ── global total: shared denominator for cross-panel bar heights ───────
    global_total = data[value_col].sum()

    # ── pre-compute per-scenario layout data ───────────────────────────────
    panel_totals = []
    scenario_data = []
    for scen in scenarios:
        sdata = data[data['scenario'] == scen]
        agg = (sdata.groupby([exporter_col, region_col])[value_col]
                    .sum().reset_index())
        agg.columns = ["exporter", "region", "value"]
        active_exp = set(agg.loc[agg["value"] > 0, "exporter"])
        active_reg = set(agg.loc[agg["value"] > 0, "region"])
        exporters  = [e for e in all_exporters_ordered if e in active_exp]
        regions    = [r for r in all_regions_ordered   if r in active_reg]
        exp_totals = agg.groupby("exporter")["value"].sum().to_dict()
        reg_totals = agg.groupby("region")["value"].sum().to_dict()
        exp_side_sum = sum(exp_totals[e] for e in exporters)
        reg_side_sum = sum(reg_totals[r] for r in regions)
        panel_total  = max(exp_side_sum, reg_side_sum)
        panel_totals.append(panel_total)
        scenario_data.append((sdata, agg, exporters, regions,
                               exp_totals, reg_totals, panel_total))

    max_panel_total = max(panel_totals)
    n = len(scenarios)

    # ── figure layout ──────────────────────────────────────────────────────
    if ax is not None and n == 1:
        fig  = ax.get_figure()
        axes = [ax]
    else:
        fw, fh = figsize if figsize else (9 * n, 9)
        fig = plt.figure(figsize=(fw, fh))
        fig.patch.set_facecolor("none")

        gap = 0.05
        col_w = (1.0 - gap * (n - 1)) / n
        axes = []
        for i, pt in enumerate(panel_totals):
            h_frac = pt / max_panel_total
            x0 = i * (col_w + gap)
            y0 = (1.0 - h_frac) / 2
            ax_i = fig.add_axes([x0, y0, col_w, h_frac])
            axes.append(ax_i)

    # ── draw one panel per scenario ────────────────────────────────────────
    transport_totals_by_panel = []
    for ax_i, scen, (sdata, agg, exporters, regions,
                     exp_totals, reg_totals, panel_total) in zip(
            axes, scenarios, scenario_data):

        flows_split = list(
            sdata[[exporter_col, region_col, transport_col, value_col]]
            .itertuples(index=False, name=None)
        )

        transport_totals = _draw_sankey_ax(
            ax_i, agg, flows_split, exporters, regions,
            global_total, panel_total,
            exp_totals, reg_totals,
            exp_colors, reg_colors, reg_labels,
            region_color_map, region_label_map,
            transport_hatches, add_value_labels,
        )
        transport_totals_by_panel.append(transport_totals)

    # ── panel titles at a consistent absolute height ───────────────────────
    for ax_i, scen in zip(axes, scenarios):
        pos = ax_i.get_position()
        fig.text(pos.x0 + pos.width / 2, pos.y1 + 0.03, scen,
                 ha="center", va="bottom", fontsize=20, color="#2C2C2A")

    # ── transport totals at bottom of each panel ───────────────────────────
    for ax_i, transport_totals in zip(axes, transport_totals_by_panel):
        pos = ax_i.get_position()
        panel_total_value = sum(transport_totals.values())
        fig.text(pos.x0, 
                 pos.y0,
                 f"Total: {panel_total_value:.0f} EJ",
                 ha="left", va="top", fontsize=18, color="#2C2C2A")

    # ── shared legend ──────────────────────────────────────────────────────
    if len(transport_types) > 1:
        legend_patches = [
            mpatches.Patch(
                facecolor="#888780", alpha=0.5,
                hatch=transport_hatches[tt],
                edgecolor="white" if transport_hatches[tt] else "none",
                label=tt,
            )
            for tt in transport_types
        ]
        fig.legend(handles=legend_patches, loc="lower center",
                   bbox_to_anchor=(0.5, -0.1), ncol=len(transport_types),
                   frameon=False, fontsize=18)

    # ── figure title above panel titles ───────────────────────────────────
    if title:
        fig.suptitle(title, fontsize=24, fontweight=title_fontweight,
                     color="#2C2C2A", y=1.12)

    legend_margin = 0.25 if len(transport_types) > 1 else 0.0
    fig.subplots_adjust(left=0, right=1, top=1, bottom=legend_margin, wspace=0)
    plt.show()

    if save_path:
        fig.savefig(save_path, dpi=160, bbox_inches="tight",
                    transparent=True)

    return fig, axes[0] if n == 1 else np.array(axes)

# Gas in 2025, 2040, 2060
plot_sankey(df = basedf,
            fuel_type="Gas", plot_year=2025,
            model="weu_security", scenario=["REF"],
            exporter_col="exporter",
            region_col="region",
            value_col="value",
            transport_col="transport_type",
            figsize = (8, 10),
            region_color_map=region_colors,
            region_label_map=region_labels,
            title= f"Gas Trade (2025)",
            ax=None,
            add_value_labels=True,
            save_path=package_data_path("weu_security", "figures", "sankey", f"Gas_2025.png"))

for y in [2040, 2060]:
    plot_sankey(df = basedf,
                fuel_type="Gas", plot_year=y,
                model="weu_security", scenario=["REF", "FSU2040", "FSULONG"],
                exporter_col="exporter",
                region_col="region",
                value_col="value",
                transport_col="transport_type",
                figsize = (20, 11),
                region_color_map=region_colors,
                region_label_map=region_labels,
                title= f"Gas Trade ({y})",
                title_fontweight="1000",
                ax=None,
                add_value_labels=False,
                save_path=package_data_path("weu_security", "figures", "sankey", f"Gas_{y}.png"))

# For crude, gas, lightoil
for f in ["Crude", "Gas", "Light Oil"]:
    for y in [2030, 2040, 2060]:
        plot_sankey(df = basedf,
                    fuel_type=f, plot_year=y,
                    model="weu_security", scenario=["REF", "REF_MEACON1.0"],
                    exporter_col="exporter",
                    region_col="region",
                    value_col="value",
                    transport_col="transport_type",
                    figsize = (18,7),
                    region_color_map=region_colors,
                    region_label_map=region_labels,
                    title= f"{f} ({y})",
                    ax=None,
                    add_value_labels=False,
                    save_path=package_data_path("weu_security", "figures", "sankey", f"MEACON_{f}_{y}.png"))

# For crude, gas, lightoil
for f in ["Crude", "Gas", "Light Oil"]:
    for y in [2030, 2040, 2060]:
        plot_sankey(df = basedf,
                    fuel_type=f, plot_year=y,
                    model="weu_security", scenario=["REF", "REF_NAM30EJ"],
                    exporter_col="exporter",
                    region_col="region",
                    value_col="value",
                    transport_col="transport_type",
                    figsize = (18,7),
                    region_color_map=region_colors,
                    region_label_map=region_labels,
                    title= f"{f} ({y})",
                    ax=None,
                    add_value_labels=False,
                    save_path=package_data_path("weu_security", "figures", "sankey", f"NAM_{f}_{y}.png"))
