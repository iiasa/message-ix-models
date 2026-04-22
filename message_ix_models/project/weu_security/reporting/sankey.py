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

# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors'] 
region_labels = config['region_labels']
#region_colors["Europe"] = "#8B0000"

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

DEFAULT_COLORS = [
    "#534AB7", "#0F6E56", "#993C1D", "#185FA5",
    "#854F0B", "#993556", "#3B6D11", "#A32D2D",
]

DEFAULT_HATCHES = [None, "///", "...", "xxx", "---", "\\\\\\"]


def _draw_sankey_ax(
    ax, agg, flows_split, exporters, regions, total,
    exp_totals, reg_totals,
    exp_colors, reg_colors, reg_labels,
    region_color_map, region_label_map,
    transport_hatches, add_value_labels = False,
):
    """Draw a single Sankey panel onto an existing axes."""

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_facecolor("none")

    NODE_W   = 0.025
    GAP      = 0.012
    H_SCALE  = 0.82
    V_OFFSET = 0.09
    X_EXP    = 0.18
    X_REG    = 0.78

    def layout_nodes(names, totals):
        total_bar = sum(totals[n] / total * H_SCALE for n in names)
        total_gap = GAP * (len(names) - 1)
        y = V_OFFSET + (H_SCALE - total_bar - total_gap) / 2
        nodes = {}
        for n in names:
            h = totals[n] / total * H_SCALE
            nodes[n] = {"y": y, "h": h, "mid": y + h / 2}
            y += h + GAP
        return nodes

    exp_nodes = layout_nodes(exporters, exp_totals)
    reg_nodes = layout_nodes(regions,   reg_totals)

    def bezier_verts(x0, y0t, y0b, x1, y1t, y1b):
        t = np.linspace(0, 1, 300)
        cx0 = x0 + 0.8 * (x1 - x0)
        cx1 = x1 - 0.8 * (x1 - x0)

        def bez(y0, y1):
            bx = (1-t)**3*x0 + 3*(1-t)**2*t*cx0 + 3*(1-t)*t**2*cx1 + t**3*x1
            by = (1-t)**3*y0 + 3*(1-t)**2*t*y0  + 3*(1-t)*t**2*y1  + t**3*y1
            return bx, by

        tx_top, ty_top = bez(y0t, y1t)
        tx_bot, ty_bot = bez(y0b, y1b)
        xs = np.concatenate([tx_top, tx_bot[::-1]])
        ys = np.concatenate([ty_top, ty_bot[::-1]])
        return np.column_stack([xs, ys])

    # group flows by (exporter, region)
    band_groups = defaultdict(list)
    for e, r, tt, v in flows_split:
        band_groups[(e, r)].append((tt, v))

    sorted_pairs = sorted(
        band_groups.keys(),
        key=lambda x: (exporters.index(x[0]), regions.index(x[1])),
    )

    exp_off = {e: 0.0 for e in exporters}
    reg_off = {r: 0.0 for r in regions}

    for (e, r) in sorted_pairs:
        subtypes  = band_groups[(e, r)]
        lh_total  = sum(v for _, v in subtypes) / total * H_SCALE
        col       = region_color_map.get(e, exp_colors[e])
        sub_exp_off = sub_reg_off = 0.0

        for tt, v in subtypes:
            lh  = v / total * H_SCALE
            y0t = exp_nodes[e]["y"] + exp_off[e] + sub_exp_off
            y1t = reg_nodes[r]["y"] + reg_off[r] + sub_reg_off
            sub_exp_off += lh
            sub_reg_off += lh

            verts = bezier_verts(X_EXP + NODE_W/2, y0t, y0t + lh,
                                 X_REG - NODE_W/2, y1t, y1t + lh)
            hatch = transport_hatches.get(tt)
            ax.add_patch(Polygon(verts, closed=True,
                                 facecolor=col, alpha=0.42, linewidth=0,
                                 hatch=hatch,
                                 edgecolor="white" if hatch else "none"))

        exp_off[e] += lh_total
        reg_off[r] += lh_total

    # exporter bars + labels
    for e, nd in exp_nodes.items():
        col = region_color_map.get(e, exp_colors[e])
        ax.add_patch(mpatches.FancyBboxPatch(
            (X_EXP - NODE_W/2, nd["y"]), NODE_W, nd["h"],
            boxstyle="round,pad=0.003",
            facecolor=col, edgecolor="none", zorder=3,
        ))
        ax.text(X_EXP - NODE_W/2 - 0.012, nd["mid"],
                region_label_map.get(e, e), ha="right", va="center",
                fontsize=12, fontweight="500", color="#2C2C2A")
        if add_value_labels:
            ax.text(X_EXP - NODE_W/2 - 0.012, nd["mid"] - 0.03,
                    f"{exp_totals[e]:,.0f} EJ", ha="right", va="center",
                    fontsize=12, color="#888780")

    # region bars + labels
    # region bars + labels
    for r, nd in reg_nodes.items():
        ax.add_patch(mpatches.FancyBboxPatch(
            (X_REG - NODE_W/2, nd["y"]), NODE_W, nd["h"],
            boxstyle="round,pad=0.003",
            facecolor=reg_colors[r], edgecolor="none", zorder=3,
        ))
        ax.text(X_REG + NODE_W/2 + 0.012, nd["mid"],
                reg_labels[r], ha="left", va="center",
                fontsize=12, fontweight="500", color="#2C2C2A")
        if add_value_labels:
            ax.text(X_REG + NODE_W/2 + 0.012, nd["mid"] - 0.03,
                    f"{reg_totals[r]:,.0f} EJ", ha="left", va="center",
                    fontsize=12, color="#888780")

    # column total labels (below all exporter and importer nodes)
    tt_exp_totals = defaultdict(float)
    tt_reg_totals = defaultdict(float)
    for e, r, tt, v in flows_split:
        tt_exp_totals[tt] += v
        tt_reg_totals[tt] += v

    exp_bottom = min(nd["y"] for nd in exp_nodes.values())
    reg_bottom = min(nd["y"] for nd in reg_nodes.values())
    label_y_offset = 0.03

    exp_label = "\n".join(
        f"{tt}: {v:,.0f} EJ" for tt, v in sorted(tt_exp_totals.items())
    )
    reg_label = "\n".join(
        f"{tt}: {v:,.0f} EJ" for tt, v in sorted(tt_reg_totals.items())
    )

    ax.text(X_EXP, exp_bottom - label_y_offset,
            exp_label,
            ha="center", va="top", fontsize=10,
            color="#2C2C2A", linespacing=1.5)

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
        panel. Pass None to include all scenarios in the data.    exporter_col : str
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
        Figure size. Defaults to (9, 9 * n_scenarios).
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
    all_exporters = list(dict.fromkeys(data[exporter_col]))
    all_regions   = list(dict.fromkeys(data[region_col]))

    transport_types = list(dict.fromkeys(data[transport_col]))
    hatch_map = hatch_map or {}
    auto_h = iter(DEFAULT_HATCHES)
    transport_hatches = {
        tt: hatch_map[tt] if tt in hatch_map else next(auto_h, "///")
        for tt in transport_types
    }

    color_map = color_map or {}
    auto_c = iter(DEFAULT_COLORS)
    exp_colors = {e: color_map.get(e) or next(auto_c, "#888780") for e in all_exporters}

    region_color_map = region_color_map or {}
    region_label_map = region_label_map or {}
    reg_colors = {r: region_color_map.get(r, "#888780") for r in all_regions}
    reg_labels = {r: region_label_map.get(r, r)         for r in all_regions}

    # ── figure layout ──────────────────────────────────────────────────────
    n = len(scenarios)
    if ax is not None and n == 1:
        fig  = ax.get_figure()
        axes = [ax]
    else:
        fw, fh = figsize if figsize else (9 * n, 9)
        fig, axes_raw = plt.subplots(1, n, figsize=(fw, fh), squeeze=False)
        axes = list(axes_raw[0, :])
        fig.patch.set_facecolor("none")

    # ── draw one panel per scenario ────────────────────────────────────────
    for ax_i, scen in zip(axes, scenarios):
        sdata = data[data['scenario'] == scen]

        agg = (sdata.groupby([exporter_col, region_col])[value_col]
                    .sum().reset_index())
        agg.columns = ["exporter", "region", "value"]

        exporters = list(agg["exporter"].unique())
        regions   = list(agg["region"].unique())
        total     = agg["value"].sum()

        exp_totals = agg.groupby("exporter")["value"].sum().to_dict()
        reg_totals = agg.groupby("region")["value"].sum().to_dict()

        flows_split = list(
            sdata[[exporter_col, region_col, transport_col, value_col]]
            .itertuples(index=False, name=None)
        )

        _draw_sankey_ax(
            ax_i, agg, flows_split, exporters, regions, total,
            exp_totals, reg_totals,
            exp_colors, reg_colors, reg_labels,
            region_color_map, region_label_map,
            transport_hatches, add_value_labels,
        )
        ax_i.set_title(scen, fontsize=14, fontweight="500",
                       color="#2C2C2A", pad=8)

    # ── shared legend for transport types ──────────────────────────────────
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
                   bbox_to_anchor=(0.5, -0.03), ncol=len(transport_types),
                   frameon=False, fontsize=12)

    if title:
        fig.suptitle(title, fontsize=18, fontweight="500",
                     color="#2C2C2A", y=1.02)

    plt.tight_layout(pad=0.6)
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
                figsize = (24, 10),
                region_color_map=region_colors,
                region_label_map=region_labels,
                title= f"Gas Trade ({y})",
                ax=None,
                add_value_labels=True,
                save_path=package_data_path("weu_security", "figures", "sankey", f"Gas_{y}.png"))

# For crude, gas, lightoil
for f in ["Crude"]:
    for y in [2030, 2040, 2060]:
        plot_sankey(df = basedf,
                    fuel_type=f, plot_year=y,
                    model="weu_security", scenario=["REF", "REF_MEACON1.0"],
                    exporter_col="exporter",
                    region_col="region",
                    value_col="value",
                    transport_col="transport_type",
                    figsize = (22,10),
                    region_color_map=region_colors,
                    region_label_map=region_labels,
                    title= f"{f} ({y})",
                    ax=None,
                    add_value_labels=False,
                    save_path=package_data_path("weu_security", "figures", "sankey", f"MEACON_{f}_{y}.png"))

# Total fuels in 2030 and 2060
total_fuels = basedf.groupby(['model', 'scenario', 'exporter', 'region', 'year'])['value'].sum().reset_index()
total_fuels['fuel_type'] = "Total"

for y in [2030, 2060]:
    plot_sankey(df = total_fuels,
                fuel_type="Total", plot_year=y,
                model="weu_security", scenario=["SSP2", "SSP2_NAM30EJ", "FSU2100_NAM30EJ"],
                exporter_col="exporter",
                region_col="region",
                value_col="value",
                figsize = (10, 18),
                region_color_map=region_colors,
                region_label_map=region_labels,
                title= f"{y}",
                ax=None,
                save_path=package_data_path("weu_security", "figures", "sankey", f"Total_NAM30EJ_{y}.png"))