# Gas imports to Europe

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import yaml

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

def chord_diagram(df, fuel_type, plot_year,
                  model, scenario,
                  figsize=(9, 9),
                  region_names=None, region_colors=None,
                  size_by="both"):
    region_names  = region_names  or {}
    region_colors = region_colors or {}

    data = df.copy()
    data = data[data['fuel_type'] == fuel_type]
    data = data[data['year'] == plot_year]
    data = data[data["model"] == model]
    data = data[data["scenario"] == scenario]

    data = data[['model', 'scenario', 'exporter', 'region', 'value']]
    data = data.groupby(['model', 'scenario', 'exporter', 'region'])['value'].sum().reset_index()

    data["exporter"] = data["exporter"].map(lambda r: region_names.get(r, r))
    data["region"]   = data["region"].map(lambda r: region_names.get(r, r))
    data = data.groupby(["exporter", "region"], as_index=False)["value"].sum()

    exporters = sorted(data["exporter"].unique())
    regions   = sorted(r for r in data["region"].unique() if data[data["region"] == r]["value"].sum() > 0)
    nodes     = exporters + regions
    n         = len(nodes)

    matrix = np.zeros((n, n))
    idx    = {name: i for i, name in enumerate(nodes)}
    for _, row in data.iterrows():
        if row["exporter"] in idx and row["region"] in idx:
            matrix[idx[row["exporter"]], idx[row["region"]]] += row["value"]

    out_totals = matrix.sum(axis=1)
    in_totals  = matrix.sum(axis=0)

    def compute_arc_totals(out_t, in_t, node_list, exp_list):
        exp_set = set(exp_list)
        if size_by == "exporter":
            # Arc sized by outgoing; pure importers fall back to incoming
            return np.array([out_t[i] if out_t[i] > 0 else in_t[i] for i in range(len(node_list))])
        elif size_by == "importer":
            # Arc sized by incoming; pure exporters fall back to outgoing
            return np.array([in_t[i] if in_t[i] > 0 else out_t[i] for i in range(len(node_list))])
        else:  # "both"
            return np.array([out_t[i] if node_list[i] in exp_set else in_t[i] for i in range(len(node_list))])

    arc_totals = compute_arc_totals(out_totals, in_totals, nodes, exporters)

    # Drop zero-flow nodes
    active    = arc_totals > 0
    nodes     = [nodes[i] for i in range(n) if active[i]]
    n         = len(nodes)
    idx       = {name: i for i, name in enumerate(nodes)}
    matrix    = matrix[np.ix_(active, active)]
    exporters = [name for name in exporters if name in idx]
    regions   = [name for name in regions   if name in idx]

    out_totals = matrix.sum(axis=1)
    in_totals  = matrix.sum(axis=0)
    arc_totals = compute_arc_totals(out_totals, in_totals, nodes, exporters)

    out_totals = np.where(out_totals == 0, 1, out_totals)
    in_totals  = np.where(in_totals  == 0, 1, in_totals)
    arc_totals = np.where(arc_totals == 0, 1, arc_totals)

    # Ribbon denominators: always directional (out for exporters, in for importers)
    # so ribbons tile their arc exactly regardless of size_by mode
    exp_set       = set(exporters)
    ribbon_totals = np.array([out_totals[i] if nodes[i] in exp_set else in_totals[i] for i in range(n)])

    # ── Colors ────────────────────────────────────────────────────────────────
    all_names    = sorted(set(exporters) | set(regions))
    fallback     = plt.cm.get_cmap("tab20", len(all_names) + 1)
    fallback_map = {name: fallback(i) for i, name in enumerate(all_names)}

    def resolve_color(display_name):
        if display_name in region_colors:
            return region_colors[display_name]
        for orig, disp in region_names.items():
            if disp == display_name and orig in region_colors:
                return region_colors[orig]
        return fallback_map[display_name]

    node_colors = [resolve_color(name) for name in nodes]

    # ── Angular layout ────────────────────────────────────────────────────────
    GAP        = 0.03
    total_flow = arc_totals.sum()
    arc_sizes  = (arc_totals / total_flow) * (2 * np.pi - n * GAP)

    starts = np.zeros(n)
    for i in range(1, n):
        starts[i] = starts[i - 1] + arc_sizes[i - 1] + GAP
    ends = starts + arc_sizes

    # ── Helpers ───────────────────────────────────────────────────────────────

    def angle_to_xy(angle, r=1.0):
        return r * np.cos(angle), r * np.sin(angle)

    def draw_arc(ax, start, end, r=1.0, color="k", lw=8):
        thetas = np.linspace(start, end, 100)
        ax.plot(r * np.cos(thetas), r * np.sin(thetas),
                color=color, lw=lw, solid_capstyle="butt")

    def draw_ribbon(ax, i, j, val, offset_i, offset_j, color, alpha=0.45):
        span_i = arc_sizes[i] * (val / ribbon_totals[i])
        span_j = arc_sizes[j] * (val / ribbon_totals[j])
        a0, a1 = starts[i] + offset_i, starts[i] + offset_i + span_i
        b0, b1 = starts[j] + offset_j, starts[j] + offset_j + span_j
        R = 0.98

        def arc_points(t0, t1, r=R, steps=20):
            ts = np.linspace(t0, t1, steps)
            return np.column_stack([r * np.cos(ts), r * np.sin(ts)])

        def bezier_to_center(start_pt, end_pt, steps=100):
            c  = np.array([0.0, 0.0])
            t  = np.linspace(0, 1, steps)[:, None]
            p0 = np.array(start_pt)
            p3 = np.array(end_pt)
            p1 = p0 * 0.1 + c * 0.9
            p2 = p3 * 0.1 + c * 0.9
            return ((1-t)**3*p0 + 3*(1-t)**2*t*p1
                    + 3*(1-t)*t**2*p2 + t**3*p3)

        pa0 = np.array(angle_to_xy(a0, R))
        pa1 = np.array(angle_to_xy(a1, R))
        pb0 = np.array(angle_to_xy(b0, R))
        pb1 = np.array(angle_to_xy(b1, R))

        verts = np.vstack([
            bezier_to_center(pa0, pb0),
            arc_points(b0, b1),
            bezier_to_center(pb1, pa1),
            arc_points(a1, a0),
        ])

        ax.add_patch(plt.Polygon(verts, closed=True,
                                 facecolor=color, edgecolor="#e0e0e0",
                                 linewidth=0.3, alpha=alpha, zorder=2))
        return span_i, span_j

    # ── Figure ────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_aspect("equal")
    ax.axis("off")
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    offsets = np.zeros(n)
    for i in range(n):
        for j in range(n):
            if matrix[i, j] > 0:
                di, dj = draw_ribbon(ax, i, j, matrix[i, j],
                                     offsets[i], offsets[j],
                                     color=node_colors[i])
                offsets[i] += di
                offsets[j] += dj

    for i, (s, e) in enumerate(zip(starts, ends)):
        draw_arc(ax, s, e, r=1.01, color=node_colors[i], lw=14)

    LABEL_R = 1.14
    for i, name in enumerate(nodes):
        mid = (starts[i] + ends[i]) / 2
        x, y = angle_to_xy(mid, LABEL_R)
        deg  = np.degrees(mid)
        rot  = deg if -90 <= (deg % 360) - 180 <= 90 else deg + 180
        ha   = "left" if np.cos(mid) >= 0 else "right"
        ax.text(x, y, name, ha=ha, va="center",
                rotation=rot, rotation_mode="anchor",
                fontsize=10, color="#1a1a1a", fontweight="500")

    seen, legend_handles = set(), []
    for i, name in enumerate(nodes):
        if name not in seen:
            legend_handles.append(mpatches.Patch(facecolor=node_colors[i], label=name))
            seen.add(name)

    ax.legend(handles=legend_handles, loc="lower center",
              bbox_to_anchor=(0.5, -0.06), ncol=min(len(legend_handles), 5),
              frameon=False, fontsize=8,
              labelcolor="#1a1a1a", handlelength=1.2)

    title_parts = ["Exporter → Region flows"]
    if model:    title_parts.append(f"model: {model}")
    if scenario: title_parts.append(f"scenario: {scenario}")
    ax.set_title("  |  ".join(title_parts), color="#1a1a1a",
                 fontsize=13, pad=14, fontweight="500")

    ax.set_xlim(-1.35, 1.35)
    ax.set_ylim(-1.35, 1.35)
    plt.tight_layout()

chord_diagram(df = basedf, fuel_type="Gas", plot_year=2060, 
              model="weu_security", scenario="FSU2100", figsize=(9, 9),
              region_names=region_labels, region_colors=region_colors,
              size_by="importer")
plt.show()
