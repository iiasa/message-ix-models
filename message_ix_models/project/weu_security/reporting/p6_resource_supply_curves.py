import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from matplotlib.lines import Line2D
from message_ix_models.util import package_data_path
import yaml

# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors']
region_labels = config['region_labels']

def plot_rsc(input_file:str,
             fig_title:str):
    # Read CSV file
    data_path = package_data_path('weu_security', 'resource_supply_curves', input_file)
    df = pd.read_csv(data_path, header=None, skiprows=2, encoding="utf-8-sig")

    def to_float(x):
        return float(str(x).replace(",", "").strip())

    regions, volumes, costs = [], [], []
    for _, row in df.iterrows():
        region = str(row[0]).strip()
        if not region or region == "nan":
            continue
        vols = [to_float(row[i]) for i in range(1, 8)]
        cost_vals = [to_float(row[i]) for i in range(12, 19)]
        regions.append(region)
        volumes.append(vols)
        costs.append(cost_vals)

    # Assign a color per *base* region name (strip " (Update)")
    base_names = [r.replace(" (Update)", "") for r in regions]
    unique_bases = list(dict.fromkeys(base_names))

    color_map = {name: region_colors.get(name, "#888888") for name in unique_bases}

    fig, ax = plt.subplots(figsize=(14, 7))

    legend_handles = {}

    for region, vols, cost_vals in zip(regions, volumes, costs):
        base = region.replace(" (Update)", "")
        is_update = "(Update)" in region
        color = color_map[base]
        linestyle = "--" if is_update else "-"

        cum = 0.0
        xs, ys = [], []
        for v, c in zip(vols, cost_vals):
            xs += [cum, cum + v]
            ys += [c, c]
            cum += v

        ax.plot(xs, ys, color=color, linestyle=linestyle, linewidth=2, alpha=0.85)

        # Use region_labels for display name, fall back to base name if not found
        if base not in legend_handles:
            display_label = region_labels.get(base, base)
            legend_handles[base] = mpatches.Patch(color=color, label=display_label)

    # Generic dashed entry to show what dashed means
    update_line = Line2D([0], [0], color="gray", linestyle="--", linewidth=2,
                        label="North America (Updated)")

    handles = list(legend_handles.values()) + [update_line]

    ax.set_xlabel("Cumulative Reserves [EJ]", fontsize=13)
    ax.set_ylabel("Levelized Cost [USD 2023 / GJ]", fontsize=13)
    ax.set_title(fig_title, fontsize=15, fontweight="bold")
    ax.legend(handles=handles, bbox_to_anchor=(1.01, 1), loc="upper left",
            fontsize=9, framealpha=0.9)
    ax.grid(True, linestyle=":", alpha=0.5)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)

    plt.tight_layout()

plot_rsc(input_file="gas.csv",
         fig_title="Natural Gas Resource Supply Curve by Region")
plot_rsc(input_file="oil.csv",
         fig_title="Oil Resource Supply Curve by Region")