# Line graph of changes to FSU exports

import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import numpy as np
import yaml

# Import plot configurations
with open(package_data_path('weu_security', 'reporting', 'plot_config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
region_colors = config['region_colors'] 
region_labels = config['region_labels']

# Import fuel supply data
indf = pd.read_csv(package_data_path('weu_security', 'reporting', 'fuel_supply_out.csv'))
indf = indf[indf['model'] == 'weu_security'] # no reexports

                             
def plot_reg_exports_area(df: pd.DataFrame,
                          plot_fuel: str | list[str],
                          plot_ref_scenario: str,
                          plot_scenarios: list[str],
                          plot_exporters: list[str],
                          plot_years: list[int],
                          plot_label_regions: list[str] | None = None,
                          plot_by: str = "default",
                          show_labels: bool = True,
                          show_total_line: bool = True):
    """Plot FSU exports as a stacked area chart.

    Args:
        plot_fuel:            Single fuel string or list of fuels.
        plot_by:              "default" — rows=exporters, cols=scenarios.
                              "fuel"    — rows=exporters, cols=fuels (one scenario required).
        plot_label_regions:   Regions to label with % change at last year.
                              None = all, [] = none.
        show_labels:          Show/hide % change annotations at last year.
        show_total_line:      Show/hide the Total (all regions) line within each panel.
    """
    if plot_by not in ("default", "fuel"):
        raise ValueError("plot_by must be 'default' or 'fuel'")

    plot_fuels = [plot_fuel] if isinstance(plot_fuel, str) else plot_fuel

    if plot_by == "fuel" and len(plot_scenarios) != 1:
        raise ValueError("plot_by='fuel' requires exactly one scenario in plot_scenarios")

    # ── Data prep ────────────────────────────────────────────────────────────
    plotdf = df[df['fuel_type'].isin(plot_fuels)]
    plotdf = plotdf[plotdf['scenario'].isin(plot_scenarios + [plot_ref_scenario])]
    plotdf = plotdf[plotdf['year'].isin(plot_years)]
    plotdf = plotdf[plotdf['supply_type'] == 'Imports']

    plotdf = plotdf.groupby(['region', 'fuel_type', 'model', 'scenario',
                             'supply_type', 'unit', 'year', 'exporter'])['value'].sum().reset_index()

    plot_keys = ["exporter", "model", "region", "supply_type", "unit", "year", "fuel_type"]

    ref = (plotdf[plotdf["scenario"] == plot_ref_scenario]
           .set_index(plot_keys)["value"]
           .rename("ref_value"))

    plotdf = plotdf[plotdf['scenario'].isin(plot_scenarios)]
    plotdf = plotdf.set_index(plot_keys).join(ref, how="inner").reset_index()

    plotdf["delta"] = plotdf["value"] - plotdf["ref_value"]
    plotdf["delta_pct"] = (plotdf["delta"] / plotdf["ref_value"].replace(0, np.nan)) * 100

    # Total exporter rows: computed BEFORE exporter filter
    total_exporters_df = (plotdf
                          .groupby(["scenario", "year", "region", "fuel_type"], as_index=False)[["delta", "ref_value"]]
                          .sum()
                          .assign(exporter="Total"))
    total_exporters_df["delta_pct"] = (total_exporters_df["delta"] / total_exporters_df["ref_value"].replace(0, np.nan)) * 100

    total_exporters_total_df = (total_exporters_df
                                .groupby(["scenario", "year", "exporter", "fuel_type"], as_index=False)[["delta", "ref_value"]]
                                .sum())
    total_exporters_total_df["delta_total"] = total_exporters_total_df["delta"]
    total_exporters_total_df["pct_total"] = (total_exporters_total_df["delta"] / total_exporters_total_df["ref_value"].replace(0, np.nan)) * 100

    plotdf = plotdf[plotdf['exporter'].isin(plot_exporters)]

    total_df = (plotdf
                .groupby(["scenario", "year", "exporter", "fuel_type"], as_index=False)[["delta", "ref_value"]]
                .sum())
    total_df["delta_total"] = total_df["delta"]
    total_df["pct_total"] = (total_df["delta"] / total_df["ref_value"].replace(0, np.nan)) * 100

    exporters = plot_exporters + ["Total"]
    last_year = max(plot_years)

    # ── Layout ───────────────────────────────────────────────────────────────
    row_items = exporters
    col_items = plot_scenarios if plot_by == "default" else plot_fuels

    n_rows = len(row_items)
    n_cols = len(col_items)

    fig, axes = plt.subplots(
        nrows=n_rows,
        ncols=n_cols,
        figsize=(4.5 * n_cols, 3.5 * n_rows),
        sharex=True,
        sharey= 'row'#True
    )

    if n_rows == 1:
        axes = np.expand_dims(axes, axis=0)
    if n_cols == 1:
        axes = np.expand_dims(axes, axis=1)

    all_label_candidates = []

    for i, exporter in enumerate(row_items):
        for j, col_item in enumerate(col_items):

            ax = axes[i, j]

            scenario = col_item if plot_by == "default" else plot_scenarios[0]
            fuel     = plot_fuels[0] if plot_by == "default" else col_item

            if exporter == "Total":
                panel_df       = total_exporters_df[
                    (total_exporters_df["scenario"] == scenario) &
                    (total_exporters_df["fuel_type"] == fuel)
                ]
                panel_total_df = total_exporters_total_df
            else:
                panel_df       = plotdf[
                    (plotdf["exporter"] == exporter) &
                    (plotdf["scenario"] == scenario) &
                    (plotdf["fuel_type"] == fuel)
                ]
                panel_total_df = total_df

            # ── Stacked area ──────────────────────────────────────────────────
            pivot = (panel_df
                     .pivot_table(index="year", columns="region", values="delta", aggfunc="sum")
                     .reindex(sorted(plot_years))
                     .fillna(0))

            pos = pivot.clip(lower=0)
            neg = pivot.clip(upper=0)

            region_order = list(pivot.columns)
            colors = [region_colors.get(r, "gray") for r in region_order]

            ax.stackplot(pivot.index, pos.T, labels=region_order, colors=colors, alpha=0.7)
            ax.stackplot(pivot.index, neg.T, colors=colors, alpha=0.7)

            if show_labels:
                for region in region_order:
                    if plot_label_regions is None or region in plot_label_regions:
                        pct_row = panel_df[
                            (panel_df["region"] == region) &
                            (panel_df["year"] == last_year)
                        ]
                        if len(pct_row) > 0:
                            pct_val = pct_row["delta_pct"].iloc[0]
                            if pd.notna(pct_val):
                                idx = region_order.index(region)
                                y_pos = pos[region_order[:idx + 1]].loc[last_year].sum()
                                all_label_candidates.append({
                                    "ax": ax,
                                    "x": last_year,
                                    "y": y_pos,
                                    "text": f"{pct_val:+.1f}%",
                                    "color": region_colors.get(region, "gray"),
                                })

            # ── Total (all regions) line ──────────────────────────────────────
            if show_total_line:
                total_sub = panel_total_df[
                    (panel_total_df["scenario"] == scenario) &
                    (panel_total_df["exporter"] == exporter) &
                    (panel_total_df["fuel_type"] == fuel)
                ].sort_values("year")

                if len(total_sub) > 0:
                    ax.plot(total_sub["year"], total_sub["delta_total"],
                            color="black", linewidth=1, marker=None, label="Total")

                    if show_labels:
                        last_total = total_sub[total_sub["year"] == last_year]
                        if len(last_total) > 0:
                            pct_val = last_total["pct_total"].iloc[0]
                            if pd.notna(pct_val):
                                all_label_candidates.append({
                                    "ax": ax,
                                    "x": last_year,
                                    "y": last_total["delta_total"].iloc[0],
                                    "text": f"{pct_val:+.1f}%",
                                    "color": "black",
                                })

            ax.axhline(0, color="black", linestyle="--", linewidth=1)
            ax.grid(True, alpha=0.3)

            if i == 0:
                ax.set_title(col_item)
            if j == 0:
                ax.set_ylabel(f"{exporter}\nΔ vs {plot_ref_scenario}")

    # ── Legend ───────────────────────────────────────────────────────────────
    handles, labels = [], []
    seen = set()
    for ax_row in axes:
        for ax in ax_row:
            for h, l in zip(*ax.get_legend_handles_labels()):
                if l not in seen:
                    handles.append(h)
                    labels.append(l)
                    seen.add(l)

    new_labels = [region_labels.get(lbl, lbl) if lbl != "Total" else "Total (All Regions)" for lbl in labels]
    paired = sorted(zip(new_labels, handles), key=lambda x: (x[0] == "Total (All Regions)", x[0]))
    new_labels, handles = zip(*paired)

    fig.legend(handles, new_labels, title="Importing Region",
               bbox_to_anchor=(1.02, 0.5), loc="center left")

    title_fuel = plot_fuels[0] if len(plot_fuels) == 1 else ", ".join(plot_fuels)
    title_scen = plot_scenarios[0] if len(plot_scenarios) == 1 else ", ".join(plot_scenarios)
    fig.suptitle(f"{title_fuel} Exports — Change vs {plot_ref_scenario} ({title_scen})",
                 fontsize=14, y=0.95)

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    for cand in all_label_candidates:
        cand["ax"].annotate(
            cand["text"],
            xy=(cand["x"], cand["y"]),
            xytext=(8, 0),
            textcoords="offset points",
            fontsize=7,
            color=cand["color"],
            va="center",
            ha="left",
            annotation_clip=False,
        )

    plt.show()

plot_reg_exports_area(df = indf,
                        plot_fuel = ['Crude', 'Gas', 'Light Oil'],
                        plot_by = "fuel",
                        plot_ref_scenario = 'FSU2040',
                        plot_scenarios = ['FSU2040_MEACON_1.0'],
                        plot_exporters = ['R12_MEA'],
                        plot_label_regions = ["Total (All Regions)", "R12_WEU", "R12_EEU", "R12_CHN"],
                        plot_years = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060],
                        show_labels = True,
                        show_total_line = True)  

plot_reg_exports_area(df = indf,
                        plot_fuel = ['Crude', 'Gas', 'Light Oil'],
                        plot_by = "fuel",
                        plot_ref_scenario = 'FSU2040',
                        plot_scenarios = ['FSU2040_MEACON_1.0'],
                        plot_exporters = ['R12_MEA', 'R12_FSU'],
                        plot_label_regions = ["Total (All Regions)", "R12_WEU", "R12_EEU", "R12_CHN"],
                        plot_years = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060],
                        show_labels = True,
                        show_total_line = True)  

plot_reg_exports_area(df = indf,
                        plot_fuel = ['Crude', 'Gas', 'Light Oil'],
                        plot_by = "fuel",
                        plot_ref_scenario = 'FSU2040',
                        plot_scenarios = ['FSU2040_NAM30EJ'],
                        plot_exporters = ['R12_NAM', 'R12_FSU','R12_MEA'],
                        plot_label_regions = ["Total (All Regions)", "R12_WEU", "R12_EEU", "R12_CHN"],
                        plot_years = [2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060],
                        show_labels = True,
                        show_total_line = True)