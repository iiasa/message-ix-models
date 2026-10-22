"""Compare the water reporting of several scenarios: markdown report + figures.

Usage:
  python water_report.py --out DIR [--name report_w] [--pdf] [--config config.yaml]
      LABEL=<model>_<scenario>_nexus.csv [LABEL=... ...]

The first file is the reference the others are compared with. Input files are
the ``<model>_<scenario>_nexus.csv`` written by ``water-ix report --water``
(IAMC long or wide format). Output: ``DIR/<name>.md`` (+ ``.pdf``) and
``DIR/*.png``. The scenario table comes from the package ``config.yaml``; the
assumptions table from ASSUMPTIONS below (values read off the SSP2 base).
"""

import argparse
from pathlib import Path

import matplotlib
import yaml

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

SOURCES = {
    "Water Extraction|Surface Water": "surface water",
    "Water Extraction|Groundwater": "renewable groundwater",
    "Water Extraction|Fossil Groundwater": "fossil groundwater",
    "Water Desalination": "desalination",
    "Water Waste|Reuse|Urban": "urban reuse",
}
OTHER = [
    "Water Withdrawal|Irrigation",
    "Water Waste|Treated|Urban",
    "Water Waste|Treated|Rural",
    "Water Extraction|Seawater|Cooling",
]
DEMANDS = {
    "irrigation": [
        "Water Withdrawal|Irrigation|Cereal",
        "Water Withdrawal|Irrigation|Oil Crops",
        "Water Withdrawal|Irrigation|Sugar Crops",
    ],
    "municipal": ["Water Withdrawal|Municipal Water"],
    "industry": [
        "Water Withdrawal|Industrial Water|Unconnected",
        "Water Withdrawal|Industrial Water|Unconnected Eff",
    ],
    "electricity + other energy": [
        "Water Withdrawal|Electricity|Cooling|Fresh Water",
        "Water Withdrawal|Electricity|Hydro",
        "Water Withdrawal|Energy|Non-Cooling",
    ],
}
SUPPLIES = {
    "surface water": ["Water Extraction|Surface Water"],
    "renewable groundwater": ["Water Extraction|Groundwater"],
    "fossil groundwater": ["Water Extraction|Fossil Groundwater"],
    "desalination": ["Water Desalination"],
    "wastewater reuse": ["Water Waste|Reuse"],
}
DEMAND_COLORS = ["#e6c9a8", "#c2a5cf", "#9ecae1", "#fdae6b"]

#: Model assumptions shared by every scenario (values read off the SSP2 base).
ASSUMPTIONS = [
    (
        "starters",
        "SSP2 / SSP3 `NPiREF_GDP_CI_50_ensemble_2` (GDP climate impacts, "
        "50th percentile), first model year 2035, R12 + 214 basins",
    ),
    (
        "hydrology",
        "RCP 7p0 (NPiREF), reliability tier `low`; availability and "
        "e-flow from the #513 GHM refresh",
    ),
    (
        "environmental flow",
        "`bound_activity_lo` on `return_flow` = VMF e-flow, "
        "never above 70 % of surface availability; off in `base`",
    ),
    (
        "renewable extraction growth",
        "`growth_activity_up` 2 %/yr (SSP2), 3 %/yr "
        "(SSP3) on surface water and renewable groundwater",
    ),
    (
        "fossil groundwater",
        "backstop, no bound; investment 0.065 USD per m³/yr "
        "(20 y), electricity 0.0001 GWa/MCM, ≈0.03 USD/m³; `var_cost` 0.4 USD/m³ "
        "where priced",
    ),
    (
        "desalination (membrane)",
        "investment 1.90 USD per m³/yr (30 y), fixed "
        "0.155, electricity 0.0003 and heat 0.001 GWa/MCM, ≈0.34 USD/m³ before heat; "
        "distillation 1.55 / 1.65",
    ),
    (
        "desalination potential",
        "`bound_total_capacity_up` on seawater intake from "
        "Marina Andrijevic's 2026 projections (SSP1/3/5 modelled; zero in 2110)",
    ),
    (
        "desalination build limit",
        "10 %/yr `growth_new_capacity_up` chained from "
        "`historical_new_capacity` ending 2025 → zero anchor in 2030; removed and "
        "re-seeded by `desal_free`",
    ),
    (
        "wastewater reuse",
        "`share_commodity_lo` `share_wat_recycle`: urban recycle "
        "≥ 0.71 (2035) → 0.80 (2100, median basin) of treated urban wastewater; no "
        "upper bound; recycle investment 1.35 USD per m³/yr, fixed 0.098",
    ),
    (
        "distribution efficiency",
        "modes `M1` (baseline losses) and `Mf` (efficient, "
        "mid-tier costs) on urban/rural distribution; `Mf` banned in `maladapt`, "
        "the only mode in the SDG build",
    ),
    (
        "SDG package (`adapt`)",
        "Mf-only distribution, treatment targets urban 0.95 / "
        "rural 0.8, connection 0.99 / 0.8, recycling floor raised (untreated gap "
        "halved after 2030/2040), e-flow bounds",
    ),
    ("solver", "CPLEX barrier (`lpmethod=4`, `scaind=-1`), message_ix PR #1023"),
]
LEVER_LABELS = {
    "build": "build",
    "from": "clone of",
    "sdgs": "water SDG package",
    "eflow": "environmental flows",
    "renewable_growth": "renewable growth cap",
    "fossil_gw_var_cost": "fossil GW var_cost (USD/m³)",
    "desal_free": "desalination freed",
    "ban_mode": "distribution mode banned",
}


def scenario_table(config: dict) -> str:
    """Markdown table of the scenarios and levers in `config`."""
    keys = ["key", "scenario", "build / clone of", "levers"]
    rows = []
    for key, spec in config["scenarios"].items():
        name = config["name"].format(ssp="SSPx", key=spec.get("label", key))
        base = (
            f"nexus build, sdgs={spec['sdgs']}"
            if "build" in spec
            else f"clone of `{spec['from']}`"
        )
        levers = []
        for k, v in spec.items():
            if k in ("build", "from", "sdgs", "label"):
                continue
            levers.append(
                LEVER_LABELS.get(k, k)
                if v is True
                else f"{LEVER_LABELS.get(k, k)} = {v}"
            )
        rows.append([f"`{key}`", f"`{name}`", base, "; ".join(levers) or "none"])
    head = "| " + " | ".join(keys) + " |"
    sep = "|" + "|".join("---" for _ in keys) + "|"
    return "\n".join([head, sep, *("| " + " | ".join(r) + " |" for r in rows)])


def assumptions_table() -> str:
    head = "| assumption | value |\n|---|---|"
    return "\n".join([head, *(f"| {k} | {v} |" for k, v in ASSUMPTIONS)])


YEARS = [2035, 2050, 2070, 2100]
COLORS = ["#4477aa", "#66ccee", "#ee6677", "#228833", "#ccbb44", "#aa3377", "#bbbbbb"]


def load(path: Path) -> pd.DataFrame:
    """IAMC rows of `path` as long frame: region, variable, unit, year, value."""
    df = pd.read_csv(path)
    df.columns = [str(c).lower() for c in df.columns]
    if "year" not in df.columns:
        years = [c for c in df.columns if c.isdigit()]
        df = df.melt(
            id_vars=[c for c in df.columns if c not in years],
            value_vars=years,
            var_name="year",
            value_name="value",
        )
    df["year"] = df["year"].astype(int)
    keep = ["region", "variable", "unit", "year", "value"]
    return df[keep].dropna(subset=["value"])


COMPONENTS = {
    "Water Withdrawal|Irrigation": [
        "Water Withdrawal|Irrigation|Cereal",
        "Water Withdrawal|Irrigation|Oil Crops",
        "Water Withdrawal|Irrigation|Sugar Crops",
    ],
}


def select(df: pd.DataFrame, variable: str) -> pd.DataFrame:
    """Rows of `variable`, or of its components when the aggregate is not reported."""
    sel = df[df["variable"] == variable]
    if sel.empty and variable in COMPONENTS:
        sel = df[df["variable"].isin(COMPONENTS[variable])]
    return sel


def world(df: pd.DataFrame, variable: str) -> pd.Series:
    """World value by year of `variable`; sum of R12 regions when World is absent."""
    sel = select(df, variable)
    if (sel["region"] == "World").any():
        return sel[sel["region"] == "World"].groupby("year")["value"].sum()
    return sel[sel["region"].str.startswith("R12_")].groupby("year")["value"].sum()


def by_region(df: pd.DataFrame, variable: str, year: int) -> pd.Series:
    sel = select(df, variable)
    sel = sel[sel["year"] == year]
    sel = sel[sel["region"].str.startswith("R12_")]
    return sel.groupby("region")["value"].sum()


def fig_sources(data: dict[str, pd.DataFrame], out: Path) -> Path:
    fig, axes = plt.subplots(1, len(data), figsize=(4.2 * len(data), 3.8), sharey=True)
    axes = [axes] if len(data) == 1 else list(axes)
    for ax, (label, df) in zip(axes, data.items()):
        table = pd.DataFrame({name: world(df, var) for var, name in SOURCES.items()})
        table = table.fillna(0.0)
        table = table[table.index >= 2035]
        ax.stackplot(table.index, table.T.values, labels=table.columns, colors=COLORS)
        ax.set_title(label)
        ax.set_xlabel("year")
        ax.grid(alpha=0.3)
    axes[0].set_ylabel("MCM/yr")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles, labels, loc="lower center", ncol=len(labels), fontsize=8, frameon=False
    )
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    path = out / "fig_sources.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def fig_regions(
    data: dict[str, pd.DataFrame], variable: str, year: int, out: Path
) -> Path:
    table = pd.DataFrame(
        {label: by_region(df, variable, year) for label, df in data.items()}
    )
    table = table.fillna(0.0)
    fig, ax = plt.subplots(figsize=(8, 3.6))
    table.plot.bar(ax=ax, color=COLORS[: len(table.columns)], width=0.8)
    ax.set_ylabel("MCM/yr")
    ax.set_title(f"{variable}, {year}")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    path = out / f"fig_{variable.split('|')[-1].lower().replace(' ', '_')}_{year}.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def world_sum(df: pd.DataFrame, variables: list[str]) -> pd.Series:
    """World value by year summed over `variables`."""
    total = pd.Series(dtype=float)
    for v in variables:
        total = total.add(world(df, v), fill_value=0.0)
    return total


def fig_balance(data: dict[str, pd.DataFrame], out: Path) -> Path:
    """Supplies above the axis, demands below; the line is supply minus demand."""
    n = len(data)
    fig, axes = plt.subplots(1, n, figsize=(4.4 * n, 4.8), sharey=True)
    axes = [axes] if n == 1 else list(axes)
    for ax, (label, df) in zip(axes, data.items()):
        sup = pd.DataFrame({k: world_sum(df, v) for k, v in SUPPLIES.items()})
        dem = pd.DataFrame({k: world_sum(df, v) for k, v in DEMANDS.items()})
        years = [
            y for y in sorted(set(sup.index) | set(dem.index)) if 2035 <= y <= 2100
        ]
        sup = sup.reindex(years).fillna(0.0)
        dem = dem.reindex(years).fillna(0.0)
        ax.stackplot(
            years, sup.T.values, labels=sup.columns, colors=COLORS[: len(sup.columns)]
        )
        ax.stackplot(years, (-dem).T.values, labels=dem.columns, colors=DEMAND_COLORS)
        ax.plot(
            years,
            sup.sum(axis=1) - dem.sum(axis=1),
            color="black",
            lw=1.2,
            label="supply - demand",
        )
        ax.axhline(0, color="black", lw=0.6)
        ax.set_title(label)
        ax.set_xlabel("year")
        ax.grid(alpha=0.3)
    axes[0].set_ylabel("MCM/yr  (supply above, demand below)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=5, fontsize=8, frameon=False)
    fig.tight_layout(rect=(0, 0.13, 1, 1))
    path = out / "fig_balance.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


CSS = """
@page { size: A4; margin: 18mm 16mm; }
body { font-family: "Segoe UI", Arial, sans-serif; font-size: 10.5pt;
       line-height: 1.4; color: #222; }
h1 { font-size: 20pt; margin: 0 0 6pt; }
h2 { font-size: 14pt; margin-top: 18pt; border-bottom: 1px solid #999;
     padding-bottom: 2pt; page-break-after: avoid; }
table { border-collapse: collapse; margin: 8pt 0; font-size: 9pt;
        page-break-inside: avoid; }
th, td { border: 1px solid #bbb; padding: 2pt 6pt; text-align: right; }
th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) {
  text-align: left; }
th { background: #eee; }
img { width: 100%; display: block; margin: 8pt 0; page-break-inside: avoid; }
code { font-family: Consolas, monospace; font-size: 9pt; background: #f3f3f3;
       padding: 0 2pt; }
"""
BROWSERS = [
    "C:/Program Files/Google/Chrome/Application/chrome.exe",
    "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
]


def build_pdf(md_path: Path) -> Path:
    """`md_path` -> HTML (+CSS) -> PDF via a headless Chrome/Edge; needs `markdown`."""
    import subprocess

    try:
        import markdown
    except ImportError as e:  # pragma: no cover
        raise SystemExit(
            "PDF needs the `markdown` package: pip install markdown"
        ) from e
    body = markdown.markdown(md_path.read_text(encoding="utf-8"), extensions=["tables"])
    html = md_path.with_suffix(".html")
    html.write_text(
        f"<!doctype html><html><head><meta charset='utf-8'><style>{CSS}</style>"
        f"</head><body>{body}</body></html>",
        encoding="utf-8",
    )
    browser = next((b for b in BROWSERS if Path(b).exists()), None)
    if browser is None:  # pragma: no cover
        raise SystemExit(f"no Chrome/Edge found; open {html} and print to PDF")
    pdf = md_path.with_suffix(".pdf")
    subprocess.run(
        [
            browser,
            "--headless=new",
            "--disable-gpu",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf.resolve()}",
            html.resolve().as_uri(),
        ],
        check=True,
        capture_output=True,
    )
    return pdf


def md_table(t: pd.DataFrame) -> str:
    """`t` (MultiIndex rows, year columns) as a markdown table, no extra deps."""
    t = t.reset_index()
    head = "| " + " | ".join(str(c) for c in t.columns) + " |"
    sep = "|" + "|".join("---" for _ in t.columns) + "|"
    body = []
    for row in t.itertuples(index=False):
        cells = [
            "" if pd.isna(v) else (f"{v:,.0f}" if isinstance(v, float) else str(v))
            for v in row
        ]
        body.append("| " + " | ".join(cells) + " |")
    return "\n".join([head, sep, *body])


def table_world(data: dict[str, pd.DataFrame], variables: dict[str, str]) -> str:
    rows = []
    for var, name in variables.items():
        for label, df in data.items():
            s = world(df, var)
            rows.append(
                {
                    "variable": name,
                    "scenario": label,
                    **{y: s.get(y, float("nan")) for y in YEARS},
                }
            )
    return md_table(pd.DataFrame(rows).set_index(["variable", "scenario"]))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument(
        "inputs", nargs="+", help="LABEL=path.csv; the first is the reference"
    )
    p.add_argument("--pdf", action="store_true", help="also render <name>.pdf")
    p.add_argument("--name", default="report", help="output stem (default: report)")
    p.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "config.yaml",
        help="package config.yaml for the scenario table",
    )
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    data: dict[str, pd.DataFrame] = {}
    for item in a.inputs:
        label, _, path = item.partition("=")
        data[label] = load(Path(path))
    ref = next(iter(data))

    figs = [fig_sources(data, a.out)]
    balance = fig_balance(data, a.out)
    for var in ["Water Extraction|Fossil Groundwater", "Water Desalination"]:
        figs.append(fig_regions(data, var, 2050, a.out))

    lines = [
        "# Water scenarios: supply mix and adaptation levers",
        "",
        f"Reference: **{ref}**. Values are World totals in MCM/yr from "
        "`water-ix report --water` (`*_nexus.csv`).",
        "",
        "## Scenarios",
        "",
        scenario_table(yaml.safe_load(a.config.read_text(encoding="utf-8"))),
        "",
        "## Model assumptions",
        "",
        assumptions_table(),
        "",
        "## Supply by source",
        "",
        f"![sources]({figs[0].name})",
        "",
        table_world(data, SOURCES),
        "",
        "## Supply and demand balance",
        "",
        "Supplies above the axis (extraction by source, desalination, wastewater"
        " reuse), withdrawals below (irrigation, municipal, industry, electricity"
        " and other energy). Seawater for cooling is left out of both sides. The"
        " line is supply minus demand: transfer losses, return flows and the"
        " overlap between extraction and reuse.",
        "",
        f"![balance]({balance.name})",
        "",
        "## Differences from the reference (World, MCM/yr)",
        "",
    ]
    diff_rows = []
    for var, name in {**SOURCES, **{v: v.split("|")[-1] for v in OTHER}}.items():
        base = world(data[ref], var)
        for label, df in data.items():
            if label == ref:
                continue
            d = world(df, var).subtract(base, fill_value=0.0)
            diff_rows.append(
                {
                    "variable": name,
                    "scenario": label,
                    **{y: d.get(y, float("nan")) for y in YEARS},
                }
            )
    if diff_rows:
        lines.append(
            md_table(pd.DataFrame(diff_rows).set_index(["variable", "scenario"]))
        )
    lines += ["", "## Fossil groundwater and desalination by region, 2050", ""]
    lines += [f"![{f.stem}]({f.name})" for f in figs[1:]]
    lines += [
        "",
        "## Other variables (World)",
        "",
        table_world(data, {v: v for v in OTHER}),
    ]
    md = a.out / f"{a.name}.md"
    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(md)
    if a.pdf:
        print(build_pdf(md))


if __name__ == "__main__":
    main()
