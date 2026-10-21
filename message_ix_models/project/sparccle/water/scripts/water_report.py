"""Compare the water reporting of several scenarios: markdown report + figures.

Usage:
  python water_report.py --out DIR LABEL=<model>_<scenario>_nexus.csv [LABEL=... ...]

The first file is the reference the others are compared with. Input files are
the ``<model>_<scenario>_nexus.csv`` written by ``water-ix report --water``
(IAMC long or wide format). Output: ``DIR/report.md`` and ``DIR/*.png``.
"""

import argparse
from pathlib import Path

import matplotlib

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


def world(df: pd.DataFrame, variable: str) -> pd.Series:
    """World value by year of `variable`; sum of R12 regions when World is absent."""
    sel = df[df["variable"] == variable]
    if (sel["region"] == "World").any():
        return sel[sel["region"] == "World"].groupby("year")["value"].sum()
    return sel[sel["region"].str.startswith("R12_")].groupby("year")["value"].sum()


def by_region(df: pd.DataFrame, variable: str, year: int) -> pd.Series:
    sel = df[(df["variable"] == variable) & (df["year"] == year)]
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
    axes[-1].legend(loc="upper left", fontsize=8, frameon=False)
    fig.tight_layout()
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
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    data: dict[str, pd.DataFrame] = {}
    for item in a.inputs:
        label, _, path = item.partition("=")
        data[label] = load(Path(path))
    ref = next(iter(data))

    figs = [fig_sources(data, a.out)]
    for var in ["Water Extraction|Fossil Groundwater", "Water Desalination"]:
        figs.append(fig_regions(data, var, 2050, a.out))

    lines = [
        "# Water scenarios: supply mix and adaptation levers",
        "",
        f"Reference: **{ref}**. Values are World totals in MCM/yr from "
        "`water-ix report --water` (`*_nexus.csv`).",
        "",
        "## Supply by source",
        "",
        f"![sources]({figs[0].name})",
        "",
        table_world(data, SOURCES),
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
    (a.out / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(a.out / "report.md")


if __name__ == "__main__":
    main()
