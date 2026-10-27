"""Compare IMAGE buildings scenarios (IAMC xlsx): markdown report + figures.

Usage:
  python buildings_report.py [--out DIR] [--name report_b] [--pdf] [LABEL=file.xlsx ...]

Defaults: the three ``SSP2_M_CP_*.xlsx`` files beside this script (BAU,
MALADAPT, POSITIVEADAPT2), output to ``results/`` beside it. The first file is
the reference. Reads the ``data`` sheet (IAMC wide format). World values are
used where the file carries them; several variables are reported by region
only and are then summed (extensive units) or averaged (prices) over regions.
The source model is IMAGE, not MESSAGEix.
"""

import argparse
import importlib.util
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

HERE = Path(__file__).resolve().parent
DEFAULT_FILES = {
    "BAU": HERE / "SSP2_M_CP_BAU.xlsx",
    "MALADAPT": HERE / "SSP2_M_CP_MALADAPT.xlsx",
    "POSITIVEADAPT2": HERE / "SSP2_M_CP_POSITIVEADAPT2.xlsx",
}
YEARS = [2030, 2050, 2070, 2100]
COLORS = ["#4477aa", "#ee6677", "#228833", "#ccbb44", "#66ccee", "#aa3377"]

FE = "Final Energy|Residential"
ES = "Energy Service|Residential"
#: World indicators tabulated: variable -> (label, scale)
KEY = {
    "Final Energy|Residential and Commercial": ("final energy, buildings (EJ/yr)", 1),
    f"{FE}|Electricity": ("residential electricity (EJ/yr)", 1),
    f"{FE}|Cooling Appliance|Electricity": (
        "cooling appliances electricity (EJ/yr)",
        1,
    ),
    f"{FE}|Space Cooling|Electricity|Air Conditioning": ("  air conditioning", 1),
    f"{FE}|Space Cooling|Electricity|Fans and Evaporative Coolers": (
        "  fans and evaporative coolers",
        1,
    ),
    f"{FE}|Space Heating": ("residential space heating (EJ/yr)", 1),
    "Emissions|CO2|Energy|Demand|Residential and Commercial": (
        "CO2, buildings direct (Mt/yr)",
        1,
    ),
    "Emissions|CO2|Energy": ("CO2, energy system (Mt/yr)", 1),
    "Secondary Energy|Electricity": ("electricity generation (EJ/yr)", 1),
    "Investment|Energy Supply|Electricity": ("investment, electricity (bn USD/yr)", 1),
    "Investment|Energy Supply": ("investment, energy supply (bn USD/yr)", 1),
    "Price|Final Energy|Residential|Electricity": (
        "residential electricity price (USD/GJ, regional mean)",
        1,
    ),
    "Consumption": ("consumption (bn USD/yr)", 1),
    "GDP|PPP": ("GDP PPP (bn USD/yr)", 1),
    "Population|Appliance Access|Cooling|Air conditioner|Total": (
        "people with AC access (million)",
        1,
    ),
    "Population|Vulnerable|Heat": ("people vulnerable to heat (million)", 1),
    f"{ES}|Floor Space|Passive Cooling and Shading": (
        "floor space with passive cooling/shading (bn m2)",
        1e-9,
    ),
    f"{ES}|Cooling Degree Days": ("cooling degree days", 1),
}
#: Variables averaged rather than summed when World is missing.
INTENSIVE = ("Price|", "Cooling Degree Days", "Heating Degree Days")
APPLIANCES = ["Air Conditioner", "Air Cooler", "Fan"]
COOLING_FE = {
    f"{FE}|Space Cooling|Electricity|Air Conditioning": "air conditioning",
    f"{FE}|Space Cooling|Electricity|Fans": "fans",
    f"{FE}|Space Cooling|Electricity|Evaporative Coolers": "evaporative coolers",
}


def _water_report():
    """The water report module, for its md_table and build_pdf helpers."""
    path = HERE.parent / "water" / "scripts" / "water_report.py"
    spec = importlib.util.spec_from_file_location("water_report", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load(path: Path) -> pd.DataFrame:
    """Long frame region, variable, unit, year, value from an IAMC xlsx data sheet."""
    df = pd.read_excel(path, sheet_name="data")
    df.columns = [str(c).lower() for c in df.columns]
    years = [c for c in df.columns if c.isdigit()]
    out = df.melt(
        id_vars=["region", "variable", "unit"], value_vars=years, var_name="year"
    )
    out["year"] = out["year"].astype(int)
    return out.dropna(subset=["value"])


def series(df: pd.DataFrame, variable: str, region: str = "World") -> pd.Series:
    """`variable` by year in `region`; World falls back to the regional sum or mean."""
    sel = df[df["variable"] == variable]
    s = sel[sel["region"] == region].set_index("year")["value"].sort_index()
    if region == "World" and (s.empty or (s.abs() < 1e-12).all()):
        regional = sel[sel["region"] != "World"].groupby("year")["value"]
        s = regional.mean() if variable.startswith(INTENSIVE) else regional.sum()
    return s


def unit(df: pd.DataFrame, variable: str) -> str:
    u = df.loc[df["variable"] == variable, "unit"]
    return "" if u.empty else str(u.iloc[0])


def ownership(df: pd.DataFrame, appliance: str) -> pd.Series:
    """Units per household, World: mean of the urban and rural values."""
    u = series(df, f"{ES}|Urban|Appliance Ownership|Cooling|{appliance}")
    r = series(df, f"{ES}|Rural|Appliance Ownership|Cooling|{appliance}")
    return u.add(r, fill_value=0.0) / 2


def fig_cooling_fe(data: dict[str, pd.DataFrame], out: Path) -> Path:
    n = len(data)
    fig, axes = plt.subplots(1, n, figsize=(4.2 * n, 3.8), sharey=True)
    axes = [axes] if n == 1 else list(axes)
    for ax, (label, df) in zip(axes, data.items()):
        t = pd.DataFrame({name: series(df, v) for v, name in COOLING_FE.items()})
        t = t.fillna(0.0)
        t = t[t.index >= 2020]
        ax.stackplot(
            t.index, t.T.values, labels=t.columns, colors=COLORS[: len(t.columns)]
        )
        ax.set_title(label)
        ax.set_xlabel("year")
        ax.grid(alpha=0.3)
    axes[0].set_ylabel("EJ/yr")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=8, frameon=False)
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    path = out / "fig_b_cooling_fe.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def fig_ownership(data: dict[str, pd.DataFrame], out: Path) -> Path:
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.6))
    for ax, appliance in zip(axes, APPLIANCES):
        for (label, df), c in zip(data.items(), COLORS):
            s = ownership(df, appliance)
            s = s[s.index >= 2020]
            ax.plot(s.index, s.values, label=label, color=c, lw=1.6)
        ax.set_title(f"{appliance} (World, urban/rural mean)")
        ax.set_ylabel("units per household")
        ax.set_xlabel("year")
        ax.grid(alpha=0.3)
    axes[0].legend(frameon=False, fontsize=8)
    fig.tight_layout()
    path = out / "fig_b_ownership.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def fig_quintiles(data: dict[str, pd.DataFrame], out: Path, year: int = 2050) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), sharey=True)
    for ax, setting in zip(axes, ("Urban", "Rural")):
        rows = {}
        for label, df in data.items():
            base = f"{ES}|{setting}|Appliance Ownership|Cooling|Air Conditioner"
            rows[label] = [
                series(df, f"{base}|Q{q}").get(year, float("nan")) for q in range(1, 6)
            ]
        t = pd.DataFrame(rows, index=[f"Q{q}" for q in range(1, 6)])
        t.plot.bar(ax=ax, color=COLORS[: len(t.columns)], width=0.8)
        ax.set_title(f"AC ownership by income quintile, {setting}, {year}")
        ax.set_ylabel("units per household")
        ax.grid(axis="y", alpha=0.3)
        ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    path = out / f"fig_b_ac_quintiles_{year}.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def fig_regions(
    data: dict[str, pd.DataFrame], variable: str, year: int, out: Path, stem: str
) -> Path:
    rows = {}
    for label, df in data.items():
        sel = df[(df["variable"] == variable) & (df["year"] == year)]
        sel = sel[sel["region"] != "World"]
        rows[label] = sel.set_index("region")["value"]
    t = pd.DataFrame(rows).fillna(0.0)
    t = t.loc[t.max(axis=1).sort_values(ascending=False).index[:14]]
    fig, ax = plt.subplots(figsize=(9, 3.6))
    t.plot.bar(ax=ax, color=COLORS[: len(t.columns)], width=0.8)
    ax.set_title(f"{variable}, {year}")
    ax.set_ylabel(unit(next(iter(data.values())), variable))
    ax.grid(axis="y", alpha=0.3)
    ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    path = out / f"fig_b_{stem}_{year}.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def table_key(data: dict[str, pd.DataFrame], md_table) -> str:
    rows = []
    for v, (name, scale) in KEY.items():
        for label, df in data.items():
            s = series(df, v) * scale
            if s.empty:
                continue
            rows.append(
                {
                    "indicator": name,
                    "scenario": label,
                    **{y: s.get(y, float("nan")) for y in YEARS},
                }
            )
    return md_table(pd.DataFrame(rows).set_index(["indicator", "scenario"]))


def table_diff(data: dict[str, pd.DataFrame], md_table) -> str:
    rows = []
    ref = next(iter(data))
    for v, (name, _) in KEY.items():
        base = series(data[ref], v)
        if base.empty:
            continue
        for label, df in data.items():
            if label == ref:
                continue
            s = series(df, v)
            d = {}
            for y in YEARS:
                b, x = base.get(y, float("nan")), s.get(y, float("nan"))
                d[y] = (x - b) / abs(b) * 100 if b == b and b != 0 else float("nan")
            rows.append({"indicator": name, "scenario": label, **d})
    return md_table(pd.DataFrame(rows).set_index(["indicator", "scenario"]))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, default=HERE / "results")
    p.add_argument("--name", default="report_b")
    p.add_argument("--pdf", action="store_true")
    p.add_argument("inputs", nargs="*", help="LABEL=file.xlsx (default: the 3 files)")
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    wr = _water_report()

    if a.inputs:
        files = {k: Path(v) for k, _, v in (i.partition("=") for i in a.inputs)}
    else:
        files = DEFAULT_FILES
    data = {label: load(path) for label, path in files.items()}
    ref = next(iter(data))

    figs = [
        fig_cooling_fe(data, a.out),
        fig_ownership(data, a.out),
        fig_quintiles(data, a.out),
        fig_regions(
            data,
            f"{FE}|Cooling Appliance|Electricity",
            2050,
            a.out,
            "cooling_fe_regions",
        ),
        fig_regions(data, "Population|Vulnerable|Heat", 2050, a.out, "heat_vulnerable"),
    ]
    lines = [
        "# Buildings scenarios (IMAGE): cooling, appliances, emissions, cost",
        "",
        "Source: IMAGE SSP2 current-policies runs supplied as IAMC xlsx;",
        f"reference **{ref}**.",
        "World values unless stated. The water scenarios of this folder come from",
        "MESSAGEix-Nexus and are reported in `../water/results/report_w.md`.",
        "",
        "## Key indicators (World)",
        "",
        table_key(data, wr.md_table),
        "",
        "## Differences from the reference (%)",
        "",
        table_diff(data, wr.md_table),
        "",
        "## Space-cooling electricity by appliance",
        "",
        f"![cooling]({figs[0].name})",
        "",
        "## Appliance ownership",
        "",
        f"![ownership]({figs[1].name})",
        "",
        f"![quintiles]({figs[2].name})",
        "",
        "## Regions, 2050",
        "",
        f"![regions]({figs[3].name})",
        "",
        f"![heat]({figs[4].name})",
        "",
        "## Notes",
        "",
        "- No demand-side (buildings) investment variable is reported; cost is read",
        "  from energy-supply investment, consumption, GDP and the residential",
        "  electricity price. Consumption and GDP are identical across the three",
        "  files, so IMAGE did not feed the adaptation cost back to the economy.",
        "- Final energy and price variables carry no World row; they are summed",
        "  (prices: averaged) over the 26 IMAGE regions.",
        "- Appliance ownership is the mean of the urban and rural values.",
        "- CO2: direct buildings emissions plus energy-system emissions; the",
        "  electricity used by appliances shows up in the latter.",
    ]
    md = a.out / f"{a.name}.md"
    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(md)
    if a.pdf:
        print(wr.build_pdf(md))


if __name__ == "__main__":
    main()
