"""Maladaptation vs +adaptation, three sectors, three indicators.

Usage:
  python malad_vs_adapt.py [--start 2025] [--end 2050]

For power plants (`malad35` vs `nofresh35`), water (`maladapt` vs `+adapt`) and
buildings (IMAGE `MALADAPT` vs `POSITIVEADAPT2`) the World values over
`--start`..`--end` (default 2025-2050, every calendar year, linear
interpolation between the reported years) of

- cumulative CO2 emissions of the energy system (Gt CO2),
- average annual electricity supply (`Secondary Energy|Electricity`, EJ/yr),
- average annual investment in electricity supply (billion USD/yr),

and the difference +adaptation minus maladaptation. The water scenarios carry
no values before their first model year 2035; 2025 and 2030 are taken from the
power-plant base run on the same starter (identical history, so the
difference is unaffected and only the level is filled). Sources: the legacy IAMC
xlsx of the power runs (`results/data/reports`), the IMAGE xlsx in
`buildings/`, and `water/results/data/elec_ssp2.csv` pulled from the water
scenarios on UniCC (generation = ACT x output of technologies feeding
`electr` at `secondary`; investment = CAP_NEW x inv_cost of those plus
`elec_t_d`; CO2 = EMISS of `TCE_CO2_FFI`, fossil fuel and industry). Writes `results/tables/malad_vs_adapt.csv`
(levels and differences) and prints the two markdown tables used in
`report_all.md`.
"""

import argparse
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ADAPT = Path(__file__).resolve().parents[1]
POWER_XLSX = (
    ADAPT / "results" / "data" / "reports"
    / "SSP_SSP2_v6.6_sp_NPiREF_GDP_CI_50_ensemble_2_cooling_CI_p_{key}.xlsx"
)
BUILD_XLSX = ADAPT / "buildings" / "SSP2_M_CP_{key}.xlsx"
WATER_CSV = ADAPT / "water" / "results" / "data" / "elec_ssp2.csv"
OUT_CSV = ADAPT / "results" / "tables" / "malad_vs_adapt.csv"
OUT_FIG = ADAPT / "results" / "figures" / "malad_vs_adapt.png"
OUT_XLSX = ADAPT / "results" / "tables" / "malad_vs_adapt.xlsx"
#: Power-plant base run: history (2025, 2030) shared with the water scenarios
POWER_BASE_XLSX = POWER_XLSX.with_name(POWER_XLSX.name.format(key="")[:-6] + ".xlsx")
#: Fixed sector colours (blue, aqua, orange: the three all-pairs-safe slots).
COLORS = {"power plants": "#2a78d6", "water": "#1baf7a", "buildings": "#eb6834"}

START, END = 2025, 2050
#: Reported years used to interpolate the window
GRID = [2025, 2030, 2035, 2040, 2045, 2050]
PAIR = {  # sector -> (maladaptation key, +adaptation key)
    "power plants": ("malad35", "nofresh35"),
    "water": ("maladapt", "+adapt"),
    "buildings": ("MALADAPT", "POSITIVEADAPT2"),
}
#: (name, unit, aggregation over the window): annual series -> one number
INDICATORS = [
    ("cumulative CO2 emissions, energy", "Gt CO2", lambda s: s.sum() / 1000),
    ("average electricity supply", "EJ/yr", lambda s: s.mean()),
    ("average electricity-supply investment", "billion USD/yr", lambda s: s.mean()),
]
#: EMISS of the water scenarios: Mt C/yr -> Mt CO2/yr
C_TO_CO2 = 44 / 12


def _wide_world(path: Path) -> pd.DataFrame:
    """World rows of an IAMC wide xlsx, index variable, columns int years."""
    df = pd.read_excel(path)
    if "Region" not in df.columns:  # IMAGE files carry the table on sheet "data"
        df = pd.read_excel(path, sheet_name="data")
    df = df[df["Region"] == "World"].set_index("Variable")
    df.columns = [int(c) if str(c).isdigit() else c for c in df.columns]
    return df


def _iamc_indicators(df: pd.DataFrame, years: list) -> dict:
    co2 = "Emissions|CO2|Energy" if "Emissions|CO2|Energy" in df.index else "Emissions|CO2"
    return {
        INDICATORS[0][0]: df.loc[co2, years],
        INDICATORS[1][0]: df.loc["Secondary Energy|Electricity", years],
        INDICATORS[2][0]: df.loc["Investment|Energy Supply|Electricity", years],
    }


def power(key: str, years: list) -> dict:
    return _iamc_indicators(_wide_world(POWER_XLSX.with_name(POWER_XLSX.name.format(key=key))), years)


def buildings(key: str, years: list) -> dict:
    return _iamc_indicators(_wide_world(BUILD_XLSX.with_name(BUILD_XLSX.name.format(key=key))), years)


def water(key: str, years: list) -> dict:
    df = pd.read_csv(WATER_CSV)
    df = df[df.scenario == key]

    hist = _iamc_indicators(_wide_world(POWER_BASE_XLSX), [y for y in years if y < 2035])

    def series(name, variables, factor=1.0):
        s = df[df.variable.isin(variables)].groupby("year")["value"].sum() * factor
        s = s.reindex(years)
        for y in hist[name].index:  # history shared with the power runs
            s[y] = hist[name][y]
        return s

    return {
        # fossil-fuel and industry CO2 (Mt C -> Mt CO2); the plain `CO2` species
        # of EMISS is a partial accounting (~550 Mt C) and not comparable
        INDICATORS[0][0]: series(INDICATORS[0][0], ["EMISS|TCE_CO2_FFI"], C_TO_CO2),
        INDICATORS[1][0]: series(INDICATORS[1][0], ["Secondary Energy|Electricity"]),
        INDICATORS[2][0]: series(
            INDICATORS[2][0],
            [
                "Investment|Energy Supply|Electricity|Generation",
                "Investment|Energy Supply|Electricity|Transmission and Distribution",
            ],
        ),
    }


LOADERS = {"power plants": power, "water": water, "buildings": buildings}


def window_label(years: list) -> str:
    return f"{min(years)}-{max(years)}"


def annual(s: pd.Series, years: list) -> pd.Series:
    """`s` (indexed by reported years) interpolated to every calendar year of the window."""
    s = s.astype(float).dropna()
    full = pd.Index(range(min(years), max(years) + 1))
    return s.reindex(s.index.union(full)).interpolate(method="index").reindex(full)


def collect(years: list) -> pd.DataFrame:
    """Long frame: sector, indicator, unit, window, maladaptation, adaptation, diff, diff_pct."""
    rows = []
    for sector, (km, ka) in PAIR.items():
        try:
            m, a = LOADERS[sector](km, years), LOADERS[sector](ka, years)
        except FileNotFoundError as e:
            print(f"{sector}: skipped ({e})")
            continue
        for ind, unit, agg in INDICATORS:
            vm, va = float(agg(annual(m[ind], years))), float(agg(annual(a[ind], years)))
            rows.append(
                dict(
                    sector=sector, indicator=ind, unit=unit, window=window_label(years),
                    maladaptation=vm, adaptation=va, diff=va - vm,
                    diff_pct=(va - vm) / abs(vm) * 100 if vm else float("nan"),
                )
            )
    return pd.DataFrame(rows)


def _num(x: float, signed: bool = False) -> str:
    digits = 0 if abs(x) >= 1000 else 1 if abs(x) >= 10 else 2
    return f"{x:{'+' if signed else ''},.{digits}f}"


def diff_table(df: pd.DataFrame, years: list) -> str:
    """Levels and +adaptation minus maladaptation over the window."""
    lines = [
        f"| sector | indicator ({window_label(years)}) | unit | maladaptation | "
        "+adaptation | difference | relative |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for _, r in df.iterrows():
        lines.append(
            f"| {r.sector} | {r.indicator} | {r.unit} | {_num(r.maladaptation)} | "
            f"{_num(r.adaptation)} | {_num(r['diff'], True)} | {r.diff_pct:+.1f} % |"
        )
    return "\n".join(lines)


def _cell(r) -> tuple:
    """(big, small) text of one matrix cell: relative change, absolute in brackets."""
    return f"{r.diff_pct:+.1f} %", f"({_num(r['diff'], True)} {r.unit})"


def matrix_table(df: pd.DataFrame, years: list) -> str:
    """Sectors as rows, indicators as columns, relative change with the absolute in brackets."""
    cols = [i[0] for i in INDICATORS]
    lines = [
        f"| {window_label(years)} | " + " | ".join(cols) + " |",
        "|---|" + "|".join("---:" for _ in cols) + "|",
    ]
    for sector in PAIR:
        sub = df[df.sector == sector].set_index("indicator")
        cells = []
        for c in cols:
            if c in sub.index:
                big, small = _cell(sub.loc[c])
                cells.append(f"**{big}** {small}")
            else:
                cells.append("")
        lines.append(f"| {sector} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_xlsx(df: pd.DataFrame, years: list, path: Path = OUT_XLSX) -> Path:
    """The matrix as a copy-ready sheet: % in large bold, absolute below in brackets;
    second sheet with the long numbers."""
    from openpyxl import Workbook
    from openpyxl.cell.rich_text import CellRichText, TextBlock
    from openpyxl.cell.text import InlineFont
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "matrix"
    cols = [i[0] for i in INDICATORS]
    head_font = Font(name="Arial", bold=True, size=11, color="FFFFFF")
    fill = PatternFill("solid", fgColor="2F4F6F")
    thin = Side(style="thin", color="BFBFBF")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    ws["A1"] = f"+adaptation vs maladaptation, World, {window_label(years)}"
    ws["A1"].font = Font(name="Arial", bold=True, size=12)
    ws.cell(row=2, column=1, value="sector").font = head_font
    ws.cell(row=2, column=1).fill = fill
    for j, c in enumerate(cols, start=2):
        cell = ws.cell(row=2, column=j, value=c)
        cell.font, cell.fill = head_font, fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    big = InlineFont(rFont="Arial", b=True, sz=20)
    small = InlineFont(rFont="Arial", sz=10, color="595959")
    for i, sector in enumerate(PAIR, start=3):
        c0 = ws.cell(row=i, column=1, value=sector)
        c0.font = Font(name="Arial", bold=True, size=11)
        c0.alignment = Alignment(vertical="center")
        c0.border = border
        sub = df[df.sector == sector].set_index("indicator")
        for j, c in enumerate(cols, start=2):
            cell = ws.cell(row=i, column=j)
            if c in sub.index:
                b, s_ = _cell(sub.loc[c])
                cell.value = CellRichText(TextBlock(big, b), TextBlock(small, "\n" + s_))
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = border
        ws.row_dimensions[i].height = 48
    ws.row_dimensions[2].height = 32
    ws.column_dimensions["A"].width = 16
    for j in range(2, len(cols) + 2):
        ws.column_dimensions[get_column_letter(j)].width = 30
    ws.cell(row=len(PAIR) + 4, column=1, value=(
        "Relative change of +adaptation against maladaptation; absolute difference in "
        "brackets. Power plants nofresh35 vs malad35, water +adapt vs maladapt, buildings "
        "IMAGE POSITIVEADAPT2 vs MALADAPT. Annual interpolation between reported years."
    ))
    ws2 = wb.create_sheet("data")
    ws2.append(list(df.columns))
    for r in df.itertuples(index=False):
        ws2.append(list(r))
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


def fig_diff(df: pd.DataFrame, years: list, path: Path = OUT_FIG) -> Path:
    """One panel per indicator, one bar per sector: relative difference
    +adaptation vs maladaptation over the window, labelled with % and absolute."""
    sectors = [s for s in PAIR if s in set(df.sector)]
    fig, axes = plt.subplots(1, len(INDICATORS), figsize=(3.0 * len(INDICATORS), 3.4))
    for ax, (ind, unit, _) in zip(axes, INDICATORS):
        sub = df[df.indicator == ind].set_index("sector").reindex(sectors)
        bars = ax.bar(
            range(len(sectors)), sub.diff_pct, 0.6, color=[COLORS[s] for s in sectors], linewidth=0
        )
        for b, v, d in zip(bars, sub.diff_pct, sub["diff"]):
            if v != v:
                continue
            ax.annotate(
                f"{v:+.1f} %\n({_num(d, True)})", (b.get_x() + b.get_width() / 2, v),
                xytext=(0, 3 if v >= 0 else -3), textcoords="offset points",
                ha="center", va="bottom" if v >= 0 else "top", fontsize=7.5, color="#333333",
            )
        ax.axhline(0, color="#555555", linewidth=0.8)
        ax.set_title(f"{ind}\n({unit})", loc="left", fontsize=9)
        ax.set_xticks(range(len(sectors)))
        ax.set_xticklabels(sectors, fontsize=8)
        lo, hi = ax.get_ylim()
        ax.set_ylim(min(lo, 0) - 0.3 * (hi - lo), max(hi, 0) + 0.3 * (hi - lo))
        ax.tick_params(labelsize=8)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.grid(axis="y", color="#e5e5e5", linewidth=0.6)
        ax.set_axisbelow(True)
    axes[0].set_ylabel("% vs maladaptation", fontsize=8)
    fig.suptitle(
        f"+adaptation relative to maladaptation, World, {window_label(years)}", fontsize=10
    )
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", type=int, default=START)
    p.add_argument("--end", type=int, default=END)
    a = p.parse_args()
    a.years = [y for y in GRID if a.start <= y <= a.end]
    df = collect(a.years)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    print(matrix_table(df, a.years))
    print()
    print(diff_table(df, a.years))
    print(f"wrote {write_xlsx(df, a.years)}")
    print(f"\nwrote {OUT_CSV}")
    print(f"wrote {fig_diff(df, a.years)}")


if __name__ == "__main__":
    main()
