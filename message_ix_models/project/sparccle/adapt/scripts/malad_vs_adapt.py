"""Maladaptation vs +adaptation, three sectors, three indicators.

Usage:
  python malad_vs_adapt.py [--years 2035 2040 2045 2050]

For power plants (`malad35` vs `nofresh35`), water (`maladapt` vs `+adapt`) and
buildings (IMAGE `MALADAPT` vs `POSITIVEADAPT2`) the World values over the
window of five-year periods ending in `--years` (default 2031-2050) of

- cumulative CO2 emissions of the energy system (Gt CO2; annual value x 5),
- average annual electricity supply (`Secondary Energy|Electricity`, EJ/yr),
- average annual investment in electricity supply (billion USD/yr),

and the difference +adaptation minus maladaptation. Sources: the legacy IAMC
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
#: Fixed sector colours (blue, aqua, orange: the three all-pairs-safe slots).
COLORS = {"power plants": "#2a78d6", "water": "#1baf7a", "buildings": "#eb6834"}

YEARS = [2035, 2040, 2045, 2050]
PERIOD = 5  # years represented by each model year (period ending in that year)
PAIR = {  # sector -> (maladaptation key, +adaptation key)
    "power plants": ("malad35", "nofresh35"),
    "water": ("maladapt", "+adapt"),
    "buildings": ("MALADAPT", "POSITIVEADAPT2"),
}
#: (name, unit, aggregation over the window): annual series -> one number
INDICATORS = [
    ("cumulative CO2 emissions, energy", "Gt CO2", lambda s: s.sum() * PERIOD / 1000),
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

    def series(variables, factor=1.0):
        s = df[df.variable.isin(variables)].groupby("year")["value"].sum() * factor
        return s.reindex(years)

    return {
        # fossil-fuel and industry CO2 (Mt C -> Mt CO2); the plain `CO2` species
        # of EMISS is a partial accounting (~550 Mt C) and not comparable
        INDICATORS[0][0]: series(["EMISS|TCE_CO2_FFI"], C_TO_CO2),
        INDICATORS[1][0]: series(["Secondary Energy|Electricity"]),
        INDICATORS[2][0]: series(
            [
                "Investment|Energy Supply|Electricity|Generation",
                "Investment|Energy Supply|Electricity|Transmission and Distribution",
            ]
        ),
    }


LOADERS = {"power plants": power, "water": water, "buildings": buildings}


def window_label(years: list) -> str:
    return f"{min(years) - PERIOD + 1}-{max(years)}"


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
            vm, va = float(agg(m[ind].astype(float))), float(agg(a[ind].astype(float)))
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
    p.add_argument("--years", nargs="+", type=int, default=YEARS)
    a = p.parse_args()
    df = collect(a.years)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    print(diff_table(df, a.years))
    print(f"\nwrote {OUT_CSV}")
    print(f"wrote {fig_diff(df, a.years)}")


if __name__ == "__main__":
    main()
