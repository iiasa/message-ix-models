"""Combined SPARCCLE adaptation report: summary + power plants + water + buildings.

Usage:
  python combined_report.py [--pdf]

Assembles ``results/report_all.md`` (and ``.pdf``) in ``adapt/results/`` from
the three sector reports already produced there (``results/report_p.md``,
``water/results/report_w.md``, ``buildings/results/report_b.md``), each demoted
one heading level and with its figure paths rewritten, and prefixes a
cross-sector summary computed from the sector data:

- power: ``results/tables/emissions_world.csv`` and the objective table of
  ``report_p.md`` (scenarios BAU / malad35 / nofresh35);
- water: the ``*_nexus.csv`` reporting in ``water/results/data`` and
  ``water/results/data/emissions_ssp2.csv`` (EMISS and OBJ pulled from the
  scenarios; scenarios BAUfgd / maladapt / +adapt);
- buildings: the IMAGE xlsx in ``buildings/`` (BAU / MALADAPT / POSITIVEADAPT2).
"""

import argparse
import importlib.util
import re
from pathlib import Path

import pandas as pd

ADAPT = Path(__file__).resolve().parents[1]
POWER = ADAPT / "results"
WATER = ADAPT / "water" / "results"
BUILD = ADAPT / "buildings"

#: Role -> scenario key per sector.
ROLES = {
    "BAU": {"power": "BAU", "water": "BAUfgd", "buildings": "BAU"},
    "maladaptation": {"power": "malad35", "water": "maladapt", "buildings": "MALADAPT"},
    "+adaptation": {
        "power": "nofresh35",
        "water": "+adapt",
        "buildings": "POSITIVEADAPT2",
    },
}
YEAR = 2050


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def demote(md: str, title: str, fig_prefix: str) -> str:
    """`md` with its H1 replaced by `title`, every heading one level deeper and
    relative figure paths prefixed with `fig_prefix`."""
    lines = []
    for line in md.splitlines():
        if line.startswith("# "):
            continue
        if line.startswith("#"):
            line = "#" + line
        line = re.sub(
            r"!\[([^\]]*)\]\((?!https?://|/)([^)]+)\)", rf"![\1]({fig_prefix}\2)", line
        )
        lines.append(line)
    return f"# {title}\n\n" + "\n".join(lines).strip() + "\n"


def pct(a: float, b: float) -> float:
    return (a - b) / abs(b) * 100 if b else float("nan")


def fmt(x, digits=1) -> str:
    return "" if x != x else f"{x:+.{digits}f} %"


def power_numbers() -> dict:
    em = pd.read_csv(POWER / "tables" / "emissions_world.csv")
    co2 = em[(em.species == "CO2") & (em.year == YEAR)].set_index("scenario")["value"]
    obj = {}
    for line in (POWER / "report_p.md").read_text(encoding="utf-8").splitlines():
        m = re.match(r"\| (\w+) \| [^|]* \| [^|]* \| ([\d,]+) \|", line)
        if m:
            obj[m.group(1)] = float(m.group(2).replace(",", ""))
    return {"co2": co2, "obj": obj}


def water_numbers(wr) -> dict:
    data = {}
    for role, keys in ROLES.items():
        f = (
            WATER
            / "data"
            / f"SSP_SSP2_v6.6_sp_CurrentPolicies_SSP2_W_{keys['water']}_nexus.csv"
        )
        if f.exists():
            data[keys["water"]] = wr.load(f)
    out = {"data": data, "co2": {}, "obj": {}}
    f = WATER / "data" / "emissions_ssp2.csv"
    if f.exists():
        em = pd.read_csv(f)
        out["co2"] = em[(em.variable == "EMISS|CO2") & (em.year == YEAR)].set_index(
            "scenario"
        )["value"]
        out["obj"] = em[em.variable == "OBJ"].set_index("scenario")["value"]
    return out


def buildings_numbers(br) -> dict:
    data = {label: br.load(path) for label, path in br.DEFAULT_FILES.items()}
    return {"data": data}


def summary_table(wr, br, p, w, b) -> str:
    rows = []
    for role in ("maladaptation", "+adaptation"):
        k = ROLES[role]
        kb = ROLES["BAU"]
        # power
        p_co2 = pct(p["co2"].get(k["power"]), p["co2"].get(kb["power"]))
        p_obj = pct(p["obj"].get(k["power"]), p["obj"].get(kb["power"]))
        # water
        w_co2 = pct(
            w["co2"].get(k["water"], float("nan")),
            w["co2"].get(kb["water"], float("nan")),
        )
        w_obj = pct(
            w["obj"].get(k["water"], float("nan")),
            w["obj"].get(kb["water"], float("nan")),
        )
        # buildings
        bd, bb = b["data"].get(k["buildings"]), b["data"].get(kb["buildings"])
        b_co2 = pct(
            br.series(bd, "Emissions|CO2|Energy").get(YEAR),
            br.series(bb, "Emissions|CO2|Energy").get(YEAR),
        )
        b_inv = pct(
            br.series(bd, "Investment|Energy Supply").get(YEAR),
            br.series(bb, "Investment|Energy Supply").get(YEAR),
        )
        rows.append(
            {
                "sector": "power plants",
                "scenario": f"{role} ({k['power']})",
                f"CO2 {YEAR} vs BAU": fmt(p_co2, 2),
                "cost vs BAU": fmt(p_obj, 3) and fmt(p_obj, 3) + " (objective)",
            }
        )
        rows.append(
            {
                "sector": "water",
                "scenario": f"{role} ({k['water']})",
                f"CO2 {YEAR} vs BAU": fmt(w_co2, 2),
                "cost vs BAU": fmt(w_obj, 2) and fmt(w_obj, 2) + " (objective)",
            }
        )
        rows.append(
            {
                "sector": "buildings (IMAGE)",
                "scenario": f"{role} ({k['buildings']})",
                f"CO2 {YEAR} vs BAU": fmt(b_co2, 1),
                "cost vs BAU": fmt(b_inv, 1)
                and fmt(b_inv, 1) + " (energy-supply investment)",
            }
        )
    t = pd.DataFrame(rows).set_index(["sector", "scenario"])
    return wr.md_table(t)


def physical_table(wr, br, w, b) -> str:
    """Key physical indicators per sector and role, World, YEAR."""
    rows = []
    wd = w["data"]
    for role, k in ROLES.items():
        d = wd.get(k["water"])
        if d is not None:
            rows.append(
                {
                    "sector": "water",
                    "scenario": f"{role} ({k['water']})",
                    "indicator": "fossil groundwater / desalination / urban reuse (MCM/yr)",  # noqa: E501
                    f"{YEAR}": " / ".join(
                        f"{wr.world(d, v).get(YEAR, float('nan')):,.0f}"
                        for v in (
                            "Water Extraction|Fossil Groundwater",
                            "Water Desalination",
                            "Water Waste|Reuse|Urban",
                        )
                    ),
                }
            )
        bd = b["data"].get(k["buildings"])
        if bd is not None:
            ac = br.series(
                bd,
                "Final Energy|Residential|Space Cooling|Electricity|Air Conditioning",
            ).get(YEAR)
            fan = br.series(
                bd,
                "Final Energy|Residential|Space Cooling|Electricity|Fans and Evaporative Coolers",  # noqa: E501
            ).get(YEAR)
            vul = br.series(bd, "Population|Vulnerable|Heat").get(YEAR)
            rows.append(
                {
                    "sector": "buildings",
                    "scenario": f"{role} ({k['buildings']})",
                    "indicator": "AC electricity (EJ) / fans+coolers (EJ) / heat-vulnerable people (million)",  # noqa: E501
                    f"{YEAR}": f"{ac:,.1f} / {fan:,.1f} / {vul:,.0f}",
                }
            )
    cs = pd.read_csv(POWER / "tables" / "cooling_shares_world.csv")
    for role, k in ROLES.items():
        r = cs[(cs.scenario == k["power"]) & (cs.year == YEAR)]
        if r.empty:
            continue
        r = r.iloc[0]
        rows.append(
            {
                "sector": "power plants",
                "scenario": f"{role} ({k['power']})",
                "indicator": "cooling shares once-through fresh / saline / closed-loop / air",  # noqa: E501
                f"{YEAR}": f"{r.ot_fresh:.0%} / {r.ot_saline:.0%} / {r.cl_fresh:.0%} / {r.air:.0%}",  # noqa: E501
            }
        )
    t = pd.DataFrame(rows).set_index(["sector", "scenario"])
    return wr.md_table(t)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pdf", action="store_true")
    a = p.parse_args()
    wr = _load("water_report", ADAPT / "water" / "scripts" / "water_report.py")
    br = _load("buildings_report", BUILD / "buildings_report.py")

    pw, wt, bl = power_numbers(), water_numbers(wr), buildings_numbers(br)

    intro = [
        "# SPARCCLE adaptation scenarios: power plants, water and buildings",
        "",
        "One report for the three sectors of the SPARCCLE adaptation protocol on the",
        "SSP2 current-policies starter. Power plants and water are MESSAGEix-GLOBIOM",
        "(`sparccle.adapt`, `sparccle.adapt.water`, UniCC runs of 2026-10-14/15 and",
        "2026-10-21/27); buildings are IMAGE runs supplied as IAMC files. Each sector",
        "chapter is the sector report as generated; this summary lines the three up.",
        "",
        "## Scenario mapping",
        "",
        "| role | power plants | water | buildings (IMAGE) |",
        "|---|---|---|---|",
        "| BAU | `BAU` (SSP2 cooling-share caps; = base) | `BAUfgd` (e-flows, growth cap, fossil GW 0.4 USD/m³, desalination freed, Mf banned) | `SSP2_M_CP_BAU` |",  # noqa: E501
        "| maladaptation | `malad35` (no new once-through cooling in WEU/EEU/NAM from 2035) | `maladapt` (as BAUfgd, fossil GW 0.7 USD/m³) | `SSP2_M_CP_MALADAPT` (AC-led cooling) |",  # noqa: E501
        "| +adaptation | `nofresh35` (no new freshwater cooling from 2035, power sector free) | `+adapt` (SDG build: efficient distribution, treatment and recycling targets; fossil GW 0.4) | `SSP2_M_CP_POSITIVEADAPT2` (passive cooling, fans and coolers) |",  # noqa: E501
        "",
        "Power-plant `base` is the cooling + climate-impact scenario `_cooling_CI_p`;",
        "water `base` is the plain nexus build without e-flows. CO2 for power and water",  # noqa: E501
        "is the World energy-system total of the MESSAGE solution; for buildings it is",
        "IMAGE's `Emissions|CO2|Energy`. Costs are the MESSAGE objective (total",
        "discounted system cost) for power and water; IMAGE reports no demand-side",
        "investment, so energy-supply investment stands in for buildings.",
        "",
        f"## Cross-sector summary, {YEAR}",
        "",
        summary_table(wr, br, pw, wt, bl),
        "",
        physical_table(wr, br, wt, bl),
        "",
        "Reading across the sectors:",
        "",
        "- **Power plants.** Maladaptation and BAU are the base within 0.2 % on every",
        "  variable; the once-through ban only reroutes 16 GW of cooling to closed loop.",  # noqa: E501
        "  +adaptation (no new freshwater cooling) costs 0.24 % and moves CO2 by about",
        "  −1 % mid-century: nuclear and coal lose most from dry cooling, gas is unchanged.",  # noqa: E501
        "- **Water.** Pricing fossil groundwater is what moves the system. At 0.4 USD/m³",  # noqa: E501
        "  (BAUfgd) desalination is still not built because membrane plants cost ~0.54",
        "  USD/m³ with their heat input; at 0.7 (maladaptation) desalination appears",
        "  (34,000 MCM/yr in 2050) and fossil groundwater falls 16 %, at +4.6 % system",
        "  cost. +adaptation reduces withdrawals from every source through efficiency",
        "  and reuse at almost no cost (+0.03 %), but builds no desalination either.",
        "- **Buildings.** Both IMAGE adaptation runs give the same 8.3 billion people",
        "  air-conditioning access and cut heat-vulnerable population by 90 %; the",
        "  maladaptation run does it with 3.5 times the BAU cooling electricity, the",
        "  +adaptation run with 2.6 times, doubling passive-cooled floor space and",
        "  shifting to fans and coolers. That difference is 3 points of energy-system",
        "  CO2 and 7 points of electricity-supply investment in 2050.",
        "",
    ]
    chapters = [
        demote(
            (POWER / "report_p.md").read_text(encoding="utf-8"),
            "1. Power plants and cooling",
            "",
        ),
        demote(
            (WATER / "report_w.md").read_text(encoding="utf-8"),
            "2. Water",
            "../water/results/",
        ),
        demote(
            (BUILD / "results" / "report_b.md").read_text(encoding="utf-8"),
            "3. Buildings (IMAGE)",
            "../buildings/results/",
        ),
    ]
    out = POWER / "report_all.md"
    out.write_text("\n".join(intro) + "\n" + "\n\n".join(chapters), encoding="utf-8")
    print(out)
    if a.pdf:
        print(wr.build_pdf(out))


if __name__ == "__main__":
    main()
