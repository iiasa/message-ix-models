"""Cooling technologies by power plant and cooling type, for solved scenarios.

Read-only. For every cooling variant (``<plant>__ot_fresh``, ``__cl_fresh``,
``__ot_saline``, ``__air``) and its parent plant, writes:

- capacity (CAP), new capacity (CAP_NEW) and activity (ACT);
- input flows (input coefficient x ACT) per commodity: freshwater, saline water,
  parasitic electricity;
- the applied climate-impact parameters: ``capacity_factor`` on the variants (dry
  cooling) and the coefficients and upper bound of the wet-cooling relations.

Usage::

    python cooling_detail.py --scenarios scenarios.txt --out results/

``scenarios.txt`` holds one ``MODEL/SCENARIO`` per line; append ``#VERSION`` to pin a
version. Outputs in ``--out``:

- ``cooling_grid.csv``: long table of all quantities;
- ``cooling_mix.csv``: capacity and activity by plant x cooling type, with shares.
"""

import argparse
from pathlib import Path

import ixmp
import message_ix
import pandas as pd

COOLING_TYPES = ("ot_fresh", "cl_fresh", "ot_saline", "air")

DIMS = {"node_loc": "region", "year_act": "year", "time": "subannual"}
REL_DIMS = {"node_rel": "region", "year_rel": "year", "relation": "label"}


def split_cooling(tech: str) -> tuple[str | None, str | None]:
    """Return (parent, cooling type) for a cooling variant, else (None, None)."""
    parent, _, kind = tech.rpartition("__")
    return (parent, kind) if parent and kind in COOLING_TYPES else (None, None)


def to_long(df, quantity, unit, value_col, dims, agg="sum"):
    """Sum solved quantities (or average coefficients) over dropped dimensions."""
    if df is None or df.empty:
        return None
    df = df.rename(columns={value_col: "value", **dims})
    keep = [
        c for c in ("region", "technology", "year", "subannual", "label") if c in df
    ]
    out = df.groupby(keep, as_index=False)["value"].agg(agg)
    return out.assign(quantity=quantity, unit=unit)


def input_flows(scen, techs):
    """Input coefficient x ACT, one quantity per input commodity."""
    par = scen.par("input", filters={"technology": techs})
    act = scen.var("ACT", filters={"technology": techs})
    on = ["node_loc", "technology", "year_vtg", "year_act", "mode", "time"]
    merged = par.rename(columns={"value": "coeff"}).merge(
        act.rename(columns={"lvl": "act"})[on + ["act"]], on=on
    )
    merged["flow"] = merged["coeff"] * merged["act"]
    return [
        to_long(grp, f"input|{commodity}", f"{grp['unit'].iloc[0]} * GWa", "flow", DIMS)
        for commodity, grp in merged.groupby("commodity")
    ]


def impact_parameters(scen, techs, parents):
    """Dry-cooling capacity factors and the wet-cooling relations.

    The wet limit is ``sum(freshwater variants) - coefficient * parent <= 0``, so the
    climate signal sits in the parent's coefficient. Relations are found through the
    cooling technologies, then read in full; only those touching nothing but cooling
    variants and their parents are kept.
    """
    cf = scen.par("capacity_factor", filters={"technology": techs})
    frames = [to_long(cf, "capacity_factor", "-", "value", DIMS, agg="mean")]

    seed = scen.par("relation_activity", filters={"technology": techs})
    subject = set(techs) | set(parents)
    relations = [
        r
        for r in seed["relation"].unique()
        if set(scen.par("relation_activity", filters={"relation": [r]})["technology"])
        <= subject
    ]
    if relations:
        ra = scen.par("relation_activity", filters={"relation": relations})
        up = scen.par("relation_upper", filters={"relation": relations})
        frames += [
            to_long(ra, "relation_coeff", "-", "value", REL_DIMS, agg="mean"),
            to_long(up, "relation_upper", "-", "value", REL_DIMS, agg="mean"),
        ]
    return frames


def one_scenario(mp, model, scenario, version):
    scen = message_ix.Scenario(mp, model, scenario, version=version)
    all_techs = set(scen.set("technology"))
    techs = sorted(t for t in all_techs if split_cooling(t)[0])
    if not techs:
        print(f"  {model}/{scenario}: no cooling technologies, skipped")
        return None
    parents = sorted({split_cooling(t)[0] for t in techs} & all_techs)

    frames = impact_parameters(scen, techs, parents)
    if scen.has_solution():
        frames += [
            to_long(
                scen.var("CAP", filters={"technology": techs}),
                "capacity",
                "GW",
                "lvl",
                DIMS,
            ),
            to_long(
                scen.var("CAP_NEW", filters={"technology": techs}),
                "new_capacity",
                "GW/yr",
                "lvl",
                {"node_loc": "region", "year_vtg": "year"},
            ),
            to_long(
                scen.var("ACT", filters={"technology": techs}),
                "activity",
                "GWa",
                "lvl",
                DIMS,
            ),
            to_long(
                scen.var("CAP", filters={"technology": parents}),
                "parent_capacity",
                "GW",
                "lvl",
                DIMS,
            ),
            to_long(
                scen.var("ACT", filters={"technology": parents}),
                "parent_activity",
                "GWa",
                "lvl",
                DIMS,
            ),
            *input_flows(scen, techs),
        ]
    else:
        print(f"  {model}/{scenario}: not solved, only impact parameters extracted")

    out = pd.concat([f for f in frames if f is not None], ignore_index=True)
    out = out.assign(model=model, scenario=scenario, version=scen.version)
    for column in ("subannual", "label", "technology"):
        out[column] = out.get(column, pd.Series(dtype=str)).fillna("")

    split = out["technology"].map(split_cooling)
    out["parent"] = [
        p or (t if t in parents else "") for (p, _), t in zip(split, out["technology"])
    ]
    out["cooling_type"] = [c or "" for _, c in split]
    print(f"  {model}/{scenario} v{scen.version}: {len(techs)} cooling technologies")
    return out


def mix_table(grid):
    """Capacity and activity by plant x cooling type, with within-plant shares."""
    sel = grid[
        grid["quantity"].isin(["capacity", "activity"]) & (grid["cooling_type"] != "")
    ]
    idx = [
        "model",
        "scenario",
        "version",
        "region",
        "year",
        "subannual",
        "parent",
        "quantity",
    ]
    wide = sel.pivot_table(
        index=idx, columns="cooling_type", values="value", aggfunc="sum", fill_value=0.0
    ).reset_index()
    types = [c for c in wide.columns if c not in idx]
    wide["total"] = wide[types].sum(axis=1)
    for t in types:
        wide[f"share_{t}"] = (wide[t] / wide["total"]).where(wide["total"] > 0)
    return wide


def read_targets(path):
    """Read MODEL/SCENARIO[#VERSION] lines; '#' at line start is a comment."""
    targets = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        model, scenario = line.split("/", 1)
        scenario, _, version = scenario.partition("#")
        targets.append((model, scenario, int(version) if version else None))
    return targets


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--scenarios", required=True)
    ap.add_argument("--platform", default="ixmp_dev")
    ap.add_argument("--out", default=".")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    mp = ixmp.Platform(args.platform)

    grids = [one_scenario(mp, *t) for t in read_targets(args.scenarios)]
    grids = [g for g in grids if g is not None]
    if not grids:
        raise SystemExit("No scenario had cooling technologies.")

    cols = [
        "model",
        "scenario",
        "version",
        "region",
        "year",
        "subannual",
        "parent",
        "cooling_type",
        "technology",
        "label",
        "quantity",
        "value",
        "unit",
    ]
    grid = pd.concat(grids, ignore_index=True)[cols]
    grid.to_csv(out_dir / "cooling_grid.csv", index=False)
    mix_table(grid).to_csv(out_dir / "cooling_mix.csv", index=False)
    print(f"Wrote {len(grid)} rows to {out_dir}")


if __name__ == "__main__":
    main()
