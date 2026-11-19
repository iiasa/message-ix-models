"""Check long-term gas contract bounds against a solved fuel_security scenario.

    python check_contract_bounds.py MODEL SCENARIO [--version N] [--platform localdb]
                                    [--gdx PATH] [--out DIR] [--tol 1e-3]

For every ``bound_activity_lo`` on a bilateral gas export technology
(``LNG_shipped_exp_<imp>``, ``gas_piped_exp_<imp>``) this compares the bound with the
solved activity (ACT summed over vintages) and reports:

1. Totals by year: contracted (bounded) volume, activity on bounded technologies, and
   all gas trade activity, i.e. how much of modelled trade the contracts cover.
2. Binding bounds: ACT within `tol` (relative) of the bound, with the dual of
   ACTIVITY_BOUND_LO where available. A binding contract forces more trade on that
   route than the model would otherwise choose.
3. Coverage by exporter and year: contracted volume / total exports, which shows
   the known gaps in the contract dataset (exporters with large trade but few
   contracts).

If the scenario has no contract bounds (e.g. solved without lt_contracts), or with
--from-data, the bounds are built from the contract dataset as bilateralize_scenario
would add them, and compared with the unconstrained solution: "binding" then means the
contract would force more trade on that route than the model chooses. No duals then.

Duals: ACTIVITY_BOUND_LO is only in the database if it was in the solve's equ_list.
Otherwise the script reads it from the solution GDX (--gdx, default message_ix's
MsgOutput file for MODEL/SCENARIO; that file is overwritten by every solve of the same
name, so check its timestamp). In a myopic solve the duals of past periods come from
the iteration that solved them; treat their magnitude with care and use the binding
flag (from levels) as the main signal.

Writes CSVs of the full comparison to --out (default: current directory).
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import ixmp
import message_ix
import pandas as pd

PREFIXES = ("LNG_shipped_exp_", "gas_piped_exp_")
IDX = ["node_loc", "technology", "year_act", "mode", "time"]

parser = argparse.ArgumentParser()
parser.add_argument("model")
parser.add_argument("scenario")
parser.add_argument("--version", type=int, default=None, help="Default: default version")
parser.add_argument("--platform", default="localdb")
parser.add_argument("--gdx", help="Solution GDX to read ACTIVITY_BOUND_LO duals from")
parser.add_argument("--out", default=".", help="Directory for the CSV outputs")
parser.add_argument("--from-data", action="store_true",
                    help="Build bounds from the contract dataset even if the scenario has some")
parser.add_argument("--contract-share", type=float, default=0.9,
                    help="With --from-data / no bounds in the scenario: share enforced")
parser.add_argument("--tol", type=float, default=1e-3,
                    help="Relative tolerance for treating a bound as binding")
args = parser.parse_args()

pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 200)

mp = ixmp.Platform(args.platform)
scen = message_ix.Scenario(mp, args.model, args.scenario, version=args.version)
print(f"Scenario: {scen.url}  (has_solution: {scen.has_solution()})")
if not scen.has_solution():
    mp.close_db()
    raise SystemExit("Scenario has no solution")

techs = [t for t in scen.set("technology") if t.startswith(PREFIXES)]
fmy = scen.firstmodelyear

# --- Bounds and activity --------------------------------------------------------------
lo = scen.par("bound_activity_lo", filters={"technology": techs})
lo = lo[lo["year_act"] >= fmy]
PROSPECTIVE = lo.empty or args.from_data
if PROSPECTIVE:
    # No contract bounds in the scenario: build them from the dataset as
    # bilateralize_scenario would, and compare with this (unconstrained) solution.
    from message_ix_models.project.fuel_security.lt_contracts import contract_bounds

    print("No contract bounds in the scenario: comparing the bounds the dataset "
          f"would add (contract_share={args.contract_share}) with the unconstrained "
          "solution. 'binding' here means ACT is below the would-be bound.")
    lo = contract_bounds(scen, contract_share=args.contract_share)
up = scen.par("bound_activity_up", filters={"technology": techs})

act = scen.var("ACT", filters={"technology": techs})
act = act[act["year_act"] >= fmy].groupby(IDX, as_index=False)["lvl"].sum()

df = lo[IDX + ["value"]].rename(columns={"value": "bound_lo"}).merge(act, on=IDX, how="left")
df["lvl"] = df["lvl"].fillna(0.0)
if not up.empty:
    df = df.merge(up[IDX + ["value"]].rename(columns={"value": "bound_up"}), on=IDX, how="left")
else:
    df["bound_up"] = float("nan")
df["slack"] = df["lvl"] - df["bound_lo"]
df["binding"] = df["slack"] <= args.tol * df["bound_lo"].abs() + 1e-6
df["violated"] = df["slack"] < -(args.tol * df["bound_lo"].abs() + 1e-6)

# --- Duals ----------------------------------------------------------------------------
def read_duals():
    """ACTIVITY_BOUND_LO marginals from the database, else from the solution GDX."""
    try:
        eq = scen.equ("ACTIVITY_BOUND_LO", filters={"technology": techs})
        if len(eq):
            print("Duals: ACTIVITY_BOUND_LO from the database")
            return eq.rename(columns={"node": "node_loc", "year": "year_act"})[IDX + ["mrg"]]
    except Exception:
        pass
    gdx = Path(args.gdx or Path(message_ix.__file__).parent.joinpath(
        "model", "output", f"MsgOutput_{args.model}_{args.scenario}.gdx"))
    if not gdx.exists():
        print(f"Duals: not in the database and {gdx} not found; skipping")
        return None
    from gams.transfer import Container

    print(f"Duals: ACTIVITY_BOUND_LO from {gdx} "
          f"(modified {datetime.fromtimestamp(os.path.getmtime(gdx))})")
    c = Container()
    c.read(str(gdx), symbols=["ACTIVITY_BOUND_LO"])
    r = c["ACTIVITY_BOUND_LO"].records
    if r is None or not len(r):
        return None
    r = r.copy()
    r.columns = IDX + list(r.columns[len(IDX):])
    r["year_act"] = r["year_act"].astype(int)
    r = r[r["technology"].isin(techs)]
    return r[IDX + ["marginal"]].rename(columns={"marginal": "mrg"})


duals = None if PROSPECTIVE else read_duals()
if duals is not None:
    df = df.merge(duals, on=IDX, how="left")
else:
    df["mrg"] = float("nan")

df["importer"] = df["technology"].str.split("_exp_").str[-1]
df["route"] = df["technology"].str.split("_exp_").str[0]

# --- 1. Totals by year ----------------------------------------------------------------
all_act = scen.var("ACT", filters={"technology": techs})
all_act = all_act[all_act["year_act"] >= fmy]
all_act["route"] = all_act["technology"].str.split("_exp_").str[0]
tot_trade = all_act.groupby(["year_act", "route"])["lvl"].sum().unstack("route")

by_year = df.groupby(["year_act", "route"]).agg(
    contracted=("bound_lo", "sum"), act_bounded=("lvl", "sum"),
    n_bounds=("bound_lo", "size"), n_binding=("binding", "sum")).unstack("route")
print("\n=== 1. Totals by year (GWa) ===")
print("Contract bounds vs activity on the bounded technologies:")
print(by_year.round(1).to_string())
print("\nAll modelled gas trade (ACT on every export technology):")
print(tot_trade.round(1).to_string())

# --- 2. Binding bounds ----------------------------------------------------------------
print(f"\n=== 2. Binding contract bounds (slack <= {args.tol:g} x bound) ===")
b = df[df["binding"]].sort_values(["year_act", "bound_lo"], ascending=[True, False])
cols = ["year_act", "node_loc", "technology", "bound_lo", "lvl", "bound_up", "mrg"]
print(b[cols].round(3).to_string(index=False) if len(b) else "None binding")
if df["violated"].any():
    print("\nWARNING: activity below the lower bound (solution violates the bound):")
    print(df.loc[df["violated"], cols].round(3).to_string(index=False))

# --- 3. Coverage by exporter ----------------------------------------------------------
exp = all_act.groupby(["node_loc", "year_act"])["lvl"].sum().rename("exports")
con = df.groupby(["node_loc", "year_act"])["bound_lo"].sum().rename("contracted")
cov = pd.concat([exp, con], axis=1).fillna(0.0)
cov["share"] = cov["contracted"] / cov["exports"].where(cov["exports"] > 0)
print("\n=== 3. Contract coverage by exporter (contracted / total exports) ===")
print(cov["share"].unstack("year_act").round(2).to_string())
print("\nTotal exports by exporter (GWa):")
print(cov["exports"].unstack("year_act").round(1).to_string())

# --- Output ---------------------------------------------------------------------------
out = Path(args.out)
out.mkdir(parents=True, exist_ok=True)
stem = f"contract_check_{args.scenario}_v{scen.version}"
df.to_csv(out / f"{stem}_bounds.csv", index=False)
cov.reset_index().to_csv(out / f"{stem}_coverage.csv", index=False)
print(f"\nWrote {out / (stem + '_bounds.csv')} and {out / (stem + '_coverage.csv')}")
mp.close_db()
