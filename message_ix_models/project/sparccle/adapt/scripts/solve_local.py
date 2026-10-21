"""Solve a scenario on a (local) ixmp platform and make it the default version.

Usage: python solve_local.py PLATFORM MODEL SCENARIO [--version N]
           [--model MESSAGE|MESSAGE-MACRO] [--solve-option KEY=VALUE ...]
           [--if-unsolved]

--solve-option adds or overrides CPLEX options (default lpmethod=4 threads=4), e.g.
--solve-option scaind=1 --solve-option numericalemphasis=1 for "optimal with unscaled
infeasibilities".
"""

import argparse
import logging

import ixmp
import message_ix

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("solve_local")

p = argparse.ArgumentParser()
p.add_argument("platform")
p.add_argument("model")
p.add_argument("scenario")
p.add_argument("--version", type=int, default=None)
p.add_argument("--model", dest="gams_model", default="MESSAGE")
p.add_argument("--solve-option", action="append", default=[], metavar="KEY=VALUE")
p.add_argument(
    "--if-unsolved",
    action="store_true",
    help="Do nothing when the scenario already has a solution.",
)
a = p.parse_args()

solve_options = {"lpmethod": "4", "threads": "4"}
solve_options.update(dict(opt.split("=", 1) for opt in a.solve_option))

mp = ixmp.Platform(a.platform)
try:
    scen = message_ix.Scenario(mp, a.model, a.scenario, version=a.version)
    log.info(f"Solving {scen.url} (has_solution={scen.has_solution()})")
    if a.if_unsolved and scen.has_solution():
        log.info("Already solved, --if-unsolved: nothing to do")
        raise SystemExit(0)
    if scen.has_solution():
        scen.remove_solution()
    log.info(f"solve_options={solve_options}")
    scen.solve(model=a.gams_model, solve_options=solve_options)
    scen.set_as_default()
    log.info(f"Solved: OBJ={float(scen.var('OBJ')['lvl'])}")
finally:
    mp.close_db()
