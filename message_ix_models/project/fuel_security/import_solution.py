"""Import a finished GAMS solution into a scenario without re-solving.

Use this when GAMS solved through but the job died while ixmp was reading the
solution back into the database, leaving the scenario without a solution.

    python import_solution.py MODEL SCENARIO VERSION [--gdx PATH] [--heap 32G] [--unlock]

--var / --equ import only the named variables / equations (default: all). The
equations are the bulk of a solution; e.g. --equ COMMODITY_BALANCE_GT keeps memory
much lower than importing every equation's levels and marginals.

--unlock releases a lock left by a killed job. Only use it when no other process
is using the scenario.

By default the GDX is the one message_ix writes for MODEL/SCENARIO:
message_ix/model/output/MsgOutput_{MODEL}_{SCENARIO}.gdx. It is overwritten by
every solve of that MODEL/SCENARIO name, so check its timestamp matches the solve.
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import ixmp
import message_ix
from ixmp.backend import ItemType

parser = argparse.ArgumentParser()
parser.add_argument("model")
parser.add_argument("scenario")
parser.add_argument("version", type=int)
parser.add_argument("--platform", default="mydb")
parser.add_argument("--gdx", help="Default: message_ix's MsgOutput file for the scenario")
parser.add_argument("--heap", default="32G", help="Java heap; reading a solution is large")
parser.add_argument("--var", nargs="*", default=[], help="Variables to import")
parser.add_argument("--equ", nargs="*", default=[], help="Equations to import")
parser.add_argument("--unlock", action="store_true", help="Release a stale lock first")
args = parser.parse_args()

gdx = Path(args.gdx or Path(message_ix.__file__).parent.joinpath(
    "model", "output", f"MsgOutput_{args.model}_{args.scenario}.gdx"))
if not gdx.exists():
    raise SystemExit(f"{gdx} not found; pass --gdx")
print(f"GDX: {gdx} (modified {datetime.fromtimestamp(os.path.getmtime(gdx))})")

mp = ixmp.Platform(args.platform, jvmargs=f"-Xmx{args.heap}")
if args.unlock:
    for s in mp._backend.jobj.getScenarioList(False, args.model, args.scenario):
        if int(s["version"]) == args.version:
            print(f"Unlocking run_id {s['run_id']}")
            mp._backend.jobj.unlockRunid(s["run_id"])
scen = message_ix.Scenario(mp, args.model, args.scenario, version=args.version)
if scen.has_solution():
    raise SystemExit(f"{scen.url} already has a solution")

print(f"Importing solution into {scen.url}")
mp._backend.read_file(gdx, ItemType.MODEL, filters=dict(scenario=scen),
                      check_solution=True, comment="Import solution from GDX",
                      equ_list=args.equ, var_list=args.var)
print("has_solution:", scen.has_solution())
mp.close_db()
