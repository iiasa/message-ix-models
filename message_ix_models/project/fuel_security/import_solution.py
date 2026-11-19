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
import jpype
import message_ix

parser = argparse.ArgumentParser()
parser.add_argument("model")
parser.add_argument("scenario")
parser.add_argument("version", type=int)
parser.add_argument("--platform", default="localdb")
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
# Call the Java method directly rather than via mp._backend.read_file(): ixmp turns a
# Java exception into a one-line RuntimeError, and when the import fails Java's own
# cleanup (discardChanges) can throw "not checked out", hiding the actual cause.
ArrayList = jpype.JClass("java.util.ArrayList")


def jlist(items):
    out = ArrayList()
    for i in items:
        out.add(i)
    return out


try:
    mp._backend.jindex[scen].readSolutionFromGDX(
        str(gdx.parent), gdx.name, "Import solution from GDX",
        jlist(args.var), jlist(args.equ), True)
except jpype.JException as e:
    print(e.stacktrace())
    mp.close_db()
    raise SystemExit("Import failed; see the Java stack trace above (incl. 'Caused by')")
mp._backend.cache_invalidate(scen)
print("has_solution:", scen.has_solution())
mp.close_db()
