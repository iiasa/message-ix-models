"""Copy the default version of each scenario from one local database into a new one.

    python fresh_database.py --source mydb --db /path/with/space/mydb_fresh

ixmp cannot delete scenario versions, so a local HSQLDB grows with every clone and
solve. This makes a new database holding only the current default version of each
scenario (with its solution, if any). Old versions are not copied.

Steps:
1. Run with --list first to see what will be copied, and whether each scenario is
   solved. A scenario NOT SOLVED here is also unsolved in --db; LOCKED ones are
   skipped by the copy until unlocked.
2. Run without --list. Make sure no other process has --source open.
3. Point the "mydb" entry in ixmp's config.json at the new --db path. Keep the old
   database files until you have checked the new one.

Options:
- --model limits the copy to some models (default: all models in --source).
- Rerun with --resume after an interrupted copy: scenarios already in --db are
  skipped. Without --resume, an existing --db is refused.
- The new database is converted to cached tables at the end, so it is kept on disk
  rather than loaded into the Java heap. Its first open after that is slow.
- The source is opened first, so --heap sets the Java heap for the whole copy. An old
  database with in-memory tables is loaded into that heap in full; size --heap for it.
"""

import argparse
from pathlib import Path

import ixmp
import message_ix

from message_ix_models.project.fuel_security.copy_scenario_to_local import (
    check_copy,
    convert_to_cached,
    copy_units_and_regions,
    scenario_exists,
)


def default_versions(source, models):
    """Default versions of all scenarios in `source`, limited to `models` if given."""
    listed = source.scenario_list(default=True)
    if models:
        listed = listed[listed["model"].isin(models)]
    return listed.sort_values(["model", "scenario"])


def solved_status(source, model, scenario, version):
    """'solved', 'NOT SOLVED', or 'LOCKED' (cannot be read, so cannot be copied)."""
    try:
        scen = message_ix.Scenario(source, model, scenario, version=version)
    except RuntimeError as exc:
        if "locked by" in str(exc):
            return "LOCKED"
        raise
    return "solved" if scen.has_solution() else "NOT SOLVED"


def copy_one(source, dest, model, scenario, version):
    """Copy one scenario version into `dest` and make it the default there."""
    try:
        original = message_ix.Scenario(source, model, scenario, version=version)
    except RuntimeError as exc:
        print(f"  SKIPPED: {exc}")  # e.g. locked by a killed job
        return False

    print(f"  v{version}, solved: {original.has_solution()}")
    copy = original.clone(model=model, scenario=scenario, platform=dest,
                          keep_solution=True)
    copy.set_as_default()  # not done by clone() for a new model/scenario name
    check_copy(original, copy)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default="mydb", help="platform name to copy from")
    parser.add_argument("--db", required=True, help="path of the new local HSQLDB")
    parser.add_argument("--model", nargs="*", help="models to copy (default: all)")
    parser.add_argument("--heap", default="32G", help="Java heap size")
    parser.add_argument("--list", action="store_true", help="only list, copy nothing")
    parser.add_argument("--resume", action="store_true",
                        help="add to an existing --db, skipping scenarios it holds")
    args = parser.parse_args()

    if Path(f"{args.db}.script").exists() and not (args.resume or args.list):
        raise SystemExit(f"{args.db} already exists; use --resume to add to it.")

    source = ixmp.Platform(args.source, jvmargs=f"-Xmx{args.heap}")
    try:
        todo = default_versions(source, args.model)
        print(f"{len(todo)} default scenario versions in {args.source}:")
        if not args.list:
            for row in todo.itertuples():
                print(f"  {row.model}/{row.scenario} v{row.version}")
        else:
            counts = {}
            for row in todo.itertuples():
                status = solved_status(source, row.model, row.scenario,
                                       int(row.version))
                counts[status] = counts.get(status, 0) + 1
                print(f"  {row.model}/{row.scenario} v{row.version}: {status}")
            print("Total: " + ", ".join(f"{n} {s}" for s, n in counts.items()))
            return

        convert_to_cached(args.db)  # with --resume; must run while --db is closed
        dest = ixmp.Platform(backend="jdbc", driver="hsqldb", path=args.db)
        failed = []
        try:
            copy_units_and_regions(source, dest)
            for i, row in enumerate(todo.itertuples(), start=1):
                print(f"[{i}/{len(todo)}] {row.model}/{row.scenario}")
                if scenario_exists(dest, row.model, row.scenario):
                    print("  already in --db, skipped")
                    continue
                if not copy_one(source, dest, row.model, row.scenario,
                                int(row.version)):
                    failed.append(f"{row.model}/{row.scenario}")
        finally:
            dest.close_db()
        # ixmp without PR #645 creates in-memory tables, which load the whole
        # database into the Java heap whenever it is opened. Store them on disk.
        convert_to_cached(args.db)

        if failed:
            print("Not copied:\n  " + "\n  ".join(failed))
        else:
            print("All scenarios copied.")
    finally:
        source.close_db()


if __name__ == "__main__":
    main()
