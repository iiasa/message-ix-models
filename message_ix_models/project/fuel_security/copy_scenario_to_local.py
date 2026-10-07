"""Copy a scenario from a shared ixmp database (read-only) into a local HSQLDB.

    python copy_scenario_to_local.py MODEL SCENARIO --db /path/with/space/mydb

Notes:
- Put --db on a disk with room (on the cluster: /hdrive, not your home).
- Needs ixmp with cached HSQLDB tables (PR #645); checked at start.
- An existing --db made by an older ixmp is converted to cached tables
  (backup of its .script kept as .script.bak); the first open after that is slow.
- Only one process may open a local database at a time.
- A locked scenario cannot be read, so cannot be copied.
"""

import argparse
import os
import shutil
from pathlib import Path

import ixmp
import message_ix


def convert_to_cached(db_path):
    """Switch an existing local database from in-memory to cached tables.

    Must run while the database is closed. HSQLDB reads table types from the
    .script file, so rewriting the CREATE statements there converts the tables.
    """
    script = Path(f"{db_path}.script")
    if not script.exists():
        return  # new database: current ixmp creates cached tables

    with script.open() as f:
        if not any(line.startswith("CREATE MEMORY TABLE") for line in f):
            return

    log = Path(f"{db_path}.log")
    if log.exists() and log.stat().st_size > 0:
        raise SystemExit(f"{log} holds changes not yet saved to {script}, so the "
                         "database was not closed cleanly or is still open. Make "
                         "sure no other process uses it, then retry.")

    backup = script.with_name(script.name + ".bak")
    shutil.copy2(script, backup)
    tmp = script.with_name(script.name + ".tmp")
    with script.open() as src, tmp.open("w") as dst:  # streamed: file can be large
        for line in src:
            if line.startswith("CREATE MEMORY TABLE"):
                line = line.replace("CREATE MEMORY TABLE", "CREATE CACHED TABLE", 1)
            dst.write(line)
    os.replace(tmp, script)
    print(f"Converted {db_path} to cached tables (backup: {backup}).")


def open_databases(source_name, db_path, heap):
    """Open the source and local databases; the first one fixes the Java heap."""
    source = ixmp.Platform(source_name, jvmargs=f"-Xmx{heap}")
    local = ixmp.Platform(backend="jdbc", driver="hsqldb", path=db_path)
    return source, local


def check_cached_tables(db_path):
    """Stop if the local database uses slow in-memory tables."""
    if "CREATE MEMORY TABLE" in Path(f"{db_path}.script").read_text():
        raise SystemExit("Local database still uses in-memory tables after "
                         "conversion: check that ixmp includes PR #645.")


def copy_units_and_regions(source, local):
    """Add the source's units and regions missing from the local database."""
    known_units = {u.strip() for u in local.units()}
    for unit in source.units():
        if unit.strip() not in known_units:
            local.add_unit(unit, "copied from source")
            known_units.add(unit.strip())

    regions = source.regions()
    regions = regions[regions["region"] == regions["mapped_to"]]  # skip synonyms
    added = True
    while added:  # parents must exist before children
        added = False
        known = set(local.regions()["region"])
        for row in regions.itertuples():
            if row.region not in known and row.parent in known:
                local.add_region(row.region, row.hierarchy, row.parent)
                known.add(row.region)
                added = True


def scenario_exists(platform, model, scenario):
    """True if `platform` holds model/scenario (filtered here: JDBC raises on misses)."""
    listed = platform.scenario_list(default=False)
    if listed.empty:
        return False
    return bool(((listed["model"] == model) & (listed["scenario"] == scenario)).any())


def objective(scen):
    """Objective value, or None if unsolved."""
    return float(scen.var("OBJ")["lvl"]) if scen.has_solution() else None


def copy_scenario(source, local, model, scenario, version, new_name):
    """Copy model/scenario into `local`; return the original and the copy."""
    try:
        original = message_ix.Scenario(source, model, scenario, version=version)
    except RuntimeError as exc:
        if "locked by" in str(exc):
            raise SystemExit(f"Cannot read {model}/{scenario}: {exc}") from None
        raise
    print(f"Read {model}/{scenario} v{original.version}, solved: {original.has_solution()}")

    if scenario_exists(local, model, new_name):
        raise SystemExit(f"{model}/{new_name} already exists locally; use --new-name.")

    copy = original.clone(model=model, scenario=new_name, platform=local,
                          keep_solution=True)
    print(f"Copied as {model}/{new_name} v{copy.version}")
    return original, copy


def check_copy(original, copy):
    """Compare row counts of a few parameters and the objective."""
    for item in ("demand", "input", "output"):
        if item in original.par_list():
            n_orig, n_copy = len(original.par(item)), len(copy.par(item))
            print(f"  {item}: {n_orig} / {n_copy} rows")
            if n_orig != n_copy:
                raise SystemExit(f"Row counts differ for {item}.")

    obj_orig, obj_copy = objective(original), objective(copy)
    print(f"  objective: {obj_orig} / {obj_copy}")
    if (obj_orig is None) != (obj_copy is None):
        raise SystemExit("Solution not copied.")
    if obj_orig is not None and abs(obj_orig - obj_copy) > 1.0:  # single precision
        raise SystemExit("Objectives differ.")
    print("Copy checked.")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("model")
    parser.add_argument("scenario")
    parser.add_argument("--version", type=int, help="default: the default version")
    parser.add_argument("--db", required=True, help="local HSQLDB path")
    parser.add_argument("--source", default="ixmp_dev", help="platform to read from")
    parser.add_argument("--new-name", help="local scenario name (default: same)")
    parser.add_argument("--heap", default="16G", help="Java heap size")
    args = parser.parse_args()

    convert_to_cached(args.db)  # before opening: HSQLDB rewrites .script on close
    source, local = open_databases(args.source, args.db, args.heap)
    try:
        check_cached_tables(args.db)
        copy_units_and_regions(source, local)
        original, copy = copy_scenario(source, local, args.model, args.scenario,
                                       args.version, args.new_name or args.scenario)
        check_copy(original, copy)
    finally:
        local.close_db()
        source.close_db()


if __name__ == "__main__":
    main()