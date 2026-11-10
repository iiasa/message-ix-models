"""Drop empty variable records from a myopic MESSAGE solution GDX before importing it.

    python filter_gdx.py IN.gdx OUT.gdx [--symbols ACT CAP]

In the recursive-dynamic (myopic) solve, message_ix's model_solve.gms fixes past
periods with

    ACT.fx(node,tec,year_all2,year_all,mode,time)$( map_period(year_all2,year_all) ) = ...

which is not limited to the technologies, modes and vintages that exist. Fixing a
variable to 0 is a non-default bound, so GAMS writes every node x tec x vintage x
year x mode combination to the GDX (e.g. 368 million ACT records for
fuel_security/baseline_bilateral, against ~0.5 million real ones). ixmp then runs out
of memory reading the solution. Perfect-foresight solves do not have this loop.

ixmp stores only level and marginal, so records with level == 0 and marginal == 0
add nothing to the stored solution. This script removes those records from the
given variables (default ACT and CAP, the two fixed over the full domain) and copies
every other symbol unchanged. Then import with:

    python import_solution.py MODEL SCENARIO VERSION --gdx OUT.gdx ...

Memory: ACT is read whole (~70 bytes per record, ~25G for 368M records, plus the
filter), so run this as its own Slurm job with enough memory.
"""

import argparse

import gams.transfer as gt

parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("output")
parser.add_argument("--symbols", nargs="+", default=["ACT", "CAP"],
                    help="Variables to filter")
args = parser.parse_args()

# Read the small symbols first, then each large one, filtering it right away
everything = gt.Container()
everything.read(args.input, records=False)  # names and types only
others = [name for name in everything.listSymbols() if name not in args.symbols]
m = gt.Container()
m.read(args.input, symbols=others)

for name in args.symbols:
    m.read(args.input, symbols=[name])
    sym = m[name]
    if not isinstance(sym, gt.Variable):
        raise SystemExit(f"{name} is not a variable")
    before = len(sym.records)
    keep = (sym.records["level"] != 0) | (sym.records["marginal"] != 0)
    sym.setRecords(sym.records[keep].reset_index(drop=True))
    print(f"{name}: {before:,} -> {len(sym.records):,} records")

# Index sets named like a value column (MESSAGE's set "level", e.g. in
# COMMODITY_BALANCE) give duplicate DataFrame columns, which gams.transfer cannot
# write. Rename those index columns; GDX keeps no column labels, so the output is
# unchanged.
value_cols = {"level", "marginal", "lower", "upper", "scale", "value"}
for name in m.listSymbols():
    sym = m[name]
    if sym.records is not None and sym.records.columns.duplicated().any():
        sym.domain_labels = [f"{d}_" if d in value_cols else d
                             for d in sym.domain_labels]

m.write(args.output)
print(f"Wrote {args.output}")
