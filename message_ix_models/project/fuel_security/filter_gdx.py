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
every other symbol unchanged. A marginal of EPS (GAMS' "zero but meaningful") counts
as non-zero, so those records are kept. Then import with:

    python import_solution.py MODEL SCENARIO VERSION --gdx OUT.gdx ...

The copy streams one record at a time through the low-level GDX API, so memory use
stays small however large the input is. (gams.transfer cannot read a symbol this
large: it overflows building the index arrays.)
"""

import argparse

from gams.core import gdx
from gams.transfer import Container

parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("output")
parser.add_argument("--symbols", nargs="+", default=["ACT", "CAP"],
                    help="Variables to filter")
args = parser.parse_args()

Container()  # finds the GAMS system directory and loads the GDX library

LEVEL, MARGINAL = gdx.GMS_VAL_LEVEL, gdx.GMS_VAL_MARGINAL
key_arr = gdx.intArray(gdx.GMS_MAX_INDEX_DIM)  # reused for every record written
val_arr = gdx.doubleArray(gdx.GMS_VAL_MAX)


def check(result, what):
    """Raise if a GDX call failed. Results are either rc or [rc, ...]."""
    rc = result[0] if isinstance(result, list) else result
    if not rc:
        raise SystemExit(f"GDX error in {what}")
    return result


def handle():
    h = gdx.new_gdxHandle_tp()
    check(gdx.gdxCreate(h, gdx.GMS_SSSIZE), "gdxCreate")
    return h


src, dst = handle(), handle()
check(gdx.gdxOpenRead(src, args.input), f"opening {args.input}")
check(gdx.gdxOpenWrite(dst, args.output, "filter_gdx.py"), f"opening {args.output}")
_, n_symbols, n_uels = check(gdx.gdxSystemInfo(src), "gdxSystemInfo")

# Register all labels (UELs) in the same order, so record keys copy unchanged
check(gdx.gdxUELRegisterRawStart(dst), "gdxUELRegisterRawStart")
for i in range(1, n_uels + 1):
    _, label, _ = check(gdx.gdxUMUelGet(src, i), f"reading label {i}")
    check(gdx.gdxUELRegisterRaw(dst, label), f"registering label {label!r}")
check(gdx.gdxUELRegisterDone(dst), "gdxUELRegisterDone")

for i in range(1, n_symbols + 1):
    _, name, dim, typ = check(gdx.gdxSymbolInfo(src, i), f"symbol {i}")
    _, n_recs, userinfo, text = check(gdx.gdxSymbolInfoX(src, i), name)

    if typ == gdx.GMS_DT_ALIAS:
        # userinfo is the number of the aliased set (0 = the universe '*')
        target = "*" if userinfo == 0 else gdx.gdxSymbolInfo(src, userinfo)[1]
        check(gdx.gdxAddAlias(dst, target, name), f"alias {name}")
        continue

    drop_empty = name in args.symbols
    if drop_empty and typ != gdx.GMS_DT_VAR:
        raise SystemExit(f"{name} is not a variable")

    check(gdx.gdxDataReadRawStart(src, i), f"reading {name}")
    check(gdx.gdxDataWriteRawStart(dst, name, text, dim, typ, userinfo),
          f"writing {name}")
    # Domain: GetDomainX returns its type (1 none, 2 relaxed, 3 regular) and names.
    # A regular domain (linked to the set) is set while writing; relaxed after.
    domain_type, domain = gdx.gdxSymbolGetDomainX(src, i) if dim else (1, [])
    if domain_type == 3:
        check(gdx.gdxSymbolSetDomain(dst, domain[:dim]), f"domain of {name}")
    kept = 0
    for _ in range(n_recs):
        _, keys, values, _ = check(gdx.gdxDataReadRaw(src), f"record of {name}")
        if drop_empty and values[LEVEL] == 0 and values[MARGINAL] == 0:
            continue
        if typ == gdx.GMS_DT_SET and values[LEVEL] != 0:
            # Set element text: re-add it to the output's text table
            _, elem_text, _ = gdx.gdxGetElemText(src, int(values[LEVEL]))
            _, values[LEVEL] = check(gdx.gdxAddSetText(dst, elem_text), "set text")
        # The writer takes C arrays, not Python lists
        for j, key in enumerate(keys):
            key_arr[j] = key
        for j, value in enumerate(values):
            val_arr[j] = value
        check(gdx.gdxDataWriteRaw(dst, key_arr, val_arr), f"writing record of {name}")
        kept += 1
    gdx.gdxDataReadDone(src)
    check(gdx.gdxDataWriteDone(dst), f"finishing {name}")

    if domain_type == 2:
        check(gdx.gdxSymbolSetDomainX(dst, i, domain[:dim]), f"domain of {name}")

    if drop_empty:
        print(f"{name}: {n_recs:,} -> {kept:,} records", flush=True)

gdx.gdxClose(src)
check(gdx.gdxClose(dst) == 0, f"closing {args.output}")
print(f"Wrote {args.output}")
