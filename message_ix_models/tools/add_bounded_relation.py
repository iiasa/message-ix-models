"""Add an upper or lower bound on a group of technologies via a relation.

Moved from ``message_data.tools.utilities.add_bounded_relation``.
"""

import itertools

import pandas as pd
from message_ix_models import ScenarioInfo


def main(
    scen,
    val,
    tec,
    rel,
    bound_type="relation_upper",
    val_conv=1,
    val_unit="GWa/a",
    node="all",
    node_rel="global",
    year_rel=None,
    verbose=False,
):
    """Add an upper or lower limit for a group of technologies.

    Parameters
    ----------
    scen : message_ix.Scenario
    val : number or dict
        Single value for a global constraint, or dict keyed by region.
    tec : str or list
        Technology name(s) to constrain.
    rel : str
        Relation name.
    bound_type : str
        ``"relation_upper"`` (default) or ``"relation_lower"``.
    val_conv : number
        Conversion factor applied to `val`.
    val_unit : str
        Unit string for the bound value.
    node : str or list
        Regions whose activity writes into the relation. ``"all"`` uses all
        non-GLB/World nodes.
    node_rel : str
        ``"global"`` (default) — constraint aggregated at the GLB node.
        ``"regional"`` — one constraint per region.
    year_rel : list, optional
        Years for the constraint. Defaults to all optimization years.
    verbose : bool
    """
    if verbose:
        print(f"Adding relation {rel} for regions {node}.")

    info = ScenarioInfo(scen)

    if node == "all":
        node = [n for n in info.N if not any(y in n for y in ["GLB", "World"])]
    else:
        node = [n for n in (node if isinstance(node, list) else [node]) if n in info.N]
        if not node:
            raise ValueError("No valid regions specified.")

    if node_rel not in ("global", "regional"):
        raise ValueError(f"node_rel={node_rel!r} must be 'global' or 'regional'.")

    glb_reg = [n for n in info.N if "GLB" in n]

    if node_rel == "global":
        val = {glb_reg[0]: val}
    else:
        if isinstance(val, dict) and len(val) != len(node):
            raise ValueError("Number of constraint values must match number of regions.")

    if isinstance(tec, str):
        tec = [tec]

    node_rel_list = node if node_rel == "regional" else glb_reg
    year_rel = info.Y if year_rel is None else year_rel

    with scen.transact("Add bound relation"):
        if rel not in scen.set("relation"):
            scen.add_set("relation", rel)

        for n in node_rel_list:
            v = val[n]
            vals = [x * val_conv for x in v] if isinstance(v, list) else v * val_conv
            scen.add_par(
                bound_type,
                pd.DataFrame(
                    {
                        "relation": rel,
                        "node_rel": n,
                        "year_rel": year_rel,
                        "value": vals,
                        "unit": val_unit,
                    }
                ),
            )

    with scen.transact("Add relation activity"):
        for n, t in itertools.product(node, tec):
            found = False
            for par in ("input", "output"):
                if found:
                    break
                tmp = scen.par(par, filters={"technology": t, "node_loc": n})
                if tmp.empty:
                    continue
                found = True
                drop_cols = [
                    c for c in [
                        "year_vtg", "time_dest", "node_dest", "time_origin",
                        "node_origin", "commodity", "level", "time",
                    ] if c in tmp.columns
                ]
                tmp = (
                    tmp.drop(drop_cols, axis=1)
                    .assign(value=1, unit=val_unit, relation=rel)
                    .drop_duplicates()
                )
                tmp["year_rel"] = tmp["year_act"]
                tmp["node_rel"] = glb_reg[0] if node_rel == "global" else n
                scen.add_par("relation_activity", tmp)
