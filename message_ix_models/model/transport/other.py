"""Transport ‘other’ technologies for energy use not elsewhere represented."""

import logging
from typing import TYPE_CHECKING

import genno
import pandas as pd
from genno import Key, quote

from . import util
from .key import exo, fv

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import AnyQuantity

log = logging.getLogger(__name__)

#: Shorthand for tags on keys.
Oi = "::O+ixmp"

#: Common, fixed values for some dimensions of MESSAGE parameters.
COMMON = util.COMMON | dict(level="final")

#: Mapping from :mod:`message_ix` parameter dimensions to source dimensions in some
#: quantities.
DIMS = util.DIMS | dict(node_loc="n", node_origin="n", year_act="y", year_vtg="y")
DIMS.pop("level", None)

#: Target key that collects all data generated in this module.
TARGET = "transport::O+ixmp"


def prepare_computer(c: "Computer") -> None:
    """Generate MESSAGE parameter data for ``transport other *`` technologies."""
    # Keys
    base = exo.energy_other
    assert {"c", "n"} == set(base.dims)
    bcast = Key("broadcast:c-t:other transport")
    k_cnt = (base + "0") * "t"  # with added dimension "t"
    k_cnty = Key(base * ("t", "y") + "1")  # with added dimensions "t", "y"

    if base not in c:
        log.warning(f"No key {base!r} → no data for 'transport other *' techs")

        names = "bound_activity_lo bound_activity_up input".split()
        c.add(TARGET, quote(dict.fromkeys(names)))
        c.add("transport_data", __name__, key=TARGET)

        return

    def broadcast_other_transport(technologies) -> "AnyQuantity":
        """Transform e.g. c="gas" to (c="gas", t="transport other gas")."""
        rows = []
        cols = ["c", "t", "value"]

        for code in filter(lambda code: "other" in code.id, technologies):
            rows.append([code.eval_annotation(id="input")["commodity"], code.id, 1.0])

        return genno.Quantity(
            pd.DataFrame(rows, columns=cols).set_index(cols[:-1])[cols[-1]]
        )

    c.add(bcast, broadcast_other_transport, "t::transport")
    c.add(k_cnt, "mul", base, bcast)

    # Project values across y using same trajectory as road freight activity
    c.add(k_cnty[0], "mul", k_cnt, fv["ROAD index"])
    # Convert units to GWa
    c.add(k_cnty[1], "convert_units", k_cnty[0], units="GWa")

    # Common dimension mapping and fixed labels for bound_activity_{lo,up} and input
    kw = dict(dims=DIMS, common=COMMON)

    # Produce MESSAGE parameters bound_activity_{lo,up}:nl-t-ya-m-h
    k_bal = Key(f"bound_activity_lo{Oi}")
    c.add(k_bal, "as_message_df", k_cnty.last, name=k_bal.name, **kw)
    k_bau = Key(f"bound_activity_up{Oi}")
    c.add(k_bau, "as_message_df", k_cnty.last, name=k_bau.name, **kw)

    # Divide by self to ensure values = 1.0 but same dimensionality
    c.add(k_cnty[2], "div", k_cnty[0], k_cnty[0])
    # Results in dimensionless; re-assign units
    c.add(k_cnty[3], "assign_units", k_cnty[2], units="GWa")

    # Produce MESSAGE parameter input:nl-t-yv-ya-m-no-c-l-h-ho
    k_input = Key(f"input{Oi}")
    c.add(k_input, "as_message_df", k_cnty.last, name=k_input.name, **kw)

    # Merge data together
    c.add(TARGET, "merge_data", k_bal, k_bau, k_input)

    # Connect `TARGET` to the "add transport data" key
    c.add("transport_data", __name__, key=TARGET)
