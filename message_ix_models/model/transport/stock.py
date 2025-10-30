"""First-period stock of non-LDV modes."""

import logging
from typing import TYPE_CHECKING

from genno import Key

from . import util
from .key import exo, pop

if TYPE_CHECKING:
    from genno import Computer

log = logging.getLogger(__name__)

Si = "stock+ixmp"
TARGET = f"transport::{Si}"


def prepare_computer(c: "Computer") -> None:
    # total stock = stock per capita × total population
    stock_total = exo.stock_cap - "cap"
    c[stock_total] = "mul", exo.stock_cap, pop

    # Convert to data for MESSAGE parameters "bound_total_capacity_{lo,up}"
    keys = []
    kw = dict(dims=util.DIMS | dict(node_loc="n", year_act="y"), common=util.COMMON)
    for par_name in "bound_total_capacity_lo", "bound_total_capacity_up":
        keys.append(Key(par_name, (), Si))
        c[keys[-1]] = "as_message_df", stock_total, dict(name=par_name) | kw

    # Merge parameter data
    c[TARGET] = "merge_data", *keys

    log.warning("Disabled: add {__name__} data")
    return

    # Connect `TARGET` to the "add transport data" key
    c.add("transport_data", __name__, key=TARGET)
