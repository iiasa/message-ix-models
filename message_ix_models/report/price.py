"""Report prices.

.. todo:: Extend with the following:

   - Subtract PRICE_EMISSION from PRICE_COMMODITY to produce IAMC variables like
     "Price|* Energy wo carbon price". This requires transforming the dimensions (e, t)
     and units [currency] / [mass] on the former to (c, l) and [currency] / [energy]
     (or other units) on the latter.
   - Add the MESSAGE parameter ``tax_emission``.
"""

from typing import TYPE_CHECKING

from genno import Keys

from . import util
from .util import IAMCConversion

if TYPE_CHECKING:
    from message_ix import Reporter

    from message_ix_models import Context

K = Keys(
    PC="PRICE_COMMODITY:n-c-l-y-h",
    PE="PRICE_EMISSION:n-type_emission-type_tec-y",
    carbon="carbon price:n-y",
    c="commodity price:n-c-l-y",
)

CONV = (
    IAMCConversion(base=K.c, var_parts=["Price", "l", "c"], unit="USD_2010 / GJ"),
    IAMCConversion(base=K.carbon, var_parts=["Price|Carbon"], unit="USD_2010 / Mt"),
)


def callback(rep: "Reporter", context: "Context") -> None:
    """Prepare reporting of prices."""
    # Add replacements for fully constructed variable names
    util.REPLACE_VARS.update(
        {
            r"^(?:PRICE_COMMODITY\|)?(Price\|Final Energy\|Residential)": r"\1",
            r"^(?:PRICE_COMMODITY\|)?(Price\|(Primary|Secondary) Energy)\|": (
                r"\1 w carbon price|"
            ),
        }
    )

    # Apply units that are not present in the scenario
    rep.add(K.PC["units"], "apply_units", K.PC, "USD_2010 / kWa", sums=True)
    rep.add(K.PE["units"], "apply_units", K.PE, "USD_2005 / Mt")

    # Prepare commodity prices: select only certain levels
    idx = dict(h="year", l=["final", "primary", "secondary"])
    rep.add(K.c, "select", K.PC["units"], indexers=idx)

    # Prepare carbon prices: select and drop 2 dimensions
    idx = dict(type_emission="TCE", type_tec="all")
    rep.add(K.carbon, "select_allow_empty", K.PE["units"], indexers=idx, drop=True)

    # Convert to IAMC structure
    for c in CONV:
        c.add_tasks(rep)
