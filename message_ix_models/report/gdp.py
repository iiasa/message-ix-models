"""Report GDP."""

from typing import TYPE_CHECKING

from genno import Key

from .key import GDP
from .util import IAMCConversion

if TYPE_CHECKING:
    from message_ix import Reporter

    from message_ix_models import Context

K = Key("gdp_ppp:n-y")

U = "billion USD_2010 / year"

CONV = (
    IAMCConversion(base=GDP["units"], var_parts=["GDP|MER"], unit=U),
    IAMCConversion(base=K, var_parts=["GDP|PPP"], unit=U),
)


def callback(r: "Reporter", context: "Context") -> None:
    """Prepare reporting of GDP."""
    r.add(GDP["units"], "apply_units", GDP, units="billion USD_2005 / year")
    r.add(K, "mul", GDP["units"], "MERtoPPP:n-y")

    for c in CONV:
        c.add_tasks(r)
