from typing import TYPE_CHECKING

from genno import Key, Quantity, quote

from . import util
from .key import activity_ldv_full, exo
from .util import EXTRAPOLATE

if TYPE_CHECKING:
    from genno import Computer

# - Use y for both year_vtg and year_act. This is because the usage pseudo-
#   technologies are ephemeral: only existing for year_vtg == year_act.
COMMON = util.COMMON | dict(
    commodity="disutility",
    level="useful",  # TODO Read this from the spec or template
    mode="all",
    time_origin="year",
)
DIMS = dict(node_loc="n", node_origin="n", technology="t", year_vtg="y", year_act="y")

TARGET = "disutility::LDV+ixmp"


def prepare_computer(c: "Computer") -> None:
    """Prepare `c` for calculating disutility inputs to LDV usage technologies."""
    k = Key("disutility:n-cg-t-y")

    # Interpolate to ensure all y::model are covered
    # NB "y::coords" is not equivalent here; includes all y, not just y::model
    c.add("y::model+coords", lambda years: dict(y=years), "y::model")
    c.add(k[1], "interpolate", exo.disutility, "y::model+coords", **EXTRAPOLATE)

    # Divide disutility per vehicle by annual driving distance per vehicle → disutility
    # per vehicle-km; convert to preferred units
    # TODO add "cg" dimension to ldv activity
    k2 = c.add(k[2], "div", k[1], activity_ldv_full)
    k3 = c.add(k[3], "mul", k2, Quantity(1.0, units="vehicle / year"))
    k4 = c.add(k[4], "convert_units", k3, units="USD / km")

    # Map (t, cg) to (t)
    k5 = c.add(k[5], "select", k4, "indexers::usage")
    c.add(k, "rename_dims", k5, quote({"t_new": "t"}))

    # Convert to message_ix-ready data
    c.add(TARGET, "as_message_df", k, name="input", dims=DIMS, common=COMMON)

    # Add to the scenario
    c.add("transport_data", __name__, key=TARGET)
