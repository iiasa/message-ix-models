from typing import TYPE_CHECKING

from genno import Keys

from .key import groups
from .util import IAMCConversion

if TYPE_CHECKING:
    from message_ix import Reporter

    from message_ix_models import Context

#: Some keys. Use the partial sum over 'g'.
K = Keys(model="EXT:n-c-y", hist="historical_extraction:n-c-y", all="EXT:n-c-y:all")

#: Describe the conversion to IAMC structure.
CONV = IAMCConversion(
    base=K.all[2],
    var_parts=["Resource|Extraction", "c"],
    sums=["c"],
    unit="EJ/yr",
    GLB_zeros=True,
)


def callback(r: "Reporter", context: "Context") -> None:
    """Prepare reporting of resource extraction."""
    # TODO Generalize the following 3 blocks. Similar operations will be used in other
    # cases.

    # Apply missing units to model contents
    r.add(K.hist[0], "apply_units", K.hist, units="GWa/year")
    r.add(K.model[0], "apply_units", K.model, units="GWa/year")

    # Add historical and current values together
    r.add(K.all[0], "add", K.hist[0], K.model[0])

    # Convert to target units
    r.add(K.all[1], "convert_units", K.all[0], units=CONV.unit)

    # Aggregate on 'c' dimension using groups from commodity_groups()
    r.add(K.all[2], "aggregate", K.all[1], groups.c, keep=False)

    # Add tasks to (a) transform to IAMC-structured data, (b) concatenate to all::iamc
    CONV.add_tasks(r)
