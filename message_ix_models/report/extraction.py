from typing import TYPE_CHECKING

from genno import Keys

from .key import groups
from .util import IAMCConversion

if TYPE_CHECKING:
    from genno import Computer

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


def callback(c: "Computer", context: "Context") -> None:
    from message_ix_models.report import util

    # Discard "EXT|" prefix on constructed variable names
    # TODO Improve handle_iamc() or underlying code to make this unnecessary
    util.REPLACE_VARS[r"^EXT\|"] = ""

    # TODO Generalize the following 3 blocks

    # Apply units
    c.add(K.hist[0], "apply_units", K.hist, units="GWa/year")
    c.add(K.model[0], "apply_units", K.model, units="GWa/year")

    # Add historical and current values together
    c.add(K.all[0], "add", K.hist[0], K.model[0])

    # Convert to target units
    c.add(K.all[1], "convert_units", K.all[0], units=CONV.unit)

    # Aggregate on 'c' dimension
    c.add(K.all[2], "aggregate", K.all[1], groups.c, keep=False)

    # Transform to IAMC-structured data
    CONV.add_tasks(c)
