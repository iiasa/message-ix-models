from functools import cache
from typing import TYPE_CHECKING, Literal

from genno import Keys

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


@cache
def get_commodity_groups() -> dict[Literal["c"], dict[str, list[str]]]:
    """Return groups of commodities for reporting of extraction.

    Transcribed from
    :mod:`message_ix_models.report.legacy.default_tables.retr_extraction`.

    .. todo:: Construct this by processing information in :file:`commodity.yaml`.
    """
    result = {
        "Coal": ["coal", "lignite"],
        "Oil|Conventional": ["crude_1", "crude_2", "crude_3"],
        "Oil|Unconventional": ["crude_4", "crude_5", "crude_6", "crude_7", "crude_8"],
        "Gas|Conventional": ["gas_1", "gas_2", "gas_3", "gas_4"],
        "Gas|Unconventional": ["gas_5", "gas_6", "gas_7", "gas_8"],
        "Uranium": ["uranium"],
    }
    result["Gas"] = result["Gas|Conventional"] + result["Gas|Unconventional"]
    result["Oil"] = result["Oil|Conventional"] + result["Oil|Unconventional"]

    return dict(c=result)


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
    c.add(f"c::{__name__} agg", get_commodity_groups())
    c.add(K.all[2], "aggregate", K.all[1], f"c::{__name__} agg", keep=False)

    # Transform to IAMC-structured data
    CONV.add_tasks(c)
