from functools import cache
from typing import TYPE_CHECKING

import genno
from genno import Keys, quote

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import AnyQuantity

    from message_ix_models import Context

UNIT = "EJ/yr"


@cache
def get_commodity_groups() -> dict[str, list[str]]:
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

    return result


def zeros_for_glb(nodes: list[str], years: list[int]) -> "AnyQuantity":
    """Return zeroes for a ``_GLB`` node.

    This ensures that the values appear after conversion to IAMC-structure.
    """
    from message_ix_models.model.structure import get_codes

    return genno.Quantity(0.0, coords=dict(n=["R11_GLB"]), units=UNIT).expand_dims(
        c=list(get_commodity_groups()),
        y=[int(c.id) for c in get_codes("year/B") if int(c.id) >= 1990],
    )


def callback(c: "Computer", context: "Context") -> None:
    from message_ix_models.report import iamc as handle_iamc
    from message_ix_models.report import util

    # Some keys
    k = Keys(
        model="EXT:n-c-y",  # Partial sum over 'g'
        hist="historical_extraction:n-c-y",  # Partial sum over 'g'
        glb="EXT:n-c:glb",
        all="EXT:n-c-y:all",
    )

    # Discard "EXT|" prefix on constructed variable names
    # TODO Improve handle_iamc() or underlying code to make this unnecessary
    util.REPLACE_VARS[r"^EXT\|"] = ""

    # Apply units
    c.add(k.hist[0], "apply_units", k.hist, units="GWa/year")
    c.add(k.model[0], "apply_units", k.model, units="GWa/year")

    # Add historical and current values together
    c.add(k.all[0], "add", k.hist[0], k.model[0])

    # Convert to target units
    c.add(k.all[1], "convert_units", k.all[0], units=UNIT)

    # Aggregate on 'c' dimension
    c.add(f"c::{__name__} agg", quote(dict(c=get_commodity_groups())))
    c.add(k.all[2], "aggregate", k.all[1], f"c::{__name__} agg", keep=False)

    # Combine with empty data for R11_GLB
    # FIXME the "n" and "y" keys are empty lists in test_compare()
    c.add(k.glb, zeros_for_glb, "n", "y")
    c.add(k.all[3], "add", k.all[2], k.glb)

    # Transform to IAMC-structured data
    handle_iamc(
        c,
        dict(
            variable=__name__,
            base=k.all[3],
            var=["Resource|Extraction", "c"],
            sums=["c"],
            unit=UNIT,
        ),
    )

    # Add to all::iamc
    c.graph["all::iamc"] += (f"{__name__}::iamc",)
