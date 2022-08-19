"""Atomic reporting computations for MESSAGEix-GLOBIOM."""
import itertools
import logging
from typing import List

from iam_units import convert_gwp
from iam_units.emissions import SPECIES
from ixmp.reporting import Quantity
import pandas as pd

log = logging.getLogger(__name__)

__all__ = [
    "gwp_factors",
    "make_output_path",
    "model_periods",
    "share_curtailment",
]


def gwp_factors():
    """Use :mod:`iam_units` to generate a Quantity of GWP factors.

    The quantity is dimensionless, e.g. for converting [mass] to [mass], andhas
    dimensions:

    - 'gwp metric': the name of a GWP metric, e.g. 'SAR', 'AR4', 'AR5'. All metrics are
       on a 100-year basis.
    - 'e': emissions species, as in MESSAGE. The entry 'HFC' is added as an alias for
      the species 'HFC134a' from iam_units.
    - 'e equivalent': GWP-equivalent species, always 'CO2'.
    """
    dims = ["gwp metric", "e", "e equivalent"]
    metric = ["SARGWP100", "AR4GWP100", "AR5GWP100"]
    species_to = ["CO2"]  # Add to this list to perform additional conversions

    data = []
    for m, s_from, s_to in itertools.product(metric, SPECIES, species_to):
        # Get the conversion factor from iam_units
        factor = convert_gwp(m, (1, "kg"), s_from, s_to).magnitude

        # MESSAGEix-GLOBIOM uses e='HFC' to refer to this species
        if s_from == "HFC134a":
            s_from = "HFC"

        # Store entry
        data.append((m[:3], s_from, s_to, factor))

    # Convert to Quantity object and return
    return Quantity(
        pd.DataFrame(data, columns=dims + ["value"]).set_index(dims)["value"].dropna()
    )


def make_output_path(config, name):
    """Return a path under the "output_dir" Path from the reporter configuration."""
    return config["output_dir"].joinpath(name)


def model_periods(y: List[int], cat_year: pd.DataFrame) -> List[int]:
    """Return the elements of `y` beyond the firstmodelyear of `cat_year`.

    .. todo:: move upstream, to :mod:`message_ix`.
    """
    return list(
        filter(
            lambda year: cat_year.query("type_year == 'firstmodelyear'")["year"].item()
            <= year,
            y,
        )
    )


# commented: currently unused
# def share_cogeneration(fraction, *parts):
#     """Deducts a *fraction* from the first of *parts*."""
#     return parts[0] - (fraction * sum(parts[1:]))


def share_curtailment(curt, *parts):
    """Apply a share of *curt* to the first of *parts*.

    If this is being used, it usually will indicate the need to split *curt* into
    multiple technologies; one for each of *parts*.
    """
    return parts[0] - curt * (parts[0] / sum(parts))
