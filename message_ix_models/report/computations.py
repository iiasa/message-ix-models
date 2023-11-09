"""Atomic reporting operations for MESSAGEix-GLOBIOM."""
import itertools
import logging
from typing import TYPE_CHECKING, Any, List, Mapping, Optional, Set, Tuple, Union

import ixmp
import pandas as pd
from genno.computations import pow
from genno.core.operator import Operator
from iam_units import convert_gwp
from iam_units.emissions import SPECIES
from ixmp.reporting import Quantity

from message_ix_models import Context

if TYPE_CHECKING:
    from genno import Computer, Key
    from sdmx.model.v21 import Code

log = logging.getLogger(__name__)

__all__ = [
    "from_url",
    "get_ts",
    "gwp_factors",
    "make_output_path",
    "model_periods",
    "remove_ts",
    "share_curtailment",
]


def codelist_to_groups(
    codes: List["Code"], dim: str = "n"
) -> Mapping[str, Mapping[str, List[str]]]:
    """Convert `codes` into a mapping from parent items to their children.

    The returned value is suitable for use with :func:`genno.computations.aggregate`.

    If this is a list of nodes per :func:`.get_codes`, then the mapping is from regions
    to the ISO 3166-1 alpha-3 codes of the countries within each region.
    """

    groups = dict()
    for code in filter(lambda c: len(c.child), codes):
        groups[code.id] = list(map(str, code.child))

    return {dim: groups}


def compound_growth(qty: Quantity, dim: str) -> Quantity:
    """Compute compound growth along `dim` of `qty`."""
    # Compute intervals along `dim`
    # The value at index d is the duration between d and the next index d+1
    c = qty.coords[dim]
    dur = (c - c.shift({dim: 1})).fillna(0).shift({dim: -1}).fillna(0)
    # - Raise the values of `qty` to the power of the duration.
    # - Compute cumulative product along `dim` from the first index.
    # - Shift, so the value at index d is the growth relative to the prior index d-1
    # - Fill in 1.0 for the first index.
    return pow(qty, Quantity(dur)).cumprod(dim).shift({dim: 1}).fillna(1.0)


@Operator.define
def exogenous_data():
    """No action.

    This exists to connect :func:`.exo_data.prepare_computer` to
    :meth:`genno.Computer.add`.
    """
    pass  # pragma: no cover


@exogenous_data.helper
def add_exogenous_data(
    func, c: "Computer", *, context=None, source=None, source_kw=None
) -> Tuple["Key"]:
    """Prepare `c` to compute exogenous data from `source`."""
    from message_ix_models.tools.exo_data import prepare_computer

    return prepare_computer(
        context or Context.get_instance(-1), c, source=source, source_kw=source_kw
    )


def get_ts(
    scenario: ixmp.Scenario,
    filters: Optional[dict] = None,
    iamc: bool = False,
    subannual: Union[bool, str] = "auto",
):
    """Retrieve timeseries data from `scenario`.

    Corresponds to :meth:`ixmp.Scenario.timeseries`.

    .. todo:: Move upstream, e.g. to :mod:`ixmp` alongside :func:`.store_ts`.
    """
    filters = filters or dict()

    return scenario.timeseries(iamc=iamc, subannual=subannual, **filters)


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

    .. todo:: Move upstream, to :mod:`message_ix`.
    """
    return list(
        filter(
            lambda year: cat_year.query("type_year == 'firstmodelyear'")["year"].item()
            <= year,
            y,
        )
    )


def remove_ts(
    scenario: ixmp.Scenario,
    config: dict,
    after: Optional[int] = None,
    dump: bool = False,
) -> None:
    """Remove all time series data from `scenario`.

    .. todo:: Improve to provide the option to remove only those periods in the model
       horizon.

    .. todo:: Move upstream, e.g. to :mod:`ixmp` alongside :func:`.store_ts`.
    """
    data = scenario.timeseries()
    N = len(data)
    count = f"{N}"

    if after:
        query = f"{after} <= year"
        data = data.query(query)
        count = f"{len(data)} of {N} ({query})"

    log.info(f"Remove {count} rows of time series data from {scenario.url}")

    # TODO improve scenario.transact() to allow timeseries_only=True; use here
    scenario.check_out(timeseries_only=True)
    try:
        scenario.remove_timeseries(data)
    except Exception:
        scenario.discard_changes()
    else:
        scenario.commit(f"Remove time series data ({__name__}.remove_all_ts)")

    if dump:
        raise NotImplementedError


# Non-weak references to objects to keep them alive
_FROM_URL_REF: Set[Any] = set()

# def from_url(url: str) -> message_ix.Scenario:
#     """Return a :class:`message_ix.Scenario` given its `url`.
#
#     .. todo:: Move upstream to :mod:`message_ix.reporting`.
#     .. todo:: Create a similar method in :mod:`ixmp.reporting` to load and return
#        :class:`ixmp.TimeSeries` (or :class:`ixmp.Scenario`) given its `url`.
#     """
#     s, mp = message_ix.Scenario.from_url(url)
#     assert s is not None
#     _FROM_URL_REF.add(s)
#     _FROM_URL_REF.add(mp)
#     return s


def from_url(url: str) -> ixmp.TimeSeries:
    """Return a :class:`ixmp.TimeSeries` given its `url`."""
    ts, mp = ixmp.TimeSeries.from_url(url)
    assert ts is not None
    _FROM_URL_REF.add(ts)
    _FROM_URL_REF.add(mp)
    return ts


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
