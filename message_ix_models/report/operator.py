"""Atomic reporting operations for MESSAGEix-GLOBIOM."""

import itertools
import logging
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Optional, Union

import ixmp
import pandas as pd
from genno import Quantity
from genno.core.operator import Operator
from genno.operator import pow
from iam_units import convert_gwp
from iam_units.emissions import SPECIES

from message_ix_models import Context
from message_ix_models.util import add_par_data, nodes_ex_world

if TYPE_CHECKING:
    from pathlib import Path

    from genno import Computer, Key
    from genno.types import AnyQuantity
    from sdmx.model.v21 import Code

log = logging.getLogger(__name__)

__all__ = [
    "add_par_data",
    "codelist_to_groups",
    "compound_growth",
    "exogenous_data",
    "filter_ts",
    "from_url",
    "get_ts",
    "gwp_factors",
    "make_output_path",
    "model_periods",
    "nodes_ex_world",
    "quantity_from_iamc",
    "remove_ts",
    "share_curtailment",
]


def codelist_to_groups(
    codes: list["Code"], dim: str = "n"
) -> Mapping[str, Mapping[str, list[str]]]:
    """Convert `codes` into a mapping from parent items to their children.

    The returned value is suitable for use with :func:`genno.operator.aggregate`.

    If this is a list of nodes per :func:`.get_codes`, then the mapping is from regions
    to the ISO 3166-1 alpha-3 codes of the countries within each region. The code for
    the region itself is also included in the values to be aggregated, so that already-
    aggregated data will pass through.
    """

    groups = dict()
    for code in filter(lambda c: len(c.child), codes):
        groups[code.id] = [code.id] + list(map(str, code.child))

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


@Operator.define()
def exogenous_data():
    """No action.

    This exists to connect :func:`.exo_data.prepare_computer` to
    :meth:`genno.Computer.add`.
    """
    pass  # pragma: no cover


@exogenous_data.helper
def add_exogenous_data(
    func, c: "Computer", *, context=None, source=None, source_kw=None
) -> tuple["Key", ...]:
    """Prepare `c` to compute exogenous data from `source`."""
    from message_ix_models.tools.exo_data import prepare_computer

    return prepare_computer(
        context or Context.get_instance(-1), c, source=source, source_kw=source_kw
    )


def filter_ts(df: pd.DataFrame, expr: re.Pattern, *, column="variable") -> pd.DataFrame:
    """Filter time series data in `df`.

    1. Keep only rows in `df` where `expr` is a full match (
       :meth:`~pandas.Series.str.fullmatch`) for the entry in `column`.
    2. Retain only the first match group ("...(...)...") from `expr` as the `column`
       entry.
    """
    return df[df[column].str.fullmatch(expr)].assign(
        variable=df[column].str.replace(expr, r"\1", regex=True)
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


def gwp_factors() -> Quantity:
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


def make_output_path(config: Mapping, name: Union[str, "Path"]) -> "Path":
    """Return a path under the "output_dir" Path from the reporter configuration."""
    return config["output_dir"].joinpath(name)


def model_periods(y: list[int], cat_year: pd.DataFrame) -> list[int]:
    """Return the elements of `y` beyond the firstmodelyear of `cat_year`.

    .. todo:: Move upstream, to :mod:`message_ix`.
    """
    y0 = cat_year.query("type_year == 'firstmodelyear'")["year"].item()
    return list(filter(lambda year: y0 <= year, y))


def remove_ts(
    scenario: ixmp.Scenario,
    config: Optional[dict] = None,
    after: Optional[int] = None,
    dump: bool = False,
) -> None:
    """Remove all time series data from `scenario`.

    Note that data stored with :meth:`.add_timeseries` using :py:`meta=True` as a
    keyword argument cannot be removed using :meth:`.TimeSeries.remove_timeseries`, and
    thus also not with this operator.

    .. todo:: Move upstream, to :mod:`ixmp` alongside :func:`.store_ts`.
    """
    if dump:
        raise NotImplementedError

    data = scenario.timeseries().drop("value", axis=1)
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


# Non-weak references to objects to keep them alive
_FROM_URL_REF: set[Any] = set()


def from_url(url: str, cls=ixmp.TimeSeries) -> ixmp.TimeSeries:
    """Return a :class:`ixmp.TimeSeries` or subclass instance, given its `url`.

    .. todo:: Move upstream, to :mod:`ixmp.report`.

    Parameters
    ----------
    cls : type, optional
        Subclass to instantiate and return; for instance, :class:`.Scenario`.
    """
    ts, mp = cls.from_url(url)
    assert ts is not None
    _FROM_URL_REF.add(ts)
    _FROM_URL_REF.add(mp)
    return ts


def quantity_from_iamc(qty: "AnyQuantity", variable: str) -> "AnyQuantity":
    """Extract data for a single measure from `qty` with (at least) dimensions v, u.

    .. todo:: Move upstream, to either :mod:`ixmp` or :mod:`genno`.

    Parameters
    ----------
    variable : str
        Regular expression to match the ``v`` dimension of `qty`.
    """
    from genno.operator import relabel, select

    expr = re.compile(variable)
    variables, replacements = [], {}
    for var in qty.coords["v"].data:
        if match := expr.fullmatch(var):
            variables.append(match.group(0))
            replacements[match.group(0)] = match.group(1)

    subset = qty.pipe(select, {"v": variables}).pipe(relabel, {"v": replacements})

    unique_units = subset.coords["Unit"].data
    assert 1 == len(unique_units)
    subset.units = unique_units[0]
    return subset.sel(Unit=unique_units[0], drop=True)


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
