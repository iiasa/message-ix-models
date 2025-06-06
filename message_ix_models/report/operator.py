"""Atomic reporting operations for MESSAGEix-GLOBIOM."""

import logging
import re
from collections.abc import (
    Callable,
    Hashable,
    Iterable,
    Mapping,
    MutableMapping,
    Sequence,
)
from functools import reduce
from itertools import filterfalse, product
from typing import TYPE_CHECKING, Any, Optional, Union

import genno
import ixmp
import pandas as pd
from genno.operator import pow
from iam_units import convert_gwp
from iam_units.emissions import SPECIES

from message_ix_models.util import MappingAdapter, add_par_data, nodes_ex_world

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Protocol

    from genno.types import AnyQuantity, TQuantity
    from sdmx.model.v21 import Code

    from message_ix_models.types import ParameterData

    class SupportsLessThan(Protocol):
        def __lt__(self, __other: Any) -> bool: ...


log = logging.getLogger(__name__)

__all__ = [
    "add_par_data",
    "broadcast_wildcard",
    "call",
    "codelist_to_groups",
    "compound_growth",
    "filter_ts",
    "from_url",
    "get_ts",
    "gwp_factors",
    "make_output_path",
    "merge_data",
    "model_periods",
    "nodes_ex_world",
    "nodes_world_agg",
    "quantity_from_iamc",
    "remove_ts",
    "select_allow_empty",
    "select_expand",
    "share_curtailment",
]


def broadcast_wildcard(
    qty: "TQuantity",
    *coords: Sequence["SupportsLessThan"],
    dim: Union[Hashable, Sequence[Hashable]] = "n",
) -> "TQuantity":
    """Broadcast over coordinates `coords` along respective dimension(s) `dim`.

    `dim` may identify a single dimension or a sequence of dimensions; `coords` must
    be given for each dimension.

    For each respective items from `dim` and `coords`, any missing coordinates along the
    dimension are populated using the values of `qty` keyed with the 'wildcard' label
    "*".
    """
    if isinstance(dim, Hashable) and not isinstance(dim, tuple):
        dim = (dim,)
    if len(dim) != len(coords):
        raise ValueError(
            f"Must provide same number of dim (got |{dim}| = {len(dim)}) as coords "
            f"(got {len(coords)})"
        )

    mapping = {}
    # Iterate over dimensions `d` and respective coords `c`
    for d, c in zip(dim, coords):
        # Identify existing, non-wildcard labels along `dim`
        existing = set(qty.coords[d].data) - {"*"}
        # Identify missing labels along `dim`
        missing = sorted(set(c) - existing)

        if not missing:
            continue  # Nothing to do for dim `d`; `qty` has a complete set of labels

        # - Each existing label mapped to itself.
        # - "*" mapped to each missing label.
        mapping[d] = [(x, x) for x in sorted(existing)] + [("*", x) for x in missing]

    if not mapping:
        return qty  # Nothing to do for *any* dimension

    # Construct a MappingAdapter and apply to `qty`
    return MappingAdapter(mapping)(qty)


def call(callable, *args, **kwargs):
    """Invoke a callable on other arguments."""
    return callable(*args, **kwargs)


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


def compound_growth(qty: "TQuantity", dim: str) -> "TQuantity":
    """Compute compound growth along `dim` of `qty`."""
    # Compute intervals along `dim`
    # The value at index d is the duration between d and the next index d+1
    c = qty.coords[dim]
    dur = (c - c.shift({dim: 1})).fillna(0).shift({dim: -1}).fillna(0)
    # - Raise the values of `qty` to the power of the duration.
    # - Compute cumulative product along `dim` from the first index.
    # - Shift, so the value at index d is the growth relative to the prior index d-1
    # - Fill in 1.0 for the first index.
    return pow(qty, type(qty)(dur)).cumprod(dim).shift({dim: 1}).fillna(1.0)


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


def gwp_factors() -> "AnyQuantity":
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
    for m, s_from, s_to in product(metric, SPECIES, species_to):
        # Get the conversion factor from iam_units
        factor = convert_gwp(m, (1, "kg"), s_from, s_to).magnitude

        # MESSAGEix-GLOBIOM uses e='HFC' to refer to this species
        if s_from == "HFC134a":
            s_from = "HFC"

        # Store entry
        data.append((m[:3], s_from, s_to, factor))

    # Convert to Quantity object and return
    return genno.Quantity(
        pd.DataFrame(data, columns=dims + ["value"]).set_index(dims)["value"].dropna()
    )


def make_output_path(config: Mapping, name: Union[str, "Path"]) -> "Path":
    """Return a path under the "output_dir" Path from the reporter configuration."""
    return config["output_dir"].joinpath(name)


def merge_data(*others: "ParameterData") -> "ParameterData":
    """Alternate form of :func:`message_ix_models.util.merge_data`.

    This form returns a new, distinct :class:`dict` and does not mutate any of its
    arguments.
    """
    keys: set[str] = reduce(lambda x, y: x | y.keys(), others, set())
    return {
        k: pd.concat([o.get(k, None) for o in others], ignore_index=True) for k in keys
    }


def model_periods(y: list[int], cat_year: pd.DataFrame) -> list[int]:
    """Return the elements of `y` beyond the firstmodelyear of `cat_year`.

    .. todo:: Move upstream, to :mod:`message_ix`.
    """
    y0 = cat_year.query("type_year == 'firstmodelyear'")["year"].item()
    return list(filter(lambda year: y0 <= year, y))


def nodes_world_agg(
    config: dict, *, dim: Hashable = "nl", name: Optional[str] = "{}_GLB"
) -> Mapping[Hashable, Mapping[Hashable, list[str]]]:
    """Mapping to aggregate e.g. nl="World" from values for child nodes of "World".

    This mapping should be used with :func:`.genno.operator.aggregate`, giving the
    argument ``keep=False``. It includes 1:1 mapping from each region name to itself.
    """
    from message_ix_models.model.structure import get_codelist

    cl = get_codelist(f"node/{config['regions']}")

    # "World" node should have be top-level (its parent is the `cl` itself) and have
    # some children. Countries (from pycountry) that are omitted from a mapping have no
    # children.
    try:
        node = next(filter(lambda n: n.parent is cl and len(n.child), cl))
    except StopIteration:
        raise RuntimeError("Failed to identify a 'World' node")

    if name:
        # FIXME Remove. This is a hack to suit the legacy reporting, which expects
        #       global aggregates at *_GLB rather than "World".
        name = name.format(config["regions"])
        log.info(f"Aggregates for {node!r} will be labelled {name!r}")
    else:
        name = node.id

    # Global total as aggregate of child nodes
    result: MutableMapping = {name: list(map(str, node.child))}
    # Also add "no-op" aggregates e.g. "R12_AFR" is the sum of ["R12_AFR"]
    result.update({c: [c] for c in map(str, node.child)})

    return {dim: result}


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

    .. deprecated:: 2025-02-17
       Use :func:`.genno.compat.pyam.operator.quantity_from_iamc` instead.

    Parameters
    ----------
    variable : str
        Regular expression to match the ``v`` dimension of `qty`.
    """
    from warnings import warn

    from genno.operator import relabel, select

    warn(
        "message-ix-models.report.operator.quantity_from_iamc(); use "
        "genno.compat.pyam.operator.quantity_from_iamc() instead",
        DeprecationWarning,
        stacklevel=2,
    )

    expr = re.compile(variable)
    variables, replacements = [], {}
    for var in qty.coords["v"].data:
        if match := expr.fullmatch(var):
            variables.append(match.group(0))
            replacements[match.group(0)] = match.group(1)

    # Prevent a mypy warning in the next line; see https://github.com/khaeru/genno#166
    assert isinstance(qty, genno.Quantity)

    subset = qty.pipe(select, {"v": variables}).pipe(relabel, {"v": replacements})

    unique_units = subset.coords["Unit"].data
    assert 1 == len(unique_units)
    subset.units = unique_units[0]
    return subset.sel(Unit=unique_units[0], drop=True)


def select_allow_empty(
    qty: "TQuantity",
    indexers: Mapping[Hashable, Iterable[Hashable]],
    *,
    inverse: bool = False,
    drop: bool = False,
) -> "TQuantity":
    """:func:`genno.operator.select` allowing for missing data.

    If *any* of the `indexers` are missing from `qty`, return an empty Quantity with the
    same dimensions as `qty`.

    .. todo:: Move upstream to :mod:`genno`.
    """
    import genno.operator

    try:
        return genno.operator.select(qty, indexers=indexers, inverse=inverse, drop=drop)
    except KeyError:
        return genno.Quantity([], coords={d: [] for d in qty.dims})


def select_expand(
    qty: "TQuantity",
    dim_cb: dict[str, Callable[[Hashable], dict[Hashable, Hashable]]],
) -> "TQuantity":
    """Select and expand dimensions using callbacks for existing coordinates.

    This combines behaviours of :func:`genno.operator.select`,
    :func:`genno.operator.relabel`, and :func:`genno.operator.rename`. Specifically,
    for each (`dim`, `function`) in `dim_cb`:

    - The `function` is applied to each coordinate/label along `dim` of `qty`.
    - If the return value is :any:`False`-y (for example, empty :class:`dict`), all data
      indexed by that label is discarded.
    - If the return value is a non-empty :class:`dict`, data are preserved. The `dim`
      label is replaced by 1+ new dimension(s) and label(s) from the keys and values,
      respectively, of the return value.

    Parameters
    ----------
    dim_cb :
       Mapping from dimensions of `qty` to callback functions. Each function should
       receive a single coordinate label, and return a :class:`dict`.
    """
    # TODO Rewrite using xarray-indexing semantics

    # Data frames to concatenate along axis=1
    dfs = [qty.to_series().rename("value").reset_index()]

    # Mask for subset of data to be preserved
    keep = pd.Series(True, index=dfs[0].index)

    # Iterate over `dim`ensions in `dim_cb`
    for dim, func in dim_cb.items():
        # Apply `func` to the original dimension labels
        dfs.append(dfs[0][dim].apply(func))

        # Update mask to exclude rows for which `func` returned None, False, {}, etc.
        keep &= dfs[-1].astype(bool)

    # - Select only `mask` values for each of the `dfs`.
    # - Concatenate with the original data.
    df = pd.concat(
        [dfs[0][keep].reset_index(drop=True)]
        + [pd.DataFrame(df[keep].tolist()) for df in dfs[1:]],
        axis=1,
    )

    # Columns not in the original, but returned by one of `dim_cb`: these may be new or
    # may preserve original values
    columns = df.columns.tolist()
    new_dims = columns[columns.index("value") + 1 :]

    # Columns that are not result dims: "value"; original dims that appear in `dim_cb`
    # and not in `new_dims`
    exclude = {"value"} | set(dim_cb) - set(new_dims)

    # Identify dimensions of the result
    result_dims = list(filterfalse(exclude.__contains__, columns))

    return type(qty)(df.set_index(result_dims)["value"])


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
