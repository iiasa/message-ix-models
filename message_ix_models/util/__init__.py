import logging
from collections import ChainMap, defaultdict
from collections.abc import Collection, Iterable, Mapping, MutableMapping, Sequence
from datetime import datetime
from functools import partial, singledispatch
from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional, Protocol, Union

import message_ix
import pandas as pd
import pint

from ._convert_units import convert_units
from ._logging import mark_time, once, preserve_log_level, silence_log
from .cache import cached
from .common import (
    HAS_MESSAGE_DATA,
    MESSAGE_DATA_PATH,
    MESSAGE_MODELS_PATH,
    Adapter,
    MappingAdapter,
    WildcardAdapter,
    load_package_data,
    load_private_data,
    local_data_path,
    package_data_path,
    private_data_path,
)
from .importlib import minimum_version
from .node import adapt_R11_R12, adapt_R11_R14, identify_nodes, nodes_ex_world
from .scenarioinfo import ScenarioInfo, Spec
from .sdmx import CodeLike, as_codes, eval_anno

if TYPE_CHECKING:
    import genno

    from message_ix_models.types import MutableParameterData, ParameterData

    from .context import Context

__all__ = [
    "HAS_MESSAGE_DATA",
    "MESSAGE_DATA_PATH",
    "MESSAGE_MODELS_PATH",
    "Adapter",
    "MappingAdapter",
    "WildcardAdapter",
    "adapt_R11_R12",
    "adapt_R11_R14",
    "add_par_data",
    "aggregate_codes",
    "as_codes",
    "broadcast",
    "cached",
    "check_support",
    "convert_units",
    "copy_column",
    "datetime_now_with_tz",
    "eval_anno",
    "ffill",
    "identify_nodes",
    "iter_keys",
    "load_package_data",
    "load_private_data",
    "local_data_path",
    "make_io",
    "make_matched_dfs",
    "make_source_tech",
    "mark_time",
    "maybe_query",
    "merge_data",
    "minimum_version",
    "package_data_path",
    "path_fallback",
    "preserve_log_level",
    "private_data_path",
    "replace_par_data",
    "same_node",
    "same_time",
    "show_versions",
    "silence_log",
    "strip_par_data",
]

log = logging.getLogger(__name__)


def add_par_data(
    scenario: message_ix.Scenario, data: "ParameterData", dry_run: bool = False
) -> int:
    """Add `data` to `scenario`.

    Parameters
    ----------
    data
        Any mapping with keys that are valid :mod:`message_ix` parameter names, and
        values that are pd.DataFrame or other arguments valid for
        :meth:`message_ix.Scenario.add_par`.
    dry_run : optional
        Only show what would be done.

    See also
    --------
    strip_par_data
    """
    # TODO optionally add units automatically
    # TODO allow units column entries to be pint.Unit objects

    total = 0

    for par_name, values in data.items():
        N = values.shape[0]
        log.info(f"{N} rows in {repr(par_name)}")
        log.debug("\n" + values.to_string(max_rows=5))

        total += N

        if dry_run:
            continue

        # Work around iiasa/ixmp#425
        values["unit"] = values["unit"].str.replace("^$", "-", regex=True)

        try:
            scenario.add_par(par_name, values)
        except Exception:  # pragma: no cover
            print(values.head())
            raise

    return total


def aggregate_codes(df: pd.DataFrame, dim: str, codes):  # pragma: no cover
    """Aggregate `df` along dimension `dim` according to `codes`."""
    raise NotImplementedError

    # Construct an inverse mapping
    mapping = {}
    for code in codes:
        mapping.update({child.id: code.id for child in code.child})

    for key, group_series in df.groupby(dim):
        print(key, group_series.replace({dim: mapping}))


def broadcast(
    df: pd.DataFrame, labels: Optional[pd.DataFrame] = None, **kwargs
) -> pd.DataFrame:
    """Fill missing data in `df` by broadcasting.

    :func:`broadcast` is suitable for use with partly-filled data frames returned by
    :func:`.message_ix.util.make_df`, with 1 column per dimension, plus a "value"
    column. It is also usable with :meth:`pandas.DataFrame.pipe` for chained operations.

    `labels` (if any) are handled first: one copy or duplicate of `df` is produced for
    each row (set of labels) in this argument. Then, `kwargs` are handled;
    :func:`broadcast` returns one copy for each element in the cartesian product of the
    dimension labels given by `kwargs`.

    Parameters
    ----------
    labels : pandas.DataFrame
        Each column (dimension) corresponds to one in `df`. Each row represents one
        matched set of labels for those dimensions.
    kwargs
        Keys are dimensions. Values are labels along that dimension to fill.

    Returns
    -------
    pandas.DataFrame
        The length is either 1 or an integer multiple of the length of `df`.

    Raises
    ------
    ValueError
        if any of the columns in `labels` or `kwargs` are not present in `df`, or if
        those columns are present but not empty.

    Examples
    --------
    >>> from message_ix import make_df
    >>> from message_ix_models.util import broadcast
    # Create a base data frame with some empty columns
    >>> base = make_df("input", technology="t", value=[1.1, 2.2])
    # Broadcast (duplicate) the data across 2 dimensions
    >>> df = base.pipe(broadcast, node_loc=["node A", "node B"], mode=["m0", "m1"])
    # Show part of the result
    >>> df.dropna(axis=1)
      mode node_loc technology  value
    0   m0   node A          t    1.1
    1   m0   node A          t    2.2
    2   m0   node B          t    1.1
    3   m0   node B          t    2.2
    4   m1   node A          t    1.1
    5   m1   node A          t    2.2
    6   m1   node B          t    1.1
    7   m1   node B          t    2.2
    """

    def _check_dim(d):
        try:
            if not df[d].isna().all():
                raise ValueError(f"Dimension {d} was not empty\n\n{df.head()}")
        except KeyError:
            raise ValueError(f"Dimension {d} not among {list(df.columns)}")

    # Broadcast using matched labels for 1+ dimensions from a data frame
    if labels is not None:
        # Check the dimensions
        for dim in labels.columns:
            _check_dim(dim)
        # Concatenate 1 copy of `df` for each row in `labels`
        df = pd.concat(
            [df.assign(**row) for _, row in labels.iterrows()],
            ignore_index=True,
            sort=False,
        )

    # Next, broadcast other dimensions given as keyword arguments
    for dim, levels in kwargs.items():
        _check_dim(dim)
        if len(levels) == 0:
            log.debug(
                f"Don't broadcast over {repr(dim)}; labels {levels} have length 0"
            )
            continue

        # - Duplicate the data
        # - Drop the existing column named 'dim'
        # - Re-add the column from the constructed MultiIndex
        # - Reindex for sequential row numbers
        df = (
            pd.concat([df] * len(levels), keys=levels, names=[dim], sort=False)
            .drop(dim, axis=1)
            .reset_index(dim)
            .reset_index(drop=True)
        )
    return df


def check_support(context, settings=dict(), desc: str = "") -> None:
    """Check whether a Context is compatible with certain `settings`.

    Raises
    ------
    :class:`NotImplementedError`
        if any `context` value for a key of `settings` is not among the values in
        `settings`.
    :class:`KeyError`
        if the key is not set on `context` at all.

    See also
    --------
    :ref:`check-support`

    """
    __tracebackhide__ = True
    for key, values in settings.items():
        if context[key] not in values:
            raise NotImplementedError(
                f"{desc} for {repr(values)}; got {repr(context[key])}"
            )


def copy_column(column_name):
    """For use with :meth:`pandas.DataFrame.assign`.

    Examples
    --------
    Modify `df` by filling the column 'baz' with the value ``3``, and copying the column
    'bar' into column 'foo'.

    >>> df.assign(foo=copy_column('bar'), baz=3)

    Note that a similar assignment can be achieved with :meth:`~pandas.DataFrame.eval`:

    >>> df.eval("foo = bar")

    :func:`copy_column` is useful in the context of more complicated calls to
    :meth:`~pandas.DataFrame.assign`.
    """
    return lambda df: df[column_name]


def datetime_now_with_tz() -> datetime:
    """Current date and time with time zone information."""
    tz = datetime.now().astimezone().tzinfo
    return datetime.now(tz)


def either_dict_or_kwargs(name: str, dict_arg: Optional[dict], kwargs: dict) -> dict:
    """Return either `dict_arg` or `kwargs`; raise :class:`ValueError` if both."""
    if dict_arg is None:
        return kwargs
    elif len(kwargs):
        raise ValueError(f"Both {name}={{...}} and positional {name} args")
    else:
        return dict_arg.copy()


def ffill(
    df: pd.DataFrame, dim: str, values: Sequence[CodeLike], expr: Optional[str] = None
) -> pd.DataFrame:
    """Forward-fill `df` on `dim` to cover `values`.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to fill forwards.
    dim : str
        Dimension to fill along. Must be a column in `df`.
    values : list of str
        Labels along `dim` that must be present in the returned data frame.
    expr : str, optional
        If provided, :meth:`.DataFrame.eval` is called. This can be used to assign one
        column to another. For instance, if `dim` == "year_vtg" and `expr` is "year_act
        = year_vtg", then forward filling is performed along the "year_vtg" dimension/
        column, and then the filled values are copied to the "year_act" column.
    """
    if dim in ("value", "unit"):
        raise ValueError(dim)

    # Mapping from (values existing in `df`) -> equal or greater members of `values`
    mapping = defaultdict(set)
    last_seen = None
    for v in sorted(set(values) | set(df[dim].unique())):
        if v in df[dim].unique():
            last_seen = v
        mapping[last_seen].add(v)

    def _maybe_eval(df):
        return df.eval(expr) if expr is not None else df

    dfs = [df]
    for key, group_df in df.groupby(dim):
        for new_label in sorted(mapping[key])[1:]:
            # Duplicate the data; assign the new_label to `dim`
            dfs.append(group_df.assign(**{dim: new_label}).pipe(_maybe_eval))

    return pd.concat(dfs, ignore_index=True)


class KeyIterator(Protocol):
    def __call__(self) -> "genno.Key": ...


def iter_keys(base: "genno.Key") -> KeyIterator:
    """Return an iterator over a sequence of keys starting with `base_key`.

    This can be used for shorthand when constructing sequences of :mod:`genno`
    computations.

    Example
    -------
    >>> base_key = genno.Key("foo:a-b-c")
    >>> k = iter_keys(base_key)
    >>> k()
    <foo:a-b-c:0>
    >>> k()
    <foo:a-b-c:1>
    >>> k()
    <foo:a-b-c:2>
    """
    return partial(next, map(lambda i: base + str(i), count()))


def make_io(
    src: tuple[str, str, str],
    dest: tuple[str, str, str],
    efficiency: float,
    on: Literal["input", "output"] = "input",
    **kwargs,
):
    """Return input and output data frames for a 1-to-1 technology.

    Parameters
    ----------
    src : tuple of str
        Input (commodity, level, unit)
    dest : tuple of str
        Output (commodity, level, unit)
    efficiency : float
        Conversion efficiency.
    on : 'input' or 'output'
        If 'input', `efficiency` applies to the input, and the output, thus the activity
        level of the technology, is in dest[2] units. If 'output', the opposite.
    kwargs
        Passed to :func:`.make_df`.

    Returns
    -------
    dict (str -> pd.DataFrame)
        Keys are 'input' and 'output'; values are data frames.
    """
    return dict(
        input=message_ix.make_df(
            "input",
            commodity=src[0],
            level=src[1],
            unit=src[2],
            value=efficiency if on == "input" else 1.0,
            **kwargs,
        ),
        output=message_ix.make_df(
            "output",
            commodity=dest[0],
            level=dest[1],
            unit=dest[2],
            value=1.0 if on == "input" else efficiency,
            **kwargs,
        ),
    )


def make_matched_dfs(
    base: Union[MutableMapping, pd.DataFrame],
    **par_value: Union[float, pint.Quantity, dict],
) -> "MutableParameterData":
    """Return data frames derived from `base` for multiple parameters.

    Creates one data frame per keyword argument.

    Parameters
    ----------
    base : pandas.DataFrame, dict, etc.
        Used to populate other columns of each data frame. Duplicates—which occur when
        the target parameter has fewer dimensions than `base`—are dropped.
    par_values :
        Argument names (e.g. ‘fix_cost’) are passed to :func:`.make_df`. If the value is
        :class:`float`, it overwrites the "value" column; if :class:`pint.Quantity`, its
        magnitude overwrites "value" and its units the "units" column, as a formatted
        string.

    Returns
    -------
    :class:`dict` of :class:`pandas.DataFrame`
        one for each parameter in `par_values`.

    Examples
    --------
    >>> input = make_df("input", ...)
    >>> cf_tl = make_matched_dfs(
    >>>     input,
    >>>     capacity_factor=1,
    >>>     technical_lifetime=pint.Quantity(8, "year"),
    >>> )
    """
    replace: dict[str, Any] = dict()
    data = ChainMap(replace, base)
    result = dict()
    for par, values in par_value.items():
        replace.clear()
        if isinstance(values, dict):
            replace.update(values)
            value = replace.pop("value")
        else:
            value = values

        if isinstance(value, pint.Quantity):
            replace["value"] = value.magnitude
            replace["unit"] = f"{value.units:~}"
        else:
            replace["value"] = value

        result[par] = (
            message_ix.make_df(par, **data).drop_duplicates().reset_index(drop=True)
        )
    return result


def make_source_tech(
    info: Union[message_ix.Scenario, ScenarioInfo], common, **values
) -> "MutableParameterData":
    """Return parameter data for a ‘source’ technology.

    The technology has no inputs; its output commodity and/or level are determined by
    `common`; either single values, or :obj:`None` if the result will be
    :meth:`~pandas.DataFrame.pipe`'d through :func:`broadcast`.

    Parameters
    ----------
    info : .Scenario or .ScenarioInfo
    common : dict
        Passed to :func:`.make_df`.
    **values
        Values for 'capacity_factor' (optional; default 1.0), 'output', 'var_cost', and
        optionally 'technical_lifetime'.

    Returns
    -------
    dict
        Suitable for :func:`add_par_data`.
    """
    # Check arguments
    if isinstance(info, message_ix.Scenario):
        info = ScenarioInfo(info)

    values.setdefault("capacity_factor", 1.0)
    missing = {"capacity_factor", "output", "var_cost"} - set(values.keys())
    if len(missing):
        raise ValueError(f"make_source_tech() needs values for {repr(missing)}")
    elif "technical_lifetime" not in values:
        log.debug("No technical_lifetime for source technology")

    # Create data for "output"
    result = dict(
        output=message_ix.make_df(
            "output",
            value=values.pop("output"),
            year_act=info.Y,
            year_vtg=info.Y,
            **common,
        )
        .pipe(broadcast, node_loc=nodes_ex_world(info.N))
        .pipe(same_node)
    )

    # Add data for other parameters
    result.update(make_matched_dfs(base=result["output"], **values))

    return result


def maybe_query(series: pd.Series, query: Optional[str]) -> pd.Series:
    """Apply :meth:`pandas.DataFrame.query` if the `query` arg is not :obj:`None`.

    :meth:`~pandas.DataFrame.query` is not chainable (`pandas-dev/pandas#37941
    <https://github.com/pandas-dev/pandas/issues/37941>`_). Use this function with
    :meth:`pandas.Series.pipe`, passing an argument that may be :obj:`None`, to have a
    chainable query operation that can be a no-op.
    """
    # Convert Series to DataFrame, query(), then retrieve the single column
    return series if query is None else series.to_frame().query(query)[0]


def merge_data(base: "MutableParameterData", *others: "ParameterData") -> None:
    """Merge dictionaries of DataFrames together into `base`.

    For use with :mod:`genno`, see instead :func:`.report.operator.merge_data` that
    *returns* the merged data rather than updating the first argument.
    """
    for other in others:
        for par, df in other.items():
            base[par] = pd.concat([base.get(par, None), df])


def path_fallback(
    *parts: Union[str, Path],
    where: Union[str, list[Union[str, Path]]] = "",
    context: Optional["Context"] = None,
) -> Path:
    """Locate a path constructed from `parts` found in the first of several directories.

    This allows to implement ‘fallback’ behaviour in which files or directories in
    certain locations are used preferentially.

    Parameters
    ----------
    parts :
        Path parts or fragments such as directory names and a final file name.
    where :
        Either:

        - :class:`str` containing one or more of the following, separated by white
          space:

          - "cache": locate `parts` in the :mod:`message_ix_models` cache directory.
            See :attr:`.Config.cache_path`.
          - "local": locate `parts` in the user's local data directory (same as
            :func:`local_data_path`).
          - "package": locate `parts` in :mod:`message_ix_models` package data (same
            as :func:`.package_data_path`).
          - "private": locate `parts` in the :mod:`message_data` :file:`/data/`
            directory (same as :func:`.private_data_path`).
          - "test": locate test data in :py:`package_data_path("test", ...)`

        - :class:`list` where each element is :class:`str` (one of the above) or a
          :class:`pathlib.Path`.

    Returns
    -------
    pathlib.Path
        The first of the locations indicated by `where` in which the file or directory
        `parts` exists.

    Raises
    ------
    ValueError
        If `where` is empty or `parts` are not found in any of the indicated locations.
    """
    from .context import Context

    context = context or Context.get_instance(-1)
    dirs, test_dir = [], None
    for item in where.split() if isinstance(where, str) else where:
        if isinstance(item, str):
            if item == "cache":
                dirs.append(context.core.cache_path)
            elif item == "local":
                dirs.append(context.core.local_data)
            elif item == "package":
                dirs.append(package_data_path())
            elif item == "private":
                dirs.append(private_data_path())
            elif item == "test":
                test_dir = package_data_path("test")
                dirs.append(test_dir)
        else:
            dirs.append(item)

    for path in [d.joinpath(*parts) for d in dirs]:
        if not path.exists():
            once(log, logging.DEBUG, f"Not found: {path}")
            continue
        elif test_dir and path.is_relative_to(test_dir):
            msg = f"Reading test (fuzzed, random, and/or partial) data from {path}"
            once(log, logging.WARNING, msg)
        return path

    raise ValueError(
        f"No directories identified among {where!r}"
        if not dirs
        else f"'{Path(*parts)!s}' not found in any of {dirs}"
    )


def replace_par_data(
    scenario: message_ix.Scenario,
    parameters: Union[str, Sequence[str]],
    filters: Mapping[str, Union[str, int, Collection[str], Collection[int]]],
    to_replace: Mapping[str, Union[Mapping[str, str], Mapping[int, int]]],
) -> None:
    """Replace data in `parameters` of `scenario`.

    Parameters
    ----------
    scenario
        Scenario in which to replace data.
    parameters : str or sequence of str
        Name(s) of parameters in which to replace data.
    filters
        Passed to :meth:`.Scenario.par` argument of the same name.
    to_replace
        Passed to :meth:`pandas.DataFrame.replace` argument of the same name.

    Examples
    --------
    Replace data in the "relation_activity" parameter for a particular technology and
    relation: assign the same values as entries in a different relation name for the
    same technology.

    >>> replace_par_data(
    ...     scenario,
    ...     "relation_activity",
    ...     dict(technology="hp_gas_i", relation="CO2_r_c"),
    ...     dict(relation={"CO2_r_c": "CO2_ind"}),
    ... )

    """
    from message_ix_models.model.build import apply_spec

    pars = parameters.split() if isinstance(parameters, str) else parameters

    # Create a Spec that requires `scenario` to have all the set elements mentioned by
    # `filters` and/or `replacements`
    s = Spec()
    for k, v in filters.items():
        s.require.set[k].extend([v] if isinstance(v, (str, int)) else v)
    for k, v in to_replace.items():
        s.require.set[k].extend(v.keys())
        s.require.set[k].extend(v.values())

    # Use apply_spec() simply to check that `scenario` contains the expected items,
    # before attempting to modify data
    apply_spec(scenario, s)

    msg = f"Replace {filters!r} with {to_replace!r}"
    log.info(msg)
    for par_name in pars:
        with scenario.transact(f"{msg} in {par_name!r}"):
            # Base data, to be replaced
            to_remove = scenario.par(par_name, filters=filters)
            # Remove the base data
            scenario.remove_par(par_name, to_remove.drop(columns=["value", "unit"]))
            # Add the modified data
            scenario.add_par(par_name, to_remove.replace(to_replace))

            log.info(f"{len(to_remove)} obs in {par_name!r}")


@singledispatch
def same_node(data: pd.DataFrame, from_col: str = "node_loc") -> pd.DataFrame:
    """Fill 'node_{,dest,loc,origin,rel,share}' in `df` from `from_col`."""
    cols = list(
        set(data.columns)
        & ({"node", "node_loc", "node_origin", "node_dest", "node_rel", "node_share"})
        - {from_col}
    )
    return data.assign(**{c: copy_column(from_col) for c in cols})


@same_node.register(dict)
def _(
    data: "MutableParameterData", from_col: str = "node_loc"
) -> "MutableParameterData":
    for key, df in data.items():
        data[key] = same_node(df, from_col=from_col)
    return data


@singledispatch
def same_time(data: pd.DataFrame) -> pd.DataFrame:
    """Fill 'time_origin'/'time_dest' in `df` from 'time'."""
    cols = list(set(data.columns) & {"time_origin", "time_dest"})
    return data.assign(**{c: copy_column("time") for c in cols})


@same_time.register(dict)
def _(data: "MutableParameterData") -> "MutableParameterData":
    for key, df in data.items():
        data[key] = same_time(df)
    return data


def show_versions() -> str:
    """Output of :func:`ixmp.show_versions`, as a :class:`str`."""
    from io import StringIO

    import ixmp

    from ._logging import preserve_log_handlers

    # Retrieve package versions
    buf = StringIO()

    # show_versions() imports pyam-iamc, which in turn imports ixmp4, which removes all
    # handlers from the root logger (?!). Preserve the message-ix-models logging config.
    with preserve_log_handlers():
        ixmp.show_versions(buf)

    return buf.getvalue()


# FIXME Reduce complexity from 14 to ≤13
def strip_par_data(  # noqa: C901
    scenario: message_ix.Scenario,
    set_name: str,
    element: str,
    dry_run: bool = False,
    dump: Optional["MutableParameterData"] = None,
) -> int:
    """Remove `element` from `set_name` in scenario, optionally dumping to `dump`.

    Parameters
    ----------
    dry_run : bool, optional
        If :data:`True`, only show what would be done.
    dump : dict, optional
        If provided, stripped data are stored in this dictionary. Otherwise, they are
        discarded.

    Returns
    -------
    int
       Total number of rows removed across all parameters.

    See also
    --------
    add_par_data
    """
    par_list = scenario.par_list()
    no_data = set()  # Names of parameters with no data being stripped
    total = 0  # Total observations stripped

    if dump is None:
        pars: Iterable[str] = []  # Don't iterate over parameters unless dumping
    else:
        log.info(
            f"Remove data with {set_name}={element!r}"
            + (" (DRY RUN)" if dry_run else "")
        )
        # Iterate over parameters with ≥1 dimensions indexed by `set_name`
        pars = scenario.items(indexed_by=set_name, par_data=False)

    for par_name in pars:
        if par_name not in par_list:  # pragma: no cover
            log.warning(
                f"  MESSAGEix parameter {par_name!r} missing in Scenario {scenario.url}"
            )
            continue

        # Iterate over dimensions indexed by `set_name`
        for dim, _ in filter(
            lambda item: item[1] == set_name,
            zip(scenario.idx_names(par_name), scenario.idx_sets(par_name)),
        ):
            # Check for contents of par_name that include `element`
            par_data = scenario.par(par_name, filters={dim: element})
            N = len(par_data)
            total += N

            if N == 0:
                # No data; no need to do anything further
                no_data.add(par_name)
                continue
            elif dump is not None:
                dump[par_name] = pd.concat(
                    [dump.get(par_name, pd.DataFrame()), par_data]
                )

            log.info(f"  {N} rows in {par_name!r}")

            # Show some debug info
            for col in filter(
                lambda c: c != set_name and c in par_data.columns,
                ("commodity", "level", "technology"),
            ):
                log.info(f"  with {col}={sorted(par_data[col].unique())}")

            if dry_run:
                continue

            # Actually remove the data
            scenario.remove_par(par_name, key=par_data)

            # NB would prefer to do the following, but raises an exception:
            # scenario.remove_par(par_name, key={set_name: [value]})

    if not dry_run and dump is not None:
        log.info(f"  {total} rows total")
    if no_data:
        log.debug(f"No data removed from {len(no_data)} other parameters")

    if not dry_run:
        log.info(f"Remove {element!r} from set {set_name!r}")
        try:
            scenario.remove_set(set_name, element)
        except Exception as e:
            if "does not have an element" in str(e):
                log.info("  …not found")
            else:  # pragma: no cover
                raise

    return total
