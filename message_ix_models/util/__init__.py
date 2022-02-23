import logging
from collections import defaultdict
from copy import copy
from typing import Dict, List, Mapping, Optional, Sequence, Union

import message_ix
import pandas as pd
from message_ix.models import MESSAGE_ITEMS
from sdmx.model import AnnotableArtefact, Annotation, Code

from ._convert_units import convert_units, series_of_pint_quantity
from .cache import cached
from .common import (
    MESSAGE_DATA_PATH,
    MESSAGE_MODELS_PATH,
    load_package_data,
    load_private_data,
    local_data_path,
    package_data_path,
    private_data_path,
)
from .node import adapt_R11_R14, identify_nodes
from .scenarioinfo import ScenarioInfo

__all__ = [
    "MESSAGE_DATA_PATH",
    "MESSAGE_MODELS_PATH",
    "adapt_R11_R14",
    "as_codes",
    "cached",
    "check_support",
    "convert_units",
    "identify_nodes",
    "load_package_data",
    "load_private_data",
    "local_data_path",
    "maybe_query",
    "package_data_path",
    "private_data_path",
    "series_of_pint_quantity",
]

log = logging.getLogger(__name__)


def add_par_data(
    scenario: message_ix.Scenario,
    data: Mapping[str, pd.DataFrame],
    dry_run: bool = False,
):
    """Add `data` to `scenario`.

    Parameters
    ----------
    data
        Dict with keys that are parameter names, and values are pd.DataFrame or other
        arguments
    dry_run : optional
        Only show what would be done.

    See also
    --------
    strip_par_data
    """
    total = 0

    for par_name, values in data.items():
        N = values.shape[0]
        log.info(f"{N} rows in {repr(par_name)}")
        log.debug(values.to_string(max_rows=5))

        total += N

        if dry_run:
            continue

        scenario.add_par(par_name, values)

    return total


def as_codes(data: Union[List[str], Dict[str, Union[Dict, Code]]]) -> List[Code]:
    """Convert *data* to a :class:`list` of :class:`.Code` objects.

    Various inputs are accepted:

    - :class:`list` of :class:`str`.
    - :class:`dict`, in which keys are :attr:`.Code.id` and values are further
      :class:`dict` with keys matching other :class:`.Code` attributes.
    """
    # Assemble results as a dictionary
    result: Dict[str, Code] = {}

    if isinstance(data, list):
        # FIXME typing ignored temporarily for PR#9
        data = dict(zip(data, data))  # type: ignore [arg-type]
    elif not isinstance(data, Mapping):
        raise TypeError(data)

    for id, info in data.items():
        # Pass through Code; convert other types to dict()
        if isinstance(info, Code):
            result[info.id] = info
            continue
        elif isinstance(info, str):
            info = dict(name=info)
        elif isinstance(info, Mapping):
            info = copy(info)
        else:
            raise TypeError(info)

        # Create a Code object
        code = Code(
            id=str(id),
            name=info.pop("name", str(id).title()),
        )

        # Store the description, if any
        try:
            code.description = info.pop("description")
        except KeyError:
            pass

        # Associate with a parent
        try:
            parent_id = info.pop("parent")
        except KeyError:
            pass  # No parent
        else:
            result[parent_id].append_child(code)

        # Associate with any children
        for id in info.pop("child", []):
            try:
                code.append_child(result[id])
            except KeyError:
                pass  # Not parsed yet

        # Convert other dictionary (key, value) pairs to annotations
        for id, value in info.items():
            code.annotations.append(
                Annotation(id=id, text=value if isinstance(value, str) else repr(value))
            )

        result[code.id] = code

    return list(result.values())


def aggregate_codes(df: pd.DataFrame, dim: str, codes):  # pragma: no cover
    """Aggregate `df` along dimension `dim` according to `codes`."""
    raise NotImplementedError

    # Construct an inverse mapping
    mapping = {}
    for code in codes:
        mapping.update({child.id: code.id for child in code.child})

    for key, group_series in df.groupby(dim):
        print(key, group_series.replace({dim: mapping}))


def broadcast(df, **kwargs):
    """Fill missing data in `df` by broadcasting.

    Arguments
    ---------
    kwargs
        Keys are dimensions. Values are labels along that dimension to fill.
    """
    for dim, levels in kwargs.items():
        # Checks
        assert df[dim].isna().all(), f"Dimension {dim} was not empty\n\n{df.head()}"
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
            pd.concat([df] * len(levels), keys=levels, names=[dim])
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
    """
    return lambda df: df[column_name]


def eval_anno(obj: AnnotableArtefact, id: str):
    """Retrieve the annotation `id` from `obj`, run :func:`eval` on its contents.

    This can be used for unpacking Python values (e.g. :class:`dict`) stored as an
    annotation on a :class:`~sdmx.model.Code`.

    Returns :obj:`None` if no attribute exists with the given `id`.
    """
    try:
        value = str(obj.get_annotation(id=id).text)
    except KeyError:
        # No such attribute
        return None

    try:
        return eval(value)
    except Exception:
        # Something that can't be eval()'d, e.g. a string
        return value


def ffill(
    df: pd.DataFrame, dim: str, values: Sequence[Union[str, Code]], expr: str = None
) -> pd.DataFrame:
    """Forward-fill `df` on `dim` to cover `values`.

    Parameters
    ----------
    df : .DataFrame
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


def iter_parameters(set_name):
    """Iterate over MESSAGEix parameters with *set_name* as a dimension.

    Parameters
    ----------
    set_name : str
        Name of a set.

    Yields
    ------
    str
        Names of parameters that have `set_name` indexing ≥1 dimension.
    """
    # TODO move upstream. See iiasa/ixmp#402 and iiasa/message_ix#444
    for name, info in MESSAGE_ITEMS.items():
        if info["ix_type"] == "par" and set_name in info["idx_sets"]:
            yield name


def make_io(src, dest, efficiency, on="input", **kwargs):
    """Return input and output data frames for a 1-to-1 technology.

    Parameters
    ----------
    src : tuple (str, str, str)
        Input commodity, level, unit.
    dest : tuple (str, str, str)
        Output commodity, level, unit.
    efficiency : float
        Conversion efficiency.
    on : 'input' or 'output'
        If 'input', `efficiency` applies to the input, and the output, thus the activity
        level of the technology, is in dest[2] units. If 'output', the opposite.
    kwargs
        Passed to :func:`make_df`.

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


def make_matched_dfs(base, **par_value):
    """Return data frames derived from *base* for multiple parameters.

    *par_values* maps from parameter names (e.g. 'fix_cost') to values.
    make_matched_dfs returns a :class:`dict` of :class:`pandas.DataFrame`, one for each
    parameter in *par_value*. The contents of *base* are used to populate the columns
    of each data frame, and the values of *par_value* overwrite the 'value' column.
    Duplicates—which occur when the target parameter has fewer dimensions than
    *base*—are dropped.

    Examples
    --------
    >>> input = make_df('input', ...)
    >>> cf_tl = make_matched_dfs(
    >>>     input,
    >>>     capacity_factor=1,
    >>>     technical_lifetime=1,
    >>> )
    """
    data = {col: v for col, v in base.iteritems() if col != "value"}
    return {
        par: message_ix.make_df(par, **data, value=value)
        .drop_duplicates()
        .reset_index(drop=True)
        for par, value in par_value.items()
    }


def make_source_tech(
    info: Union[message_ix.Scenario, ScenarioInfo], common, **values
) -> Dict[str, pd.DataFrame]:
    """Return parameter data for a ‘source’ technology.

    The technology has no inputs; its output commodity and/or level are determined by
    `common`; either single values, or :obj:`None` if the result will be
    :meth:`~DataFrame.pipe`'d through :func:`broadcast`.

    Parameters
    ----------
    info : .Scenario or .ScenarioInfo
    common : dict
        Passed to :func:`make_df`.
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
        .pipe(broadcast, node_loc=info.N[1:])
        .pipe(same_node)
    )

    # Add data for other parameters
    result.update(make_matched_dfs(base=result["output"], **values))

    return result


def maybe_query(series: pd.Series, query: Optional[str]) -> pd.Series:
    """Apply :meth:`pandas.Series.query` if the `query` arg is not :obj:`None`.

    :meth:`~pandas.Series.query` is not chainable (`pandas-dev/pandas#37941
    <https://github.com/pandas-dev/pandas/issues/37941>`_). Use this function with
    :func:`pandas.Series.pipe`, passing an argument that may be :obj:`None`, to have a
    chainable query operation that can be a no-op.
    """
    # Convert Series to DataFrame, query(), then retrieve the single column
    return series if query is None else series.to_frame().query(query)[0]


def merge_data(base, *others):
    """Merge dictionaries of DataFrames together into `base`."""
    for other in others:
        for par, df in other.items():
            base[par] = pd.concat([base.get(par, None), df])


def same_node(df):
    """Fill 'node_origin'/'node_dest' in `df` from 'node_loc'."""
    cols = list(set(df.columns) & {"node_origin", "node_dest"})
    return df.assign(**{c: copy_column("node_loc") for c in cols})


def strip_par_data(
    scenario, set_name, value, dry_run=False, dump: Dict[str, pd.DataFrame] = None
):
    """Remove data from parameters of *scenario* where *value* in *set_name*.

    Returns
    -------
    Total number of rows removed across all parameters.

    See also
    --------
    add_par_data
    """
    par_list = scenario.par_list()
    no_data = []
    total = 0

    # Iterate over parameters with ≥1 dimensions indexed by `set_name`
    for par_name in iter_parameters(set_name):
        if par_name not in par_list:  # pragma: no cover
            log.warning(
                f"MESSAGEix parameter {repr(par_name)} missing in Scenario "
                f"{scenario.model}/{scenario.scenario}"
            )
            continue

        # Iterate over dimensions indexed by `set_name`
        for dim, _ in filter(
            lambda item: item[1] == set_name,
            zip(scenario.idx_names(par_name), scenario.idx_sets(par_name)),
        ):
            # Check for contents of par_name that include *value*
            par_data = scenario.par(par_name, filters={dim: value})
            N = len(par_data)

            if N == 0:
                # No data; no need to do anything further
                no_data.append(par_name)
                continue
            elif dump is not None:
                dump[par_name] = pd.concat(
                    [dump.get(par_name, pd.DataFrame()), par_data]
                )

            log.info(f"Remove {N} rows in {repr(par_name)}")

            # Show some debug info
            for col in "commodity level technology".split():
                if col == set_name or col not in par_data.columns:
                    continue

                log.info(f"  with {col}={sorted(par_data[col].unique())}")

            if not dry_run:
                # Actually remove the data
                scenario.remove_par(par_name, key=par_data)

                # NB would prefer to do the following, but raises an exception:
                # scenario.remove_par(par_name, key={set_name: [value]})

            total += N

    level = logging.INFO if total > 0 else logging.DEBUG
    log.log(level, f"{total} rows removed.")
    log.debug(f"No data removed from {len(no_data)} other parameters")

    return total
