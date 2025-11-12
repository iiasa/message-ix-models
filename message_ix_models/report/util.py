import logging
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from itertools import count
from typing import TYPE_CHECKING

import pandas as pd
from dask.core import quote
from genno import Key, Keys
from genno.compat.pyam.util import collapse as genno_collapse
from genno.core.key import single_key
from message_ix import Reporter
from sdmx.model.v21 import Code

if TYPE_CHECKING:
    from genno import Computer

log = logging.getLogger(__name__)


#: Replacements used in :meth:`collapse`.
#: These are applied using :meth:`pandas.DataFrame.replace` with ``regex=True``; see the
#: documentation of that method.
#:
#: - Applied to whole strings along each dimension.
#: - These columns have :meth:`str.title` applied before these replacements.
#:
#: See also :func:`add_replacements`.
REPLACE_DIMS: dict[str, dict[str, str]] = {
    "c": {
        # in land_out, for CH4 emissions from GLOBIOM
        "Agri_Ch4": "GLOBIOM|Emissions|CH4 Emissions Total",
    },
    "l": {
        # FIXME this is probably not generally applicable and should be removed
        "Final Energy": "Final Energy|Residential",
    },
    "t": dict(),
}

#: Replacements used in :func:`collapse` after 'variable' labels are constructed. These
#: are applied using :meth:`pandas.DataFrame.replace` with ``regex=True``; see the
#: documentation of that method. For documentation of regular expressions, see
#: https://docs.python.org/3/library/re.html and https://regex101.com.
#:
#: .. todo:: These may be particular or idiosyncratic to a single 'template'. The
#:    strings used to collapse multiple conceptual dimensions into the IAMC 'variable'
#:    dimension are known to vary across these templates, in ways that are sometimes not
#:    documented.
#:
#:    This setting is currently applied universally. To improve, specify a different
#:    mapping with the replacements needed for each individual template, and load the
#:    correct one when reporting scenarios to that template.
REPLACE_VARS = {
    # Secondary energy: remove duplicate "Solids"
    r"(Secondary Energy\|Solids)\|Solids": r"\1",
    # CH4 emissions from MESSAGE technologies
    r"(Emissions\|CH4)\|Fugitive": r"\1|Energy|Supply|Fugitive",
    # CH4 emissions from GLOBIOM
    r"(Emissions\|CH4)\|((Gases|Liquids|Solids|Elec|Heat)(.*))": (
        r"\1|Energy|Supply|\3|Fugitive\4"
    ),
    r"^(land_out CH4.*\|)Awm": r"\1Manure Management",
    r"^land_out CH4\|Emissions\|Ch4\|Land Use\|Agriculture\|": (
        "Emissions|CH4|AFOLU|Agriculture|Livestock|"
    ),
    # Strip internal prefix
    r"^land_out CH4\|": "",
    # Prices
    r"Residential\|(Biomass|Coal)": r"Residential|Solids|\1",
    r"Residential\|Gas": "Residential|Gases|Natural Gas",
    r"Import Energy\|Lng": "Primary Energy|Gas",
    r"Import Energy\|Coal": "Primary Energy|Coal",
    r"Import Energy\|Oil": "Primary Energy|Oil",
    r"Import Energy\|(Liquids\|(Biomass|Oil))": r"Secondary Energy|\1",
    r"Import Energy\|Lh2": "Secondary Energy|Hydrogen",
}


@dataclass
class IAMCConversion:
    """Description of a conversion to IAMC data structure.

    Instance fields contain information needed to prepare the conversion.
    :meth:`add_tasks` adds tasks to a :class:`.Computer` to perform it.
    """

    #: Key for data to be converted.
    base: Key

    #: Parts of the variable expression. This is passed as the :py:`var` argument to
    #: :func:`collapse`.
    var_parts: list[str]

    #: Exact unit string for output.
    unit: str

    #: Explicit dimension renaming.
    rename: Mapping[str, str] = field(default_factory=dict)

    #: Dimension(s) to sum over.
    sums: list[str] = field(default_factory=list)

    #: If :any:`True`, ensure data is present for ``R##_GLB``.
    GLB_zeros: bool = False

    def __post_init__(self) -> None:
        # Ensure base is a Key
        self.base = Key(self.base)

    def add_tasks(self, c: "Computer") -> None:
        """Add tasks to convert :attr:`base` to IAMC structure.

        The tasks include, in order:

        1. If :attr:`GLB_zeroes` is :any:`True`:

           - Create a quantity with the same shape as :attr:`base`, filled with all
             zeros (:func:`.zeros_like`) and a single coord like ``R##_GLB`` for the
             :math:`n` dimension (:func:`.node_glb`).
           - Add this to :attr:`base`.

           These steps ensure that values for ``R##_GLB`` will appear in the
           IAMC-structured result.

        2. Convert to the given :attr:`units` (:func:`~genno.operator.convert_units`).
           The :attr:`base` quantity **must** have dimensionally compatible units.

        Steps (3) to (6) are repeated for (at least) an empty string (:py:`""`) and for
        any expressions like :py:`"x-y-z"` in :attr:`sums`.

        3. Subtract the given dimension(s) (if any) from the dimensions of :attr:`base`.
           For example, if :attr:`base` is ``<foo:x-y-z>`` and :attr:`sums` includes
           :py:`"x-z"`, this gives a reference to ``<foo:y>``, which is the base
           quantity summed over the :math:`(x, z)` dimensions.

        4. Reduce the :attr:`var_parts` in the same way. For example, if
           :attr:`var_parts` is :py:`["Variable prefix", "z", "x", "y", "Foo"]`, the
           above sum reduces this to :py:`["Variable prefix", "y", "Foo"]`.

        5. Call :func:`genno.compat.pyam.iamc` to add further tasks to convert the
           quantity from (3) to IAMC structure. :func:`callback` in this module is used
           to help format the individual dimension labels and collapsed ‘variable’
           labels.

           This step results in keys like ``base 0::iamc``, ``base 1::iamc``, etc. added
           to `rep`.

        6. Append the key from (5) to the task at :data:`.report.key.all_iamc`. This
           ensures that the converted data is concatenated with all other
           IAMC-structured data.
        """
        from genno.compat.pyam import iamc as handle_iamc

        from .key import all_iamc, coords

        k = Keys(base=self.base, glb=self.base + "glb")

        if self.GLB_zeros:
            # Quantity of zeros in the same shape as self.base, without an 'n' dimension
            c.add(k.glb[0], "zeros_like", self.base, drop=["n"])

            # Add the 'n' dimension
            c.add(k.glb[1], "expand_dims", k.glb[0], coords.n_glb)

            # Add zeros to base data & update the base key for next steps
            c.add(k.base[0], "add", self.base, k.glb[1])
        else:
            # Simple alias
            c.add(k.base[0], k.base)

        # Convert to target units
        c.add(k.base[1], "convert_units", k.base[0], units=self.unit)

        # Common keyword arguments for genno.compat.pyam.iamc
        args: dict = dict(rename=self.rename, unit=self.unit)

        # Populate rename
        for d in set(self.base.dims) - set(self.var_parts):
            if d in {"n", "nd", "nl", "no"}:
                args["rename"].setdefault(d, "region")
            elif d in {"y", "ya", "yv"}:
                args["rename"].setdefault(d, "year")

        # Check rename arg
        assert dict(region=1, year=1) == Counter(args["rename"].values()), (
            f"Expected 1 region and 1 year dimension; got {args['rename']}"
        )

        # Identify a `start` value that does not duplicate existing keys
        label = self.var_parts[0]
        for start in count():
            if f"{label} {start}::iamc" not in c:
                break

        # Iterate over dimensions to be partly summed
        # TODO move some or all of this logic upstream
        keys = []
        for i, dims in enumerate(
            map(lambda s: s.split("-"), [""] + self.sums), start=start
        ):
            k.sum = k.base[1].drop(*dims)
            if k.sum != k.base[1]:
                c.add(k.sum, "sum", k.base[1], dimensions=dims)

            # Parts (string literals or dimension IDs) to concatenate into ‘variable’.
            # Exclude any summed dimensions from the expression.
            var_parts = [v for v in self.var_parts if v not in dims]

            # Invoke genno's built-in handler to add more tasks:
            # - Base key: the partial sum of k.base over any `dims`.
            # - "variable" argument is used only to construct keys; the resulting IAMC-
            #   structured data is available at `{variable}::iamc`.
            # - Collapse using `var_parts` and the collapse() function in this module.
            handle_iamc(
                c,
                args
                | dict(
                    base=k.sum,
                    variable=f"{label} {i}",
                    collapse=dict(callback=collapse, var=var_parts),
                ),
            )
            keys.append(f"{label} {i}::iamc")

        # Concatenate each of `keys` into all::iamc
        c.graph[all_iamc] += tuple(keys)


def collapse(df: pd.DataFrame, var=[]) -> pd.DataFrame:
    """Callback for the `collapse` argument to :meth:`~.Reporter.convert_pyam`.

    Replacements from :data:`REPLACE_DIMS` and :data:`REPLACE_VARS` are applied.
    The dimensions listed in the `var` argument are automatically dropped from the
    returned :class:`pyam.IamDataFrame`. If :py:`var[0]` contains the word "emissions",
    then :func:`collapse_gwp_info` is invoked.

    Adapted from :func:`genno.compat.pyam.collapse`.

    Parameters
    ----------
    var : list of str, optional
        Strings or dimensions to concatenate to a 'variable' string. The first of these
        usually a :class:`str` used to populate the column; others may be fixed strings
        or the IDs of dimensions in the input data. The components are joined using the
        pipe ('|') character.

    See also
    --------
    REPLACE_DIMS
    REPLACE_VARS
    collapse_gwp_info
    test_collapse
    """
    # Convert some dimension labels to title-case strings
    for dim in filter(lambda d: d in df.columns, "clt"):
        df[dim] = df[dim].astype(str).str.title()

    if "l" in df.columns:
        # Level: to title case, add the word 'energy'
        df["l"] = df["l"] + " Energy"

    if len(var) and "emissions" in var[0].lower():
        log.info(f"Collapse GWP info for {var[0]}")
        df, var = collapse_gwp_info(df, var)

    # - Apply replacements to individual dimensions.
    # - Use the genno built-in to assemble the variable column.
    # - Apply replacements to assembled columns.
    return (
        df.replace(REPLACE_DIMS, regex=True)
        .pipe(genno_collapse, columns=dict(variable=var))
        .replace(dict(variable=REPLACE_VARS), regex=True)
    )


def collapse_gwp_info(df, var):
    """:meth:`collapse` helper for emissions data with GWP dimensions.

    The dimensions 'e equivalent', and 'gwp metric' dimensions are combined
    with the 'e' dimension, using a format like::

        '{e} ({e equivalent}-equivalent, {GWP metric} metric)'

    For example::

        'SF6 (CO2-equivalent, AR5 metric)'
    """
    # Check that *df* contains the necessary columns
    cols = ["e equivalent", "gwp metric"]
    missing = set(["e"] + cols) - set(df.columns)
    if len(missing):
        log.warning(f"…skip; {missing} not in columns {list(df.columns)}")
        return df, var

    # Format the column with original emissions species
    df["e"] = (
        df["e"]
        + " ("
        + df["e equivalent"]
        + "-equivalent, "
        + df["gwp metric"]
        + " metric)"
    )

    # Remove columns from further processing
    [var.remove(c) for c in cols]
    return df.drop(cols, axis=1), var


def copy_ts(rep: Reporter, other: str, filters: dict | None) -> Key:
    """Prepare `rep` to copy time series data from `other` to `scenario`.

    Parameters
    ----------
    other_url : str
       URL of the other scenario from which to copy time series data.
    filters : dict, optional
       Filters; passed via :func:`.store_ts` to :meth:`ixmp.TimeSeries.timeseries`.

    Returns
    -------
    str
        Key for the copy operation.
    """

    # A unique ID for this copy operation, to avoid collision if copy_ts() used multiple
    # times
    _id = f"{hash(other + repr(filters)):x}"

    k1 = rep.add("from_url", f"scenario {_id}", quote(other))
    k2 = rep.add("get_ts", f"ts data {_id}", k1, filters)
    return single_key(rep.add("store_ts", f"copy ts {_id}", "scenario", k2))


def add_replacements(dim: str, codes: Iterable[Code]) -> None:
    """Update :data:`REPLACE_DIMS` for dimension `dim` with values from `codes`.

    For every code in `codes` that has an annotation with the ID ``report``, the code
    ID is mapped to the value of the annotation. For example, the following in one of
    the :doc:`/pkg-data/codelists`:

    .. code-block:: yaml

       foo:
         report: fOO

       bar:
         report: Baz

       qux: {}  # No "report" annotation → no mapping

    …results in entries :py:`{"foo": "fOO", "bar": "Baz"}` added to :data:`REPLACE_DIMS`
    and used by :func:`collapse`.
    """
    for code in codes:
        try:
            label = str(code.get_annotation(id="report").text)
        except KeyError:
            pass
        else:
            REPLACE_DIMS[dim][f"{code.id.title()}$"] = label
