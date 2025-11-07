import logging
from collections.abc import Iterable
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

from message_ix_models.util import nodes_ex_world

if TYPE_CHECKING:
    from genno import Computer

log = logging.getLogger(__name__)


#: Replacements used in :meth:`collapse`.
#: These are applied using :meth:`pandas.DataFrame.replace` with ``regex=True``; see the
#: documentation of that method.
#:
#: - Applied to whole strings along each dimension.
#: - These columns have :meth:`str.title` applied before these replacements.
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

#: Replacements used in :meth:`collapse` after the 'variable' column is assembled.
#: These are applied using :meth:`pandas.DataFrame.replace` with ``regex=True``; see
#: the documentation of that method. For documentation of regular expressions, see
#: https://docs.python.org/3/library/re.html and https://regex101.com.
#:
#: .. todo:: These may be particular or idiosyncratic to a single "template". The
#:    strings used to collapse multiple conceptual dimensions into the IAMC "variable"
#:    column are known to vary in poorly-documented ways across these templates.
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


def n_glb(nodes: list[str]) -> dict[str, list[str]]:
    """Return :py:`{"n": ["R##_GLB"]}` based on the existing `nodes`."""
    return {"n": ["".join(str(nodes_ex_world(nodes)[0]).partition("_")[:2] + ("GLB",))]}


_RENAME = {"n": "region", "nl": "region", "y": "year", "ya": "year", "yv": "year"}


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

    #: Dimension(s) to sum over.
    sums: list[str] = field(default_factory=list)

    #: If :any:`True`, ensure data is present for "R11_GLB".
    GLB_zeros: bool = False

    def __post_init__(self) -> None:
        # Ensure base is a Key
        self.base = Key(self.base)

    def add_tasks(self, c: "Computer") -> None:
        from genno.compat.pyam import iamc as handle_iamc

        from .key import all_iamc

        k = Keys(base=self.base, glb=self.base + "glb")

        if self.GLB_zeros:
            # Quantity of zeros like self.base
            c.add(k.glb[0], "zeros_like", self.base, drop=["n"])

            # Add a key that gives an expand_dims arg for the next task
            # TODO Move to add_structure()
            c.add("n::glb", n_glb, "n")

            # Add the 'n' dimension
            c.add(k.glb[1], "expand_dims", k.glb[0], "n::glb")

            # Add zeros to base data & update the base key for next steps
            k.base += "glb"
            c.add(k.base, "add", self.base, k.glb[1])

        # Common keyword arguments for genno.compat.pyam.iamc
        args: dict = dict(rename=_RENAME, unit=self.unit)

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
            # Parts (string literals or dimension IDs) to concatenate into ‘variable’.
            # Exclude any summed dimensions from the expression.
            var_parts = [v for v in self.var_parts if v not in dims]

            # Invoke genno's built-in handler
            # - Base key: the partial sum of k.base over any `dims`.
            # - "variable" argument is used only to construct keys; the resulting IAMC-
            #   structured data is available at `{variable}::iamc`.
            # - Collapse using `var_parts` and the collapse() function in this module.
            handle_iamc(
                c,
                args
                | dict(
                    base=k.base.drop(*dims),
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
    The dimensions listed in the `var` arguments are automatically dropped from the
    returned :class:`pyam.IamDataFrame`. If ``var[0]`` contains the word "emissions",
    then :meth:`collapse_gwp_info` is invoked.

    Adapted from :func:`genno.compat.pyam.collapse`.

    Parameters
    ----------
    var : list of str, optional
        Strings or dimensions to concatenate to the 'Variable' column. The first of
        these is usually a string value used to populate the column. These are joined
        using the pipe ('|') character.

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
    """Update :data:`REPLACE_DIMS` for dimension `dim` with values from `codes`."""
    for code in codes:
        try:
            label = str(code.get_annotation(id="report").text)
        except KeyError:
            pass
        else:
            REPLACE_DIMS[dim][f"{code.id.title()}$"] = label
