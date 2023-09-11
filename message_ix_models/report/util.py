import logging
from typing import Dict, Iterable, Optional, Union

import pandas as pd
from dask.core import quote
from genno import Quantity
from genno.compat.pyam.util import collapse as genno_collapse
from genno.core.key import single_key
from iam_units import registry
from message_ix.reporting import Key, Reporter
from sdmx.model.v21 import Code

from message_ix_models.util import eval_anno

log = logging.getLogger(__name__)


#: Replacements used in :meth:`collapse`.
#: These are applied using :meth:`pandas.DataFrame.replace` with ``regex=True``; see the
#: documentation of that method.
#:
#: - Applied to whole strings along each dimension.
#: - These columns have :meth:`str.title` applied before these replacements.
REPLACE_DIMS: Dict[str, Dict[str, str]] = {
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


def as_quantity(info: Union[dict, float, str]) -> Quantity:
    """Convert values from a :class:`dict` to Quantity.

    .. todo:: move upstream, to :mod:`genno`.
    """
    if isinstance(info, str):
        q = registry.Quantity(info)
        return Quantity(q.magnitude, units=q.units)
    elif isinstance(info, float):
        return Quantity(info)
    elif isinstance(info, dict):
        data = info.copy()
        dim = data.pop("_dim")
        unit = data.pop("_unit")
        return Quantity(pd.Series(data).rename_axis(dim), units=unit)
    else:
        raise TypeError(type(info))


def collapse(df: pd.DataFrame, var=[]) -> pd.DataFrame:
    """Callback for the `collapse` argument to :meth:`~.Reporter.convert_pyam`.

    Replacements from :data:`REPLACE_DIMS` and :data:`REPLACE_VARS` are applied.
    The dimensions listed in the `var` arguments are automatically dropped from the
    returned :class:`pyam.IamDataFrame`. If ``var[0]`` contains the word "emissions",
    then :meth:`collapse_gwp_info` is invoked.

    Adapted from :func:`genno.compat.pyam.collapse`.

    Parameters
    ----------
    var : list of str, *optional*
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


def copy_ts(rep: Reporter, other: str, filters: Optional[dict]) -> Key:
    """Prepare `rep` to copy time series data from `other` to `scenario`.

    Parameters
    ----------
    other_url : str
       URL of the other scenario from which to copy time series data.
    filters : dict, *optional*
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
        label = eval_anno(code, "report")
        if label is not None:
            REPLACE_DIMS[dim][f"{code.id.title()}$"] = label
