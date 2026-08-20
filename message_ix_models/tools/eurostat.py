"""Handle data from Eurostat.

Eurostat administers, with the IEA and UNECE, the five joint annual questionnaires
through which countries report their energy statistics, and publishes the resulting
balances for the EU member states together with candidate countries and others that
file. A country's primary national return goes to one collector and not the other, so
this source and :mod:`.tools.unsd` are complementary rather than alternative: between
them they cover the Western Balkans, Moldova and Ukraine as well as Central Asia,
without recourse to the proprietary :class:`.IEA_EWEB`.

The data are served without registration from the Eurostat dissemination API. Reuse is
permitted for commercial and non-commercial purposes with acknowledgement.

"ESTAT" throughout this module is Eurostat's agency ID in SDMX, and is the ID under
which :mod:`sdmx` knows the web service.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import genno

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.util import cached
from message_ix_models.util.sdmx import fetch_data

if TYPE_CHECKING:
    import pandas as pd
    from genno.types import AnyQuantity

log = logging.getLogger(__name__)

#: ID of the dataflow retrieved by :func:`fetch`: complete energy balances.
DATAFLOW = "NRG_BAL_C"

#: Order in which :data:`DATAFLOW` declares its dimensions, and thus the order of the
#: parts of a positional data key. Note that the flow dimension precedes the product
#: dimension, and that ``geo`` comes last; both are the reverse of :mod:`.tools.unsd`.
#: See :func:`.util.sdmx.fetch_data`.
KEY_ORDER = ("freq", "nrg_bal", "siec", "unit", "geo")

#: Mapping from the dimension IDs of :data:`DATAFLOW` to the dimensions of the data
#: returned by :func:`load_data` and :class:`ESTAT_ENERGY_BALANCE`.
DIMS = {
    "geo": "n",
    "TIME_PERIOD": "y",
    "siec": "product",
    "nrg_bal": "flow",
}

#: Label selected on the ``unit`` dimension: terajoules. :data:`DATAFLOW` also carries
#: gigawatt-hours and tonnes of oil equivalent, so a query that does not constrain the
#: unit returns a mixture that cannot be summed.
UNIT = "TJ"

#: Labels appearing on the ``geo`` dimension that are not ISO 3166-1 alpha-2 codes, with
#: the alpha-3 code each maps to. Eurostat retains a few pre-ISO or politically-neutral
#: labels; ``XK`` has no ISO 3166-1 entry at all.
GEO = {
    "EL": "GRC",  # Greece; ISO 3166-1 alpha-2 is GR
    "UK": "GBR",  # United Kingdom; ISO 3166-1 alpha-2 is GB
    "XK": "XKX",  # Kosovo; user-assigned, following World Bank practice
}


def make_key(
    *, geo: tuple[str, ...], product: tuple[str, ...], flow: tuple[str, ...]
) -> str:
    """Return a positional data key for :data:`DATAFLOW`.

    Dimensions not named here are left empty, meaning unfiltered; the unit dimension is
    always constrained to :data:`UNIT`, and the frequency to annual.

    The labels selected on each dimension are sorted. Their order carries no meaning to
    the service, so sorting makes the key — and thus the :func:`.cached` key of
    :func:`fetch` — independent of the order in which the caller happened to list them.
    """
    labels = {
        "freq": ("A",),
        "nrg_bal": flow,
        "siec": product,
        "unit": (UNIT,),
        "geo": geo,
    }
    return ".".join("+".join(sorted(labels.get(d, ()))) for d in KEY_ORDER)


@cached
def fetch(
    *,
    geo: tuple[str, ...],
    product: tuple[str, ...],
    flow: tuple[str, ...],
    start: int,
    end: int,
) -> "pd.Series":
    """Retrieve part of :data:`DATAFLOW`.

    The result has the dimension IDs of the dataflow on its index, not yet renamed.
    Decorated with :func:`.cached`, so repeated calls with identical arguments do not
    re-query the web service.

    The cache has no expiry, so a revised national submission is not picked up until the
    entry is invalidated: either set :attr:`.Config.cache_skip` to force the query, or
    delete the cached file from :attr:`.Config.cache_path`.

    Parameters
    ----------
    geo
        Reference areas, as Eurostat ``geo`` labels, for instance :py:`("RS",)` for
        Serbia.
    product, flow
        Labels to select on the ``siec`` and ``nrg_bal`` dimensions. Empty selects all
        labels, which for this dataflow is a large query.
    start, end
        First and last period, inclusive.
    """
    return fetch_data(
        "ESTAT",
        DATAFLOW,
        make_key(geo=geo, product=product, flow=flow),
        startPeriod=str(start),
        endPeriod=str(end),
    )


def load_data(
    *,
    geo: "tuple[str, ...] | list[str]",
    product: "tuple[str, ...] | list[str]" = (),
    flow: "tuple[str, ...] | list[str]" = (),
    start: int = 2000,
    end: int = 2022,
) -> "pd.DataFrame":
    """Return part of :data:`DATAFLOW` as a :class:`pandas.DataFrame`.

    This is the entry point for consumers that process the data with :mod:`pandas` and
    have no :class:`genno.Computer` to which tasks could be added; those that do build
    one should prefer :class:`ESTAT_ENERGY_BALANCE`.

    Columns are ``n`` (ISO 3166-1 alpha-3), ``y``, ``product``, ``flow`` and ``value``,
    with values in TJ. Observations absent from the source are absent from the result:
    they are **not** filled with zero, because a zero and an unreported value are
    different facts and no consumer can distinguish them after the fact. Coverage begins
    well after 2000 for several non-member countries.

    See :func:`fetch` for the parameters.

    Raises
    ------
    ValueError
        if the data contain a reference area with no ISO 3166-1 alpha-3 code, for
        instance one of the aggregates such as ``EU27_2020``.
    RuntimeError
        if the data have a dimension named in neither :data:`DIMS` nor
        :data:`KEY_ORDER`.
    """
    from message_ix_models.util.pycountry import iso_3166_alpha_3

    series = fetch(
        geo=tuple(geo),
        product=tuple(product),
        flow=tuple(flow),
        start=start,
        end=end,
    )

    # Guard against the dataflow gaining a dimension. The column selection below would
    # drop it silently, collapsing observations that differ only on that dimension onto
    # identical index entries.
    known = set(DIMS) | set(KEY_ORDER)
    if extra := sorted(set(series.index.names) - known):
        raise RuntimeError(
            f"Unexpected dimension(s) {extra} in Eurostat data; expected only "
            f"{sorted(known)}"
        )

    df = series.rename("value").reset_index()
    df = df.rename(columns=DIMS)[["n", "y", "product", "flow", "value"]]

    # Map reference areas to alpha-3, handling the labels in GEO first.
    # iso_3166_alpha_3() returns None for a label it does not recognize, which would
    # make those observations vanish rather than fail, so check before mapping. Compare
    # .cepii.get_mapping(), which passes on_missing="raise" to MappingAdapter for the
    # same reason.
    labels = {n: GEO.get(n) or iso_3166_alpha_3(n) for n in df["n"].unique()}
    if missing := sorted(k for k, v in labels.items() if v is None):
        raise ValueError(
            f"No ISO 3166-1 alpha-3 code for Eurostat reference area {missing}"
        )

    return df.assign(n=df["n"].map(labels)).astype({"y": int})


@register_source
class ESTAT_ENERGY_BALANCE(ExoDataSource):
    """Complete energy balances published by Eurostat.

    The data have the same dimensionality and units as :class:`.IEA_EWEB` —
    :py:`"energy:n-y-product-flow"` in TJ — under the distinct key tag :py:`":estat"`,
    so a consumer written against the IEA balance can take this source without change on
    the |n| and |y| dimensions. The labels on ``product`` and ``flow`` are Eurostat's
    own ``siec`` and ``nrg_bal`` codes, **not** IEA codes.

    Reference areas are given as Eurostat ``geo`` labels.

    Aggregation on |n| and interpolation on |y| are both **off** by default, unlike
    :class:`.BaseOptions`. A balance records what each country reported for each year it
    reported; interpolating onto the model periods would fill the years it did not,
    making “not reported” and “reported as zero” indistinguishable, and would discard
    every observed period besides. Compare :meth:`.IEA_EWEB.transform`, which suppresses
    both for the same reason. A caller that wants either **may** ask for it explicitly.

    Example
    -------
    >>> keys = ESTAT_ENERGY_BALANCE.add_tasks(
    ...     computer, context=context, geo=("RS",), start=2000, end=2022
    ... )
    >>> result = computer.get(keys[0])
    """

    key = genno.Key("energy:n-y-product-flow:estat")

    @dataclass
    class Options(BaseOptions):
        #: By default, do not aggregate.
        aggregate: bool = False
        #: By default, do not interpolate.
        interpolate: bool = False

        #: Reference areas to retrieve, as Eurostat ``geo`` labels.
        geo: tuple[str, ...] = ()

        #: Select only these labels on the ``siec`` dimension.
        product: tuple[str, ...] = ()

        #: Select only these labels on the ``nrg_bal`` dimension.
        flow: tuple[str, ...] = ()

        #: First and last period, inclusive.
        start: int = 2000
        end: int = 2022

        def __post_init__(self) -> None:
            if not self.geo:
                raise ValueError("geo=(); at least one reference area is required")

    options: Options

    def __init__(self, *args, **kwargs) -> None:
        self.options = self.Options.from_args(self, *args, **kwargs)
        super().__init__()

    def get(self) -> "AnyQuantity":
        """Retrieve the data and return it with :mod:`message_ix_models` dimensions."""
        opt = self.options
        df = load_data(
            geo=opt.geo,
            product=opt.product,
            flow=opt.flow,
            start=opt.start,
            end=opt.end,
        )
        return genno.Quantity(
            df.set_index(["n", "y", "product", "flow"])["value"], units="TJ"
        )
