"""Handle data from the UN Statistics Division (UNSD).

UNSD compiles national energy balances for the countries that the five joint annual
questionnaires do not reach. Those questionnaires are administered by Eurostat, the IEA
and UNECE, and a country's primary national return goes to one collector and not the
other, so this source and :mod:`.tools.eurostat` are complementary rather than
alternative: between them they cover Central Asia, the Western Balkans, North Africa and
the Middle East without recourse to the proprietary :class:`.IEA_EWEB`.

The data are served without registration from https://unstats.un.org/unsd/energystats/api/
UNdata's terms of use permit them to be “copied freely, duplicated and further
distributed provided that UNdata is cited as the reference”.

.. _unsd-granularity:

.. note:: On granularity, for anyone arriving here from a calibration.

   :class:`UNSD_ENERGY_BALANCE` carries 9 fuel groups on ``product``. Calibration
   mappings written against the IEA balance reference on the order of 69 distinct
   PRODUCT codes, so this source cannot identify individual technologies and **must
   not** be substituted into such a mapping. The commodity-level dataflow
   ``DF_UNDATA_ENERGY`` carries 75 commodities and is the appropriate source there; it
   is deliberately not implemented here, because no consumer needs it yet.
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

#: ID of the dataflow retrieved by :func:`fetch`.
DATAFLOW = "DF_UNData_EnergyBalance"

#: Order in which :data:`DATAFLOW` declares its dimensions, and thus the order of the
#: parts of a positional data key. See :func:`.util.sdmx.fetch_data`.
KEY_ORDER = ("REF_AREA", "COMMODITY", "TRANSACTION", "UNIT")

#: Mapping from the dimension IDs of :data:`DATAFLOW` to the dimensions of the data
#: returned by :func:`load_data` and :class:`UNSD_ENERGY_BALANCE`.
DIMS = {
    "REF_AREA": "n",
    "TIME_PERIOD": "y",
    "COMMODITY": "product",
    "TRANSACTION": "flow",
}

#: Label selected on the ``UNIT`` dimension: terajoules. :data:`DATAFLOW` also carries
#: cubic metres, kilowatts, kilowatt-hours and metric tons, so a query that does not
#: constrain the unit returns a mixture that cannot be summed.
UNIT = "HSO"


def make_key(
    *, geo: tuple[str, ...], product: tuple[str, ...], flow: tuple[str, ...]
) -> str:
    """Return a positional data key for :data:`DATAFLOW`.

    Dimensions not named here are left empty, meaning unfiltered; the unit dimension is
    always constrained to :data:`UNIT`.

    The labels selected on each dimension are sorted. Their order carries no meaning to
    the service, so sorting makes the key — and thus the :func:`.cached` key of
    :func:`fetch` — independent of the order in which the caller happened to list them.
    """
    labels = {
        "REF_AREA": geo,
        "COMMODITY": product,
        "TRANSACTION": flow,
        "UNIT": (UNIT,),
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
        Reference areas, as ISO 3166-1 numeric codes, for instance :py:`("398",)` for
        Kazakhstan.
    product, flow
        Labels to select on the ``COMMODITY`` and ``TRANSACTION`` dimensions. Empty
        selects all labels.
    start, end
        First and last period, inclusive.
    """
    return fetch_data(
        "UNSD",
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
    one should prefer :class:`UNSD_ENERGY_BALANCE`.

    Columns are ``n`` (ISO 3166-1 alpha-3), ``y``, ``product``, ``flow`` and ``value``,
    with values in TJ. Observations absent from the source are absent from the result:
    they are **not** filled with zero, because a zero and an unreported value are
    different facts and no consumer can distinguish them after the fact.

    See :func:`fetch` for the parameters.

    Raises
    ------
    ValueError
        if the data contain a reference area with no ISO 3166-1 alpha-3 code, for
        instance one of the aggregates published alongside the countries.
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
            f"Unexpected dimension(s) {extra} in UNSD data; expected only "
            f"{sorted(known)}"
        )

    df = series.rename("value").reset_index()
    df = df.rename(columns=DIMS)[["n", "y", "product", "flow", "value"]]

    # Map reference areas to alpha-3. iso_3166_alpha_3() returns None for a label it
    # does not recognize, which would make those observations vanish rather than fail,
    # so check before mapping. Compare .cepii.get_mapping(), which passes
    # on_missing="raise" to MappingAdapter for the same reason.
    labels = {n: iso_3166_alpha_3(n) for n in df["n"].unique()}
    if missing := sorted(k for k, v in labels.items() if v is None):
        raise ValueError(
            f"No ISO 3166-1 alpha-3 code for UNSD reference area {missing}"
        )

    return df.assign(n=df["n"].map(labels)).astype({"y": int})


@register_source
class UNSD_ENERGY_BALANCE(ExoDataSource):
    """Energy balances published by the UN Statistics Division.

    The data have the same dimensionality and units as :class:`.IEA_EWEB` —
    :py:`"energy:n-y-product-flow"` in TJ — under the distinct key tag :py:`":unsd"`, so
    a consumer written against the IEA balance can take this source without change on
    the |n| and |y| dimensions. The labels on ``product`` and ``flow`` are UNSD's own,
    **not** IEA codes; see the granularity note above before calibrating with them.

    Reference areas are given as ISO 3166-1 numeric codes.

    Aggregation on |n| and interpolation on |y| are both **off** by default, unlike
    :class:`.BaseOptions`. A balance records what each country reported for each year it
    reported; interpolating onto the model periods would fill the years it did not,
    making “not reported” and “reported as zero” indistinguishable, and would discard
    every observed period besides. Compare :meth:`.IEA_EWEB.transform`, which suppresses
    both for the same reason. A caller that wants either **may** ask for it explicitly.

    Example
    -------
    >>> keys = UNSD_ENERGY_BALANCE.add_tasks(
    ...     computer, context=context, geo=("398",), start=2000, end=2022
    ... )
    >>> result = computer.get(keys[0])
    """

    key = genno.Key("energy:n-y-product-flow:unsd")

    @dataclass
    class Options(BaseOptions):
        #: By default, do not aggregate.
        aggregate: bool = False
        #: By default, do not interpolate.
        interpolate: bool = False

        #: Reference areas to retrieve, as ISO 3166-1 numeric codes.
        geo: tuple[str, ...] = ()

        #: Select only these labels on the ``COMMODITY`` dimension.
        product: tuple[str, ...] = ()

        #: Select only these labels on the ``TRANSACTION`` dimension.
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
