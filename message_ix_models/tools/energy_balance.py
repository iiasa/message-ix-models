"""Energy balance of any country from an openly-licensed provider.

UNSD publishes an energy balance for every country, compiled from the national returns.
For the countries that file the joint Eurostat/IEA/UNECE questionnaire, Eurostat
publishes the same returns directly, usually one year sooner and, for most of them,
from 1990 where UNSD starts in 1992 or later. :func:`get_source` chooses between the two
for a given country; :func:`load_data` retrieves the balance in one vocabulary—the
``product`` and ``flow`` codes and the sign conventions of
:class:`.UNSD_ENERGY_BALANCE`—whichever service it comes from.
"""

import logging
from typing import TYPE_CHECKING

from message_ix_models.tools.eurostat import COUNTRY_CODE, ESTAT_ENERGY_BALANCE_UNSD
from message_ix_models.tools.unsd import UNSD_ENERGY_BALANCE

if TYPE_CHECKING:
    import pandas as pd

    from message_ix_models.tools.exo_data import SDMXSource

log = logging.getLogger(__name__)

#: Labels on the ``geo`` dimension of Eurostat's ``NRG_BAL_C`` data flow whose series
#: covers at least the periods that UNSD's does for the same country, as of 2026-09-04:
#: Eurostat 1990–2024 (Montenegro from 2005, Kosovo from 2000) against UNSD 1990 or
#: 1992–2023. Five other labels in the data flow are omitted because their series is
#: shorter than UNSD's at one end or the other: Bosnia and Herzegovina ("BA", from
#: 2014), Georgia ("GE", from 2013), Moldova ("MD", from 2010), Ukraine ("UA", to
#: 2020), and the United Kingdom ("UK", to 2019). Kosovo ("XK") has no UNSD series.
ESTAT_GEO = frozenset(
    """AL AT BE BG CY CZ DE DK EE EL ES FI FR HR HU IE IS IT LT LU LV ME MK MT NL NO PL
    PT RO RS SE SI SK TR XK""".split()
)


def get_source(country: str) -> tuple[type["SDMXSource"], str]:
    """Return the data source class and its |n| label for `country`.

    Parameters
    ----------
    country
        ISO 3166-1 alpha-3 code, or "XKX" for Kosovo.

    Returns
    -------
    tuple
        :class:`.ESTAT_ENERGY_BALANCE_UNSD` and the Eurostat ``geo`` label if `country`
        is in :data:`ESTAT_GEO`; otherwise :class:`.UNSD_ENERGY_BALANCE` and the ISO
        3166-1 numeric code. Either class can also be used directly, for instance
        :class:`.UNSD_ENERGY_BALANCE` for a country that Eurostat also covers.

    Raises
    ------
    ValueError
        if `country` is not a known code.
    """
    from pycountry import countries

    c = countries.get(alpha_3=country)
    a2 = {a3: label for label, a3 in COUNTRY_CODE.items()}.get(country)
    if a2 is None and c is not None:
        a2 = c.alpha_2

    if a2 in ESTAT_GEO:
        return ESTAT_ENERGY_BALANCE_UNSD, a2
    elif c is not None:
        return UNSD_ENERGY_BALANCE, c.numeric
    else:
        raise ValueError(f"country={country!r} is not an ISO 3166-1 alpha-3 code")


def load_data(
    country: str,
    *,
    product: tuple[str, ...] = (),
    flow: tuple[str, ...] = (),
    start: int | None = None,
    end: int | None = None,
) -> "pd.DataFrame":
    """Return the energy balance of `country` as a :class:`pandas.DataFrame`.

    This is the entry point for consumers that process the data with :mod:`pandas` and
    have no :class:`genno.Computer` to which tasks could be added; those that do build
    one should use :func:`get_source` and :meth:`.ExoDataSource.add_tasks`.

    Parameters
    ----------
    country
        Passed to :func:`get_source`.
    product, flow
        Codes of :class:`.UNSD_ENERGY_BALANCE` to select. If empty, all products or
        flows are returned that both providers can supply: the keys of
        :data:`.UNSD_PRODUCT` and :data:`.UNSD_FLOW`.
    start, end
        First and last period.

    Returns
    -------
    pandas.DataFrame
        with columns "n" (ISO 3166-1 alpha-3 code), "y", "product", "flow", and "value"
        in TJ. Periods and products the provider does not report are absent, not zero.
    """
    from genno import Computer

    from message_ix_models import Context
    from message_ix_models.tools.eurostat import UNSD_FLOW, UNSD_PRODUCT

    cls, label = get_source(country)
    product = product or tuple(UNSD_PRODUCT)
    flow = flow or tuple(UNSD_FLOW)

    c = Computer()
    keys = cls.add_tasks(
        c,
        context=Context.get_instance(-1),
        n=(label,),
        product=product,
        flow=flow,
        start=start,
        end=end,
    )
    result = c.get(keys[0]).to_series().rename("value").reset_index()
    log.info(f"{len(result)} observations for {country} from {cls.source}")
    return result[["n", "y", "product", "flow", "value"]]
