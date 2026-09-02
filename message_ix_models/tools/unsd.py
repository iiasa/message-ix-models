"""Handle data from the UN Statistics Division (UNSD)."""

import logging
from functools import partial
from typing import TYPE_CHECKING

import pandas as pd
from genno import Key
from genno.operator import assign_units, mul

from message_ix_models.tools.exo_data import SDMXSource, register_source
from message_ix_models.util import MappingAdapter

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import AnyQuantity

log = logging.getLogger(__name__)

#: Commodity code for natural gas in ``DF_UNDATA_ENERGY``.
NATURAL_GAS = "3000"

#: Factor from gross to net calorific value for natural gas, as used by UNSD.
NATURAL_GAS_GCV_TO_NCV = 0.9


@register_source
class UNSD_ENERGY_BALANCE(SDMXSource):
    """Provider of energy balance data from UNSD.

    The data have the dimensions and units of :class:`.IEA_EWEB`, but UNSD's own codes
    on the ``product`` (9 fuel groups) and ``flow`` dimensions. Labels on |n| are ISO
    3166-1 numeric codes, for instance "398" for Kazakhstan.
    """

    source = "UNSD"
    dataflow = "DF_UNData_EnergyBalance"
    dims = {
        "REF_AREA": "n",
        "TIME_PERIOD": "y",
        "COMMODITY": "product",
        "TRANSACTION": "flow",
    }
    select = {"UNIT": ("HSO",)}  # Terajoules
    units = "TJ"
    key = Key("energy:n-y-product-flow:unsd")

    @classmethod
    def get_mapping(cls) -> MappingAdapter:
        from pycountry import countries

        return MappingAdapter(
            {"n": [(c.numeric, c.alpha_3) for c in countries]}, on_missing="raise"
        )


@register_source
class UNSD_ENERGY(UNSD_ENERGY_BALANCE):
    """Provider of commodity-level energy data from UNSD.

    As :class:`UNSD_ENERGY_BALANCE`, but with individual commodities on ``product``.
    The source gives values in physical units; see :meth:`transform`.
    """

    dataflow = "DF_UNDATA_ENERGY"
    select = {"FREQ": ("A",)}
    attributes = "o"
    units = ""
    key = Key("energy:n-y-product-flow:unsd-commodity")

    def transform(self, c: "Computer", base_key: Key) -> Key:
        """Convert to TJ using :func:`to_tj`, then :meth:`.SDMXSource.transform`."""
        k = base_key
        c.add(k["factor"], partial(self.attribute, "CONVERSION_FACTOR"))
        c.add(k["TJ"], to_tj, k, k["factor"])
        return super().transform(c, k["TJ"])


def to_tj(qty: "AnyQuantity", factor: "AnyQuantity") -> "AnyQuantity":
    """Convert `qty` to TJ using the conversion `factor` of each observation.

    Natural gas, reported on a gross calorific basis, is also multiplied by
    :data:`NATURAL_GAS_GCV_TO_NCV`. Observations with no conversion factor, for
    instance capacities and stocks, are dropped.
    """
    f = factor.to_series().dropna()
    if n_dropped := factor.size - f.size:
        log.info(f"Drop {n_dropped} observations with no conversion factor to TJ")

    products = f.index.unique("product")
    ncv = type(qty)(
        pd.Series(
            [NATURAL_GAS_GCV_TO_NCV if p == NATURAL_GAS else 1.0 for p in products],
            index=products,
        )
    )
    return assign_units(mul(qty, type(qty)(f), ncv), "TJ")
