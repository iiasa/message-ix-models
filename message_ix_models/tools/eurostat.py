"""Handle data from Eurostat."""

from genno import Key

from message_ix_models.tools.exo_data import SDMXSource, register_source
from message_ix_models.util import MappingAdapter

#: ISO 3166-1 alpha-3 codes for labels on the ``geo`` dimension that are not ISO 3166-1
#: alpha-2 codes. "XKX" is the user-assigned code for Kosovo used by the World Bank.
COUNTRY_CODE = {"EL": "GRC", "UK": "GBR", "XK": "XKX"}


@register_source
class ESTAT_ENERGY_BALANCE(SDMXSource):
    """Provider of energy balance data from Eurostat.

    The data have the dimensions and units of :class:`.IEA_EWEB`, but Eurostat's own
    codes on the ``product`` (``siec``) and ``flow`` (``nrg_bal``) dimensions. Labels on
    |n| are Eurostat ``geo`` labels, for instance "RS" for Serbia.
    """

    source = "ESTAT"
    dataflow = "NRG_BAL_C"
    dims = {"geo": "n", "TIME_PERIOD": "y", "siec": "product", "nrg_bal": "flow"}
    select = {"freq": ("A",), "unit": ("TJ",)}
    units = "TJ"
    key = Key("energy:n-y-product-flow:estat")

    @classmethod
    def get_mapping(cls) -> MappingAdapter:
        from pycountry import countries

        labels = [(c.alpha_2, c.alpha_3) for c in countries] + list(
            COUNTRY_CODE.items()
        )
        return MappingAdapter({"n": labels}, on_missing="raise")
