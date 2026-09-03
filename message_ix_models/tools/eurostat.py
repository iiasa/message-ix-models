"""Handle data from Eurostat."""

from collections.abc import Mapping
from typing import TYPE_CHECKING

from genno import Key

from message_ix_models.tools.exo_data import SDMXSource, register_source
from message_ix_models.util import MappingAdapter

if TYPE_CHECKING:
    from genno import Computer
    from genno.types import AnyQuantity

#: ISO 3166-1 alpha-3 codes for labels on the ``geo`` dimension that are not ISO 3166-1
#: alpha-2 codes. "XKX" is the user-assigned code for Kosovo used by the World Bank.
COUNTRY_CODE = {"EL": "GRC", "UK": "GBR", "XK": "XKX"}

#: Eurostat ``siec`` product codes that make up each product of
#: :class:`.UNSD_ENERGY_BALANCE`. Coke, briquettes, coal tar, and manufactured gases are
#: coal products in UNSD's vocabulary, so the Eurostat aggregate "C0000X0350-0370"
#: (solid fossil fuels), which includes them, is not used; oil shale and peat count with
#: primary coal. The oil-product members are non-overlapping and sum to Eurostat's
#: "O4000XBIO" total less the primary oil members.
UNSD_PRODUCT: Mapping[str, tuple[str, ...]] = {
    "B00_CL": ("C0110", "C0121", "C0129", "C0210", "C0220", "P1100", "S2000"),
    "B01_CP": ("C0311", "C0312", "C0320", "C0330", "C0340", "C0350-0370", "P1200"),
    "B02_PO": ("O4100_TOT", "O4200", "O4300", "O4400X4410", "O4500"),
    "B03_OP": (
        "O4610",
        "O4620",
        "O4630",
        "O4640",
        "O4651",
        "O4652XR5210B",
        "O4653",
        "O4661XR5230B",
        "O4669",
        "O4671XR5220B",
        "O4680",
        "O4691",
        "O4692",
        "O4693",
        "O4694",
        "O4695",
        "O4699",
    ),
    "B04_NG": ("G3000",),
    "B07_EL": ("E7000",),
    "B08_HT": ("H8000",),
}

#: Eurostat ``nrg_bal`` flow codes and signs that make up each flow of
#: :class:`.UNSD_ENERGY_BALANCE`. UNSD reports exports as negative values, and Eurostat
#: as positive. UNSD's transformation flows are net—the energy entering a plant negative
#: and the energy leaving it positive—where Eurostat reports inputs and outputs as
#: separate, positive flows. Agriculture and forestry plus fishing form the UNSD
#: agriculture group.
UNSD_FLOW: Mapping[str, tuple[tuple[str, int], ...]] = {
    "B01_01": (("PPRD", 1),),
    "B02_03": (("IMP", 1),),
    "B03_04": (("EXP", -1),),
    "B11_088": (("TO_EHG", 1), ("TI_EHG_E", -1)),
    "B20_086": (("TO_RPI_RO", 1), ("TI_RPI_RI_E", -1)),
    "B25_FEC": (("FC_E", 1),),
    "B26_121": (("FC_IND_E", 1),),
    "B40_122": (("FC_TRA_E", 1),),
    "B48_1232": (("FC_OTH_AF_E", 1), ("FC_OTH_FISH_E", 1)),
    "B49_1235": (("FC_OTH_CP_E", 1),),
    "B50_1231": (("FC_OTH_HH_E", 1),),
    "B51_1234": (("FC_OTH_NSP_E", 1),),
}

#: Eurostat codes for the primary energy of renewable electricity and heat.
RENEWABLE_ELECTRICITY = ("RA100", "RA300", "RA420", "RA500")
RENEWABLE_HEAT = ("RA200", "RA410")

#: Rules for :func:`to_unsd_vocabulary`, each a tuple of (UNSD product, UNSD flow,
#: Eurostat ``siec`` codes, Eurostat ``nrg_bal`` code, sign). Most are the product of
#: :data:`UNSD_PRODUCT` and :data:`UNSD_FLOW`. The rest express conventions of UNSD's
#: balance that Eurostat's does not share: hydro, wind, solar photovoltaic, and marine
#: energy entering power plants is primary production of electricity, and is not output
#: of electricity and heat plants; pumped-storage output is not output of those plants
#: either, and electricity entering pumped storage is not their input; geothermal and
#: solar thermal energy is primary production of heat, and its direct use is final
#: consumption of heat.
UNSD_RULES: tuple[tuple[str, str, tuple[str, ...], str, int], ...] = (
    tuple(
        (product, flow, siec, nrg_bal, sign)
        for product, siec in UNSD_PRODUCT.items()
        for flow, codes in UNSD_FLOW.items()
        for nrg_bal, sign in codes
    )
    + (
        ("B07_EL", "B01_01", RENEWABLE_ELECTRICITY, "TI_EHG_E", 1),
        ("B07_EL", "B11_088", RENEWABLE_ELECTRICITY, "TI_EHG_E", -1),
        ("B07_EL", "B11_088", ("E7000",), "TO_EHG_PH", -1),
        ("B07_EL", "B11_088", ("E7000",), "TI_EHG_E", 1),
        ("B08_HT", "B01_01", RENEWABLE_HEAT, "PPRD", 1),
    )
    + tuple(
        ("B08_HT", flow, RENEWABLE_HEAT, nrg_bal, 1)
        for flow, codes in UNSD_FLOW.items()
        for nrg_bal, _ in codes
        if nrg_bal.startswith("FC_")
    )
)


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


def _rules(
    product: tuple[str, ...], flow: tuple[str, ...]
) -> list[tuple[str, str, tuple[str, ...], str, int]]:
    """Rules of :data:`UNSD_RULES` for the UNSD `product` and `flow` codes, or all."""
    for name, codes, valid in (
        ("product", product, UNSD_PRODUCT),
        ("flow", flow, UNSD_FLOW),
    ):
        if unknown := sorted(set(codes) - set(valid)):
            raise ValueError(f"{name} code(s) {unknown} not in {sorted(valid)}")
    return [
        r
        for r in UNSD_RULES
        if (not product or r[0] in product) and (not flow or r[1] in flow)
    ]


@register_source
class ESTAT_ENERGY_BALANCE_UNSD(ESTAT_ENERGY_BALANCE):
    """Provider of energy balance data from Eurostat, in the vocabulary of UNSD.

    Data are retrieved as by :class:`.ESTAT_ENERGY_BALANCE`, then converted with
    :func:`to_unsd_vocabulary` so that they have the ``product`` and ``flow`` codes and
    the sign conventions of :class:`.UNSD_ENERGY_BALANCE`, for countries whose Eurostat
    series is preferred to their UNSD series; see :mod:`.tools.energy_balance`.

    The ``product`` and ``flow`` options take UNSD codes: the keys of
    :data:`UNSD_PRODUCT` and :data:`UNSD_FLOW`. If either is empty, every Eurostat code
    in :data:`UNSD_RULES` is retrieved, so the result covers exactly the mapped
    vocabulary.
    """

    key = Key("energy:n-y-product-flow:estat-unsd")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        rules = _rules(self.options.product, self.options.flow)
        self.query_key["siec"] = tuple(dict.fromkeys(s for r in rules for s in r[2]))
        self.query_key["nrg_bal"] = tuple(dict.fromkeys(r[3] for r in rules))

    def transform(self, c: "Computer", base_key: Key) -> Key:
        """Apply :func:`to_unsd_vocabulary`, then :meth:`.SDMXSource.transform`."""
        k = base_key["unsd"]
        c.add(k, to_unsd_vocabulary, base_key)
        return super().transform(c, k)


def to_unsd_vocabulary(qty: "AnyQuantity") -> "AnyQuantity":
    """Convert Eurostat energy balance data to the vocabulary of UNSD.

    Each observation of `qty`, with Eurostat codes on the "product" and "flow"
    dimensions, contributes to the UNSD product and flow of every rule in
    :data:`UNSD_RULES` that names its codes, multiplied by the rule's sign.
    Observations whose pair of codes appears in no rule—for instance hydro energy
    imported, or electricity produced as primary energy—are not part of the vocabulary
    and are dropped.

    Raises
    ------
    ValueError
        if `qty` has a product or flow label that appears in no rule at all. Dropping
        such observations silently would understate the aggregates they belong to.
    """
    import pandas as pd

    rules = pd.DataFrame(
        [(p, f, s, n, sign) for p, f, siec, n, sign in UNSD_RULES for s in siec],
        columns=["product", "flow", "siec", "nrg_bal", "sign"],
    )

    series = qty.to_series()
    labels = series.index.to_frame(index=False).assign(value=series.to_numpy())
    if missing := sorted(set(labels["product"]) - set(rules["siec"])):
        raise ValueError(f"Unmapped Eurostat product code(s): {missing}")
    if missing := sorted(set(labels["flow"]) - set(rules["nrg_bal"])):
        raise ValueError(f"Unmapped Eurostat flow code(s): {missing}")

    result = (
        labels.rename(columns={"product": "siec", "flow": "nrg_bal"})
        .merge(rules, on=["siec", "nrg_bal"])
        .assign(value=lambda df: df["value"] * df["sign"])
        .groupby(list(series.index.names))["value"]
        .sum()
    )
    return type(qty)(result, units=qty.units)
