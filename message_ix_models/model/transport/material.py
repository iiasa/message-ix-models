"""MESSAGEix-Transport integration with :mod:`.model.material`.

In order to use this code:

1. Set :py:`extra_modules="material"` when constructing
   :class:`.transport.config.Config`.
2. Call :func:`.transport.build.main`.

The code expects that an existing/base |Scenario| is available at the :py:`"scenario"`
key, so that existing data for the MESSAGE ``demand`` parameter can be adjusted.
"""

from collections import defaultdict
from typing import TYPE_CHECKING

from genno import Keys
from genno.core.key import single_key

from message_ix_models.util.genno import Collector

from . import key, util

if TYPE_CHECKING:
    from genno import Computer


#: Target key that collects all data generated in this module.
TARGET = "transport::material+ixmp"

collect = Collector(TARGET, "{}::material+ixmp".format)

# FIXME Do not hard-code this. Instead, use 1 or more of:
# - Labels in input_cap_new.csv that align with .model.materials.
# - A CircEUlar-specific 'commodity' codelist that records correspondence with
#   .model.materials
COMMODITY_INFO = {
    "automotive steel": "steel",
    "cast Al": "aluminum",
    "cast iron": "pig_iron",  # NB Several other commodities exist
    # "co": "",  # Missing
    # "copper electric grade": "copper",  # Commented in material/set.yaml
    # "li": "",  # Missing
    # "mn": "",  # Missing
    # "nickel": "",  # Missing
    # "other": "",  # Missing
    # "p": "",  # Missing
    # "plastics": "",  # Missing
    "stainless steel": "steel",
    "wrought Al": "aluminum",
    # "zinc": "",  # Missing
}

# FIXME Do not hard code this
TECHNOLOGY = {
    "BEV": {"ELC_100"},
    "ICE": {
        "IAHe_ptrp",
        "IAHm_ptrp",
        "ICAe_ptrp",
        "ICE_conv",
        "ICE_nga",
        "ICEm_ptrp",
        "ICH_chyb",
        "IGH_ghyb",
    },
    "PHEV": {"PHEV_ptrp"},
}

DIMS = dict(
    commodity="c",
    node_loc="n",
    node_dest="n",
    node_origin="n",
    year_vtg="y",
    technology="t",
)

# Keyword arguments for as_message_df() for different parameters
_DEMAND_KW = dict(name="demand", dims=DIMS, common=dict())
_ICN_KW = dict(
    name="input_cap_new", dims=DIMS, common=util.COMMON | dict(level="demand")
)
_OCR_KW = dict(
    name="output_cap_ret", dims=DIMS, common=util.COMMON | dict(level="end_of_life")
)


def prepare_computer(c: "Computer") -> None:
    """Prepare `c` to calculate and add data for materiality of transport."""
    # Collect data in `TARGET` and connect to the "add transport data" key
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    k = Keys(
        exo=(key.exo.input_cap_new - "exo") / "scenario",
        # Same key as used in .transport.ldv.stock
        # TODO Move to .key
        sales="sales:n-t-y:LDV",
        demand=key.demand_base + "MT",
    )

    # From input_cap_new.csv, select:
    # - Only a single scenario
    #   TODO Retrieve the CircEUlar scenario ID from config
    indexers = dict(scenario="_CT_C_D_D")
    c.add(k.exo[0], "select", key.exo.input_cap_new, indexers=indexers)

    # Aggregate ≥1 original commodity IDs into .model.material commodity IDs
    c_groups = defaultdict(list)
    for c_original, c_model in COMMODITY_INFO.items():
        c_groups[c_model].append(c_original)
    # Aggregate each original technology ID into 1 or more 'groups' of length 1
    # (this is equivalent to a broadcast operation)
    t_groups = {}
    for t_original, t_model in TECHNOLOGY.items():
        t_groups.update({t: [t_original] for t in t_model})

    c.add(
        k.exo[1], "aggregate", k.exo[0], groups=dict(c=c_groups, t=t_groups), keep=False
    )

    # Convert units: (material commodities [Mt]) / (transport CAP/CAP_NEW [Mvehicle])
    c.add(k.exo[2], "convert_units", k.exo[1], units="Mt / Mvehicle")

    # Convert data to MESSAGE-format data frames
    collect("input_cap_new", "as_message_df", k.exo[2], **_ICN_KW)
    collect("output_cap_ret", "as_message_df", k.exo[2], **_OCR_KW)

    # Multiply base-period LDV sales by material intensity
    tmp = single_key(c.add("demand::MT+0", "mul", k.exo[2], k.sales, sums=True))

    # Sum on "t" dimension; expand "l" dimension
    c.add(k.demand[0], "expand_dims", tmp / "t", dim={"l": ["demand"]})

    # Convert units: material commodities demand [Mt/year]
    c.add(k.demand[1], "convert_units", k.demand[0], units="Mt / year")

    # Share of this transport total in existing material demand as of y₀
    c.add(k.demand["share"], "div", key.demand_base, k.demand[1])

    # Multiply existing material demand by this share
    c.add(k.demand["adj"], "mul", key.demand_base, k.demand["share"])

    # Convert data to MESSAGE-format data frame
    collect("demand", "as_message_df", k.demand["adj"], **_DEMAND_KW)
