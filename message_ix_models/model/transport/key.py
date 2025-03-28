"""Keys to refer to various quantities."""

from types import SimpleNamespace

from genno import Key, KeySeq

from message_ix_models.report.key import GDP, PRICE_COMMODITY
from message_ix_models.util.genno import Keys

from .data import iter_files

__all__ = [
    "activity_ldv_full",
    "cg",
    "cost",
    "exo",
    "fv_cny",
    "fv",
    "gdp_cap",
    "gdp_index",
    "gdp_ppp",
    "ldv_cny",
    "ldv_ny",
    "ldv_nycg",
    "mer_to_ppp",
    "ms",
    "n",
    "pdt_cap",
    "pdt_cny",
    "pdt_ny",
    "pdt_nyt",
    "pop_at",
    "pop",
    "price",
    "sw",
    "t_modes",
    "y",
]

# Existing keys, either from Reporter.from_scenario() or .build.add_structure()
gdp_exo = Key("gdp", "ny")
mer_to_ppp = Key("MERtoPPP", "ny")

# Keys for new quantities

#: Quantities for broadcasting (t) to (t, c, l). See :func:`.broadcast_t_c_l`.
#:
#: - :py:`.input`: Quantity for broadcasting (all values 1) from every transport |t|
#:   (same as ``t::transport``) to the :math:`(c, l)` that that technology receives as
#:   input.
#: - :py:`.output`: same as above, but for the :math:`(c, l)` that the technology
#:   produces as output.
bcast_tcl = Keys(
    input="broadcast:t-c-l:transport+input",
    output="broadcast:t-c-l:transport+output",
)

#: Quantities for broadcasting (y) to (yv, ya). See :func:`.broadcast_y_yv_ya`.
#:
#: - :py:`.all`: Quantity for broadcasting (all values 1) from every |y| to every
#:   possible combination of :math:`(y^V=y, y^A)`—including historical periods.
#: - :py:`.model`: same as above, but only model periods (``y::model``).
#: - :py:`.no_vintage`: same as above, but only the cases where :math:`y^V = y^A`.
bcast_y = Keys(
    all="broadcast:y-yv-ya:all",
    model="broadcast:y-yv-ya:model",
    no_vintage="broadcast:y-yv-ya:no vintage",
)

#: Shares of population with consumer group (`cg`) dimension.
cg = Key("cg share:n-y-cg")

cost = Key("cost", "nyct")

#: Population.
pop = Key("population", "ny")

#: Population with `area_type` dimension.
pop_at = pop * "area_type"

#: GDP at purchasing power parity.
gdp_ppp = GDP + "PPP"

#: :data:`.gdp_ppp` per capita.
gdp_cap = gdp_ppp + "capita"

gdp_index = gdp_cap + "index"

fv = Key("freight activity", "nty")
fv_cny = Key("freight activity", "cny")

ldv_cny = Key("pdt ldv", "cny")
ldv_ny = Key("pdt ldv", "ny")
ldv_nycg = Key("pdt ldv") * cg
ms = Key("mode share", "nty")

#: Passenger distance travelled.
_pdt = Key("pdt", "ny")

#: PDT per capita.
pdt_cap = _pdt + "capita"

#: PDT with 'c' dimension, for demand.
pdt_cny = _pdt * "c"
pdt_ny = _pdt + "total"

#: PDT with 't' dimension. The labels along the 't' dimension are modes, not individual
#: technologies.
pdt_nyt = _pdt * "t"

#: Prices.
price = KeySeq(PRICE_COMMODITY / ("h", "l") + "transport")

#: Keys for :mod:`.transport.report`.
report = SimpleNamespace(
    all="transport all",
    sdmx=Key("transport::sdmx"),
)

sw = Key("share weight", "nty")

# Keys for (partial or full) sets or indexers

#: List of nodes excepting "World" or "*_GLB".
n = "n::ex world"

#: List of transport modes.
t_modes = "t::transport modes"

#: Model periods.
y = "y::model"

#: Keys referring to loaded input data flows (exogenous data loaded from files).
#: Attributes correspond to the members of :mod:`.transport.data`; see
#: :doc:`/transport/input` for a complete list.
#:
#: .. code-block:: python
#:
#:    >>> from message_ix_models.model.transport.key import exo
#:    >>> exo.act_non_ldv
#:    <activity:n-t-y:non-ldv+exo>
exo = Keys()

for name, df in iter_files():
    setattr(exo, name, df.key)

activity_ldv_full = exo.activity_ldv / "scenario" + "full"
