"""Keys to refer to various quantities."""

from genno import Key

__all__ = [
    "PRICE_COMMODITY",
    "cg",
    "cost",
    "fv_cny",
    "fv",
    "gdp_cap",
    "gdp_index",
    "gdp_ppp",
    "gdp",
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
    "price_full",
    "price_sel0",
    "price_sel1",
    "price",
    "sw",
    "t_modes",
    "y",
]

# Existing keys, either from Reporter.from_scenario() or .build.add_structure()
gdp = Key("GDP", "ny")
gdp_exo = Key("gdp", "ny")
mer_to_ppp = Key("MERtoPPP", "ny")
PRICE_COMMODITY = Key("PRICE_COMMODITY", "nclyh")
price_full = PRICE_COMMODITY / ("h", "l")

# Keys for new quantities

#: Shares of population with consumer group (`cg`) dimension.
cg = Key("cg share:n-y-cg")

cost = Key("cost", "nyct")

#: Population.
pop = Key("population", "ny")

#: Population with `area_type` dimension.
pop_at = pop * "area_type"

#: GDP at purchasing power parity.
gdp_ppp = gdp + "PPP"

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

price_sel1 = price_full + "transport"
price = price_sel1 + "smooth"
price_sel0 = price_sel1 + "raw units"

sw = Key("share weight", "nty")

# Keys for (partial or full) sets or indexers

#: List of nodes excepting "World" or "*_GLB".
n = "n::ex world"

#: List of transport modes.
t_modes = "t::transport modes"

#: Model periods.
y = "y::model"
