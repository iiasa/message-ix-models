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
gdp = Key("GDP:n-y")
mer_to_ppp = Key("MERtoPPP:n-y")
PRICE_COMMODITY = Key("PRICE_COMMODITY", "nclyh")
price_full = PRICE_COMMODITY.drop("h", "l")

# Keys for new quantities
pop_at = Key("population", "n y area_type".split())
pop = pop_at.drop("area_type")
cg = Key("cg share", "n y cg".split())
gdp_ppp = gdp + "PPP"
gdp_cap = gdp_ppp + "capita"
gdp_index = gdp_cap + "index"
ms = Key("mode share:n-t-y")
pdt_nyt = Key("pdt", "nyt")  # Total PDT shared out by mode
pdt_cap = pdt_nyt.drop("t") + "capita"
pdt_ny = pdt_nyt.drop("t") + "total"
pdt_cny = Key("pdt", "cny")  # With 'c' instead of 't' dimension, for demand
ldv_ny = Key("pdt ldv", "ny")
ldv_nycg = Key("pdt ldv") * cg
ldv_cny = Key("pdt ldv", "cny")
fv = Key("freight activity", "nty")
fv_cny = Key("freight activity", "cny")
price_sel1 = price_full + "transport"
price_sel0 = price_sel1 + "raw units"
price = price_sel1 + "smooth"
cost = Key("cost", "nyct")
sw = Key("share weight", "nty")

n = "n::ex world"
t_modes = "t::transport modes"
y = "y::model"
