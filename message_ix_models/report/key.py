"""Keys for setting up reporting tasks."""

from genno import Key, Keys

#: Gross domestic product.
GDP = Key("GDP", "ny")

#: Commodity price.
#:
#:  .. note:: genno ≤ 1.27.1 is sensitive to the dimension order; more recent versions
#:     are not.
PRICE_COMMODITY = Key("PRICE_COMMODITY", "nclyh")

#: All IAMC-structured data.
all_iamc = Key("all", (), "iamc")

#: Identifiers for coordinates, including:
#:
#: - :py:`.n_glb`: the output of :func:`.node_glb`.
coords = Keys(
    n_glb="n::glb",
)

#: Identifiers for grouping/aggregation mappings, including:
#:
#: - :py:`.c`: the output of :func:`.get_commodity_groups`.
#: - :py:`.t`: the output of :func:`.get_technology_groups`.
groups = Keys(
    c="c::groups",
    t="t::groups",
)
