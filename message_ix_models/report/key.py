"""Keys for setting up reporting tasks."""

from genno import Key, Keys

GDP = Key("GDP", "ny")

# NB genno ≤ 1.27.1 is sensitive to the order
PRICE_COMMODITY = Key("PRICE_COMMODITY", "nclyh")

#: All IAMC-structured data.
all_iamc = Key("all", (), "iamc")

#: Identifiers for grouping/aggregation mappings.
groups = Keys(
    c="c::groups",
)
