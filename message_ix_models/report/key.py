"""Keys for setting up reporting tasks."""

from genno import Key

GDP = Key("GDP", "ny")

# NB genno ≤ 1.27.1 is sensitive to the order
PRICE_COMMODITY = Key("PRICE_COMMODITY", "nclyh")
