from dataclasses import dataclass

from message_ix_models.tools.policy import Policy


@dataclass
class TaxEmission(Policy):
    """Emission tax at a fixed value."""

    #: Passed to :mod:`.add_tax_emission`.
    value: float

    __hash__ = Policy.__hash__


@dataclass
class ExogenousEmissionPrice(Policy):
    """Emission tax using data from file using :class:`.PRICE_EMISSION`."""

    #: Passed to :meth:`.PRICE_EMISSION.add_tasks`.
    source_url: str

    __hash__ = Policy.__hash__
