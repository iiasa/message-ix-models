from dataclasses import dataclass, fields

from message_ix_models.model.structure import codelists
from message_ix_models.util.context import _ALIAS


@dataclass
class Config:
    """Settings and valid values for :mod:`message_ix_models.model` and submodules.

    For backwards compatibility, it is possible to access these on a :class:`Context`
    using:

    .. code-block:: python

       c = Context()
       c.regions = "R14"

    …however, it is best to access them explicitly as:

    .. code-block:: python

       c.model.regions = "R14"
    """

    #: The 'node' codelist (regional aggregation) to use. Must be one of the lists of
    #: nodes described at :doc:`/pkg-data/node`.
    regions: str = "R14"

    #: The 'relations' codelist to use. Must be one of the lists of relations described
    #: at :doc:`/pkg-data/relation`.
    relations: str = "A"

    #: The 'year' codelist (time periods) to use, Must be one of the lists of periods
    #: described at :doc:`/pkg-data/year`.
    years: str = "B"

    #: Create the reference energy system with dummy commodities and technologies. See
    #: :func:`.bare.get_spec`.
    res_with_dummies: bool = False

    def check(self):
        """Check the validity of :attr:`regions`, :attr:`relations`, :attr:`years`."""
        for attr, name in [
            ("regions", "node"),
            ("relations", "relation"),
            ("years", "year"),
        ]:
            if getattr(self, attr) not in codelists(name):
                raise ValueError(
                    f"{attr}={getattr(self, attr)!r} not among {codelists(name)}"
                )


# Extend the list of settings that can be set directly on a Context instance.
_ALIAS.update({f.name: "model" for f in fields(Config)})
