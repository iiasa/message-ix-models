from dataclasses import dataclass, fields

from message_ix_models.model.structure import codelists
from message_ix_models.util.context import _ALIAS


@dataclass
class Config:
    """Settings and valid values for :mod:`message_ix_models.model` and submodules."""

    #: The 'node' codelist (regional aggregation) to use. Must be one of the lists of
    #: nodes described at :doc:`/pkg-data/node`.
    regions: str = "R14"

    #: The 'year' codelist (time periods) to use, Must be one of the lists of periods
    #: described at :doc:`/pkg-data/year`.
    years: str = "B"

    #: Create the reference energy system with dummy commodities and technologies. See
    #: :func:`.bare.get_spec`.
    res_with_dummies: bool = False

    def check(self):
        """Check the validity of :attr:`regions` and :attr:`years`."""
        if self.regions not in codelists("node"):
            raise ValueError(f"regions={self.regions!r} not among {codelists('node')}")
        if self.years not in codelists("year"):
            raise ValueError(f"regions={self.years!r} not among {codelists('year')}")


# Extend the list of settings that can be set directly on a Context instance.
_ALIAS.update({f.name: "model" for f in fields(Config)})
