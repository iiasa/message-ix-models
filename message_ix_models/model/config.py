import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from message_ix_models.util.config import ConfigHelper
from message_ix_models.util.node import identify_nodes

if TYPE_CHECKING:
    import message_ix

log = logging.getLogger(__name__)


@dataclass
class Config(ConfigHelper):
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

    #: ID of the relation used to constrain global total CO₂ emissions. A code with this
    #: ID **must** be in the code list identified by :attr:`relations`.
    #:
    #: In :mod:`message_data`, this ID was stored in the module data variable
    #: :py:`message_data.projects.engage.runscript_main.RELATION_GLOBAL_CO2`.
    relation_global_co2: str = "CO2_Emission_Global_Total"

    #: The 'year' codelist (time periods) to use, Must be one of the lists of periods
    #: described at :doc:`/pkg-data/year`.
    years: str = "B"

    #: Create the reference energy system with dummy commodities and technologies. See
    #: :func:`.bare.get_spec`.
    res_with_dummies: bool = False

    #: Default or preferred units for model quantities and reporting.
    units: dict = field(
        default_factory=lambda: {"energy": "GWa", "CO2 emissions": "Mt / a"}
    )

    def check(self):
        """Check the validity of :attr:`regions`, :attr:`relations`, :attr:`years`."""
        from message_ix_models.model.structure import codelists

        for attr, name in [
            ("regions", "node"),
            ("relations", "relation"),
            ("years", "year"),
        ]:
            if getattr(self, attr) not in codelists(name):
                raise ValueError(
                    f"{attr}={getattr(self, attr)!r} not among {codelists(name)}"
                )

    def regions_from_scenario(
        self, scenario: Optional["message_ix.Scenario"] = None
    ) -> None:
        """Update :attr:`regions` by inspection of an existing `scenario`.

        The attribute is updated only if:

        1. `scenario` is not :any:`None`.
        2. :func:`identify_nodes` returns the ID of a node code list.
        3. The returned ID from (2) is different from the current value of
           :attr:`regions`.
        """
        if scenario is None:
            return

        try:
            # Identify the node code list used in `scenario`
            regions = identify_nodes(scenario)
        except ValueError:
            pass
        else:
            # log.debug(
            #     f"scenario.set('node') = {' '.join(sorted(scenario.set('node')))}"
            # )
            if regions != self.regions:
                log.info(
                    f"Replace .model.Config.regions={self.regions!r} with {regions!r} "
                    f" from contents of {scenario.url!r}"
                )
                self.regions = regions
