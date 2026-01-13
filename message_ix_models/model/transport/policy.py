import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING

from genno import Key

from message_ix_models import ScenarioInfo
from message_ix_models.tools.policy import Policy
from message_ix_models.util import package_data_path
from message_ix_models.util.genno import Collector

if TYPE_CHECKING:
    from typing import TypedDict

    from genno import Computer

    from .config import Config

    class AsMessageDfKw(TypedDict):
        name: str
        dims: dict[str, str]
        common: dict[str, str]


log = logging.getLogger(__name__)

#: Target key that collects all data generated in this module.
TARGET = "transport::policy+ixmp"

TAX_EMISSION_KW: "AsMessageDfKw" = dict(
    name="tax_emission", dims=dict(type_year="y"), common={}
)

collect = Collector(TARGET, "{}::policy+ixmp".format)


@dataclass
class TaxEmission(Policy):
    """Emission tax at a fixed value."""

    #: Price in [currency] / t CO₂. Same as the `price` parameter to
    #: :mod:`.add_tax_emission`.
    value: float

    __hash__ = Policy.__hash__

    def add_tasks(self, c: "Computer") -> None:
        """Add tasks to prepare and add MESSAGE parameter data representing the policy.

        This mimics the behaviour of :func:`.navigate.workflow.tax_emission` and
        :func:`.tools.add_tax_emission.main`.
        """
        from genno import Quantity
        from iam_units import convert_gwp

        # Discount rate. {add_,}tax_emission() store this value on the scenario and then
        # retrieve it again to compute nominal future values of the tax. They use the
        # MACRO parameter "drate", which is not present if MACRO has not been set up on
        # the scenario. They ignore the MESSAGE "interestrate" parameter.
        drate = 0.05

        k = Key(self.__class__.__name__.lower(), "y")

        # Quantity filled with 1 + 'drate'
        c.add(k, lambda y: Quantity([1 + drate] * len(y), coords={"y": y}), "y::model")

        # Compute compound growth along dimension 'y'
        c.add(k[1], "compound_growth", k, dim="y")

        # Index to values as of y0, such that the y0 value = 1.0
        c.add(k[2], "index_to", k[1], "y0::coord")

        # Convert value from t CO₂ to t C
        value = convert_gwp(None, (self.value, "t"), "CO2", "C").magnitude

        # Multiply indexed by self.value and `cf`
        c.add(k[3], "mul", k[2], Quantity(value, units="USD / t"))

        # Convert to tax_emission parameter data
        kw = deepcopy(TAX_EMISSION_KW)
        kw["common"].update(node="World", type_emission="TCE", type_tec="all")

        collect(k.name, "as_message_df", k[3], **kw)


@dataclass
class ExogenousEmissionPrice(Policy):
    """Emission tax using data from file using :class:`.PRICE_EMISSION`."""

    #: Passed to :meth:`.PRICE_EMISSION.add_tasks`.
    source_url: str

    __hash__ = Policy.__hash__

    def add_tasks(self, c: "Computer") -> None:
        """Add tasks to prepare and add MESSAGE parameter data representing the policy.

        The class :class:`.PRICE_EMISSION` is used to retrieve values from file; these
        are then transformed to values for the MESSAGE parameter ``tax_emission``.
        """
        from message_ix_models.model.emissions import PRICE_EMISSION

        # Add tasks to retrieve PRICE_EMISSION data from file
        keys = PRICE_EMISSION.add_tasks(
            c,
            context=None,
            strict=False,  # Ignore existing n::codes key
            base_path=package_data_path("transport", "R12", "price-emission"),
            scenario_info=ScenarioInfo.from_url(self.source_url),
        )

        k = Key(self.__class__.__name__.lower(), keys[0].dims, "tmp")

        # Discard values for type_emission="CO2_shipping_IMO". The base scenarios on
        # which .model.transport operates do not have this structure.
        c.add(
            k,
            "select",
            keys[0],
            indexers={"type_emission": ["CO2_shipping_IMO"]},
            inverse=True,
        )

        # TODO Perhaps fill backwards to 2030, 2025, 2020

        # Convert to tax_emission parameter data
        kw = deepcopy(TAX_EMISSION_KW)
        kw["dims"].update(node="n", type_emission="type_emission", type_tec="type_tec")
        collect(k.name, "as_message_df", k, **kw)


def prepare_computer(c: "Computer") -> None:
    """Prepare `c` to calculate and add data for transport policies."""

    # Collect data in `TARGET` and connect to the "add transport data" key
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    # Retrieve the configuration
    config: "Config" = c.graph["context"].transport

    for policy in config.policy:
        assert isinstance(policy, (ExogenousEmissionPrice, TaxEmission))
        policy.add_tasks(c)
