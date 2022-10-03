import logging
from dataclasses import InitVar, dataclass, field, replace
from typing import Dict, List, Optional

from genno import Quantity
from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.util import identify_nodes, load_private_data, private_data_path
from message_ix_models.util.config import ConfigHelper

from message_data.reporting.util import as_quantity
from message_data.tools import generate_set_elements

log = logging.getLogger(__name__)


@dataclass
class DataSourceConfig(ConfigHelper):
    """Sources for input data."""

    dummy_demand: bool = False

    dummy_supply: bool = False

    #: Emissions: ID of a dump from a base scenario.
    emissions: str = "1"

    gdp: str = "SSP2"

    population: str = "SSP2"

    LDV: str = "US-TIMES MA3T"

    non_LDV: str = "IKARUS"


def quantity_field(value):
    """Field with a mutable default value that is a :class:`.Quantity`."""
    return field(default_factory=lambda: as_quantity(value))


@dataclass
class Config(ConfigHelper):
    #: Values for constraints:
    #:
    #: ``LDV growth_activity``
    #:     Default 0.0192 = (1.1 ^ 0.2) - 1.0; or ±10% each 5 years
    constraint: Dict = field(
        default_factory=lambda: {
            "LDV growth_activity": 0.0192,
        }
    )

    #: Scaling factors for costs.
    #:
    #: ``ldv nga``
    #:    Scaling factor to reduce the cost of NGA vehicles.
    #:
    #:    .. note:: DLM: “applied to the original US-TIMES cost data. That original data
    #:       simply seems too high - much higher than conventional gasoline vehicles in
    #:       the base-year and in future, which is strange.
    #:
    #: ``bus inv``
    #:    Investment costs of bus technologies, relative to the cost of ``ICG_bus``.
    #:    Dictionary with 1 key per ``BUS`` technology.
    #:
    #:    - Used in ikarus.py
    #:    - This is from the IKARUS data in GEAM_TRP_Technologies.xlsx; sheet
    #:      'updateTRPdata', with the comment "Original data from Sei (PAO)."
    #:    - This probably refers to some source that gave relative costs of different
    #:      buses, in PAO, for this year; it is applied across all years.
    cost: Dict = field(
        default_factory=lambda: {
            #
            "ldv nga": 0.85,
            "bus inv": {
                "ICH_bus": 1.153,  # ie. 150,000 / 130,000
                "PHEV_bus": 1.153,
                "FC_bus": 1.538,  # ie. 200,000 / 130,000
                "FCg_bus": 1.538,
                "FCm_bus": 1.538,
            },
        }
    )

    #: Set of modes handled by demand projection. This list must correspond to groups
    #: specified in the corresponding technology.yaml file.
    #:
    #: .. todo:: Read directly from technology.yaml
    demand_modes: List[str] = field(
        default_factory=lambda: ["LDV", "2W", "AIR", "BUS", "RAIL"]
    )

    #: Various efficiency factors.
    efficiency: Dict = field(
        default_factory=lambda: {
            "*": 0.2,
            "hev": 0.2,
            "phev": 0.2,
            "fcev": 0.2,
            # Similar to 'cost/bus inv' above, except for output efficiency.
            "bus output": {
                "ICH_bus": 1.424,  # ie. 47.6 / 33.42
                "PHEV_bus": 1.424,
                "FC_bus": 1.563,  # ie. 52.25 / 33.42
                "FCg_bus": 1.563,
                "FCm_bus": 1.563,
            },
        }
    )

    #: Various other factors.
    factor: Dict = field(default_factory=dict)

    #: If :obj:`True` (the default), do not record/preserve parameter data when removing
    #: set elements from the base model.
    fast: bool = True

    #: Fixed future point for total passenger activity.
    #: Default = 149500 * 1.12818725502081 * 2
    #:
    #: Original comment: “Assume only half the speed (330 km/h) and not as steep a
    #: curve.”
    fixed_demand: Quantity = quantity_field("337327.9893 km / year")

    #: Fixed future point for total passenger activity.
    fixed_GDP: Quantity = quantity_field("300 kUSD / passenger / year")

    #: Load factors for vehicles.
    #:
    #: ``freight``: similar to IEA “Future of Trucks” (2017) values; see
    #: .transport.data.freight. Alternately use 5.0, similar to Roadmap 2017 values.
    load_factor: Dict = field(
        default_factory=lambda: {"freight": 10.0}  # tonne km per vehicle km
    )

    #: Logit share exponents or cost distribution parameters [0]
    lamda: float = -2.0

    #: LDV activity for average drivers in the model base year. Dimensions: (node,).
    #: Units: [vehicle-distance / time].
    ldv_activity: Optional[Quantity] = None

    #: Period in which LDV costs match those of a reference region. Dimensions: (node,).
    ldv_cost_catch_up_year: Dict = field(default_factory=dict)

    #: Lifetime of light duty vehicles. Dimensions: (consumer type,). Units: [time].
    ldv_lifetime: Quantity = quantity_field(
        dict(modest=20, average=15, frequent=10, _dim="consumer type", _unit="year")
    )

    #: Base year shares of activity by mode. This should be the stem of a CSV file in
    #: the directory data/transport/{regions}/mode-share/.
    mode_share: str = "default"

    #: Used by :func:`.get_USTIMES_MA3T` to map MESSAGE regions to U.S. census divisions
    #: appearing in MA³T.
    node_to_census_division: Dict = field(default_factory=dict)

    #: Load factors/occupancy in [passenger km / vehicle km].
    non_ldv_output: Dict = field(
        default_factory=lambda: {
            # Urban public rail transport (agg. of metro, trams, regional trains, etc.)
            "rail_pub": 80,
            # Intercity trains
            "dMspeed_rai": 200,
            "Mspeed_rai": 200,
            "Hspeed_rai": 200,
            # Aircraft
            "con_ar": 115,
            "conm_ar": 115,
            "conE_ar": 115,
            "conh_ar": 115,
            # Buses
            "ICE_M_bus": 20.9,
            "ICE_H_bus": 20.9,
            "ICG_bus": 20.9,
            "ICAe_bus": 20.9,
            "ICH_bus": 20.9,
            "PHEV_bus": 20.9,
            "FC_bus": 20.9,
            "FCg_bus": 20.9,
            "FCm_bus": 20.9,
            "Trolley_bus": 20.9,
        }
    )

    #: Scaling factors for production function [0]
    scaling: float = 1.0

    #: Mapping from nodes to other nodes towards which share weights should converge.
    share_weight_convergence: Dict = field(default_factory=dict)

    #: Speeds of transport modes. The labels on the 't' dimension must match
    #: :attr:`demand_modes`. Source: Schäefer et al. (2010)
    speeds: Quantity = quantity_field(
        {
            "_dim": "t",
            "_unit": "km / hour",
            "LDV": 54.5,  # = 31 + 78 / 2
            "2W": 31,
            "AIR": 270,
            "BUS": 19,
            "RAIL": 35,
        }
    )

    #: Work hours per year, used to compute the value of time.
    work_hours: Quantity = quantity_field("1600 hours / passenger / year")

    #: Year for share convergence.
    year_convergence: int = 2110

    #: Information on the structure of MESSAGEix-Transport, processed from contents of
    #: :file:`set.yaml` and :file:`technology.yaml`.
    set: Dict = field(default_factory=dict, repr=False)

    #: Identifier of a Transport Futures scenario.
    futures_scenario: InitVar[str] = None

    #: Sources for input data.
    data_source: DataSourceConfig = field(default_factory=DataSourceConfig)

    def __post_init__(self, futures_scenario):
        if futures_scenario not in (None, "default", "base", "A---", "debug"):
            raise ValueError(f"Unknown Transport Futures scenario {futures_scenario!r}")

        if futures_scenario:
            self.mode_share = futures_scenario

            if self.mode_share == "A---":
                log.info(f"Set fixed demand for TF scenario {futures_scenario!r}")
                self.fixed_demand = as_quantity("275000 km / year")

    @classmethod
    def from_context(
        cls, context: Context, scenario: Scenario = None, options: Dict = None
    ):
        """Configure `context` for building MESSAGEix-Transport.

        1. If `scenario` is given, ``context.model.regions`` is updated to match. See
           :func:`.identify_nodes`.
        2. ``context.transport`` is set to an instance of :class:`Config`.

           Configuration files and metadata are read and override the class defaults.

           The files listed in :data:`.METADATA` are stored in the respective
           attributes, e.g. :attr:`set` corresponding to
           :file:`data/transport/set.yaml`.

           If a subdirectory of :file:`data/transport/` exists corresponding to
           ``context.model.regions`` then the files are loaded from that subdirectory,
           e.g. :file:`data/transport/ISR/set.yaml` is preferred to
           :file:`data/transport/set.yaml`.
        """
        from message_data.model.transport.utils import path_fallback

        # Handle arguments
        options = options or dict()

        try:
            # Identify the node codelist used in `scenario`
            regions = identify_nodes(scenario) if scenario else context.model.regions
        except (AttributeError, ValueError):
            pass
        else:
            if context.model.regions != regions:
                log.info(
                    f"Set Context.model.regions = {regions!r} from scenario contents"
                )
                context.model.regions = regions

        # Default configuration
        config = cls()

        try:
            # Update with region-specific configuration
            config.read_file(
                private_data_path("transport", context.model.regions, "config.yaml")
            )
        except FileNotFoundError as e:
            log.warning(e)

        # Overrides specific to regional versions
        tmp = dict()
        for fn in ("set.yaml", "technology.yaml"):
            # Field name
            name = fn.split(".yaml")[0]

            # Load and store the data from the YAML file: either in a subdirectory for
            # context.model.regions, or the top-level data directory
            path = path_fallback(context, fn).relative_to(private_data_path())
            tmp[name] = load_private_data(*path.parts)

        # Merge contents of technology.yaml into set.yaml
        config.set.update(tmp.pop("set"))
        config.set["technology"]["add"] = tmp.pop("technology")

        # Convert some values to codes
        for set_name in config.set:
            generate_set_elements(config.set, set_name)

        # Separate data source options
        ds_options = options.pop("data source", {})

        for name in {"quiet"}:
            if name in options:
                log.debug(f"Discard option {name}={options.pop(name)!r}")

        # Update values, store on context
        context["transport"] = replace(
            config, **options, data_source=config.data_source.replace(**ds_options)
        )
