import logging
from dataclasses import InitVar, dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, Literal, Optional, Union

import message_ix
from genno import Quantity

from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.project.navigate import T35_POLICY as NAVIGATE_SCENARIO
from message_ix_models.project.ssp import SSP_2024, ssp_field
from message_ix_models.project.transport_futures import SCENARIO as FUTURES_SCENARIO
from message_ix_models.report.util import as_quantity
from message_ix_models.util import identify_nodes, package_data_path
from message_ix_models.util.config import ConfigHelper

if TYPE_CHECKING:
    from sdmx.model.common import Codelist

log = logging.getLogger(__name__)


@dataclass
class DataSourceConfig(ConfigHelper):
    """Sources for input data."""

    #: Emissions: ID of a dump from a base scenario.
    emissions: str = "1"

    #: Non-passenger and non-light-duty vehicles.
    non_LDV: str = "IKARUS"


def quantity_field(value):
    """Field with a mutable default value that is a :class:`.Quantity`."""
    return field(default_factory=lambda: as_quantity(value))


@dataclass
class Config(ConfigHelper):
    """Configuration for MESSAGEix-Transport.

    This dataclass stores and documents all configuration settings required and used by
    :mod:`~message_ix_models.model.transport`. It also handles (via
    :meth:`.from_context`) loading configuration and values from files like
    :file:`config.yaml`, while respecting higher-level configuration, for instance
    :attr:`.model.Config.regions`.
    """

    #: Information about the base model.
    base_model_info: ScenarioInfo = field(default_factory=ScenarioInfo)

    #: Values for constraints.
    #:
    #: "LDV growth_activity_lo", "LDV growth_activity_up"
    #:    Allowable *annual* decrease or increase (respectively) in activity of each LDV
    #:    technology. For example, a value of 0.01 means the activity may increase by 1%
    #:    from one year to the next. For periods of length >1 year, MESSAGE compounds
    #:    the value. Defaults are multiples of 0.0192 = (1.1 ^ 0.2) - 1.0; or ±10% each
    #:    5 years. See :func:`ldv.constraint_data`.
    #: "non-LDV growth_new_capacity_up"
    #:    Allowable annual increase in new capacity (roughly, sales) of each technology
    #:    for transport modes *other than* LDV. See :func:`non_ldv.growth_new_capacity`.
    #: "* initial_*_up"
    #:    Base value for growth constraints. These values are arbitrary.
    constraint: dict = field(
        default_factory=lambda: {
            "LDV growth_activity_lo": -0.0192,
            "LDV growth_activity_up": 0.0192 * 3.0,
            "non-LDV growth_activity_lo": -0.0192 * 1.0,
            "non-LDV growth_activity_up": 0.0192 * 2.0,
            "non-LDV growth_new_capacity_up": 0.0192 * 1.0,
            "non-LDV initial_activity_up": 1.0,
            "non-LDV initial_new_capacity_up": 1.0,
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
    cost: dict = field(
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

    #: Sources for input data.
    data_source: DataSourceConfig = field(default_factory=DataSourceConfig)

    #: Set of modes handled by demand projection. This list must correspond to groups
    #: specified in the corresponding technology.yaml file.
    #:
    #: .. todo:: Read directly from technology.yaml
    demand_modes: list[str] = field(
        default_factory=lambda: ["LDV", "2W", "AIR", "BUS", "RAIL"]
    )

    #: Include dummy ``demand`` data for testing and debugging.
    dummy_demand: bool = False

    #: Include dummy data for LDV technologies.
    dummy_LDV: bool = False

    #: Include dummy technologies supplying commodities required by transport, for
    #: testing and debugging.
    dummy_supply: bool = False

    #: Various efficiency factors.
    efficiency: dict = field(
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

    #: Generate relation entries for emissions.
    emission_relations: bool = True

    #: Various other factors.
    factor: dict = field(default_factory=dict)

    #: If :obj:`True` (the default), do not record/preserve parameter data when removing
    #: set elements from the base model.
    fast: bool = True

    #: Fixed future point for total passenger activity.
    fixed_GDP: Quantity = quantity_field("1500 kUSD_2005 / passenger / year")

    #: Fixed future point for total passenger activity.
    #:
    #: AJ: Assuming mean speed of the high-speed transport is 330 km/h leads to 132495
    #: passenger km / capita / year (Schafer & Victor 2000).
    #: Original comment (DLM): “Assume only half the speed (330 km/h) and not as steep a
    #: curve.”
    fixed_pdt: Quantity = quantity_field("132495 km / year")

    #: Load factors for vehicles [tonne km per vehicle km].
    #:
    #: ``F ROAD``: similar to IEA “Future of Trucks” (2017) values; see
    #: .transport.freight. Alternately use 5.0, similar to Roadmap 2017 values.
    load_factor: dict = field(
        default_factory=lambda: {
            "F ROAD": 10.0,
            "F RAIL": 10.0,
        }
    )

    #: Logit share exponents or cost distribution parameters [0]
    lamda: float = -2.0

    #: Period in which LDV costs match those of a reference region.
    #: Dimensions: (node,).
    ldv_cost_catch_up_year: dict = field(default_factory=dict)

    #: Method for calibrating LDV stock and sales:
    #:
    #: - :py:`"A"`: use data from :file:`ldv-new-capacity.csv`, if it exists.
    #: - :py:`"B"`: use func:`.ldv.stock`; see the function documentation.
    ldv_stock_method: Literal["A", "B"] = "B"

    #: Tuples of (node, technology (transport mode), commodity) for which minimum
    #: activity should be enforced. See :func:`.non_ldv.bound_activity_lo`.
    minimum_activity: dict[tuple[str, tuple[str, ...], str], float] = field(
        default_factory=dict
    )

    #: Base year shares of activity by mode. This should be the stem of a CSV file in
    #: the directory :file:`data/transport/{regions}/mode-share/`.
    mode_share: str = "default"

    #: List of modules containing model-building calculations.
    modules: list[str] = field(
        default_factory=lambda: (
            "groups demand freight ikarus ldv disutility non_ldv plot data"
        ).split()
    )

    #: Used by :func:`.get_USTIMES_MA3T` to map MESSAGE regions to U.S. census divisions
    #: appearing in MA³T.
    node_to_census_division: dict = field(default_factory=dict)

    #: **Temporary** setting for the SSP 2024 project: indicates whether the base
    #: scenario used is a policy (carbon pricing) scenario, or not. This currently does
    #: not affect *any* behaviour of :mod:`~message_ix_models.model.transport` except
    #: the selection of a base scenario via :func:`.base_scenario_url`.
    policy: bool = False

    #: Flags for distinct scenario features according to projects. In addition to
    #: providing values directly, this can be set by passing :attr:`futures_scenario` or
    #: :attr:`navigate_scenario` to the constructor, or by calling
    #: :meth:`set_futures_scenario` or :meth:`set_navigate_scenario` on an existing
    #: Config instance.
    #:
    #: :mod:`.transport.build` and :mod:`.transport.report` code will respond to these
    #: settings in documented ways.
    project: dict[str, Enum] = field(
        default_factory=lambda: dict(
            futures=FUTURES_SCENARIO.BASE, navigate=NAVIGATE_SCENARIO.REF
        )
    )

    #: Scaling factors for production function [0]
    scaling: float = 1.0

    #: Mapping from nodes to other nodes towards which share weights should converge.
    share_weight_convergence: dict = field(default_factory=dict)

    #: Specification for the structure of MESSAGEix-Transport, processed from contents
    #: of :file:`set.yaml` and :file:`technology.yaml`.
    spec: Spec = field(default_factory=Spec)

    #: Speeds of transport modes. The labels on the 't' dimension must match
    #: :attr:`demand_modes`. Source: Schäefer et al. (2010)
    #:
    #: .. note:: Temporarily ignored for :pull:`551`; data are read instead from
    #:    :file:`speed.csv`.
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

    #: Enum member indicating a Shared Socioeconomic Pathway, if any, to use for
    #: exogenous data.
    ssp: ssp_field = ssp_field(default=SSP_2024["2"])

    #: :any:`True` if a base model or MESSAGEix-Transport scenario (possibly with
    #: solution data) is available.
    with_scenario: bool = False

    #: :any:`True` if solution data is available.
    with_solution: bool = False

    #: Work hours per year, used to compute the value of time.
    work_hours: Quantity = quantity_field("1600 hours / passenger / year")

    #: Year for share convergence.
    year_convergence: int = 2110

    # Init-only variables

    #: Extra entries for :attr:`modules`, supplied to the constructor. May be either a
    #: space-delimited string (:py:`"module_a -module_b"`) or sequence of strings.
    #: Values prefixed with a hyphen (:py:`"-module_b"`) are *removed* from
    #: :attr:`.modules`.
    extra_modules: InitVar[Union[str, list[str]]] = None

    #: Identifier of a Transport Futures scenario, used to update :attr:`project` via
    #: :meth:`.ScenarioFlags.parse_futures`.
    futures_scenario: InitVar[str] = None

    #: Identifiers of NAVIGATE T3.5 demand-side scenarios, used to update
    #: :attr:`project` via :meth:`.ScenarioFlags.parse_navigate`.
    navigate_scenario: InitVar[str] = None

    def __post_init__(self, extra_modules, futures_scenario, navigate_scenario):
        # Handle extra_modules
        em = extra_modules or []
        for m in em.split() if isinstance(em, str) else em:
            if m.startswith("-"):
                try:
                    idx = self.modules.index(m[1:])
                except ValueError:
                    pass
                else:
                    self.modules.pop(idx)
            else:
                self.modules.append(m)

        # Handle values for :attr:`futures_scenario` and :attr:`navigate_scenario`
        self.set_futures_scenario(futures_scenario)
        self.set_navigate_scenario(navigate_scenario)

    @classmethod
    def from_context(
        cls,
        context: Context,
        scenario: Optional[message_ix.Scenario] = None,
        options: Optional[dict] = None,
    ) -> "Config":
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
           for instance :file:`data/transport/ISR/set.yaml` is preferred to
           :file:`data/transport/set.yaml`.
        """
        from .structure import make_spec

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
                    f"Override Context.model.regions={context.model.regions!r} with "
                    f"{regions!r} from scenario contents"
                )
                context.model.regions = regions

        # Default configuration
        config = cls()

        try:
            # Update with region-specific configuration
            config.read_file(
                package_data_path("transport", context.model.regions, "config.yaml")
            )
        except FileNotFoundError as e:
            log.warning(e)

        # Data structure that cannot be stored in YAML
        if isinstance(config.minimum_activity, list):
            config.minimum_activity = {
                tuple(row[:-1]): row[-1] for row in config.minimum_activity
            }

        # Separate data source options
        ds_options = options.pop("data source", {})
        # Update values, store on context
        result = context["transport"] = replace(
            config, **options, data_source=config.data_source.replace(**ds_options)
        )

        # Create the structural spec
        result.spec = make_spec(context.model.regions)

        return result

    def check(self):
        """Check consistency of :attr:`project`."""
        s1 = self.project["futures"]
        s2 = self.project["navigate"]

        if all(map(lambda s: s.value > 0, [s1, s2])):
            raise ValueError(f"Scenario settings {s1} and {s2} are not compatible")

    def set_futures_scenario(self, value: Optional[str]) -> None:
        """Update :attr:`project` from a string indicating a Transport Futures scenario.

        See :meth:`ScenarioFlags.parse_futures`. This method alters :attr:`mode_share`
        and :attr:`fixed_demand` according to the `value` (if any).
        """
        if value is None:
            return

        s = FUTURES_SCENARIO.parse(value)
        self.project.update(futures=s)
        self.check()

        self.mode_share = s.id()

        if self.mode_share == "A---":
            log.info(f"Set fixed demand for TF scenario {value!r}")
            self.fixed_demand = as_quantity("275000 km / year")

    def set_navigate_scenario(self, value: Optional[str]) -> None:
        """Update :attr:`project` from a string representing a NAVIGATE scenario.

        See :meth:`ScenarioFlags.parse_navigate`.
        """
        if value is None:
            return

        s = NAVIGATE_SCENARIO.parse(value)
        self.project.update(navigate=s)
        self.check()


def get_cl_scenario() -> "Codelist":
    """Generate ``Codelist=IIASA_ECE:CL_TRANSPORT_SCENARIO``.

    This code lists contains unique IDs for scenarios supported by the
    MESSAGEix-Transport workflow (:mod:`.transport.workflow`), plus the annotations:

    - ``SSP-URN``: the URN of a code identifying the SSP scenario to be used for
      sociodemographic data, for instance
      "urn:sdmx:org.sdmx.infomodel.codelist.Code=ICONICS:SSP(2024).1".
    - ``is-LED-scenario``: either "True" or "False".
    - ``EDITS-activity-id``: either "None", "'CA'", or "'HA'".
    """
    from sdmx.model import common

    from message_ix_models.util.sdmx import read, write

    # Other data structures
    as_ = read("IIASA_ECE:AGENCIES")
    cl_ssp_2024 = read("ICONICS:SSP(2024)")

    cl = common.Codelist(
        id="CL_TRANSPORT_SCENARIO", maintainer=as_["IIASA_ECE"], version="1.0.0"
    )

    def _a(*values):
        """Shorthand to generate the annotations."""
        return [
            common.Annotation(id="SSP-URN", text=values[0]),
            common.Annotation(id="is-LED-scenario", text=repr(values[1])),
            common.Annotation(id="EDITS-activity-id", text=repr(values[2])),
        ]

    for ssp_code in cl_ssp_2024:
        cl.append(
            common.Code(
                id=f"SSP{ssp_code.id}", annotations=_a(ssp_code.urn, False, None)
            )
        )

    for ssp in ("1", "2"):
        ssp_code = cl_ssp_2024[ssp]
        cl.append(
            common.Code(
                id=f"LED-SSP{ssp_code.id}",
                name=f"Low Energy Demand/High-with-Low scenario with SSP{ssp_code.id} "
                "demographics",
                annotations=_a(ssp_code.urn, True, None),
            )
        )

    for id_, name in (("CA", "Current Ambition"), ("HA", "High Ambition")):
        cl.append(
            common.Code(
                id=f"EDITS-{id_}",
                name=f"EDITS scenario with ITF PASTA {id_!r} activity",
                annotations=_a(cl_ssp_2024["2"].urn, False, id_),
            )
        )

    write(cl)

    return cl
