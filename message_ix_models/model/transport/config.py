import logging
import re
from copy import deepcopy
from dataclasses import InitVar, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from genno import Quantity
from genno.operator import as_quantity

from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.project.navigate import T35_POLICY as NAVIGATE_SCENARIO
from message_ix_models.project.ssp import SSP_2024, ssp_field
from message_ix_models.project.transport_futures import SCENARIO as FUTURES_SCENARIO
from message_ix_models.util import package_data_path
from message_ix_models.util.config import ConfigHelper
from message_ix_models.util.sdmx import AnnotationsMixIn

from .policy import ExogenousEmissionPrice, TaxEmission

if TYPE_CHECKING:
    from sdmx.model import common

    from message_ix_models.tools.policy import Policy


log = logging.getLogger(__name__)

#: All files in :file:`data/transport/R12/price-emission/`.
PRICE_EMISSION_URL = {
    # "LED-SSP2": "SSP_LED_v5.3.1/baseline_1000f_v1",
    # "LED-SSP2": "SSP_LED_v5.3.1/INDC2030i_SSP2 - Very Low Emissions_v1",
    "LED-SSP2": "SSP_LED_v5.3.1/SSP2 - Very Low Emissions_v2",
    # "SSP1": "SSP_SSP1_v5.3.1/baseline_1000f_v1",
    # "SSP1": "SSP_SSP1_v5.3.1/INDC2030i_SSP1 - Low Emissions_a_v1",
    # "SSP1": "SSP_SSP1_v5.3.1/INDC2030i_SSP1 - Low Emissions_v1",
    # "SSP1": "SSP_SSP1_v5.3.1/INDC2030i_SSP1 - Very Low Emissions_v1",
    # "SSP1": "SSP_SSP1_v5.3.1/SSP1 - Low Emissions_a_v2",
    "SSP1": "SSP_SSP1_v5.3.1/SSP1 - Low Emissions_v2",
    # "SSP1": "SSP_SSP1_v5.3.1/SSP1 - Very Low Emissions_v2",
    # "SSP2": "SSP_SSP2_v5.3.1/baseline_1000f_v2",
    # "SSP2": "SSP_SSP2_v5.3.1/baselineS_10_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/baselineS_110_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/baselineS_15_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/baselineS_20_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/baselineS_25_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/baselineS_50_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/baselineS_5_v3",
    # "SSP2": "SSP_SSP2_v5.3.1/INDC2030i_SSP2 - Low Emissions_a_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/INDC2030i_SSP2 - Low Emissions_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/npiref2035_low_dem_scen2_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/NPIREF_price_cap_5$_bkp_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/NPiREF_SSP2 - Low Overshootf_price_cap_5$_bkp_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/NPiREF_SSP2 - Low Overshootf_v3",
    # "SSP2": "SSP_SSP2_v5.3.1/NPiREF_SSP2 - Medium-Low Emissionsf_v1",
    # "SSP2": "SSP_SSP2_v5.3.1/NPiREF_v10",
    # "SSP2": "SSP_SSP2_v5.3.1/SSP2 - Low Emissions_a_v2",
    "SSP2": "SSP_SSP2_v5.3.1/SSP2 - Low Emissions_v2",
    # "SSP2": "SSP_SSP2_v5.3.1/SSP2 - Low Overshoot_v2",
    # "SSP2": "SSP_SSP2_v5.3.1/SSP2 - Medium Emissions_a_v2",
    # "SSP2": "SSP_SSP2_v5.3.1/SSP2 - Medium Emissions_v2",
    # "SSP2": "SSP_SSP2_v5.3.1/SSP2 - Medium-Low Emissions_v2",
    "SSP3": "SSP_SSP3_v5.3.1/baseline_1000f_v1",
    # "SSP4": "SSP_SSP4_v5.3.1/baseline_1000f_v1",
    # "SSP4": "SSP_SSP4_v5.3.1/NPi2030_v1",
    # "SSP4": "SSP_SSP4_v5.3.1/NPiREF_SSP4 - Low Overshootf_v1",
    # "SSP4": "SSP_SSP4_v5.3.1/NPiREF_v1",
    "SSP4": "SSP_SSP4_v5.3.1/SSP4 - Low Overshoot_v2",
    # "SSP5": "SSP_SSP5_v5.3.1/baseline_1000f_v2",
    # "SSP5": "SSP_SSP5_v5.3.1/baseline2055_low_dem_scen_v1",
    # "SSP5": "SSP_SSP5_v5.3.1/baseline2060_low_dem_scen_v2",
    # "SSP5": "SSP_SSP5_v5.3.1/NPi2030_v1",
    # "SSP5": "SSP_SSP5_v5.3.1/NPiREF_SSP5 - Low Overshootf_v1",
    # "SSP5": "SSP_SSP5_v5.3.1/NPiREF_v1",
    "SSP5": "SSP_SSP5_v5.3.1/SSP5 - Low Overshoot_v2",
}


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
            "groups demand constraint freight ikarus ldv disutility other passenger "
            "plot data"
        ).split()
    )

    #: Used by :func:`.get_USTIMES_MA3T` to map MESSAGE regions to U.S. census divisions
    #: appearing in MA³T.
    node_to_census_division: dict = field(default_factory=dict)

    #: Instances of :class:`.Policy` subclasses applicable in a workflow or to a
    #: scenario.
    policy: set["Policy"] = field(default_factory=set)

    #: Flags for distinct scenario features according to projects. In addition to
    #: providing values directly, this can be set by passing :attr:`futures_scenario` or
    #: :attr:`navigate_scenario` to the constructor, or by calling
    #: :meth:`set_futures_scenario` or :meth:`set_navigate_scenario` on an existing
    #: Config instance.
    #:
    #: :mod:`.transport.build` and :mod:`.transport.report` code will respond to these
    #: settings in documented ways.
    project: dict[str, Any] = field(
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
    def from_context(cls, context: Context, options: Optional[dict] = None) -> "Config":
        """Configure `context` for building MESSAGEix-Transport.

        :py:`context.transport` is set to an instance of :class:`Config`.

        Configuration files and metadata are read and override the class defaults.

        The files listed in :data:`.METADATA` are stored in the respective attributes
        for instance :attr:`set` corresponding to :file:`data/transport/set.yaml`.

        If a subdirectory of :file:`data/transport/` exists corresponding to
        :py:`context.model.regions` (:attr:`.model.Config.regions`), then the files are
        loaded from that subdirectory, for instance :file:`data/transport/ISR/set.yaml`
        is preferred to :file:`data/transport/set.yaml`.

        .. note:: This method previously had behaviour similar to
           :meth:`.model.Config.regions_from_scenario`. Calling code should call that
           method if it is needed to ensure that :attr:`.model.Config.regions` has the
           desired value.
        """
        from .structure import make_spec

        # Handle arguments
        options = options or dict()

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

    def use_scenario_code(self, code: "common.Code") -> tuple[str, str]:
        """Update settings given a `code` with :class:`ScenarioCodeAnnotations`.

        Returns
        -------
        tuple of str
            The entries are:

            1. A short label suitable for a :class:`.Workflow` step name, for instance
               "SSP3 policy" or "SSP5", where the first part is :py:`code.id`. See
               :func:`.transport.workflow.generate`.
            2. A longer, more explicity label suitable for (part of) a
               :attr:`message_ix.Scenario.scenario` name in an :mod:`ixmp` database, for
               instance "SSP_2024.3".
        """
        sca = ScenarioCodeAnnotations.from_obj(code)

        # Look up the SSP_2024 Enum
        self.ssp = SSP_2024.by_urn(sca.SSP_URN)

        # Store settings on the Config instance
        self.base_scenario_url = sca.base_scenario_URL

        if sca.policy:
            self.policy.add(sca.policy)

        self.project["LED"] = sca.is_LED_scenario
        self.project["EDITS"] = {"activity": sca.EDITS_activity_id}

        # Construct labels including the SSP code and policy identifier
        # 1. ‘Short’ label used for workflow steps
        # 2. ‘Full’ label used in the scenario name
        return code.id, re.sub("^SSP", "SSP_2024.", code.id)


@dataclass
class ScenarioCodeAnnotations(AnnotationsMixIn):
    """Set of annotations appearing on each Code in ``CL_TRANSPORT_SCENARIO``."""

    SSP_URN: str
    is_LED_scenario: bool
    EDITS_activity_id: Optional[str]
    base_scenario_URL: str
    policy: Optional["Policy"]

    @classmethod
    def from_obj(cls, obj, globals=None):
        globals = (globals or {}) | dict(
            TaxEmission=TaxEmission,
            ExogenousEmissionPrice=ExogenousEmissionPrice,
        )
        return super().from_obj(obj, globals=globals)


def get_cl_scenario() -> "common.Codelist":
    """Retrieve ``Codelist=IIASA_ECE:CL_TRANSPORT_SCENARIO``.

    This code lists contains unique IDs for scenarios supported by the
    MESSAGEix-Transport workflow (:mod:`.transport.workflow`), plus the annotations:

    - ``SSP-URN``: the URN of a code identifying the SSP scenario to be used for
      sociodemographic data, for instance
      "urn:sdmx:org.sdmx.infomodel.codelist.Code=ICONICS:SSP(2024).1".
    - ``is-LED-scenario``: either "True" or "False".
    - ``EDITS-activity-id``: either "None", "'CA'", or "'HA'".
    """
    from sdmx.model import common

    from message_ix_models.util.sdmx import read

    IIASA_ECE = read("IIASA_ECE:AGENCIES")["IIASA_ECE"]

    return refresh_cl_scenario(
        common.Codelist(
            id="CL_TRANSPORT_SCENARIO", maintainer=IIASA_ECE, version="1.0.0"
        )
    )


def refresh_cl_scenario(
    existing: Optional["common.Codelist"] = None,
) -> "common.Codelist":
    """Refresh ``Codelist=IIASA_ECE:CL_TRANSPORT_SCENARIO``.

    The code list is entirely regenerated. If it is different from `cl`, the new
    version is returned. Otherwise, `cl` is returned unaltered.
    """
    from sdmx.model import common

    from message_ix_models.util.sdmx import read, write

    # Other data structures
    IIASA_ECE = read("IIASA_ECE:AGENCIES")["IIASA_ECE"]
    cl_ssp_2024 = read("ICONICS:SSP(2024)")

    cl: "common.Codelist" = common.Codelist(
        id="CL_TRANSPORT_SCENARIO",
        maintainer=IIASA_ECE,
        version="1.1.0",
        is_external_reference=False,
        is_final=False,
    )

    # - Model name:
    #   - 2024-11-25: use _v1.1 per a Microsoft Teams message.
    #   - 2025-02-20: update to _v2.1 per discussion with OF. At this point _v2.3 is the
    #     latest appearing in the database.
    #   - 2025-05-05: update to _v5.0.
    #   - 2025-06-24: update to _v6.1.
    # - The scenario names appear to form a sequence from "baseline_DEFAULT" to
    #   "baseline_DEFAULT_step_15" and finally "baseline". The one used below is the
    #   latest in this sequence for which y₀=2020, rather than 2030.
    base_url = "ixmp://ixmp-dev/SSP_SSP{}_v6.1/baseline_DEFAULT_step_13"

    def _code(id, name, c, led, edits):
        """Shorthand for creating a code."""
        sca = ScenarioCodeAnnotations(c.urn, led, edits, base_url.format(c.id), None)
        return common.Code(id=id, name=name, **sca.get_annotations(dict))

    # SSP baselines and policies
    for ssp_code in cl_ssp_2024:
        c_base = _code(f"SSP{ssp_code.id}", "", ssp_code, False, None)
        cl.append(c_base)

        # Simple carbon tax
        c = deepcopy(c_base)
        c.get_annotation(id="policy").text = repr(TaxEmission(1000.0))
        c.id += " tax"
        cl.append(c)

        # PRICE_EMISSION from exogenous data file
        c = deepcopy(c_base)
        c.get_annotation(id="policy").text = repr(
            ExogenousEmissionPrice("ixmp://ixmp-dev/" + PRICE_EMISSION_URL[c.id])
        )
        c.id += " exo price"
        cl.append(c)

    # LED
    name_template = "Low Energy Demand/High-with-Low scenario with SSP{} demographics"
    for ssp in ("1", "2"):
        ssp_code = cl_ssp_2024[ssp]
        name = name_template.format(ssp_code.id)
        cl.append(_code(f"LED-SSP{ssp_code.id}", name_template, ssp_code, True, None))

    # EDITS
    ssp_code = cl_ssp_2024["2"]
    name_template = "EDITS scenario with ITF PASTA {!r} activity"
    for id_, name in (("CA", "Current Ambition"), ("HA", "High Ambition")):
        cl.append(
            _code(f"EDITS-{id_}", name_template.format(id_), ssp_code, False, id_)
        )

    # FIXME This condition may appear to be always False, because the date/time differs.
    #       Adjust upstream (in sdmx1) to ignore this difference.
    if existing is None or not cl.compare(existing, strict=True):
        # No existing code list or new code list differs from existing
        write(cl)
        return cl
    else:
        return existing
