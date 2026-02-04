import logging
import re
from collections.abc import Iterator
from dataclasses import InitVar, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Literal

from genno import Quantity
from genno.operator import as_quantity

from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.project.navigate import T35_POLICY as NAVIGATE_SCENARIO
from message_ix_models.project.ssp import SSP_2024, ssp_field
from message_ix_models.project.transport_futures import SCENARIO as FUTURES_SCENARIO
from message_ix_models.util import package_data_path, short_hash
from message_ix_models.util.config import ConfigHelper
from message_ix_models.util.sdmx import AnnotationsMixIn, StructureFactory

from .policy import ExogenousEmissionPrice, TaxEmission

if TYPE_CHECKING:
    from sdmx.model import common

    from message_ix_models.tools.policy import Policy


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

    # Private attribute for `code` property
    _code: "common.Code | None" = None

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
            "data stock policy"
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
    extra_modules: InitVar[str | list[str]] = []

    #: Identifier of a Transport Futures scenario, used to update :attr:`project` via
    #: :meth:`.ScenarioFlags.parse_futures`.
    futures_scenario: InitVar[str] = None

    #: Identifiers of NAVIGATE T3.5 demand-side scenarios, used to update
    #: :attr:`project` via :meth:`.ScenarioFlags.parse_navigate`.
    navigate_scenario: InitVar[str] = None

    def __post_init__(self, extra_modules, futures_scenario, navigate_scenario) -> None:
        self.use_modules(extra_modules)

        # Handle values for :attr:`futures_scenario` and :attr:`navigate_scenario`
        self.set_futures_scenario(futures_scenario)
        self.set_navigate_scenario(navigate_scenario)

    @classmethod
    def from_context(cls, context: Context, options: dict | None = None) -> "Config":
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

        # Separate data source options and "code"
        ds_options = options.pop("data source", {})
        code = options.pop("code", None)
        # Update values, store on context
        result = context["transport"] = replace(
            config, **options, data_source=config.data_source.replace(**ds_options)
        )

        # Set the scenario code, if any, triggering the setter magic
        if code:
            result.code = code

        # Create the structural spec
        result.spec = make_spec(context.model.regions)

        return result

    @property
    def code(self) -> "common.Code":
        """A :class:`sdmx.Code <sdmx.model.common.Code>` for the transport scenario.

        :py:`.code.id` is a short label suitable for a :class:`.Workflow` step name, for
        instance "SSP3 policy" or "SSP5". See :func:`.transport.workflow.generate`.

        `code` can be set either using a Code instance with
        :class:`ScenarioCodeAnnotations`—such as from :func:`.get_cl_scenario`—or the ID
        of a item in this particular code list. When set, other Config attributes are
        also updated:

        - :attr:`ssp`: per :attr:`ScenarioCodeAnnotations.SSP_URN`.
        - :attr:`base_scenario_url`: per
          :attr:`ScenarioCodeAnnotations.base_scenario_URL`.
        - :attr:`policy`: per :attr:`ScenarioCodeAnnotations.policy`.
        - :attr:`project`: the "DIGSY", "EDITS", and "LED" keys are set per
          :attr:`ScenarioCodeAnnotations.DIGSY_scenario_URN`,
          :attr:`~ScenarioCodeAnnotations.EDITS_scenario_URN`, and
          :attr:`~ScenarioCodeAnnotations.is_LED_scenario`, respectively.
        """
        assert self._code is not None
        return self._code

    @code.setter
    def code(self, value: "str | common.Code") -> None:
        from message_ix_models.project.digsy.structure import SCENARIO as DIGSY
        from message_ix_models.project.edits.structure import SCENARIO as EDITS

        c = self._code = CL_SCENARIO.get()[value] if isinstance(value, str) else value

        sca = ScenarioCodeAnnotations.from_obj(c)

        # Look up the SSP_2024 Enum
        self.ssp = SSP_2024.by_urn(sca.SSP_URN)

        # Store settings on the Config instance
        self.base_scenario_url = sca.base_scenario_URL

        if sca.policy:
            self.policy.add(sca.policy)

        # Update `project`
        self.project["LED"] = sca.is_LED_scenario
        self.project["DIGSY"] = DIGSY.by_urn(sca.DIGSY_scenario_URN)
        self.project["EDITS"] = EDITS.by_urn(sca.EDITS_scenario_URN)

        self.use_modules(sca.extra_modules)

    @property
    def label(self) -> str:
        """‘Full’ label used in the scenario name.

        Compared to :attr:`code.id <code>`, this is a longer, more explicit label,
        suitable for (part of) a :attr:`message_ix.Scenario.scenario` name in an
        :mod:`ixmp` database, for instance "SSP_2024.3".
        """
        return re.sub("^(M )?SSP", "SSP_2024.", self.code.id)

    def check(self):
        """Check consistency of :attr:`project`."""
        s1 = self.project["futures"]
        s2 = self.project["navigate"]

        if all(map(lambda s: s.value > 0, [s1, s2])):
            raise ValueError(f"Scenario settings {s1} and {s2} are not compatible")

    def get_target_url(self, context: "Context") -> str:
        """Construct a target URL for a built MESSAGEix-Transport scenario.

        If the :attr:`.dest` URL is set on `context` (for instance, provided via the
        :program:`--dest` CLI option), this URL returned with `label` appended to the
        scenario name.

        If not, a form is used like:

        - :py:`model = "MESSAGEix-GLOBIOM 1.1-T-{regions}"`. Any value of the "model"
          key from :attr:`.core.Config.dest_scenario` is appended.
        - :py:`scenario = "{label}"`. Any value of the "scenario" key from
          :attr:`.core.Config.dest_scenario` is appended; if this is not set, then
          either "policy" (if :attr:`.transport.Config.policy` is set) or "baseline".
        """
        if context.core.dest:
            raise NotImplementedError
            # Value from --dest CLI option
            # TODO Check that this works if a version # is specified
            return f"{context.dest} {self.label or ''}".strip()
        else:
            # Model name
            model_name = (
                "MESSAGEix-GLOBIOM 1.1-"
                + ("MT" if "material" in self.modules else "T")
                + f"-{context.model.regions} "
                # Append value from --model-extra CLI option
                + context.core.dest_scenario.get("model", "")
            ).rstrip()

            # Scenario name
            scenario_name = (
                f"{self.label or ''} "
                # Append value from --scenario-extra CLI option
                + (
                    context.core.dest_scenario.get("scenario")
                    or ("" if self.policy else "baseline")
                )
            ).rstrip()
            # Strip leading "M ", which is reflected in `model_name`
            scenario_name = re.sub("^M ", "", scenario_name)

            return f"{model_name}/{scenario_name}"

    def set_futures_scenario(self, value: str | None) -> None:
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

    def set_navigate_scenario(self, value: str | None) -> None:
        """Update :attr:`project` from a string representing a NAVIGATE scenario.

        See :meth:`ScenarioFlags.parse_navigate`.
        """
        if value is None:
            return

        s = NAVIGATE_SCENARIO.parse(value)
        self.project.update(navigate=s)
        self.check()

    def use_modules(self, *module_names: str) -> None:
        """Handle extra_modules."""
        for entry in module_names:
            for m in entry.split() if isinstance(entry, str) else entry:
                if m.startswith("-"):
                    # Remove a module
                    try:
                        self.modules.remove(m[1:])
                    except ValueError:
                        pass
                else:
                    self.modules.append(m)


@dataclass
class ScenarioCodeAnnotations(AnnotationsMixIn):
    """Set of annotations appearing on each Code in ``CL_TRANSPORT_SCENARIO``.

    See :attr:`.Config.code`.
    """

    #: The URN of a code identifying the SSP scenario to be used for sociodemographic
    #: data, for instance
    #: "urn:sdmx:org.sdmx.infomodel.codelist.Code=ICONICS:SSP(2024).1".
    SSP_URN: str

    #: :data:`True` if the scenario is a "Low Energy Demand" scenario.
    is_LED_scenario: bool

    #: URN of a code from :class:`.digsy.structure.SCENARIO`.
    DIGSY_scenario_URN: str

    #: URN of a code from :class:`.edits.structure.SCENARIO`.
    EDITS_scenario_URN: str

    #: :mod:`ixmp` URL of a base scenario on which the MESSAGEix-Transport scenario is
    #: to be built.
    base_scenario_URL: str

    #: Entries for :attr:`.Config.policy`.
    policy: "Policy | None"

    #: Entries for :attr:`.Config.extra_modules`.
    extra_modules: list[str] = field(default_factory=list)

    @classmethod
    def from_obj(cls, obj, globals=None):
        globals = (globals or {}) | dict(
            TaxEmission=TaxEmission,
            ExogenousEmissionPrice=ExogenousEmissionPrice,
        )
        return super().from_obj(obj, globals=globals)


class CL_SCENARIO(StructureFactory["common.Codelist"]):
    """SDMX code list ``IIASA_ECE:CL_TRANSPORT_SCENARIO``.

    This code lists contains unique IDs for scenarios supported by the
    MESSAGEix-Transport workflow (:mod:`.transport.workflow`). Each code has the set
    of annotations described by :class:`ScenarioCodeAnnotations`.
    """

    urn = "IIASA_ECE:CL_TRANSPORT_SCENARIO"
    version = "1.3.0"

    #: - Model name:
    #:   - 2024-11-25: use _v1.1 per a Microsoft Teams message.
    #:   - 2025-02-20: update to _v2.1 per discussion with OF. At this point _v2.3 is
    #:     the latest appearing in the database.
    #:   - 2025-05-05: update to _v5.0.
    #:   - 2025-06-24: update to _v6.1.
    #: - The scenario names appear to form a sequence from "baseline_DEFAULT" to
    #:   "baseline_DEFAULT_step_15" and finally "baseline". The one used below is the
    #:   latest in this sequence for which y₀=2020, rather than 2030.
    base_url = "ixmp://ixmp-dev/SSP_SSP{}_v6.1/baseline_DEFAULT_step_13"

    @classmethod
    def create(cls) -> "common.Codelist":
        from sdmx.model import common

        import message_ix_models.project.digsy.structure
        import message_ix_models.project.edits.structure
        from message_ix_models.util.sdmx import read

        # Other data structures
        IIASA_ECE = read("IIASA_ECE:AGENCIES")["IIASA_ECE"]
        cl_ssp_2024 = read("ICONICS:SSP(2024)")
        cl_edits = message_ix_models.project.edits.structure.get_cl_scenario()
        cl_digsy = message_ix_models.project.digsy.structure.get_cl_scenario()

        cl: "common.Codelist" = common.Codelist(
            id="CL_TRANSPORT_SCENARIO",
            maintainer=IIASA_ECE,
            version=cls.version,
            is_external_reference=False,
            is_final=True,
        )

        def _append_code(
            id: str,
            name: str,
            ssp: str,
            led: bool = False,
            edits: str = "_Z",
            digsy: str = "_Z",
            policy=None,
        ) -> None:
            """Shorthand for creating a code."""
            for modules, id_prefix, name_suffix in (
                ([], "", ""),
                (["material"], "M ", " with materials"),
            ):
                sca = ScenarioCodeAnnotations(
                    cl_ssp_2024[ssp].urn,  # Expand e.g. "1" to a full URN
                    led,
                    cl_digsy[digsy].urn,
                    cl_edits[edits].urn,
                    cls.base_url.format(ssp),  # Format base scenario URL
                    policy,
                    modules,
                )
                code = common.Code(
                    id=id_prefix + id,
                    name=name + name_suffix,
                    **sca.get_annotations(dict),
                )
                cl.append(code)

        # Baselines and policy scenarios for each SSP
        te = TaxEmission(1000.0)
        for ssp in "12345":
            id_ = name = f"SSP{ssp}"
            _append_code(id_, name + " baseline", ssp)

            # Simple carbon tax
            _append_code(id_ + " tax", name + " with tax", ssp, policy=te)

            # PRICE_EMISSION from exogenous data file
            for eep, hash in iter_price_emission("R12", f"SSP{ssp}"):
                name += " with exogenous price"
                _append_code(f"{id_} exo price {hash}", name, ssp, policy=eep)

        # LED
        name = "Low Energy Demand/High-with-Low scenario with SSP{} demographics"
        for ssp in "12":
            _append_code(f"LED-SSP{ssp}", name.format(ssp), ssp, led=True)

        # DIGSY
        ssp, name = "2", "DIGSY {!r} scenario with SSP2"
        for id_ in ("BEST-C", "BEST-S", "WORST-C", "WORST-S"):
            _append_code(f"DIGSY-{id_}", name.format(id_), ssp, digsy=id_)

            # PRICE_EMISSION from exogenous data file
            for eep, hash in iter_price_emission("R12", f"SSP{ssp}"):
                _append_code(
                    f"DIGSY-{id_} exo price {hash}",
                    name.format(id_) + " with exogenous price",
                    ssp,
                    policy=eep,
                )

        # EDITS
        ssp, name = "2", "EDITS scenario with ITF PASTA {!r} activity"
        for id_ in ("CA", "HA"):
            _append_code(f"EDITS-{id_}", name.format(id_), ssp, edits=id_)

        return cl


def iter_price_emission(
    regions: str, ssp_or_led: str
) -> Iterator[tuple[ExogenousEmissionPrice, str]]:
    """Iterate over available data in :file:`transport/{regions}/price-emission/`.

    Yields 2-tuple, similar to :meth:`.ScenarionInfo.from_path`:

    1. :class:`ExogenousEmissionPrice` with the scenario URL matching the filename.
    2. A 4-character hash of the scenario URL.

    Only files with paths/model names containing ``SSP{ssp_or_led}`` are returned; all
    others are skipped.
    """
    # TODO Integrate some or all of this functionality with the PRICE_EMISSION class

    base_dir = package_data_path("transport", regions, "price-emission")
    model_pattern = r"SSP_(?P<ssp_or_led>SSP[12345]|LED)_v(?P<model_version>[\d\.]+)"

    for path in base_dir.glob("*.csv"):
        info, groups = ScenarioInfo.from_path(path, model_pattern=model_pattern)
        if groups["ssp_or_led"] != ssp_or_led:
            continue

        yield (
            ExogenousEmissionPrice(f"ixmp://ixmp-dev/{info.url}"),
            short_hash(info.url, 4),
        )
