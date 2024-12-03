"""Build MESSAGEix-Transport on a base model."""

import logging
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
from genno import Computer, KeyExistsError, Quantity, quote
from message_ix import Scenario

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model import bare, build
from message_ix_models.util import minimum_version
from message_ix_models.util._logging import mark_time

from . import Config
from .structure import get_technology_groups

if TYPE_CHECKING:
    import pathlib

    from genno.types import AnyQuantity

log = logging.getLogger(__name__)


def write_report(qty: "AnyQuantity", path: Path, kwargs=None) -> None:
    """Similar to :func:`.genno.operator.write_report`, but include units.

    .. todo:: Move upstream, to :mod:`genno`.
    """
    from genno import operator

    from message_ix_models.util import datetime_now_with_tz

    kwargs = kwargs or dict()
    kwargs.setdefault(
        "header_comment",
        f"""`{qty.name}` data from MESSAGEix-Transport calibration.

Generated: {datetime_now_with_tz().isoformat()}

Units: {qty.units:~}
""",
    )

    operator.write_report(qty, path, kwargs)


def add_debug(c: Computer) -> None:
    """Add tasks for debugging the build."""
    from genno import Key, KeySeq

    from .key import gdp_cap, ms, pdt_nyt

    context: Context = c.graph["context"]
    config: Config = context.transport

    # Path to output file
    if config.with_scenario and config.with_solution:
        # Output to a directory corresponding to the Scenario URL
        label = c.graph["scenario"].url.replace("/", "_")
    else:
        # Output to a directory name constructed from settings
        # Remove ":" to be compatible with actions/upload-artifact
        ssp = str(config.ssp).replace(":", "_")
        label = f"{ssp}-{context.model.regions}-{context.model.years}"
    output_dir = context.get_local_path("transport", f"debug-{label}")
    output_dir.mkdir(exist_ok=True, parents=True)

    # Store in the config, but not at "output_dir" that is used by e.g. reporting
    c.graph["config"]["transport build debug dir"] = output_dir

    # FIXME Duplicated from base.prepare_reporter()
    e_iea = Key("energy:n-y-product-flow:iea")
    e_fnp = KeySeq(e_iea.drop("y"))
    e_cnlt = Key("energy:c-nl-t:iea+0")
    # Transform IEA EWEB data for comparison
    c.add(e_fnp[0], "select", e_iea, indexers=dict(y=2020), drop=True)
    c.add(e_fnp[1], "aggregate", e_fnp[0], "groups::iea to transport", keep=False)
    c.add(e_cnlt, "rename_dims", e_fnp[1], quote(dict(flow="t", n="nl", product="c")))

    # Write some intermediate calculations from the build process to file
    debug_keys = []
    for i, (key, stem) in enumerate(
        (
            (gdp_cap, "gdp-ppp-cap"),
            (pdt_nyt, "pdt"),
            (pdt_nyt + "capita+post", "pdt-cap"),
            (ms, "mode-share"),
            (e_fnp[0], "energy-iea-0"),
            (e_cnlt, "energy-iea-1"),
        )
    ):
        debug_keys.append(f"transport debug {i}")
        c.add(debug_keys[-1], write_report, key, output_dir.joinpath(f"{stem}.csv"))

    def _(*args) -> "pathlib.Path":
        """Do nothing with the computed `args`, but return `output_path`."""
        return output_dir

    debug_plots = (
        "demand-exo demand-exo-capita demand-exo-capita-gdp inv_cost"
        # FIXME The following currently don't work, as their required/expected input
        #       keys (from the post-solve/report step) do not exist in the build step
        # " var-cost fix-cost"
    ).split()

    c.add(
        "transport build debug",
        _,
        # NB To omit some or all of these calculations / plots from the debug outputs
        #    for individuals, comment 1 or both of the following lines
        *debug_keys,
        *[f"plot {p}" for p in debug_plots],
    )
    # log.info(c.describe("transport build debug"))

    # Also generate these debugging outputs when building the scenario
    c.graph["add transport data"].append("transport build debug")


def debug_multi(context: Context, *paths: Path) -> None:
    """Generate plots comparing data from multiple build debug directories."""
    from .plot import ComparePDT, ComparePDTCap0, ComparePDTCap1

    if isinstance(paths[0], Scenario):
        # Workflow was called with --from="…", so paths from the previous step are not
        # available; try to guess
        paths = sorted(
            filter(
                Path.is_dir, context.get_local_path("transport").glob("debug-ICONICS_*")
            )
        )

    c = Computer(config={"transport build debug dir": paths[0].parent})
    c.require_compat("message_ix_models.report.operator")

    for cls in (ComparePDT, ComparePDTCap0, ComparePDTCap1):
        key = c.add(f"compare {cls.basename}", cls, *paths)
        c.get(key)


def add_exogenous_data(c: Computer, info: ScenarioInfo) -> None:
    """Add exogenous data to `c` that mocks data coming from an actual Scenario.

    The specific quantities added are:

    - ``GDP:n-y``, from GEA, SSP, or SHAPE data; see :func:`.gdp_pop`.
    - ``PRICE_COMMODITY:n-c-y``, currently mocked based on the shape of ``GDP:n-y``
      using :func:`.dummy_prices`.

      .. todo:: Add an external data source.

    - ``MERtoPPP:n-y``, from :file:`mer-to-ppp.csv`. If ``context.model.regions`` is
      “R14”, data are adapted from R11 using :obj:`.adapt_R11_R14`.

    See also
    --------
    :doc:`/reference/model/transport/input`
    """
    # Ensure that the SSPOriginal and SSPUpdate data providers are available
    import message_ix_models.project.advance.data  # noqa: F401
    import message_ix_models.project.ssp.data  # noqa: F401
    import message_ix_models.tools.iea.web  # noqa: F401
    from message_ix_models.project.ssp import SSP_2017, SSP_2024
    from message_ix_models.tools.exo_data import prepare_computer

    # Ensure that the MERtoPPP data provider is available
    from . import data  # noqa: F401

    # Added keys
    keys = {}

    context = c.graph["context"]
    config: "Config" = c.graph["config"]["transport"]

    # Identify appropriate source keyword arguments for loading GDP and population data
    source = str(config.ssp)
    if config.ssp in SSP_2017:
        source_kw: tuple[dict[str, Any], ...] = (
            dict(measure="GDP", model="IIASA GDP"),
            dict(measure="POP", model="IIASA GDP"),
        )
    elif config.ssp in SSP_2024:
        source_kw = (
            dict(measure="GDP", model="IIASA GDP 2023"),
            dict(measure="POP"),
        )

    for kw in source_kw:
        keys[kw["measure"]] = prepare_computer(
            context, c, source, source_kw=kw, strict=False
        )

    # Add data for MERtoPPP
    kw = dict(measure="MERtoPPP", nodes=context.model.regions)
    prepare_computer(context, c, "transport MERtoPPP", source_kw=kw, strict=False)

    # Add IEA Extended World Energy Balances data; select only the flows related to
    # transport
    kw = dict(
        provider="IEA",
        edition="2024",
        flow=(
            "DOMESAIR DOMESNAV PIPELINE RAIL ROAD TOTTRANS TRNONSPE WORLDAV WORLDMAR"
        ).split(),
    )
    prepare_computer(context, c, "IEA_EWEB", source_kw=kw, strict=False)

    # Add IEA Future of Trucks data
    for kw in dict(measure=1), dict(measure=2):
        prepare_computer(context, c, "IEA Future of Trucks", source_kw=kw, strict=False)

    # Add ADVANCE data
    common = dict(model="MESSAGE", scenario="ADV3TRAr2_Base", aggregate=False)
    for n, m, u in (
        ("pdt ldv", "Transport|Service demand|Road|Passenger|LDV", "Gp km / a"),
        ("fv", "Transport|Service demand|Road|Freight", "Gt km"),
    ):
        # Add the base data
        kw = dict(measure=m, name=f"advance {n}")
        kw.update(common)
        key, *_ = prepare_computer(context, c, "ADVANCE", source_kw=kw, strict=False)
        # Broadcast to R12
        c.add(f"{n}:n:advance", "broadcast_advance", key, "y0", "config")

    # Alias for other computations which expect the upper-case name
    c.add("MERtoPPP:n-y", "mertoppp:n-y")
    try:
        c.add("GDP:n-y", "gdp:n-y", strict=True)
    except KeyExistsError as e:
        log.info(repr(e))  # Solved scenario that already has this key

    # Ensure correct units
    c.add("population:n-y", "mul", "pop:n-y", Quantity(1.0, units="passenger"))

    # Dummy prices
    try:
        c.add(
            "PRICE_COMMODITY:n-c-y",
            "dummy_prices",
            keys["GDP"][0],
            sums=True,
            strict=True,
        )
    except KeyExistsError as e:
        log.info(repr(e))  # Solved scenario that already has this key

    # Data from files
    from .files import FILES, add

    # Identify the mode-share file according to the config setting
    add(
        key="mode share:n-t:exo",
        path=("mode-share", config.mode_share),
        name="Reference (base year) mode share",
        units="dimensionless",
        replace=True,
    )

    for f in FILES:
        c.add("", f, context=context)


def add_structure(c: Computer):
    """Add keys to `c` for structures required by :mod:`.transport.build` computations.

    This uses :attr:`.transport.Config.base_model_info` and
    :attr:`.transport.Config.spec` to mock the contents that would be reported from an
    already-populated Scenario for sets "node", "year", and "cat_year". It also adds
    many other keys.
    """
    from operator import itemgetter

    from ixmp.report import configure

    config: "Config" = c.graph["context"].transport
    info = config.base_model_info  # Information about the base scenario
    spec = config.spec  # Specification for MESSAGEix-Transport structure

    # Update RENAME_DIMS with transport-specific concepts/dimensions. This allows to use
    # genno.operator.load_file(…, dims=RENAME_DIMS) in add_exogenous_data()
    # TODO move to a more appropriate location
    configure(
        rename_dims={
            "area_type": "area_type",
            "attitude": "attitude",
            "census_division": "census_division",
            "consumer_group": "cg",
            "driver_type": "driver_type",
            "vehicle_class": "vehicle_class",
        }
    )

    for key, *comp in (
        # Configuration
        ("info", lambda c: c.transport.base_model_info, "context"),
        (
            "transport info",
            lambda c: c.transport.base_model_info | c.transport.spec.add,
            "context",
        ),
        ("dry_run", lambda c: c.core.dry_run, "context"),
        # Structure
        ("c::transport", quote(spec.add.set["commodity"])),
        ("c::transport+base", quote(spec.add.set["commodity"] + info.set["commodity"])),
        ("cg", quote(spec.add.set["consumer_group"])),
        ("indexers:cg", spec.add.set["consumer_group indexers"]),
        ("n", quote(list(map(str, info.set["node"])))),
        ("nodes", quote(info.set["node"])),
        ("indexers:scenario", quote(dict(scenario=repr(config.ssp).split(":")[1]))),
        ("t::transport", quote(spec.add.set["technology"])),
        # Dictionary form for aggregation
        # TODO Choose a more informative key
        ("t::transport all", quote(dict(t=spec.add.set["technology"]))),
        ("t::transport modes", quote(config.demand_modes)),
        ("y", quote(info.set["year"])),
        (
            "cat_year",
            pd.DataFrame([["firstmodelyear", info.y0]], columns=["type_year", "year"]),
        ),
    ):
        try:
            c.add(key, *comp, strict=True)  # Raise an exception if `key` exists
        except KeyExistsError:
            continue  # Already present; don't overwrite

    # Create quantities for broadcasting (t,) to (t, c, l) dimensions
    for kind in "input", "output":
        c.add(
            f"broadcast:t-c-l:transport+{kind}",
            "broadcast_t_c_l",
            "t::transport",
            "c::transport+base",
            kind=kind,
            default_level="final",
        )

    # Retrieve information about the model structure
    t_groups = get_technology_groups(spec)

    # List of nodes excluding "World"
    # TODO move upstream, to message_ix
    c.add("n::ex world", "nodes_ex_world", "n")
    c.add(
        "n:n:ex world",
        lambda n: Quantity([1.0] * len(n), coords={"n": n}),
        "n::ex world",
    )
    c.add("n::ex world+code", "nodes_ex_world", "nodes")
    c.add("nl::world agg", "nodes_world_agg", "config")

    # Model periods only
    c.add("y::model", "model_periods", "y", "cat_year")
    c.add("y0", itemgetter(0), "y::model")

    # Quantities for broadcasting y to (yv, ya)
    for base, tag, method in (
        ("y", ":all", "product"),  # All periods
        ("y::model", "", "product"),  # Model periods only
        ("y::model", ":no vintage", "zip"),  # Model periods with no vintaging
    ):
        c.add(f"broadcast:y-yv-ya{tag}", "broadcast_y_yv_ya", base, base, method=method)

    # Mappings for use with aggregate, select, etc.
    c.add("t::transport agg", quote(dict(t=t_groups)))
    # Sum across modes, including "non-ldv"
    c.add("t::transport modes 0", quote(dict(t=list(t_groups.keys()))))
    # Sum across modes, excluding "non-ldv"
    c.add(
        "t::transport modes 1",
        quote(dict(t=list(filter(lambda k: k != "non-ldv", t_groups.keys())))),
    )

    # Groups of technologies and indexers
    for id, techs in t_groups.items():
        # FIXME Combine or disambiguate these keys
        # Indexer-form of technology groups
        c.add(f"t::transport {id}", quote(dict(t=techs)))
        # List form of technology groups
        c.add(f"t::{id}", quote(techs))

    # Mappings for use with IEA Extended World Energy Balances data
    c.add("groups::iea eweb", "groups_iea_eweb", "t::transport")
    # Unpack
    c.add("groups::iea to transport", itemgetter(0), "groups::iea eweb")
    c.add("groups::transport to iea", itemgetter(1), "groups::iea eweb")
    c.add("indexers::iea to transport", itemgetter(2), "groups::iea eweb")


@minimum_version("message_ix 3.8")
def get_computer(
    context: Context,
    obj: Optional[Computer] = None,
    *,
    visualize: bool = True,
    **kwargs,
) -> Computer:
    """Return a :class:`genno.Computer` set up for model-building calculations."""
    from . import operator

    # Configure
    config = Config.from_context(context, **kwargs)

    # Structure information for the base model
    scenario = kwargs.get("scenario")
    if scenario:
        config.base_model_info = ScenarioInfo(scenario)

        config.with_scenario = True
        config.with_solution = scenario.has_solution()
    else:
        base_spec = bare.get_spec(context)
        config.base_model_info = base_spec["add"]

        config.with_scenario = config.with_solution = False

    # Ensure that members of e.g. base_model_info.set["commodity"] are Code objects with
    # their respective annotations
    config.base_model_info.substitute_codes()

    # Create a Computer
    c = obj or Computer()

    # Require modules with operators
    c.require_compat("ixmp.report.operator")
    c.require_compat("message_ix.report.operator")
    c.require_compat("message_ix_models.report.operator")
    c.require_compat(operator)

    # Transfer data from `context` to "config" in the genno graph
    for k, v in {
        "regions": context.model.regions,
        "transport": context.transport,
        "data source": dict(),
        "output_dir": context.get_local_path(),
    }.items():
        c.graph["config"].setdefault(k, v)

    # Attach the context and scenario
    c.add("context", context)
    c.add("scenario", scenario)
    # Add a computation that is an empty list.
    # Individual modules's prepare_computer() functions can append keys.
    c.add("add transport data", [])

    # Add structure-related keys
    add_structure(c)
    # Add exogenous data
    add_exogenous_data(c, config.base_model_info)

    # For each module in transport.Config.modules, invoke the function
    # prepare_computer() to add further calculations
    for name in context.transport.modules:
        module = import_module(name if "." in name else f"..{name}", __name__)
        module.prepare_computer(c)

    # Add tasks for debugging the build
    add_debug(c)

    if visualize:
        path = context.get_local_path("transport", "build.svg")
        path.parent.mkdir(exist_ok=True)
        c.visualize(filename=path, key="add transport data")
        log.info(f"Visualization written to {path}")

    return c


def main(
    context: Context,
    scenario: Scenario,
    options: Optional[dict] = None,
    **option_kwargs,
):
    """Build MESSAGEix-Transport on `scenario`.

    See also
    --------
    add_data
    apply_spec
    get_spec
    """
    from .emission import strip_emissions_data
    from .util import sum_numeric

    # Check arguments
    options = dict() if options is None else options.copy()
    dupe = set(options.keys()) & set(option_kwargs.keys())
    if len(dupe):
        raise ValueError(f"Option(s) {repr(dupe)} appear in both `options` and kwargs")
    options.update(option_kwargs)

    # Use fast=True by default
    options.setdefault("fast", True)
    dry_run = options.pop("dry_run", False)

    log.info("Configure MESSAGEix-Transport")
    mark_time()

    # Set up a Computer for input data calculations. This also:
    # - Creates a Config instance
    # - Generates and stores context.transport.spec, i.e the specification of the
    #   MESSAGEix-Transport structure: required, added, and removed set items
    # - Prepares the "add transport data" key used below
    c = get_computer(context, scenario=scenario, options=options)

    def _add_data(s, **kw):
        assert s is c.graph["scenario"]
        result = c.get("add transport data")
        # For calls to add_par_data(), int() are returned with number of observations
        log.info(f"Added {sum_numeric(result)} total obs")

    if dry_run:
        return c.get("transport build debug")

    # First strip existing emissions data
    strip_emissions_data(scenario, context)

    # Apply the structural changes AND add the data
    log.info("Build MESSAGEix-Transport")
    build.apply_spec(scenario, context.transport.spec, data=_add_data, **options)

    # Required for time series data from genno reporting that is expected by legacy
    # reporting
    # TODO Include this in the spec, while not using it as a value for `node_loc`
    scenario.platform.add_region(f"{context.model.regions}_GLB", "region", "World")

    mark_time()

    scenario.set_as_default()
    log.info(f"Built {scenario.url} and set as default version")

    return scenario
