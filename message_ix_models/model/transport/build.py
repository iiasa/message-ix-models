"""Build MESSAGEix-Transport on a base model."""

import logging
from functools import partial
from importlib import import_module
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import genno
import pandas as pd
from genno import Computer, KeyExistsError, quote
from message_ix import Scenario

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model import bare, build
from message_ix_models.model.structure import get_codelist
from message_ix_models.util import minimum_version
from message_ix_models.util._logging import mark_time
from message_ix_models.util.graphviz import HAS_GRAPHVIZ

from . import Config
from .structure import get_technology_groups

if TYPE_CHECKING:
    import pathlib


log = logging.getLogger(__name__)


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
        c.add(
            debug_keys[-1],
            "write_report_debug",
            key,
            output_dir.joinpath(f"{stem}.csv"),
        )

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
    from . import (
        data,  # noqa: F401
        key,
    )
    from .files import FILES, add

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
        k, *_ = prepare_computer(context, c, "ADVANCE", source_kw=kw, strict=False)
        # Broadcast to R12
        c.add(f"{n}:n:advance", "broadcast_advance", k, "y0", "config")

    # Alias for other computations which expect the upper-case name
    c.add("MERtoPPP:n-y", "mertoppp:n-y")

    # FIXME Ensure the latter case for a simulated solution
    # if key.GDP in c:
    if False:
        pass  # Solved scenario that already has this key
    else:
        c.add(key.GDP, keys["GDP"][0])

    # Ensure correct units
    c.add("population:n-y", "mul", "pop:n-y", genno.Quantity(1.0, units="passenger"))

    # FIXME Adjust to derive PRICE_COMMODITY c=transport from solved scenario with
    #       MESSAGEix-Transport detail, then uncomment the following line
    # if key.price.base - "transport" in c:
    if False:
        # Alias PRICE_COMMODITY:… to PRICE_COMMODITY:*:transport, e.g. solved scenario
        # that already has this key
        c.add(key.price[0], key.price.base - "transport")
    else:
        # Not solved scenario → dummy prices
        c.add(key.price[0], "dummy_prices", keys["GDP"][0], sums=True)

    # Data from files

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


#: :mod:`genno` tasks for model structure information that are 'static'—that is, do not
#: change based on :class:`~.transport.config.Config` settings. See
#: :func:`add_structure`.
#:
#: These include:
#:
#: - ``info``: :attr:`transport.Config.base_model_info
#:   <transport.config.Config.base_model_info>`, an instance of :class:`.ScenarioInfo`.
#: - ``transport info``: the logical union of
#:   :attr:`~.transport.config.Config.base_model_info` and the :attr:`.Spec.add` member
#:   of :attr:`Config.spec <.transport.config.Config.spec>`. This includes
#:   all set elements that will be present in the build model.
#: - ``dry_run``: :attr:`.Config.dry_run`.
#: - ``e::codelist``: :func:`.get_codelist` for :ref:`emission-yaml`.
#: - ``groups::iea to transport``, ``groups::transport to iea``, ``indexers::iea to
#:   transport``: the 3 outputs of :func:`.groups_iea_eweb`, for use with IEA Extended
#:   World Energy Balances data.
#: - ``n::ex world``: |n| as :class:`list` of :class:`str`, excluding "World". See
#:   :func:`.nodes_ex_world`.
#: - ``n::ex world+code``: |n| as :class:`list`` of :class:`.Code`, excluding "World".
#: - ``n:n:ex world``: a 1-dimensional :class:`.Quantity` for broadcasting (values all
#:   1).
#: - ``nl::world agg``: :class:`dict` mapping to aggregate "World" from individual |n|.
#:   See :func:`.nodes_world_agg`.
STRUCTURE_STATIC = (
    ("info", lambda c: c.transport.base_model_info, "context"),
    (
        "transport info",
        lambda c: c.transport.base_model_info | c.transport.spec.add,
        "context",
    ),
    ("dry_run", lambda c: c.core.dry_run, "context"),
    ("e::codelist", partial(get_codelist, "emission")),
    ("groups::iea eweb", "groups_iea_eweb", "t::transport"),
    ("groups::iea to transport", itemgetter(0), "groups::iea eweb"),
    ("groups::transport to iea", itemgetter(1), "groups::iea eweb"),
    ("indexers::iea to transport", itemgetter(2), "groups::iea eweb"),
    ("n::ex world", "nodes_ex_world", "n"),
    (
        "n:n:ex world",
        lambda n: genno.Quantity([1.0] * len(n), coords={"n": n}),
        "n::ex world",
    ),
    ("n::ex world+code", "nodes_ex_world", "nodes"),
    ("nl::world agg", "nodes_world_agg", "config"),
)


def add_structure(c: Computer) -> None:
    """Add tasks to `c` for structures required by :mod:`.transport.build`.

    These include:

    - The following keys *only* if not already present in `c`. If, for example, `c` is
      a :class:`.Reporter` prepared from an already-solved :class:`.Scenario`, the
      existing tasks referring to the Scenario contents are not changed.

      - ``n``: |n| as :class:`list` of :class:`str`.
      - ``y``: |y| in the base model.
      - ``cat_year``: simulated data structure for "cat_year" with at least 1 row
        :py:`("firstmodelyear", y0)`.
      - ``y::model``: |y| within the model horizon as :class:`list` of :class:`int`.
      - ``y0``: The first model period, :class:`int`.

    - All tasks from :data:`STRUCTURE_STATIC`.
    - ``c::transport``: the |c| set of the :attr:`~.Spec.add` member of
      :attr:`Config.spec <.transport.config.Config.spec>`, transport commodities to be
      added.
    - ``c::transport+base``: all |c| that will be present in the build model
    - ``cg``: "consumer group" set elements.
    - ``indexers:cg``: ``cg`` as indexers.
    - ``nodes``: |n| in the base model.
    - ``indexers:scenario``: :class:`dict` mapping "scenario" to the short form of
      :attr:`Config.ssp <.transport.config.Config.ssp>` (for instance, "SSP1"), for
      indexing.
    - ``t::transport``: all transport |t| to be added, :class:`list`.
    - ``t::transport agg``: :class:`dict` mapping "t" to the output of
      :func:`.get_technology_groups`. For use with operators like 'aggregate', 'select',
      etc.
    - ``t::transport all``: :class:`dict` mapping "t" to ``t::transport``.
      .. todo:: Choose a more informative key.
    - ``t::transport modes``: :attr:`Config.demand_modes
      <.transport.config.Config.demand_modes>`.
    - ``t::transport modes 0``: :class:`dict` mapping "t" to the keys only from
      ``t::transport agg``. Use with 'aggregate' to produce the sum across modes,
      including "non-LDV".
    - ``t::transport modes 1``: same as ``t::transport modes 0`` except excluding
      "non-ldv".
    - ``t::RAIL`` etc.: transport |t| in the "RAIL" mode/group as :class:`list` of
      :class:`str`. See :func:`.get_technology_groups`.
    - ``t::transport RAIL`` etc.: :class:`dict` mapping "t" to the elements of
      ``t::RAIL``.
    - All of the keys in :data:`.bcast_tcl` and :data:`.bcast_y`.
    """
    from ixmp.report import configure

    from . import key
    from .operator import broadcast_t_c_l, broadcast_y_yv_ya

    # Retrieve configuration and other information
    config: "Config" = c.graph["context"].transport  # .model.transport.Config object
    info = config.base_model_info  # ScenarioInfo describing the base scenario
    spec = config.spec  # Specification for MESSAGEix-Transport structure to be built
    t_groups = get_technology_groups(spec)  # Technology groups/hierarchy

    # Update RENAME_DIMS with transport-specific concepts/dimensions. This allows to use
    # genno.operator.load_file(…, dims=RENAME_DIMS) in add_exogenous_data()
    # TODO Read from a concept scheme or list of dimensions
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

    # Tasks only to be added if not already present in `c`. These must be done
    # separately because add_queue does not support the strict/pass combination.
    for task in (
        ("n", quote(list(map(str, info.set["node"])))),
        ("y", quote(info.set["year"])),
        (
            "cat_year",
            pd.DataFrame([["firstmodelyear", info.y0]], columns=["type_year", "year"]),
        ),
        (key.y, "model_periods", "y", "cat_year"),
        ("y0", itemgetter(0), "y::model"),
    ):
        try:
            c.add(*task, strict=True)
        except KeyExistsError:  # Already present
            # log.debug(f"Use existing {c.describe(task[0])}")
            pass

    # Assemble a queue of tasks
    # - `Static` tasks
    # - Single 'dynamic' tasks based on config, info, spec, and/or t_groups
    # - Multiple static and dynamic tasks generated in loops etc.
    tasks: list[tuple] = list(STRUCTURE_STATIC) + [
        ("c::transport", quote(spec.add.set["commodity"])),
        ("c::transport+base", quote(spec.add.set["commodity"] + info.set["commodity"])),
        ("cg", quote(spec.add.set["consumer_group"])),
        ("indexers:cg", spec.add.set["consumer_group indexers"]),
        ("nodes", quote(info.set["node"])),
        ("indexers:scenario", quote(dict(scenario=repr(config.ssp).split(":")[1]))),
        ("t::transport", quote(spec.add.set["technology"])),
        ("t::transport agg", quote(dict(t=t_groups))),
        ("t::transport all", quote(dict(t=spec.add.set["technology"]))),
        (key.t_modes, quote(config.demand_modes)),
        ("t::transport modes 0", quote(dict(t=list(t_groups.keys())))),
        (
            "t::transport modes 1",
            quote(dict(t=list(filter(lambda k: k != "non-ldv", t_groups.keys())))),
        ),
    ]

    # Quantities for broadcasting (t,) to (t, c, l) dimensions
    tasks.extend(
        (
            getattr(key.bcast_tcl, kind),
            partial(broadcast_t_c_l, kind=kind, default_level="final"),
            "t::transport",
            "c::transport+base",
        )
        for kind in ("input", "output")
    )

    # Quantities for broadcasting y to (yv, ya)
    for k, base, method in (
        (key.bcast_y.all, "y", "product"),  # All periods
        (key.bcast_y.model, "y::model", "product"),  # Model periods only
        (key.bcast_y.no_vintage, "y::model", "zip"),  # Model periods with no vintaging
    ):
        tasks.append((k, partial(broadcast_y_yv_ya, method=method), base, base))

    # Groups of technologies and indexers
    # FIXME Combine or disambiguate these keys
    for id, techs in t_groups.items():
        tasks += [
            # Indexer-form of technology groups
            (f"t::transport {id}", quote(dict(t=techs))),
            # List form of technology groups
            (f"t::{id}", quote(techs)),
        ]

    # - Change each task from single-tuple form to (args, kwargs) with strict=True.
    # - Add all to the Computer, making 2 passes.
    c.add_queue(map(lambda t: (t, dict(strict=True)), tasks), max_tries=2, fail="raise")


@minimum_version("message_ix 3.8")
def get_computer(
    context: Context,
    obj: Optional[Computer] = None,
    *,
    visualize: bool = True,
    **kwargs,
) -> Computer:
    """Return a :class:`genno.Computer` set up for model-building calculations.

    The returned computer contains:

    - Everything added by :func:`.add_structure`, :func:`.add_exogenous_data`, and
      :func:`.add_debug`.
    - For each module in :attr:`.transport.config.Config.modules`, everything added by
      the :py:`prepare_computer()` function in that module.
    - ``context``: a reference to `context`.
    - ``scenario``: a reference to a Scenario, if one appears in `kwargs`.
    - ``add transport data``: a list of keys which, when computed, will cause all
      transport data to be computed and added to ``scenario``.

    Parameters
    ----------
    obj :
       If `obj` is an existing :class:`.Computer` (or subclass, such as
       :class`.Reporter`), tasks are added the existing tasks in its graph. Otherwise, a
       new Computer is created and populated.
    visualize :
       If :any:`True` (the default), a file :file:`transport/build.svg` is written in
       the local data directory with a visualization of the ``add transport data`` key.
    """
    from . import key, operator

    # Configure
    config = Config.from_context(context, **kwargs)

    # Structure information for the base model
    scenario = kwargs.get("scenario")
    if scenario:
        # Retrieve structure information from an existing base model/`scenario`
        config.base_model_info = ScenarioInfo(scenario)

        config.with_scenario = True
        config.with_solution = scenario.has_solution()
    else:
        # Generate a Spec/ScenarioInfo for a non-existent base model/`scenario` as
        # described by `context`
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
    c.add(key.report.all, [])  # Needed by .plot.prepare_computer()

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

    if visualize and HAS_GRAPHVIZ:
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
