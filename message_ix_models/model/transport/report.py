"""Reporting/postprocessing for MESSAGEix-Transport."""

import logging
import re
from copy import deepcopy
from typing import TYPE_CHECKING

import pandas as pd
from genno import Computer, Key, Keys, MissingKeyError
from genno.core.key import single_key
from message_ix import Reporter

from message_ix_models import Context, ScenarioInfo
from message_ix_models.report import STAGE, add_plots
from message_ix_models.report.util import add_replacements

from . import Config, key, plot

if TYPE_CHECKING:
    from message_ix_models import Spec

log = logging.getLogger(__name__)


#: Units for final energy. This *exact* value (and not e.g. "EJ / year") is required for
#: the legacy reporting to properly handle the result.
_FE_UNIT = "EJ/yr"


CONVERT_IAMC = (
    # NB these are currently tailored to produce the variable names expected for the
    #    NGFS project
    dict(
        variable="transport activity",
        base="out:nl-t-ya-c:transport+units",
        var=["Energy Service|Transportation", "t", "c"],
        sums=["c", "t", "c-t"],
    ),
    dict(
        variable="transport stock",
        base="CAP:nl-t-ya:ldv+units",
        var=["Transport|Stock|Road|Passenger|LDV", "t"],
        unit="Mvehicle",
    ),
    dict(
        variable="transport sales",
        base="CAP_NEW:nl-t-yv:ldv+units",
        var=["Transport|Sales|Road|Passenger|LDV", "t"],
        unit="Mvehicle",
    ),
    # Final energy
    #
    # The following are 4 different partial sums of in::transport, in which
    # individual technologies are already aggregated to modes
    dict(
        variable="transport fe",
        base="in:nl-t-ya-c:transport+units",
        var=["Final Energy|Transportation", "t", "c"],
        sums=["c", "t", "c-t"],
        unit=_FE_UNIT,
    ),
    dict(
        variable="transport fe ldv",
        base="in:nl-t-ya-c:ldv+units",
        var=["Final Energy|Transportation|Road|Passenger|LDV", "t", "c"],
        unit="EJ/yr",
    ),
    # Emissions using MESSAGEix emission_factor parameter
    # base: auto-sum over dimensions yv, m, h
    # var: Same as in data/report/global.yaml
    # dict(
    #     variable="transport emi 0",
    #     base="emi:nl-t-ya-e-gwp metric-e equivalent:gwpe+agg",
    #     var=[
    #         "Emissions|CO2|Energy|Demand|Transportation",
    #         "t",
    #         "e",
    #         "e equivalent",
    #         "gwp metric",
    #     ],
    # ),
    # dict(
    #     variable="transport emi 1",
    #     base="emi:nl-t-ya-e:transport+units",
    #     var=["Emissions", "e", "Energy|Demand|Transportation", "t"],
    #     sums=["t"],
    #     unit="Mt/yr",
    # ),
    #
    # # For debugging
    # dict(variable="debug ACT", base="ACT:nl-t-ya", var=["DEBUG", "t"], unit="-"),
    # dict(variable="debug CAP", base="CAP:nl-t-ya", var=["DEBUG", "t"], unit="-"),
    # dict(
    #     variable="debug CAP_NEW", base="CAP_NEW:nl-t-yv", var=["DEBUG", "t"], unit="-"
    # ),
)


#: Quantities in which to select transport technologies only. See
#: :func:`select_transport_techs`.
SELECT = [
    "CAP_NEW",
    "CAP",
    "fix_cost",
    "historical_new_capacity",
    "in",
    "input",
    "inv_cost",
    "out",
    "var_cost",
]


# TODO Type c as (string) "Computer" once genno supports this
def add_iamc_store_write(c: Computer, base_key) -> "Key":
    """Write `base_key` to CSV, XLSX, and/or both; and/or store on "scenario".

    If `base_key` is, for instance, "foo::iamc", this function adds the following keys:

    - "foo::iamc+all": both of:

      - "foo::iamc+file": both of:

        - "foo::iamc+csv": write the data in `base_key` to a file named :file:`foo.csv`.
        - "foo::iamc+xlsx": write the data in `base_key` to a file named
          :file:`foo.xlsx`.

        The files are created in a subdirectory using :func:`make_output_path`—that is,
        including a path component given by the scenario URL.

      - "foo::iamc+store" store the data in `base_key` as time series data on the
        scenario identified by the key "scenario".

    .. todo:: Move upstream, to :mod:`message_ix_models`.
    """
    k = Key(base_key)

    file_keys = []
    for suffix in ("csv", "xlsx"):
        # Create the path
        path = c.add(
            k[f"{suffix} path"], "make_output_path", "config", name=f"{k.name}.{suffix}"
        )
        # Write `key` to the path
        file_keys.append(c.add(k[suffix], "write_report", base_key, path))

    # Write all files
    c.add(k["file"], file_keys)

    # Store data on "scenario"
    c.add(k["store"], "store_ts", "scenario", base_key)

    # Both write and store
    return single_key(c.add(k["all"], [k["file"], k["store"]]))


def aggregate(c: "Computer") -> None:
    """Aggregate individual transport technologies to modes."""
    from genno.operator import aggregate as func

    config: Config = c.graph["config"]["transport"]

    for k in map(lambda s: Key(c.infer_keys(s)), "emi in out".split()):
        try:
            # Reference the function to avoid the genno magic which would treat as sum()
            # NB aggregation on the nl dimension *could* come first, but this can use a
            #    lot of memory when applied to e.g. out:*: for a full global model.
            c.add(k[0], func, k, "t::transport agg", keep=False)
            c.add(k[1], func, k[0], "nl::world agg", keep=False)
            c.add(k["transport"], "select", k[1], "t::transport modes 1", sums=True)
        except MissingKeyError:
            if config.with_solution:
                raise


def callback(rep: Reporter, context: Context) -> None:
    """:meth:`.prepare_reporter` callback for MESSAGEix-Transport.

    Among others, adds:

    - ``{in,out}::transport``: with outputs aggregated by technology group or
      "mode".
    - ``transport plots``: the plots from :mod:`.transport.plot`.

      If the scenario to be reported is not solved, only a subset of plots are added.
    - :data:`.key.report.all`: all of the above.
    """
    from . import base, build

    N_keys = len(rep.graph)

    # - Configure MESSAGEix-Transport.
    # - Add structure and other information.
    # - Call, inter alia:
    #   - demand.prepare_computer() for ex-post mode and demand calculations.
    check = build.get_computer(
        context, obj=rep, visualize=False, scenario=rep.graph.get("scenario")
    )

    assert check is rep  # Same `rep` was returned

    if False:
        log.info("Filter out non-transport technologies")

        # Plain "transport" from the base model, for e.g. prices
        t_filter = {"transport"}
        # MESSAGEix-Transport -specific technologies
        t_filter.update(map(str, rep.get("t::transport").copy()))
        # # Required commodities (e.g. fuel) from the base model
        # t_filter.update(spec.require.set["commodity"])

        rep.set_filters(t=sorted(t_filter))

    # Configure replacements for conversion to IAMC data structure
    add_replacements("t", context.transport.spec.add.set["technology"])

    # Apply some functions that prepare further tasks. Order matters here.
    aggregate(rep)
    select_transport_techs(rep)
    reapply_units(rep)
    misc(rep)
    convert_iamc(rep)  # Adds to key.report.all
    convert_sdmx(rep)  # Adds to key.report.all
    add_plots(rep, plot, key.report.plot)
    base.prepare_reporter(rep)  # Tasks that prepare data to parametrize the base model

    log.info(f"Added {len(rep.graph) - N_keys} keys")
    # TODO Write an SVG visualization of reporting calculations


def check(scenario):
    """Check that the transport model solution is complete.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario with solution.

    Returns
    -------
    pd.Series
        Index entries are str descriptions of checks. Values are :obj:`True` if the
        respective check passes.
    """
    # NB this is here to avoid circular imports
    from message_ix_models.report import prepare_reporter, register

    register(callback)
    rep, key = prepare_reporter(scenario, "global.yaml", "transport check")
    return rep.get(key)


def configure_legacy_reporting(config: dict) -> None:
    """Callback to configure the legacy reporting.

    .. warning:: This requires code changes from :pull:`254` that are not yet merged to
       :mod:`message_ix_models` ``main``, particularly the variable
       :py:`default_tables.COMMODITY`. The PR (or part of it) must be completed in order
       to use this function.
    """
    from message_ix_models.report.legacy.default_tables import (  # type: ignore [attr-defined]
        COMMODITY,
    )

    # NB the legacy reporting doesn't pass a context object to the hook that calls this
    #    function, so get an instance directly
    context = Context.get_instance()

    # If it does not already exist, read transport configuration onto the Context,
    # including reporting config
    context.setdefault("transport", Config.from_context(context))

    # Get a spec
    spec: "Spec" = context.transport.spec

    # Commented: pp_utils._retr_act_data() raises IndexError if lists are empty
    # # Clear existing entries
    # # NB it should not have any effect to leave these in
    # for key in config:
    #     if key.startswith("trp "):
    #         # log.debug(f"Discard '{key}': {config[key]}")
    #         config[key] = []

    # Iterate over technologies in the transport model spec
    for t in spec.add.set["technology"]:
        try:
            # Retrieve the input commodity for this technology
            commodity = t.eval_annotation("input")["commodity"]
        except (TypeError, KeyError):  # No annotation, or no "commodity" info
            commodity = None
        else:
            # Map to the shorthands used in legacy reporting
            commodity = COMMODITY.get(commodity)

        if commodity is None:
            # log.debug(f"{t}: No legacy reporting")  # Verbose
            continue

        group = f"trp {commodity}"
        # log.debug(f"{t} → '{group}'")
        config[group].append(t.id)


def convert_iamc(c: "Computer") -> None:
    """Add tasks from :data:`.CONVERT_IAMC`."""
    from message_ix_models.report import iamc as handle_iamc
    from message_ix_models.report import util

    from .key import report as k_report

    util.REPLACE_VARS.update({r"^CAP\|(Transport)": r"\1"})

    keys = []
    for info in CONVERT_IAMC:
        handle_iamc(c, deepcopy(info))
        keys.append(f"{info['variable']}::iamc")

    # Concatenate IAMC-format tables
    k = Key("transport", tag="iamc")
    c.add(k, "concat", *keys)

    # Add tasks for writing IAMC-structured data to file and storing on the scenario
    c.apply(add_iamc_store_write, k)

    c.graph[k_report.all].append(
        # Use ths line to both store and write to file IAMC structured-data
        k + "all"
        # Use this line for "transport::iamc+file" instead of "transport::iamc+all"
        # k + " file"
    )


def convert_sdmx(c: "Computer") -> None:
    """Add tasks to convert data to SDMX."""
    from sdmx.message import StructureMessage

    from message_ix_models.util.sdmx import DATAFLOW, Dataflow

    from .key import report as k_report
    from .operator import write_sdmx_data

    # Directory for SDMX output
    dir_ = "dir::transport sdmx"
    c.add(dir_, "make_output_path", "config", name="sdmx")

    # Add a key that returns a reference to a shared StructureMessage
    sm = "sdmx structure message"
    c.add(sm, StructureMessage)

    # Write each quantity in DATAFLOW to .{csv,xml}; update the shared StructureMessage
    keys = []
    for df in filter(lambda df: df.intent & Dataflow.FLAG.OUT, DATAFLOW.values()):
        keys.append(Key(df.key.name, tag="sdmx"))
        c.add(keys[-1], write_sdmx_data, df.key, sm, "scenario", dir_, df_urn=df.df.urn)

    # Collect all the keys *then* write the collected structures to file
    c.add(k_report.sdmx, "write_sdmx_structures", sm, dir_, *keys)

    # Connect to the main report key
    c.graph[k_report.all].append(k_report.sdmx)


def misc(c: "Computer") -> None:
    """Add miscellaneous tasks.

    Among others, these include:

    - ``calibrate fe`` → a file :file:`calibrate-fe.csv`. See the header comment.
    """
    config: "Config" = c.graph["config"]["transport"]

    # Configuration for :func:`check`. Adds a single key, 'transport check', that
    # depends on others and returns a :class:`pandas.Series` of :class:`bool`.
    c.add("transport check", "transport_check", "scenario", "ACT:nl-t-yv-va-m-h")

    # Exogenous data
    c.add("distance:nl:non-ldv", "distance_nonldv", "config")

    # Demand per capita
    c.add("demand::capita", "divdemand:n-c-y", key.pop)

    # Adjustment factor for LDV calibration: fuel economy ratio
    k_num = Key("in:nl-t-ya-c:transport+units") / "c"  # As in CONVERT_IAMC
    k_denom = Key("out:nl-t-ya-c:transport+units") / "c"  # As in CONVERT_IAMC
    k_check = single_key(c.add("fuel economy::check", "div", k_num, k_denom))
    c.add(
        k_check + "sel",
        "select",
        k_check,
        indexers=dict(t="LDV", ya=config.base_model_info.y0),
        drop=True,
    )

    k_ratio = single_key(
        c.add("fuel economy::ratio", "div", key.exo.input_ref_ldv, k_check + "sel")
    )
    c.add("calibrate fe path", "make_output_path", "config", name="calibrate-fe.csv")
    hc = "\n\n".join(
        [
            "Calibration factor for LDV fuel economy",
            f"Ratio of ldv-fuel-economy-ref.csv\n      to ({k_num} / {k_denom})",
            "Units: dimensionless\n",
        ]
    )
    c.add(
        "calibrate fe",
        "write_report",
        k_ratio,
        "calibrate fe path",
        kwargs=dict(header_comment=hc),
    )


def multi(context: Context, targets: list[str], *, use_platform: bool = False) -> None:
    """Report outputs from multiple scenarios."""

    from message_ix_models.tools.iamc import to_quantity

    k = Keys(
        in_="in::pd",
        concat="concat::pd",
        all0="all:n-SCENARIO-UNIT-VARIABLE-y",
        all1="all:n-s-UNIT-v-y",
        plot_data="plot data:n-s-y",
    )

    # Computer, with configuration expected by Plot.add_tasks
    c = Computer(config=dict(output_dir=context.get_local_path("report")))
    # Order is important: use genno.compat.pyam.operator.quantity_from_iamc over local
    c.require_compat("message_ix_models.report.operator")
    c.require_compat("pyam")

    c.add("context", context)

    # Retrieve all data for each of the `targets` as pd.DataFrame
    kw0 = dict(filename="transport.csv", use={"file"})
    for i, info in enumerate(map(ScenarioInfo.from_url, targets)):
        c.add(k.in_[i], "latest_reporting", "context", info, **kw0)

    # Concatenate these together
    c.add(k.concat, pd.concat, list(k.in_.generated))

    # Change e.g. "SSP_2024.1 exo price baseline" to "SSP_2024.1 exo price"
    # FIXME Address this in .transport.workflow
    replace = dict(SCENARIO={re.compile("([^12345]) baseline"): r"\1"})

    # Convert to genno.Quantity
    kw1 = dict(non_iso_3166="keep", replace=replace, unique="MODEL")
    c.add(k.all0, to_quantity, k.concat, **kw1)

    # Rename dimensions
    c.add(k.all1, "rename_dims", k.all0, name_dict={"SCENARIO": "s", "VARIABLE": "v"})

    # Collect all plots
    add_plots(c, plot, "multi", stage=STAGE.REPORT, single=False)

    return c.get("multi")


def reapply_units(c: "Computer") -> None:
    """Apply units to transport quantities.

    :func:`.ixmp.report.operator.data_for_quantity` drops units for most data extracted
    from a MESSAGEix-GLOBIOM :class:`.Scenario`, because the data contain a mix of
    inconsistent units.

    Here, add tasks to reapply units to selected subsets of data that are guaranteed to
    have certain units.
    """
    # TODO Infer these values from technology.yaml etc.
    for base, (op, units) in {
        # Vehicle stocks
        # FIXME should not need the extra [vehicle] in the numerator
        "CAP:nl-t-ya:non-ldv": ("apply", "v**2 Tm / a"),
        "CAP:*:ldv": ("apply", "Mv"),
        "CAP_NEW:*:ldv": ("apply", "Mv"),
        # NB these units are correct for final energy only
        "in:*:transport": ("apply", "GWa / a"),
        "in:*:ldv": ("apply", "GWa / a"),
        "out:*:transport": ("apply", "Tm / a"),
        "out:*:ldv": ("apply", "Tm / a"),
        # Units of ACT are not carried, so must correct here:
        # - Add [time]: -1
        # - Remove [vehicle]: -1, [distance]: -1
        #
        # When run together with global.yaml reporting, emi:* is assigned units of
        # "Mt / year". Using apply_units() causes these to be *converted* to  kt/a, i.e.
        # increasing the magnitude; so use assign_units() instead.
        "emi:*:transport": ("assign", "kt / a"),
    }.items():
        key = c.infer_keys(base)
        c.add(key + "units", f"{op}_units", key, units=units, sums=True)


def select_transport_techs(c: "Computer") -> None:
    """Select subsets of transport technologies.

    Applied to the quantities in :data:`SELECT`.
    """
    # Infer the full dimensionality of each key to be selected
    for k in map(lambda name: c.infer_keys(f"{name}:*"), SELECT):
        c.add(k + "transport all", "select", k, "t::transport all", sums=True)
        c.add(k + "ldv", "select", k, "t::transport LDV", sums=True)
        c.add(k + "non-ldv", "select", k, "t::transport P ex LDV", sums=True)
