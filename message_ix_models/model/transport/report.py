"""Reporting/postprocessing for MESSAGEix-Transport."""

import logging
import re
from copy import deepcopy
from typing import TYPE_CHECKING

import pandas as pd
from genno import Computer, Key, Keys
from genno.core.key import single_key
from message_ix import Reporter

from message_ix_models import Context, ScenarioInfo
from message_ix_models.report import STAGE, add_plots
from message_ix_models.report.util import add_replacements

from . import Config, plot
from . import key as K

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Spec

log = logging.getLogger(__name__)


#: Units for final energy. This *exact* value (and not e.g. "EJ / year") is required for
#: the legacy reporting to properly handle the result.
_FE_UNIT = "EJ/yr"

#: Quantities to convert to IAMC. See :func:`convert_iamc`.
CONVERT_IAMC = (
    # NB these are currently tailored to produce the variable names expected for the
    #    NGFS project
    dict(
        variable="T activity",
        base="out:nl-t-ya-c:T",
        var=["Energy Service|Transportation", "t", "c"],
        sums=["c"],
    ),
    dict(
        variable="T stock",
        base="CAP:nl-t-ya:T",
        var=["Stocks|Transportation", "t"],
        unit="Mvehicle",
    ),
    dict(
        variable="T sales",
        base="CAP_NEW:nl-t-yv:T",
        var=["Sales|Transportation", "t"],
        unit="Mvehicle",
    ),
    # Final energy
    #
    # The following are 4 different partial sums of in::transport, in which
    # individual technologies are already aggregated to modes
    dict(
        variable="T final energy",
        base="in:nl-t-ya-c:T",
        var=["Final Energy|Transportation", "t", "c"],
        sums=["c"],
        unit=_FE_UNIT,
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


#: Quantities in which to select transport technologies only. See :func:`callback`.
QUANTITY = [
    "CAP_NEW",
    "CAP",
    "emi",
    "fix_cost",
    "historical_new_capacity",
    "in",
    "input",
    "inv_cost",
    "out",
    "var_cost",
]

#: Units to apply or assign to specific quantities. See :func:`callback`.
#:
#: - Previously ``CAP:nl-t-ya:non-ldv`` was converted to "v**2 Tm / a".
#: - For ``emi``, units of ``ACT`` are not carried, so a correction is needed:
#:
#:   - Add [time]: -1
#:   - Remove [vehicle]: -1, [distance]: -1
#:
#:   When run together with global.yaml reporting, emi:* is assigned units of
#:   "Mt / year". Using apply_units() causes these to be *converted* to  kt/a, i.e.
#:   increasing the magnitude; so use assign_units() instead.
UNITS = {
    "CAP": ("apply", "Mv"),
    "CAP_NEW": ("apply", "Mv"),
    "emi": ("assign", "kt / a"),
    "in": ("apply", "GWa / a"),
    "out": ("apply", "Tm / a"),
}


def callback(rep: Reporter, context: Context) -> None:
    """:meth:`.prepare_reporter` callback for MESSAGEix-Transport.

    `rep` is extended with tasks for transport reporting. Among others, these include:

    1. Select subsets of transport technologies. For each input quantity in
       :data:`SELECT`, for example ``CAP_NEW:*``, tasks are added to compute:

       - ``CAP_NEW:*:transport all`` —selects only the technologies in
         ``t::transport all``.
       - ``CAP_NEW:*:ldv`` —selects only the technologies in ``t::transport LDV``.
       - ``CAP_NEW:*:non-ldv`` —selects only the technologies in
         ``t::transport P ex LDV``.

    2. (Re) apply units. :func:`.ixmp.report.operator.data_for_quantity` drops units for
       most data extracted from a MESSAGEix-GLOBIOM :class:`.Scenario`, because the data
       contain a mix of inconsistent units.

       For every item in :data:`UNITS`, add a task to apply or assign units to selected
       subsets of data that are guaranteed to have those units.

    3. Aggregate in 3 stages, using :data:`key.agg.t <.transport.key.agg>` and
       ``nl::world agg``, ``t::transport modes 1``, producing keys like ``emi:*:T``.
       These values are aggregated by technology group and/or mode.
    4. Invoke :func:`misc`.
    5. Invoke :func:`convert_iamc`.
    6. Invoke :func:`convert_sdmx`.
    7. Add plots from :mod:`.transport.plot` using :func:`.report.add_plots`. These
       appear at :data:`key.report.plot <.transport.key.report>`. If the scenario to be
       reported is not solved, only a subset of plots are added.
    8. Invoke :func:`.transport.base.prepare_reporter`.
    9. :data:`key.report.all <.transport.key.report>` which includes all of the above.
    """
    from genno.operator import aggregate

    from . import base, build

    N_keys = len(rep.graph)  # Number of keys prior to additions

    # - Configure MESSAGEix-Transport, if not already configured.
    # - Add structure and other information from `scenario`.
    # - Call, inter alia:
    #   - demand.prepare_computer() for ex-post mode and demand calculations.
    # - Check that the same Reporter object is returned.
    s: "Scenario | None" = rep.graph.get("scenario")
    assert build.get_computer(context, obj=rep, visualize=False, scenario=s) is rep

    # Apply common steps for each of QUANTITY
    # - Infer the full dimensionality of each key to be selected
    for k_base in rep.infer_keys(QUANTITY):
        k = k_base["T"]  # Target key

        # 1. Select all transport technologies
        rep.add(k[0], "select", k_base, K.coord.t, sums=True)

        # 2. Apply units
        if k.name in UNITS:
            op, units = UNITS[k.name]
            rep.add(k[1], f"{op}_units", k[0], units=units, sums=True)
        else:
            rep.add(k[1], k[0])  # Simple alias / no-op

        # 1. Select further subsets of transport technologies.
        rep.add(k["ldv"], "select", k[1], K.coord.t["LDV"], sums=True)
        rep.add(k["non-ldv"], "select", k[1], K.coord.t["P ex LDV"], sums=True)

        # 3. Aggregate according to groups
        # Reference the function to avoid the genno magic which would treat as sum()
        # NB aggregation on the nl dimension *could* come first, but this can use a
        #    lot of memory when applied to e.g. out:*: for a full global model.
        rep.add(k[2], aggregate, k[1], K.agg.t, keep=True)
        rep.add(k, aggregate, k[2], "nl::world agg", keep=False, sums=True)
        rep.add(k["modes"], "select", k, "t::transport modes 1", sums=True)

    # Apply some functions that prepare further tasks. Order matters here.
    misc(rep)
    convert_iamc(rep)  # Adds to key.report.all
    convert_sdmx(rep)  # Adds to key.report.all
    add_plots(rep, plot, K.report.plot)
    base.prepare_reporter(rep)  # Tasks that prepare data to parametrize the base model

    log.info(f"Added {len(rep.graph) - N_keys} keys")


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

    # Configure replacements for technology IDs in conversion to IAMC data structure
    cfg: Config = c.graph["context"].transport
    add_replacements("t", cfg.spec.add.set["technology"])

    # Update replacements for fully-constructed IAMC variable codes
    # - Quantity.name is prepended automatically; this occurs with quantities derived
    #   from CAP and CAP_NEW. Remove the prefix.
    util.REPLACE_VARS.update({r"^CAP(_NEW)?\|(S(ale|tock)s\|Transportation)": r"\2"})

    keys = []
    for info in CONVERT_IAMC:
        handle_iamc(c, deepcopy(info))
        keys.append(f"{info['variable']}::iamc")

    # Concatenate IAMC-format tables
    k = Key("transport", tag="iamc")
    c.add(k, "concat", *keys)

    # Add tasks for writing IAMC-structured data to file and storing on the scenario
    c.apply(util.store_write_ts, k)

    # Use this line to both store and write to file IAMC structured-data
    c.graph[K.report.all] += (k + "all",)
    # Use this line for "transport::iamc+file" instead of "transport::iamc+all", i.e. to
    # write IAMC-structured data to file but *not* store on scenario
    # c.graph[K.report.all] += (k + "file",)


def convert_sdmx(c: "Computer") -> None:
    """Add tasks to convert data to SDMX."""
    from sdmx.message import StructureMessage

    from message_ix_models.util.sdmx import DATAFLOW, Dataflow

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
    c.add(K.report.sdmx, "write_sdmx_structures", sm, dir_, *keys)

    # Connect to the main report key
    c.graph[K.report.all] += (K.report.sdmx,)


def misc(c: "Computer") -> None:
    """Add miscellaneous tasks.

    Among others, these include:

    - ``calibrate fe`` → a file :file:`calibrate-fe.csv`. See the header comment.
    """
    config: "Config" = c.graph["config"]["transport"]

    # Configuration for :func:`check`. Adds a single key, 'transport check', that
    # depends on others and returns a :class:`pandas.Series` of :class:`bool`.
    # TODO Replace with use of message_ix_models.testing.check
    c.add("transport check", "transport_check", "scenario", "ACT:nl-t-yv-va-m-h")

    # Exogenous data
    c.add("distance:nl:non-ldv", "distance_nonldv", "config")

    # Demand per capita
    c.add("demand::capita", "divdemand:n-c-y", K.pop)

    # Adjustment factor for LDV calibration: fuel economy ratio
    k_num = Key("in:nl-t-ya-c:T") / "c"  # As in CONVERT_IAMC
    k_denom = Key("out:nl-t-ya-c:T") / "c"  # As in CONVERT_IAMC
    k_check = single_key(c.add("fuel economy::check", "div", k_num, k_denom))
    c.add(
        k_check + "sel",
        "select",
        k_check,
        indexers=dict(t="LDV", ya=config.base_model_info.y0),
        drop=True,
    )

    k_ratio = single_key(
        c.add("fuel economy::ratio", "div", K.exo.input_ref_ldv, k_check + "sel")
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
