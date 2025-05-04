"""Reporting/postprocessing for MESSAGEix-Transport."""

import logging
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any

import genno
import pandas as pd
from genno import Computer, Key, KeySeq, MissingKeyError
from genno.core.key import single_key
from message_ix import Reporter

from message_ix_models import Context, ScenarioInfo
from message_ix_models.report.util import add_replacements

from . import Config
from .key import exo

if TYPE_CHECKING:
    import ixmp
    from genno import Computer

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
    k = KeySeq(base_key)

    file_keys = []
    for suffix in ("csv", "xlsx"):
        # Create the path
        path = c.add(
            k[f"{suffix} path"],
            "make_output_path",
            "config",
            name=f"{k.base.name}.{suffix}",
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

    for key in map(lambda s: KeySeq(c.infer_keys(s)), "emi in out".split()):
        try:
            # Reference the function to avoid the genno magic which would treat as sum()
            # NB aggregation on the nl dimension *could* come first, but this can use a
            #    lot of memory when applied to e.g. out:*: for a full global model.
            c.add(key[0], func, key.base, "t::transport agg", keep=False)
            c.add(key[1], func, key[0], "nl::world agg", keep=False)
            c.add(key["transport"], "select", key[1], "t::transport modes 1", sums=True)
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
    from . import base, build, key

    N_keys = len(rep.graph)

    # Collect all reporting tasks
    rep.add(key.report.all, [])

    # - Configure MESSAGEix-Transport.
    # - Add structure and other information.
    # - Call, inter alia:
    #   - demand.prepare_computer() for ex-post mode and demand calculations.
    #   - plot.prepare_computer() for plots; adds to key.report.all.
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


def latest_reporting_from_file(
    info: ScenarioInfo, base_dir: Path
) -> tuple[Any, int, pd.DataFrame]:
    """Locate and retrieve the latest reported output for the scenario `info`.

    The file :file:`transport.csv` is sought in a subdirectory of `base_dir` identified
    by :attr:`.ScenarioInfo.path`.

    .. todo:: Move upstream, to :mod:`message_ix_models`.

    Returns
    -------
    tuple
        1. The path of the file read.
        2. :class:`int`: The scenario version corresponding to the data read.
        3. :class:`pandas.DataFrame`: the data.

        If no data is found, all the elements are :any:`None`.
    """
    dirs = sorted(base_dir.glob(info.path.replace("vNone", "v*")), reverse=True)
    for _dir in dirs:
        path = _dir.joinpath("transport.csv")
        if not path.exists():
            log.info(f"Skip {_dir}; no file 'transport.csv'")
            continue
        path_version = int(path.parent.name.split("v")[-1])
        return (
            path,
            path_version,
            pd.read_csv(path).assign(
                Scenario=lambda df: df.Scenario + f"#{path_version}"
            ),
        )

    return None, -1, pd.DataFrame()


def latest_reporting_from_platform(
    info: ScenarioInfo, platform: "ixmp.Platform", minimum_version: int = -1
) -> tuple[Any, int, pd.DataFrame]:
    """Retrieve the latest reported output for the scenario described by `info`.

    The time series data attached to a scenario on `platform` is retrieved.

    .. todo:: Move upstream, to :mod:`message_ix_models`.

    Returns
    -------
    tuple
        1. The :class:`.Scenario` object.
        2. :class:`int`: The scenario version corresponding to the data read.
        3. :class:`pandas.DataFrame`: the data.

        If no data is found or the latest version with reporting time series data is
        <= `minimum_version`, all the elements are :any:`None`.
    """
    from message_ix import Scenario

    for _, row in (
        platform.scenario_list(model=info.model, scen=info.scenario, default=False)
        .sort_values(["version"], ascending=False)
        .iterrows()
    ):
        if row.version <= minimum_version:
            log.info(f"{row.version} ≤ minimum {minimum_version}")
            break
        elif row.is_locked:
            log.info(f"Skip {info.url} {row.version}; locked")
            continue

        s = Scenario(
            platform, model=info.model, scenario=info.scenario, version=row.version
        )
        if s.has_solution():
            return (
                s,
                row.version,
                s.timeseries().assign(
                    # Scenario=lambda df: df.Scenario + f"v{row.version}"
                ),
            )
        else:
            log.info(f"Skip {info.url} {row.version}; no reporting output")
            del s

    return None, -1, pd.DataFrame()


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
    c.add("demand::capita", "divdemand:n-c-y", "population:n-y")

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
        c.add("fuel economy::ratio", "div", exo.input_ref_ldv, k_check + "sel")
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


def multi(context: Context, targets):
    """Report outputs from multiple scenarios."""
    import plotnine as p9

    from message_ix_models.report.operator import quantity_from_iamc
    from message_ix_models.tools.iamc import _drop_unique

    report_dir = context.get_local_path("report")
    platform = context.get_platform()

    dfs = []
    for target in map(ScenarioInfo.from_url, targets):
        path, path_version, df_path = latest_reporting_from_file(target, report_dir)
        scen, scen_version, df_scen = latest_reporting_from_platform(target, platform)

        if path_version == scen_version == -1:
            raise RuntimeError(f"No reporting output available for {target}")
        elif path_version >= scen_version:
            source = "file"
            df = df_path
            version = path_version
        else:
            source = "platform"
            df = df_scen
            version = scen_version

        log.info(f"{target.url = } {source = } {version = }")

        dfs.append(df)

    # Convert to a genno.Quantity
    cols = ["Variable", "Model", "Scenario", "Region", "Unit"]
    data = genno.Quantity(
        pd.concat(dfs)
        .sort_values(cols)
        .melt(id_vars=cols, var_name="y")
        .astype({"y": int})
        .pipe(_drop_unique, columns="Model", record=dict())
        .rename(columns={"Variable": "v", "Scenario": "s", "Region": "n"})
        .dropna(subset=["value"])
        .set_index("v s n y Unit".split())["value"]
    )

    # Select a subset of data
    qty = quantity_from_iamc(data, r"Transport\|Stock\|Road\|Passenger\|LDV\|(.*)")

    # Plot
    # TODO Move to .transport.plot
    plot = (
        p9.ggplot(qty.to_dataframe().reset_index())
        + p9.aes(x="y", y="value", color="v")
        + p9.facet_grid("n ~ s")
        + p9.geom_point()
        + p9.geom_line()
        + p9.theme(figure_size=(11.7, 16.6))
    )
    plot.save("debug.pdf")

    return data


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
    for key in map(lambda name: c.infer_keys(f"{name}:*"), SELECT):
        c.add(key + "transport all", "select", key, "t::transport all", sums=True)
        c.add(key + "ldv", "select", key, "t::transport LDV", sums=True)
        c.add(key + "non-ldv", "select", key, "t::transport P ex LDV", sums=True)
