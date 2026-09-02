"""Compatibility from legacy to :mod:`genno`-based reporting."""

import logging
from dataclasses import asdict
from functools import partial
from itertools import count
from types import ModuleType
from typing import TYPE_CHECKING

import pandas as pd
from genno import Key, Keys, quote

from message_ix_models.util import minimum_version, package_data_path

from .config import Config, TableConfig

if TYPE_CHECKING:
    from pathlib import Path

    from message_ix import Reporter, Scenario

    from message_ix_models import Context

log = logging.getLogger(__name__)

#: Keys for :class:`.Reporter` tasks added by :func:`callback`. These include:
#:
#: - Inputs to :func:`prepare_module_globals`:
#:
#:   - :py:`.mu`: :class:`dict` representation of
#:     :attr:`.legacy.config.Config.model_units`.
#:   - :py:`.uc`: :class:`dict`, same as :attr:`.legacy.config.Config.unit_conversion`.
#:   - :py:`.upd`: :class:`pathlib.Path` to “urban percentage data”, a CSV file.
#:   - :py:`.tm`:  :class:`list` of :class:`types.ModuleType`, including all modules
#:     from which tables are used.
#:
#: - :py:`.prep`: Run :func:`prepare_module_globals`. Always returns :any:`True`.
#: - :py:`.av`: load the CSV file at :attr:`.legacy.Config.config.var_def`, return
#:   unique entries from the ‘Variable’ column as :class:`list`.
#: - :py:`.mapping`: load the CSV file at :attr:`.legacy.Config.config.aggr_def`.
#: - :py:`.result`: the final result as IAMC-structured :class:`pandas.DataFrame`.
KEY = Keys(
    av="allowed_vars::legacy",
    mapping="mapping::legacy",
    mu="model units::legacy",
    prep="prepare modules::legacy",
    result="legacy::iamc",
    uc="unit conversion::legacy",
    upd="urban_perc_data::legacy",
    tm="table modules::legacy",
)

#: Table names to skip
SKIP: set[str] = set()


# NB with message_ix 3.10 and earlier:
# - On GitHub Actions, all tables fail with ReferenceError: weakly-referenced object no
#   longer exists.
# - Locally, retr_macro() fails with RuntimeError: unhandled Java exception: There is no
#   parameter 'gdp_calibrate'
@minimum_version("message_ix 3.11")
def callback(rep: "Reporter", context: "Context") -> None:
    """Insert tasks to invoke legacy reporting tables.

    This function calls :meth:`.legacy.config.Config.from_file` on
    :file:`default_run_config.yaml` to read and prepare configuration.

    It then sets up the following on `rep`:

    - All of :data:`KEY`.
    - Multiple keys like ``Enrgy_PE::table+iamc``, with tasks that call
      :func:`run_table` with a given :class:`TableConfig` instance and return the result
      as IAMC-structured :class:`pandas.DataFrame`.
    - Intermediate keys in the preparation of :py:`KEY.result` or ``legacy::iamc``:

      - ``legacy::iamc+0`` concatenates all the above tables together.
      - ``legacy::iamc+1`` applies :func:`merge_ts` if :attr:`.Config.merge_ts` is
        :any:`True`.
      - ``legacy::iamc+2`` filters using :py:`KEY.av`.
      - ``legacy::iamc+3`` applies :func:`merge_hist` if :attr:`.Config.merge_hist` is
        :any:`True`. Currently ``legacy::iamc`` is an alias for this key.

    These keys and tasks can then be overridden by user code before calling
    :meth:`.Reporter.get`.

    Raises
    ------
    NotImplementedError
        if :attr:`~.Config.run_history` is :any:`True`.
    """

    # .report.legacy.config.Config instance. This is only used within the current
    # function to set up `rep`; it is not preserved or available within the Reporter.
    # TODO Allow a different file
    cfg = Config.from_file(
        package_data_path("report", "legacy", "default_run_config.yaml")
    )

    if cfg.run_history:
        raise NotImplementedError

    # Iterate over TableConfig instances derived from default_run_config.yaml
    table_keys, modules = [], set()
    for tc in filter(lambda tc: tc.name not in SKIP, cfg.table):
        # Make a key for this task
        key = Key(tc.name, tag="table+iamc")

        # Avoid duplicates by adding to the name
        for i in count():
            if key in table_keys:
                key = Key(f"{tc.name} {i}", tag="table+iamc")
            else:
                break

        # Add a task to call run_legacy_table with this function
        rep.add(key, run_table, KEY.prep, table_config=tc)
        table_keys.append(key)
        modules.add(tc.module)

    # Add tasks that return the configuration information to be used by
    # prepare_module_globals()
    rep.add(KEY.mapping, partial(pd.read_csv, cfg.aggr_def))
    rep.add(KEY.mu, quote(asdict(cfg.model_units)))
    rep.add(KEY.uc, quote(cfg.unit_conversion))
    rep.add(KEY.upd, quote(cfg.urban_perc))
    rep.add(KEY.tm, modules)

    # Keys for tasks already in `rep`
    args_common = ("config", "scenario", "n", "y", "y::model")
    # Keys or literal values added above
    args_from_config = (KEY.mu, cfg.run_history, KEY.uc, KEY.upd, KEY.tm)

    # Add a task to set module-level globals in pp_utils, default_tables, etc. This task
    # 1. is required by, thus runs before, the individual run_table() tasks, and
    # 2. runs only once.
    rep.add(KEY.prep, prepare_module_globals, *args_common, *args_from_config)

    # Add a task to concatenate all tables together
    k = KEY.result
    rep.add(k[0], lambda *args: pd.concat(args), *table_keys)

    rep.add(k[1], merge_ts, k[0], condition=cfg.merge_ts)

    # Apply allowed_vars from Config.var_def
    rep.add(KEY.av, lambda: pd.read_csv(cfg.var_def)["Variable"].unique().tolist())
    rep.add(k[2], lambda df, av: df[df.Variable.isin(av)], k[1], KEY.av)

    rep.add(k[3], merge_hist, k[2], condition=cfg.merge_hist)

    # Simple alias
    rep.add(k, k[3])

    # TODO Append to all::iamc key


def merge_hist(data: pd.DataFrame, *, condition: bool) -> pd.DataFrame:
    """Imitate :py:`iamc_report_hackathon(…, merge_hist=True)`."""
    if condition:  # pragma: no cover
        raise NotImplementedError
    return data


def merge_ts(data: pd.DataFrame, *, condition: bool) -> pd.DataFrame:
    """Imitate :py:`iamc_report_hackathon(…, merge_ts=True)`."""
    if condition:  # pragma: no cover
        raise NotImplementedError
    return data


def prepare_module_globals(
    config: dict,
    scenario: "Scenario",
    nodes: list[str],
    years: list[int],
    years_model: list[int],
    model_units: dict,
    run_history: bool,
    unit_conversion: dict,
    urban_perc_data: "Path",
    table_modules: list["ModuleType"],
) -> bool:
    """Prepare global variables for :func:`run_table`.

    Functions in the module :mod:`.report.legacy.pp_utils` and in `table_modules` such
    as :mod:`.report.legacy.default_tables` reference module-global variables. When
    :func:`.iamc_report_hackathon.main` is used, these are set directly before the table
    function is called.

    :func:`prepare_module_globals` ensures these module-global variables have the
    necessary contents, using values passed as arguments: either from standard
    :class:`.Reporter` contents per :meth:`message_ix_models.report.prepare_computer`,
    or added by :func:`callback` in this module from a :class:`.legacy.config.Config`
    instance.
    """
    from message_ix_models.report.legacy import pp_utils
    from message_ix_models.report.legacy.postprocess import PostProcess

    pp = PostProcess(scenario)

    for module in table_modules:
        # Set globals in each of the table_modules
        # TODO When adding support for run_history = True, set kyoto_hist_data and
        #      lu_hist_data
        if getattr(module, "pp", None) is None:
            setattr(module, "pp", pp)
            setattr(module, "mu", model_units)
            # NB The variable must be string "True", not bool
            setattr(module, "run_history", str(run_history))
            # NB Must be a path to a CSV file. Not used in default_tables.py; see
            #    ENGAGE_SSP2_v417_tables.py.
            setattr(module, "urban_perc_data", urban_perc_data)

    if getattr(pp_utils, "regions", None) is None:
        region_id = config["model"].regions
        globalname = f"{region_id}_GLB"

        setattr(pp_utils, "all_tecs", scenario.set("technology"))
        setattr(pp_utils, "all_years", years)
        setattr(pp_utils, "globalname", globalname)
        setattr(pp_utils, "model_nm", scenario.model)
        setattr(
            pp_utils,
            "regions",
            {n: n.partition("_")[2] for n in nodes if n != "World"}
            | {globalname: "World"},
        )
        setattr(pp_utils, "region_id", region_id)
        setattr(pp_utils, "scen_nm", scenario.scenario)
        setattr(pp_utils, "unit_conversion", unit_conversion)
        setattr(pp_utils, "years", years_model)

    return True


def run_table(prepared: bool, table_config: "TableConfig") -> pd.DataFrame:
    """Invoke a single function (‘table’) from legacy reporting.

    1. Run the :attr:`.TableConfig.function`. No arguments are passed; if any are
       required, they **must** be prepared using :any:`functools.partial`.
    2. Prepend :attr:`.TableConfig.variable_prefix` to entries in the ‘Variable’ column.

    If any exception is raised, a warning is logged, and an empty data frame is
    returned.

    Parameters
    ----------
    table_config :
        Information about the table.
    """
    try:
        # - Run the table function.
        # - Prepend the variable_prefix to strings in the Variable column.
        return table_config.function().assign(
            Variable=lambda df: df.Variable.str.replace(
                "^", table_config.variable_prefix + "|", regex=True
            )
        )
    except Exception as e:
        # Something went wrong. Show an exception message and return empty data.
        mod = table_config.module.__name__
        log.error(
            f"{type(e).__name__} invoking {mod}.(retr_){table_config.name}:\n"
            + e.args[0],
        )
        return pd.DataFrame()
