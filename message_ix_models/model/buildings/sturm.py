"""Interface to STURM."""

import gc
import logging
import re
import subprocess
from collections.abc import Mapping, MutableMapping
from enum import Enum, auto
from typing import TYPE_CHECKING

import pandas as pd
from message_ix import Scenario

from message_ix_models import Context

if TYPE_CHECKING:
    from collections.abc import Callable

    from message_ix_models.model.buildings.config import Config

    # Common signature for _sturm_*()
    RunSTURMFunction = Callable[
        [Context, pd.DataFrame, MutableMapping, bool], tuple[pd.DataFrame, pd.DataFrame]
    ]

log = logging.getLogger(__name__)


class METHOD(Enum):
    """Method for invoking STURM and other message-ix-buildings code."""

    #: Invoke STURM using :mod:`rpy2`.
    RPY2 = auto()

    #: Invoke :file:`run_STURM.R` using :program:`Rscript`. This is an older method
    #: used prior to 2026. It may not may still be supported by the version of
    #: :file:`run_STURM.R` on the ``main`` branch of message-ix-buildings.
    RSCRIPT_A = auto()

    #: Invoke STURM and other scripts from :data:`RSCRIPT_B_FILES`, using
    #: :program:`Rscript`. This method was developed in 2026 for use in the NGFS and
    #: CircEUlar projects, and corresponds to changes on the message-ix-buildings
    #: ``{NAME}`` branch.
    RSCRIPT_B = auto()


#: R files used for :data:`METHOD.RSCRIPT_B`.
RSCRIPT_B_FILES = [
    "run_STURM_Circular_resid_glo.R",
    "run_STURM_Circular_comm_glo.R",
    "run_GLANCE_placeholder.R",
    "run_MIXB_aligner.R",
]


def run(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM, either using rpy2 or via Rscript.

    Returns
    -------
    pd.DataFrame
        The `sturm_scenarios` data frame.
    pd.DataFrame or None
        The `comm_sturm_scenarios` data frame. If `first_iteration` is :obj:`False`,
        this is empty.
    """
    from importlib.util import find_spec

    has_rpy2 = find_spec("rpy2") is not None

    # Retrieve config from the Context object
    config: "Config" = context.buildings

    if context.model.regions != "R12":
        raise NotImplementedError(
            f"sturm.run(…) with regions={context.model.regions!s}!=R12"
        )

    # Check whether METHOD.RPY2 can be used
    if config.sturm_method is METHOD.RPY2 and not has_rpy2:
        log.warning("rpy2 NOT found; will invoke STURM using Rscript")
        # Change the Config setting
        config.sturm_method = METHOD.RSCRIPT_A

    # Identify the function for calling STURM
    func: "RunSTURMFunction" = {
        METHOD.RPY2: _sturm_rpy2,
        METHOD.RSCRIPT_A: _sturm_rscript_A,
        METHOD.RSCRIPT_B: _sturm_rscript_B,
    }[config.sturm_method]

    # Common arguments for invoking STURM
    # - _sturm_rpy2() passes these while calling an R function through rpy2.
    # - _sturm_rscript_A() converts some to command-line arguments given to Rscript.
    # - _sturm_rscript_B() does not use them.
    args = dict(
        run=config.sturm_scenario,
        scenario_name=config.sturm_scenario,
        path_out=str(config._output_path),
        geo_level_report=context.model.regions,
        report_type=["MESSAGE", "NAVIGATE"],
        report_var=["energy", "material"],
    )

    result = func(context, prices, args, first_iteration)

    # Dump data for debugging
    if config._output_path is not None:
        result[0].to_csv(config._output_path.joinpath("debug-sturm-resid.csv"))
        result[1].to_csv(config._output_path.joinpath("debug-sturm-comm.csv"))

    return result


def _sturm_rpy2(
    context: Context, prices: pd.DataFrame, args: MutableMapping, first_iteration: bool
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`rpy2`.

    This function corresponds to :data:`METHOD.RPY2`.
    """
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter

    config: "Config" = context.buildings

    # Source R code
    r = ro.r
    r.source(str(config.sturm_code_dir.joinpath("F10_scenario_runs_MESSAGE_2100.R")))

    # Add additional keyword arguments expected by the run_scenario() R function
    args.update(
        path_in=str(config.code_dir.joinpath("STURM_data")),
        path_rcode=str(config.sturm_code_dir),
        prices=prices,
    )

    with localconverter(ro.default_converter + pandas2ri.converter):
        # Residential
        sturm_scenarios = r.run_scenario(sector="resid", prices=prices, **args)
        # Commercial
        # NOTE: run only on the first iteration!
        comm_sturm_scenarios = (
            r.run_scenario(sector="comm", **args)
            if first_iteration
            else pd.DataFrame(columns=sturm_scenarios.index)
        )

    del r
    gc.collect()

    return sturm_scenarios, comm_sturm_scenarios


def _sturm_rscript_A(
    context: Context, prices: pd.DataFrame, args: Mapping, first_iteration: bool
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`subprocess` and :program:`Rscript`.

    This function corresponds to :data:`METHOD.RSCRIPT_A`.
    """
    # Retrieve info from the Context object
    config = context.buildings

    # Write prices to a temporary file
    temp_dir = context.get_local_path("buildings", "temp")
    temp_dir.mkdir(exist_ok=True, parents=True)
    input_path = temp_dir.joinpath("prices.csv")
    prices.to_csv(input_path)

    # Prepare command-line call
    command = [
        "Rscript",
        "run_STURM.R",
        # Format contents of `args`
        f"--scenario={args['scenario_name']}",
        f"--path_out={args['path_out']}",
        f"--geo_level_report={args['geo_level_report']}",
        f"--report_type={','.join(args['report_type'])}",
        f"--report_var={','.join(args['report_var'])}",
        # Input data path
        f"--price_data={input_path}",
    ]
    log.debug(command)

    def check_call(sector: str) -> pd.DataFrame:
        """Invoke the run_STURM.R script and return its output."""
        # Need to supply cwd= because the script uses R's getwd() to find others
        try:
            subprocess.run(command + [f"--sector={sector}"], cwd=config.code_dir)
        except subprocess.CalledProcessError as e:
            print(f"{e.output = } {e.stderr = }")
            raise
        # Read output, then remove the file
        of = config._output_path.joinpath(f"{sector}_sturm.csv")
        result = pd.read_csv(of)
        of.unlink()

        return result

    # Residential
    sturm_scenarios = check_call(sector="resid")

    # Commercial
    comm_sturm_scenarios = (
        check_call(sector="comm")
        if first_iteration
        else pd.DataFrame(columns=sturm_scenarios.columns)
    )

    input_path.unlink()
    temp_dir.rmdir()

    return sturm_scenarios, comm_sturm_scenarios


def _sturm_rscript_B(
    context: Context, prices: pd.DataFrame, args: Mapping, first_iteration: bool
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`subprocess` and :program:`Rscript`.

    This function corresponds to :data:`METHOD.RSCRIPT_B`.

    Steps include:

    - Write prices to a file named :file:`input_prices_R12.csv`.
    - Write a :file:`scenario_config.yaml` expected by the scripts.
    - Invoke each of the scripts named in :data:`RSCRIPT_B_FILES`, in that order.

    Parameters
    ----------
    context :
    """
    import yaml

    from .config import DEFAULT_DATA_PATHS

    config: "Config" = context.buildings

    # Prepare path for writing STURM input file with price data
    assert config.sturm_input_dir.exists()
    assert config.data_paths["prices"] == DEFAULT_DATA_PATHS["prices"]
    price_input = config.sturm_input_dir.joinpath(config.data_paths["prices"])

    # Write `prices` to file
    prices.to_csv(price_input, index=False)
    log.info(f"Updated prices written to {price_input}")
    log.info(f"Total rows {len(prices)}")

    # Write a YAML file with configuration needed by the R scripts
    path = config.sturm_code_dir.joinpath("scenario_config.yaml")
    payload = {"scenarios": [context.buildings.code]}
    header = (
        "# Shared STURM scenario list\n"
        "# Used by STURM / MIXB runner scripts (see message_ix_buildings/sturm)\n"
    )

    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False)

    log.info(f"Wrote STURM scenarios {payload} to {path}")

    # Invoke R scripts, in order
    for name in RSCRIPT_B_FILES:
        command = ["Rscript", name]
        log.debug(command)
        subprocess.check_call(command, cwd=config.sturm_code_dir)

    # Return empty data frames. The workflows that use this code do not use the
    # MESSAGEix-Buildings output returned by this function.
    return pd.DataFrame(), pd.DataFrame()


def scenario_name(name: str) -> str:
    """Return a STURM scenario name for a corresponding NAVIGATE scenario name.

    STURM works from prepared data that is available for a subset of all the NAVIGATE
    scenario IDs. Perform the following mapping:

    - Replace "15C", "20C", or other policy labels with "NPi": i.e. use the same STURM
      input data regardless of the climate policy scenario.
    - Remove trailing "_d" and "_u", e.g. "…-act_u" becomes "…-act".
    - Remove trailing text like " + ENGAGE step #".
    - "NAV_Dem-" is prepended if it is missing.
    - Map the string "baseline" to "SSP2".

    Other values pass through unaltered.
    """
    result = re.sub(
        r"^(NAV_Dem-)?(15C|20?C|NPi|Ctax|1\d00 Gt)-([^_\+\s]+)(_[du])?.*",
        r"NAV_Dem-NPi-\3",
        name,
    )

    # Replacements for WP6
    # NB this could and maybe should be done by reference to the code list
    for info in (
        ("AdvPE", "ele"),
        ("AdvPEL", "ele"),
        ("AllEn", "all"),
        ("AllEnL", "all"),
        ("Default", "ref"),
        ("LowCE", "act-tec"),
        ("LowCEL", "act-tec"),
    ):
        result = result.replace(*info)

    return {
        "baseline": "SSP2",
    }.get(result, result)


# MIXB demand CSV basenames under ``sturm/message_linking``
# ({code} = context.buildings.code).
_MIXB_DEMAND_CSV = (
    "resid_sturm_aligned_{}.csv",
    "comm_sturm_aligned_{}.csv",
    "resid_comm_glance_aligned_{}.csv",
)


def call_buildings_demand(context: Context, scenario: Scenario) -> Scenario:
    """Update `scenario` demand with data from :file:`sturm/message_linking`.

    .. note:: Despite the name, this function **does not** call STURM. It appears to
       parallel the behaviour of :func:`.buildings.build.main` with
       :func:`prepare_data_B`. It is not currently called anywhere.
    """
    config: "Config" = context.buildings
    linking_dir = config.sturm_code_dir.joinpath("message_linking")
    assert linking_dir.exists()
    code = context.buildings.code
    demand = pd.concat(
        [pd.read_csv(linking_dir / name.format(code)) for name in _MIXB_DEMAND_CSV],
        ignore_index=True,
    )

    exclude_expr = r"_mat_|_floor_|other_uses_|v_no_heat|non-comm"
    # TODO: do we need dynamic materials demand for CircEUlar too?
    demand = demand[~demand["commodity"].str.contains(exclude_expr, na=False)].copy()
    demand["level"] = "useful"
    # TODO: "useful" to match build; consider unifying demand levels to "final"

    if 2110 not in demand["year"].values and 2100 in demand["year"].values:
        df_2110 = demand[demand["year"] == 2100].copy()
        df_2110["year"] = 2110
        demand = pd.concat([demand, df_2110], ignore_index=True)
        log.info("Added 2110 demand rows by copying from 2100")

    with scenario.transact(
        "Add Buildings demand from message_ix_buildings/sturm/message_linking"
    ):
        scenario.add_par("demand", demand)

    log.info(
        "Added %d demand rows to %s/%s (code=%r, %s)",
        len(demand),
        scenario.model,
        scenario.scenario,
        code,
        linking_dir,
    )
    return scenario
