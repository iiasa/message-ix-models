"""Interface to STURM."""

import gc
import logging
import re
import subprocess
from collections.abc import Mapping, MutableMapping

import pandas as pd

from message_ix_models import Context

log = logging.getLogger(__name__)


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
    try:
        import rpy2  # noqa: F401

        has_rpy2 = True
    except ImportError:
        has_rpy2 = False

    # Retrieve config from the Context object
    config = context.buildings

    method = config.sturm_method
    if method is None:
        m, func = ("rpy2", _sturm_rpy2) if has_rpy2 else ("Rscript", _sturm_rscript)
        log.info(f"Will invoke STURM using {m}")
    elif method == "rpy2" and not has_rpy2:
        if first_iteration:
            log.warning("rpy2 NOT found; will invoke STURM using Rscript")
        func = _sturm_rscript
    elif method == "Rscript":
        func = _sturm_rscript
    else:
        raise ValueError(method)

    # Common arguments for invoking STURM
    args = dict(
        run=config.sturm_scenario,
        scenario_name=config.sturm_scenario,
        path_rcode=str(config.code_dir.joinpath("STURM_model")),
        path_in=str(config.code_dir.joinpath("STURM_data")),
        path_out=str(config._output_path),
        geo_level_report=context.model.regions,
        report_type=["MESSAGE", "NAVIGATE"],
        report_var=["energy", "material"],
    )

    if args["geo_level_report"] != "R12":
        raise NotImplementedError

    result = func(context, prices, args, first_iteration)

    # Dump data for debugging
    result[0].to_csv(config._output_path.joinpath("debug-sturm-resid.csv"))
    result[1].to_csv(config._output_path.joinpath("debug-sturm-comm.csv"))

    return result


def _sturm_rpy2(
    context: Context, prices: pd.DataFrame, args: MutableMapping, first_iteration: bool
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`rpy2`."""
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter

    args.update(prices=prices)

    # Source R code
    r = ro.r
    r.source(str(args["path_rcode"].joinpath("F10_scenario_runs_MESSAGE_2100.R")))

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


def _sturm_rscript(
    context: Context, prices: pd.DataFrame, args: Mapping, first_iteration: bool
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`subprocess` and :program:`Rscript`."""
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
