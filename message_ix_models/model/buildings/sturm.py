"""Interface to STURM."""
import gc
import logging
import subprocess
from typing import Optional, Tuple

import pandas as pd
from message_ix_models import Context

log = logging.getLogger(__name__)


def run(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Invoke STURM, either using rpy2 or via Rscript.

    Returns
    -------
    pd.DataFrame
        The `sturm_scenarios` data frame.
    pd.DataFrame or None
        The `comm_sturm_scenarios` data frame if `first_iteration` is :obj:`True`;
        otherwise :obj:`None`.
    """
    try:
        import rpy2  # noqa: F401

        has_rpy2 = True
    except ImportError:
        has_rpy2 = False

    method = context["buildings"].get("sturm_method")
    if method is None:
        m, func = ("rpy2", _sturm_rpy2) if has_rpy2 else ("Rscript", _sturm_rscript)
        log.info(f"Will invoke STURM using {m}")
    elif method == "rpy2" and not has_rpy2:
        if first_iteration:
            log.warning("rpy2 NOT found; will invoke STURM using Rscript")
        func = _sturm_rscript
    elif method not in ("rpy2", "Rscript"):
        raise ValueError(method)

    return func(context, prices, first_iteration)


def _sturm_rpy2(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Invoke STURM using :mod:`rpy2`."""
    import rpy2.robjects as ro
    import rpy2.situation
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter

    if first_iteration:
        log.info("\n".join(rpy2.situation.iter_info()))

    # Retrieve info from the Context object
    config = context["buildings"]

    # Path to R code
    rcode_path = config["code_dir"].joinpath("STURM_model")

    # Source R code
    r = ro.r
    r.source(str(rcode_path.joinpath("F10_scenario_runs_MESSAGE_2100.R")))

    # Common arguments for invoking STURM
    args = dict(
        run=config["sturm scenario"],
        scenario_name=config["sturm scenario"],
        prices=prices,
        path_rcode=str(rcode_path),
        path_in=str(config["code_dir"].joinpath("STURM_data")),
        path_out=str(config["output path"]),
        geo_level_report=context.regions,  # Should be R12
        report_type=["MESSAGE", "NAVIGATE"],
        report_var=["energy", "material"],
    )

    with localconverter(ro.default_converter + pandas2ri.converter):
        # Residential
        sturm_scenarios = r.run_scenario(**args, sector="resid")
        # Commercial
        # NOTE: run only on the first iteration!
        comm_sturm_scenarios = (
            r.run_scenario(**args, sector="comm") if first_iteration else None
        )

    del r
    gc.collect()

    return sturm_scenarios, comm_sturm_scenarios


def _sturm_rscript(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Invoke STURM using :mod:`subprocess` and :program:`Rscript`."""
    # TODO report_type and report_var are not passed
    # Retrieve info from the Context object
    config = context["buildings"]

    # Prepare input files
    # Temporary directory within the MESSAGE_Buildings directory
    temp_dir = config["code_dir"].joinpath("temp")
    temp_dir.mkdir(exist_ok=True)

    # Write prices to file
    input_path = temp_dir.joinpath("prices.csv")
    prices.to_csv(input_path)

    def check_call(sector: str) -> pd.DataFrame:
        """Invoke the run_STURM.R script and return its output."""
        # Need to supply cwd= because the script uses R's getwd() to find others
        subprocess.check_call(
            [
                "Rscript",
                "run_STURM.R",
                f"--sector={sector}",
                f"--ssp={config['clim_scen']}",
                f"--ssp={config['ssp']}",
            ],
            cwd=config["code_dir"],
        )

        # Read output, then remove the file
        output_path = temp_dir.joinpath(f"{sector}_sturm.csv")
        result = pd.read_csv(output_path)
        output_path.unlink()

        return result

    # Residential
    sturm_scenarios = check_call(sector="resid")

    # Commercial
    comm_sturm_scenarios = check_call(sector="comm") if first_iteration else None

    input_path.unlink()
    temp_dir.rmdir()

    return sturm_scenarios, comm_sturm_scenarios


def scenario_name(name: str) -> str:
    """Return a STURM scenario name for a corresponding MESSAGEix-GLOBIOM name."""
    return {
        "baseline": "SSP2_BL",
    }.get(name, f"NAV_Dem-{name}")
