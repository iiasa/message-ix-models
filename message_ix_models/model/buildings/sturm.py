"""Interface to STURM."""
import gc
import subprocess
from typing import Optional, Tuple

import pandas as pd
from message_ix_models import Context


def run_sturm(
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
        import rpy2.situation

        if first_iteration:
            print(*rpy2.situation.iter_info(), sep="\n")

        return _sturm_rpy2(context, prices, first_iteration)
    except ImportError:
        if first_iteration:
            print("rpy2 NOT found")

        return _sturm_rscript(context, prices, first_iteration)


def _sturm_rpy2(
    context: Context, prices: pd.DataFrame, first_iteration: bool
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`rpy2`."""
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter

    # Retrieve info from the Context object
    config = context["buildings"]

    # Path to R code
    rcode_path = config["code_dir"].joinpath("STURM_model")

    # Source R code
    r = ro.r
    r.source(str(rcode_path.joinpath("F10_scenario_runs_MESSAGE_2100.R")))

    # Common arguments for invoking STURM
    args = dict(
        run=config["ssp"],
        scenario_name=f"{config['ssp']}_{config['clim_scen']}",
        prices=prices,
        path_rcode=str(rcode_path),
        path_in=str(config["code_dir"].joinpath("STURM_data")),
        path_out=str(config["code_dir"].joinpath("STURM_output")),
        geo_level_report=context.regions,  # Should be R12
        report_type=["MESSAGE", "NGFS"],
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
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke STURM using :mod:`subprocess` and :program:`Rscript`."""
    # TODO report_type and report_var are not passed
    # Retrieve info from the Context object
    config = context["buildings"]

    # Prepare input files
    # Temporary directory in the MESSAGE_Buildings directory
    temp_dir = config["code_dir"].joinpath("temp")
    temp_dir.mkdir(exist_ok=True)

    # Write prices to file
    input_path = temp_dir.joinpath("prices.csv")
    prices.to_csv(input_path)

    def run_edited(sector: str) -> pd.DataFrame:
        """Edit the run_STURM.R script, then run it."""
        # Read the script and split lines
        script_path = config["code_dir"].joinpath("run_STURM.R")
        lines = script_path.read_text().split("\n")

        # Replace some lines
        # FIXME(PNK) This is extremely fragile. Instead use a template or regex
        # replacements
        lines[8] = f"ssp_scen <- \"{config['ssp']}\""
        lines[9] = f"clim_scen <- \"{config['clim_scen']}\""
        lines[10] = f'sect <- "{sector}"'

        script_path.write_text("\n".join(lines))

        # Need to supply cwd= because the script uses R's getwd() to find others
        subprocess.check_call(["Rscript", "run_STURM.R"], cwd=config["code_dir"])

        # Read output, then remove the file
        output_path = temp_dir.joinpath(f"{sector}_sturm.csv")
        result = pd.read_csv(output_path)
        output_path.unlink()

        return result

    # Residential
    sturm_scenarios = run_edited(sector="resid")

    # Commercial
    comm_sturm_scenarios = run_edited(sector="comm") if first_iteration else None

    input_path.unlink()
    temp_dir.rmdir()

    return sturm_scenarios, comm_sturm_scenarios
