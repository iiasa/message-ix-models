"""Interface to STURM."""

import gc
import logging
import re
import subprocess
from collections.abc import Mapping, MutableMapping
from pathlib import Path

import ixmp
import numpy as np
import pandas as pd
from message_ix import Scenario

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


def _message_buildings_install_dir() -> Path:
    """Return MESSAGEix-Buildings path from ixmp (``message_buildings_dir``)."""
    message_buildings_dir = None
    for key in ("message_buildings_dir", "message buildings dir"):
        try:
            value = ixmp.config.get(key)
        except (AttributeError, KeyError):
            continue
        if value:
            message_buildings_dir = value
            break
    if not message_buildings_dir:
        raise ValueError(
            "ixmp config key 'message_buildings_dir' (or 'message buildings dir') is "
            "not set."
        )
    return Path(message_buildings_dir).expanduser().resolve()


def call_sturm(context: Context, scenario: Scenario) -> Scenario:
    """Merge scenario prices into STURM inputs, then run MESSAGEix-Buildings STURM."""
    buildings_root = _message_buildings_install_dir()
    sturm_dir = buildings_root.joinpath("message_ix_buildings", "sturm")
    price_dir = sturm_dir.joinpath("data")

    # Duplicate the original energy price input file in STURM
    original_price_input_file = price_dir.joinpath("input_prices_R12.csv")

    if not original_price_input_file.exists():
        raise FileNotFoundError(
            f"Original price input file not found: {original_price_input_file}"
        )

    original_price_input_backup = price_dir.joinpath("input_prices_R12_ori.csv")
    df_prices_ori = pd.read_csv(original_price_input_file)
    df_prices_ori.to_csv(original_price_input_backup, index=False)
    log.info("Saved copy of original STURM prices to %s", original_price_input_backup)

    # Retrieve new energy commodity prices from the scenario
    df_prices = scenario.var(
        "PRICE_COMMODITY",
        filters={
            "level": "final",
            "commodity": [
                "biomass",
                "coal",
                "lightoil",
                "gas",
                "electr",
                "d_heat",
            ],
        },
    )

    # Map R12 regions to R11 regions
    # R12_CHN -> R11_CHN
    # R12_RCPA -> R11_CPA
    # Other R12_* -> R11_* (replace R12_ with R11_)
    def map_r12_to_r11(node):
        """Map R12 region codes to R11 region codes"""
        if node == "R12_CHN":
            return "R11_CHN"
        elif node == "R12_RCPA":
            return "R11_CPA"
        elif node.startswith("R12_"):
            return node.replace("R12_", "R11_")
        else:
            return node  # Keep as is if not R12

    # Apply the mapping
    df_prices["node"] = df_prices["node"].apply(map_r12_to_r11)

    # Identify key columns for merging
    key_cols = ["node", "commodity", "level", "year", "time"]
    # Filter to only columns that exist in both dataframes
    key_cols = [
        col
        for col in key_cols
        if col in df_prices_ori.columns and col in df_prices.columns
    ]

    # Merge the original dataframe with price data
    df_updated = pd.merge(
        df_prices_ori,
        df_prices[key_cols + ["lvl"]],
        on=key_cols,
        how="left",
        suffixes=("", "_new"),
    )

    rows_updated = (
        df_updated["lvl_new"].notna().sum() if "lvl_new" in df_updated.columns else 0
    )

    lvl_original = df_updated["lvl"].copy()
    lvl_scenario = df_updated["lvl_new"].fillna(df_updated["lvl"])

    # Calculate the factor (ratio) between scenario and original values for analysis
    # Factor = scenario / original
    # Factor < 1 means scenario is lower than original
    factor = np.where(lvl_original != 0, lvl_scenario / lvl_original, np.nan)

    # For rows where factor < 1 (scenario < original), use original value
    # Otherwise, use scenario value
    df_updated["lvl"] = np.where(
        (factor < 1) & (df_updated["lvl_new"].notna()), lvl_original, lvl_scenario
    )
    df_updated = df_updated.drop(columns=["lvl_new"])

    # Save the updated prices to the default price input file in STURM
    df_updated.to_csv(original_price_input_file, index=False)
    log.info("Updated prices saved to %s", original_price_input_file)
    log.info("Total rows: %d", len(df_updated))
    log.info("Rows with updated prices: %d", rows_updated)

    # Run STURM (via Rscript)
    for name in ("run_STURM_bmt_resid.R", "run_STURM_bmt_comm.R"):
        script = sturm_dir.joinpath(name)
        if not script.is_file():
            raise FileNotFoundError(f"STURM BMT R script not found: {script}")
        log.info("Running Rscript %s (cwd=%s)", name, sturm_dir)
        subprocess.run(
            ["Rscript", name],
            cwd=sturm_dir,
            check=True,
        )

    return scenario


def call_buildings_demand(context: Context, scenario: Scenario) -> Scenario:
    """Retrieve buildings demand from message_buildings_dir and add to scenario."""
    # Support both key spellings in local ixmp config.
    buildings_root = _message_buildings_install_dir()

    temp_dir = buildings_root.joinpath("message_ix_buildings", "sturm", "temp")
    if not temp_dir.exists():
        raise FileNotFoundError(f"Buildings demand directory not found: {temp_dir}")

    demand = pd.concat(
        [
            pd.read_csv(temp_dir / name)
            for name in ("resid_sturm.csv", "comm_sturm.csv")
        ],
        ignore_index=True,
    )

    exclude_expr = r"_mat_|_floor_|other_uses_|v_no_heat|_cook_|_apps_"
    # TODO: do we need dynamic materials demand for CircEUlar too?
    demand = demand[~demand["commodity"].str.contains(exclude_expr, na=False)].copy()
    demand["level"] = "useful"
    # TODO: "useful" to match build; consider unifying demand levels to "final"

    with scenario.transact("Add Buildings demand from message_ix_buildings/sturm/temp"):
        scenario.add_par("demand", demand)

    log.info("Added %d Buildings demand rows from %s", len(demand), temp_dir)
    return scenario
