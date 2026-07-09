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


# MIXB demand CSV basenames under ``sturm/message_linking``
# ({code} = :func:`format_sturm_code` applied to ``context.buildings.code``).
_MIXB_DEMAND_CSV = (
    "resid_sturm_aligned_{code}.csv",
    "comm_sturm_aligned_{code}.csv",
    "resid_comm_glance_aligned_{code}.csv",
)


def format_sturm_code(code: str, sturm_scen: str = "r") -> str:
    """Return MIXB filename code suffix under ``sturm/message_linking``.
    """
    return code + "_" + sturm_scen if code != "R" else code


def message_linking_path(context: Context, attr: str) -> Path:
    """Resolve :attr:`~.buildings.Config.data_paths` entry to a CSV path.

    Absolute paths are returned unchanged. Relative paths are resolved under
    ``message_ix_buildings/sturm/message_linking``, with ``{code}`` substituted using
    :func:`format_sturm_code`.
    """
    val = context.buildings.data_paths[attr]
    path = Path(val)
    if path.is_absolute():
        return path
    code = format_sturm_code(context.buildings.code)
    return (
        _message_buildings_install_dir()
        .joinpath("message_ix_buildings", "sturm", "message_linking")
        .joinpath(str(val).format(code=code))
    )


def _pass_scen_config_to_mixb(sturm_dir: Path, scenarios: list[str]) -> None:
    """Write ``scenario_config.yaml`` for MESSAGEix-Buildings STURM runners."""
    import yaml

    path = sturm_dir.joinpath("scenario_config.yaml")
    payload = {"scenarios": scenarios}
    header = (
        "# Shared STURM scenario list\n"
        "# Used by STURM / MIXB runner scripts (see message_ix_buildings/sturm)\n"
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False)
    log.info("Wrote STURM scenarios %s to %s", scenarios, path)


def _write_sturm_prices(
    scenario: Scenario, price_default: Path, price_input: Path
) -> int:
    """Write ``input_prices_R12.csv`` for STURM.

    If `scenario` has a solution, merge ``PRICE_COMMODITY`` into the reference
    levels from `price_default` (with floors). Otherwise copy `price_default` unchanged.

    Returns
    -------
    int
        Number of price rows updated from the scenario solution.
    """
    df_prices_ori = pd.read_csv(price_default)

    if not scenario.has_solution():
        log.info(
            "Scenario has no solution; writing reference prices from %s to %s",
            price_default,
            price_input,
        )
        df_prices_ori.to_csv(price_input, index=False)
        return 0

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
    # TODO: maybe suggest MixB colleagues to update to avoid this
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
    # Factor = scenario / original; factor < 1 means scenario is below STURM reference.
    factor = np.where(lvl_original != 0, lvl_scenario / lvl_original, np.nan)

    has_scenario = df_updated["lvl_new"].notna()
    below_reference = (factor < 1) & has_scenario
    # Floor non-electricity prices at the STURM reference; allow lower electr prices.
    use_reference_floor = below_reference & (df_updated["commodity"] != "electr")
    df_updated["lvl"] = np.where(use_reference_floor, lvl_original, lvl_scenario)
    df_updated = df_updated.drop(columns=["lvl_new"])

    # STURM R scripts read input_prices_R12.csv; default file is left unchanged.
    df_updated.to_csv(price_input, index=False)
    return rows_updated


def call_sturm(context: Context, scenario: Scenario) -> Scenario:
    """Merge scenario prices into STURM inputs, then run MESSAGEix-Buildings STURM.

    Read reference levels from ``input_prices_R12_default.csv``. If `scenario` has a
    solution, apply ``PRICE_COMMODITY`` (with floors) and write
    ``input_prices_R12.csv``; otherwise copy the reference file unchanged. Update
    ``scenario_config.yaml`` from :attr:`context.buildings.code`, then run STURM.
    """
    buildings_root = _message_buildings_install_dir()
    sturm_dir = buildings_root.joinpath("message_ix_buildings", "sturm")
    price_dir = sturm_dir.joinpath("data")

    price_default = price_dir.joinpath("input_prices_R12_default.csv")
    price_input = price_dir.joinpath("input_prices_R12.csv")

    if not price_default.exists():
        raise FileNotFoundError(f"STURM reference prices not found: {price_default}")

    rows_updated = _write_sturm_prices(scenario, price_default, price_input)
    log.info("Updated prices written to %s (reference: %s)", price_input, price_default)
    log.info("Rows with updated prices: %d", rows_updated)

    code = context.buildings.code
    _pass_scen_config_to_mixb(sturm_dir, [code + "_r" if code != "R" else code])

    # Run STURM (via Rscript)
    for name in (
        "run_STURM_Circular_resid_glo.R",
        "run_STURM_Circular_comm_glo.R",
        "run_GLANCE_placeholder.R",
        "run_MIXB_aligner.R",
    ):
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
    """Retrieve MIXB buildings demand from ``sturm/message_linking`` and add it."""
    buildings_root = _message_buildings_install_dir()
    linking_dir = buildings_root.joinpath(
        "message_ix_buildings", "sturm", "message_linking"
    )
    code = format_sturm_code(context.buildings.code)
    demand = pd.concat(
        [
            pd.read_csv(linking_dir / name.format(code=code))
            for name in _MIXB_DEMAND_CSV
        ],
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
