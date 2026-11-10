"""Interface to single-region (WEU) STURM — METHOD-B style R runners.

Mirrors :func:`message_ix_models.model.buildings.sturm._sturm_rscript_B`:

- Write a prices data frame to STURM ``data/input_prices_R12.csv``.
- Write RC final energy (genno ``in`` for :data:`RC_FE_TECHS`) to
  :file:`message_linking/final_energy_rc.csv`.
- Write the scenario assumption set to :file:`message_linking/config.yaml`
  (WEU runners).
- Invoke the message-ix-buildings runners.

TODO: maybe other ways to call message-ix-buildings instead of Rscript later
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import yaml

from message_ix_models import Context
from message_ix_models.model.buildings.sturm import METHOD as STURM_METHOD

if TYPE_CHECKING:
    import message_ix

    from message_ix_models.model.buildings.config import Config

log = logging.getLogger(__name__)

#: R files for the WEU single-region STURM run (start with resid + comm).
RSCRIPT_WEU_FILES = [
    "run_STURM_resid_WEU_2026.R",
    "run_STURM_comm_WEU_2026.R",
]

#: RC final energy techs
RC_FE_TECHS = [
    "biomass_rc",
    "coal_rc",
    "elec_rc",
    "eth_rc",
    "foil_rc",
    "gas_rc",
    "h2_fc_RC",
    "h2_rc",
    "heat_rc",
    "hp_el_rc",
    "loil_rc",
    "meth_rc",
    "sp_el_RC",
    "sp_el_RC_RT",
]

#: Default CSV name under STURM ``message_linking/`` for RC final energy.
DEFAULT_FE_CSV = "final_energy_rc.csv"


def _ensure_sturm_dirs(context: Context) -> tuple[Path, Path]:
    """Return ``(sturm_code_dir, sturm_input_dir)`` under message-ix-buildings."""
    config: "Config" = context.buildings
    config.sturm_method = STURM_METHOD.RSCRIPT_B
    code_dir = config.sturm_code_dir
    input_dir = config.sturm_input_dir
    if not code_dir.exists():
        raise FileNotFoundError(
            f"STURM code directory not found: {code_dir}. "
            "Install / configure message_ix_buildings."
        )
    return code_dir, input_dir


def _write_scenario_config(path: Path, scenarios: list[str]) -> Path:
    """Write ``scenarios: […]`` YAML for WEU runners (local copy of B-style write)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"scenarios": list(scenarios)}
    header = (
        "# Shared STURM scenario list (WEU / single-region)\n"
        "# Written by message_ix_models.model.buildings.sturm_single_region\n"
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False)
    log.info("Wrote STURM scenarios %s to %s", payload, path)
    return path


def get_rc_final_energy(
    context: Context, scenario: "message_ix.Scenario"
) -> pd.DataFrame:
    """Return RC final energy from genno ``in`` (input × ACT) for :data:`RC_FE_TECHS`.
    """
    from message_ix_models.report import prepare_reporter

    if not scenario.has_solution():
        raise ValueError(
            f"Scenario {scenario.url} has no solution; cannot compute RC final energy"
        )

    rep, _ = prepare_reporter(context, scenario=scenario)
    rep.add("rc fe in", "select", "in:nl-t-ya-c-l", dict(t=list(RC_FE_TECHS)))
    qty = rep.get("rc fe in")
    df = (
        qty.to_dataframe()
        .reset_index()
        .rename(
            columns={
                "nl": "node",
                "t": "technology",
                "ya": "year",
                "c": "commodity",
                "l": "level",
            }
        )
    )
    value_col = "value" if "value" in df.columns else df.columns[-1]
    if value_col != "value":
        df = df.rename(columns={value_col: "value"})

    unit = str(getattr(qty, "units", "") or "") or "GWa/a"
    df = df.assign(unit=unit)

    keep = ["node", "technology", "year", "commodity", "level", "value", "unit"]
    df = df.loc[:, [c for c in keep if c in df.columns]]
    log.info(
        "RC final energy (in): %d rows for %d techs on %s",
        len(df),
        df["technology"].nunique() if not df.empty else 0,
        scenario.url,
    )
    return df


def write_rc_final_energy(
    context: Context,
    scenario: "message_ix.Scenario",
    path: Path | None = None,
) -> Path:
    """Write :func:`get_rc_final_energy` to STURM ``message_linking/`` (or `path`)."""
    code_dir, _ = _ensure_sturm_dirs(context)
    out = path or code_dir.joinpath("message_linking", DEFAULT_FE_CSV)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = get_rc_final_energy(context, scenario)
    df.to_csv(out, index=False)
    log.info("Wrote RC final energy (%d rows) to %s", len(df), out)
    return out


def run(
    context: Context,
    prices: pd.DataFrame | None = None,
    scenario: "message_ix.Scenario | None" = None,
) -> None:
    """Run WEU STURM scripts (METHOD-B style).

    Parameters
    ----------
    context :
        :attr:`context.buildings` must be configured.
    prices :
        If given, written to STURM ``data/input_prices_R12.csv`` (same as
        :func:`._sturm_rscript_B`). If :obj:`None`, the existing price file is
        left unchanged (baseline / no price feedback).
    scenario :
        If given (and solved), write RC final energy via
        :func:`write_rc_final_energy` before invoking R.
    """
    from .config import DEFAULT_DATA_PATHS

    config: "Config" = context.buildings
    code_dir, input_dir = _ensure_sturm_dirs(context)
    assert input_dir.exists()

    if prices is not None:
        price_name = Path(config.data_paths.get("prices", DEFAULT_DATA_PATHS["prices"]))
        price_input = input_dir.joinpath(price_name.name)
        prices.to_csv(price_input, index=False)
        log.info("Updated prices written to %s (%d rows)", price_input, len(prices))
    else:
        log.info("No prices passed; keeping existing STURM price file")

    if scenario is not None:
        write_rc_final_energy(context, scenario)
    else:
        log.info("No scenario passed; skipping RC final energy export")

    scenario_id = config.code or "SSP2"
    _write_scenario_config(
        code_dir.joinpath("message_linking", "config.yaml"),
        [scenario_id],
    )

    for name in RSCRIPT_WEU_FILES:
        script = code_dir.joinpath(name)
        if not script.exists():
            raise FileNotFoundError(f"STURM R script not found: {script}")
        command = ["Rscript", name]
        log.info("Running %s (cwd=%s)", command, code_dir)
        subprocess.check_call(command, cwd=code_dir)
