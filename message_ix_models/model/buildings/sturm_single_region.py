"""Interface to single-region (WEU) STURM — METHOD-B style R runners.

Mirrors :func:`message_ix_models.model.buildings.sturm._sturm_rscript_B`:

- Write a prices data frame to STURM ``data/input_prices_R12.csv``.
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
    from message_ix_models.model.buildings.config import Config

log = logging.getLogger(__name__)

#: R files for the WEU single-region STURM run (start with resid + comm).
RSCRIPT_WEU_FILES = [
    "run_STURM_resid_WEU_2026.R",
    "run_STURM_comm_WEU_2026.R",
]


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


def run(context: Context, prices: pd.DataFrame | None = None) -> None:
    """Run WEU STURM scripts (METHOD-B style).

    Parameters
    ----------
    context :
        :attr:`context.buildings` must be configured.
    prices :
        If given, written to STURM ``data/input_prices_R12.csv`` (same as
        :func:`._sturm_rscript_B`). If :obj:`None`, the existing price file is
        left unchanged (baseline / no price feedback).
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

    scenario_id = config.code or config.sturm_scenario or "SSP2"
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
