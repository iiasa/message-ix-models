"""Single-region / single-country STURM linking workflow.

Intended for a MESSAGE scenario linked with a single-country STURM, driven by::

    mix-models bmt run --workflow=single-region TARGET [--from=STEP_A STEP_B]

"""

from __future__ import annotations

import logging
from pathlib import Path

import ixmp
import message_ix

from message_ix_models import Context
from message_ix_models.model.bmt.workflow import solve_args
from message_ix_models.model.workflow import solve
from message_ix_models.util import package_data_path
from message_ix_models.workflow import Workflow

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: File name of the MESSAGE Excel dump under ``data/bmt/``.
SCENARIO_XLSX = "SSP2_Baseline_WEU_NEST_EXP1.1"

#: ixmp platform that receives / already holds the scenario.
PLATFORM_NAME = "local"

#: Model name on the platform (scenario name comes from :data:`SCENARIO_XLSX`).
MODEL_NAME = "WEU_NEST_EXP1.2"


def _scenario_xlsx_path() -> Path:
    """Resolve ``data/bmt/{SCENARIO_XLSX}.xlsx``."""
    name = SCENARIO_XLSX if SCENARIO_XLSX.endswith(".xlsx") else f"{SCENARIO_XLSX}.xlsx"
    return package_data_path("bmt", name)


def _scenario_name() -> str:
    """Scenario name on the platform (= Excel file stem, ``.`` → ``p``)."""
    name = SCENARIO_XLSX
    if name.endswith(".xlsx"):
        name = name[: -len(".xlsx")]
    return name.replace(".", "p")


# ---------------------------------------------------------------------------
# Workflow step functions
# ---------------------------------------------------------------------------


def load_from_excel(context: Context) -> message_ix.Scenario:
    """Load a MESSAGE scenario from Excel onto :data:`PLATFORM_NAME`.

    1. Require the Excel file at ``data/bmt/{SCENARIO_XLSX}.xlsx``;
    2. Open :data:`PLATFORM_NAME`.
    3. If ``(MODEL_NAME, scenario_name)`` already exists there, load it from
       ixmp and return (no re-import).
    4. Otherwise create a new scenario (name = Excel stem), ``read_excel``,
       set as default, and return it.
    """
    path = _scenario_xlsx_path()
    if not path.exists():
        raise FileNotFoundError(
            f"Scenario Excel not found at {path}. "
            f"Put {path.name} in message_ix_models/data/bmt/ and try again."
        )

    scenario_name = _scenario_name()
    log.info(
        "Using platform=%s model=%s scenario=%s excel=%s",
        PLATFORM_NAME,
        MODEL_NAME,
        scenario_name,
        path,
    )

    mp = ixmp.Platform(PLATFORM_NAME)
    # Keep Platform alive for later workflow steps (Scenario only holds a weakref).
    context["_platform"] = mp
    try:
        listed = mp.scenario_list(default=False)
    except Exception as e:  # pragma: no cover
        log.warning("Could not list scenarios on %s: %s", PLATFORM_NAME, e)
        listed = None

    if listed is not None and not listed.empty:
        hit = listed[
            (listed["model"] == MODEL_NAME) & (listed["scenario"] == scenario_name)
        ]
        if not hit.empty:
            log.info(
                "Scenario already on %s (%d version(s)); loading from ixmp",
                PLATFORM_NAME,
                len(hit),
            )
            return message_ix.Scenario(mp, MODEL_NAME, scenario_name)

    log.info("Scenario not on platform; reading Excel into new scenario")
    s = message_ix.Scenario(
        mp,
        MODEL_NAME,
        scenario_name,
        version="new",
        annotation=f"Loaded from {path.name}",
    )
    s.read_excel(path, add_units=True, init_items=True, commit_steps=False)
    s.set_as_default()
    log.info("Loaded and set as default: ixmp://%s/%s", PLATFORM_NAME, s.url)
    return s


# ---------------------------------------------------------------------------
# Workflow graph
# ---------------------------------------------------------------------------


def generate(context: Context) -> Workflow:
    from message_ix_models.model.bmt.config import apply_bmt_config
    from message_ix_models.model.buildings.build_single_region import main as build_B

    apply_bmt_config(
        context, path=package_data_path("bmt", "config_single_region.yaml")
    )

    context.model.regions = "WEU"

    wf = Workflow(context)

    # Option 1: Excel import
    # name = wf.add("M from excel", load_from_excel, "context")

    # Option 2: start from a scenario already on the platform
    base_url = f"ixmp://{PLATFORM_NAME}/{MODEL_NAME}/{_scenario_name()}"
    url = f"ixmp://{PLATFORM_NAME}/{MODEL_NAME}"

    name = wf.add_step("M", None, target=base_url)
    name = wf.add_step("M solved", name, solve, **solve_args())

    name = wf.add_step(
        "B built",
        name,
        build_B,
        target=f"{url}/base_B",
        clone=dict(keep_solution=True),
        update_prices=False,
    )

    name = wf.add_step("B solved", name, solve, **solve_args())

    return wf
