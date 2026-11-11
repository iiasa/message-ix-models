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
PLATFORM_NAME = "message-jdbc-pg"

#: Model name on the platform (scenario name comes from :data:`SCENARIO_XLSX`).
MODEL_NAME = "WEU_NEST_EXP1.2"

#: Carbon price at year :data:`MITIG_PRICE_YEAR` (USD / tC); linear from 0 at FMY.
MITIG_CARBON_PRICE = 400.0

#: Year at which :data:`MITIG_CARBON_PRICE` is reached.
MITIG_PRICE_YEAR = 2110


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


def prep_for_macro(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Prepare a WEU scenario for MACRO after :py:`shift_first_model_year=2030`.

    Mirrors :func:`.bmt.workflow.prep_for_macro` / :file:`run_exp12.py` ``macro_prep``:

    1. Drop ``initializeyear_macro`` / ``baseyear_macro`` from ``cat_year``.
    2. Drop ``rc_spec`` / ``rc_therm`` from ``sector`` (buildings demand stays
       exogenous; MACRO covers ``i_spec``, ``i_therm``, ``i_feed``, ``transport``).
    3. Solve with :py:`model="MESSAGE"` only.
    """
    log.info("Preparing WEU scenario for MACRO (FMY=%s)", scenario.firstmodelyear)
    scenario.set_as_default()

    df = scenario.set(
        "cat_year", {"type_year": ["initializeyear_macro", "baseyear_macro"]}
    )
    if df is not None and len(df) > 0:
        with scenario.transact("Remove init and baseyear for macro"):
            scenario.remove_set("cat_year", df)

    # Buildings end-uses stay exogenous; keep industry + transport for MACRO.
    # ``sector`` is a MACRO set — absent until MACRO was added previously.
    if "sector" in scenario.set_list():
        sectors_to_remove = ["rc_spec", "rc_therm"]
        existing = set(map(str, scenario.set("sector").tolist()))
        to_remove = [s for s in sectors_to_remove if s in existing]
        if to_remove:
            with scenario.transact("Remove rc_spec, rc_therm from sector set"):
                scenario.remove_set("sector", to_remove)
    else:
        log.info(
            "No MACRO sector set yet; MACRO sectors come from the workbook "
            "(exclude rc_spec / rc_therm there)"
        )

    solve(context, scenario, **solve_args())
    return scenario


def add_mitig(
    context: Context,
    scenario: message_ix.Scenario,
    *,
    price: float = MITIG_CARBON_PRICE,
    price_year: int = MITIG_PRICE_YEAR,
) -> message_ix.Scenario:
    """Add a TCE carbon tax (linear 0→`price` from FMY→`price_year`) and solve MM.

    ``tax_emission`` uses ``type_emission="TCE"``, node ``WEU``, ``type_tec="all"``.
    `price` is already in USD/tC.
    """
    import pandas as pd
    from message_ix import make_df

    from message_ix_models import ScenarioInfo

    if scenario.has_solution():
        scenario.remove_solution()

    info = ScenarioInfo(scenario)
    fmy = int(info.y0)
    years = [int(y) for y in info.Y]
    if price_year <= fmy:
        raise ValueError(f"price_year={price_year} must be after FMY={fmy}")

    # Linear ramp: 0 at FMY → price at price_year (clamp after).
    span = float(price_year - fmy)
    frac = pd.Series(
        [min(max((y - fmy) / span, 0.0), 1.0) for y in years],
        index=pd.Index(years, name="year"),
    )
    values = price * frac

    data = make_df(
        "tax_emission",
        value=values.values,
        type_year=values.index,
        node="WEU",
        type_emission="TCE",
        type_tec="all",
        unit="USD/tC",
    )
    log.info(
        "Adding tax_emission on TCE: linear 0→%.1f USD/tC from %s→%s",
        price,
        fmy,
        price_year,
    )
    with scenario.transact("Added linear TCE carbon price"):
        scenario.add_par("tax_emission", data)
    scenario.set_as_default()

    log.info("Solving MESSAGE-MACRO mitigation scenario")
    solve(context, scenario, **solve_args("MESSAGE-MACRO"))
    scenario.set_as_default()

    return scenario


def add_macro(
    context: Context,
    scenario: message_ix.Scenario,
    *,
    macro_file: str | None = None,
) -> message_ix.Scenario:
    """Add MACRO from a BMT workbook and solve MESSAGE-MACRO.

    `macro_file` overrides the default from ``config_single_region.yaml``
    (``macro:`` / ``context.macro``). Pass it via :meth:`.Workflow.add_step`
    when different branches need different calibration workbooks.
    """
    import pandas as pd

    from message_ix_models.model.material.data_util import (
        _filter_macro_data_to_scenario,
    )

    if macro_file is None:
        macro_file = getattr(context, "macro", None)
    if macro_file is None:
        bmt = getattr(context, "bmt", None)
        macro_file = bmt.get("macro") if isinstance(bmt, dict) else None
    if not macro_file:
        raise ValueError(
            "No MACRO workbook configured; pass macro_file= to the step or set "
            "macro: in config_single_region.yaml"
        )

    path = package_data_path("bmt", Path(macro_file).name)
    if not path.exists():
        raise FileNotFoundError(f"MACRO calibration workbook not found: {path}")

    log.info("Adding MACRO from %s", path)
    data = {
        k: v for k, v in pd.read_excel(path, sheet_name=None).items() if k != "README"
    }
    _filter_macro_data_to_scenario(data, scenario)
    scenario = scenario.add_macro(data, check_convergence=False)
    scenario.set_as_default()

    log.info("MACRO added; solving MESSAGE-MACRO")
    solve(context, scenario, **solve_args("MESSAGE-MACRO"))
    return scenario


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

    name = wf.add_step("base", None, target=base_url)
    name = wf.add_step("base solved", name, solve, **solve_args())

    name = wf.add_step(
        "base prep macro",
        "base solved",
        prep_for_macro,
        target=f"{url}/base_message",
        clone=dict(shift_first_model_year=2030),
    )
    name = wf.add_step(
        "base macro",
        name,
        add_macro,
        target=f"{url}/base_message_macro",
        macro_file="SSP2_WEU_NEST_EXP1.2_macro.xlsx",
    )
    name = wf.add_step(
        "mitig",
        name,
        add_mitig,
        target=f"{url}/mitig",
        clone=dict(keep_solution=False),
        price=MITIG_CARBON_PRICE,
        price_year=MITIG_PRICE_YEAR,
    )

    name = wf.add_step(
        "B built",
        "base solved",
        build_B,
        target=f"{url}/base_B",
        clone=dict(keep_solution=True),
        update_prices=False,
        update_demand_only=False,
    )
    name = wf.add_step("B solved", name, solve, **solve_args())
    name = wf.add_step(
        "B prep macro",
        name,
        prep_for_macro,
        target=f"{url}/base_B_message",
        clone=dict(shift_first_model_year=2030),
    )
    name = wf.add_step(
        "B macro",
        name,
        add_macro,
        target=f"{url}/base_B_message_macro",
        macro_file="SSP2_WEU_NEST_EXP1.2_macro_b_added.xlsx",
    )
    name = wf.add_step(
        "B mitig",
        name,
        add_mitig,
        target=f"{url}/mitig_B",
        clone=dict(keep_solution=False),
        price=MITIG_CARBON_PRICE,
        price_year=MITIG_PRICE_YEAR,
    )
    name = wf.add_step(
        "B mitig iterated",
        name,
        build_B,
        target=f"{url}/mitig_B_iterated",
        clone=dict(keep_solution=True),
        update_prices=True,
        update_demand_only=True,
    )
    name = wf.add_step(
        "B mitig iterated solved",
        name,
        solve,
        **solve_args(),
    )

    return wf
