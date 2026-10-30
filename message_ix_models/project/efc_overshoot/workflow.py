"""Workflow for overshoot (peak-and-decline) scenarios on the EFC model.

Per peak level the chain is ENGAGE-style:

- ``base``: climate bounds and taxes removed; floor on global CO2.
- ``EN1``: cumulative budget on World ``TCE_CO2`` over the budget window. This is the
  only step that shapes the peak P and end point F.
- ``EN2``: the EN1 World ``TCE_CO2`` path becomes an annual bound; budget and floor
  are removed.
- ``EN3``: the EN2 World ``TCE_CO2`` price is applied as a World ``TCE_non-CO2`` tax.

The chain stops at EN3: ENGAGE step_4 removes every tax, including the EN3 one.

Every level starts from ``baseline macro``: MACRO calibrated on our own solved
reference, so each level can carry a MESSAGE-MACRO solve whose demand is transferred
to its other steps (:func:`transfer_demand`).
"""

import hashlib
import logging
import shutil
import subprocess
from collections.abc import Iterable, Mapping
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from message_ix_models.util import identify_nodes, local_data_path, package_data_path
from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context

log = logging.getLogger(__name__)

#: Mt C per Gt CO₂.
MTC_PER_GTCO2 = 1000 * 12 / 44

#: Relation carrying global FFI + AFOLU CO₂, bounded below by the floor. ENGAGE
#: step_2 removes the floor through ``context.model.relation_global_co2``; the two
#: must name the same relation.
GLOBAL_CO2_RELATION = "CO2_Emission_Global_Total"

#: ``type_emission`` values treated as climate policy and removed in the base step.
#: Others, e.g. the IMO shipping bound on ``CO2_shipping_IMO``, are baseline policy.
CLIMATE_TYPE_EMISSION = ("TCE", "TCE_CO2", "TCE_non-CO2")

_REQUIRED = ("model", "parent", "first_model_year", "budget", "levels", "macro")

#: Index of the ``demand`` parameter and the ``DEMAND`` variable.
DEMAND_DIMS = ["node", "commodity", "level", "year", "time"]


def load_config(path: Path | None = None) -> dict[str, Any]:
    """Read and check the workflow configuration."""
    import yaml

    with open(path or package_data_path("efc_overshoot", "config.yaml")) as f:
        config = yaml.safe_load(f)

    missing = [k for k in _REQUIRED if k not in config]
    if missing:
        raise ValueError(f"efc_overshoot config lacks {missing}")
    b = config["budget"]
    if not b["first_year"] < b["last_year"]:
        raise ValueError(f"Budget window {b['first_year']}–{b['last_year']} is empty")
    if not config["levels"]:
        raise ValueError("efc_overshoot config defines no peak levels")
    for name, level in config["levels"].items():
        if not {"F", "floor"} <= set(level):
            raise ValueError(f"Level {name!r} needs both F and floor; got {level}")
        if level["floor"] > 0:
            raise ValueError(f"Level {name!r}: floor {level['floor']} must be ≤ 0")
    return config


def gtco2_to_mtc(value: float) -> float:
    """Convert Gt CO₂ to Mt C."""
    return value * MTC_PER_GTCO2


def window_years(model_years: Iterable[int], first: int, last: int) -> list[int]:
    """Model years inside the budget window, inclusive."""
    years = sorted(y for y in model_years if first <= y <= last)
    if not years:
        raise ValueError(f"No model years in budget window {first}–{last}")
    return years


def budget_mtc(F: float, duration: pd.Series) -> float:
    """Average annual bound (Mt C/yr) that caps cumulative emissions at `F` Gt CO₂.

    MESSAGE's EMISSION_CONSTRAINT bounds Σ duration·EMISS / Σ duration, so the
    cumulative quantity is divided by the summed period durations of the window.
    """
    total = float(duration.sum())
    if not total > 0:
        raise ValueError(f"Budget window has total duration {total}")
    return gtco2_to_mtc(F) / total


def check_trajectory(emiss: pd.DataFrame, model_years: Iterable[int]) -> None:
    """Raise unless `emiss` has a finite World TCE_CO2 level for every model year.

    ENGAGE step_2 removes the cumulative budget and the floor before adding the
    trajectory; an empty trajectory would leave the scenario with no CO₂ constraint.
    """
    sel = emiss.query("node == 'World' and emission == 'TCE_CO2' and type_tec == 'all'")
    have = set(sel.loc[sel["lvl"].notna(), "year"].astype(int))
    missing = sorted(set(model_years) - have)
    if missing:
        raise ValueError(f"EN1 solution lacks World TCE_CO2 EMISS for {missing}")


def tax_from_price(
    price: pd.DataFrame, type_emission: str = "TCE_non-CO2"
) -> pd.DataFrame:
    """``tax_emission`` data at World for `type_emission`, from the World CO₂ price.

    Only ``PRICE_EMISSION[World, TCE_CO2, all, year]`` is used. Regional rows and other
    types (e.g. ``CO2_shipping_IMO``) would otherwise be relabelled as non-CO₂ taxes.
    """
    sel = price.query(
        "node == 'World' and type_emission == 'TCE_CO2' and type_tec == 'all'"
    )
    if sel.empty:
        raise ValueError("No PRICE_EMISSION for World TCE_CO2")
    if sel["lvl"].isna().any():
        raise ValueError("PRICE_EMISSION for World TCE_CO2 contains NaN")
    if not (sel["lvl"] != 0).any():
        raise ValueError("PRICE_EMISSION for World TCE_CO2 is zero in every year")
    return pd.DataFrame(
        dict(
            node="World",
            type_emission=type_emission,
            type_tec="all",
            type_year=sel["year"].astype(int).astype(str).values,
            value=sel["lvl"].values,
            unit="USD/tC",
        )
    )


@cache
def code_revision() -> str:
    """Commit of the checkout this module runs from, marked ``-dirty`` if modified."""
    here = Path(__file__).parent
    try:
        rev = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=here,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--", "."],
            cwd=here.parent.parent,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return rev + ("-dirty" if dirty else "")


def demand_from_solution(demand: pd.DataFrame, DEMAND: pd.DataFrame) -> pd.DataFrame:
    """Rows of the `demand` parameter with values from the solved `DEMAND`.

    Each row keeps its unit from `demand`: message_data's ``transfer_demands`` labels
    every row GWa, but this model's demands are also in t, Gp * km / a and Gt * km / a.
    Only rows that `DEMAND` covers are returned, so rows outside the solved horizon
    (e.g. historical years) stay as they are when the result is added.
    """
    if DEMAND["lvl"].isna().any():
        raise ValueError("DEMAND contains NaN")
    dup = demand.duplicated(DEMAND_DIMS).sum()
    if dup:
        raise ValueError(f"demand has {dup} duplicate keys")
    m = demand.merge(DEMAND[DEMAND_DIMS + ["lvl"]], on=DEMAND_DIMS, how="right")
    orphan = m[m["unit"].isna()]
    if len(orphan):
        raise ValueError(
            f"{len(orphan)} DEMAND rows have no demand row in the target, e.g. "
            f"{orphan[DEMAND_DIMS].head(3).to_dict('records')}"
        )
    return m.assign(value=m["lvl"])[DEMAND_DIMS + ["value", "unit"]]


def transfer_demand(source: "Scenario", target: "Scenario") -> None:
    """Set the demand parameter of `target` to the solved DEMAND of `source`."""
    if not source.has_solution():
        raise ValueError(f"{source.url} has no solution to take DEMAND from")
    if target.has_solution():
        raise ValueError(f"{target.url} has a solution; clone it without one first")
    data = demand_from_solution(target.par("demand"), source.var("DEMAND"))
    with target.transact(f"Demand from DEMAND of {source.url}"):
        target.add_par("demand", data)
    log.info(f"Transferred {len(data)} demand rows from {source.url}")


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def copy_macro_workbook(template: Path, dest: Path) -> None:
    """Copy the shipped calibration workbook to `dest`, which is refreshed instead.

    material's ``update_macro_calib_file`` writes into the workbook it is given; the
    shipped one (``data/material/macro/``) is shared with the EFC workflow.
    """
    if dest.resolve() == template.resolve():
        raise ValueError(f"Refusing to use the shipped workbook {template} as the copy")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(template, dest)


def macro_demand_gap(demand: pd.DataFrame, DEMAND: pd.DataFrame) -> float:
    """Largest relative difference between solved DEMAND and the demand parameter.

    A calibrated MESSAGE-MACRO reference reproduces its own demand.
    """
    m = demand.merge(DEMAND, on=DEMAND_DIMS, how="inner")
    if m.empty:
        raise ValueError("demand and DEMAND share no rows")
    rel = (m["lvl"] - m["value"]).abs() / m["value"].abs().where(m["value"] != 0, 1.0)
    return float(rel.max())


def _stamp(scenario: "Scenario", step: str) -> None:
    """Record which code built `scenario`; B/L calibration is only valid per build."""
    scenario.set_meta(
        {"efc_overshoot_rev": code_revision(), "efc_overshoot_step": step}
    )


def _remove_solution(scenario: "Scenario") -> None:
    try:
        scenario.remove_solution()
    except ValueError:
        pass  # No solution


def _solve(context: "Context", scenario: "Scenario", model: str = "MESSAGE") -> None:
    from message_ix_models.project.efc.workflow import solve

    solve(context, scenario, model=model)


def build_baseline(context: "Context", scenario: "Scenario") -> "Scenario":
    """Add the EFC hydrogen technologies to the parent and solve."""
    from message_ix_models.project.efc.workflow import build_hydrogen

    build_hydrogen(context, scenario)
    _solve(context, scenario)
    _stamp(scenario, "baseline")
    return scenario


def solve_reference(context: "Context", scenario: "Scenario") -> "Scenario":
    """Prepare the reference for MACRO calibration and solve it with MESSAGE.

    BMT's prep_for_macro removes the MACRO years from ``cat_year`` and the sectors
    the calibration workbook lacks, then solves. Its solve passes --cap_comm=1, which
    is a no-op here: the parent has no input_cap*/output_cap* data.
    """
    from message_ix_models.model.bmt.workflow import prep_for_macro

    prep_for_macro(context, scenario)
    _stamp(scenario, "baseline_fmy")
    return scenario


def calibrate_macro(context: "Context", scenario: "Scenario") -> "Scenario":
    """Calibrate MACRO on the solved reference; solve and check MESSAGE-MACRO.

    The calibration workbook is a copy, refreshed from `scenario`'s solution; the
    shipped one is never written and is checked byte-identical afterwards. Returns
    the calibrated clone, named by message_ix ``{scenario}_macro``.
    """
    from message_ix_models.model.material.data_util import add_macro_materials
    from message_ix_models.model.material.util import update_macro_calib_file

    config = context["efc_overshoot"]["macro"]
    template = package_data_path("material", "macro", config["template"])
    dest = local_data_path(
        "efc_overshoot",
        "macro",
        f"macro_calibration_{scenario.scenario}_v{scenario.version}.xlsx",
        context=context,
    ).resolve()
    before = file_digest(template)

    copy_macro_workbook(template, dest)
    # Both functions join their argument to data/material/macro/; an absolute path
    # replaces that base, so they read and write `dest` only.
    update_macro_calib_file(scenario, str(dest))
    calibrated = add_macro_materials(scenario, str(dest))

    if file_digest(template) != before:
        raise RuntimeError(f"Shipped MACRO workbook {template} changed")

    _solve(context, calibrated, model="MESSAGE-MACRO")
    gap = macro_demand_gap(calibrated.par("demand"), calibrated.var("DEMAND"))
    log.info(f"MACRO reproduces reference demand within {gap:.2e}")
    if gap > config["demand_tolerance"]:
        raise ValueError(
            f"Calibrated MESSAGE-MACRO deviates from reference demand by {gap:.2e} "
            f"> {config['demand_tolerance']}"
        )
    calibrated.set_meta(
        {
            "efc_overshoot_macro_workbook": str(dest),
            "efc_overshoot_macro_workbook_sha256": file_digest(dest),
            "efc_overshoot_macro_gap": gap,
        }
    )
    _stamp(calibrated, "baseline_macro")
    return calibrated


def step_0(context: "Context", scenario: "Scenario", *, level: str) -> "Scenario":
    """Remove climate bounds and taxes; add the floor on global CO₂."""
    from message_ix_models.tools import add_CO2_emission_constraint

    config = context["efc_overshoot"]
    floor = config["levels"][level]["floor"]
    _remove_solution(scenario)

    with scenario.transact(f"Remove climate policy {CLIMATE_TYPE_EMISSION}"):
        for par in ("bound_emission", "tax_emission"):
            df = scenario.par(
                par, filters={"type_emission": list(CLIMATE_TYPE_EMISSION)}
            )
            if len(df):
                log.info(f"Remove {len(df)} {par} rows")
                scenario.remove_par(par, df)

    add_CO2_emission_constraint.main(
        scenario,
        relation_name=GLOBAL_CO2_RELATION,
        constraint_value=gtco2_to_mtc(floor),
        type_rel="lower",
        reg=f"{identify_nodes(scenario)}_GLB",
    )
    _stamp(scenario, f"{level} base")
    return scenario


def en1(context: "Context", scenario: "Scenario", *, level: str) -> "Scenario":
    """Cumulative World TCE_CO₂ budget over the window; solve."""
    from message_ix_models import ScenarioInfo
    from message_ix_models.tools.add_budget import main as add_budget

    config = context["efc_overshoot"]
    b = config["budget"]
    info = ScenarioInfo(scenario)
    years = window_years(info.Y, b["first_year"], b["last_year"])
    duration = scenario.par("duration_period", filters={"year": years})
    value = budget_mtc(config["levels"][level]["F"], duration["value"])
    log.info(f"{level}: budget {value:.2f} Mt C/yr over {years[0]}–{years[-1]}")

    with scenario.transact(f"Add type_year {b['type_year']!r} for the budget"):
        scenario.add_set("type_year", b["type_year"])
        scenario.add_set(
            "cat_year", pd.DataFrame(dict(type_year=b["type_year"], year=years))
        )

    add_budget(
        scenario,
        value,
        type_emission="TCE_CO2",
        type_year=b["type_year"],
        unit="Mt C/yr",
    )

    after = [y for y in info.Y if y > b["last_year"]]
    if after and config.get("bound_after_window") is not None:
        with scenario.transact(f"Bound World TCE_CO2 in {after}"):
            scenario.add_par(
                "bound_emission",
                pd.DataFrame(
                    dict(
                        node="World",
                        type_emission="TCE_CO2",
                        type_tec="all",
                        type_year=[str(y) for y in after],
                        value=gtco2_to_mtc(config["bound_after_window"]),
                        unit="Mt C/yr",
                    )
                ),
            )

    _solve(context, scenario)
    _stamp(scenario, f"{level} EN1")
    return scenario


def en2(context: "Context", scenario: "Scenario", *, level: str) -> "Scenario":
    """EN1 World TCE_CO₂ path as annual bounds (ENGAGE step_2); solve."""
    from message_ix_models import ScenarioInfo
    from message_ix_models.project.engage.workflow import PolicyConfig, step_2

    config = context["efc_overshoot"]
    type_year = config["budget"]["type_year"]
    if context.model.relation_global_co2 != GLOBAL_CO2_RELATION:
        raise ValueError(
            f"step_2 would remove {context.model.relation_global_co2!r}, "
            f"not the floor on {GLOBAL_CO2_RELATION!r}"
        )

    info = ScenarioInfo(scenario)
    check_trajectory(
        scenario.var("EMISS", filters={"node": ["World"], "type_tec": ["all"]}), info.Y
    )

    step_2(context, scenario, PolicyConfig())

    # step_2 removes only type_year "cumulative"; the window budget would otherwise
    # share the CO₂ dual with the annual bounds and distort PRICE_EMISSION.
    with scenario.transact(f"Remove budget on type_year {type_year!r}"):
        df = scenario.par("bound_emission", filters={"type_year": [type_year]})
        scenario.remove_par("bound_emission", df)

    bounds = scenario.par(
        "bound_emission",
        filters={"node": ["World"], "type_emission": ["TCE_CO2"], "type_tec": ["all"]},
    )
    if set(bounds["type_year"]) != {str(y) for y in info.Y}:
        raise ValueError(
            f"EN2 World TCE_CO2 bounds cover {sorted(bounds['type_year'])}, "
            f"not the model years {info.Y}"
        )

    _solve(context, scenario)
    _stamp(scenario, f"{level} EN2")
    return scenario


def en3(context: "Context", scenario: "Scenario", *, level: str) -> "Scenario":
    """EN2 World CO₂ price as a World non-CO₂ tax; solve."""
    tax = tax_from_price(scenario.var("PRICE_EMISSION"))
    _remove_solution(scenario)

    with scenario.transact("Add World TCE_CO2 price as TCE_non-CO2 tax"):
        scenario.add_par("tax_emission", tax)

    _solve(context, scenario)
    _stamp(scenario, f"{level} EN3")
    return scenario


def report(context: "Context", scenario: "Scenario") -> "Scenario":
    from message_ix_models.project.efc.workflow import report as efc_report

    return efc_report(context, scenario)


def configure_context(context: "Context", config: Mapping[str, Any]) -> str:
    """Set up `context` for the workflow; return the platform name."""
    import yaml

    from message_ix_models.model.transport.config import Config as TransportConfig

    platform = config.get("platform") or context.platform_info.get("name")
    if not platform:
        raise ValueError(
            "No platform: set `platform` in data/efc_overshoot/config.yaml or pass "
            "--platform"
        )
    context.platform_info["name"] = platform
    context.ssp = "SSP2"
    context.model.regions = "R12"
    # Read by message_data's ScenarioRunner, which ENGAGE step_2 instantiates
    context.run_reporting_only = False
    context["efc_overshoot"] = dict(config)

    # Transport reporting inside report() needs the transport configuration that
    # apply_bmt_config() reads. Its Buildings part is not used: the chain is MT, and
    # BuildingsConfig requires a MESSAGEix-Buildings checkout.
    with open(package_data_path("bmt", "config.yaml")) as f:
        bmt = yaml.safe_load(f) or {}
    TransportConfig.from_context(context, options=bmt.get("transport", {}))
    return platform


def generate(context: "Context", config: Mapping[str, Any] | None = None) -> Workflow:
    """Create the overshoot workflow."""
    config = config or load_config()
    wf = Workflow(context)
    platform = configure_context(context, config)

    url = f"ixmp://{platform}/{config['model']}"
    p = config["parent"]
    fmy = config["first_model_year"]

    wf.add_step(
        "parent",
        None,
        target=f"ixmp://{platform}/{p['model']}/{p['scenario']}#{p['version']}",
    )
    wf.add_step(
        "baseline",
        "parent",
        build_baseline,
        target=f"{url}/os_baseline",
        clone=dict(keep_solution=False),
    )
    # Earlier periods become fixed from the baseline solution, so the clone needs it
    wf.add_step(
        "baseline fmy",
        "baseline",
        solve_reference,
        target=f"{url}/os_baseline_fmy{fmy}",
        clone=dict(keep_solution=True, shift_first_model_year=fmy),
    )
    wf.add_step("baseline fmy reported", "baseline fmy", report)
    # add_macro clones to "{scenario}_macro" itself; the target names that clone so
    # that --from "baseline macro" loads it
    wf.add_step(
        "baseline macro",
        "baseline fmy",
        calibrate_macro,
        target=f"{url}/os_baseline_fmy{fmy}_macro",
    )

    for level in config["levels"]:
        wf.add_step(
            f"{level} base",
            "baseline macro",
            step_0,
            target=f"{url}/os_{level}_base",
            clone=dict(keep_solution=False),
            level=level,
        )
        wf.add_step(
            f"{level} EN1",
            f"{level} base",
            en1,
            target=f"{url}/os_{level}_EN1",
            clone=dict(keep_solution=False),
            level=level,
        )
        # EN2 and EN3 read the previous solution (EMISS, PRICE_EMISSION)
        wf.add_step(
            f"{level} EN2",
            f"{level} EN1",
            en2,
            target=f"{url}/os_{level}_EN2",
            clone=dict(keep_solution=True),
            level=level,
        )
        wf.add_step(
            f"{level} EN3",
            f"{level} EN2",
            en3,
            target=f"{url}/os_{level}_EN3",
            clone=dict(keep_solution=True),
            level=level,
        )
        wf.add_step(f"{level} EN3 reported", f"{level} EN3", report)

    return wf
