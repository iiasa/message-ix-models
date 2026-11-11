"""Outcome checks for :mod:`.bmt.workflow_single_region`.

These are **not** pytest unit tests. They validate scenarios already stored on an
ixmp platform after a single-region workflow run, e.g.::

    mix-models bmt run --workflow=single-region TARGET

Run from Python, pointing at any ixmp platform that holds the workflow scenario
names::

    from message_ix_models.model.bmt.test_outcome_single_region import run_checks

    # defaults: platform / model from workflow_single_region
    run_checks()

    # or explicitly:
    run_checks(platform="local", model="WEU_NEST_EXP1.2")
    run_checks(platform="my_platform", model="WEU_NEST_EXP1.2")

Returns ``0`` if all checks pass, else ``1``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from message_ix_models import Context
from message_ix_models.model.bmt import workflow_single_region as wf_sr
from message_ix_models.model.bmt.config import apply_bmt_config
from message_ix_models.model.buildings.build import MATERIALS
from message_ix_models.model.buildings.build_single_region import (
    _remap_demand_nodes,
    _target_node,
)
from message_ix_models.util import load_package_data, package_data_path

log = logging.getLogger(__name__)

YEAR_TRANSPORT = 2050
COMM_TRANSPORT = "transport"
RTOL = 1e-5
ATOL = 1e-6


@dataclass(frozen=True)
class CheckConfig:
    """Where to load workflow scenarios from."""

    platform: str = wf_sr.PLATFORM_NAME
    model: str = wf_sr.MODEL_NAME


class CheckError(Exception):
    """One outcome check failed."""


def _scenario_exists(mp, model: str, scenario: str) -> bool:
    listed = mp.scenario_list(default=False)
    if listed is None or listed.empty:
        return False
    hit = listed[(listed["model"] == model) & (listed["scenario"] == scenario)]
    return not hit.empty


def _load(mp, cfg: CheckConfig, scenario: str):
    import message_ix

    if not _scenario_exists(mp, cfg.model, scenario):
        raise FileNotFoundError(f"Missing ixmp://{cfg.platform}/{cfg.model}/{scenario}")
    return message_ix.Scenario(mp, cfg.model, scenario)


def _bmt_context() -> Context:
    """Context with :file:`config_single_region.yaml` (same as the workflow)."""
    ctx = Context()
    apply_bmt_config(ctx, path=package_data_path("bmt", "config_single_region.yaml"))
    ctx.model.regions = "WEU"
    return ctx


def _aligned_csv_paths(context: Context) -> tuple[Path, Path]:
    """Resolve resid/comm ``*_aligned_*.csv`` under message-ix-buildings."""
    from message_ix_models.model.buildings import sturm_single_region

    code_dir, _ = sturm_single_region._ensure_sturm_dirs(context)
    code = context.buildings.code
    linking = code_dir.joinpath("message_linking")
    resid = linking / f"resid_sturm_aligned_{code}.csv"
    comm = linking / f"comm_sturm_aligned_{code}.csv"
    if not resid.exists() or not comm.exists():
        raise FileNotFoundError(
            f"Aligned STURM CSVs not found under {linking} "
            f"(looked for {resid.name!r}, {comm.name!r})"
        )
    return resid, comm


def _message_scenario_for_base_B_sturm(mp, cfg: CheckConfig):
    """MESSAGE scenario paired with the ``B built`` STURM run.

    Prefer the workflow base (``base solved`` parent); fall back to ``base_B``.
    """
    base_name = wf_sr._scenario_name()
    if _scenario_exists(mp, cfg.model, base_name):
        scen = _load(mp, cfg, base_name)
        if scen.has_solution():
            return scen
        log.warning("%s has no solution; falling back to base_B for STURM", scen.url)
    scen = _load(mp, cfg, "base_B")
    if not scen.has_solution():
        raise CheckError(
            f"Need a solved MESSAGE scenario for STURM FE export "
            f"(tried {base_name!r} and base_B)"
        )
    return scen


def _default_sturm_prices(context: Context) -> pd.DataFrame:
    """Load ``sturm/data/input_prices_R12_default.csv``."""
    from message_ix_models.model.buildings import sturm_single_region

    _, input_dir = sturm_single_region._ensure_sturm_dirs(context)
    path = input_dir.joinpath("input_prices_R12_default.csv")
    if not path.exists():
        raise FileNotFoundError(f"Default STURM prices not found: {path}")
    return pd.read_csv(path)


def _run_sturm_for_base_B(mp, cfg: CheckConfig, context: Context) -> None:
    """Re-run WEU STURM for the baseline check.

    Uses ``input_prices_R12_default.csv`` (not on-disk / mitig prices) and RC FE
    from the baseline MESSAGE scenario, then writes aligned resid/comm CSVs.
    """
    from message_ix_models.model.buildings import sturm_single_region

    message = _message_scenario_for_base_B_sturm(mp, cfg)
    prices = _default_sturm_prices(context)
    log.info(
        "Running STURM paired with %s (prices=%s)",
        message.url,
        "input_prices_R12_default.csv",
    )
    sturm_single_region.run(context, prices=prices, scenario=message)


def _expected_buildings_demand(scenario, context: Context) -> pd.DataFrame:
    """Aligned resid+comm demand after the same transforms as the WEU build step."""
    resid_path, comm_path = _aligned_csv_paths(context)
    resid = pd.read_csv(resid_path)
    comm = pd.read_csv(comm_path)

    source_nodes = set(map(str, resid["node"].unique())) | set(
        map(str, comm["node"].unique())
    )
    target = _target_node(scenario, source_nodes)
    if target is None:
        nodes = set(map(str, scenario.set("node").tolist()))
        for cand in ("WEU", "R12_WEU", "R11_WEU"):
            if cand in nodes:
                target = cand
                break
    resid = _remap_demand_nodes(resid, target)
    comm = _remap_demand_nodes(comm, target)

    demand = pd.concat([resid, comm], ignore_index=True)

    if 2110 not in demand["year"].values and 2100 in demand["year"].values:
        extra = demand[demand["year"] == 2100].copy()
        extra["year"] = 2110
        demand = pd.concat([demand, extra], ignore_index=True)

    buildings_commodities = set(load_package_data("buildings", "commodity.yaml"))
    demand = demand[demand["commodity"].isin(buildings_commodities)].copy()
    demand = demand.assign(
        level=demand["commodity"].map(
            lambda c: (
                "demand"
                if ("floor" in str(c) or any(m in str(c).lower() for m in MATERIALS))
                else "useful"
            )
        ),
        year=lambda d: d["year"].astype(int),
        value=lambda d: d["value"].astype(float),
    )
    cols = ["node", "commodity", "year", "level", "time", "value"]
    return demand.loc[:, cols]


def _demand_table(
    scenario, *, commodities: Iterable[str] | None = None
) -> pd.DataFrame:
    """Prefer MACRO-adjusted ``DEMAND`` variable; else the ``demand`` parameter."""
    filters = {"commodity": list(commodities)} if commodities is not None else None
    if scenario.has_solution() and "DEMAND" in scenario.var_list():
        df = scenario.var("DEMAND", filters=filters)
        if not df.empty:
            return df.rename(columns={"lvl": "value"}) if "lvl" in df.columns else df
    return scenario.par("demand", filters=filters)


def _sum_demand(
    scenario,
    *,
    commodities: Iterable[str] | None = None,
    year: int | None = None,
    commodity_prefix: tuple[str, ...] | None = None,
) -> float:
    df = _demand_table(scenario, commodities=commodities)
    if df.empty:
        return 0.0
    out = df.copy()
    out["commodity"] = out["commodity"].astype(str)
    out["year"] = out["year"].astype(int)
    if year is not None:
        out = out[out["year"] == year]
    if commodity_prefix is not None:
        out = out[out["commodity"].str.startswith(commodity_prefix)]
    if commodities is not None:
        wanted = set(commodities)
        out = out[out["commodity"].isin(wanted)]
    if out.empty:
        return 0.0
    return float(out["value"].sum())


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_base_B_demand_matches_aligned_csv(mp, cfg: CheckConfig) -> None:
    """Compare ``base_B`` resid/comm ``demand`` to a fresh baseline STURM run.

    1. Run WEU STURM with ``input_prices_R12_default.csv`` + RC FE from the
       baseline MESSAGE scenario.
    2. Read ``resid_sturm_aligned_*.csv`` / ``comm_sturm_aligned_*.csv``.
    3. Assert those values (after node/level transforms) match ``base_B``.
    """
    context = _bmt_context()
    _run_sturm_for_base_B(mp, cfg, context)

    scenario = _load(mp, cfg, "base_B")
    expected = _expected_buildings_demand(scenario, context)
    if expected.empty:
        raise CheckError("Expected aligned demand is empty after filtering")

    actual = scenario.par(
        "demand",
        filters={"commodity": sorted(expected["commodity"].unique())},
    )
    if actual.empty:
        raise CheckError("base_B has no resid_/comm_ demand rows")

    keys = ["node", "commodity", "year", "level", "time"]
    actual = actual.assign(
        year=lambda d: d["year"].astype(int),
        value=lambda d: d["value"].astype(float),
        node=lambda d: d["node"].astype(str),
        commodity=lambda d: d["commodity"].astype(str),
        level=lambda d: d["level"].astype(str),
        time=lambda d: d["time"].astype(str),
    )
    expected = expected.assign(
        node=lambda d: d["node"].astype(str),
        commodity=lambda d: d["commodity"].astype(str),
        level=lambda d: d["level"].astype(str),
        time=lambda d: d["time"].astype(str),
    )

    merged = expected.merge(
        actual.loc[:, keys + ["value"]],
        on=keys,
        how="left",
        suffixes=("_exp", "_act"),
    )
    missing = merged["value_act"].isna().sum()
    if missing:
        raise CheckError(
            f"{missing} aligned demand rows missing on base_B; "
            f"examples:\n{merged[merged['value_act'].isna()].head(10)}"
        )
    if not np.allclose(
        merged["value_act"].to_numpy(),
        merged["value_exp"].to_numpy(),
        rtol=RTOL,
        atol=ATOL,
    ):
        raise CheckError("base_B demand values differ from aligned STURM CSVs")


def check_mitig_transport_demand_below_baseline(mp, cfg: CheckConfig) -> None:
    """Carbon-tax mitig scenarios have lower demand than paired baselines.

    Uses transport in 2050 as the example commodity/year.
    """
    base_mm = _load(mp, cfg, "base_message_macro")
    mitig = _load(mp, cfg, "mitig")
    base_b_mm = _load(mp, cfg, "base_B_message_macro")
    mitig_b = _load(mp, cfg, "mitig_B")

    for scen in (base_mm, mitig, base_b_mm, mitig_b):
        if not scen.has_solution():
            raise CheckError(f"{scen.url} has no solution; cannot read DEMAND")

    base_t = _sum_demand(base_mm, commodities=[COMM_TRANSPORT], year=YEAR_TRANSPORT)
    mitig_t = _sum_demand(mitig, commodities=[COMM_TRANSPORT], year=YEAR_TRANSPORT)
    base_b_t = _sum_demand(base_b_mm, commodities=[COMM_TRANSPORT], year=YEAR_TRANSPORT)
    mitig_b_t = _sum_demand(mitig_b, commodities=[COMM_TRANSPORT], year=YEAR_TRANSPORT)

    if not (base_t > 0 and base_b_t > 0):
        raise CheckError("Baseline transport demand in 2050 is zero")
    if not mitig_t < base_t:
        raise CheckError(
            f"mitig transport 2050 ({mitig_t}) is not lower than "
            f"base_message_macro ({base_t})"
        )
    if not mitig_b_t < base_b_t:
        raise CheckError(
            f"mitig_B transport 2050 ({mitig_b_t}) is not lower than "
            f"base_B_message_macro ({base_b_t})"
        )


def check_buildings_mitig_delta_differs_from_rc_delta(mp, cfg: CheckConfig) -> None:
    """STURM buildings demand response under mitig differs from exogenous rc_* delta.

    Compares:

    - ``Δ_rc`` = (rc_therm + rc_spec) in ``mitig`` − same in ``base_message_macro``
    - ``Δ_B`` = Σ(comm_* + resid_*) in ``mitig_B_iterated`` − same in
      ``base_B_message_macro``

    ``mitig_B_iterated`` is used (not ``mitig_B``) because resid/comm demand is only
    refreshed from STURM after the price-feedback build step.
    """
    base_mm = _load(mp, cfg, "base_message_macro")
    mitig = _load(mp, cfg, "mitig")
    base_b_mm = _load(mp, cfg, "base_B_message_macro")
    mitig_b_it = _load(mp, cfg, "mitig_B_iterated")

    rc = ("rc_therm", "rc_spec")
    delta_rc = _sum_demand(mitig, commodities=rc) - _sum_demand(base_mm, commodities=rc)
    delta_b = _sum_demand(
        mitig_b_it, commodity_prefix=("comm_", "resid_")
    ) - _sum_demand(base_b_mm, commodity_prefix=("comm_", "resid_"))
    log.info(
        "Δ_rc (mitig − base_message_macro, rc_therm+rc_spec) = %s; "
        "Δ_B (mitig_B_iterated − base_B_message_macro, comm_*+resid_*) = %s",
        delta_rc,
        delta_b,
    )

    if np.isclose(delta_rc, delta_b, rtol=RTOL, atol=ATOL):
        raise CheckError(
            f"Expected different buildings mitigation deltas, got Δ_rc={delta_rc}, "
            f"Δ_B={delta_b}"
        )


CHECKS = (
    (
        "base_B demand matches STURM (re-run vs MESSAGE base)",
        check_base_B_demand_matches_aligned_csv,
    ),
    (
        "mitig DEMAND < baseline",
        check_mitig_transport_demand_below_baseline,
    ),
    (
        "buildings mitig Δ differs from rc_* Δ",
        check_buildings_mitig_delta_differs_from_rc_delta,
    ),
)


def run_checks(
    *,
    platform: str = wf_sr.PLATFORM_NAME,
    model: str = wf_sr.MODEL_NAME,
) -> int:
    """Run all outcome checks. Return 0 if all pass, else 1."""
    import ixmp

    cfg = CheckConfig(platform=platform, model=model)
    log.info("Outcome checks on ixmp://%s/%s/…", cfg.platform, cfg.model)

    mp = ixmp.Platform(cfg.platform)
    failures = 0
    try:
        for name, fn in CHECKS:
            try:
                fn(mp, cfg)
            except Exception as e:
                failures += 1
                log.error("FAIL  %s: %s", name, e)
            else:
                log.info("PASS  %s", name)
    finally:
        mp.close_db()

    if failures:
        log.error("%d / %d checks failed", failures, len(CHECKS))
        return 1
    log.info("All %d checks passed", len(CHECKS))
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(run_checks())
