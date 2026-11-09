"""Build MESSAGEix-Buildings on a single-region (WEU / NEST) MESSAGE scenario.

Mirrors the METHOD-B path in :mod:`.buildings.build` / :mod:`.buildings.workflow.ngfs`:

1. Optionally refresh STURM energy prices from the solved scenario
   (:func:`.build.get_prices_B`).
2. Run WEU STURM (:func:`.sturm_single_region.run`).
3. Build demand / technologies into MESSAGE: load
   ``message_linking/{resid,comm}_sturm_*.csv`` (METHOD-B schema), remap nodes
   if needed, keep ``demand_static`` empty for now, then call
   :func:`.build.main` with :attr:`~.buildings.config.METHOD.B`.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import message_ix
import pandas as pd

from message_ix_models import Context

if TYPE_CHECKING:
    from message_ix_models.model.buildings.config import Config

log = logging.getLogger(__name__)

#: Columns expected by :func:`.build.prepare_data_B` for STURM / static demand.
_DEMAND_COLUMNS = ["node", "commodity", "year", "level", "time", "unit", "value"]

#: STURM MESSAGE report node → possible NEST single-region node names.
# YJ: should be something to be fixed at STURM side...
_NODE_ALIASES = {
    "R12_WEU": ("WEU", "R12_WEU", "R11_WEU"),
    "R11_WEU": ("WEU", "R12_WEU", "R11_WEU"),
}


def get_prices_for_sturm(
    context: Context, scenario: message_ix.Scenario
) -> pd.DataFrame:
    """Return STURM price table via :func:`.build.get_prices_B`."""
    from message_ix_models.model.buildings import sturm_single_region
    from message_ix_models.model.buildings.build import get_prices_B

    _, input_dir = sturm_single_region._ensure_sturm_dirs(context)
    return get_prices_B(scenario, input_dir)


def _target_node(scenario: message_ix.Scenario, source_nodes: set[str]) -> str | None:
    """Pick the scenario node that should receive WEU STURM demand."""
    nodes = set(map(str, scenario.set("node").tolist()))
    # Prefer explicit aliases for WEU report nodes
    for src in source_nodes:
        for cand in _NODE_ALIASES.get(src, ()):
            if cand in nodes:
                return cand
    # Single regional node (exclude World / global aggregates)
    regional = [
        n
        for n in nodes
        if n not in {"World", "R12_GLB", "R11_GLB"} and not n.endswith("_GLB")
    ]
    if len(regional) == 1:
        return regional[0]
    return None


def _remap_demand_nodes(df: pd.DataFrame, target: str | None) -> pd.DataFrame:
    """Replace STURM report nodes with the MESSAGE scenario node when needed."""
    if target is None or "node" not in df.columns or df.empty:
        return df
    out = df.copy()
    mapping = {
        src: target
        for src in out["node"].unique()
        if src != target and src in _NODE_ALIASES
    }
    if mapping:
        log.info("Remapping demand nodes %s -> %s", mapping, target)
        out["node"] = out["node"].replace(mapping)
    return out


def _resolve_input_csv(
    context: Context, key: str, *, search: list[Path]
) -> Path | None:
    """Resolve a :attr:`.Config.data_paths` entry under STURM dirs."""
    name = Path(context.buildings.data_paths[key]).name
    for base in search:
        path = base.joinpath(name)
        if path.exists():
            return path
    return None


def _prepare_method_b_inputs(
    context: Context, scenario: message_ix.Scenario
) -> dict[str, Path]:
    """Load ``message_linking`` STURM CSVs, remap nodes, write METHOD-B inputs.

    TODO: not efficient but a little too path-dependent to fully refactor.
    """
    from message_ix_models.model.buildings import sturm_single_region

    code_dir, input_dir = sturm_single_region._ensure_sturm_dirs(context)
    linking_dir = code_dir.joinpath("message_linking")

    work = context.get_local_path("buildings", "weu_build")
    work.mkdir(parents=True, exist_ok=True)

    # Resid/comm come from message_linking only (written by WEU R runners),
    # not from sturm/output/report_MESSAGE_*.
    sturm_r_src = _resolve_input_csv(context, "sturm_r", search=[linking_dir])
    sturm_c_src = _resolve_input_csv(context, "sturm_c", search=[linking_dir])
    prices_src = _resolve_input_csv(context, "prices", search=[input_dir])

    if sturm_r_src is None or sturm_c_src is None:
        raise FileNotFoundError(
            "WEU STURM linking CSVs not found under "
            f"{linking_dir} (looked for "
            f"{context.buildings.data_paths['sturm_r']!r} / "
            f"{context.buildings.data_paths['sturm_c']!r}). "
            "Run STURM first or set data_paths in config_single_region.yaml."
        )
    if prices_src is None:
        raise FileNotFoundError(
            f"STURM prices CSV not found under {input_dir} "
            f"({context.buildings.data_paths['prices']!r})."
        )

    sturm_r = pd.read_csv(sturm_r_src)
    sturm_c = pd.read_csv(sturm_c_src)
    source_nodes = set(sturm_r["node"].unique()) | set(sturm_c["node"].unique())
    target = _target_node(scenario, source_nodes)
    sturm_r = _remap_demand_nodes(sturm_r, target)
    sturm_c = _remap_demand_nodes(sturm_c, target)

    # demand_static: placeholder empty table until GLANCE / apps demand is wired.
    log.info("demand_static placeholder: empty table (METHOD B still runs)")
    demand_static = pd.DataFrame(columns=_DEMAND_COLUMNS)

    paths: dict[str, Path] = {}
    for key, df, src in (
        ("sturm_r", sturm_r, sturm_r_src),
        ("sturm_c", sturm_c, sturm_c_src),
        ("demand_static", demand_static, None),
        ("prices", pd.read_csv(prices_src), prices_src),
    ):
        dest = work.joinpath(Path(context.buildings.data_paths[key]).name)
        df.to_csv(dest, index=False)
        paths[key] = dest
        log.info("METHOD B input %s: %s -> %s (%d rows)", key, src, dest, len(df))

    return paths


def main(
    context: Context,
    scenario: message_ix.Scenario,
    *,
    update_prices: bool = False,
) -> message_ix.Scenario:
    """WEU buildings build step.

    1. Optionally refresh STURM energy prices from the solved scenario
    (:func:`.build.get_prices_B`).
    2. Run WEU STURM (:func:`.sturm_single_region.run`).
    3. Build demand / technologies into MESSAGE: load
    ``message_linking/{resid,comm}_sturm_*.csv`` (METHOD-B schema), remap nodes
    if needed, keep ``demand_static`` empty for now, then call
    :func:`.build.main` with :attr:`~.buildings.config.METHOD.B`.

    Parameters
    ----------
    context :
        :attr:`context.buildings` from e.g. :file:`config_single_region.yaml`.
    scenario :
        Solved MESSAGE scenario.
    update_prices :
        If :obj:`True` (mitigation / carbon-budget runs), refresh STURM prices
        from `scenario` before running R. If :obj:`False` (baseline), keep the
        existing STURM price file.
    """
    from message_ix_models.model.buildings import build, sturm_single_region
    from message_ix_models.model.buildings.config import METHOD

    config: "Config" = context.buildings
    # build.get_spec requires nodes from context.regions (default R14). NEST has WEU.
    if getattr(context.model, "regions", None) != "WEU":
        log.info("Setting context.model.regions=WEU for single-region build")
        context.model.regions = "WEU"
    log.info(
        "WEU buildings build (update_prices=%s, code=%r, regions=%r)",
        update_prices,
        config.code,
        context.model.regions,
    )

    prices = get_prices_for_sturm(context, scenario) if update_prices else None
    sturm_single_region.run(context, prices=prices)

    paths = _prepare_method_b_inputs(context, scenario)
    # Absolute paths so build.main does not use private_data_path.
    config.data_paths = {
        "prices": paths["prices"],
        "sturm_r": paths["sturm_r"],
        "sturm_c": paths["sturm_c"],
        "demand_static": paths["demand_static"],
    }
    # Honour buildings.method from config_single_region.yaml (YAML yields str "B").
    method = config.method
    config.method = method if isinstance(method, METHOD) else METHOD[str(method)]

    log.info(
        "Calling build.main (%s) for %s/%s with_materials=%s",
        config.method,
        scenario.model,
        scenario.scenario,
        config.with_materials,
    )

    _orig = build._replace_ue_rt_share_with_share_mode
    build._replace_ue_rt_share_with_share_mode = _replace_ue_rt_share_with_share_mode
    try:
        build.main(context, scenario)
    finally:
        build._replace_ue_rt_share_with_share_mode = _orig
    return scenario


#: Same techs as :func:`.build._replace_ue_rt_share_with_share_mode`.
#: YJ: suggested by SJ as techs that do not need good storage representation
#: YJ: maybe function in init and tech in yaml later
_SHARE_MODE_TECHS = [
    "electr_comm_cool",
    "electr_resid_cool",
    # "electr_resid_apps",
    "electr_resid_other_uses",
    "electr_comm_other_uses",
    # "electr_resid_cook",
    # "electr_resid_heat",
    # "electr_comm_heat",
    # "electr_resid_hotwater",
    # "electr_comm_hotwater",
]


def _replace_ue_rt_share_with_share_mode(scenario: message_ix.Scenario) -> None:
    """Single-region variant of :func:`.build._replace_ue_rt_share_with_share_mode`.

    Uses regional nodes present on `scenario` (e.g. ``WEU``), not ``R12_*``. With one
    region, ``node_share`` is set directly (no node broadcast).
    """
    from message_ix import make_df

    from message_ix_models.util import broadcast, nodes_ex_world

    with scenario.transact("Remove old UE_RT_elec_share_RC_max constraint"):
        ue_rt_rows = scenario.par(
            "share_commodity_up", filters={"shares": "UE_RT_elec_share_RC_max"}
        )
        if not ue_rt_rows.empty:
            scenario.remove_par("share_commodity_up", ue_rt_rows)
            log.info(
                "Removed %d rows from share_commodity_up for UE_RT_elec_share_RC_max",
                len(ue_rt_rows),
            )
        if "UE_RT_elec_share_RC_max" in scenario.set("shares").tolist():
            scenario.remove_set("shares", "UE_RT_elec_share_RC_max")
            log.info("Removed UE_RT_elec_share_RC_max from shares set")

    regions = [str(n) for n in nodes_ex_world(list(map(str, scenario.set("node"))))]
    if not regions:
        log.warning(
            "No regional nodes for share_mode_up; skipping RT_elec_share_RC_max"
        )
        return

    all_years = sorted(int(y) for y in scenario.set("year"))
    base = make_df(
        "share_mode_up",
        shares="RT_elec_share_RC_max",
        mode="M2",
        time="year",
        value=0.35,
        unit="-",
    ).assign(node_share=None, technology=None, year_act=None)

    if len(regions) == 1:
        df_share_mode_up = base.assign(node_share=regions[0]).pipe(
            broadcast,
            technology=_SHARE_MODE_TECHS,
            year_act=all_years,
        )
        log.info("share_mode_up for single node_share=%s", regions[0])
    else:
        df_share_mode_up = base.pipe(
            broadcast,
            node_share=regions,
            technology=_SHARE_MODE_TECHS,
            year_act=all_years,
        )
        log.info("share_mode_up for %d regional nodes", len(regions))

    with scenario.transact("add the share mode constraint"):
        scenario.add_set("shares", "RT_elec_share_RC_max")
        scenario.add_par("share_mode_up", df_share_mode_up)
        log.info("Added share mode constraint RT_elec_share_RC_max")
