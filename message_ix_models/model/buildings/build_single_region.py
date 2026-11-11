"""Build MESSAGEix-Buildings on a single-region (WEU / NEST) MESSAGE scenario.

Mirrors the METHOD-B path in :mod:`.buildings.build` / :mod:`.buildings.workflow.ngfs`:

1. Optionally refresh STURM energy prices from the solved scenario
   (:func:`.build.get_prices_B`).
2. Run WEU STURM (:func:`.sturm_single_region.run`).
3. Either update buildings ``demand`` only, or full METHOD-B build
   (:func:`.build.main`) from ``message_linking/{resid,comm}_sturm_*.csv``.
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


#: MESSAGE single-region node → STURM ``input_prices_R12*.csv`` node.
#: The STURM file is named ``*_R12`` but stores R11 labels (``R11_WEU``).
# YJ: this should really be fixed...before 2027!
_WEU_PRICE_NODE = {"WEU": "R11_WEU", "R12_WEU": "R11_WEU", "R11_WEU": "R11_WEU"}
_STURM_WEU_NODE = "R11_WEU"


def _drop_unnamed_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed")]]


def get_prices_for_sturm(
    context: Context, scenario: message_ix.Scenario
) -> pd.DataFrame:
    """Return full STURM ``input_prices_R12`` table with only WEU rows updated.

    Unlike :func:`.build.get_prices_B` (R12 → R11 adapt of all regions), this:

    1. Keeps the existing multi-region price CSV structure intact.
    2. Takes ``PRICE_COMMODITY`` from the single-region scenario (node ``WEU``).
    3. Maps ``WEU`` → ``R11_WEU`` (label used in the STURM price files).
    4. Applies the same electr / floor rules as :func:`.build.get_prices_B`, but
       only for ``R11_WEU`` rows; all other nodes are left unchanged.
    """
    import numpy as np

    from message_ix_models.model.buildings import sturm_single_region
    from message_ix_models.model.buildings.build import get_prices
    from message_ix_models.model.buildings.config import DEFAULT_DATA_PATHS

    _, input_dir = sturm_single_region._ensure_sturm_dirs(context)
    config: "Config" = context.buildings
    price_name = Path(
        config.data_paths.get("prices", DEFAULT_DATA_PATHS["prices"])
    ).name
    price_path = input_dir.joinpath(price_name)
    default_path = input_dir.joinpath("input_prices_R12_default.csv")

    if not price_path.exists():
        if not default_path.exists():
            raise FileNotFoundError(
                f"STURM price file not found: {price_path} (nor {default_path})"
            )
        price_path = default_path

    df_full = _drop_unnamed_cols(pd.read_csv(price_path))
    df_floor = _drop_unnamed_cols(
        pd.read_csv(default_path) if default_path.exists() else df_full.copy()
    )

    df_scen = get_prices(scenario).copy()
    if df_scen.empty:
        raise ValueError(
            f"No PRICE_COMMODITY (final fuels) on {scenario.url}; "
            "cannot update STURM WEU prices"
        )

    df_scen["node"] = df_scen["node"].astype(str).replace(_WEU_PRICE_NODE)
    unknown = sorted(set(df_scen["node"]) - {_STURM_WEU_NODE})
    if unknown:
        log.warning(
            "Dropping non-WEU price nodes from single-region update: %s", unknown
        )
        df_scen = df_scen[df_scen["node"] == _STURM_WEU_NODE]
    if df_scen.empty:
        raise ValueError(
            f"No WEU prices after node remap on {scenario.url} "
            f"(looked for nodes in {_WEU_PRICE_NODE})"
        )

    cols = [
        c
        for c in ("node", "commodity", "level", "year", "time")
        if c in df_scen.columns and c in df_floor.columns
    ]
    weu_floor = df_floor.loc[df_floor["node"].astype(str) == _STURM_WEU_NODE].copy()
    merged = pd.merge(
        weu_floor,
        df_scen[cols + ["lvl"]],
        on=cols,
        how="left",
        suffixes=("", "_new"),
    )

    lvl_original = merged["lvl"].copy()
    lvl_scenario = merged["lvl_new"].fillna(merged["lvl"])
    factor = np.where(lvl_original != 0, lvl_scenario / lvl_original, np.nan)
    has_scenario = merged["lvl_new"].notna()
    below_reference = (factor < 1) & has_scenario
    use_reference_floor = below_reference & (merged["commodity"] != "electr")
    merged["lvl"] = np.where(use_reference_floor, lvl_original, lvl_scenario)
    merged = merged.drop(columns=["lvl_new"])

    # Full table: non-WEU unchanged; R11_WEU replaced with merged values.
    out = df_full.copy()
    weu_mask = out["node"].astype(str) == _STURM_WEU_NODE
    updated = merged.copy()
    for c in out.columns:
        if c not in updated.columns:
            # e.g. keep original ``mrg`` if present on the WEU block
            updated[c] = out.loc[weu_mask, c].to_numpy() if weu_mask.any() else np.nan
    updated = updated.loc[:, out.columns]

    out = pd.concat([out.loc[~weu_mask], updated], ignore_index=True)
    node_cats = list(dict.fromkeys(df_full["node"].astype(str)))
    out = (
        out.assign(
            node=pd.Categorical(
                out["node"].astype(str), categories=node_cats, ordered=True
            )
        )
        .sort_values(["node", "commodity", "level", "year", "time"], kind="mergesort")
        .assign(node=lambda d: d["node"].astype(str))
        .reset_index(drop=True)
    )

    log.info(
        "Updated STURM WEU prices: %d R11_WEU rows (%d matched scenario lvl)",
        len(updated),
        int(has_scenario.sum()),
    )
    return out


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


def _data_path_name(context: Context, key: str) -> str:
    """Basename from :attr:`.Config.data_paths`, with ``{code}`` filled in."""
    raw = str(context.buildings.data_paths[key])
    return Path(raw.format(code=context.buildings.code)).name


def _resolve_input_csv(
    context: Context, key: str, *, search: list[Path]
) -> Path | None:
    """Resolve a :attr:`.Config.data_paths` entry under STURM dirs."""
    name = _data_path_name(context, key)
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
            f"{_data_path_name(context, 'sturm_r')!r} / "
            f"{_data_path_name(context, 'sturm_c')!r}). "
            "Run STURM first or set data_paths in config_single_region.yaml."
        )
    if prices_src is None:
        raise FileNotFoundError(
            f"STURM prices CSV not found under {input_dir} "
            f"({_data_path_name(context, 'prices')!r})."
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
        dest = work.joinpath(_data_path_name(context, key))
        df.to_csv(dest, index=False)
        paths[key] = dest
        log.info("METHOD B input %s: %s -> %s (%d rows)", key, src, dest, len(df))

    return paths


def _load_demand_csv(path: Path) -> pd.DataFrame:
    """Load a METHOD-B demand CSV, keeping :data:`_DEMAND_COLUMNS` only."""
    df = pd.read_csv(path)
    missing = [c for c in _DEMAND_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing demand columns {missing}")
    return df.loc[:, _DEMAND_COLUMNS]


def _update_buildings_demand_only(
    context: Context, scenario: message_ix.Scenario, paths: dict[str, Path]
) -> None:
    """Replace buildings ``demand`` from STURM CSVs; do not touch structure/techs."""
    from message_ix_models.model.buildings.build import MATERIALS
    from message_ix_models.util import load_package_data

    sturm_r = _load_demand_csv(paths["sturm_r"])
    sturm_c = _load_demand_csv(paths["sturm_c"])
    demand_static = _load_demand_csv(paths["demand_static"])

    # Same 2110 fill as :func:`.build.prepare_data_B`.
    parts = []
    for df in (sturm_r, sturm_c, demand_static):
        if df.empty:
            continue
        if 2110 not in df["year"].values and 2100 in df["year"].values:
            df_2110 = df[df["year"] == 2100].copy()
            df_2110["year"] = 2110
            df = pd.concat([df, df_2110], ignore_index=True)
        parts.append(df)

    if not parts:
        raise ValueError("No STURM / static demand rows to update")

    commodity_info = load_package_data("buildings", "commodity.yaml")
    buildings_commodities = set(commodity_info.keys())
    demand = pd.concat(parts, ignore_index=True)
    demand = demand[demand["commodity"].isin(buildings_commodities)].copy()
    # Same as prepare_data_B: materials/floor → demand, energy → useful
    demand = demand.assign(
        level=demand["commodity"].map(
            lambda c: (
                "demand"
                if ("floor" in str(c) or any(m in str(c).lower() for m in MATERIALS))
                else "useful"
            )
        )
    )

    if scenario.has_solution():
        log.info("Removing solution before demand-only update")
        scenario.remove_solution()

    existing = scenario.par("demand")
    to_remove = existing[existing["commodity"].isin(buildings_commodities)]
    log.info(
        "Updating buildings demand only: remove %d / add %d rows (%s)",
        len(to_remove),
        len(demand),
        scenario.url,
    )
    with scenario.transact("Update WEU buildings demand from STURM (demand-only)"):
        if not to_remove.empty:
            scenario.remove_par("demand", to_remove)
        scenario.add_par("demand", demand)

    scenario.set_as_default()
    log.info(
        "Demand-only update done; has_solution=%s; set as default: %s",
        scenario.has_solution(),
        scenario.url,
    )


def main(
    context: Context,
    scenario: message_ix.Scenario,
    *,
    update_prices: bool = False,
    update_demand_only: bool = False,
) -> message_ix.Scenario:
    """WEU buildings build step.

    1. Optionally refresh STURM energy prices from the solved scenario
    (:func:`.build.get_prices_B`).
    2. Run WEU STURM (:func:`.sturm_single_region.run`).
    3. Either update buildings ``demand`` only (`update_demand_only`), or full
    METHOD-B build via :func:`.build.main` (technologies / sets / demand).

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
    update_demand_only :
        If :obj:`True`, still run steps 1–2, then only replace buildings
        ``demand`` from the STURM linking CSVs (no tech/set rebuild). Default
        :obj:`False` runs the full :func:`.build.main` path.
    """
    from message_ix_models.model.buildings import build, sturm_single_region
    from message_ix_models.model.buildings.config import METHOD

    config: "Config" = context.buildings
    # build.get_spec requires nodes from context.regions (default R14). NEST has WEU.
    if getattr(context.model, "regions", None) != "WEU":
        log.info("Setting context.model.regions=WEU for single-region build")
        context.model.regions = "WEU"
    log.info(
        "WEU buildings build (update_prices=%s, update_demand_only=%s, "
        "code=%r, regions=%r)",
        update_prices,
        update_demand_only,
        config.code,
        context.model.regions,
    )

    prices = get_prices_for_sturm(context, scenario) if update_prices else None
    sturm_single_region.run(context, prices=prices, scenario=scenario)

    paths = _prepare_method_b_inputs(context, scenario)  # CSV IO only; no solution
    config.data_paths = {
        "prices": paths["prices"],
        "sturm_r": paths["sturm_r"],
        "sturm_c": paths["sturm_c"],
        "demand_static": paths["demand_static"],
    }

    if update_demand_only:
        _update_buildings_demand_only(context, scenario, paths)
        return scenario

    method = config.method
    config.method = method if isinstance(method, METHOD) else METHOD[str(method)]

    if scenario.has_solution():
        log.info("Removing solution before build.main edits")
        scenario.remove_solution()

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
