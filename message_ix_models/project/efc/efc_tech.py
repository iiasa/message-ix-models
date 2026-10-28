"""EFC additional technologies.

- :file:`data/efc/set.yaml` — require / add / remove set elements; optional
  ``map_tec_addon`` / ``cat_addon`` pairs and ``parameter_copy`` rules
- :file:`data/efc/technology.yaml` — codes for technologies to **add**
- :file:`data/efc/efc_tech_parameter.csv` — parameter values (CSV → ``add_par``)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.build import apply_spec
from message_ix_models.model.structure import generate_set_elements
from message_ix_models.util import (
    add_par_data,
    broadcast,
    load_package_data,
    nodes_ex_world,
    package_data_path,
    same_node,
)
from message_ix_models.util.scenarioinfo import Spec

if TYPE_CHECKING:
    import message_ix

    from message_ix_models import Context
    from message_ix_models.types import MutableParameterData

log = logging.getLogger(__name__)

_PARAMETER_CSV = package_data_path("efc", "efc_tech_parameter.csv")

# Optional CSV columns → MESSAGE dims (when non-empty on the row).
_OPTIONAL_DIMS = (
    "commodity",
    "level",
    "emission",
    "relation",
    "mode",
    "shares",
    "type_addon",
)

# Top-level set.yaml keys that are not MESSAGE 1-D sets for :func:`make_spec`.
_META_KEYS = frozenset({"parameter_copy", "map_tec_addon", "cat_addon"})


def _load_set_yaml() -> dict[str, Any]:
    """Load :file:`data/efc/set.yaml`."""
    path = package_data_path("efc", "set.yaml")
    return dict(load_package_data(*path.parts))


def make_spec() -> Spec:
    """Return the structural :class:`Spec` for the efc_tech module.

    Configuration is loaded from :file:`data/efc/set.yaml` and
    :file:`data/efc/technology.yaml`. Indexed addon maps and
    ``parameter_copy`` are applied later in :func:`add_data`.
    """
    tech_path = package_data_path("efc", "technology.yaml")
    technology = load_package_data(*tech_path.parts)

    sets: dict[str, Any] = dict(_load_set_yaml())
    sets.setdefault("technology", {})
    sets["technology"]["add"] = technology

    s = Spec()

    for set_name in list(sets):
        if set_name in _META_KEYS or not isinstance(sets[set_name], dict):
            continue
        generate_set_elements(sets, set_name)

        for action in {"add", "remove", "require"}:
            s[action].set[set_name].extend(sets[set_name].get(action, []))
        try:
            s.add.set[f"{set_name} indexers"] = sets[set_name]["indexers"]
        except KeyError:
            pass

    return s


def _is_all(value: Any) -> bool:
    return str(value).strip().lower() == "all"


def _cell(row: pd.Series, name: str) -> Any:
    """Return a non-empty CSV cell, or ``None``."""
    if name not in row.index or pd.isna(row[name]):
        return None
    text = str(row[name]).strip()
    return text if text else None


def _unit_from_row(row: pd.Series) -> str:
    """Return the CSV ``unit`` cell; raise if missing."""
    unit = _cell(row, "unit")
    if unit is None:
        raise ValueError(
            f"efc_tech_parameter.csv row for {row.get('parameter')!r}/"
            f"{row.get('technology')!r} is missing unit"
        )
    return unit


def _par_columns(par_name: str) -> set[str]:
    """Return MESSAGE index/value columns for *par_name*."""
    return set(make_df(par_name).columns)


def _row_kwargs(row: pd.Series, par_name: str) -> dict[str, Any]:
    """Build ``make_df`` kwargs from one CSV row.

    Only dimensions that exist on *par_name* are included. Unit comes from the
    CSV ``unit`` column.
    """
    cols = _par_columns(par_name)
    kwargs: dict[str, Any] = {
        "value": float(row["value"]),
        "unit": _unit_from_row(row),
    }
    tech = _cell(row, "technology")
    if tech is not None and "technology" in cols:
        kwargs["technology"] = tech
    if "time" in cols:
        kwargs["time"] = "year"
    for dim in _OPTIONAL_DIMS:
        val = _cell(row, dim)
        if dim in cols and val is not None:
            kwargs[dim] = val

    if par_name == "input" and "time_origin" in cols:
        kwargs["time_origin"] = "year"
    elif par_name == "output" and "time_dest" in cols:
        kwargs["time_dest"] = "year"

    return kwargs


def _set_node_year_kwargs(
    kwargs: dict[str, Any],
    row: pd.Series,
    cols: set[str],
) -> None:
    """Fill ``node_*`` / ``year_*`` on *kwargs* from one CSV row."""
    node_val: Any = None if _is_all(row["node"]) else str(row["node"])
    if "node_share" in cols:
        kwargs["node_share"] = node_val
    elif "node_loc" in cols:
        kwargs["node_loc"] = node_val
    elif "node" in cols:
        kwargs["node"] = node_val
    elif "node_rel" in cols:
        kwargs["node_rel"] = node_val

    if _is_all(row["year"]):
        for ydim in ("year_act", "year_vtg", "year_rel"):
            if ydim in cols:
                kwargs[ydim] = None
        return

    y = int(row["year"])
    for ydim in ("year_act", "year_vtg", "year_rel"):
        if ydim in cols:
            kwargs[ydim] = y


def _broadcast_nodes(
    df: pd.DataFrame, cols: set[str], nodes: list[Any]
) -> pd.DataFrame:
    """Broadcast over the node dimension present on *cols*."""
    if "node_share" in cols:
        return df.pipe(broadcast, node_share=nodes)
    if "node_loc" in cols:
        return df.pipe(broadcast, node_loc=nodes)
    if "node" in cols:
        return df.pipe(broadcast, node=nodes)
    if "node_rel" in cols:
        return df.pipe(broadcast, node_rel=nodes)
    return df


def _broadcast_years(
    df: pd.DataFrame,
    cols: set[str],
    years: list[Any],
    yv_ya: pd.DataFrame,
) -> pd.DataFrame:
    """Broadcast ``year=all`` over the year dimensions present on *cols*."""
    if "year_act" in cols and "year_vtg" in cols:
        # Full vintage–activity lifetime (materials-style), not diagonal.
        return df.pipe(broadcast, labels=yv_ya)
    if "year_act" in cols:
        df = df.pipe(broadcast, year_act=years)
        if "year_rel" in cols:
            return df.assign(year_rel=df["year_act"])
        return df
    if "year_rel" in cols:
        return df.pipe(broadcast, year_rel=years)
    if "year_vtg" in cols:
        return df.pipe(broadcast, year_vtg=years)
    return df


def _broadcast_row(
    df: pd.DataFrame,
    row: pd.Series,
    cols: set[str],
    nodes: list[Any],
    years: list[Any],
    yv_ya: pd.DataFrame,
) -> pd.DataFrame:
    """Broadcast ``all`` node/year placeholders on *df*."""
    if _is_all(row["node"]):
        df = _broadcast_nodes(df, cols, nodes)
    if _is_all(row["year"]):
        df = _broadcast_years(df, cols, years, yv_ya)
    return df


def _frame_from_row(
    row: pd.Series,
    nodes: list[Any],
    years: list[Any],
    yv_ya: pd.DataFrame,
) -> tuple[str, pd.DataFrame]:
    """Build one MESSAGE parameter frame from a CSV row."""
    par_name = str(row["parameter"]).strip()
    cols = _par_columns(par_name)
    kwargs = _row_kwargs(row, par_name)
    _set_node_year_kwargs(kwargs, row, cols)

    df = make_df(par_name, **kwargs)
    df = _broadcast_row(df, row, cols, nodes, years, yv_ya)
    if par_name in {"input", "output", "relation_activity"}:
        df = df.pipe(same_node)
    return par_name, df


def gen_data_from_csv(
    scenario: "message_ix.Scenario",
    info: ScenarioInfo,
) -> "MutableParameterData":
    """Load :file:`data/efc/efc_tech_parameter.csv` into MESSAGE parameter frames.

    - ``node`` = ``all`` → broadcast to all non-World nodes
    - ``year`` = ``all`` → for params with both ``year_vtg`` and ``year_act``
      (e.g. ``input`` / ``output``), broadcast over :attr:`.ScenarioInfo.yv_ya`
      (full vintage–activity pairs); vintage-only params (``inv_cost``,
      ``technical_lifetime``) get ``year_vtg`` over model years;
      ``relation_activity`` gets matching ``year_act`` / ``year_rel``
    - ``unit`` is taken from the CSV column
    - otherwise use the given node / year as-is
    """
    del scenario  # units come from the CSV; scenario kept for call-site parity
    if not _PARAMETER_CSV.exists():
        log.warning("Missing parameter CSV: %s", _PARAMETER_CSV)
        return {}

    raw = pd.read_csv(_PARAMETER_CSV)
    if raw.empty:
        return {}

    nodes = list(nodes_ex_world(info.N))
    years = list(info.Y)
    yv_ya = info.yv_ya
    buckets: dict[str, list[pd.DataFrame]] = defaultdict(list)

    for _, row in raw.iterrows():
        par_name, df = _frame_from_row(row, nodes, years, yv_ya)
        buckets[par_name].append(df)

    data: MutableParameterData = {
        name: pd.concat(parts, ignore_index=True) for name, parts in buckets.items()
    }
    log.info(
        "Loaded %s: %s",
        _PARAMETER_CSV.name,
        {k: len(v) for k, v in data.items()},
    )
    return data


def _register_indexed_sets(
    scenario: "message_ix.Scenario",
    sets_cfg: dict[str, Any],
) -> None:
    """Register multi-index addon maps from ``set.yaml`` (must be checked out)."""
    for pair in sets_cfg.get("map_tec_addon", {}).get("add", []) or []:
        technology, type_addon = pair
        existing = scenario.set("map_tec_addon")
        already = False
        if isinstance(existing, pd.DataFrame) and not existing.empty:
            already = (
                (existing["technology"] == technology)
                & (existing["type_addon"] == type_addon)
            ).any()
        if already:
            continue
        scenario.add_set(
            "map_tec_addon",
            pd.DataFrame([{"technology": technology, "type_addon": type_addon}]),
        )
        log.info("map_tec_addon: %s → %s", technology, type_addon)

    for pair in sets_cfg.get("cat_addon", {}).get("add", []) or []:
        type_addon, technology = pair
        scenario.add_cat("addon", type_addon, technology)
        log.info("cat_addon: %s → %s", type_addon, technology)


def gen_parameter_copies(
    scenario: "message_ix.Scenario",
    rules: list[dict[str, Any]],
) -> "MutableParameterData":
    """Copy parameter rows between technologies (generic ``parameter_copy`` rules)."""
    buckets: dict[str, list[pd.DataFrame]] = defaultdict(list)
    for rule in rules:
        node = rule["node"]
        src = rule["from_technology"]
        dst = rule["to_technology"]
        for par_name in rule["parameters"]:
            parent = scenario.par(
                par_name,
                filters={"technology": src, "node_loc": node},
            )
            if parent.empty:
                log.warning(
                    "parameter_copy: no %s for %s in %s; skip → %s",
                    par_name,
                    src,
                    node,
                    dst,
                )
                continue
            copied = parent.copy()
            copied["technology"] = dst
            buckets[par_name].append(copied)
            log.info(
                "parameter_copy: %s %s → %s in %s (%d rows)",
                par_name,
                src,
                dst,
                node,
                len(copied),
            )
    return {
        name: pd.concat(parts, ignore_index=True) for name, parts in buckets.items()
    }


def _merge_par_data(
    *parts: "MutableParameterData",
) -> "MutableParameterData":
    """Concatenate parameter frames from multiple sources."""
    buckets: dict[str, list[pd.DataFrame]] = defaultdict(list)
    for part in parts:
        for name, df in part.items():
            if df is not None and not df.empty:
                buckets[name].append(df)
    return {
        name: pd.concat(frames, ignore_index=True) for name, frames in buckets.items()
    }


def add_data(
    scenario: "message_ix.Scenario",
    dry_run: bool = False,
) -> None:
    """Add parameter rows from the CSV (after Spec set add/remove).

    Also registers indexed addon maps and applies ``parameter_copy`` rules from
    :file:`set.yaml`. Called from :func:`apply_spec` while checked out.
    """
    if dry_run:
        log.info("dry_run: skipping efc_tech add_data")
        return

    sets_cfg = _load_set_yaml()
    _register_indexed_sets(scenario, sets_cfg)

    info = ScenarioInfo(scenario)
    data = _merge_par_data(
        gen_data_from_csv(scenario, info),
        gen_parameter_copies(scenario, sets_cfg.get("parameter_copy") or []),
    )
    if data:
        add_par_data(scenario, data, dry_run=dry_run)


def build(
    context: "Context",
    scenario: "message_ix.Scenario",
) -> "message_ix.Scenario":
    """Apply efc_tech Spec and CSV parameters to *scenario*."""
    del context  # signature parity with other EFC build steps
    apply_spec(
        scenario,
        make_spec(),
        add_data,
        fast=False,
        message="EFC efc_tech: Spec + efc_tech_parameter.csv",
    )
    return scenario
