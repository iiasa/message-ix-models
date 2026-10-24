"""EFC additional technologies.

- :file:`data/efc/set.yaml` — require / add / remove set elements
- :file:`data/efc/technology.yaml` — codes for technologies to **add**
- :file:`data/efc/efc_tech_parameter.csv` — parameter values
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

# Optional columns in the CSV → MESSAGE parameter dimensions (when non-empty).
_OPTIONAL_DIMS = ("commodity", "level", "emission", "relation", "mode")


def _load_set_yaml() -> dict[str, Any]:
    """Load :file:`data/efc/set.yaml`."""
    path = package_data_path("efc", "set.yaml")
    return dict(load_package_data(*path.parts))


def make_spec() -> Spec:
    """Return the structural :class:`Spec` for the efc_tech module.

    Configuration is loaded from :file:`data/efc/set.yaml` and
    :file:`data/efc/technology.yaml`.
    """
    tech_path = package_data_path("efc", "technology.yaml")
    technology = load_package_data(*tech_path.parts)

    sets: dict[str, Any] = dict(_load_set_yaml())
    sets.setdefault("technology", {})
    sets["technology"]["add"] = technology

    s = Spec()

    for set_name in list(sets):
        if not isinstance(sets[set_name], dict):
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


def _unit_from_row(row: pd.Series) -> str:
    """Return the CSV ``unit`` cell; raise if missing."""
    if (
        "unit" not in row.index
        or pd.isna(row["unit"])
        or str(row["unit"]).strip() == ""
    ):
        raise ValueError(
            f"efc_tech_parameter.csv row for {row.get('parameter')!r}/"
            f"{row.get('technology')!r} is missing unit"
        )
    return str(row["unit"]).strip()


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
        "technology": row["technology"],
        "value": float(row["value"]),
        "unit": _unit_from_row(row),
    }
    if "time" in cols:
        kwargs["time"] = "year"
    for dim in _OPTIONAL_DIMS:
        if (
            dim in cols
            and dim in row.index
            and pd.notna(row[dim])
            and str(row[dim]).strip() != ""
        ):
            kwargs[dim] = row[dim]

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
    """Fill ``node_loc`` / ``year_*`` on *kwargs* from one CSV row."""
    kwargs["node_loc"] = None if _is_all(row["node"]) else str(row["node"])
    if _is_all(row["year"]):
        if "year_act" in cols:
            kwargs["year_act"] = None
        if "year_vtg" in cols:
            kwargs["year_vtg"] = None
        return

    y = int(row["year"])
    if "year_act" in cols:
        kwargs["year_act"] = y
    if "year_vtg" in cols:
        kwargs["year_vtg"] = y


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
        df = df.pipe(broadcast, node_loc=nodes)
    if not _is_all(row["year"]):
        return df
    if "year_act" in cols and "year_vtg" in cols:
        # Full vintage–activity lifetime (materials-style), not diagonal.
        return df.pipe(broadcast, labels=yv_ya)
    if "year_act" in cols:
        return df.pipe(broadcast, year_act=years)
    if "year_vtg" in cols:
        return df.pipe(broadcast, year_vtg=years)
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
    if par_name in {"input", "output"}:
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
      ``technical_lifetime``) get ``year_vtg`` over model years
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


def add_data(
    scenario: "message_ix.Scenario",
    dry_run: bool = False,
) -> None:
    """Add parameter rows from the CSV (after Spec set add/remove).

    Called from :func:`apply_spec` while the scenario is checked out.
    Level ``refinery`` (and its parameter data) is already stripped via
    :file:`set.yaml`.
    """
    if dry_run:
        log.info("dry_run: skipping efc_tech add_data")
        return

    info = ScenarioInfo(scenario)
    data = gen_data_from_csv(scenario, info)
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
