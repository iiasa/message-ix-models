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


def _unit_for(
    scenario: "message_ix.Scenario",
    par_name: str,
    technology: str,
    *,
    default: str = "GWa",
) -> str:
    """Reuse an existing unit for *technology* / *par_name*, else *default*."""
    sample = scenario.par(par_name, filters={"technology": technology})
    if len(sample):
        return str(sample.iloc[0]["unit"])
    return default


def _row_kwargs(row: pd.Series, par_name: str, unit: str) -> dict[str, Any]:
    """Build ``make_df`` kwargs from one CSV row."""
    kwargs: dict[str, Any] = {
        "technology": row["technology"],
        "value": float(row["value"]),
        "unit": unit,
        "time": "year",
    }
    for dim in _OPTIONAL_DIMS:
        if dim in row.index and pd.notna(row[dim]) and str(row[dim]).strip() != "":
            kwargs[dim] = row[dim]

    if par_name == "input":
        kwargs["time_origin"] = "year"
    elif par_name == "output":
        kwargs["time_dest"] = "year"

    return kwargs


def gen_data_from_csv(
    scenario: "message_ix.Scenario",
    info: ScenarioInfo,
) -> "MutableParameterData":
    """Load :file:`data/efc/efc_tech_parameter.csv` into MESSAGE parameter frames.

    - ``node`` = ``all`` → broadcast to all non-World nodes
    - ``year`` = ``all`` → broadcast to model years (``year_vtg`` = ``year_act``)
    - otherwise use the given node / year as-is
    """
    if not _PARAMETER_CSV.exists():
        log.warning("Missing parameter CSV: %s", _PARAMETER_CSV)
        return {}

    raw = pd.read_csv(_PARAMETER_CSV)
    if raw.empty:
        return {}

    nodes = list(nodes_ex_world(info.N))
    years = list(info.Y)
    buckets: dict[str, list[pd.DataFrame]] = defaultdict(list)

    for _, row in raw.iterrows():
        par_name = str(row["parameter"]).strip()
        unit = _unit_for(scenario, par_name, str(row["technology"]))
        kwargs = _row_kwargs(row, par_name, unit)

        kwargs["node_loc"] = None if _is_all(row["node"]) else str(row["node"])
        if _is_all(row["year"]):
            kwargs["year_act"] = None
            kwargs["year_vtg"] = None
        else:
            y = int(row["year"])
            kwargs["year_act"] = y
            kwargs["year_vtg"] = y

        df = make_df(par_name, **kwargs)

        if _is_all(row["node"]):
            df = df.pipe(broadcast, node_loc=nodes)
        if _is_all(row["year"]):
            df = df.pipe(broadcast, year_act=years).assign(
                year_vtg=lambda d: d["year_act"]
            )

        if par_name in {"input", "output"}:
            df = df.pipe(same_node)

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
