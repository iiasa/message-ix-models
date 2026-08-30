"""Policies."""

from __future__ import annotations

import logging
import re
from abc import ABC
from collections.abc import Collection
from typing import TYPE_CHECKING, cast

import message_ix
import pandas as pd
import yaml
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    broadcast,
    local_data_path,
    nodes_ex_world,
    private_data_path,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import TypeVar

    from message_ix_models.util.context import Context

    T = TypeVar("T", bound="Policy")

log = logging.getLogger(__name__)


class Policy(ABC):
    """Base class for policies.

    This class has no attributes or public methods. Other modules in
    :mod:`message_ix_models`:

    - **should** subclass Policy to represent different kinds of policy.
    - **may** add attributes, methods, etc. to aid with the *implementation* of those
      policies in concrete scenarios.
    - in contrast, **may** use minimal subclasses as mere flags to be interpreted by
      other code.

    The default implementation of :func:`hash` returns a value the same for every
    instance of a subclass. This means that two instances of the same subclass hash
    equal. See :attr:`.Config.policy`.
    """

    def __hash__(self) -> int:
        return hash(type(self))


def single_policy_of_type(collection: Collection[Policy], cls: type["T"]) -> "T | None":
    """Return a single member of `collection` of type `cls`."""
    if matches := list(filter(lambda p: isinstance(p, cls), collection)):
        if len(matches) > 1:
            raise ValueError(f"Ambiguous: {len(matches)} instance of {cls}")
        return cast("T", matches[0])

    return None


# Older ScenarioRunner class dependent on message_data repository
def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE"
) -> message_ix.Scenario:
    """Plain solve."""
    solve_options = {
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    }

    scenario.solve(model, solve_options=solve_options)
    scenario.set_as_default()

    return scenario


def make_scenario_runner(
    context,
):
    """Create and initialize a ScenarioRunner for policy scenario generation."""
    from message_data.model.scenario_runner import ScenarioRunner

    biomass_trade = getattr(context, "biomass_trade", False)

    config_path = (
        private_data_path(*context.policy_config_path)
        if isinstance(context.policy_config_path, tuple)
        else private_data_path(context.policy_config_path)
    )
    with open(config_path) as f:
        config = yaml.safe_load(f)

    model_name = context.dest_scenario["model"]
    model_config = config[model_name]
    # print(f"DEBUG model_config[{model_name!r}] = {model_config}")

    slack_data = model_config["policy_slacks"][model_config["slack_scn"]][context.ssp]

    sr = ScenarioRunner(
        context,
        slack_data=slack_data,
        biomass_trade=biomass_trade,
    )

    # Pre-populate baseline scenario(s) if they do not exist.
    # Use baseline_DEFAULT to match the workflow target
    # (e.g., base cloned -> baseline_DEFAULT).
    # policy_baseline is used by the runner's internal logic; baseline_DEFAULT is the
    # prerequisite name passed to sr.add(..., start_scen="baseline_DEFAULT")
    # by add_glasgow, etc.
    if "policy_baseline" not in sr.scen:
        base_scenario = message_ix.Scenario(
            mp=sr.mp,
            model=sr.model_name,
            scenario="baseline_DEFAULT",
            cache=False,
        )
        sr.scen["policy_baseline"] = base_scenario
        sr.scen["baseline_DEFAULT"] = base_scenario

    return sr


def add_NPi2030(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Add NPi2030 to the scenario."""

    sr = make_scenario_runner(context)
    sr.add(
        "NPi2030",
        "baseline_DEFAULT",
        # must start with this scenario name (hard-coded in the general scenario runner)
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        run_reporting=False,
        solve_typ="MESSAGE-MACRO",
    )

    # sr.add(
    #     "npi_low_dem_scen",
    #     "NPi2030",
    #     slice_year=2025,
    #     tax_emission=150,
    #     run_reporting = False,
    #     solve_typ="MESSAGE-MACRO",
    # )

    sr.run_all()

    # return sr.scen["npi_low_dem_scen"]
    return sr.scen["NPi2030"]


def add_NDC2030(context, scenario):
    """Add NDC policies to the scenario."""
    sr = make_scenario_runner(context)

    sr.add(
        "INDC2030i_weak",
        "baseline_DEFAULT",
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        copy_demands="baseline_low_dem_scen",
        # or other low demand scenario
        # e.g., "npi_low_dem_scen"
        run_reporting=False,
        solve_typ="MESSAGE-MACRO",
    )

    sr.run_all()

    return sr.scen["INDC2030i_weak"]


def add_glasgow(context, scenario, level, start_scen, target_scen, slice_yr):
    """Add Glasgow policies to the scenario.

    Examples
    --------
    In a :class:`~message_ix_models.workflow.Workflow`, use this function as the step
    action and pass the same keyword arguments that
    :meth:`~message_ix_models.workflow.Workflow.add_step` forwards to the
    callable::

        wf.add_step(
            "glasgow_partial_2030 solved",
            "base reported",
            add_glasgow,
            target=f"{model_name}/glasgow_partial_2030",
            target_scen="glasgow_partial_2030",
            slice_yr=2025,
            start_scen="baseline_DEFAULT",
            level="Partial",
        )

        wf.add_step(
            "glasgow_full_2030 solved",
            "base reported",
            add_glasgow,
            target=f"{model_name}/glasgow_full_2030",
            start_scen="baseline_DEFAULT",
            target_scen="glasgow_full_2030",
            slice_yr=2025,
            level="Full",
        )
    """
    sr = make_scenario_runner(context)

    # Prepare add() arguments
    add_kwargs = {
        "mk_INDC": True,
        "slice_year": slice_yr,
        "run_reporting": False,
        "solve_typ": "MESSAGE-MACRO",
    }
    if level.lower() == "full":
        add_kwargs["copy_demands"] = "baseline_low_dem_scen"
        # or other low demand scenario
        # e.g., "npi_low_dem_scen"

    sr.add(target_scen, start_scen, **add_kwargs)

    sr.run_all()

    # Return the target scenario that was created
    return sr.scen[target_scen]


def add_forever_constant(
    context: Context,
    scenario: message_ix.Scenario,
    specified_price: Mapping[str, float] | None = None,
    solve_type: str = "MESSAGE-MACRO",
) -> message_ix.Scenario:
    """Apply constant carbon prices from first model year to 2110.

    - If `specified_price` is given, use those node-level constant values.
    - Otherwise, use model-derived values from ``PRICE_EMISSION`` at
      ``scenario.firstmodelyear`` and extend them as constants through 2110.

    Example
    -------
    `specified_price` can be::

        {
            "R12_AFR": 7.33,
            "R12_CHN": 7.33,
            "R12_EEU": 91.667,...
        }
    or copied from a lookup run.
    """
    fmy = int(scenario.firstmodelyear)

    info = ScenarioInfo(scenario)
    model_years = [y for y in info.Y if fmy <= y <= 2110]

    if specified_price:
        base_prices = pd.DataFrame(
            {"node": list(specified_price), "lvl": list(specified_price.values())}
        )
    else:
        base_prices = scenario.var("PRICE_EMISSION").loc[
            lambda df: df.year == fmy, ["node", "lvl"]
        ]
        missing = (set(info.N) - {"World", "R12_GLB"}) - set(base_prices.node)
        if missing:
            base_prices = pd.concat(
                [base_prices, pd.DataFrame({"node": list(missing), "lvl": 0})],
                ignore_index=True,
            )

    price_long = pd.concat(
        [base_prices.assign(year=year) for year in model_years], ignore_index=True
    )
    df = make_df(
        "tax_emission",
        node=price_long["node"],
        type_emission="TCE",
        type_tec="all",
        type_year=price_long["year"],
        unit="USD/tC",
        value=price_long["lvl"],
    )

    with scenario.transact("applying constant cprice"):
        bound_df = scenario.par("bound_emission")
        if not bound_df.empty:
            scenario.remove_par("bound_emission", bound_df)
        scenario.add_par("tax_emission", df)

    if specified_price:
        detail = ", ".join(
            f"{node}={price}" for node, price in sorted(specified_price.items())
        )
        log.info(
            f"Added specified constant carbon prices ({fmy}-2110) to "
            f"{scenario.model}/{scenario.scenario}: {detail}"
        )
    else:
        log.info(
            f"Added constant carbon prices ({fmy}-2110) to "
            f"{scenario.model}/{scenario.scenario}"
        )
    solve(context, scenario, model=solve_type)
    scenario.set_as_default()
    return scenario


def add_forever_interpolate(
    context: Context,
    scenario: message_ix.Scenario,
    price_2100: float = 200,
    solve_type: str = "MESSAGE-MACRO",
) -> message_ix.Scenario:
    """Apply interpolated carbon prices from first model year to 2110.

    - Base values are taken from ``PRICE_EMISSION`` at ``scenario.firstmodelyear``.
    - Values are interpolated to reach `price_2100` in years 2100 and 2110.
    """
    fmy = int(scenario.firstmodelyear)
    info = ScenarioInfo(scenario)
    regions = set(info.N) - {"World", "R12_GLB"}
    years = [y for y in info.Y if fmy <= y <= 2110]

    base = scenario.var("PRICE_EMISSION").loc[
        lambda df: df.year == fmy, ["node", "lvl"]
    ]
    missing = regions - set(base.node)
    if missing:
        base = pd.concat(
            [base, pd.DataFrame({"node": list(missing), "lvl": 0})], ignore_index=True
        )
    base = base.assign(year=fmy)

    long = (
        pd.concat(
            [
                base,
                pd.DataFrame({"node": list(regions), "lvl": price_2100, "year": 2100}),
                pd.DataFrame({"node": list(regions), "lvl": price_2100, "year": 2110}),
            ],
            ignore_index=True,
        )
        .pivot_table(values="lvl", index="year", columns="node")
        .reindex(sorted(set(years) | {fmy, 2100, 2110}))
        .sort_index()
        .interpolate(method="index")
        .loc[years]
        .reset_index()
        .melt(id_vars="year", var_name="node", value_name="lvl")
    )
    df = make_df(
        "tax_emission",
        node=long["node"],
        type_emission="TCE",
        type_tec="all",
        type_year=long["year"],
        unit="USD/tC",
        value=long["lvl"],
    )

    with scenario.transact("applying interpolated cprice"):
        bound_df = scenario.par("bound_emission")
        if not bound_df.empty:
            scenario.remove_par("bound_emission", bound_df)
        scenario.add_par("tax_emission", df)

    log.info(
        f"Added interpolated carbon prices ({fmy}-2110, {price_2100} at 2100/2110) to "
        f"{scenario.model}/{scenario.scenario}"
    )
    solve(context, scenario, model=solve_type)
    scenario.set_as_default()
    return scenario


def load_anchor_data(context: Context) -> pd.DataFrame:
    """Read anchor CSV from static data under local data."""
    _ANCHOR_REQUIRED_COLUMNS: tuple[str, ...] = (
        "policy_id",
        "depth",
        "speed",
        "arrival",
        "node",
        "parameter",
        "unit_convertion",
        "year_act",
    )
    _ANCHOR_DROP_COLUMNS: frozenset[str] = frozenset({"description", "indicator"})
    cols = list(_ANCHOR_REQUIRED_COLUMNS)

    anchor_name = getattr(context, "anchor_data_file", None)
    if not anchor_name:
        log.warning(
            "load_anchor_data: context.anchor_data_file is not set; "
            "returning empty DataFrame"
        )
        return pd.DataFrame(columns=cols)

    path = local_data_path("anchor", anchor_name, context=context)
    if not path.is_file():
        private_path = private_data_path("anchor", anchor_name)
        if private_path.is_file():
            log.info(
                "load_anchor_data: %s not found in local data; "
                "falling back to private data at %s",
                path.resolve(),
                private_path.resolve(),
            )
            path = private_path
        else:
            log.warning(
                "load_anchor_data: anchor CSV not found at %s or %s "
                "(checked local data and message-data); returning empty DataFrame",
                path.resolve(),
                private_path.resolve(),
            )
            return pd.DataFrame(columns=cols)

    df = pd.read_csv(path)
    df = df.drop(columns=[c for c in _ANCHOR_DROP_COLUMNS if c in df.columns])
    missing = [c for c in _ANCHOR_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"anchor CSV {path} is missing required columns {missing}; "
            f"columns after filter: {list(df.columns)}"
        )
    df = df.copy()

    # Numeric parsing and derived values.
    df["depth"] = pd.to_numeric(df["depth"], errors="coerce")
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df["arrival"] = pd.to_numeric(df["arrival"], errors="coerce")
    df["year_act"] = pd.to_numeric(df["year_act"], errors="coerce")
    df["unit_convertion"] = pd.to_numeric(df["unit_convertion"], errors="coerce")
    df["depth_converted"] = df["depth"] * df["unit_convertion"]

    # Broadcast if multiple regions
    df["node"] = df["node"].astype(str).str.split(",")
    df = df.explode("node", ignore_index=True)
    df["node"] = df["node"].str.strip()
    df = df.loc[df["node"].ne("")].copy()

    log.info("load_anchor_data: read anchor data from %s", path.resolve())
    log.info("load_anchor_data: preview (head):\n%s", df.head().to_string())

    # For debugging
    debug_path = local_data_path("anchor", "_debug_loaded.csv", context=context)
    df.to_csv(debug_path, index=False)

    return df


# Anchor helpers: ``anchor_*`` apply policies per parameter; ``_prepare_*`` build
# parameter tables; unprefixed ``_*`` are small shared utilities.


def _growth(base_value: float, speed: float, step: int) -> float:
    return base_value * ((1 + speed / 100.0) ** (5 * step) - 1.0)


def _year_act_num(df: pd.DataFrame) -> pd.Series:
    """Numeric year for comparisons (``year_act``, ``year_rel``, or ``year_vtg``)."""
    for col in ("year_act", "year_rel", "year_vtg"):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    raise KeyError(f"no year column in DataFrame; columns: {list(df.columns)}")


def _row_node(row: pd.Series) -> str | None:
    """Return the anchor row node, or :obj:`None` to apply to every region."""
    if "node" not in row.index or pd.isna(row["node"]):
        return None
    node = str(row["node"]).strip()
    return node or None


def _years_from(g: pd.DataFrame, start_year: int, end_year: int | None = None):
    """Sorted integer years in *g* from *start_year*, optionally up to *end_year*."""
    years = []
    for y in _year_act_num(g).dropna().unique():
        year = int(y)
        if year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue
        years.append(year)
    return sorted(years)


def _node_year_mask(df: pd.DataFrame, node_col: str, region, year: int) -> pd.Series:
    return (df[node_col].astype(str) == str(region)) & (_year_act_num(df) == year)


# YJ: so far 3 arguments for emission_factor, input, share_comm*,
# YJ: relation_activity; not good enough, later think about smarter refactoring
def _apply_speed_depth(
    df_update: pd.DataFrame,
    df_loop: pd.DataFrame,
    node_col: str,
) -> None:
    """Apply speed growth from year_act, capped/floored at depth_converted.

    ``year_act`` keeps the existing value; later periods compound toward
    ``depth_converted``. Growth is applied only to the matching ``node``.
    """
    cols = ["year_act", "speed", "depth_converted"]
    if "node" in df_loop.columns:
        cols = [*cols, "node"]
    for _, r in df_loop.loc[df_loop["speed"].notna(), cols].iterrows():
        if (
            pd.isna(r["year_act"])
            or pd.isna(r["speed"])
            or pd.isna(r["depth_converted"])
        ):
            continue
        start_year = int(r["year_act"])
        speed = float(r["speed"])
        depth_cap = float(r["depth_converted"])
        target_node = _row_node(r)
        for region, g in df_update.groupby(node_col):
            if target_node is not None and str(region).strip() != target_node:
                continue
            base = g.loc[_year_act_num(g) == start_year, "value"]
            if base.empty:
                continue
            base_value = float(base.iat[0])
            # Compound growth from 0 stays 0; use depth as the scale instead.
            growth_ref = base_value if base_value != 0.0 else depth_cap
            years = _years_from(g, start_year)
            for step, year in enumerate(years):
                idx = _node_year_mask(df_update, node_col, region, year)
                candidate = base_value + _growth(growth_ref, speed, step)
                if speed >= 0:
                    df_update.loc[idx, "value"] = min(depth_cap, candidate)
                else:
                    df_update.loc[idx, "value"] = max(depth_cap, candidate)


def _apply_speed_arrival(
    df_update: pd.DataFrame,
    df_loop: pd.DataFrame,
    node_col: str,
) -> None:
    """Apply speed growth between year_act and arrival."""
    cols = ["year_act", "speed", "arrival"]
    if "node" in df_loop.columns:
        cols = [*cols, "node"]
    for _, r in df_loop.loc[
        df_loop["speed"].notna() & df_loop["arrival"].notna(),
        cols,
    ].iterrows():
        if r[cols[:3]].isna().any():
            continue
        start_year = int(r["year_act"])
        end_year = int(r["arrival"])
        speed = float(r["speed"])
        if end_year < start_year:
            continue
        target_node = _row_node(r)
        for region, g in df_update.groupby(node_col):
            if target_node is not None and str(region).strip() != target_node:
                continue
            base = g.loc[_year_act_num(g) == start_year, "value"]
            if base.empty:
                continue
            base_value = float(base.iat[0])
            growth_ref = base_value if base_value != 0.0 else 1.0
            years = _years_from(g, start_year, end_year)
            for step, year in enumerate(years):
                idx = _node_year_mask(df_update, node_col, region, year)
                df_update.loc[idx, "value"] = base_value + _growth(
                    growth_ref, speed, step
                )


# YJ: hmm maybe not only the target year but by default all years onward?
def _apply_arrival_depth(
    df_update: pd.DataFrame,
    df_loop: pd.DataFrame,
    node_col: str,
) -> None:
    """Set value to depth_converted at ``year_act`` for matching node."""
    cols = ["year_act", "depth_converted"]
    if "node" in df_loop.columns:
        cols = [*cols, "node"]
    for _, r in df_loop.loc[df_loop["arrival"].notna(), cols].iterrows():
        if pd.isna(r["year_act"]) or pd.isna(r["depth_converted"]):
            continue
        target_node = _row_node(r)
        target_year = int(r["year_act"])
        idx = _year_act_num(df_update) == target_year
        if target_node is not None:
            idx = idx & (df_update[node_col].astype(str) == target_node)
        df_update.loc[idx, "value"] = float(r["depth_converted"])


def _apply_depth_speed_arrival(
    df_initial: pd.DataFrame,
    df_loop: pd.DataFrame,
    node_col: str,
) -> pd.DataFrame:
    """Return an updated copy of *df_initial* using depth/speed/arrival logic."""
    has_speed = df_loop["speed"].notna().any()
    has_arrival = df_loop["arrival"].notna().any()
    df_update = df_initial.copy()

    if has_speed and not has_arrival:
        _apply_speed_depth(df_update, df_loop, node_col)
    elif has_speed and has_arrival:
        _apply_speed_arrival(df_update, df_loop, node_col)
    elif has_arrival and not has_speed:
        _apply_arrival_depth(df_update, df_loop, node_col)
    else:
        log.warning(
            "_apply_depth_speed_arrival: no speed or arrival; "
            "parameter values are left unchanged"
        )

    return df_update


def add_anchor(
    context: Context,
    scenario: message_ix.Scenario,
    policy_ids: Collection[str] | None = None,
) -> message_ix.Scenario:
    """Add anchor data to the scenario.

    Parameters
    ----------
    policy_ids : collection of str, optional
        If given, only rows with these ``policy_id`` values are applied.

    Examples
    --------
    Direct call::

        add_anchor(context, scenario, policy_ids=["gp_1", "gp_2"])

    In a :class:`~message_ix_models.workflow.Workflow` step, pass ``policy_ids``
    as a keyword argument to :meth:`~message_ix_models.workflow.Workflow.add_step`::

        wf.add_step("with anchors", "base", add_anchor, policy_ids=["gp_1"])
    """

    df_anchor = load_anchor_data(context)

    if policy_ids is not None:
        ids = {str(p) for p in policy_ids}
        df_anchor = df_anchor.loc[df_anchor["policy_id"].astype(str).isin(ids)].copy()
        if df_anchor.empty:
            log.warning("add_anchor: no anchor rows match policy_ids=%s", sorted(ids))

    anchor_emission_factor(df_anchor, scenario)
    anchor_input_output(df_anchor, scenario)
    anchor_growth_activity(df_anchor, scenario)
    anchor_inv_cost(df_anchor, scenario)
    anchor_share_comm(df_anchor, scenario)
    anchor_relation_activity(df_anchor, scenario)

    return scenario


def anchor_emission_factor(  # noqa: C901
    df_anchor: pd.DataFrame, scenario: message_ix.Scenario
) -> None:
    """Apply anchor settings to parameter ``emission_factor``."""

    # Filter for emission_factor parameter rows
    df_ef = df_anchor.loc[df_anchor["parameter"] == "emission_factor"].copy()
    if df_ef.empty:
        log.info("anchor_emission_factor: no policies tuning 'emission_factor'")
        return

    updates: list[pd.DataFrame] = []

    key_cols = ["policy_id", "technology", "mode", "emission"]
    for keys, group in df_ef.groupby(key_cols, dropna=False):
        _policy_id, technology, mode, emission = keys

        df_initial = scenario.par(
            "emission_factor",
            filters={
                "technology": [technology],
                "mode": [mode],
                "emission": [emission],
            },
        )

        df_ef_loop = group.copy()
        df_update = _apply_depth_speed_arrival(
            df_initial, df_ef_loop, node_col="node_loc"
        )
        updates.append(df_update)

    df_updates = pd.concat(updates, ignore_index=True) if updates else pd.DataFrame()
    if df_updates.empty:
        return

    # For debugging
    debug_updates_path = local_data_path("anchor", "_debug_anchor_emission_factor.csv")
    df_updates.to_csv(debug_updates_path, index=False)

    with scenario.transact("apply anchor emission_factor"):
        scenario.add_par("emission_factor", df_updates)

    log.info(
        "anchor_emission_factor: added %d emission_factor rows",
        len(df_updates),
    )

    return


_INPUT_OUTPUT_PARS = ("input", "output")


def anchor_input_output(  # noqa: C901
    df_anchor: pd.DataFrame, scenario: message_ix.Scenario
) -> None:
    """Apply anchor settings to ``input`` and ``output``."""

    df_io = df_anchor.loc[df_anchor["parameter"].isin(_INPUT_OUTPUT_PARS)].copy()
    if df_io.empty:
        log.info("anchor_input_output: not applicable")
        return

    for par_name, df_par in df_io.groupby("parameter", dropna=False):
        updates: list[pd.DataFrame] = []
        for (_policy_id, technology, mode, commodity, level), group in df_par.groupby(
            ["policy_id", "technology", "mode", "commodity", "level"], dropna=False
        ):
            df_initial = scenario.par(
                par_name,
                filters={
                    "technology": [technology],
                    "mode": [mode],
                    "commodity": [commodity],
                    "level": [level],
                },
            )
            updates.append(
                _apply_depth_speed_arrival(
                    df_initial, group.copy(), node_col="node_loc"
                )
            )

        df_updates = (
            pd.concat(updates, ignore_index=True) if updates else pd.DataFrame()
        )
        if df_updates.empty:
            continue

        debug_updates_path = local_data_path("anchor", f"_debug_anchor_{par_name}.csv")
        df_updates.to_csv(debug_updates_path, index=False)

        with scenario.transact(f"apply anchor {par_name}"):
            scenario.add_par(par_name, df_updates)

        log.info(
            "anchor_input_output: added %d %s rows",
            len(df_updates),
            par_name,
        )

    return


_GROWTH_ACTIVITY_PARS = ("growth_activity_up", "growth_activity_lo")


def anchor_growth_activity(  # noqa: C901
    df_anchor: pd.DataFrame, scenario: message_ix.Scenario
) -> None:
    """Apply anchor settings to ``growth_activity_up`` and ``growth_activity_lo``."""

    df_growth = df_anchor.loc[df_anchor["parameter"].isin(_GROWTH_ACTIVITY_PARS)].copy()
    if df_growth.empty:
        log.info(
            "anchor_growth_activity: no policies tuning "
            "'growth_activity_up'/'growth_activity_lo'"
        )
        return

    info = ScenarioInfo(scenario)

    for par_name, df_par in df_growth.groupby("parameter", dropna=False):
        updates: list[pd.DataFrame] = []
        for (_policy_id, technology), group in df_par.groupby(
            ["policy_id", "technology"], dropna=False
        ):
            nodes = _nodes(group) or nodes_ex_world(info.N)
            years = [int(y) for y in info.Y]
            techs = [t.strip() for t in str(technology).split(",") if t.strip()]
            for tec in techs:
                df_initial = make_df(
                    par_name,
                    technology=tec,
                    time="year",
                    unit="???",
                    value=0.0,
                ).pipe(broadcast, node_loc=nodes, year_act=years)

                updates.append(
                    _apply_depth_speed_arrival(
                        df_initial, group.copy(), node_col="node_loc"
                    )
                )

        df_updates = pd.concat(updates, ignore_index=True)
        if df_updates.empty:
            continue

        debug_updates_path = local_data_path("anchor", f"_debug_anchor_{par_name}.csv")
        df_updates.to_csv(debug_updates_path, index=False)

        with scenario.transact(f"apply anchor {par_name}"):
            scenario.add_par(par_name, df_updates)

        log.info(
            "anchor_growth_activity: added %d %s rows",
            len(df_updates),
            par_name,
        )

    return


def anchor_inv_cost(  # noqa: C901
    df_anchor: pd.DataFrame, scenario: message_ix.Scenario
) -> None:
    """Apply anchor settings to parameter ``inv_cost``."""

    df_ic = df_anchor.loc[df_anchor["parameter"] == "inv_cost"].copy()
    if df_ic.empty:
        log.info("anchor_inv_cost: no policies tuning 'inv_cost'")
        return

    info = ScenarioInfo(scenario)
    updates: list[pd.DataFrame] = []

    for (_policy_id, technology), group in df_ic.groupby(
        ["policy_id", "technology"], dropna=False
    ):
        nodes = _nodes(group) or nodes_ex_world(info.N)
        years = [int(y) for y in info.Y]
        techs = [t.strip() for t in str(technology).split(",") if t.strip()]
        for tec in techs:
            df_initial = scenario.par(
                "inv_cost",
                filters={"technology": [tec]},
            )
            if df_initial.empty:
                df_initial = make_df(
                    "inv_cost",
                    technology=tec,
                    unit="???",
                    value=0.0,
                ).pipe(broadcast, node_loc=nodes, year_vtg=years)

            updates.append(
                _apply_depth_speed_arrival(
                    df_initial, group.copy(), node_col="node_loc"
                )
            )

    df_updates = pd.concat(updates, ignore_index=True) if updates else pd.DataFrame()
    if df_updates.empty:
        return

    debug_updates_path = local_data_path("anchor", "_debug_anchor_inv_cost.csv")
    df_updates.to_csv(debug_updates_path, index=False)

    with scenario.transact("apply anchor inv_cost"):
        scenario.add_par("inv_cost", df_updates)

    log.info(
        "anchor_inv_cost: added %d inv_cost rows",
        len(df_updates),
    )

    return


_SHARE_COMM_PARS = ("share_commodity_lo", "share_commodity_up")


def anchor_share_comm(  # noqa: C901
    df_anchor: pd.DataFrame, scenario: message_ix.Scenario
) -> None:
    """Apply anchor settings to ``share_commodity_lo`` and ``share_commodity_up``.

    For each policy group the function registers share sets (``shares``,
    ``type_tec``, ``cat_tec``, ``map_shares_commodity_share``,
    ``map_shares_commodity_total``) once, then adds whichever bound parameters
    are present in the anchor data.
    """

    df_share = df_anchor.loc[df_anchor["parameter"].isin(_SHARE_COMM_PARS)].copy()
    if df_share.empty:
        log.info("anchor_share_comm: not applicable")
        return

    map_shares_commodity_share_rows: list[pd.DataFrame] = []
    map_shares_commodity_total_rows: list[pd.DataFrame] = []
    cat_tec_rows: list[pd.DataFrame] = []
    bound_rows: dict[str, list[pd.DataFrame]] = {par: [] for par in _SHARE_COMM_PARS}
    share_names: list[str] = []

    key_cols = ["policy_id", "technology", "mode", "commodity", "level"]
    available = [c for c in key_cols if c in df_share.columns]
    for _, group in df_share.groupby(available, dropna=False):
        row0 = group.iloc[0]
        share_name = f"policy_{row0['policy_id']}"
        share_names.append(share_name)

        map_shares_commodity_share_rows.append(
            make_df(
                "map_shares_commodity_share",
                shares=share_name,
                node_share=group["node"],
                node=group["node"],
                type_tec=f"{share_name}_share",
                mode="M1",
                commodity=row0["commodity"],
                level=row0["level"],
            )
        )
        map_shares_commodity_total_rows.append(
            make_df(
                "map_shares_commodity_total",
                shares=share_name,
                node_share=group["node"],
                node=group["node"],
                type_tec=f"{share_name}_total",
                mode="M1",
                commodity=row0["commodity"],
                level=row0["level"],
            )
        )

        for par_name in _SHARE_COMM_PARS:
            df_bound = group.loc[group["parameter"] == par_name]
            if df_bound.empty:
                continue
            bound_rows[par_name].append(
                make_df(
                    par_name,
                    shares=share_name,
                    node_share=df_bound["node"],
                    year_act=df_bound["year_act"],
                    time="year",
                    unit="-",
                    value=df_bound["depth"],
                )
            )

        # TODO: apply better ways to group technologies
        if re.search(r"[\^\$\|\(\)]", str(row0["technology"])):
            tech_share = [
                t
                for t in map(str, scenario.set("technology"))
                if re.search(str(row0["technology"]), t)
            ]
        else:
            tech_share = [str(row0["technology"])]
        if tech_share:
            cat_tec_rows.append(
                pd.DataFrame(
                    {"type_tec": f"{share_name}_share", "technology": tech_share}
                )
            )

        df_output = scenario.par(
            "output",
            filters={"commodity": [row0["commodity"]], "level": [row0["level"]]},
        )
        tech_total_group = (
            sorted(df_output["technology"].dropna().astype(str).unique().tolist())
            if not df_output.empty
            else []
        )
        if tech_total_group:
            cat_tec_rows.append(
                pd.DataFrame(
                    {"type_tec": f"{share_name}_total", "technology": tech_total_group}
                )
            )

    df_map_share = pd.concat(
        map_shares_commodity_share_rows, ignore_index=True
    ).drop_duplicates()
    df_map_total = pd.concat(
        map_shares_commodity_total_rows, ignore_index=True
    ).drop_duplicates()
    df_cat_tec = pd.concat(cat_tec_rows, ignore_index=True).drop_duplicates()
    bound_frames = {par: _concat_or_empty(rows) for par, rows in bound_rows.items()}

    debug_dir = local_data_path("anchor")
    pd.DataFrame({"shares": sorted(set(share_names))}).to_csv(
        debug_dir / "_debug_anchor_shares.csv", index=False
    )
    df_map_share.to_csv(
        debug_dir / "_debug_anchor_map_shares_commodity_share.csv", index=False
    )
    df_map_total.to_csv(
        debug_dir / "_debug_anchor_map_shares_commodity_total.csv", index=False
    )
    df_cat_tec.to_csv(debug_dir / "_debug_anchor_cat_tec.csv", index=False)
    for par_name, df_bound in bound_frames.items():
        if not df_bound.empty:
            df_bound.to_csv(debug_dir / f"_debug_anchor_{par_name}.csv", index=False)

    if not share_names or all(df.empty for df in bound_frames.values()):
        return

    with scenario.transact("apply anchor share_commodity_lo / share_commodity_up"):
        scenario.add_set("shares", sorted(set(share_names)))
        scenario.add_set(
            "type_tec",
            sorted(df_cat_tec["type_tec"].dropna().astype(str).unique().tolist()),
        )
        scenario.add_set("cat_tec", df_cat_tec)
        scenario.add_set("map_shares_commodity_share", df_map_share)
        scenario.add_set("map_shares_commodity_total", df_map_total)
        for par_name, df_bound in bound_frames.items():
            if not df_bound.empty:
                scenario.add_par(par_name, df_bound)

    log.info(
        "anchor_share_comm: added %d share_commodity_lo rows, "
        "%d share_commodity_up rows",
        len(bound_frames["share_commodity_lo"]),
        len(bound_frames["share_commodity_up"]),
    )

    return


def _nodes(df: pd.DataFrame) -> list[str]:
    """Unique non-empty node names from an exploded anchor frame."""
    return (
        df["node"].astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist()
    )


def _tech_mode(df: pd.DataFrame) -> pd.DataFrame:
    """Expand comma-separated ``technology`` and ``mode`` into cross-product rows."""
    rows: list[pd.Series] = []
    for _, row in df.iterrows():
        techs = [t.strip() for t in str(row["technology"]).split(",") if t.strip()]
        modes = [m.strip() for m in str(row["mode"]).split(",") if m.strip()]
        for tec in techs:
            for mode in modes:
                new = row.copy()
                new["technology"] = tec
                new["mode"] = mode
                rows.append(new)
    return pd.DataFrame(rows) if rows else df.iloc[0:0].copy()


def _relation_years(scenario: message_ix.Scenario, relation_name: str) -> list[int]:
    """Years to use when scaffolding a relation parameter time series."""
    existing = scenario.par("relation_activity", filters={"relation": [relation_name]})
    if not existing.empty and "year_act" in existing.columns:
        years = (
            pd.to_numeric(existing["year_act"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        if years:
            return sorted(set(years))
    return [int(y) for y in ScenarioInfo(scenario).Y]


def _prepare_relation_activity_coefficient(
    scenario: message_ix.Scenario,
    technology: str,
    mode: str,
    commodity: str,
    level: str,
    node: str,
    year_act: int,
    unit_convertion: float,
    cache: dict[tuple[str, str, str, str], pd.DataFrame],
) -> float | None:
    """Return ``output`` coefficient × ``unit_convertion`` for one anchor row."""
    key = (technology, mode, commodity, level)
    if key not in cache:
        cache[key] = scenario.par(
            "output",
            filters={
                "technology": [technology],
                "mode": [mode],
                "commodity": [commodity],
                "level": [level],
            },
        )

    df_output = cache[key]
    if df_output.empty:
        return None

    match = df_output.loc[
        (df_output["node_loc"].astype(str) == node)
        & (_year_act_num(df_output) == year_act)
    ]
    if match.empty:
        return None

    return float(match.iloc[0]["value"]) * float(unit_convertion)


def _prepare_relation_bounds(
    df_bound: pd.DataFrame,
    scenario: message_ix.Scenario,
    par_name: str,
    relation_names: list[str],
) -> list[pd.DataFrame]:
    """Prepare ``relation_lower`` or ``relation_upper`` rows from anchor *df_bound*."""
    rows: list[pd.DataFrame] = []
    if df_bound.empty:
        return rows

    for relation_name, group in df_bound.groupby("relation", dropna=False):
        relation_name = str(relation_name)
        if relation_name not in relation_names:
            relation_names.append(relation_name)

        df_loop = group.copy()
        df_initial = scenario.par(par_name, filters={"relation": [relation_name]})

        if df_initial.empty:
            nodes = _nodes(df_loop)
            years = _relation_years(scenario, relation_name)
            frames = [
                make_df(
                    par_name,
                    relation=relation_name,
                    node_rel=node,
                    year_rel=year,
                    value=0.0,
                    unit="-",
                )
                for node in nodes
                for year in years
            ]
            df_initial = (
                pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            )

        if not df_initial.empty:
            rows.append(
                _apply_depth_speed_arrival(df_initial, df_loop, node_col="node_rel")
            )

    return rows


def _concat_or_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate *frames*, or return an empty DataFrame if there are none."""
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def anchor_relation_activity(  # noqa: C901
    df_anchor: pd.DataFrame, scenario: message_ix.Scenario
) -> None:
    """Apply anchor settings to ``relation_activity`` and optional bound parameters.

    For each unique ``relation`` value in the anchor data the function:

    * registers the relation name in the ``relation`` set if it is not already
      present,
    * builds ``relation_activity`` rows (expanding comma-separated ``technology``
      and ``mode`` fields into all cross-product pairs) with coefficients taken
      from the scenario ``output`` (matching technology, mode, commodity, and
      level) multiplied by ``unit_convertion``, and
    * if paired ``relation_lower`` and/or ``relation_upper`` rows are present,
      builds those bound rows using ``depth_converted`` and the depth/speed/
      arrival logic shared with the other anchor functions. Missing bounds are
      skipped; activity rows are still added.

    Two anchor rows that share the same ``relation`` are merged into a single
    ``relation_activity`` add (i.e. they contribute different technology/mode
    combinations to the same relation constraint).
    """

    bound_pars = ("relation_lower", "relation_upper")
    df_rel = df_anchor.loc[
        df_anchor["parameter"].isin(["relation_activity", *bound_pars])
    ].copy()
    if df_rel.empty:
        log.info(
            "anchor_relation_activity: no policies tuning "
            "'relation_activity'/'relation_lower'/'relation_upper'"
        )
        return

    relation_activity_rows: list[pd.DataFrame] = []
    relation_names: list[str] = []
    output_cache: dict[tuple[str, str, str, str], pd.DataFrame] = {}

    df_rel_act = df_rel.loc[df_rel["parameter"] == "relation_activity"].copy()

    # Process relation_activity rows
    if not df_rel_act.empty:
        df_rel_act = _tech_mode(df_rel_act)
        for relation_name, group in df_rel_act.groupby("relation", dropna=False):
            relation_name = str(relation_name)
            if relation_name not in relation_names:
                relation_names.append(relation_name)

            act_rows: list[pd.DataFrame] = []
            for _, row in group.iterrows():
                commodity = str(row["commodity"]).strip()
                level = str(row["level"]).strip()
                node = str(row["node"]).strip()
                year_act = int(row["year_act"])
                technology = str(row["technology"])
                mode = str(row["mode"])
                coeff = _prepare_relation_activity_coefficient(
                    scenario,
                    technology,
                    mode,
                    commodity,
                    level,
                    node,
                    year_act,
                    float(row["unit_convertion"]),
                    output_cache,
                )
                if coeff is None:
                    log.warning(
                        "anchor_relation_activity: no output match for "
                        "relation=%s, tech=%s, mode=%s, commodity=%s, "
                        "level=%s, node=%s, year_act=%d; skipping",
                        relation_name,
                        technology,
                        mode,
                        commodity,
                        level,
                        node,
                        year_act,
                    )
                    continue

                act_rows.append(
                    make_df(
                        "relation_activity",
                        relation=relation_name,
                        node_rel=node,
                        year_rel=year_act,
                        node_loc=node,
                        technology=technology,
                        year_act=year_act,
                        mode=mode,
                        value=coeff,
                        unit="-",
                    )
                )

            if act_rows:
                relation_activity_rows.append(_concat_or_empty(act_rows))

    bound_frames: dict[str, pd.DataFrame] = {}
    for par_name in bound_pars:
        bound_frames[par_name] = _concat_or_empty(
            _prepare_relation_bounds(
                df_rel.loc[df_rel["parameter"] == par_name].copy(),
                scenario,
                par_name,
                relation_names,
            )
        )

    df_ra = _concat_or_empty(relation_activity_rows)
    df_rl = bound_frames["relation_lower"]
    df_ru = bound_frames["relation_upper"]

    # Debug dumps
    debug_dir = local_data_path("anchor")
    pd.DataFrame({"relation": sorted(set(relation_names))}).to_csv(
        debug_dir / "_debug_anchor_relations.csv", index=False
    )
    if not df_ra.empty:
        df_ra.to_csv(debug_dir / "_debug_anchor_relation_activity.csv", index=False)
    if not df_rl.empty:
        df_rl.to_csv(debug_dir / "_debug_anchor_relation_lower.csv", index=False)
    if not df_ru.empty:
        df_ru.to_csv(debug_dir / "_debug_anchor_relation_upper.csv", index=False)

    existing_relations = set(map(str, scenario.set("relation")))
    new_relations = sorted(set(relation_names) - existing_relations)

    if not new_relations and df_ra.empty and df_rl.empty and df_ru.empty:
        return

    with scenario.transact(
        "apply anchor relation_activity / relation_lower / relation_upper"
    ):
        if new_relations:
            scenario.add_set("relation", new_relations)
        if not df_ra.empty:
            scenario.add_par("relation_activity", df_ra)
        if not df_rl.empty:
            scenario.add_par("relation_lower", df_rl)
        if not df_ru.empty:
            scenario.add_par("relation_upper", df_ru)

    log.info(
        "anchor_relation_activity: added %d relation_activity rows, "
        "%d relation_lower rows, %d relation_upper rows",
        len(df_ra),
        len(df_rl),
        len(df_ru),
    )

    return
