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
from message_ix_models.util import local_data_path

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

    from message_ix_models.util import private_data_path

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
        "timing",
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
        log.warning(
            "load_anchor_data: anchor CSV not found at %s "
            "(expected under message-static-data/anchor/ "
            "within message local data from ixmp config); returning empty DataFrame",
            path.resolve(),
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
    df["timing"] = pd.to_numeric(df["timing"], errors="coerce")
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
    debug_path = local_data_path("anchor", "_debug_loaded_anchor.csv", context=context)
    df.to_csv(debug_path, index=False)

    return df


def add_anchor(
    context: Context,
    scenario: message_ix.Scenario,
) -> message_ix.Scenario:
    """Add anchor data to the scenario."""

    df_anchor = load_anchor_data(context)

    anchor_emission_factor(df_anchor, scenario)
    anchor_input(df_anchor, scenario)
    anchor_share_comm_lo(df_anchor, scenario)

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
        policy_id, technology, mode, emission = keys

        # Prepare original emission_factor rows
        df_initial = scenario.par(
            "emission_factor",
            filters={
                "technology": [technology],
                "mode": [mode],
                "emission": [emission],
            },
        )
        log.info(
            "Initial emission_factor, tech:%s; mode:%s, emission:%s, "
            "region number: %d, slice number: %d",
            technology,
            mode,
            emission,
            df_initial["node_loc"].nunique() if "node_loc" in df_initial else 0,
            pd.to_numeric(df_initial["year_act"], errors="coerce").dropna().nunique()
            if "year_act" in df_initial
            else 0,
        )

        df_ef_loop = group.copy()

        has_speed = df_ef_loop["speed"].notna().any()
        has_timing = df_ef_loop["timing"].notna().any()

        df_update = df_initial.copy()

        # Anchor by speed
        if has_speed and not has_timing:
            for _, r in df_ef_loop.loc[
                df_ef_loop["speed"].notna(), ["year_act", "speed", "depth_converted"]
            ].iterrows():
                if r.isna().any():
                    continue
                start_year = int(r["year_act"])
                speed = float(r["speed"])
                depth_cap = float(r["depth_converted"])
                for region, g in df_update.groupby("node_loc"):
                    base = g.loc[g["year_act"] == start_year, "value"]
                    if base.empty:
                        continue
                    base_value = float(base.iat[0])
                    years = sorted(y for y in g["year_act"].unique() if y >= start_year)
                    for step, year in enumerate(years, start=1):
                        idx = (df_update["node_loc"] == region) & (
                            df_update["year_act"] == year
                        )
                        growth = base_value * ((1 + speed / 100.0) ** (5 * step) - 1.0)
                        candidate = base_value + growth
                        if speed >= 0:
                            df_update.loc[idx, "value"] = min(depth_cap, candidate)
                        else:
                            df_update.loc[idx, "value"] = max(depth_cap, candidate)

        # Anchor by timing
        elif has_timing and not has_speed:
            for _, r in df_ef_loop.loc[
                df_ef_loop["timing"].notna(), ["year_act", "depth_converted", "node"]
            ].iterrows():
                if r.isna().any():
                    continue
                idx = (df_update["year_act"] >= int(r["year_act"])) & (
                    df_update["node_loc"] == r["node"]
                )
                df_update.loc[idx, "value"] = float(r["depth_converted"])

        updates.append(df_update)

    df_updates = pd.concat(updates, ignore_index=True) if updates else pd.DataFrame()
    if df_updates.empty:
        log.info("anchor_emission_factor: no emission_factor updates to apply")
        return

    # For debugging
    debug_updates_path = local_data_path(
        "anchor", "_debug_anchor_updates_emission_factor.csv"
    )
    df_updates.to_csv(debug_updates_path, index=False)

    # with scenario.transact("apply anchor emission_factor"):
    #     scenario.add_par("emission_factor", df_updates)

    log.info(
        "anchor_emission_factor: applied %d updated emission_factor rows",
        len(df_updates),
    )

    return


def anchor_input(df_anchor: pd.DataFrame, scenario: message_ix.Scenario) -> None:  # noqa: C901
    """Apply anchor settings to parameter ``input``."""

    # Filter for emission_factor parameter rows
    df_input = df_anchor.loc[df_anchor["parameter"] == "input"].copy()
    if df_input.empty:
        log.info("anchor_input: no policies tuning 'input'")
        return

    updates: list[pd.DataFrame] = []

    key_cols = ["policy_id", "technology", "mode", "commodity", "level"]
    for keys, group in df_input.groupby(key_cols, dropna=False):
        policy_id, technology, mode, commodity, level = keys

        # Prepare original emission_factor rows
        df_initial = scenario.par(
            "input",
            filters={
                "technology": [technology],
                "mode": [mode],
                "commodity": [commodity],
                "level": [level],
            },
        )
        log.info(
            "Initial input, tech:%s; mode:%s, commodity:%s, level:%s, "
            "region number: %d, slice number: %d",
            technology,
            mode,
            commodity,
            level,
            df_initial["node_loc"].nunique() if "node_loc" in df_initial else 0,
            pd.to_numeric(df_initial["year_act"], errors="coerce").dropna().nunique()
            if "year_act" in df_initial
            else 0,
        )

        df_input_loop = group.copy()

        has_speed = df_input_loop["speed"].notna().any()
        has_timing = df_input_loop["timing"].notna().any()

        df_update = df_initial.copy()

        # Anchor by speed
        if has_speed and not has_timing:
            for _, r in df_input_loop.loc[
                df_input_loop["speed"].notna(), ["year_act", "speed", "depth_converted"]
            ].iterrows():
                if r.isna().any():
                    continue
                start_year = int(r["year_act"])
                speed = float(r["speed"])
                depth_cap = float(r["depth_converted"])
                for region, g in df_update.groupby("node_loc"):
                    base = g.loc[g["year_act"] == start_year, "value"]
                    if base.empty:
                        continue
                    base_value = float(base.iat[0])
                    years = sorted(y for y in g["year_act"].unique() if y >= start_year)
                    for step, year in enumerate(years, start=1):
                        idx = (df_update["node_loc"] == region) & (
                            df_update["year_act"] == year
                        )
                        growth = base_value * ((1 + speed / 100.0) ** (5 * step) - 1.0)
                        candidate = base_value + growth
                        if speed >= 0:
                            df_update.loc[idx, "value"] = min(depth_cap, candidate)
                        else:
                            df_update.loc[idx, "value"] = max(depth_cap, candidate)

        # Anchor by timing
        elif has_timing and not has_speed:
            for _, r in df_input_loop.loc[
                df_input_loop["timing"].notna(), ["year_act", "depth_converted", "node"]
            ].iterrows():
                if r.isna().any():
                    continue
                idx = (df_update["year_act"] >= int(r["year_act"])) & (
                    df_update["node_loc"] == r["node"]
                )
                df_update.loc[idx, "value"] = float(r["depth_converted"])

        updates.append(df_update)

    df_updates = pd.concat(updates, ignore_index=True) if updates else pd.DataFrame()
    if df_updates.empty:
        log.info("anchor_input: no input updates to apply")
        return

    # For debugging
    debug_updates_path = local_data_path("anchor", "_debug_anchor_updates_input.csv")
    df_updates.to_csv(debug_updates_path, index=False)

    # with scenario.transact("apply anchor input"):
    #     scenario.add_par("input", df_updates)

    log.info("anchor_input: applied %d updated input rows", len(df_updates))

    return


def anchor_share_comm_lo(  # noqa: C901
    df_anchor: pd.DataFrame, scenario: message_ix.Scenario
) -> None:
    """Apply anchor settings to parameter ``share_comm``."""

    # Filter for share_comm parameter rows
    df_share_comm = df_anchor.loc[df_anchor["parameter"] == "share_commodity_lo"].copy()
    if df_share_comm.empty:
        log.info("anchor_share_comm: no policies tuning 'share_comm'")
        return

    # Prepare df for share commodity sets and parameters
    map_shares_commodity_share_rows: list[pd.DataFrame] = []
    map_shares_commodity_total_rows: list[pd.DataFrame] = []
    cat_tec_rows: list[pd.DataFrame] = []
    share_commodity_lo_rows: list[pd.DataFrame] = []
    share_names: list[str] = []

    key_cols = ["policy_id", "technology", "mode", "commodity", "level"]
    available = [c for c in key_cols if c in df_share_comm.columns]
    for _, group in df_share_comm.groupby(available, dropna=False):
        df_share_comm_loop = group.copy()
        row0 = df_share_comm_loop.iloc[0]

        share_name = f"policy_{row0['policy_id']}"
        share_names.append(share_name)

        map_shares_commodity_share_rows.append(
            make_df(
                "map_shares_commodity_share",
                shares=share_name,
                node_share=df_share_comm_loop["node"],
                node=df_share_comm_loop["node"],
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
                node_share=df_share_comm_loop["node"],
                node=df_share_comm_loop["node"],
                type_tec=f"{share_name}_total",
                mode="M1",
                commodity=row0["commodity"],
                level=row0["level"],
            )
        )
        share_commodity_lo_rows.append(
            make_df(
                "share_commodity_lo",
                shares=share_name,
                node_share=df_share_comm_loop["node"],
                year_act=df_share_comm_loop["year_act"],
                time="year",
                unit="-",
                value=df_share_comm_loop["depth"],
            )
        )

        # TODO: apply better ways to group technologies
        # Assign technologies for {share_name}_share
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

        # Assign technologies for {share_name}_total
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
    df_share_lo = pd.concat(
        share_commodity_lo_rows, ignore_index=True
    ).drop_duplicates()

    # For debugging
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
    df_share_lo.to_csv(debug_dir / "_debug_anchor_share_commodity_lo.csv", index=False)

    with scenario.transact("apply anchor share_commodity_lo"):
        scenario.add_set("shares", sorted(set(share_names)))
        scenario.add_set(
            "type_tec",
            sorted(df_cat_tec["type_tec"].dropna().astype(str).unique().tolist()),
        )
        scenario.add_set("cat_tec", df_cat_tec)
        scenario.add_set("map_shares_commodity_share", df_map_share)
        scenario.add_set("map_shares_commodity_total", df_map_total)
        scenario.add_par("share_commodity_lo", df_share_lo)

    log.info(
        "anchor_share_comm_lo: added %d shares, %d share-map rows, "
        "%d total-map rows, %d share_commodity_lo rows",
        len(set(share_names)),
        len(df_map_share),
        len(df_map_total),
        len(df_share_lo),
    )

    return
