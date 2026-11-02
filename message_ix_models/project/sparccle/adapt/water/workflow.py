"""SPARCCLE water adaptation scenarios on a MESSAGEix-Nexus build.

Per starter (SSP2, SSP3; ``config.yaml``), on its local HSQLDB platform:

- ``base``: ``mix-models water-ix nexus`` on the starter (RCP 7p0 hydrology,
  no SDG package). The build solves the scenario.
- ``BAU``: clone of ``base`` with the "AU policies": environmental-flow lower
  bounds on ``return_flow`` (:func:`.water_supply.eflow_bound_rows`) and the
  renewable-water growth cap of the SSP on ``extract_surfacewater`` and
  ``extract_groundwater``.
- ``maladapt``: ``BAU`` plus no efficiency improvement in distribution: the
  efficient mode ``Mf`` of every distribution technology is bounded to zero.
- ``BAUfg``: ``BAU`` plus a variable cost on fossil groundwater above the
  levelised cost of membrane desalination, so desalination is used first.
- ``BAUfgd``: ``BAUfg`` with desalination freed: the water module's
  ``historical_new_capacity`` for ``membrane``/``distillation`` ends in 2025
  while the first model year is 2035, so its 10 %/yr growth chain on new
  capacity starts from zero; the 2025 builds are copied to 2030 and the growth
  cap is removed.
- Since 11-02 every constrained scenario also caps fossil groundwater at its
  last historical dispatch (``fossil_gw_cap``: ``bound_activity_up`` = factor ×
  2030 ``historical_activity``, zero where none). ``desal_potential`` swaps the
  desalination potential for another SSP series of the projection (unused).
- ``adapt`` (scenario ``..._W_+adapt``): a second nexus build with the water
  module's SDG package (efficient distribution only, treatment and recycling
  targets, e-flow bounds), then the growth cap.

``mix-models sparccle_water run --config <yaml> "SSP2 BAU" --go`` builds one
scenario; ``scripts/run_sparccle_water.sh`` chains localisation, builds and
solves on UniCC.
"""

import logging
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import yaml
from message_ix import make_df

from message_ix_models.model.water.config import Config as WaterConfig
from message_ix_models.model.water.data.water_supply import (
    eflow_bound_rows,
    read_e_flow,
)
from message_ix_models.model.water.utils import KM3_TO_MCM
from message_ix_models.project.sparccle.workflow import local_platform_name
from message_ix_models.util import ScenarioInfo, broadcast, package_data_path
from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

#: Renewable water extraction technologies carrying the growth cap.
RENEWABLE_EXTRACTION = ["extract_surfacewater", "extract_groundwater"]

#: Scenario keys that change an existing scenario rather than build one.
LEVERS = (
    "eflow",
    "renewable_growth",
    "ban_mode",
    "fossil_gw_var_cost",
    "desal_free",
    "fossil_gw_cap",
    "desal_potential",
)

#: Fossil groundwater backstop technology.
FOSSIL_GW = "extract_gw_fossil"

#: Seawater intake carrying the desalination potential (``bound_total_capacity_up``).
SALINE_INTAKE = "extract_salinewater_basin"

#: Desalination plant technologies (growth-capped by the water module).
DESAL_PLANTS = ["membrane", "distillation"]


def load_config(path: str | Path | None = None) -> dict:
    """Load ``config.yaml`` (packaged copy by default); ``local_db`` is expanded."""
    path = Path(path) if path else Path(__file__).with_name("config.yaml")
    with open(path) as f:
        config = yaml.safe_load(f)
    config["local_db"] = str(Path(config["local_db"]).expanduser())
    for key, spec in config["scenarios"].items():
        if ("build" in spec) == ("from" in spec):
            raise ValueError(f"scenario {key}: exactly one of build/from required")
    return config


def scenario_name(config: dict, ssp: str, key: str) -> str:
    """Name of scenario `key` of starter `ssp` on the local platform.

    The scenario's ``label`` (default: `key`) fills ``{key}`` in ``config["name"]``;
    step names stay regex-safe while the scenario name may carry ``+``.
    """
    label = config["scenarios"][key].get("label", key)
    return config["name"].format(ssp=ssp, key=label)


def platform_of(config: dict, ssp: str) -> str:
    """ixmp platform name of the local copy of starter `ssp`."""
    st = config["starters"][ssp]
    return local_platform_name(st["model"], st["scenario"])


def growth_cap_rows(growth: pd.DataFrame, value: float) -> pd.DataFrame:
    """`growth` (``growth_activity_up``) rows of the renewable extractors at `value`."""
    out = growth[growth["technology"].isin(RENEWABLE_EXTRACTION)].copy()
    out["value"] = value
    return out.reset_index(drop=True)


def mode_ban_rows(output: pd.DataFrame, mode: str, years: list[int]) -> pd.DataFrame:
    """``bound_activity_up = 0`` in `years` per (node, tech) of `output` in `mode`."""
    combos = (
        output.loc[output["mode"] == mode, ["node_loc", "technology"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return make_df(
        "bound_activity_up",
        node_loc=combos["node_loc"],
        technology=combos["technology"],
        mode=mode,
        time="year",
        value=0.0,
        unit="MCM/year",
    ).pipe(broadcast, year_act=years)


def fossil_gw_cost_rows(inp: pd.DataFrame, value: float) -> pd.DataFrame:
    """``var_cost`` rows at `value` for every (node, vintage, year, mode) of `inp`.

    `inp` is the ``input`` of ``extract_gw_fossil``; its index columns give the
    vintage/activity pairs the technology exists for.
    """
    cols = ["node_loc", "technology", "year_vtg", "year_act", "mode", "time"]
    idx = inp[cols].drop_duplicates().reset_index(drop=True)
    return make_df("var_cost", **idx, value=value, unit="USD/MCM")


def desal_seed_rows(hist: pd.DataFrame, from_year: int, to_year: int) -> pd.DataFrame:
    """`hist` (``historical_new_capacity``) rows of `from_year` copied to `to_year`."""
    out = hist[hist["year_vtg"] == from_year].copy()
    out["year_vtg"] = to_year
    return out.reset_index(drop=True)


def fossil_gw_cap_rows(
    hist: pd.DataFrame, inp: pd.DataFrame, years: list[int], factor: float
) -> pd.DataFrame:
    """``bound_activity_up`` on fossil groundwater in `years`.

    The cap is `factor` × the ``historical_activity`` of the last historical year
    in `hist` per (node, mode, time). Every basin of `inp` (the technology's
    ``input``) gets a row; basins without history are capped at zero, so fossil
    groundwater cannot appear where none was pumped.
    """
    last = int(hist["year_act"].max())
    h = hist.loc[hist["year_act"] == last, ["node_loc", "mode", "time", "value"]]
    combos = (
        inp[["node_loc", "technology", "mode", "time"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    df = combos.merge(h, on=["node_loc", "mode", "time"], how="left")
    df["value"] = df["value"].fillna(0.0) * factor
    return make_df("bound_activity_up", **df, unit="MCM/year").pipe(
        broadcast, year_act=years
    )


def desal_potential_rows(
    bound: pd.DataFrame, proj: pd.DataFrame, ssp: str
) -> pd.DataFrame:
    """`bound` (``bound_total_capacity_up`` of the seawater intake) revalued.

    `proj` is ``projected_desalination_potential_km3_year_<regions>.csv``
    (``BCU_name, ssp, year, cap_km3_year``); the `ssp` series replaces the
    values, negative potentials become zero and (basin, year) pairs without a
    row get zero, as the water module's backfill does.
    """
    p = proj[proj["ssp"] == ssp]
    if p.empty:
        raise ValueError(f"no {ssp!r} rows in the desalination projection")
    p = pd.DataFrame(
        {
            "node_loc": "B" + p["BCU_name"].astype(str),
            "year_act": p["year"].astype(int),
            "new": p["cap_km3_year"].clip(lower=0) * KM3_TO_MCM,
        }
    )
    out = bound.merge(p, on=["node_loc", "year_act"], how="left")
    out["value"] = out["new"].fillna(0.0)
    return out.drop(columns="new")


def scenario_status(platform: str, model: str, scenario: str) -> str:
    """``"missing"``, ``"unsolved"`` or ``"solved"`` for the default version."""
    import ixmp
    from message_ix import Scenario

    mp = ixmp.Platform(name=platform)
    try:
        existing = mp.scenario_list(default=True)
        hit = (existing["model"] == model) & (existing["scenario"] == scenario)
        if not hit.any():
            return "missing"
        return "solved" if Scenario(mp, model, scenario).has_solution() else "unsolved"
    finally:
        mp.close_db()


def basins_of(scenario: "Scenario") -> set[str]:
    """Basin names (``BCU_name``) of the water nodes ``B<name>`` in `scenario`."""
    return {n[1:] for n in scenario.set("node") if n.startswith("B")}


def water_context(  # pragma: no cover
    context: "Context", scenario: "Scenario", *, regions: str, rcps: str, rels: str
) -> "Context":
    """`context` set up so the water data functions read `scenario`'s inputs."""
    context.regions = regions
    cfg = WaterConfig.from_context(context)
    cfg.RCP, cfg.REL, cfg.time = rcps, rels, ["year"]
    cfg.valid_basins = basins_of(scenario)
    context["water build info"] = ScenarioInfo(scenario)
    return context


def build_nexus(  # pragma: no cover
    context: "Context",
    scenario: "Scenario",
    *,
    ssp: str,
    regions: str,
    rcps: str,
    rels: str,
    sdgs: str,
    new_name: str,
) -> "Scenario":
    """Build (and solve) the nexus scenario `new_name` from *scenario* via ``water-ix``.

    The database is closed around the subprocess: HSQLDB admits one process.
    """
    import ixmp
    from message_ix import Scenario

    platform_name, model = scenario.platform.name, scenario.model
    url = f"ixmp://{platform_name}/{model}/{scenario.scenario}"
    cmd = [
        "mix-models", "--url", url,
        "water-ix", "--regions", regions, "--ssp", ssp, "--output-scenario", new_name,
        "nexus", "--rcps", rcps, "--rels", rels, "--sdgs", sdgs,
    ]  # fmt: skip
    scenario.platform.close_db()
    del scenario
    log.info("→ %s", " ".join(cmd))
    subprocess.run(cmd, check=True)

    mp = ixmp.Platform(name=platform_name)
    context.core._mp = mp
    return Scenario(mp, model, new_name)


class Changes:
    """Parameter rows to add, rows to remove and log messages, gathered by levers."""

    def __init__(self) -> None:
        self.rows: dict[str, pd.DataFrame] = {}
        self.removals: dict[str, pd.DataFrame] = {}
        self.messages: list[str] = []

    def __bool__(self) -> bool:
        return bool(self.rows or self.removals)

    def add(self, par: str, df: pd.DataFrame) -> None:
        """Append `df` to the rows of `par` (several levers may write one parameter)."""
        prev = self.rows.get(par)
        self.rows[par] = (
            df if prev is None else pd.concat([prev, df], ignore_index=True)
        )


def _lever_eflow(  # pragma: no cover
    ch: Changes, context: "Context", scenario: "Scenario", config: dict
) -> None:
    ctx = water_context(context, scenario, regions=config["regions"], **config["nexus"])
    dmd_df, df_env = read_e_flow(ctx)
    df = eflow_bound_rows(df_env, dmd_df)
    ch.rows["bound_activity_lo"] = df
    ch.messages.append(f"e-flow lower bounds on return_flow ({len(df)} rows)")


def _lever_growth(
    ch: Changes, scenario: "Scenario", value: float
) -> None:  # pragma: no cover
    current = scenario.par(
        "growth_activity_up", filters={"technology": RENEWABLE_EXTRACTION}
    )
    if current.empty:
        raise RuntimeError(
            f"{scenario.url}: no growth_activity_up on {RENEWABLE_EXTRACTION}"
        )
    if (current["value"] == value).all():
        ch.messages.append(f"renewable growth already {value}")
    else:
        ch.rows["growth_activity_up"] = growth_cap_rows(current, value)
        ch.messages.append(f"renewable growth {value} ({len(current)} rows)")


def _lever_fossil(
    ch: Changes, scenario: "Scenario", cost: float
) -> None:  # pragma: no cover
    inp = scenario.par("input", filters={"technology": ["extract_gw_fossil"]})
    df = fossil_gw_cost_rows(inp, cost)
    ch.rows["var_cost"] = df
    ch.messages.append(f"extract_gw_fossil var_cost {cost} USD/MCM ({len(df)} rows)")


def _lever_desal(
    ch: Changes, scenario: "Scenario", info: ScenarioInfo
) -> None:  # pragma: no cover
    # The module's historical_new_capacity ends in 2025 while the first model
    # year is 2035, so the 10 %/yr growth chain on new desalination capacity
    # starts from zero and no plant can ever be built. Seed the period before
    # the first model year from the last historical one and drop the cap.
    hist = scenario.par("historical_new_capacity", filters={"technology": DESAL_PLANTS})
    last = int(hist["year_vtg"].max())
    seed_year = max(y for y in info.set["year"] if y < info.y0)
    n_seed = 0
    if seed_year > last:
        ch.rows["historical_new_capacity"] = desal_seed_rows(hist, last, seed_year)
        n_seed = len(ch.rows["historical_new_capacity"])
    growth = scenario.par(
        "growth_new_capacity_up", filters={"technology": DESAL_PLANTS}
    )
    ch.removals["growth_new_capacity_up"] = growth
    ch.messages.append(
        f"desalination freed: {last}->{seed_year} historical builds copied"
        f" ({n_seed} rows), growth_new_capacity_up removed ({len(growth)} rows)"
    )


def _lever_ban(
    ch: Changes, scenario: "Scenario", mode: str, years: list[int]
) -> None:  # pragma: no cover
    output = scenario.par("output", filters={"mode": [mode]})
    df = mode_ban_rows(output, mode, years)
    ch.add("bound_activity_up", df)
    techs = sorted(df["technology"].unique())
    ch.messages.append(f"mode {mode} banned from {years[0]}: {', '.join(techs)}")


def _lever_fossil_cap(
    ch: Changes, scenario: "Scenario", years: list[int], factor: float
) -> None:  # pragma: no cover
    hist = scenario.par("historical_activity", filters={"technology": [FOSSIL_GW]})
    if hist.empty:
        raise RuntimeError(f"{scenario.url}: no historical_activity on {FOSSIL_GW}")
    inp = scenario.par("input", filters={"technology": [FOSSIL_GW]})
    df = fossil_gw_cap_rows(hist, inp, years, factor)
    ch.add("bound_activity_up", df)
    ch.messages.append(
        f"{FOSSIL_GW} capped at {factor} x {int(hist['year_act'].max())} activity"
        f" from {years[0]} ({len(df)} rows, {(df['value'] == 0).sum()} at zero)"
    )


def _lever_desal_potential(
    ch: Changes, context: "Context", scenario: "Scenario", ssp: str
) -> None:  # pragma: no cover
    bound = scenario.par(
        "bound_total_capacity_up", filters={"technology": [SALINE_INTAKE]}
    )
    if bound.empty:
        raise RuntimeError(
            f"{scenario.url}: no bound_total_capacity_up on {SALINE_INTAKE}"
        )
    proj = pd.read_csv(
        package_data_path(
            "water",
            "infrastructure",
            f"projected_desalination_potential_km3_year_{context.regions}.csv",
        )
    )
    df = desal_potential_rows(bound, proj, ssp)
    ch.rows["bound_total_capacity_up"] = df
    world = df.groupby("year_act")["value"].sum()
    ch.messages.append(
        f"desalination potential from the {ssp} projection ({len(df)} rows;"
        f" World 2050 {world.get(2050, float('nan')):,.0f} MCM/yr,"
        f" was {bound.loc[bound['year_act'] == 2050, 'value'].sum():,.0f})"
    )


def _collect(  # pragma: no cover
    context: "Context", scenario: "Scenario", *, config: dict, key: str, ssp: str
) -> Changes:
    """Gather the parameter changes of every lever set on scenario `key`."""
    spec = config["scenarios"][key]
    starter = config["starters"][ssp]
    info = ScenarioInfo(scenario)
    years = [y for y in info.Y if y >= spec.get("full_from", info.y0)]
    ch = Changes()

    if spec.get("eflow"):
        _lever_eflow(ch, context, scenario, config)
    if spec.get("renewable_growth"):
        _lever_growth(ch, scenario, float(starter["renewable_growth"]))
    if spec.get("fossil_gw_var_cost") is not None:
        _lever_fossil(ch, scenario, float(spec["fossil_gw_var_cost"]))
    if spec.get("desal_free"):
        _lever_desal(ch, scenario, info)
    if spec.get("ban_mode"):
        _lever_ban(ch, scenario, spec["ban_mode"], years)
    if spec.get("fossil_gw_cap") is not None:
        _lever_fossil_cap(ch, scenario, years, float(spec["fossil_gw_cap"]))
    if spec.get("desal_potential"):
        _lever_desal_potential(ch, context, scenario, str(spec["desal_potential"]))
    return ch


def apply(  # pragma: no cover
    context: "Context", scenario: "Scenario", *, config: dict, key: str, ssp: str
) -> None:
    """Apply the levers of scenario `key` in place; drops the solution on change."""
    ch = _collect(context, scenario, config=config, key=key, ssp=ssp)

    message = f"sparccle_water {ssp} {key}: " + "; ".join(ch.messages)
    if not ch:
        log.info("%s — nothing to change", message)
        return
    if scenario.has_solution():
        scenario.remove_solution()
    scenario.check_out()
    for par, df in ch.removals.items():
        if not df.empty:
            scenario.remove_par(par, df)
    for par, df in ch.rows.items():
        scenario.add_par(par, df)
    log.info(message)
    scenario.commit(message)


def generate(
    context: "Context", *, config_path: str | Path | None = None, **options
) -> Workflow:
    """Build the workflow: per starter, ``<SSP> starter`` plus one step per scenario."""
    del options
    config = load_config(config_path)
    context.regions = config["regions"]
    wf = Workflow(context)

    targets = []
    for ssp, st in config["starters"].items():
        platform, model = platform_of(config, ssp), st["model"]
        starter_step = f"{ssp} starter"
        wf.add_step(
            starter_step, None, target=f"ixmp://{platform}/{model}/{st['scenario']}"
        )

        for key, spec in config["scenarios"].items():
            step = f"{ssp} {key}"
            target = f"ixmp://{platform}/{model}/{scenario_name(config, ssp, key)}"
            if spec.get("build") == "nexus":
                # "<SSP> <key>" must be the last step of the key: the job script
                # runs exactly that target, so a build with levers is split into
                # "<SSP> <key> build" and the in-place "<SSP> <key>" constrain step.
                has_levers = any(spec.get(lever) for lever in LEVERS)
                build_step = f"{step} build" if has_levers else step
                wf.add_step(
                    build_step,
                    starter_step,
                    build_nexus,
                    target=target,
                    ssp=ssp,
                    regions=config["regions"],
                    sdgs=spec.get("sdgs", "baseline"),
                    new_name=scenario_name(config, ssp, key),
                    **config["nexus"],
                )
                if has_levers:
                    # constrain the built scenario in place; the caller re-solves
                    wf.add_step(
                        step,
                        build_step,
                        apply,
                        target=target,
                        config=config,
                        key=key,
                        ssp=ssp,
                    )
            else:
                wf.add_step(
                    step,
                    f"{ssp} {spec['from']}",
                    apply,
                    target=target,
                    clone=True,
                    set_as_default=True,
                    config=config,
                    key=key,
                    ssp=ssp,
                )
            targets.append(step)

    wf.add("all", targets)
    wf.default_key = "all"
    return wf
