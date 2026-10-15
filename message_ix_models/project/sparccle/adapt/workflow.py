"""SPARCCLE adaptation scenarios on the cooling + power-plant CID scenario.

Every scenario is a clone of ``base`` (the solved ``_cooling_CI_p`` scenario)
on the local HSQLDB platform named in ``config.yaml``:

- ``BAU``: the water module's SSP cooling-share constraints switched on. The
  scenario already holds ``share_commodity_up`` for ``share_cooling_ot_fresh``
  and ``share_cooling_air`` (SSP2) and the ``shares``/``type_tec`` elements,
  but :func:`.water.build.get_spec` leaves ``cat_tec`` and the
  ``map_shares_commodity_*`` mappings out behind a FIXME, so the constraints are
  inert. This step adds the mappings for the four ``share_cooling_*`` shares.
- ``malad35``: no new once-through cooling capacity in ``regulation_nodes``.
- ``adapt35`` / ``adapt35h40``: no new once-through cooling anywhere, no new
  coal, gas or oil power-plant capacity, near-zero activity growth for biomass
  and nuclear power. ``h40`` phases in: half effect in 2035, full from 2040.
  The base's activity-decline floors (``growth_activity_lo`` /
  ``initial_activity_lo``, e.g. -5 %/yr on ``gas_cc`` and ``coal_ppl``) are
  removed for the banned plants: with no new capacity the fleet retires and
  activity has to reach zero, and the floor chains back to the first model
  year, where it contradicts historical activity (CPLEX presolve:
  ``ACTIVITY_CONSTRAINT_LO(R12_LAM,gas_cc,2035,year): 0 >= 28.1``).

"Half effect" in a year means ``bound_new_capacity_up`` at half of the base
solution's ``CAP_NEW`` for that year, and growth rates halved (half of
``growth.ref_default`` where the base has no growth constraint).

``mix-models sparccle_adapt run --config <yaml> KEY --go`` builds one scenario;
solving is left to the caller (``scripts/run_sparccle_adapt.sh`` next to this module).
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import yaml
from message_ix import make_df

from message_ix_models.util import ScenarioInfo
from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yaml"

#: Cooling types of the water module; each ``<parent>__<type>`` technology
#: outputs commodity ``<type>`` at level ``share``.
COOLING_TYPES = ("ot_fresh", "cl_fresh", "ot_saline", "air")

#: Fraction of the base solution's ``CAP_NEW`` allowed in a half-effect year.
HALF_FRACTION = 0.5


def load_config(path: str | Path | None = None) -> dict:
    """Load and validate the project config."""
    with open(path or _DEFAULT_CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    required = [
        "platform",
        "model",
        "base",
        "regulation_nodes",
        "technologies",
        "growth",
        "scenarios",
    ]
    if missing := [key for key in required if key not in config]:
        raise ValueError(f"Config missing required keys: {missing}")

    for key, spec in config["scenarios"].items():
        spec.setdefault("suffix", f"_{key}")
        half, full = spec.get("half_in"), spec.get("full_from")
        if half is not None and (full is None or half >= full):
            raise ValueError(f"{key}: half_in ({half}) must precede full_from ({full})")
    return config


def cooling_share_sets(output: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Set elements that make the water module's ``share_cooling_*`` shares live.

    `output` is the scenario's ``output`` parameter at ``level="share"``, where
    each cooling technology outputs its own cooling type as a commodity. Mirrors
    the mappings :mod:`.water.build` constructs and then discards.
    """
    cool = output.loc[
        output["commodity"].isin(COOLING_TYPES),
        ["node_loc", "technology", "commodity"],
    ].drop_duplicates()
    if cool.empty:
        raise ValueError("No cooling technologies with output at level 'share'")
    techs = cool[["technology", "commodity"]].drop_duplicates()
    nodes = sorted(cool["node_loc"].unique())

    cat_tec = pd.concat(
        [
            pd.DataFrame(
                {
                    "type_tec": "share_cooling_" + techs["commodity"] + "_share",
                    "technology": techs["technology"],
                }
            ),
            pd.DataFrame(
                [
                    {"type_tec": f"share_cooling_{c}_tot", "technology": t}
                    for c in COOLING_TYPES
                    for t in techs["technology"]
                ]
            ),
        ],
        ignore_index=True,
    )
    share = pd.DataFrame(
        [
            dict(
                shares=f"share_cooling_{c}",
                node_share=n,
                node=n,
                type_tec=f"share_cooling_{c}_share",
                mode="M1",
                commodity=c,
                level="share",
            )
            for c in COOLING_TYPES
            for n in nodes
        ]
    )
    total = pd.DataFrame(
        [
            dict(
                shares=f"share_cooling_{c}",
                node_share=n,
                node=n,
                type_tec=f"share_cooling_{c}_tot",
                mode="M1",
                commodity=c_all,
                level="share",
            )
            for c in COOLING_TYPES
            for n in nodes
            for c_all in COOLING_TYPES
        ]
    )
    return {
        "cat_tec": cat_tec,
        "map_shares_commodity_share": share,
        "map_shares_commodity_total": total,
    }


def no_new_capacity(
    combos: pd.DataFrame,
    years_full: list[int],
    half_year: int | None = None,
    ref_cap_new: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """``bound_new_capacity_up`` rows: 0 in `years_full`, half the base in `half_year`.

    `combos` has columns ``node_loc`` and ``technology``. `ref_cap_new` is the
    base solution's ``CAP_NEW`` (columns ``node_loc, technology, year_vtg, lvl``);
    combos absent from it had no new capacity, so their half bound is 0.
    """
    combos = combos.reset_index(drop=True)
    rows = combos.merge(pd.DataFrame({"year_vtg": years_full}), how="cross").assign(
        value=0.0
    )
    if half_year is not None:
        if ref_cap_new is None:
            raise ValueError("half_year requires ref_cap_new")
        ref = ref_cap_new.loc[
            ref_cap_new["year_vtg"] == half_year, ["node_loc", "technology", "lvl"]
        ]
        half = combos.assign(year_vtg=half_year).merge(ref, how="left")
        half["value"] = HALF_FRACTION * half.pop("lvl").fillna(0.0)
        rows = pd.concat([half, rows], ignore_index=True)
    return make_df(
        "bound_new_capacity_up",
        node_loc=rows["node_loc"],
        technology=rows["technology"],
        year_vtg=rows["year_vtg"],
        value=rows["value"],
        unit="GW",
    )


def near_zero_growth(
    combos: pd.DataFrame,
    years_full: list[int],
    near_zero: float,
    half_year: int | None = None,
    ref_growth: pd.DataFrame | None = None,
    ref_initial: pd.DataFrame | None = None,
    ref_default: float = 0.05,
) -> dict[str, pd.DataFrame]:
    """``growth_activity_up`` / ``initial_activity_up`` rows freezing activity.

    In `years_full` growth is `near_zero` and the seed ``initial_activity_up`` is
    0, for every combo. In `half_year` growth is half of the existing value
    (`ref_growth`, columns ``node_loc, technology, year_act, value``) or half of
    `ref_default` where none exists, and the seed is halved. The soft
    relaxation (``soft_activity_up``) of the base is left untouched.
    """
    combos = combos.reset_index(drop=True)
    years = pd.DataFrame({"year_act": years_full})
    growth = combos.merge(years, how="cross").assign(value=near_zero)
    initial = combos.merge(years, how="cross").assign(value=0.0)

    if half_year is not None:
        key = ["node_loc", "technology"]

        def _ref(df: pd.DataFrame | None, default: float) -> pd.Series:
            if df is None:
                return pd.Series(default, index=combos.index)
            ref = df.loc[df["year_act"] == half_year, key + ["value"]]
            return combos.merge(ref, how="left")["value"].fillna(default)

        growth = pd.concat(
            [
                combos.assign(
                    year_act=half_year,
                    value=HALF_FRACTION * _ref(ref_growth, ref_default),
                ),
                growth,
            ],
            ignore_index=True,
        )
        initial = pd.concat(
            [
                combos.assign(
                    year_act=half_year, value=HALF_FRACTION * _ref(ref_initial, 0.0)
                ),
                initial,
            ],
            ignore_index=True,
        )

    def _df(name: str, rows: pd.DataFrame, unit: str) -> pd.DataFrame:
        return make_df(
            name,
            node_loc=rows["node_loc"],
            technology=rows["technology"],
            year_act=rows["year_act"],
            time="year",
            value=rows["value"],
            unit=unit,
        )

    return {
        "growth_activity_up": _df("growth_activity_up", growth, "-"),
        "initial_activity_up": _df("initial_activity_up", initial, "GWa"),
    }


#: Parameters that put a floor under declining activity.
DECLINE_FLOOR_PARS = ("growth_activity_lo", "initial_activity_lo")


def decline_floor_rows(
    par: pd.DataFrame, techs: list[str], years: list[int]
) -> pd.DataFrame:
    """Rows of a decline-floor parameter for `techs` in `years`, to be removed.

    `par` is ``growth_activity_lo`` or ``initial_activity_lo`` as returned by
    :meth:`.Scenario.par`, with a ``year_act`` column.
    """
    mask = par["technology"].isin(techs) & par["year_act"].isin(years)
    return par.loc[mask].reset_index(drop=True)


def tech_nodes(
    scenario: "Scenario", techs: list[str], nodes: list[str] | None = None
) -> pd.DataFrame:
    """``node_loc``/``technology`` pairs for which `techs` exist in `scenario`."""
    filters: dict = {"technology": techs}
    if nodes:
        filters["node_loc"] = nodes
    lifetime = scenario.par("technical_lifetime", filters=filters)
    return lifetime[["node_loc", "technology"]].drop_duplicates().reset_index(drop=True)


def once_through_techs(scenario: "Scenario", suffixes: list[str]) -> list[str]:
    """Cooling technologies of the once-through types in `suffixes`."""
    ends = tuple(f"__{s}" for s in suffixes)
    return sorted(t for t in scenario.set("technology") if t.endswith(ends))


def _apply_shares(scenario: "Scenario") -> str:  # pragma: no cover
    """Add the set elements that activate the SSP cooling shares."""
    sets = cooling_share_sets(scenario.par("output", filters={"level": "share"}))
    shares = sorted(set(sets["map_shares_commodity_share"]["shares"]))
    if missing := set(shares) - set(scenario.set("shares")):
        raise ValueError(f"shares set lacks {sorted(missing)}")
    for name, df in sets.items():
        scenario.add_set(name, df)
    n = len(scenario.par("share_commodity_up", filters={"shares": shares}))
    return f"SSP cooling shares active ({n} share_commodity_up rows)"


def _apply_no_new_capacity(  # pragma: no cover
    scenario: "Scenario",
    techs: list[str],
    nodes: list[str] | None,
    years_full: list[int],
    half_year: int | None,
    base: "Scenario | None",
    label: str,
) -> str:
    """Bound new capacity of `techs` (in `nodes`, or everywhere) to zero."""
    combos = tech_nodes(scenario, techs, nodes)
    ref = (
        base.var("CAP_NEW", filters={"technology": techs, "year_vtg": [half_year]})
        if base is not None
        else None
    )
    scenario.add_par(
        "bound_new_capacity_up", no_new_capacity(combos, years_full, half_year, ref)
    )
    return (
        f"no new capacity for {label}: {len(combos)} node/tech pairs"
        f" from {years_full[0]}" + (f", half in {half_year}" if half_year else "")
    )


def _apply_growth(  # pragma: no cover
    scenario: "Scenario",
    techs: list[str],
    years_full: list[int],
    half_year: int | None,
    growth_cfg: dict,
) -> str:
    """Freeze activity growth of `techs`."""
    combos = tech_nodes(scenario, techs)
    kw: dict = {}
    if half_year is not None:
        f = {"technology": techs, "year_act": [half_year]}
        kw = dict(
            ref_growth=scenario.par("growth_activity_up", filters=f),
            ref_initial=scenario.par("initial_activity_up", filters=f),
            ref_default=growth_cfg["ref_default"],
        )
    for name, df in near_zero_growth(
        combos, years_full, growth_cfg["near_zero"], half_year, **kw
    ).items():
        scenario.add_par(name, df)
    return (
        f"near-zero activity growth ({growth_cfg['near_zero']}) for"
        f" {', '.join(techs)}: {len(combos)} node/tech pairs"
    )


def _apply_drop_decline_floor(  # pragma: no cover
    scenario: "Scenario", techs: list[str], years: list[int]
) -> str:
    """Remove the decline floors of `techs` in `years`."""
    counts = []
    for name in DECLINE_FLOOR_PARS:
        rows = decline_floor_rows(
            scenario.par(name, filters={"technology": techs}), techs, years
        )
        if not rows.empty:
            scenario.remove_par(name, rows)
        counts.append(f"{len(rows)} {name}")
    return f"decline floors removed from {years[0]}: " + ", ".join(counts)


def apply(  # pragma: no cover
    context: "Context", scenario: "Scenario", *, config: dict, key: str
) -> None:
    """Apply the constraints of scenario `key` in place on the (unsolved) clone."""
    spec = config["scenarios"][key]
    info = ScenarioInfo(scenario)
    years_full = [y for y in info.Y if y >= spec.get("full_from", info.y0)]
    half_year = spec.get("half_in")
    if half_year is not None and half_year not in info.Y:
        raise ValueError(f"{key}: half_in={half_year} is not a model year")
    tech_cfg = config["technologies"]

    base = None
    if half_year is not None:
        from message_ix import Scenario

        base = Scenario(scenario.platform, scenario.model, config["base"])
        if not base.has_solution():
            raise RuntimeError(f"{base.url} has no solution to take CAP_NEW from")

    scenario.check_out()
    messages: list[str] = []

    if spec.get("ssp_cooling_shares"):
        messages.append(_apply_shares(scenario))

    ban = spec.get("once_through_ban")
    if ban:
        messages.append(
            _apply_no_new_capacity(
                scenario,
                once_through_techs(scenario, tech_cfg["once_through"]),
                config["regulation_nodes"] if ban == "regulation" else None,
                years_full,
                half_year,
                base,
                f"once-through cooling ({ban})",
            )
        )

    if spec.get("thermal_no_new"):
        messages.append(
            _apply_no_new_capacity(
                scenario,
                tech_cfg["thermal_no_new"],
                None,
                years_full,
                half_year,
                base,
                "coal/gas/oil power plants",
            )
        )
        if spec.get("drop_decline_floor"):
            years = ([half_year] if half_year is not None else []) + years_full
            messages.append(
                _apply_drop_decline_floor(scenario, tech_cfg["thermal_no_new"], years)
            )

    if spec.get("near_zero_growth"):
        messages.append(
            _apply_growth(
                scenario,
                tech_cfg["near_zero_growth"],
                years_full,
                half_year,
                config["growth"],
            )
        )

    message = f"sparccle_adapt {key}: " + "; ".join(messages)
    log.info(message)
    scenario.commit(message)


def generate(
    context: "Context", *, config_path: str | Path | None = None, **options
) -> Workflow:
    """Build the workflow: ``base`` plus one clone-and-constrain step per scenario."""
    del options
    config = load_config(config_path)
    platform, model, base = config["platform"], config["model"], config["base"]

    wf = Workflow(context)
    wf.add_step("base", None, target=f"ixmp://{platform}/{model}/{base}")

    targets = []
    for key, spec in config["scenarios"].items():
        targets.append(
            wf.add_step(
                key,
                "base",
                apply,
                target=f"ixmp://{platform}/{model}/{base}{spec['suffix']}",
                clone=True,
                set_as_default=True,
                config=config,
                key=key,
            )
        )

    wf.add("all", targets)
    wf.default_key = "all"
    return wf
