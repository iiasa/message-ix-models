"""SPARCCLE physical-impact workflow.

Builds a :class:`message_ix_models.workflow.Workflow` whose nodes are the
SPARCCLE Phase-1 cooling-module clones and the Phase-2 CID variants
(``CI_b`` / ``CI_p`` / ``CI_bp``) for every starter declared in
``scenario_config.yaml``. ``mix-models sparccle run TARGET`` selects the
subgraph, ``--from`` truncates at a given step, ``--go`` actually runs.

Phase-1 cooling delegates to ``mix-models water-ix cooling`` via
subprocess. The action returns the produced ``{base}_cooling`` scenario
so downstream variants can clone from it. The subprocess avoids a
locked-scenario deadlock that an in-process build runs into; it also solves
the ``_cooling`` clone, which no downstream step uses. Both are a known
limitation until that deadlock is resolved.

Phase-2 CID actions (``apply_buildings``, ``apply_cooling``,
``_apply_buildings_and_cooling``) clone the upstream scenario, load the
MAGICC GSAT ensemble, and call into the domain CID kernels in
:mod:`.buildings.impacts`, :mod:`.buildings.space_split` and
:mod:`.water.data.cooling_impacts`.
"""

import logging
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from genno import KeyExistsError

from message_ix_models.tools.impacts import load_magicc_gmt, persist_gmt_mean
from message_ix_models.util import package_data_path
from message_ix_models.workflow import Workflow

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = package_data_path(
    "..", "project", "sparccle", "scenario_config.yaml"
)


def load_config(path: str | Path) -> dict:
    """Load the SPARCCLE scenario config."""
    with open(path) as f:
        raw = yaml.safe_load(f)

    required = ["platform_info", "starters", "cooling"]
    if missing := [key for key in required if key not in raw]:
        raise ValueError(f"Config missing required keys: {missing}")

    config = dict(raw)
    config["cooling"] = {"rcps": "no_climate", "rels": "low", **raw["cooling"]}
    config["cid"] = {
        "n_runs": None,
        "min_year": None,
        "response_percentile": None,
        "climate_label": None,
        **raw.get("cid", {}),
    }
    config["regions"] = raw.get("regions", "R12")
    return config


def name_token(cid_cfg: dict) -> str:
    """Suffix recording which climate and response percentiles a build used.

    Empty unless the config declares ``cid.climate_label``, so a config that
    does not opt in produces the same target names it always has. When it does,
    the token names both axes: the label for the MAGICC input, and the RIME
    response percentile — ``iE`` for the ensemble mean, ``i90`` for the p90
    conditional response — which otherwise reaches only the commit message and
    leaves two different constructions sharing one scenario name.

    Both halves are abbreviated hard because the whole token has to fit in
    what :data:`MAX_SCENARIO_NAME` leaves after the starter name and the
    ``_cooling_CI_bp`` suffix.
    """
    label = cid_cfg.get("climate_label")
    if not label:
        return ""
    percentile = cid_cfg["response_percentile"]
    return f"_{label}_i{'E' if percentile is None else percentile}"


#: Width of ``SCENARIO.NAME`` in the ixmp JDBC schema before migration V1.31
#: widens it to 1000. Databases at the narrow width refuse an over-long name
#: only when the clone is written.
MAX_SCENARIO_NAME = 64


def validate_names(config: dict, token: str) -> None:
    """Fail fast if any name the graph will write is too long for the database.

    Every CI step clones before its action runs, so a name the backend
    refuses surfaces as a write failure partway through the graph with the
    earlier clones already made. Collect every over-long target and raise
    once.
    """
    suffixes = (
        "_cooling",
        f"_CI_b{token}",
        f"_cooling_CI_p{token}",
        f"_cooling_CI_bp{token}",
    )
    too_long = sorted(
        {
            f"{name} ({len(name)} characters)"
            for starter in config["starters"]
            for name in (starter["scenario"] + suffix for suffix in suffixes)
            if len(name) > MAX_SCENARIO_NAME
        }
    )
    if too_long:
        raise ValueError(
            "SPARCCLE preflight: target scenario names exceed the database "
            f"limit of {MAX_SCENARIO_NAME} characters:\n  - " + "\n  - ".join(too_long)
        )


def _resolve_magicc_dir(
    starter: dict,
    *,
    magicc_root: str | Path | None,
    magicc_file: str | Path | None,
    magicc_model_suffix: str,
    n_starters: int,
) -> str:
    """Resolve a starter's MAGICC output directory.

    Precedence:

    1. ``magicc_file`` — single absolute path; only valid with one starter.
       Sets the directory to the file's parent (the loader globs there).
    2. ``magicc_root`` — composes ``<root>/<model><suffix>/<scenario_stem>/``,
       where ``scenario_stem`` is the starter scenario with any trailing
       ``_PHY`` suffix removed.
    3. The starter's own ``magicc_output_dir`` from the YAML (legacy path).

    Raises if none of the three is available.
    """
    if magicc_file is not None:
        if n_starters != 1:
            raise ValueError(
                "--magicc-file requires exactly one starter in the config; "
                f"got {n_starters}. Use --magicc-root for multi-starter configs."
            )
        return str(Path(magicc_file).resolve().parent)

    if magicc_root is not None:
        model_dir = f"{starter['model']}{magicc_model_suffix}"
        scenario_stem = starter["scenario"].removesuffix("_PHY")
        return str(Path(magicc_root) / model_dir / scenario_stem)

    if starter.get("magicc_output_dir"):
        return starter["magicc_output_dir"]

    tag = f"{starter['model']}/{starter['scenario']}"
    raise ValueError(
        f"Starter {tag} lacks magicc_output_dir and no --magicc-root or "
        "--magicc-file was given."
    )


def _missing_magicc(starters: list[dict]) -> list[str]:
    out: list[str] = []
    for starter in starters:
        tag = f"{starter['model']}/{starter['scenario']}"
        magicc_dir = Path(starter["magicc_output_dir"])
        if not magicc_dir.is_dir():
            out.append(f"{tag}: magicc_output_dir not a directory: {magicc_dir}")
        elif not list(magicc_dir.glob("*_IAMC_climateassessment.xlsx")):
            out.append(f"{tag}: no *_IAMC_climateassessment.xlsx in {magicc_dir}")
    return out


def _missing_buildings(ssps: set[str]) -> list[str]:
    buildings_dir = package_data_path("buildings")
    coeff_dir = buildings_dir / "correction_coefficients"
    paths = [
        buildings_dir / f"theta_{mode}_{ssp}.csv"
        for ssp in sorted(ssps)
        for mode in ("cool", "heat")
    ]
    paths += [buildings_dir / f"rc_sector_fractions_{ssp}.csv" for ssp in sorted(ssps)]
    paths += [
        coeff_dir / f"correction_coefficients_{mode}_SSP2_{sector}.csv"
        for mode in ("cool", "heat")
        for sector in ("comm", "resid")
    ]
    return [f"buildings: {p}" for p in paths if not p.is_file()]


def _missing_rime() -> list[str]:
    from message_ix_models.tools.impacts import impacts_data_path

    rime_dir = impacts_data_path("rime")
    paths = [
        rime_dir / "r12_thermoelectric_gwl.nc",
        rime_dir / "region_EI_cool_gwl_binned.nc",
        rime_dir / "region_EI_heat_gwl_binned.nc",
    ]
    return [f"RIME: {p}" for p in paths if not p.is_file()]


def validate_inputs(config: dict) -> None:
    """Fail fast if any on-disk input the CID actions need is missing.

    Each ``CI_*`` step clones a scenario before its action runs, so a
    missing MAGICC output or RIME dataset surfaces only after a fresh
    DB clone has been written. This preflight collects every missing
    path across all starters and raises once, so the user can fix them
    in a single pass rather than one clone at a time.
    """
    missing = (
        _missing_magicc(config["starters"])
        + _missing_buildings({s["ssp"] for s in config["starters"]})
        + _missing_rime()
    )
    if missing:
        raise FileNotFoundError(
            "SPARCCLE preflight: required inputs not found:\n  - "
            + "\n  - ".join(missing)
        )


def phase1_cooling(  # pragma: no cover
    context: "Context",
    scenario: "Scenario",
    *,
    ssp: str,
    regions: str = "R12",
    rcps: str = "no_climate",
    rels: str = "low",
) -> "Scenario":
    """Build the cooling module on top of *scenario* via ``water-ix`` subprocess.

    The subprocess reads ``scenario`` and produces ``{scenario}_cooling``
    in the same database. We then load and return it.
    """
    from message_ix import Scenario

    del context

    url = f"ixmp://{scenario.platform.name}/{scenario.model}/{scenario.scenario}"
    cmd = [
        "mix-models",
        "--url",
        url,
        "water-ix",
        "cooling",
        "--regions",
        regions,
        "--ssp",
        ssp,
        "--rcps",
        rcps,
        "--rels",
        rels,
    ]
    log.info("→ %s", " ".join(cmd))
    subprocess.run(cmd, check=True)

    target_name = f"{scenario.scenario}_cooling"
    log.info("Loading produced scenario %s/%s", scenario.model, target_name)
    return Scenario(scenario.platform, scenario.model, target_name)


def apply_buildings(  # pragma: no cover
    context: "Context",
    scenario: "Scenario",
    *,
    magicc_dir: str | Path,
    n_runs: int | None = None,
    reference_scenario: str = "SSP2",
    response_percentile: int | str | None = None,
) -> None:
    """Apply building-energy CIDs in place on *scenario* (``CI_b`` step).

    Space conditioning is first carved out of the MACRO sectors ``rc_spec`` and
    ``rc_therm`` onto the non-MACRO commodities ``space_cooling`` and
    ``space_heating`` (:func:`.apply_space_demand_split`). The RIME-driven
    demand is then written onto those commodities (:func:`.apply_space_cids`),
    so MACRO cannot trade the climate increment away against energy prices.

    *response_percentile* selects a conditional response percentile within each
    RIME GWL window (e.g. ``90``); ``None`` uses the ensemble-mean surface.

    Raises
    ------
    RuntimeError
        If the split fails its pre-solve verification.
    """
    from message_ix_models.model.buildings.impacts import compute_building_cids
    from message_ix_models.model.buildings.space_split import (
        apply_space_cids,
        apply_space_demand_split,
    )
    from message_ix_models.util import ScenarioInfo

    split = apply_space_demand_split(scenario, reference_scenario=reference_scenario)
    if not split["verification"]["passed"]:
        raise RuntimeError(
            f"Space split failed verification: {split['verification']['checks']}"
        )

    gmt = load_magicc_gmt(magicc_dir, n_runs=n_runs)
    n = gmt.values.shape[0] if gmt.values.ndim > 1 else 1
    cooling, heating = compute_building_cids(
        gmt,
        ScenarioInfo(scenario).Y,
        reference_scenario=reference_scenario,
        regions=context.model.regions,
        response_percentile=response_percentile,
    )
    pct = "mean" if response_percentile is None else f"p{response_percentile}"
    apply_space_cids(
        scenario,
        cooling,
        heating,
        commit_message=f"Building CIDs ({reference_scenario}, {n} runs, {pct})",
    )
    persist_gmt_mean(scenario, gmt)


def apply_cooling(  # pragma: no cover
    context: "Context",
    scenario: "Scenario",
    *,
    magicc_dir: str | Path,
    n_runs: int | None = None,
    min_year: int | None = None,
    response_percentile: int | str | None = None,
) -> None:
    """Apply wet + dry cooling CIDs in place on *scenario* (``CI_p`` step).

    *min_year* defaults to
    :data:`model.water.data.cooling_impacts._DEFAULT_MIN_YEAR` when ``None``.
    *response_percentile* selects a conditional response percentile within each
    RIME GWL window (e.g. ``90``); ``None`` uses the ensemble-mean surface.
    """
    from message_ix_models.model.water.data.cooling_impacts import apply_cooling_cids

    gmt = load_magicc_gmt(magicc_dir, n_runs=n_runs)
    n = gmt.values.shape[0] if gmt.values.ndim > 1 else 1
    kwargs = {"min_year": min_year} if min_year is not None else {}
    pct = "mean" if response_percentile is None else f"p{response_percentile}"
    apply_cooling_cids(
        scenario,
        gmt,
        commit_message=f"Cooling CIDs (wet+dry), {n} runs, {pct}",
        regions=context.model.regions,
        response_percentile=response_percentile,
        **kwargs,
    )
    persist_gmt_mean(scenario, gmt)


def _apply_buildings_and_cooling(  # pragma: no cover
    context: "Context",
    scenario: "Scenario",
    *,
    magicc_dir: str | Path,
    n_runs: int | None = None,
    reference_scenario: str = "SSP2",
    min_year: int | None = None,
    response_percentile: int | str | None = None,
) -> None:
    """``CI_bp`` composite: buildings then cooling CIDs in place."""
    apply_buildings(
        context,
        scenario,
        magicc_dir=magicc_dir,
        n_runs=n_runs,
        reference_scenario=reference_scenario,
        response_percentile=response_percentile,
    )
    apply_cooling(
        context,
        scenario,
        magicc_dir=magicc_dir,
        n_runs=n_runs,
        min_year=min_year,
        response_percentile=response_percentile,
    )


def generate(
    context: "Context",
    *,
    config_path: str | Path | None = None,
    magicc_root: str | Path | None = None,
    magicc_file: str | Path | None = None,
    magicc_model_suffix: str = "",
    **options,
) -> Workflow:
    """Build the SPARCCLE Phase-1 + Phase-2 workflow.

    Parameters
    ----------
    context
        Context object (passed to each step).
    config_path
        Path to ``scenario_config.yaml``. Defaults to the packaged copy.
    magicc_root, magicc_file, magicc_model_suffix
        MAGICC location overrides; see :func:`_resolve_magicc_dir` for the
        precedence rules. When neither ``magicc_root`` nor ``magicc_file``
        is given, the per-starter ``magicc_output_dir`` from the YAML is
        used.
    options
        Reserved for forward-compatibility with extra CLI options;
        currently ignored.

    Returns
    -------
    Workflow
        Graph with one ``base``/``cooling``/``CI_b``/``CI_p``/``CI_bp``
        step set per starter, plus an aggregating ``all CI`` default.
    """
    del options

    config = load_config(config_path or _DEFAULT_CONFIG_PATH)

    n_starters = len(config["starters"])
    for starter in config["starters"]:
        starter["magicc_output_dir"] = _resolve_magicc_dir(
            starter,
            magicc_root=magicc_root,
            magicc_file=magicc_file,
            magicc_model_suffix=magicc_model_suffix,
            n_starters=n_starters,
        )

    validate_inputs(config)

    wf = Workflow(context)

    platform = config["platform_info"]["name"]
    regions = config["regions"]
    # apply_buildings and apply_cooling read context.model.regions, which
    # defaults to "R14"; bind it here so Phase-2 kernels match the yaml.
    context.model.regions = regions
    cooling_cfg = config["cooling"]
    cid_cfg = config["cid"]
    token = name_token(cid_cfg)
    validate_names(config, token)

    targets: list[str] = []
    for starter in config["starters"]:
        model = starter["model"]
        scenario = starter["scenario"]
        ssp = starter["ssp"]
        magicc_dir = starter["magicc_output_dir"]

        label = f"{ssp}/{scenario}"
        base_step = f"{label} base"
        try:
            wf.add_step(
                base_step,
                None,
                target=f"ixmp://{platform}/{model}/{scenario}",
            )
        except KeyExistsError:
            pass  # shared starter URL across SSPs

        cooling_step = f"{label} cooling"
        wf.add_step(
            cooling_step,
            base_step,
            phase1_cooling,
            target=f"ixmp://{platform}/{model}/{scenario}_cooling",
            ssp=ssp,
            regions=regions,
            rcps=cooling_cfg["rcps"],
            rels=cooling_cfg["rels"],
        )

        common = {"magicc_dir": magicc_dir}
        if cid_cfg["n_runs"] is not None:
            common["n_runs"] = cid_cfg["n_runs"]
        if cid_cfg["response_percentile"] is not None:
            common["response_percentile"] = cid_cfg["response_percentile"]

        buildings = {
            **common,
            "reference_scenario": ssp,
        }
        cooling = dict(common)
        if cid_cfg["min_year"] is not None:
            cooling["min_year"] = cid_cfg["min_year"]

        targets.append(
            wf.add_step(
                f"{label} CI_b",
                base_step,
                apply_buildings,
                target=f"ixmp://{platform}/{model}/{scenario}_CI_b{token}",
                clone=True,
                set_as_default=True,
                **buildings,
            )
        )
        targets.append(
            wf.add_step(
                f"{label} CI_p",
                cooling_step,
                apply_cooling,
                target=f"ixmp://{platform}/{model}/{scenario}_cooling_CI_p{token}",
                clone=True,
                set_as_default=True,
                **cooling,
            )
        )
        targets.append(
            wf.add_step(
                f"{label} CI_bp",
                cooling_step,
                _apply_buildings_and_cooling,
                target=f"ixmp://{platform}/{model}/{scenario}_cooling_CI_bp{token}",
                clone=True,
                set_as_default=True,
                **buildings,
                **(
                    {"min_year": cid_cfg["min_year"]}
                    if cid_cfg["min_year"] is not None
                    else {}
                ),
            )
        )

    wf.add("all CI", targets)
    wf.default_key = "all CI"

    return wf
