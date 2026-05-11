"""SPARCCLE physical-impact workflow.

Builds a :class:`message_ix_models.workflow.Workflow` whose nodes are the
SPARCCLE Phase-1 cooling-module clones and the Phase-2 CID variants
(``CI_b`` / ``CI_p`` / ``CI_bp``) for every starter declared in
``scenario_config.yaml``. ``mix-models sparccle run TARGET`` selects the
subgraph, ``--from`` truncates at a given step, ``--go`` actually runs.

Phase-1 cooling delegates to ``mix-models water-ix cooling`` via
subprocess. The action returns the produced ``{base}_cooling`` scenario
so downstream variants can clone from it.

Phase-2 CID actions (``apply_buildings``, ``apply_cooling``,
``_apply_buildings_and_cooling``) clone the upstream scenario, load the
MAGICC GSAT ensemble, and call into the domain CID kernels in
:mod:`model.buildings.impacts` and :mod:`model.water.data.cooling_impacts`.
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
    config["cid"] = {"n_runs": None, "min_year": None, **raw.get("cid", {})}
    config["regions"] = raw.get("regions", "R12")
    return config


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
    from message_ix_models.util import package_data_path

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


def phase1_cooling(
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


def apply_buildings(
    context: "Context",
    scenario: "Scenario",
    *,
    magicc_dir: str | Path,
    n_runs: int | None = None,
    reference_scenario: str = "SSP2",
) -> None:
    """Apply building-energy CIDs in place on *scenario* (``CI_b`` step)."""
    from message_ix_models.model.buildings.impacts import (
        apply_building_cids,
        compute_building_cids,
    )
    from message_ix_models.util import ScenarioInfo

    gmt = load_magicc_gmt(magicc_dir, n_runs=n_runs)
    n = gmt.values.shape[0] if gmt.values.ndim > 1 else 1
    cooling, heating = compute_building_cids(
        gmt,
        ScenarioInfo(scenario).Y,
        reference_scenario=reference_scenario,
        regions=context.model.regions,
    )
    apply_building_cids(
        scenario,
        cooling,
        heating,
        commit_message=f"Building CIDs ({reference_scenario}, {n} runs)",
        reference_scenario=reference_scenario,
    )
    persist_gmt_mean(scenario, gmt)


def apply_cooling(
    context: "Context",
    scenario: "Scenario",
    *,
    magicc_dir: str | Path,
    n_runs: int | None = None,
    min_year: int | None = None,
) -> None:
    """Apply wet + dry cooling CIDs in place on *scenario* (``CI_p`` step).

    *min_year* defaults to
    :data:`model.water.data.cooling_impacts._DEFAULT_MIN_YEAR` when ``None``.
    """
    from message_ix_models.model.water.data.cooling_impacts import apply_cooling_cids

    gmt = load_magicc_gmt(magicc_dir, n_runs=n_runs)
    n = gmt.values.shape[0] if gmt.values.ndim > 1 else 1
    kwargs = {"min_year": min_year} if min_year is not None else {}
    apply_cooling_cids(
        scenario,
        gmt,
        commit_message=f"Cooling CIDs (wet+dry), {n} runs",
        regions=context.model.regions,
        **kwargs,
    )
    persist_gmt_mean(scenario, gmt)


def _apply_buildings_and_cooling(
    context: "Context",
    scenario: "Scenario",
    *,
    magicc_dir: str | Path,
    n_runs: int | None = None,
    reference_scenario: str = "SSP2",
    min_year: int | None = None,
) -> None:
    """``CI_bp`` composite: buildings then cooling CIDs in place."""
    apply_buildings(
        context,
        scenario,
        magicc_dir=magicc_dir,
        n_runs=n_runs,
        reference_scenario=reference_scenario,
    )
    apply_cooling(
        context,
        scenario,
        magicc_dir=magicc_dir,
        n_runs=n_runs,
        min_year=min_year,
    )


def generate(
    context: "Context",
    *,
    config_path: str | Path | None = None,
    **options,
) -> Workflow:
    """Build the SPARCCLE Phase-1 + Phase-2 workflow.

    Parameters
    ----------
    context
        Context object (passed to each step).
    config_path
        Path to ``scenario_config.yaml``. Defaults to the packaged copy.
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
    validate_inputs(config)

    wf = Workflow(context)

    platform = config["platform_info"]["name"]
    regions = config["regions"]
    cooling_cfg = config["cooling"]
    cid_cfg = config["cid"]

    targets: list[str] = []
    for starter in config["starters"]:
        model = starter["model"]
        scenario = starter["scenario"]
        ssp = starter["ssp"]
        magicc_dir = starter["magicc_output_dir"]
        if not magicc_dir:
            raise ValueError(f"Starter {model}/{scenario} lacks magicc_output_dir")

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
                target=f"ixmp://{platform}/{model}/{scenario}_CI_b",
                clone=True,
                **buildings,
            )
        )
        targets.append(
            wf.add_step(
                f"{label} CI_p",
                cooling_step,
                apply_cooling,
                target=f"ixmp://{platform}/{model}/{scenario}_CI_p",
                clone=True,
                **cooling,
            )
        )
        targets.append(
            wf.add_step(
                f"{label} CI_bp",
                cooling_step,
                _apply_buildings_and_cooling,
                target=f"ixmp://{platform}/{model}/{scenario}_CI_bp",
                clone=True,
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
