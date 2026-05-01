"""GMT array extraction and ingestion.

Three entry points for assembling a :class:`GmtArray` (``values`` +
``years`` NamedTuple):

- :func:`gmt_ensemble` — extract wide-DataFrame rows as ``(n_rows, n_years)``.
- :func:`gmt_expectation` — nanmean across rows as ``(n_years,)``.
- :func:`load_magicc_gmt` — read a MAGICC climate-assessment Excel file
  and return the per-run GSAT trajectories.
"""

import functools
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_GSAT_VAR = "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"
_ID_COLS = ["Model", "Scenario", "Region", "Variable", "Unit"]


class GmtArray(NamedTuple):
    """GMT values with year labels.

    Attributes
    ----------
    values
        Shape ``(n_runs, n_years)`` for ensemble or ``(n_years,)`` for
        expectation.
    years
        Year labels, shape ``(n_years,)``.
    """

    values: np.ndarray
    years: np.ndarray


def _year_columns(df: pd.DataFrame, id_cols: Sequence[str]) -> list:
    """Identify year columns: all columns not in *id_cols* that are numeric.

    Accepts both ``int`` columns (``2020``) and ``str``-of-digits
    (``"2020"``). Returns original column labels sorted by integer value.

    Raises
    ------
    ValueError
        If no year columns are found.
    """
    id_set = set(id_cols)
    cols = sorted(
        (
            c
            for c in df.columns
            if c not in id_set
            and (isinstance(c, (int, float)) or (isinstance(c, str) and c.isdigit()))
        ),
        key=lambda c: int(c),
    )
    if not cols:
        raise ValueError(
            f"No year columns found. ID columns: {list(id_cols)}; "
            f"all columns: {list(df.columns)}"
        )
    return cols


def gmt_ensemble(df: pd.DataFrame, id_cols: Sequence[str]) -> GmtArray:
    """Extract wide DataFrame rows as a 2D array.

    Parameters
    ----------
    df
        Wide DataFrame with ID columns and year columns (int or str-digits).
    id_cols
        Non-year columns (e.g. ``["Model", "Scenario", "Variable"]``).

    Returns
    -------
    GmtArray
        ``.values`` shape ``(n_rows, n_years)``, ``.years`` shape
        ``(n_years,)``.
    """
    cols = _year_columns(df, id_cols)
    values = df[cols].values.astype(float)
    years = np.array([int(c) for c in cols])
    return GmtArray(values, years)


def gmt_expectation(arr: GmtArray) -> GmtArray:
    """Per-year nanmean of *arr* along the run axis.

    A 2-D ``(n_runs, n_years)`` input collapses to 1-D ``(n_years,)``.
    A 1-D input is returned unchanged.
    """
    values = np.asarray(arr.values)
    if values.ndim == 1:
        return arr
    return GmtArray(values=np.nanmean(values, axis=0), years=arr.years)


_GMT_VARIABLE = "Physical Climate Impact|Surface Temperature (GSAT)|Mean"


def persist_gmt_mean(scen, gmt: GmtArray) -> None:
    """Persist the ensemble-mean GMT trajectory on *scen* as a global timeseries.

    Writes one ``World``-region row per year under
    ``Physical Climate Impact|Surface Temperature (GSAT)|Mean`` (degC).

    Idempotent: if the variable already exists on the scenario and matches
    the computed mean, return without writing. Raises if the existing rows
    disagree with the new mean — that means the upstream MAGICC ensemble
    changed between CID-step invocations on the same scenario, which is a
    workflow-integrity bug worth surfacing rather than silently overwriting.
    """
    from message_ix_models.tools.iamc import frame_to_iamc

    mean = gmt_expectation(gmt)
    ts = (
        pd.DataFrame(
            {
                "region": "World",
                "year": np.asarray(mean.years, dtype=int),
                "value": np.asarray(mean.values),
            }
        )
        .pipe(frame_to_iamc, _GMT_VARIABLE, "degC")
        .sort_values("year")
        .reset_index(drop=True)
    )
    if ts.empty:
        return

    existing = scen.timeseries(variable=_GMT_VARIABLE)
    if not existing.empty:
        prior = (
            existing[existing["region"] == "World"][["year", "value"]]
            .sort_values("year")
            .reset_index(drop=True)
        )
        proposed = ts[["year", "value"]]
        if prior["year"].equals(proposed["year"]) and np.allclose(
            prior["value"].to_numpy(), proposed["value"].to_numpy(), atol=1e-9
        ):
            log.debug("GMT mean already persisted on scenario; skipping write")
            return
        raise ValueError(
            f"Existing {_GMT_VARIABLE} on scenario disagrees with computed "
            "mean — upstream MAGICC ensemble changed between CID steps."
        )

    scen.check_out(timeseries_only=True)
    try:
        scen.add_timeseries(ts)
        scen.commit(f"Persist GMT mean ({_GMT_VARIABLE})")
    except BaseException:
        scen.discard_changes()
        raise


def load_magicc_gmt(magicc_dir: str | Path, n_runs: int | None = None) -> GmtArray:
    """Load a GSAT ensemble from a MAGICC climate-assessment Excel file.

    Reads the IAMC-format ``*_IAMC_climateassessment.xlsx`` file in
    *magicc_dir* and extracts individual GSAT trajectories identified by
    ``Model = ...|run_{N}``. Results are cached by ``(Path, n_runs)`` so
    repeated calls within a single workflow run reuse the same array.

    Parameters
    ----------
    magicc_dir
        Directory containing the climate-assessment Excel output.
    n_runs
        Maximum number of runs to load. ``None`` loads all available.

    Returns
    -------
    GmtArray
        ``.values`` shape ``(n_runs, n_years)``, degC above pre-industrial.
        ``.years`` shape ``(n_years,)``, integer year labels.
    """
    return _load_magicc_gmt_cached(Path(magicc_dir), n_runs)


@functools.lru_cache(maxsize=None)
def _load_magicc_gmt_cached(magicc_dir: Path, n_runs: int | None) -> GmtArray:
    iamc_files = sorted(magicc_dir.glob("*_IAMC_climateassessment.xlsx"))
    if not iamc_files:
        raise FileNotFoundError(f"No *_IAMC_climateassessment.xlsx in {magicc_dir}")
    if len(iamc_files) > 1:
        raise ValueError(
            f"Multiple *_IAMC_climateassessment.xlsx in {magicc_dir}: "
            f"{[p.name for p in iamc_files]}. Resolve ambiguity before loading."
        )
    iamc_path = iamc_files[0]

    log.info("Loading MAGICC ensemble from %s", iamc_path.name)
    df = pd.read_excel(iamc_path, sheet_name="data")

    gsat = df[
        (df["Variable"] == _GSAT_VAR)
        & df["Model"].str.contains("|run_", na=False, regex=False)
    ]
    if gsat.empty:
        raise ValueError(
            f"No individual GSAT runs found. Variables: "
            f"{df['Variable'].unique()[:5].tolist()}"
        )
    if n_runs is not None:
        gsat = gsat.head(n_runs)

    array = gmt_ensemble(gsat, _ID_COLS)
    log.info(
        "GMT ensemble: %d runs x %d years (%d-%d)",
        array.values.shape[0],
        array.values.shape[1],
        int(array.years[0]),
        int(array.years[-1]),
    )
    return array
