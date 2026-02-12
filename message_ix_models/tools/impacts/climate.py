"""GMT input parsing for climate impact predictions.

Isolates all MAGICC-specific format knowledge. The MAGICC variable string
``"AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"``
lives in exactly one place: :func:`load_magicc_ensemble`.

Callers interact through :func:`load_gmt`, which auto-detects input format.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# MAGICC variable identifier — single source of truth
_MAGICC_GSAT_VARIABLE = (
    "AR6 climate diagnostics|Surface Temperature (GSAT)|MAGICCv7.5.3"
)


def load_gmt(
    source: np.ndarray | pd.DataFrame | str | Path,
    n_runs: int | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Load GMT data from any supported format.

    Auto-detects input type and dispatches to the appropriate loader.

    Parameters
    ----------
    source
        One of:

        - **ndarray** ``(n_years,)`` or ``(n_runs, n_years)`` — used directly.
          Years assumed to be 2020..2020+n_years-1.
        - **DataFrame** with ``"Model"`` column containing ``"|run_"`` —
          treated as MAGICC ensemble (``*_all_runs.xlsx``).
        - **DataFrame** with ``"Variable"`` containing ``"Percentile"`` —
          treated as percentile summary.
        - **str or Path** ending in ``".xlsx"`` — loaded as MAGICC Excel file.
    n_runs
        For ensemble sources, limit to first *n_runs* members.

    Returns
    -------
    gmt_2d : np.ndarray
        Shape ``(n_runs, n_years)``. For single trajectories, ``n_runs=1``.
    years : np.ndarray
        Year labels, shape ``(n_years,)``.
    """
    # ndarray passthrough
    if isinstance(source, np.ndarray):
        if source.ndim == 1:
            source = source[np.newaxis, :]
        if n_runs is not None and source.ndim == 2:
            source = source[:n_runs]
        n_years = source.shape[1]
        years = np.arange(2020, 2020 + n_years)
        return source, years

    # Path → load Excel then recurse
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"MAGICC file not found: {path}")
        log.info(f"Loading MAGICC file: {path.name}")
        df = pd.read_excel(path)
        return load_gmt(df, n_runs=n_runs)

    # DataFrame dispatch
    if isinstance(source, pd.DataFrame):
        if (
            "Model" in source.columns
            and source["Model"].str.contains("|run_", na=False, regex=False).any()
        ):
            gmt_2d, years = load_magicc_ensemble(source)
            if n_runs is not None:
                gmt_2d = gmt_2d[:n_runs]
            return gmt_2d, years

        if (
            "Variable" in source.columns
            and source["Variable"]
            .str.contains("Percentile", na=False, regex=False)
            .any()
        ):
            percentile_dict = load_magicc_percentiles(source)
            # Use median (50th) as default, or first available
            key = (
                "50.0th Percentile"
                if "50.0th Percentile" in percentile_dict
                else next(iter(percentile_dict))
            )
            gmt_1d, years = percentile_dict[key]
            return gmt_1d[np.newaxis, :], years

        raise ValueError(
            "DataFrame format not recognized. Expected MAGICC ensemble "
            "(Model column with '|run_') or percentile summary."
        )

    raise TypeError(f"Unsupported source type: {type(source)}")


def load_magicc_ensemble(
    source: pd.DataFrame | str | Path,
    variable_filter: str | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Load full MAGICC ensemble from ``*_all_runs.xlsx``.

    Parameters
    ----------
    source
        MAGICC output DataFrame or path to Excel file.
    variable_filter
        Override the default GSAT variable string for filtering.

    Returns
    -------
    gmt_2d : np.ndarray
        Shape ``(n_runs, n_years)``.
    years : np.ndarray
        Year labels.
    """
    if isinstance(source, (str, Path)):
        source = pd.read_excel(source)

    var_filter = variable_filter or _MAGICC_GSAT_VARIABLE

    # Filter to GSAT rows with run IDs
    mask = source["Variable"].str.contains(var_filter, na=False, regex=False) & source[
        "Model"
    ].str.contains("|run_", na=False, regex=False)
    gsat_rows = source[mask]

    if gsat_rows.empty:
        raise ValueError(
            f"No GSAT ensemble data found. Variable filter: '{var_filter}'"
        )

    # Extract year columns
    year_cols = sorted(
        [
            c
            for c in source.columns
            if isinstance(c, (int, float)) or (isinstance(c, str) and c.isdigit())
        ]
    )
    if not year_cols:
        raise ValueError("No year columns found in MAGICC DataFrame")

    years = np.array([int(y) for y in year_cols])

    # Extract run IDs and sort
    def _parse_run_id(model_str):
        if "|run_" in str(model_str):
            try:
                return int(str(model_str).split("|run_")[1].split("|")[0])
            except (IndexError, ValueError):
                return None
        return None

    gsat_rows = gsat_rows.copy()
    gsat_rows["_run_id"] = gsat_rows["Model"].apply(_parse_run_id)
    gsat_rows = gsat_rows.dropna(subset=["_run_id"])
    gsat_rows = gsat_rows.sort_values("_run_id")

    # Build 2D array
    gmt_2d = gsat_rows[year_cols].values.astype(float)

    log.info(f"Loaded MAGICC ensemble: {gmt_2d.shape[0]} runs, {len(years)} years")
    return gmt_2d, years


def load_magicc_percentiles(
    source: pd.DataFrame | str | Path,
) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    """Load percentile-binned GMT trajectories.

    Parameters
    ----------
    source
        MAGICC output DataFrame or path to Excel file.

    Returns
    -------
    dict
        Keys are percentile names (e.g. ``"50.0th Percentile"``),
        values are ``(gmt_1d, years)`` tuples.
    """
    if isinstance(source, (str, Path)):
        source = pd.read_excel(source)

    mask = source["Variable"].str.contains(
        _MAGICC_GSAT_VARIABLE, na=False, regex=False
    ) & source["Variable"].str.contains("Percentile", na=False, regex=False)
    pct_rows = source[mask]

    if pct_rows.empty:
        raise ValueError("No percentile data found in MAGICC DataFrame")

    year_cols = sorted(
        [
            c
            for c in source.columns
            if isinstance(c, (int, float)) or (isinstance(c, str) and c.isdigit())
        ]
    )
    years = np.array([int(y) for y in year_cols])

    result = {}
    for _, row in pct_rows.iterrows():
        var_str = row["Variable"]
        # Extract percentile label from variable string
        parts = var_str.split("|")
        pct_label = parts[-1].strip() if len(parts) > 3 else var_str
        gmt_1d = row[year_cols].values.astype(float)
        result[pct_label] = (gmt_1d, years)

    return result


def percentiles_to_ensemble(
    percentile_trajectories: dict[str, tuple[np.ndarray, np.ndarray]],
    method: str = "weighted",
) -> tuple[np.ndarray, np.ndarray]:
    """Convert percentile trajectories to a mini-ensemble array.

    Useful when only percentile summaries are available but a 2D ensemble
    array is needed for prediction. Currently stacks trajectories with
    equal weight (true density-proportional weighting is not yet
    implemented).

    .. warning::
        This assumes approximate linearity of the emulator response.
        Use :func:`~message_ix_models.tools.impacts.rime.check_emulator_linearity`
        to verify before relying on this.

    Parameters
    ----------
    percentile_trajectories
        Output of :func:`load_magicc_percentiles`.
    method
        ``"weighted"`` — currently equal-weight stacking (placeholder
        for future density-proportional weighting).

    Returns
    -------
    gmt_2d : np.ndarray
        Shape ``(n_percentiles, n_years)``.
    years : np.ndarray
        Year labels.
    """
    if method != "weighted":
        raise ValueError(f"Unknown method: {method}")

    if not percentile_trajectories:
        raise ValueError("Empty percentile_trajectories dict")

    trajectories = []
    years: np.ndarray | None = None
    for label, (gmt_1d, yrs) in sorted(percentile_trajectories.items()):
        trajectories.append(gmt_1d)
        if years is None:
            years = yrs

    gmt_2d = np.stack(trajectories, axis=0)
    assert years is not None  # guaranteed by non-empty dict
    return gmt_2d, years
