"""Generic reporting for all technologies (in|/out| flow).
No unit column.
"""

import logging
from typing import Literal

import pandas as pd
from message_ix.report import Reporter

log = logging.getLogger(__name__)

# Keep ya; sum yv, nd, h, hd, e, etc.
_FLOW_KEY = "nl-t-ya-m-c-l"

_ID_COLUMNS = ["model", "scenario", "region", "variable"]


def _long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long ``year`` / ``value`` rows to one column per year."""
    if df.empty:
        return pd.DataFrame(columns=_ID_COLUMNS)

    wide = df.pivot_table(
        index=_ID_COLUMNS,
        columns="year",
        values="value",
        aggfunc="sum",
    )
    wide.columns.name = None
    year_cols = sorted(wide.columns, key=int)
    return wide[year_cols].reset_index()


def _flow_quantity_to_frame(qty) -> pd.DataFrame:
    """Convert a genno/xarray quantity to a flat DataFrame."""
    if hasattr(qty, "to_series"):
        series = qty.to_series()
    else:
        series = pd.Series(qty)
    return series.reset_index(name="value")


# Maybe not needed
def _aggregate_over_vintage(df: pd.DataFrame) -> pd.DataFrame:
    """Sum over ``yv`` when the reporter key still exposes it."""
    if "yv" not in df.columns:
        return df
    group_cols = [c for c in df.columns if c not in {"yv", "value"}]
    return df.groupby(group_cols, dropna=False, as_index=False)["value"].sum()


def flow_to_iamc(
    df: pd.DataFrame,
    flow: Literal["in", "out"],
    model_name: str,
    scenario_name: str,
    *,
    firstmodelyear: int | None = None,
) -> pd.DataFrame:
    """Map Reporter ``in``/``out`` data to wide rows (one column per year)."""
    if df.empty:
        return pd.DataFrame(columns=_ID_COLUMNS)

    # Maybe not needed
    df = _aggregate_over_vintage(df)

    required = {"nl", "t", "ya", "m", "c", "l", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Flow data missing columns: {sorted(missing)}")

    if firstmodelyear is not None:
        df = df[df["ya"] >= firstmodelyear]

    df = df.assign(
        variable=(
            flow
            + "|"
            + df["t"].astype(str)
            + "|"
            + df["m"].astype(str)
            + "|"
            + df["c"].astype(str)
            + "|"
            + df["l"].astype(str)
        ),
        model=model_name,
        scenario=scenario_name,
    )
    df = df.rename(columns={"nl": "region", "ya": "year"})
    df = df[df["value"].abs() > 0]

    return _long_to_wide(df)


def genno_generic(
    rep: Reporter,
    model_name: str,
    scenario_name: str,
    *,
    flows: tuple[Literal["in", "out"], ...] = ("in", "out"),
    firstmodelyear: int | None = None,
) -> pd.DataFrame:
    """Report all technology input/output flows as ``in|t|m|c|l`` / ``out|t|m|c|l``.

    Parameters
    ----------
    rep
        :class:`message_ix.Reporter` built from the target scenario.
    flows
        Reporter quantities to export. Defaults to both ``in`` and ``out``.
    firstmodelyear
        If given, only activity years ``>= firstmodelyear`` are retained.

    Notes
    -----
    Data are retrieved at ``{flow}:nl-t-ya-m-c-l``, so ``yv`` is summed by the
    reporter. Values are model-native magnitudes (no unit conversion).
    """
    rep.set_filters()
    dfs: list[pd.DataFrame] = []

    for flow in flows:
        key = f"{flow}:{_FLOW_KEY}"
        try:
            raw = rep.get(key)
        except Exception as exc:
            log.warning("Could not retrieve %s: %s", key, exc)
            continue

        part = flow_to_iamc(
            _flow_quantity_to_frame(raw),
            flow,
            model_name,
            scenario_name,
            firstmodelyear=firstmodelyear,
        )
        if not part.empty:
            dfs.append(part)

    rep.set_filters()

    if not dfs:
        return pd.DataFrame(columns=_ID_COLUMNS)

    return pd.concat(dfs, ignore_index=True)
