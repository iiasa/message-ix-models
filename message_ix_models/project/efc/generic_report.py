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


# Captive H2 on a mass basis (Mt H2). meth_h2 omitted (merchant H2).
# - NH3: 0.178 t H2 / t NH3
# - methanol: 0.189 t H2 / t methanol; fuel GWa bridged at meth_t_d
# TODO: YJ, recalibrate after checking CHN 2020 benchmark
#   0.697615 GWa/Mt before applying 0.189
_CAPTIVE_NH3 = frozenset(
    {
        "biomass_NH3",
        "biomass_NH3_ccs",
        "coal_NH3",
        "coal_NH3_ccs",
        "electr_NH3",
        "fueloil_NH3",
        "fueloil_NH3_ccs",
        "gas_NH3",
        "gas_NH3_ccs",
    }
)
_CAPTIVE_METH = frozenset(
    {"meth_bio", "meth_bio_ccs", "meth_coal", "meth_coal_ccs", "meth_ng", "meth_ng_ccs"}
)
_CAPTIVE_NH3_MT_H2 = 0.178
_CAPTIVE_METH_MT_H2 = 0.189
_CAPTIVE_METH_GWA_TO_MT_H2 = (1.0 / 0.697615) * _CAPTIVE_METH_MT_H2


def append_captive_hydrogen(df: pd.DataFrame) -> pd.DataFrame:
    """Append ``in|t|m|hydrogen|captive`` from NH3 / methanol ``out`` rows.

    Values are Mt H2 (mass stoich). ``meth_h2`` is excluded (merchant H2).
    """
    if df.empty:
        return df

    parts = df["variable"].astype(str).str.split("|", expand=True)
    if parts.shape[1] < 5:
        return df

    year_cols = [c for c in df.columns if c not in _ID_COLUMNS]
    chunks: list[pd.DataFrame] = []

    # level-specific factors so GWa fuel and Mt feedstock both land in Mt H2
    specs: list[tuple[frozenset[str], str, frozenset[str], float]] = [
        (_CAPTIVE_NH3, "NH3", frozenset({"secondary_material"}), _CAPTIVE_NH3_MT_H2),
        (_CAPTIVE_METH, "methanol", frozenset({"primary_material"}), _CAPTIVE_METH_MT_H2),
        (_CAPTIVE_METH, "methanol", frozenset({"primary"}), _CAPTIVE_METH_GWA_TO_MT_H2),
    ]

    for techs, commodity, levels, factor in specs:
        hit = (
            (parts[0] == "out")
            & parts[1].isin(techs)
            & (parts[3] == commodity)
            & parts[4].isin(levels)
        )
        if not hit.any():
            continue
        block = df.loc[hit].copy()
        block["variable"] = (
            "in|" + parts.loc[hit, 1] + "|" + parts.loc[hit, 2] + "|hydrogen|captive"
        )
        block[year_cols] = block[year_cols].mul(factor)
        chunks.append(block)

    if not chunks:
        return df

    extra = pd.concat(chunks, ignore_index=True)
    log.info(
        "Appended %d captive-H2 row(s) (in|*|*|hydrogen|captive, Mt H2)", len(extra)
    )
    return pd.concat([df, extra], ignore_index=True)
