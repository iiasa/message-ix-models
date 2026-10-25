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
# Calibrated to 2020 CHN benchmark (39 Mt H2 production, of which 12.48 Mt
# for methanol and 10.53 Mt for ammonia):
# - NH3: 0.171 t H2 / t NH3
# - methanol: 0.242 t H2 / t methanol; fuel GWa bridged at meth_t_d
#   0.697615 GWa/Mt before applying 0.242
# - syn_liq: CTL FT 0.286 t H2 / t liquid;
#   same HHV bridge as methanol 0.937 → 0.889 GWa H2 / GWa lightoil
#   (2.27273 GWa coal in per GWa liquid ⇒ 0.391 GWa H2 / GWa coal);
#   then GWa H2 → Mt H2 at LHV 120 MJ/kg (0.03154 EJ/GWa)
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
_CAPTIVE_SYN = frozenset({"syn_liq", "syn_liq_ccs"})
_CAPTIVE_NH3_MT_H2 = 0.171
_CAPTIVE_METH_MT_H2 = 0.242
_CAPTIVE_METH_GWA_TO_MT_H2 = (1.0 / 0.697615) * _CAPTIVE_METH_MT_H2
_CAPTIVE_SYN_GWA_H2 = 0.889  # GWa H2 / GWa lightoil (FT; see comment above)
_CAPTIVE_SYN_GWA_TO_MT_H2 = _CAPTIVE_SYN_GWA_H2 * (0.03154 / 0.120)


def append_captive_hydrogen(df: pd.DataFrame) -> pd.DataFrame:
    """Append ``in|t|m|hydrogen|captive`` from NH3 / methanol / syn_liq ``out`` rows.

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
        (
            _CAPTIVE_METH,
            "methanol",
            frozenset({"primary_material"}),
            _CAPTIVE_METH_MT_H2,
        ),
        (_CAPTIVE_METH, "methanol", frozenset({"primary"}), _CAPTIVE_METH_GWA_TO_MT_H2),
        (_CAPTIVE_SYN, "lightoil", frozenset({"secondary"}), _CAPTIVE_SYN_GWA_TO_MT_H2),
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


# International shipping bunkers are reported only at R12_GLB.
# Downscale methods (see append_regional_shipping):
# - by_fuel: ship_reg,f = ship_glb,f * exp_reg,f / exp_glb,f  (per-fuel mix)
# - uniform_mix: same GLB fuel structure everywhere; regional totals from
#   bunker-linked fuel trade (exp + imp)
_GLB_REGIONS = frozenset({"R12_GLB", "World", "WORLD", "glb"})
_ShippingMethod = Literal["by_fuel", "uniform_mix"]
_BUNKER_EXPORT_TECH: dict[str, str] = {
    "foil_bunker": "foil_exp",
    "foil_occ_bunker": "foil_exp",
    "LNG_bunker": "LNG_exp",
    "LNG_occ_bunker": "LNG_exp",
    "meth_bunker": "meth_exp",
    "eth_bunker": "eth_exp",
    "NH3_bunker": "export_NH3",
    "LH2_bunker": "lh2_exp",
}
# meth_* has both GWa fuel (primary) and Mt feedstock (primary_material);
# only energy-level rows are used for shares / trade weights.
_TRADE_ENERGY_LEVELS: dict[str, frozenset[str] | None] = {
    "meth_exp": frozenset({"primary"}),
    "meth_imp": frozenset({"primary"}),
}
# GWa bunker-linked trade techs for uniform_mix totals (exp + imp).
# loil included (materials uses loil_bunker/loil_exp even if not in ribbon list).
_WEIGHT_EXPORT_TECHS = frozenset(
    {"foil_exp", "loil_exp", "LNG_exp", "meth_exp", "eth_exp", "lh2_exp"}
)
_WEIGHT_IMPORT_TECHS = frozenset(
    {"foil_imp", "loil_imp", "LNG_imp", "meth_imp", "eth_imp", "lh2_imp"}
)


def _regional_nodes(df: pd.DataFrame) -> list[str]:
    """R12 (or other) nodes excluding the global reporting node."""
    regions = [
        r for r in sorted(df["region"].astype(str).unique()) if r not in _GLB_REGIONS
    ]
    return regions


def _normalize_shares(weight: pd.DataFrame) -> pd.DataFrame:
    """Region×year absolute weights → shares (0 if column total is 0)."""
    total = weight.sum(axis=0)
    return weight.div(total.where(total != 0), axis=1).fillna(0.0)


def _sum_tech_inputs(
    df: pd.DataFrame,
    parts: pd.DataFrame,
    techs: frozenset[str],
    year_cols: list,
    regions: list[str],
) -> pd.DataFrame:
    """Sum ``in|{tech}|…`` over *techs* by region (energy levels only when mapped)."""
    weight = pd.DataFrame(0.0, index=regions, columns=year_cols)
    for tech in sorted(techs):
        levels = _TRADE_ENERGY_LEVELS.get(tech)
        hit = (
            (parts[0] == "in")
            & (parts[1] == tech)
            & ~df["region"].astype(str).isin(_GLB_REGIONS)
        )
        if levels is not None:
            hit = hit & parts[4].isin(levels)
        if not hit.any():
            continue
        block = df.loc[hit, ["region", *year_cols]].copy()
        block[year_cols] = block[year_cols].apply(pd.to_numeric, errors="coerce")
        weight = weight.add(
            block.groupby("region", sort=False)[year_cols]
            .sum()
            .reindex(regions)
            .fillna(0.0),
            fill_value=0.0,
        )
    return weight


def _export_shares(
    df: pd.DataFrame,
    parts: pd.DataFrame,
    export_tech: str,
    year_cols: list,
    regions: list[str],
) -> pd.DataFrame:
    """Region × year shares of ``in|{export_tech}|…`` (sum over modes/levels)."""
    return _normalize_shares(
        _sum_tech_inputs(df, parts, frozenset({export_tech}), year_cols, regions)
    )


def _trade_shares(
    df: pd.DataFrame,
    parts: pd.DataFrame,
    year_cols: list,
    regions: list[str],
) -> pd.DataFrame:
    """Region×year shares from summed bunker-linked fuel trade (exp + imp)."""
    abs_w = _sum_tech_inputs(df, parts, _WEIGHT_EXPORT_TECHS, year_cols, regions).add(
        _sum_tech_inputs(df, parts, _WEIGHT_IMPORT_TECHS, year_cols, regions),
        fill_value=0.0,
    )
    return _normalize_shares(abs_w)


def _broadcast_glb_to_regions(
    block_glb: pd.DataFrame,
    share: pd.DataFrame,
    regions: list[str],
    year_cols: list,
    columns,
) -> pd.DataFrame | None:
    """Scale each GLB row by region×year ``share``; drop all-zero region rows."""
    if block_glb.empty:
        return None
    n_reg = len(regions)
    vals = block_glb[year_cols].to_numpy()  # (n_var, n_year)
    scaled = share.to_numpy()[None, :, :] * vals[:, None, :]  # (n_var, n_reg, n_year)
    n_var = len(block_glb)
    out = pd.DataFrame(
        {
            "model": block_glb["model"].to_numpy().repeat(n_reg),
            "scenario": block_glb["scenario"].to_numpy().repeat(n_reg),
            "region": list(regions) * n_var,
            "variable": block_glb["variable"].to_numpy().repeat(n_reg),
        }
    )
    for i, c in enumerate(year_cols):
        out[c] = scaled[:, :, i].reshape(-1)
    keep = out[year_cols].abs().gt(0).any(axis=1)
    if not keep.any():
        return None
    return out.loc[keep, columns]


def _bunker_masks(df: pd.DataFrame, parts: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Return ``(bunker_glb, bunker_reg)`` boolean masks for ``in|`` bunker rows."""
    is_bunker = (
        (parts[0] == "in") & parts[1].isin(_BUNKER_EXPORT_TECH) & (parts[4] == "final")
    )
    glb = df["region"].astype(str).isin(_GLB_REGIONS)
    return is_bunker & glb, is_bunker & ~glb


def _strip_regional_bunker(
    df: pd.DataFrame, parts: pd.DataFrame, bunker_reg: pd.Series
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """Drop prior regional bunker rows; refresh ``df``, ``parts``, ``bunker_glb``."""
    if not bunker_reg.any():
        bunker_glb, _ = _bunker_masks(df, parts)
        return df, parts, bunker_glb

    log.info(
        "Removing %d existing regional bunker row(s) before re-downscale",
        int(bunker_reg.sum()),
    )
    df = df.loc[~bunker_reg].copy()
    parts = df["variable"].astype(str).str.split("|", expand=True)
    bunker_glb, _ = _bunker_masks(df, parts)
    return df, parts, bunker_glb


def _shipping_chunks(
    df: pd.DataFrame,
    parts: pd.DataFrame,
    glb: pd.DataFrame,
    regions: list[str],
    year_cols: list,
    method: _ShippingMethod,
) -> list[pd.DataFrame]:
    """Build regional downscaled frames for ``method``."""
    chunks: list[pd.DataFrame] = []
    if method == "uniform_mix":
        share = _trade_shares(df, parts, year_cols, regions)
        part = _broadcast_glb_to_regions(
            glb.drop(columns=["_export"]), share, regions, year_cols, df.columns
        )
        if part is not None:
            chunks.append(part)
        return chunks

    # by_fuel: independent export share per bunker fuel
    for export_tech in sorted(glb["_export"].unique()):
        share = _export_shares(df, parts, export_tech, year_cols, regions)
        part = _broadcast_glb_to_regions(
            glb.loc[glb["_export"] == export_tech].drop(columns=["_export"]),
            share,
            regions,
            year_cols,
            df.columns,
        )
        if part is not None:
            chunks.append(part)
    return chunks


def append_regional_shipping(
    df: pd.DataFrame,
    *,
    method: _ShippingMethod = "uniform_mix",
) -> pd.DataFrame:
    """Append R12-regional copies of GLB international shipping bunker ``in|`` rows.

    Parameters
    ----------
    method
        ``"by_fuel"``
            Each bunker fuel is scaled by that fuel's own export share
            (``foil_exp``, ``LNG_exp``, …). Regional *fuel mixes* can differ
            from GLB (materials_dac_tables style).
        ``"uniform_mix"`` (default)
            One regional share (from bunker-linked fuel trade: exp + imp) is
            applied to every bunker variable. Every region keeps the GLB fuel
            structure; only the regional *totals* differ and sum to GLB.
    """
    if df.empty:
        return df
    if method not in ("by_fuel", "uniform_mix"):
        raise ValueError(f"method must be 'by_fuel' or 'uniform_mix', got {method!r}")

    parts = df["variable"].astype(str).str.split("|", expand=True)
    if parts.shape[1] < 5:
        return df

    year_cols = [c for c in df.columns if c not in _ID_COLUMNS]
    regions = _regional_nodes(df)
    if not regions or not year_cols:
        return df

    bunker_glb, bunker_reg = _bunker_masks(df, parts)
    if not bunker_glb.any():
        return df

    df, parts, bunker_glb = _strip_regional_bunker(df, parts, bunker_reg)
    if not bunker_glb.any():
        return df

    glb = df.loc[bunker_glb, _ID_COLUMNS + year_cols].copy()
    glb[year_cols] = glb[year_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    glb["_export"] = parts.loc[bunker_glb, 1].map(_BUNKER_EXPORT_TECH).to_numpy()

    chunks = _shipping_chunks(df, parts, glb, regions, year_cols, method)
    if not chunks:
        return df

    extra = pd.concat(chunks, ignore_index=True)
    log.info(
        "Appended %d regional shipping bunker row(s) from R12_GLB (%s)",
        len(extra),
        method if method == "by_fuel" else f"{method}/trade",
    )
    return pd.concat([df, extra], ignore_index=True)
