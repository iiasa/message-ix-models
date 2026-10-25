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


# International shipping bunkers are reported only at R12_GLB. Downscale to the
# 12 R12 regions with each region's share of the matching fuel export (same
# idea as message_data materials_dac_tables: ship_reg = ship_glb * exp_reg/exp_glb).
_GLB_REGIONS = frozenset({"R12_GLB", "World", "WORLD", "glb"})
# bunker tech → export tech used for the regional share
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
# meth_exp has both GWa fuel (primary) and Mt feedstock (primary_material);
# only energy-level exports are used for methanol bunker shares.
_EXPORT_ENERGY_LEVELS: dict[str, frozenset[str] | None] = {
    "meth_exp": frozenset({"primary"}),
}


def _regional_nodes(df: pd.DataFrame) -> list[str]:
    """R12 (or other) nodes excluding the global reporting node."""
    regions = [
        r for r in sorted(df["region"].astype(str).unique()) if r not in _GLB_REGIONS
    ]
    return regions


def _export_shares(
    df: pd.DataFrame,
    parts: pd.DataFrame,
    export_tech: str,
    year_cols: list,
    regions: list[str],
) -> pd.DataFrame:
    """Region × year shares of ``in|{export_tech}|…`` (sum over modes/levels)."""
    levels = _EXPORT_ENERGY_LEVELS.get(export_tech)
    hit = (
        (parts[0] == "in")
        & (parts[1] == export_tech)
        & ~df["region"].astype(str).isin(_GLB_REGIONS)
    )
    if levels is not None:
        hit = hit & parts[4].isin(levels)
    if not hit.any():
        return pd.DataFrame(0.0, index=regions, columns=year_cols)

    block = df.loc[hit, ["region", *year_cols]].copy()
    block[year_cols] = block[year_cols].apply(pd.to_numeric, errors="coerce")
    exp = (
        block.groupby("region", sort=False)[year_cols]
        .sum()
        .reindex(regions)
        .fillna(0.0)
    )
    total = exp.sum(axis=0)
    # years with no export → all-zero shares (fillna after div-by-zero → NaN)
    return exp.div(total.where(total != 0), axis=1).fillna(0.0)


def append_regional_shipping(df: pd.DataFrame) -> pd.DataFrame:
    """Append R12-regional copies of GLB international shipping bunker ``in|`` rows.

    Original GLB rows are kept. Variable names (including mode) are unchanged.
    """
    if df.empty:
        return df

    parts = df["variable"].astype(str).str.split("|", expand=True)
    if parts.shape[1] < 5:
        return df

    year_cols = [c for c in df.columns if c not in _ID_COLUMNS]
    regions = _regional_nodes(df)
    if not regions or not year_cols:
        return df

    bunker_hit = (
        (parts[0] == "in")
        & parts[1].isin(_BUNKER_EXPORT_TECH)
        & (parts[4] == "final")
        & df["region"].astype(str).isin(_GLB_REGIONS)
    )
    if not bunker_hit.any():
        return df

    glb = df.loc[bunker_hit, _ID_COLUMNS + year_cols].copy()
    glb[year_cols] = glb[year_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    glb["_export"] = parts.loc[bunker_hit, 1].map(_BUNKER_EXPORT_TECH).to_numpy()

    share_by_export = {
        export_tech: _export_shares(df, parts, export_tech, year_cols, regions)
        for export_tech in sorted(glb["_export"].unique())
    }

    chunks: list[pd.DataFrame] = []
    n_reg = len(regions)
    for export_tech, share in share_by_export.items():
        block_glb = glb.loc[glb["_export"] == export_tech]
        if block_glb.empty:
            continue
        # Broadcast: each GLB row → one row per region
        vals = block_glb[year_cols].to_numpy()  # (n_var, n_year)
        # (n_var, n_reg, n_year)
        scaled = share.to_numpy()[None, :, :] * vals[:, None, :]
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
        if keep.any():
            chunks.append(out.loc[keep, df.columns])

    if not chunks:
        return df

    extra = pd.concat(chunks, ignore_index=True)
    log.info(
        "Appended %d regional shipping bunker row(s) (downscaled from R12_GLB)",
        len(extra),
    )
    return pd.concat([df, extra], ignore_index=True)
