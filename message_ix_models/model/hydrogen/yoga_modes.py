"""Make the hyway electrolyser techs compatible with meth_h2's 6 split modes.

Background: Yoga's upstream ``update_meth_h2_modes`` (``message_ix_models.
project.ssp.script.util.functions``) hardcodes ``h2_elec`` as the
methanol_synthesis_addon parent and broadcasts its feedstock/fuel modes into
the six split modes (``{feedstock, fuel}_{bic, dac, fic}``). In the hydrogen
module, ``add_hydrogen_techs`` removes ``h2_elec`` and adds the hyway techs
(h2_elec_alk/pem/soe + h2_pyro_elec + h2_ct), each populated from per-tech
CSVs that still carry the pre-Yoga mode shape: addon_conversion/addon_up and
historical_activity in feedstock/fuel modes, everything else in M1. The
historical activity is absolute, so this module restores its pre-broadcast
mode total after replicating the per-unit parameters. Without the broadcast,
meth_h2 (which already has the 6 split modes upstream) cannot bind to any
electrolyser parent and ``ADDON_ACTIVITY_UP`` collapses — meth_h2 silently
zeros.

This module ports Yoga's broadcast logic onto a configurable list of parent
techs. Call after ``add_hydrogen_techs`` has populated the parameter data:

    with scenario.transact(message="..."):
        apply_meth_h2_mode_parity(scenario, ["h2_elec_alk", "h2_elec_pem",
                                             "h2_elec_soe"])
"""

from __future__ import annotations

import logging

import message_ix
import pandas as pd

from message_ix_models.util import broadcast

log = logging.getLogger(__name__)


METHANOL_ADDON_TYPE = "methanol_synthesis_addon"

ORIGINAL_MODES = ("feedstock", "fuel")
SPLIT_SUFFIXES = ("bic", "dac", "fic")
SPLIT_MODES: list[str] = [
    f"{base}_{sfx}" for base in ORIGINAL_MODES for sfx in SPLIT_SUFFIXES
]

# ACTIVITY_CONSTRAINT_UP/LO sum historical activity over mode.
HISTORY_KEYS = ["node_loc", "technology", "year_act", "time"]
HISTORY_ATOL = 1e-9


def _history_totals(rows: pd.DataFrame) -> pd.DataFrame:
    """Return historical activity totalled over mode."""
    if rows.empty:
        return pd.DataFrame(columns=HISTORY_KEYS + ["value"])

    required = HISTORY_KEYS + ["mode", "value"]
    missing = [column for column in required if column not in rows]
    if missing:
        raise ValueError(f"Historical activity is missing columns: {missing}")
    if rows[required].isna().any().any():
        raise ValueError("Historical activity contains null keys, modes, or values")

    return rows.groupby(HISTORY_KEYS, as_index=False)["value"].sum()


def _verify_history_restored(before: pd.DataFrame, after_rows: pd.DataFrame) -> None:
    """Raise unless every post-broadcast mode total matches its source total."""
    after = _history_totals(after_rows)
    merged = before.merge(
        after,
        on=HISTORY_KEYS,
        how="outer",
        suffixes=("_before", "_after"),
    ).fillna(0.0)
    delta = (merged["value_before"] - merged["value_after"]).abs()
    bad = merged[delta > HISTORY_ATOL]
    if not bad.empty:
        raise RuntimeError(
            "historical_activity totals differ from their pre-parity values in "
            f"{len(bad)} group(s); worst delta {delta.loc[bad.index].max():.6g}"
        )


def _restored_history_rows(rows: pd.DataFrame, before: pd.DataFrame) -> pd.DataFrame:
    """Return split-mode rows rescaled to preserve pre-broadcast totals."""
    if rows.empty and before.empty:
        return rows.copy()
    if (rows["value"] < -HISTORY_ATOL).any():
        raise RuntimeError("historical_activity contains negative values")

    try:
        _verify_history_restored(before, rows)
    except RuntimeError:
        pass
    else:
        return rows.iloc[0:0].copy()

    split = rows[rows["mode"].isin(SPLIT_MODES)]
    untouched = rows[~rows["mode"].isin(SPLIT_MODES)]
    if split.empty:
        raise RuntimeError(
            "historical_activity totals changed without any split-mode rows"
        )

    split_now = _history_totals(split).rename(columns={"value": "split_now"})
    untouched_now = _history_totals(untouched).rename(columns={"value": "untouched"})
    target = before.rename(columns={"value": "total_before"}).merge(
        untouched_now, on=HISTORY_KEYS, how="left"
    )
    target["untouched"] = target["untouched"].fillna(0.0)
    target["target_split"] = target["total_before"] - target["untouched"]

    groups = target[HISTORY_KEYS + ["target_split"]].merge(
        split_now, on=HISTORY_KEYS, how="outer", indicator=True
    )
    unexpected = groups[groups["_merge"] == "right_only"]
    missing = groups[
        (groups["_merge"] == "left_only")
        & (groups["target_split"].abs() > HISTORY_ATOL)
    ]
    if not unexpected.empty or not missing.empty:
        raise RuntimeError(
            "split historical_activity groups do not match the pre-parity source groups"
        )

    groups = groups[groups["_merge"] == "both"].drop(columns="_merge")
    negative = groups[groups["target_split"] < -HISTORY_ATOL]
    zero_source = groups[
        (groups["split_now"].abs() <= HISTORY_ATOL)
        & (groups["target_split"].abs() > HISTORY_ATOL)
    ]
    if not negative.empty:
        raise RuntimeError("untouched historical_activity exceeds the pre-parity total")
    if not zero_source.empty:
        raise RuntimeError(
            "nonzero historical_activity target has zero split-mode activity"
        )

    groups["factor"] = 1.0
    nonzero = groups["split_now"].abs() > HISTORY_ATOL
    groups.loc[nonzero, "factor"] = (
        groups.loc[nonzero, "target_split"] / groups.loc[nonzero, "split_now"]
    )
    changed = groups[(groups["factor"] - 1.0).abs() > HISTORY_ATOL]
    if changed.empty:
        raise RuntimeError(
            "historical_activity totals differ but no split-mode scaling is available"
        )

    adjusted_split = split.merge(
        groups[HISTORY_KEYS + ["factor"]], on=HISTORY_KEYS, how="left"
    )
    adjusted_split["value"] *= adjusted_split["factor"]
    adjusted_split = adjusted_split.drop(columns="factor")
    candidate = pd.concat([untouched, adjusted_split], ignore_index=True)
    _verify_history_restored(before, candidate)

    return adjusted_split.merge(changed[HISTORY_KEYS], on=HISTORY_KEYS, how="inner")[
        rows.columns
    ]


def _ensure_split_modes_in_set(scenario: message_ix.Scenario) -> None:
    existing = set(scenario.set("mode").tolist())
    to_add = [m for m in SPLIT_MODES if m not in existing]
    if to_add:
        scenario.add_set("mode", to_add)
        log.info(f"Added split modes to `mode` set: {to_add}")


def _broadcast_parent_modes(scenario: message_ix.Scenario, parent: str) -> None:
    """Port one parent tech's mode-indexed parameter rows.

    Two patterns, applied per parameter:

    - **Yoga pattern** (``addon_conversion``, ``addon_up`` carry
      feedstock/fuel mode rows from the per-tech CSV): broadcast
      ``feedstock`` → ``{feedstock_bic, feedstock_dac, feedstock_fic}``,
      ``fuel`` → ``{fuel_bic, fuel_dac, fuel_fic}``, then remove the
      originals — same as upstream ``update_meth_h2_modes``.
    - **M1 broadcast** (``input``, ``output``, ``var_cost``, etc.):
      replicate the M1 rows across all six split modes so the electrolyser
      can operate in any of them. M1 is preserved — it remains the tech's
      primary operating mode for hydrogen unrelated to meth_h2.
    """
    for par in scenario.par_list():
        if "mode" not in scenario.idx_sets(par):
            continue

        feedstock_rows = scenario.par(
            par, filters={"technology": parent, "mode": "feedstock"}
        )
        fuel_rows = scenario.par(par, filters={"technology": parent, "mode": "fuel"})

        # Yoga pattern: feedstock/fuel → 6 split modes; drop originals.
        for original_mode, original_rows in (
            ("feedstock", feedstock_rows),
            ("fuel", fuel_rows),
        ):
            if original_rows.empty:
                continue
            df = original_rows.copy(deep=True)
            df["mode"] = None
            split_for_orig = [f"{original_mode}_{sfx}" for sfx in SPLIT_SUFFIXES]
            df = df.pipe(broadcast, mode=split_for_orig)
            scenario.add_par(par, df)
            scenario.remove_par(par, original_rows)
            log.debug(
                f"  {parent} {par}: {len(original_rows)} {original_mode} rows "
                f"→ {len(df)} split rows."
            )

        # M1 pattern: only fires when feedstock/fuel are absent for this par.
        # Skips parameters that are not mode-indexed in a per-tech sense
        # (e.g. relation_activity is mode-indexed but conceptually keyed on
        # the relation, not the tech's operating mode — we still broadcast it
        # so the tech's contribution applies in all modes it can operate in).
        if feedstock_rows.empty and fuel_rows.empty:
            m1_rows = scenario.par(par, filters={"technology": parent, "mode": "M1"})
            if m1_rows.empty:
                continue
            df = m1_rows.copy(deep=True)
            df["mode"] = None
            df = df.pipe(broadcast, mode=SPLIT_MODES)
            scenario.add_par(par, df)
            log.debug(
                f"  {parent} {par}: {len(m1_rows)} M1 rows "
                f"→ {len(df)} split rows (M1 retained)."
            )


def _register_in_map_tec_addon(
    scenario: message_ix.Scenario, parents: list[str]
) -> None:
    existing = scenario.set("map_tec_addon")
    rows = []
    for parent in parents:
        already = (
            (existing["technology"] == parent)
            & (existing["type_addon"] == METHANOL_ADDON_TYPE)
        ).any()
        if not already:
            rows.append({"technology": parent, "type_addon": METHANOL_ADDON_TYPE})

    if rows:
        scenario.add_set("map_tec_addon", pd.DataFrame(rows))
        log.info(
            f"Registered {len(rows)} new parent(s) in map_tec_addon: "
            f"{[r['technology'] for r in rows]}"
        )


def apply_meth_h2_mode_parity(
    scenario: message_ix.Scenario,
    parents: list[str],
) -> None:
    """Port Yoga's mode-parity fix to the methanol_synthesis_addon parents.

    Must be called inside a check-out window (e.g. ``with scenario.transact(...)``)
    and after the parent techs have been populated with their per-tech
    parameter data (e.g. by ``add_hydrogen_techs``).

    Parameters
    ----------
    scenario
        Scenario with the hyway techs already added.
    parents
        Hyway tech names to register as methanol_synthesis_addon parents.
        Typically ``["h2_elec_alk", "h2_elec_pem", "h2_elec_soe"]``.
    """
    techs_in_scen = set(scenario.set("technology").tolist())
    missing = [p for p in parents if p not in techs_in_scen]
    if missing:
        raise ValueError(
            f"Methanol-addon parents not present in scenario: {missing}. "
            "Run add_hydrogen_techs first."
        )

    history_before = _history_totals(
        scenario.par("historical_activity", filters={"technology": parents})
    )

    _ensure_split_modes_in_set(scenario)

    for parent in parents:
        _broadcast_parent_modes(scenario, parent)

    history_after = scenario.par("historical_activity", filters={"technology": parents})
    restored = _restored_history_rows(history_after, history_before)
    if not restored.empty:
        scenario.add_par("historical_activity", restored)
        log.info(
            "Rescaled %d split historical_activity rows to preserve pre-parity "
            "mode totals",
            len(restored),
        )
    _verify_history_restored(
        history_before,
        scenario.par("historical_activity", filters={"technology": parents}),
    )

    _register_in_map_tec_addon(scenario, parents)

    log.info(
        f"Meth_h2 mode-parity port applied for {len(parents)} parent(s): {parents}"
    )
