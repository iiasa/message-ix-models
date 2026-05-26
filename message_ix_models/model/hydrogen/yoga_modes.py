"""Make the hyway electrolyser techs compatible with meth_h2's 6 split modes.

Background: Yoga's upstream ``update_meth_h2_modes`` (``message_ix_models.
project.ssp.script.util.functions``) hardcodes ``h2_elec`` as the
methanol_synthesis_addon parent and broadcasts its feedstock/fuel modes into
the six split modes (``{feedstock, fuel}_{bic, dac, fic}``). In the hydrogen
module, ``add_hydrogen_techs`` removes ``h2_elec`` and adds the hyway techs
(h2_elec_alk/pem/soe + h2_pyro_elec + h2_ct), each populated from per-tech
CSVs that still carry the pre-Yoga mode shape: addon_conversion/addon_up in
feedstock/fuel modes, everything else in M1. Without the broadcast, meth_h2
(which already has the 6 split modes upstream) cannot bind to any electrolyser
parent and ``ADDON_ACTIVITY_UP`` collapses — meth_h2 silently zeros.

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


def _ensure_split_modes_in_set(scenario: message_ix.Scenario) -> None:
    existing = set(scenario.set("mode").tolist())
    to_add = [m for m in SPLIT_MODES if m not in existing]
    if to_add:
        scenario.add_set("mode", to_add)
        log.info(f"Added split modes to `mode` set: {to_add}")


def _broadcast_parent_modes(
    scenario: message_ix.Scenario, parent: str
) -> None:
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
        fuel_rows = scenario.par(
            par, filters={"technology": parent, "mode": "fuel"}
        )

        # Yoga pattern: feedstock/fuel → 6 split modes; drop originals.
        for original_mode, original_rows in (
            ("feedstock", feedstock_rows),
            ("fuel", fuel_rows),
        ):
            if original_rows.empty:
                continue
            df = original_rows.copy(deep=True)
            df["mode"] = None
            split_for_orig = [
                f"{original_mode}_{sfx}" for sfx in SPLIT_SUFFIXES
            ]
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
            m1_rows = scenario.par(
                par, filters={"technology": parent, "mode": "M1"}
            )
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

    _ensure_split_modes_in_set(scenario)

    for parent in parents:
        _broadcast_parent_modes(scenario, parent)

    _register_in_map_tec_addon(scenario, parents)

    log.info(
        f"Meth_h2 mode-parity port applied for {len(parents)} parent(s): {parents}"
    )
