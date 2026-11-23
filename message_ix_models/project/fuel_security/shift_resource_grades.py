# -*- coding: utf-8 -*-
"""
Move NAM gas resource from grade 4 into grade 2 so the cheap grades match proved reserves

In SSP2 v6.6, R12_NAM's cheap gas grades (gas_1-3, var_cost <= ~60) hold ~18,100 GWa,
less than US + Canadian proved reserves, which are economic at current prices by
definition. With resource_remaining = 0.05/yr these small grades hit
RESOURCE_CONSTRAINT immediately and NAM production decays, while the shale volume
sits in the costlier gas_4 (var_cost ~78).

This moves just enough ``resource_volume`` from ``gas_4`` to ``gas_2`` that the cheap
grades sum to proved reserves. The total NAM volume is unchanged. Nothing is moved
if the cheap grades already reach the target, and at most all of gas_4 is moved.

Proved reserves, beginning of 2024 (NRCan Energy Fact Book 2025-2026, "Canada-U.S.
resources"): United States 604 Tcf (EIA) + Canada 196 Tcf (NRCan approximation) =
800 Tcf.
"""
import logging

import message_ix

log = logging.getLogger(__name__)

#: US + Canada proved natural gas reserves, beginning of 2024 (Tcf).
NAM_PROVED_GAS_TCF = 604 + 196

#: Tcf -> GWa on a net calorific basis: 28.317 bcm/Tcf x ~36 PJ/bcm (NCV), matching
#: IEA WEB energy units, which reproduce the model's 2020 gas history exactly.
TCF_TO_GWA = 28.317 * 36_000 * 3.1712e-5  # bcm/Tcf * TJ/bcm * GWa/TJ ~= 32.3


def shift_nam_gas_grades(
    scenario: message_ix.Scenario,
    node: str = "R12_NAM",
    target_tcf: float = NAM_PROVED_GAS_TCF,
    cheap_grades: tuple = ("gas_1", "gas_2", "gas_3"),
    source: str = "gas_4",
    destination: str = "gas_2",
) -> float:
    """
    Move ``resource_volume`` from `source` to `destination` at `node` so that
    `cheap_grades` together hold `target_tcf` of proved reserves.

    Returns:
        The volume moved, in GWa.
    """
    target = target_tcf * TCF_TO_GWA
    rv = scenario.par("resource_volume", filters={"node": [node]})
    rv = rv[rv["commodity"].str.match(r"^gas_\d+$")]
    if rv["commodity"].duplicated().any():
        raise ValueError(f"{node} gas resource_volume has several grades per commodity; "
                         "the shift assumes one")
    vol = rv.set_index("commodity")["value"]
    if source not in vol or destination not in vol:
        raise ValueError(f"{node} resource_volume lacks {source} or {destination}")

    cheap = vol.reindex(list(cheap_grades)).fillna(0).sum()
    shift = min(max(target - cheap, 0.0), vol[source])
    print(f"{node} cheap gas grades {cheap:,.0f} GWa vs proved reserves {target:,.0f} GWa "
          f"({target_tcf:,.0f} Tcf); moving {shift:,.0f} GWa {source} -> {destination} "
          f"({shift / vol[source]:.0%} of {source})")
    if shift == 0:
        return 0.0

    new = rv[rv["commodity"].isin([source, destination])].copy()
    new.loc[new["commodity"] == source, "value"] -= shift
    new.loc[new["commodity"] == destination, "value"] += shift
    with scenario.transact(f"Shift {node} {source} resource into {destination}"):
        scenario.add_par("resource_volume", new)
    return shift
