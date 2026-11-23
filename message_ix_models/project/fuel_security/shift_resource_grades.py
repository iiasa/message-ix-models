# -*- coding: utf-8 -*-
"""
Move NAM gas resource into grade 2 so the cheap grades hold the shale/tight resource

In SSP2 v6.6, R12_NAM's cheap gas grades (gas_1-3, var_cost <= ~60) hold ~18,100 GWa,
while most of the shale and tight gas sits in costlier grades (gas_4 at ~78,
gas_5-7 at >= ~109). With resource_remaining = 0.05/yr, a grade on its
RESOURCE_CONSTRAINT can only produce 5% of what is left each year, so the small
cheap grades decay by ~25% per 5-year period and NAM production collapses.

Keeping ~1,400 GWa/yr from cheap grades through 2060 needs >= 1,400 / 0.05 = 28,000 GWa
remaining at all times plus ~30 years of output, i.e. ~70,000 GWa (~2,170 Tcf). This
matches the US shale + tight gas technically recoverable resource, whose breakeven
costs are in the gas_2-gas_4 range rather than gas_5+.

This moves ``resource_volume`` into ``gas_2`` until the cheap grades hold the target,
drawing from the source grades in order of cost (gas_4, gas_5, gas_6, gas_7). The
total NAM volume is unchanged. Nothing is moved if the cheap grades already reach the
target. A source grade may be emptied: historical_extraction is not used in any
equation and EXT only exists from the first model year, so its extraction
technology is simply unused after 2025.

Sources (NRCan Energy Fact Book 2025-2026, "Canada-U.S. resources", p. 124):
- US shale and tight gas, technically recoverable, year-end 2020: 2,172 Tcf (default)
- US + Canada proved reserves, beginning of 2024: 604 + 196 = 800 Tcf
"""
import logging

import message_ix

log = logging.getLogger(__name__)

#: US shale + tight gas technically recoverable resource, year-end 2020 (Tcf).
US_SHALE_TIGHT_TRR_TCF = 2172

#: US + Canada proved natural gas reserves, beginning of 2024 (Tcf).
NAM_PROVED_GAS_TCF = 604 + 196

#: Tcf -> GWa on a net calorific basis: 28.317 bcm/Tcf x ~36 PJ/bcm (NCV), matching
#: IEA WEB energy units, which reproduce the model's 2020 gas history exactly.
TCF_TO_GWA = 28.317 * 36_000 * 3.1712e-5  # bcm/Tcf * TJ/bcm * GWa/TJ ~= 32.3


def shift_nam_gas_grades(
    scenario: message_ix.Scenario,
    node: str = "R12_NAM",
    target_tcf: float = US_SHALE_TIGHT_TRR_TCF,
    cheap_grades: tuple = ("gas_1", "gas_2", "gas_3"),
    sources: tuple = ("gas_4", "gas_5", "gas_6", "gas_7"),
    destination: str = "gas_2",
) -> float:
    """
    Move ``resource_volume`` from `sources` (in order) to `destination` at `node`
    so that `cheap_grades` together hold `target_tcf`.

    Returns:
        The total volume moved, in GWa.
    """
    target = target_tcf * TCF_TO_GWA
    rv = scenario.par("resource_volume", filters={"node": [node]})
    rv = rv[rv["commodity"].str.match(r"^gas_\d+$")]
    if rv["commodity"].duplicated().any():
        raise ValueError(f"{node} gas resource_volume has several grades per commodity; "
                         "the shift assumes one")
    vol = rv.set_index("commodity")["value"].copy()
    missing = [c for c in (destination, *sources) if c not in vol]
    if missing:
        raise ValueError(f"{node} resource_volume lacks {missing}")

    cheap = vol.reindex(list(cheap_grades)).fillna(0).sum()
    need = max(target - cheap, 0.0)
    print(f"{node} cheap gas grades {cheap:,.0f} GWa vs target {target:,.0f} GWa "
          f"({target_tcf:,.0f} Tcf); need {need:,.0f} GWa")

    moved = 0.0
    for src in sources:
        if need <= 0:
            break
        take = min(need, vol[src])
        print(f"  {src} -> {destination}: {take:,.0f} GWa ({take / vol[src]:.0%} of {src})")
        vol[src] -= take
        vol[destination] += take
        need -= take
        moved += take
    if need > 0:
        log.warning(f"{node}: sources exhausted, {need:,.0f} GWa short of the target")
    if moved == 0:
        return 0.0

    changed = [destination] + [s for s in sources if vol[s] != rv.set_index("commodity").at[s, "value"]]
    new = rv[rv["commodity"].isin(changed)].copy()
    new["value"] = new["commodity"].map(vol)
    with scenario.transact(f"Shift {node} gas resource into {destination}"):
        scenario.add_par("resource_volume", new)
    return moved
