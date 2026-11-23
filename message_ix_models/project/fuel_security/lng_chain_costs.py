# -*- coding: utf-8 -*-
"""
Add LNG liquefaction, regasification and shipping costs

After bilateralization the LNG chain is almost free: ``LNG_prod`` (liquefaction) and
``LNG_regas`` have no costs, only a 9-10% gas loss in liquefaction, and tankers cost
~0.002 per unit of tanker capacity (~$0.5/MMBtu US Gulf Coast to NW Europe). The
calibrated route costs in ``fix_cost`` never apply, because trade technologies have
no capacity. Landed LNG therefore costs barely more than gas at the wellhead.

This adds, uniformly across regions:

- ``var_cost`` on ``LNG_prod``: a liquefaction tolling fee. The feedgas premium in
  US tolling contracts (115% of Henry Hub) is already represented by the input
  coefficient (1.09-1.10), so only the fixed fee is added.
- ``var_cost`` on ``LNG_regas``: a regasification terminal tariff.
- ``var_cost`` on the LNG tanker technologies: raised so US Gulf Coast to NW Europe
  shipping is ~$1/MMBtu on existing tankers (~$1.5-2/MMBtu when new tankers are built).

Costs are in USD_2010/kWa, the MESSAGEix-GLOBIOM convention. 1 kWa = 29.89 MMBtu;
USD_2024 -> USD_2010 with the US GDP deflator (~0.77).

Sources (2024-2026 USD):
- Liquefaction: US long-term tolling fees $2.3-3.2/MMBtu (Reuters/Poten, Venture
  Global-Vitol); Argus indicative US contract uses $3.00. Default $2.75.
- Regasification: NW European terminal breakevens $0.3-0.5/MMBtu (S&P Global
  Platts trader estimates); Zeebrugge regulated tariff ~$0.23/MMBtu; all-in import
  costs incl. boil-off and ETS $0.6-0.9. Default $0.40.
- Shipping: Platts USGC-NWE freight assessments ~$1-3/MMBtu.
"""
import logging

import message_ix
import pandas as pd

log = logging.getLogger(__name__)

#: kWa -> MMBtu (8,760 kWh x 3,412.14 Btu/kWh)
MMBTU_PER_KWA = 29.89

#: USD_2024 -> USD_2010, US GDP implicit price deflator (approx. 96.2 / 125.2)
USD2024_TO_USD2010 = 0.77

LIQUEFACTION_USD_MMBTU = 2.75
REGAS_USD_MMBTU = 0.40

#: Tanker var_cost per unit of tanker capacity (Mt-km). The bilateralize default
#: is 0.002 for every shipped commodity; 0.004 gives ~$1/MMBtu USGC-NWE.
LNG_TANKER_VAR_COST = 0.004
LNG_TANKERS = ("LNG_tanker_LNG", "LNG_tanker_loil")


def usd_mmbtu_to_model(value: float) -> float:
    """Convert USD_2024/MMBtu to USD_2010/kWa."""
    return value * MMBTU_PER_KWA * USD2024_TO_USD2010


def _var_cost_rows(scenario: message_ix.Scenario, technology: str, value: float,
                   unit: str) -> pd.DataFrame:
    """var_cost for every (node, vintage, activity year, mode, time) of `technology`,
    taken from its ``input`` parameter."""
    inp = scenario.par("input", filters={"technology": [technology]})
    if inp.empty:
        raise ValueError(f"{technology} has no input rows; cannot place var_cost")
    keys = ["node_loc", "technology", "year_vtg", "year_act", "mode", "time"]
    df = inp[keys].drop_duplicates()
    return df.assign(value=value, unit=unit)


def add_lng_chain_costs(
    scenario: message_ix.Scenario,
    liquefaction_usd_mmbtu: float = LIQUEFACTION_USD_MMBTU,
    regas_usd_mmbtu: float = REGAS_USD_MMBTU,
    tanker_var_cost: float = LNG_TANKER_VAR_COST,
) -> pd.DataFrame:
    """
    Set ``var_cost`` on LNG_prod, LNG_regas and the LNG tankers, same in all regions.

    Any existing ``var_cost`` on these technologies is replaced. Pass ``None`` for a
    component to leave it unchanged.

    Returns:
        One row per technology: value set and number of rows written.
    """
    # Use the unit label the scenario already uses for var_cost
    ref = scenario.par("var_cost", filters={"technology": ["gas_extr_1"]})
    unit = ref["unit"].iloc[0] if len(ref) else "USD/GWa"

    frames, summary = [], []
    for tec, usd in (("LNG_prod", liquefaction_usd_mmbtu), ("LNG_regas", regas_usd_mmbtu)):
        if usd is None:
            continue
        value = usd_mmbtu_to_model(usd)
        df = _var_cost_rows(scenario, tec, value, unit)
        frames.append(df)
        summary.append(dict(technology=tec, usd_mmbtu=usd, value=value, rows=len(df)))

    if tanker_var_cost is not None:
        for tec in LNG_TANKERS:
            vc = scenario.par("var_cost", filters={"technology": [tec]})
            if vc.empty:
                log.warning(f"{tec} has no var_cost rows; skipped")
                continue
            old = vc["value"].unique()
            frames.append(vc.assign(value=tanker_var_cost))
            summary.append(dict(technology=tec, usd_mmbtu=None, value=tanker_var_cost,
                                rows=len(vc), previous=", ".join(map(str, old))))

    if frames:
        with scenario.transact("Add LNG liquefaction, regas and shipping costs"):
            scenario.add_par("var_cost", pd.concat(frames))

    summary = pd.DataFrame(summary)
    print("LNG chain var_cost (USD_2010/kWa):")
    print(summary.round(3).to_string(index=False))
    return summary
