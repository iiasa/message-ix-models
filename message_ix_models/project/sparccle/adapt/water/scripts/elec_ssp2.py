"""Electricity supply, electricity-supply investment and EMISS totals of the
SSP2 water scenarios (World = sum of R12 nodes). Output: elec_ssp2.csv."""
import ixmp, message_ix, pandas as pd

GWA_TO_EJ = 0.0315576
KEYS = ["maladapt", "+adapt", "BAUfgd", "base"]
OUT = "/home/vinca/sparccle/out/reports/water/elec_ssp2.csv"
mp = ixmp.Platform("sparccle_SSP_SSP2_v6.6_sp_NPiREF_GDP_CI_50_ensemble_2")
rows = []


def add(key, var, unit, series):
    for y, v in series.items():
        rows.append(dict(scenario=key, variable=var, unit=unit, year=int(y), value=float(v)))


for key in KEYS:
    s = message_ix.Scenario(mp, "SSP_SSP2_v6.6_sp", f"CurrentPolicies_SSP2_W_{key}")
    out = s.par("output", filters={"commodity": ["electr"], "level": ["secondary"]})
    out = out[out.node_loc.str.startswith("R12_")]
    gen = sorted(t for t in out.technology.unique()
                 if not any(x in t for x in ("imp", "exp", "stor", "_t_d", "trade")))
    print(key, "generation techs:", gen, flush=True)
    keys5 = ["node_loc", "technology", "year_vtg", "year_act", "mode", "time"]
    o = out[out.technology.isin(gen)].drop_duplicates(keys5)[keys5 + ["value"]]
    act = s.var("ACT", filters={"technology": gen})
    act = act[act.node_loc.str.startswith("R12_")]
    m = act.merge(o, on=keys5)
    m["ej"] = m.lvl * m.value * GWA_TO_EJ
    add(key, "Secondary Energy|Electricity", "EJ/yr", m.groupby("year_act")["ej"].sum())
    for t, g in m.groupby("technology"):
        add(key, f"Secondary Energy|Electricity|{t}", "EJ/yr", g.groupby("year_act")["ej"].sum())
    # investment = CAP_NEW * inv_cost (GW * USD/kW = million USD) -> billion USD
    for label, techs in (("Generation", gen), ("Transmission and Distribution", ["elec_t_d"])):
        cn = s.var("CAP_NEW", filters={"technology": techs})
        cn = cn[cn.node_loc.str.startswith("R12_")]
        ic = s.par("inv_cost", filters={"technology": techs})[["node_loc", "technology", "year_vtg", "value"]]
        mi = cn.merge(ic, on=["node_loc", "technology", "year_vtg"])
        mi["busd"] = mi.lvl * mi.value / 1000
        add(key, f"Investment|Energy Supply|Electricity|{label}", "billion USD/yr", mi.groupby("year_vtg")["busd"].sum())
    em = s.var("EMISS", filters={"type_tec": ["all"]})
    em = em[em.node.str.startswith("R12_")].groupby(["emission", "year"])["lvl"].sum()
    for e in em.index.get_level_values(0).unique():
        add(key, f"EMISS|{e}", "model units", em.loc[e])
    pd.DataFrame(rows).to_csv(OUT, index=False)  # partial file survives an OOM
    del s, act, m, em, cn, mi
    import gc; gc.collect()
    print(key, "done", flush=True)

mp.close_db()
print("done")
