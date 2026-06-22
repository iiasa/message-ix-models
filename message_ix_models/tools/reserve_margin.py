"""Update reserve margin (peak load factor) for electricity supply.

Moved from ``message_data.scenario_generation.reserve_margin.res_marg``.

Reference: Johnsonn et al. (2017), doi: 10.1016/j.eneco.2016.07.010,
section 2.2.1 (Firm capacity requirement).
"""


def update(scen, contin=0.2):
    """Update regional reserve margin values based on electricity demand.

    Parameters
    ----------
    scen : message_ix.Scenario
    contin : float
        Backup capacity for contingency as fraction of peak capacity (default 0.2).
    """
    scen.check_out()

    demands = scen.par("demand")
    demands = (
        demands[demands.commodity.isin(["i_spec", "rc_spec"])]
        .set_index(["node", "commodity", "year", "level", "time", "unit"])
        .sort_index()
    )
    input_eff = (
        scen.par("input", {"technology": ["elec_t_d"]})
        .set_index([
            "node_loc", "year_act", "year_vtg", "commodity", "level", "mode",
            "node_origin", "technology", "time", "time_origin", "unit",
        ])
        .sort_index()
    )

    for reg in demands.index.get_level_values("node").unique():
        if "_GLB" in reg:
            continue
        for year in demands.index.get_level_values("year").unique():
            rc_spec = demands.loc[reg, "rc_spec", year, "useful", "year"]["value"].iloc[0]
            i_spec = demands.loc[reg, "i_spec", year, "useful", "year"].iloc[0]
            inp = input_eff.loc[
                reg, year, year, "electr", "secondary", "M1",
                reg, "elec_t_d", "year", "year",
            ].value.iloc[0]

            val = (
                ((i_spec * 1.0 + rc_spec * 2.0) / (i_spec + rc_spec))
                * (1.0 + contin)
                * inp
                * -1.0
            )
            scen.add_par(
                "relation_activity",
                {
                    "relation": ["res_marg"],
                    "node_rel": [reg],
                    "year_rel": [year],
                    "node_loc": [reg],
                    "technology": ["elec_t_d"],
                    "year_act": [year],
                    "mode": ["M1"],
                    "value": [val],
                    "unit": ["GWa"],
                },
            )

    scen.commit("Update reserve margin")
