from message_ix import make_df

from message_ix_models.util import broadcast


def relax_steel_constraints(scen) -> None:
    bof_dims = {
        "node_loc": ["R12_AFR", "R12_EEU", "R12_LAM", "R12_MEA", "R12_NAM"],
        "technology": "bof_steel",
        "year_act": 2100,
        "time": "year",
        "unit": "???",
    }
    bf_bio_dims = {
        "node_loc": [
            "R12_CHN",
            "R12_FSU",
            "R12_PAO",
            "R12_PAS",
            "R12_RCPA",
            "R12_SAS",
            "R12_WEU",
        ],
        "technology": "bf_biomass_steel",
        "time": "year",
        "unit": "???",
    }
    gro = make_df("growth_activity_up", **bof_dims, value=0.1)
    ini = make_df("initial_activity_up", **bof_dims, value=0.5)
    gro_bio = make_df("growth_activity_up", **bf_bio_dims, value=0.1).pipe(
        broadcast, year_act=[2080, 2090, 2100]
    )
    with scen.transact():
        scen.add_par("growth_activity_up", gro)
        scen.add_par("growth_activity_up", gro_bio)
        scen.add_par("initial_activity_up", ini)


if __name__ == "__main__":
    relax_steel_constraints([])
