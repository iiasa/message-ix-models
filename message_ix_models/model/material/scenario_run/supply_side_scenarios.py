"""Infrastructure Supply Side Scenarios"""

_SUPPLY_SCENARIOS = [
"recycling",
"substitution",
"fuel_switching",
"ccs",
"all",
]

def fuel_switch(scen):

    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    # fmy = s_info.y0
    nodes.remove("World")

    # Clinker substituion not allowed
    # CCS is not allowed

    technologies = {'bf_ccs_steel': ['M2'],
                    'dri_gas_ccs_steel': ['M1'],
                    'clinker_wet_ccs_cement': ['M1'],
                    'clinker_dry_ccs_cement': ['M1'],
                    'meth_bio_ccs': ['fuel', 'feedstock'],
                    'meth_coal_ccs': ['fuel', 'feedstock'],
                    'meth_ng_ccs': ['fuel', 'feedstock'],
                    'gas_NH3_ccs': ['M1'],
                    'coal_NH3_ccs': ['M1'],
                    'biomass_NH3_ccs': ['M1'],
                    'fueloil_NH3_ccs': ['M1'],
                    'DUMMY_clay_supply_cement': ['M1']
                    }

    scen.check_out()

    for key, value in technologies.items():
        for n in nodes:
            for y in yva:
                bound_activity = pd.DataFrame({
                     "node_loc": n,
                     "technology": key,
                     "year_act": y,
                     "mode": value,
                     "time": 'time',
                     "value": 0,
                     "unit": t})
                scen.add_par("bound_activity_lo", bound_activity)
                scen.add_par("bound_activity_up", bound_activity)

def only_ccs(scen):
