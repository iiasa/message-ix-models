"""
Mappings of process heat technologies and levels to their respective sectors.
Currently only used to define commodity share constraints for industry sectors
in share_constraints.py.
"""

non_foil_ind_tecs_ht = {
    # Cement industry
    "cement": [
        "furnace_elec_cement",
        "furnace_loil_cement",
        "furnace_biomass_cement",
        "furnace_ethanol_cement",
        "furnace_methanol_cement",
        "furnace_gas_cement",
        "furnace_coal_cement",
        "furnace_h2_cement",
        "furnace_coke_cement",
    ],
    # Aluminum industry
    "aluminum": [
        "furnace_coal_aluminum",
        "furnace_elec_aluminum",
        "furnace_loil_aluminum",
        "furnace_ethanol_aluminum",
        "furnace_biomass_aluminum",
        "furnace_methanol_aluminum",
        "furnace_gas_aluminum",
        "furnace_h2_aluminum",
    ],
    # High Value Chemicals
    "petro": [
        "furnace_coke_petro",
        "furnace_coal_petro",
        "furnace_elec_petro",
        "furnace_loil_petro",
        "furnace_ethanol_petro",
        "furnace_biomass_petro",
        "furnace_methanol_petro",
        "furnace_gas_petro",
        "furnace_h2_petro",
    ],
    # Resins
    "resins": [
        "furnace_coal_resins",
        "furnace_elec_resins",
        "furnace_loil_resins",
        "furnace_ethanol_resins",
        "furnace_biomass_resins",
        "furnace_methanol_resins",
        "furnace_gas_resins",
        "furnace_h2_resins",
    ],
    # Refining
    "refining": [
        "furnace_coal_refining",
        "furnace_elec_refining",
        "furnace_coke_refining",
        "furnace_loil_refining",
        "furnace_ethanol_refining",
        "furnace_methanol_refining",
        "furnace_biomass_refining",
        "furnace_gas_refining",
        "furnace_h2_refining",
    ],
}

# Final Energy Industry Electricity Technologies
foil_ind_tecs_ht = {
    "cement": "furnace_foil_cement",
    "aluminum": "furnace_foil_aluminum",
    "petro": "furnace_foil_petro",
    "resins": "furnace_foil_resins",
    "refining": "furnace_foil_refining",
}
levels = {
    "cement": "useful_cement",
    "aluminum": "useful_aluminum",
    "petro": "useful_petro",
    "resins": "useful_resins",
    "refining": "secondary",
}

other_ind_th_tecs = [
    "eth_i",
    "elec_i",
    "loil_i",
    "biomass_i",
    # "meth_i", deactivated in MESSAGEix-Materials
    "gas_i",
    "coal_i",
    "h2_i",
    "h2_fc_I",
    "foil_i",
    "heat_i",
    "hp_el_i",
    "solar_i",
]
