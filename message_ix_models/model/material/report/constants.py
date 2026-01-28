reg_map = {
    "China (R12)": "R12_CHN",
    "Rest of Centrally planned Asia (R12)": "R12_RCPA",
    "Former Soviet Union (R12)": "R12_FSU",
    "Latin America (R12)": "R12_LAM",
    "Middle East and Africa (R12)": "R12_MEA",
    "South Asia (R12)": "R12_SAS",
    "Pacific Asia (R12)": "R12_PAS",
    "Western Europe (R12)": "R12_WEU",
    "Eastern Europe (R12)": "R12_EEU",
    "Pacific OECD (R12)": "R12_PAO",
    "Subsaharan Africa (R12)": "R12_AFR",
    "North America (R12)": "R12_NAM",
    "GLB region (R12)": "World",
}
version = "6.5"
med_and_medlow = [
    (f"SSP_SSP2_v{version}", "baseline_DEFAULT_step_14"),  # fmy = 2020
    (f"SSP_SSP2_v{version}", "NPi2030"),  # fmy = 2030
    (f"SSP_SSP2_v{version}", "NPiREF"),  # fmy = 2035; Medium Emissions is clone
    (f"SSP_SSP2_v{version}", "NPiREF_SSP2 - Medium-Low Emissionsf"),  # fmy = 2045
]
low_emi = [
    (f"SSP_SSP1_v{version}", "INDC2030i_uncon"),
    (f"SSP_SSP1_v{version}", "INDC2030i_uncon_SSP1 - Low Emissions"),
    (f"SSP_SSP2_v{version}", "INDC2030i_weak"),  # fmy = 2030
    (f"SSP_SSP2_v{version}", "INDC2030i_weak_SSP2 - Low Emissions_c"),  # fmy = 2035
]
high = [
    (f"SSP_SSP3_v{version}", "baseline_DEFAULT_step_14"),  # fmy = 2020
    (f"SSP_SSP3_v{version}", "baseline"),  # fmy = 2030; High Emissions is clone
    (f"SSP_SSP5_v{version}", "baseline"),  # fmy = 2030; High Emissions is clone
]
low_overshoot = [
    (f"SSP_SSP4_v{version}", "baseline_DEFAULT_step_14"),  # fmy = 2020
    (f"SSP_SSP4_v{version}", "NPi2030"),  # fmy = 2040)]
    (f"SSP_SSP4_v{version}", "NPiREF"),  # fmy = 2040)]
    (f"SSP_SSP5_v{version}", "baseline_DEFAULT_step_14"),  # fmy = 2020
    (f"SSP_SSP5_v{version}", "NPi2030"),  # fmy = 2040)]
    (f"SSP_SSP5_v{version}", "NPiREF"),  # fmy = 2040)]
    (f"SSP_SSP2_v{version}", "NPiREF_SSP2 - Low Overshootf"),  # fmy = 2040)]
    (f"SSP_SSP4_v{version}", "NPiREF_SSP4 - Low Overshootf"),  # fmy = 2040)]
    (f"SSP_SSP5_v{version}", "NPiREF_SSP5 - Low Overshootf"),  # fmy = 2040)]
]
very_low = [
    (f"SSP_LED_v{version}", "baseline_DEFAULT_step_14"),  # fmy = 2020
    (f"SSP_SSP1_v{version}", "baseline_DEFAULT_step_14"),  # fmy = 2020
    (f"SSP_SSP2_v{version}", "INDC2030i"),
    (f"SSP_LED_v{version}", "INDC2030i"),
    (f"SSP_SSP1_v{version}", "INDC2030i"),
    (f"SSP_SSP2_v{version}", "INDC2030i_forever"),
    (f"SSP_LED_v{version}", "INDC2030i_forever"),
    (f"SSP_SSP1_v{version}", "INDC2030i_forever"),
    (f"SSP_SSP2_v{version}", "SSP2 - Very Low Emissions"),  # fmy = 2040)]
    (f"SSP_LED_v{version}", "SSP2 - Very Low Emissions"),  # fmy = 2040)]
    (f"SSP_SSP1_v{version}", "SSP1 - Very Low Emissions"),  # fmy = 2040)]
]

vars = [
    "Primary Energy",
    "Primary Energy|Biomass",
    "Primary Energy|Biomass|Energy Crops",
    "Primary Energy|Biomass|Fuelwood",
    "Primary Energy|Biomass|Modern",
    "Primary Energy|Biomass|Other",
    "Primary Energy|Biomass|Residues",
    "Primary Energy|Biomass|Residues|Crops",
    "Primary Energy|Biomass|Residues|Forest industry",
    "Primary Energy|Biomass|Residues|Logging",
    "Primary Energy|Biomass|Roundwood harvest",
    "Primary Energy|Biomass|Traditional",
    "Primary Energy|Coal",
    "Primary Energy|Gas",
    "Primary Energy|Geothermal",
    "Primary Energy|Hydro",
    "Primary Energy|Non-Biomass Renewables",
    "Primary Energy|Nuclear",
    "Primary Energy|Ocean",
    "Primary Energy|Oil",
    "Primary Energy|Other",
    "Primary Energy|Solar",
    "Primary Energy|Wind",
]
final_model_name = "MESSAGEix-GLOBIOM-GAINS 2.1-M-R12"
