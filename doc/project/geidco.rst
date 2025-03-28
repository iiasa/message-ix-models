GEIDCO
******

GEIDCO V
========
For “Phase V”, as of 2025-01:

- Project lead: :gh-user:`vruijven`, :gh-user:`byersiiasa`
- Lead modeler: :gh-user:`adrivinca`

GEIDCO IV
=========
For "Phase IV", from 2022-09 to 2023-06:

- Project lead: :gh-user:`vruijven`
- Lead modeler: :gh-user:`guofei2016` (maintainer: :gh-user:`yiyi1991`)

Introduction
------------
The GEIDCO IV project aims to design and develop two contrasting scenarios: a Business-as-Usual (BAU) scenario and the Global Energy Interconnection (GEI) 1.5°C scenario. 
Under the GEI 1.5°C scenario, the project analyzes global and regional pathways toward carbon peaking and carbon neutrality. 
This includes a detailed assessment of energy demand across regions, sub-sectors, and energy types, with a particular focus on end-use sectors such as industry, transport, and buildings.

A key component of the project is the comprehensive life cycle assessment of core technologies in the new power system, including photovoltaics, wind power, hydro power, ultra-high voltage (UHV) transmission, and energy storage. 
The study quantifies carbon emissions, energy consumption, and key resource requirements for these technologies at the end-use level. 
Furthermore, by integrating the GEI carbon neutrality scenario, the project estimates global installed capacities for photovoltaics, wind power, hydro power, UHV transmission expansion, and energy storage deployment. 
This enables an in-depth evaluation of the carbon and resource footprints of these technologies while considering the geographic distribution of critical resources needed for achieving global carbon neutrality.

Paper
-----
Guo, F., van Ruijven, B.J., Zakeri, B. et al. Implications of intercontinental renewable electricity trade for energy systems and emissions. Nat Energy 7, 1144–1156 (2022). https://doi.org/10.1038/s41560-022-01136-0

Scenario identifier
-------------------
- Model: MESSAGEix-GLOBIOM_GEI_IV
- Scenario (baseline): SSP2_lc_openres_0_V2
- Scenario (mitigation): SSP2_lc_openres_50_V2

Data
----
R11 (original GEIDCO IV):
    stored at the sharepoint, Documents - ECE.prog\\Projects\\GEIDCO_2022-2023\\Model and data

R12 (baseline scenario for GEIDCO V): 
    see message-data branch `project/geidco_r12 <https://github.com/iiasa/message_data/tree/project/geidco_r12>`_

TBA: 
  a generator for more general use in the message-ix-models repo `project/geidco <https://github.com/iiasa/message-ix-models/tree/project/geidco>`_ (generate bare sheets, users fill in minimum techno-economic parameters for UHV technologies, build scenario)

Code references
---------------
TBA: 
    the generator ()
        message_ix_models.tools.generate_interpipe
    the runner (:func: bare, :func: build, :func: solve)
        message_ix_models.project.geidco.run_baseline


