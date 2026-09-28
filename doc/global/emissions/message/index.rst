.. _emission_energy:

Emissions from energy (|MESSAGEix|)
-----------------------------------

Carbon-dioxide (CO2)
~~~~~~~~~~~~~~~~~~~~
The MESSAGE model includes a detailed representation of energy-related and - via the link to GLOBIOM - land-use CO2 emissions (Riahi and Roehrl, 2000 :cite:`riahi_greenhouse_2000`; Riahi, Rubin et al., 2004 :cite:`riahi_prospects_2004`; Rao and Riahi, 2006 :cite:`rao_role_2006`; Riahi et al., 2011 :cite:`riahi_rcp_2011`). CO2 emission factors of fossil fuels and biomass are based on the 1996 version of the IPCC guidelines for national greenhouse gas inventories :cite:`ipcc_revised_1996` (see :numref:`tab-emissionfactor`). It is important to note that biomass is generally treated as being carbon neutral in the energy system, because the effects on the terrestrial carbon stocks are accounted for on the land use side, i.e. in GLOBIOM (see section :ref:`globiom`). The CO2 emission factor of biomass is, however, relevant in the application of carbon capture and storage (CCS) where the carbon content of the fuel and the capture efficiency of the applied process determine the amount of carbon captured per unit of energy.

.. _tab-emissionfactor:
.. list-table:: Carbon emission factors used in MESSAGE based on IPCC (1996, Table 1-2 :cite:`ipcc_revised_1996`). For convenience, emission factors are shown in three different units.
   :widths: 20 26 26 26
   :header-rows: 1

   * - Fuel
     - Emission factor [tC/TJ]
     - Emission factor [tCO2/TJ]
     - Emission factor [tC/kWyr]
   * - Hard coal
     - 25.8
     - 94.6
     - 0.814
   * - Lignite
     - 27.6
     - 101.2
     - 0.870
   * - Crude oil
     - 20.0
     - 73.3
     - 0.631
   * - Light fuel oil
     - 20.0
     - 73.3
     - 0.631
   * - Heavy fuel oil
     - 21.1
     - 77.4
     - 0.665
   * - Methanol
     - 17.4
     - 63.8
     - 0.549
   * - Natural gas
     - 15.3
     - 56.1
     - 0.482
   * - Solid biomass
     - 29.9
     - 109.6
     - 0.942

CO2 emissions of fossil fuels for the entire energy system are accounted for at the resource extraction level by applying the CO2 emission factors listed in :numref:`tab-emissionfactor` to the extracted fossil fuel quantities. In this economy-wide accounting, carbon emissions captured in CCS processes remove carbon from the balance equation, i.e. they contribute with a negative emission coefficient. In parallel, a sectoral acounting of CO2 emissions is performed which applies the same emission factors to fossil fuels used in individual conversion processes. In addition to conversion processes, also CO2 emissions from energy use in fossil fuel resource extraction are explicitly accounted for. A relevant feature of MESSAGE in this context is that CO2 emissions from the extraction process increase when moving from conventional to unconventional fossil fuel resources (McJeon et al., 2014 :cite:`mcjeon_gas_2014`).

CO2 mitigation options in the energy system include technology and fuel shifts; efficiency improvements; and CCS. A large number of specific mitigation technologies are modeled bottom-up in MESSAGE with a dynamic representation of costs and efficiencies. As mentioend above, MESSAGE also includes a detailed representation of carbon capture and sequestration from both fossil fuel and biomass combustion (see :numref:`tab_CCScapturerates`).

.. _tab_CCScapturerates:
.. list-table:: Carbon capture rates in [%]
   :widths: 25 45 15
   :header-rows: 1

   * - Conversion Process
     - Plant type
     - Capture rate
   * - Electricity generation
     - supercritical PC power plant with desulphurization/denox and CCS
     - 90%
   * - Electricity generation
     - IGCC power plant with CCS
     - 90%
   * - Electricity generation
     - biomass IGCC power plant with CCS
     - 86%
   * - Liquid fuel production
     - Fischer-Tropsch coal-to-liquids with CCS
     - 85%
   * - Liquid fuel production
     - coal methanol-to-gasoline with CCS
     - 85%
   * - Liquid fuel production
     - Fischer-Tropsch gas-to-liquids with CCS
     - 90%
   * - Liquid fuel production
     - Fischer-Tropsch biomass-to-liquids with CCS
     - 65%
   * - Liquid fuel production
     - Biomass to Gasoline via the Methanol-to-Gasoline (MTG) Process with CCS
     - 67%
   * - Hydrogen production
     - coal gasification with CCS
     - 92%
   * - Hydrogen production
     - biomass gasification with CCS
     - 85%
   * - Hydrogen production
     - steam methane reforming with CCS
     - 90%

.. _emission_energy_nonco2:

Non-CO2 GHGs
~~~~~~~~~~~~

CH4 and N2O emissions from the energy system, industry, product use, waste and wastewater
are represented through implied emission factors and abatement technologies derived from GAINS,
so that they are part of the optimization
(see :ref:`emission_nonco2`).
CH4 and N2O emissions from agriculture and land use come from GLOBIOM
(see :ref:`emission_land`).

F-gases are represented directly in |MESSAGEix|.
HFC emissions from refrigeration and air-conditioning, foams, aerosols, solvents and fire extinguishers
are linked to drivers such as population,
residential and commercial energy demand and transport demand,
with historical intensities based on EPA (2013) :cite:`environmental_protection_agency_epa_global_2013`.
SF6 emissions from electrical equipment and magnesium production
are linked to electricity transmission and distribution and to transport demand.
A small set of abatement options,
such as refrigerant recovery, leak repair and replacement with alternative substances,
is available, with potentials bounded by technical feasibility
(Rao and Riahi, 2006 :cite:`rao_role_2006`).

.. _emission_energy_airpollution:

Air pollution
~~~~~~~~~~~~~

Emissions of sulfur dioxide (SO2), nitrogen oxides (NOx), ammonia (NH3),
non-methane volatile organic compounds (VOC), black carbon (BC) and organic carbon (OC)
are calculated in GAINS after the scenario is solved,
from the scenario activity translated to GAINS resolution
(see :ref:`policy_overview` and :ref:`gains_airpollution`).
