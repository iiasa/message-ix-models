.. _emission_energy:

Emission from energy (MESSAGE)
------------------------------

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



Non-CO2 GHGs
~~~~~~~~~~~~
MESSAGE includes a representation of non-CO2 GHGs (CH4, N2O, HFCs, SF6, PFCs) mandated by the Kyoto Protocol (Rao and Riahi, 2006 :cite:`rao_role_2006`) with the exception of NF3. Included is a representation of emissions and mitigation options from both energy related processes as well as non-energy sources like municipal solid waste disposal and wastewater. CH4 and N2O emissions from land are taken care of by the link to GLOBIOM (see Section :ref:`emission_land`).

.. _gains:

Air pollution
~~~~~~~~~~~~~
Air pollution implications are derived with the help of the GAINS (Greenhouse gas-Air pollution INteractions and Synergies) model. GAINS allows for the development of cost-effective emission control strategies to
meet environmental objectives on climate, human health and ecosystem impacts until 2030 (Amann et al., 2011 :cite:`amann_cost-effective_2011`). These impacts are considered in a multi-pollutant context,
quantifying the contributions of sulfur dioxide (SO2), nitrogen oxides (NOx), ammonia (NH3), non-methane volatile organic compounds (VOC), and primary emissions of particulate matter (PM), including fine
and coarse PM as well as carbonaceous particles (BC, OC). As a stand-alone model, it also tracks emissions of six greenhouse gases of the Kyoto basket with exception of NF3. The GAINS model has global
coverage and holds essential information about key sources of emissions, environmental policies, and further mitigation opportunities for about 170 country-regions. The model relies on exogenous projections
of energy use, industrial production, and agricultural activity for which it distinguishes all key emission sources and several hundred control measures. GAINS can develop finely resolved mid-term air pollutant
emission trajectories with different levels of mitigation ambition (Cofala et al., 2007 :cite:`cofala_scenarios_2007`; Amann et al., 2013 :cite:`amann_regional_2013`). The results of such scenarios are used as
input to global IAM frameworks to characterize air pollution trajectories associated with various long-term energy developments
(see further for example Riahi et al., 2012 :cite:`riahi_chapter_2012`; Rao et al., 2013 :cite:`rao_better_2013`; Fricko et al., 2017 :cite:`fricko_marker_2017`).
