Emissions and climate
======================

Emissions (energy)
------------------
Carbon-dioxide (CO2)
~~~~~~~~~~~~~~~~~~~~
The MESSAGE model includes a detailed representation of energy-related and land-use CO2 emissions (Riahi and Roehrl, 2000 :cite:`riahi_greenhouse_2000`; Riahi, Rubin et al., 2004 :cite:`riahi_prospects_2004`; Rao and Riahi, 2006 :cite:`rao_role_2006`; Riahi et al., 2011 :cite:`riahi_rcp_2011`). Energy related CO2 mitigation options include technology and fuel shifts; efficiency improvements; and carbon capture. A number of specific mitigation technologies are modeled bottom-up in MESSAGE with a dynamic representation of costs and efficiencies. MESSAGE also includes a detailed representation of carbon capture and sequestration from both fossil fuel and biomass combustion. Land-use CO2 was previously represented using methodology documented in Riahi et al. (2007) :cite:`riahi_scenarios_2007` but is currently updated based on information from the GLOBIOM model.

Non-CO2 GHGs
~~~~~~~~~~~~~~~~
MESSAGE includes a representation of non-CO2 GHGs (CH4, N2O, HFCs, SF6, PFCs) mandated by the Kyoto Protocol (Rao and Riahi, 2006 :cite:`rao_role_2006`). Included is a representation of emissions and mitigation options from both energy related processes as well as non-energy sources like livestock, municipal solid waste disposal, manure management, fertilizer use, rice cultivation, wastewater, and crop residue burning.

Air pollution
~~~~~~~~~~~~~~
Air pollution implications are derived with the help of the GAINS (Greenhouse gas–Air pollution INteractions and Synergies) model. GAINS allows for the development of cost-effective emission control strategies to meet environmental objectives on climate, human health and ecosystem impacts until 2030 (Amann et al., 2011). These impacts are considered in a multi-pollutant context, quantifying the contributions of sulfur dioxide (SO2), nitrogen oxides (NOx), ammonia (NH3), non-methane volatile organic compounds (VOC), and primary emissions of particulate matter (PM), including fine and coarse PM as well as carbonaceous particles (BC, OC). As a stand-alone model, it also tracks emissions of six greenhouse gases of the Kyoto basket. The GAINS model has global coverage and holds essential information about key sources of emissions, environmental policies, and further mitigation opportunities for about 170 country-regions. The model relies on exogenous projections of energy use, industrial production, and agricultural activity for which it distinguishes all key emission sources and several hundred control measures. GAINS can develop finely resolved mid-term air pollutant emission trajectories with different levels of mitigation ambition (Cofala et al., 2007; Amann et al., 2013). The results of such scenarios are used as input to global IAM frameworks to characterize air pollution trajectories associated with various long-term energy developments (see further for example Riahi et al., 2012; Rao et al., 2013).

F-gases
~~~~~~~~~~~~~

**Overview of F-Gas Representation in MESSAGE GLOBIOM**

MESSAGE-GLOBIOM models the following *HFC-sources*, details of which can be found in the EPA report (EPA, 2013).  For each of the different sources, the main driver to which the source is linked within the model has been listed along with any mitigation option available. Mitigation options are based on information provided by the EPA report (**EPA, ????**).

* Solvents
  * Driver: Population
* Fire Extinguishers 
  * Driver: Population 
* Aerosols from Medical Use (MDI)
  * Driver: Population 
* Aerosols from Non-Medical Use (Non-MDI)
  * Driver: Population 
* Refrigeration & Air-Conditioning from Residential and Commercial Sector
  * Driver: Residential & commercial specific demand (mainly electricity)
  * Mitigation: Refrigerant recovery (refrigerant_recovery bounded by technical applicability)
  * Mitigation: Leak repair (leak_repair bounded by technical applicability)
  * Mitigation: Ammonia secondary loop (ammonia_secloop bounded by technical applicability)
* Air-Conditioning from Transport Sector 
  * Driver: Transport demand  
  * Mitigation: Transcritical vapor cycle CO2 systems (mvac_co2 bounded by technical applicability)
* Foams
  * Driver: Residential & commercial thermal demand
  * Mitigation: Replacement with HC (repl_hc bounded by technical applicability)

MESSAGE-GLOBIOM further models *SF6* including the following sources:

* Electrical Equipment
  * Driver: Electricity transmission and distribution
  * Mitigation: Recycling of gas carts (recycling_gas1)
  * Mitigation: Leak repairs (leak_repairsf6)
* Magnesium
  * Driver: Transportation demand
  * Mitigation: Replacement of SF6 by SO2 (replacement_so2)

MESSAGE-GLOBIOM also models *CF4*:

* Aluminum
  * Driver: Transportation demand
  * Mitigation: Retrofit of soderberg process
* Semi-Conductor Production
  * Driver: **fixed output based on …**

**Sources used to derive historical numbers and methods applied to develop future trajectories**

SF6 is associated with two main sources.  SF6 from semiconductor production used in electrical equipment manufacturing, currently making up the bulk (88% in 2010) of total SF6 emissions.  SF6 from magnesium used in the car industry is the second notable source.  The historical data, up to 2010, comes from EDGAR (EDGAR4.2, 2011). Alternatively, EPA data could also be used, which equally offers country based data split out for the two sources mentioned above. 

Future developments in SSP2, from 2020 onwards, foresee a 1% and 1.5% annual intensity decline for SF6 from magnesium use in manufacturing processes and electrical equipment manufacturing respectively.  The intensity rate for SF6 from magnesium use in manufacturing processes declines 1.5% and 0.5% in SSP1 and SSP3 respectively.  For SF6 from electrical equipment manufacturing, the intensity declines at a rate of 2% annually while in SSP3 the rate of decline is lower, at 1% annually.

`EPA <http://www.epa.gov/climatechange/EPAactivities/economics/nonco2projections.html>`_ data, broken down onto the eleven regions depicted in the MESSAGE-GLOBIOM model, is used to represent the historical developments of HFCs and includes projection data up to 2020.  For the remainder of the modelling timeframe (up to 2100), the intensity remains unchanged for the following sources, scaled only with the development of the underlying driver.

* Foam
* Solvent
* Aerosol MDI
* Aerosol Non-MDI
* Fire Extinguishers

Exceptions have been made for:
* AC from Transport Sector 
* Refrigeration & AC from Residential and Commercial Sector

The mitigation potentials remain unchanged across the different SSPs, as these are bound by the technical feasibility (Rao S., Riahi K., 2006).  A further improvement could foresee adaptations of these bounds across the SSP to better reflect the storylines as well as to update the MACs to reflect numbers from the latest EPA report (EPA, 2013).

In the current version of MESSAGE-GLOBIOM, for the above mentioned time-frame, the regional absolute HFC values from the data-source and the historical development of the respective drivers are used to derive a coefficient representing the HFC intensity.


Climate
------------
The response of the carbon-cycle and climate to anthropogenic climate drivers is modelled with the MAGICC model (Model for the Assessment of Greenhouse-gas Induced Climate Change). MAGICC is a reduced-complexity coupled global climate and carbon cycle model which calculates projections for atmospheric concentrations of GHGs and other atmospheric climate drivers like air pollutants, together with consistent projections of radiative forcing, global annual-mean surface air temperature, and ocean-heat uptake (Meinshausen et al., 2011a). MAGICC is an upwelling-diffusion, energy-balance model, which produces outputs for global- and hemispheric-mean temperature. Here, MAGICC is used in a deterministic setup (Meinshausen et al., 2011b), but also a probabilistic setup (Meinshausen et al., 2009) has been used earlier with the IIASA IAM framework (Rogelj et al., 2013a; Rogelj et al., 2013b; Rogelj et al., 2015). Climate feedbacks on the global carbon cycle are accounted for through the interactive coupling of the climate model and a range of gas-cycle models.

The HFC intensity of the transport sector remains unchanged for all regions across SSP2 and SSP3 from 2020 onwards, with exception of Western Europe (WEU) and Eastern Europe (EEU), where the current legislation in line with the Montreal Protocol would see a phase-out of HFC use in mobile AC by 2020.  This exception also applies for SSP1.  For the remaining regions, the assumption is made in SSP1 that there is a saturation of AC use in the transport sector due to the increased awareness and legislative intervention, thus leading to a reduction of the intensity by 50% until 2100.  Further, in SSP1, there is a lower share of individual-conventional transport in comparison with other SSPs, which leads to overall lower mobile AC requirements. OECD countries start this transition in 2030, the Reforming Economies by 2040 and the remaining regions following as of 2050.  This implies that those countries starting at a later point in time profit from experience in other more advanced parts of the world therefore allowing them to improve at a higher rate.

As for refrigeration and air-conditioning of the residential and commercial sector, it is assumed that regions will converge towards a certain intensity level based on their income development.  The point of convergence is defined by the intensity level attained by the designated frontier region in 2020.

For SSP1, Western Europe is the frontier region, whereas the USA, is the frontier for SSP3.  For SSP2, a mixture of the two is used.  The diagrams below (**Figure 1**) illustrate how regions converge towards the designated frontier region over time.

**FIGURE**

The SSP storylines and the therewith associated income developments lead to very different convergence time points.  In SSP1, income grows very rapidly in developing, therefore leading to a convergence of intensity levels for almost all regions by middle of the century, with the exception of Africa which converges by 2080.  In SSP2, all regions converge latest by the end of the century which is very different to SSP3, where the convergence is much slower due to the low income level developments.  In SSP3, only few regions converge by the end of the century.

