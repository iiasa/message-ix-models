F-gases
-------
Overview
~~~~~~~~
MESSAGE models the following *HFC-sources*, details of which can be found in the EPA report (EPA, 2013 :cite:`environmental_protection_agency_epa_global_2013`).  For each of the different sources, the main driver to which the source is linked within the model has been listed along with any mitigation option available. Mitigation options are based on information provided by the EPA report (**EPA, ????**).

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

MESSAGE further models *SF6* including the following sources:

* Electrical Equipment

  * Driver: Electricity transmission and distribution
  * Mitigation: Recycling of gas carts (recycling_gas1)
  * Mitigation: Leak repairs (leak_repairsf6)

* Magnesium

  * Driver: Transportation demand
  * Mitigation: Replacement of SF6 by SO2 (replacement_so2)

MESSAGE also models *CF4*:

* Aluminum

  * Driver: Transportation demand
  * Mitigation: Retrofit of soderberg process

* Semi-Conductor Production

  * Driver: **fixed output based on …**

Data sources and methods
~~~~~~~~~~~~~~~~~~~~~~~~

SF6 is associated with two main sources. SF6 from semiconductor production used in electrical equipment manufacturing, currently making up the bulk (88% in 2010) of total SF6 emissions. SF6 from magnesium used in the car industry is the second notable source.  The historical data, up to 2010, comes from EDGAR (EDGAR4.2, 2011 :cite:`joint_research_centre_global_emissions_emission_2011`). Alternatively, EPA data could also be used, which equally offers country based data split out for the two sources mentioned above.

Future developments in SSP2, from 2020 onwards, foresee a 1% and 1.5% annual intensity decline for SF6 from magnesium use in manufacturing processes and electrical equipment manufacturing respectively. The intensity rate for SF6 from magnesium use in manufacturing processes declines 1.5% and 0.5% in SSP1 and SSP3 respectively. For SF6 from electrical equipment manufacturing, the intensity declines at a rate of 2% annually while in SSP3 the rate of decline is lower, at 1% annually.

`EPA <http://www.epa.gov/climatechange/EPAactivities/economics/nonco2projections.html>`_ data, broken down onto the eleven regions depicted in the MESSAGE-GLOBIOM model, is used to represent the historical developments of HFCs and includes projection data up to 2020.  For the remainder of the modelling timeframe (up to 2100), the intensity remains unchanged for the following sources, scaled only with the development of the underlying driver.

* Foam
* Solvent
* Aerosol MDI
* Aerosol Non-MDI
* Fire Extinguishers

Exceptions have been made for:

* AC from Transport Sector
* Refrigeration & AC from Residential and Commercial Sector

The mitigation potentials remain unchanged across the different SSPs, as these are bound by the technical feasibility (Rao S., Riahi K., 2006 :cite:`rao_role_2006`).  A further improvement could foresee adaptations of these bounds across the SSP to better reflect the storylines as well as to update the MACs to reflect numbers from the latest EPA report (EPA, 2013 :cite:`environmental_protection_agency_epa_global_2013`).

In the current version of MESSAGE-GLOBIOM, for the above mentioned time-frame, the regional absolute HFC values from the data-source and the historical development of the respective drivers are used to derive a coefficient representing the HFC intensity.

The HFC intensity of the transport sector remains unchanged for all regions across SSP2 and SSP3 from 2020 onwards, with exception of Western Europe (WEU) and Eastern Europe (EEU), where the current legislation in line with the Montreal Protocol would see a phase-out of HFC use in mobile AC by 2020.  This exception also applies for SSP1.  For the remaining regions, the assumption is made in SSP1 that there is a saturation of AC use in the transport sector due to the increased awareness and legislative intervention, thus leading to a reduction of the intensity by 50% until 2100.  Further, in SSP1, there is a lower share of individual-conventional transport in comparison with other SSPs, which leads to overall lower mobile AC requirements. OECD countries start this transition in 2030, the Reforming Economies by 2040 and the remaining regions following as of 2050.  This implies that those countries starting at a later point in time profit from experience in other more advanced parts of the world therefore allowing them to improve at a higher rate.

As for refrigeration and air-conditioning of the residential and commercial sector, it is assumed that regions will converge towards a certain intensity level based on their income development.  The point of convergence is defined by the intensity level attained by the designated frontier region in 2020.

For SSP1, Western Europe is the frontier region, whereas the USA, is the frontier for SSP3.  For SSP2, a mixture of the two is used.  The diagrams below (:numref:`fig-hfcint`) illustrate how regions converge towards the designated frontier region over time.

.. _fig-hfcint:
.. figure:: /_static/regional_HFC_intensity.png

   Regional HFC Intensity Developments for Refrigeration and Air-Conditioning in the Residential and Commercial Sector across the SSPs

The SSP storylines and the therewith associated income developments lead to very different convergence time points. In SSP1, income grows very rapidly in developing, therefore leading to a convergence of intensity levels for almost all regions by middle of the century, with the exception of Africa which converges by 2080. In SSP2, all regions converge latest by the end of the century which is very different to SSP3, where the convergence is much slower due to the low income level developments. In SSP3, only few regions converge by the end of the century.

HFC-23
~~~~~~

When comparing the data used in MESSAGE-GLOBIOM with the original data source (both from the EPA), there seems to be a discrepancy. The data currently used in MESSAGE-GLOBIOM shows that in 2010, global HFC emissions add up to approximately 555 MtCO2equivalent across all sources. The raw data from the EPA shows only 442 MtCO2equivalent for the same time period. The difference equates to approximately 113 MtCO2equvalent, similar to what is quoted for HFC-23 from HCFC-22 production by the EPA.  This means, that the current totals used in MESSAGE-GLOBIOM are not far off; global values from the two data sources are shown in :numref:`tab-hfcsource`.

.. _tab-hfcsource:
.. list-table:: HFCs by source in [MtCO2e]
   :widths: 26 26 26
   :header-rows: 1

   * - [MtCO2e]
     - MESSAGE-GLOBIOM
     - EPA 2012 (raw)
   * - Ref AC
     - 392.1
     - 349.3
   * - Foams
     - 36.7
     - 21.7
   * - Solvents
     - 58.6
     - 5.2
   * - Aerosols
     - 54.2
     - 45.5
   * - Fire extinguishers
     - 13.9
     - 21.2
   * - HCF-23
     -
     - 128.0
   * - **Totals**
     - 555.6
     - 570.8

A possible explanation could be, that the differences have occurred due to a distribution of emissions associated with HFC-23 across the various sectors.  But a closer look at the regions shows, that the differences between the two data sets do not show any resemblance of the regionally reported values for HFC-23.  Some slight variations could occur from different regional aggregations, but these should not be too substantial.  In some cases, regional variations are due to higher raw data values whereas if the redistribution of HFC-23 were to explain the difference, then raw data values would have to be lower than the current data used in MESSAGE-GLOBIOM across all regions.

Further, HFC-23 emissions from HCFC-22 production amount to approximately 128MtCO2e according to the EPA (EPA, 2013 :cite:`environmental_protection_agency_epa_global_2013`).  EDGAR numbers show that 259 MtCO2e of HFC-23 are emitted in 2010, a stark difference to the EPA numbers.

`EPA reports <http://www.epa.gov/methane/pdfs/fulldocumentofdeveloped.pdf>`_ explain that HFC-23 emissions result from semiconductor production and are a byproduct of HCFC-22 production – used in part for refrigeration and air-conditioning as well as a feedstock for the production of synthetic polymers.  A large surge is to be expected in HFC-23 emissions from feedstock production (EPA, 2013 :cite:`environmental_protection_agency_epa_global_2013`), which is currently not regulated, while dispersive uses will be phased out in accordance with the Montreal protocol.  Comparatively, EDGAR data shows that HFC-23 comes from the production of halocarbons and SF6 (98%), Other F-Gases (1.3%) and minimal amounts from semiconductor and electronic manufacturing.

Conclusions:

1.	HFC aggregates from the raw 2012 EPA data should be used in MESSAGE-GLOBIOM rather than the currently used pre-aggregated data.
2.	HFC-23 from HCFC-22 production should be modelled separately, which would also allow specific emission reduction technologies to be depicted as described in the report by the EPA (EPA, 2013 :cite:`environmental_protection_agency_epa_global_2013`).

Similar to HFC-23, EPA data also breaks out HFCs from semiconductor manufacturing, a category which is currently neglected in MESSAGE-GLOBIOM, not being such a significant contributor towards total HFCs, but which could be easily integrated analogue to SF6 from semiconductor production.

Distribution of HFCs onto HFC compound Groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For reporting purposes, but more importantly for use in calculating the climate impacts (MAGICC6), developments of the different HFC compounds are required.  Ideally, CF4, C2F6, C6F14, HFC23, HFC32, HFC43-10, HFC125, HFC134a, HFC143a, HFC227ea, HFC245fa and SF6 are to be reported directly into the MAGICC input file (GAS.SCEN).  MESSAGE-GLOBIOM models F-gas developments, with the exception of SF6 and CF4, in HFC-134aequivalent.

From the literature, only few sources provide some orientation for deriving such a split.  Below is a table (:numref:`fig-hfcsec`) which summarizes how many of the four available sources agree on which compound comes from the different sectors.  Although EDGAR seems to be an obvious first choice to derive this split, due to the level of regional details included in their historical data on the different HFC compounds, a split of sources is only available for HFC-134a and HFC-23.  Sources included below are therefore limited to Ashford et. al, 2004, Velders et. al, 2009, UNEP Ozone Secretariat, 2015, Harnisch et. al, 2009, whereby not each of these include details for all sectors/compounds and only in a few cases are actual distributions in the form of shares (%) detailed.

Based on the above sources, :numref:`fig-hfcshare` shows available shares suggested by the various data sources.  An “X” marks where no further details are available and where assumptions need to be made.

Finally, :numref:`fig-hfcglob` is an attempt to use the available information, with assumptions made where no data on the split is available, to allocate the total HFCs per sector onto the different compounds.  The resulting sums for the individual compounds have been compared to other data sets.


.. _fig-hfcsec:
.. figure:: /_static/Sources_HFC.png

   Sources indicating which HFC compound results from which sector/activity

.. _fig-hfcshare:
.. figure:: /_static/Shares_HFC.png

   Available shares (ranges) for HFC compound distribution/activity per sector

.. _fig-hfcglob:
.. figure:: /_static/global_HFC.png

   Assumed shares and globally resulting HFC compound distribution. *For comparability, totals do not include HFC-23.*
