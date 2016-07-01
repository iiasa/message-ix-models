Emissions from land (GLOBIOM)
----

Crop sector emissions
~~~~
Crop emissions sources accounted in the paper are N2O fertilization emissions, from synthetic fertilizer and from organic fertilizers, as well as CH4 methane emissions from rice cultivation. Synthetic fertilizers are calculated on a Tier 1 approach, using the information provided by EPIC on the fertilizer use for each management system at the Simulation Unit level and applying the emission factor from IPCC AFOLU guidelines. Synthetic fertilizer use is therefore built in a bottom up approach, but upscaled to the International Fertilizer Association statics on total fertilizer use per crop at the national level for the case where calculated fertilizers are found too low at the aggregated level. This correction ensures a full consistency with observed fertilizer purchases. In the case of rice, we only apply a Tier 1 approach, with a simple formula where emissions are proportional to the area of rice cultivated. Emission factor is taken from EPA (EPA 2012 :cite:`environmental_protection_agency_epa_US_2012`).

Livestock emissions
~~~~
In GLOBIOM, we assign the following emission accounts to livestock directly: CH4 from enteric fermentation, CH4 and N2O from manure management, and N2O from excreta on pasture (N2O from manure applied on cropland is reported in a separate account linked to crop production). In brief, CH4 from enteric fermentation is a simultaneous output of the feed-yield calculations done with the RUMINANT model, as well as nitrogen content of excreta and the amount of volatile solids. The assumptions about proportions of different manure management systems, manure uses, and emission coefficients are based on detailed literature review. Detailed description of how these coefficients have been determined including the literature review is provided in (Herrero, Havlik et al. 2013 :cite:`herrero_global_2013`).

Land use change emissions
~~~~
Land use change emissions are computed based on the difference between initial and final land cover equilibrium carbon stock. For forest, above and below-ground living biomass carbon data are sourced from (Kindermann, Obersteiner et al. 2008 :cite:'kindermann_global_2008`), where geographically explicit allocation of the carbon stocks is provided. The carbon stocks are consistent with the 2010 Forest Assessment Report (FAO 2010 :cite:`food_and_agricultural_organization_fao_global_2010`). Therefore, our emission factors for deforestation are in line with those of FAO. Additionally, carbon stock from grasslands and other natural vegetation is also taken into account using the above and below ground carbon from the biomass map from (Ruesch and Gibbs 2008 :cite:`ruesch_new_ipcc_2008`). When forest or natural vegetation is converted into agricultural use, we consider in our approach that all below and above ground biomass is released in the atmosphere. However, we do not account for litter, dead wood and soil organic carbon.

Comparison with other literature
~~~~
In order to put our numbers in perspective with other sources we compared them with FAO (Tubiello, Salvatore et al. 2013 :cite:`tubiello_faostat_2013`) where a simple but transparent approach is used, largely relying on FAOSTAT activity numbers and IPCC Tier 1 emission coefficients (see Table 1).

Our 2000 data for crops are overall about 11% higher than Tubiello et al., mainly because of rice where we are closer to EPA (EPA 2012 :cite:`environmental_protection_agency_epa_US_2012`) which is higher than Tubiello et al. For livestock, we are by some 18% lower than Tubiello et al. So in total we have about 10% GHG emissions less in 2000 than the values reported. The year 2010 is already the result of simulations and hence may be interesting to compare with the data. In order to facilitate the comparison, we have included the columns e), f) and g) in Table 1. Columns e) and f) compare GLOBIOM data for 2000 and projections for 2010 respectively, with numbers reported by Tubiello et al. Column g) compares the relative change in emissions between 2000 and 2010 from these two sources (1.00 would indicate the same relative change in GLOBIOM and in Tubiello et al.). We can see that the relative change in total agricultural emissions in GLOBIOM is the same as the development reported by Tubiello et al. – an increase by 11%. The behavior of GLOBIOM is over this period very close to the reported trends also at the level of individual accounts. The only exception is emissions from manure management where the relative change projected in GLOBIOM is by 13% higher than the relative change observed in the Tubiello’s numbers. 

.. _tab-globff:
.. list-table:: Comparison of agricultural GHG emissions from GLOBIOM and from FAO for the years 2000 and 2010
   :header-rows: 3

   * -
     - 1 GLOBIOM
     -
     - 2 Tubiello et al.
     -
     -
     - 1 / 2
     -
   * -
     - a)
     - b)
     - c)
     - d)
     - e)
     - f)
     - g)
   * - 
     - 2000
     - 2010
     - 2000
     - 2010
     - 2000
     - 2010
     - 2010/2000
   * - Crops 
     - 1,239
     - 1,365
     - 1,114
     - 1,298
     - 1.11
     - 1.05
     - 0.95
   * - Synthetic fertilizer
     - 522
     - 640
     - 521
     - 683
     - 1.00
     - 0.94
     - 0.93
   * - Manure applied
     - 83
     - 96
     - 103
     - 116
     - 0.81
     - 0.83
     - 1.03
   * - Rice
     - 633
     - 629
     - 490
     - 499
     - 1.29
     - 1.26
     - 0.98
   * - Livestock
     - 2,362
     - 2,625
     - 2,893
     - 3,135
     - 0.82
     - 0.84
     - 1.03
   * - Enteric fermentation
     - 1,502
     - 1,661
     - 1,863
     - 2,018
     - 0.81
     - 0.82
     - 1.02
   * - Manure on pastures
     - 403
     - 441
     - 682
     - 764
     - 0.59
     - 0.58
     - 0.98
   * - Manure management
     - 457
     - 524
     - 348
     - 353
     - 1.31
     - 1.48
     - 1.13
   * - Total Agriculture
     - 3,601
     - 3,991
     - 4,007
     - 4,433
     - 0.90
     - 0.90
     - 1.00
