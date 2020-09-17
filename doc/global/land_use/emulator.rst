.. _emulator:

Land-Use Emulator
=================

The land-use emulator refers to a set of land-use scenarios, provided by GLOBIOM, which are integrated into MESSAGE using a dedicated set of equations (add reference to land-use relevant code documentation).  Each land-use scenario is derived as a result of combining varying degrees of available biomass potential (for use in the energy sector) at varying carbon prices.  Each land-use scenario therefore represents a distinct land-use development pathway. The figure below (add link) illustrates the combination of biomass- and carbon prices for which land-use pathways are derived.

.. _fig-Land-Use_Pathway_Scenario_Matrix:
.. figure:: /_static/Land-Use_Pathway_Scenario_Matrix.png
   :width: 800px

   Land-Use Pathway Scenario Matrix.

In their entirety, the various land-use pathways provide MESSAGE with a range of biomass potentials available for energy production at different costs, along with the associated land-use related emissions (CO2, CH4 and N2O). The different carbon prices provide MESSAGE with options of how to mitigate the land-use related GHG emissions. The combination of land-use pathways can therefore be depicted as a trade-off surface, illustrated for SSP2 (add reference) in the figure below (add link). The figure depicts global biomass potentials and respective GHG emissions at different carbon prices cumulated from 2010 to 2100.

.. _fig-CD_Links_SSP2_v2_baseline_Global_LanduseSurface:
.. figure:: /_static/CD_Links_SSP2_v2_baseline_Global_LanduseSurface.png
   :width: 800px

   Land-Use Pathway Trade-Off Surface for SSP2.

Equations and constraints
-------------------------

The land-use pathways are integrated into MESSAGE using a dedicated set of equations (add link to documentation). At the core, these state that the linear combination of land-use pathways must be equal to 1.

:math:`\sum_{s \in S} LAND_{n,s,y} = 1`

In order to correctly represent the transitional dynamics between land-use pathways, such as the rate at which changes in land-use can occur, e.g. the conversion from land-type A to land-type B, additional constraints are required as the underlying dependencies are only represented in the full fletched GLOBIOM model. Based on rates derived from GLOBIOM, for each of the eleven MESSAGE regions, the upscaling of plantation forest area is limited using `DYNAMIC_LAND_TYPE_CONSTRAINT_UP`.
The total area of plantation forest in a given region and time-period is determined, by the shares of available area summed up for other land types in the previous period within that region.

:math:`plantation\_forest_{n,s,y} <= crop\_land_{n,s,y-1} * X_{n} + grass\_land_{n,s,y-1} * Y_{n} + other\_land_{n,s,y-1} * Z_{n}`

The table below shows the corresponding shares for each land type and region. (insert reference to GLBOIOM land type descriptions).

.. _tab-land_type_shares:
.. list-table:: Shares of land-type by region used to derive the growth rate of plantation forest.
   :widths: 20 15 15 15
   :header-rows: 1

   * - Region
     - Crop land [%]
     - Grass land [%]
     - Other land [%]
   * - Sub-Saharan Africa
     - 0.05
     - 0.05
     - 0.05
   * - Centrally Planned Asia and China
     - 0.05
     - 0.05
     - 0.02
   * - Central and Eastern Europe
     - 0.05
     - 0.02
     - 0.02
   * - Former Soviet Union
     - 0.05
     - 0.05
     - 0.02
   * - Latin America and the Caribbean
     - 0.05
     - 0.05
     - 0.05
   * - Middle East and North Africa
     - 0.05
     - 0.05
     - 0.05
   * - North America
     - 0.05
     - 0.05
     - 0.02
   * - Pacific OECD
     - 0.05
     - 0.05
     - 0.05
   * - Other Pacific Asia
     - 0.05
     - 0.05
     - 0.05
   * - South Asia
     - 0.05
     - 0.05
     - 0.05
   * - Western Europe
     - 0.05
     - 0.02
     - 0.02

In addition to constraining the growth of plantation forest (reference to globiom forest type description), the increase of the current forest area ("old forest", add reference) is prohibited. The existing forest area can only be de-forested and afforestation is depicted as another land-use type.

:math:`old\_forest_{n,s,y} <= old\_forest_{n,s,y-1}`

The third and last set of constraints required for the land-use emulator enforce gradual transitions between land-use pathways.  Too rapid switches between land-use pathways, i.e. full transitioning between land-use pathways in adjacent timesteps, can occur for several reasons.  Slight numerical `non-convexities` in input data, i.e. numerical inconsistencies can occur for individual time-steps.  Land-use pathways, cumulatively (across time) depict consistent behaviour i.e. as carbon prices increase, the cumulative emissions decrease within a single biomass potential category (see trade-off surface figure).  Yet for the same carbon price across multiple biomass potential categories, inconsistencies may occur, for example as a result of data scaling or aggregation. (GLOBIOM colleagues may want to expand on this). Without such transitional constraint between pathways, the optimal least-cost solution could be to switch between two land-use pathways for only a single timestep. This could introduce artifacts as a result (e.g. price incosnsistencies). 
As can be seen in the figure above (insert reference to matrix), the carbon price categories have been chosen to span a broad range of mitigation options, with stepped carbon price increases that best reflect increases in global mitigation efforts, while at the same time ensuring that inclusion of the land-use emulator in MESSAGE, does not result in too long solving times. The transitional constraints between pathways further contribute to smoothing the step wise increases between the carbon price categories.
The transition rate has been set, so that land-use pathways can be phased out at a rate of 5% annually.  This value was derived based on a sensitivity analysis, showing that this factor best matched the transition results of the full fletched GLOBIOM model.

Adaptation of the Reference-Energy-System (RES)
-----------------------------------------------

Prior to the use of the land-use emulator, biomass supply curves were used to inform the energy system of the biomass availablity (see REFERENCE GEA?). The incorporation of the land-use emulator, requires two changes to the RES to be undertaken. On the on hand, an additional level/commodity has been introduced to link the land-use pathways with the energy system, while emissions are depicted using the dedicated land-emissions formulation (add reference to GAMS). 

.. _fig-LU_Emulator_adapted_RES:
.. figure:: /_static/Land-Use_Pathway_RES.PNG
   :width: 800px

   Adaptations of a simplified RES for inclusion of the land-use emulator.

Biomass, independent of the type of feedstock, is treated as a single commodity in the energy system. Bioenergy can therefore be used for use in power gernation or liquefaction or gasification process alike (see details on energy system). The only exception is made for non-commercial biomass (fuel wood). Non-commercial biomass supply and demand have been aliogned between the two models.  These are derived based on population and GDP proctions (add reference pachauri).
Note, that because each of the land-use pathways has been calculated accounting for mitigation of all GHGs, MESSAGE scenarios aiming to only reduce a single green-house-gas for example, will either need to account for the fact that a price on CH4 for example will equally result in reductions of CO2 and N2O in the land-use sector.  Equally, other land-use policies, such as the limitation of deforestation, can be implemented, but will most likely include other land-use related trends, which are artifacts as opposed to results of the policy, due to the limitations of using an emulator, and therefore a limited solution space. The land-use pathways are meant to represent the broad, as opposed to a specific policy land-scape, consistent with SSP storylines (see land-use paper of SSPs). For some larger projects or studies, matrixes, i.e. input data sets from GLOBIOM, can be tailored to allow the analysis of specific policies in MESSAGE.

Results and validation
----------------------

The figure below illustrates, based on the land-use pathway trade-off surface, how scenarios navigate throughout the land-use pathways over the course of a scenario. Note that time dependency is not depicted in the figure. The figure consists of four panels, each of which shows the results for scenarios of varying long-term climate mitigation policies. The orange shaded areas represent the choice of land-use pathways combined over time for all regions. The scenarios include a.) a SSP2 based no-policy, baseline scenario, b.) a SSP2 based policy scenario with a cumulative CO2 budget of 1600 GtCO2 (limiting global temperature increase compared to pre-industrial times to approximately 1.9 °C) c.)  a SSP2 based policy scenario with a cumulative CO2 budget of 1000 GtCO2 (limiting global temperature increase compared to pre-industrial times to approximately 1.6 °C) d.) a SSP2 based policy scenario with a cumulative CO2 budget of 400 GtCO2 (limiting global temperature increase compared to pre-industrial times to approximately 1.3 °C). More details on these scenarios can be found here (insert link to CD-Links documentation).

.. _fig-CD_Links_SSP2_v2_Global_LanduseSurface_RESULTS:
.. figure:: /_static/CD_Links_SSP2_v2_Global_LanduseSurface_RESULTS.png
   :width: 800px

   Global land-use pathway choice across CD-Links scenario set.

In the baseline scenario (a), only land-use pathways without a carbon price are used. In the least stringent scenario (b), the carbon price reaches approximately 500$(2005)/tCO2 in 2100 (see figure below). In 2090, the carbon price is well below 450$(2005)/tCO2, hence it is to be expected that no biomass price categories above 225$(2005) i.e. not making use of the next highest carbon price category of GHG400(450$(2005)). In the two stringent scenarios (c and d), the land-use pathways with the highest carbon price, GHG2000 (2256$(2005)/tCO2) are employed. Not visible from the figure is the timing at which the highest carbon price pathways are used. While in scenario (c), the carbon price reaches approximately 1000$(2005)/tCO2 and 1600$(2005)/tCO2 in 2100 and 2110 respectively, the highest price land-use pathways are only partially used in select regions at the very end of the century. The categories which are mostly used are as the GHG1000, (1128$(2005)/tCO2). For scenario (d), where the carbon price breaches the 2000$(2005)/tCO2 barrier already in 2090, the GHG2000 categories are used most commonly and across all regions.

.. _fig-CD_Links_SSP2_v2_Global_LanduseSurface_TEMP-CPRICE:
.. figure:: /_static/CD_Links_SSP2_v2_Global_LanduseSurface_TEMP-CPRICE.png
   :width: 800px

   Temperature and carbon-price development across CD-Links scenario set.

A first validation of the land-use emulator implementation, is performed by setting the carbon price such that a specific mitigation category (GHGXXX) is predominantely used. The figure below (add reference)depicts the results of four validation scenarios.  The carbon prices were set so that the land-use pathway mitigation categories, GHG005, GHG100, GHG400 and GHG1000, are predominantely used across cumulatively across all regions and the entire optimization time-horizon. 

.. _fig-ENGAGE_SSP2_v4.1.2_sens_Global_validation_cprice:
.. figure:: /_static/ENGAGE_SSP2_v4.1.2_sens_Global_validation_cprice.png
   :width: 800px

   Distribution of land-use related carbon price category use for different carbon price levels.

In addition to informing MESSAGE of the biomass and land-use related emission quantities and prices, the land-use input matrix includes information related to land-use by type, production and demand of other non-bioenergy related land produces as well as information on crop-yields, irrigation water-use, amongst others. Region specific quantities of biomass from different feedstocks, the carbon price trajectory as well as GDP developments can be *plugged* back into the full fletched GLOBIOM land-use model. Thus, despite the slightly adjusted results, allows the land-use impacts to be analysed in greater detail. 
Such validation or *feedback* runs were conducted for the shared-socio-economic pathways (reference). The figures below (reference) compares how the emulated results (full lines) for GHG- and CH4 emissions across various scenarios compare with the results of the full fletched GLOBIOM model. The differences in emissions are updated in the original MESSAGE scenario in order to correctly account for changes in atmospheric concentrations.

.. _fig-SSP1_feedback:
.. figure:: /_static/SSP1_feedback.png
   :width: 800px

   SSP1 Emulated land-use results vs. GLOBIOM feedback.
