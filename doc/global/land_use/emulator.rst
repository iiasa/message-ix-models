.. _emulator:

Land-Use Emulator
-----------------

The land-use emulator refers to a set of land-use scenarios, provided by GLOBIOM, which are integrated into MESSAGE using a didicated set of equations (add reference to land-use relevant code documentation).  Each land-use scenario represents a combination of biomass potential (for use in the energy sector) and a carbon price.  Each land-use scenario therefore represents a disctinct land-use development pathway. The figure belowillustrates the combination of biomass- and carbon prices for which land-use pathways are available.

.. _fig-Land-Use_Pathway_Scenario_Matrix:
.. figure:: /_static/Land-Use_Pathway_Scenario_Matrix.png
   :width: 800px

   Land-Use Pathway Scenario Matrix.

In their entierty, the various land-use pathways provide MESSAGE with a range of biomass potentials availabe for energy production at different costs, along with the associated land-use related emissions (CO2, CH4 and N2O). The different carbon prices provide MESSAGE with options of how to mitigate the land-use related GHG emissions. The combination of land-use pathways can therefore be depicted as a trade-off surface, illsutrated for SSP2 in the figure below. This represents the cumulative biomass potentials and incurred GHG emissions for different carbon prices for the time period from 2010 to 2100.

.. _fig-CD_Links_SSP2_v2_baseline_Global_LanduseSurface:
.. figure:: /_static/CD_Links_SSP2_v2_baseline_Global_LanduseSurface.png
   :width: 800px

   Land-Use Pathway Trade-Off Surface for SSP2.

The land-use pathways are integrated into MESSAGE using a dedicated set of equations (add link to documentaiton). At the core, these state that the linear combination of land-use pathways must be equal to 1.

:math:`\sum_{s \in S} LAND_{n,s,y} = 1`

In order to correctly represent the transitional dynamics between land-use pathways, such as the rate at which changes in land-use can occur, e.g. the conversion from land-type A to land-type B, additional constraints are required as the underlying dependencies are only represented in the full fletched GLOBIOM model. Based on rates derived from GLOBIOM, for each of the eleven MESSAGE regions, the rate at which plantation forest area can be upscaled is limited using `DYNAMIC_LAND_TYPE_CONSTRAINT_UP`.
For this, shares of the specifc shares of land from one period, determine the possible increase in Mha of plantation forest in the following time period.

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

In addition to constraining the growth of platation forest (reference to globiom forest type description), the increase of old forerst is prohibited.

:math:`old\_forest_{n,s,y} <= old\_forest_{n,s,y-1}`

The third and last constraint required for the land-use emulator is an overall growth constraint for switching from one land-use pathway to another.  This avoids too rapid switches between land-use pathways between adjacent timesteps. Such switches can occur if there are numerical `non-convexities` in input data between pathways.  These can occur for single time-steps and without such a growth constraint the optimizer may choose to fully swtich between two land-use pathways for only a single timestep.  Further, this also contributes to smoothing transitions between carbon price steps.  As can be seen in the figure above (insert reference to matrix), there is only a limited set of carbon price categories.  Hence, should there be no growth constraint, then the optimizer would only choose a certain category, when the carbon price rises above the repsective category price. In GLOBIOM, there is already mitigation at carbon prices between categories,, especially lower pirces, hence the growth constraint will help mimic this behavior.  The growth rate is set to 5% annualy, and was derived based on a senstivity anlysis, showing that factor best matched the transition results of the full fletched GLOBIOM model.  
