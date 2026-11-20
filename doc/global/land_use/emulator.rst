.. _emulator:

Land-Use Emulator
=================

The land-use emulator integrates a set of land-use scenarios into |name|.
These land-use scenarios are developed by the economic land-use model GLOBIOM (see :ref:`overview`),
which can assess competition for land between agriculture, bioenergy, and forestry.
The land-use scenarios form a two-dimensional scenario matrix
(the so-called `Lookup-Table <https://github.com/iiasa/GLOBIOM-G4M_LookupTable>`_)
combining different carbon and biomass price trajectories.
This allows |name| to represent biomass supply curves conditional on different carbon prices
as well as marginal abatement cost curves conditional on different biomass prices
for the land-use sector.
The linkage between an energy model, here MESSAGEix, and a land-use model is important to explore the potential of bioenergy and the implications of using biomass for energy generation on emissions, the cost of the system, and land use.
In the MESSAGEix formulation, a dedicated set of :ref:`land use equations <message-ix:section_landuse_emulator>` establishes this linkage as follows.

Each land-use scenario represents a distinct land-use development pathway for a given biomass price and carbon price.
The biomass prices are exogenous inputs to the GLOBIOM runs.
They are set at levels chosen so that the range covers the full biomass potential in every region
(see :doc:`/global/energy/resource/bioenergy`).
At each price level, GLOBIOM determines how much biomass is supplied to the energy system
and from which feedstocks,
so that across the price levels the scenarios trace a regional biomass supply curve.
At lower biomass prices, biomass mainly stems from forest residues, for example from sawmills or logging residues.
With increasing prices, land use shifts to make room for short rotation tree plantations grown for energy production.
Through increased competition with agricultural land, this can indirectly cause deforestation of today's forest.
At very high prices, roundwood is harvested for energy production (for further details see :ref:`forestry`), competing with material uses.

Each biomass price level is combined with a range of carbon prices,
which reflect the cost of mitigating land-use related greenhouse gas (GHG) emissions.
Seven biomass price categories (BIO00, BIO03, BIO05, BIO08, BIO13, BIO30 and BIO60)
and twelve carbon price categories (GHG000, GHG010, GHG020, GHG050, GHG100, GHG200, GHG400, GHG600, GHG1000, GHG1500, GHG2000 and GHG3000)
together give 84 land-use scenarios (:numref:`fig-Land-Use_Pathway_Scenario_Matrix`).
In each scenario, both prices rise linearly from 2020 to the level of their category in 2100.
The category names give that level in US$2000, which corresponds to 0.11 to 67.7 US$2005/GJ for biomass
and 11.3 to 3385 US$2005/tCO2 for carbon.
Each scenario carries, per region and period, the land area by cover type,
land-use CO2, CH4 and N2O emissions,
the quantity of biomass supplied to the energy system,
and a cost.

.. _fig-Land-Use_Pathway_Scenario_Matrix:
.. figure:: /_static/emulator_land_scenario_matrix.png
   :width: 800px
   :align: center

   Land-use scenario matrix. Rows are the seven biomass price categories, columns the twelve carbon price categories, labelled with their 2100 price levels in US$2005/GJ and US$2005/tCO2. Each cell is one land-use scenario provided by GLOBIOM. The shading is for orientation only and carries no data.

In their entirety,
the combination of these distinct land-use pathways provides |name| with a range of biomass potentials
available for energy generation at different costs,
the BIO-categories,
along with the associated land-use related emissions (CO2, CH4 and N2O).
The different carbon prices provide |name| with options
for mitigating land-use related GHG emissions,
the GHG-categories.
The combination of land-use pathways can therefore be depicted as a trade-off surface,
illustrated for SSP2 in :numref:`fig-Landuse_Tradeoff_Surface`.
The figure shows global biomass potentials and the corresponding GHG emissions at different carbon prices, cumulated from 2010 to 2100.

.. _fig-Landuse_Tradeoff_Surface:
.. figure:: /_static/emulator_tradeoff_surface.png
   :width: 600px
   :align: center

   Land-use pathway trade-off surface for SSP2. The x-axis shows the 2100 carbon price of each GHG-category in US$2005/tCO2, the y-axis the cumulative biomass potential in ZJ and the z-axis cumulative AFOLU emissions (CO2, CH4 and N2O, AR4 GWP100) in Gt CO2-equiv, both summed over 2010 to 2100 and the 12 regions. Each mesh node is one land-use scenario. The shading follows cumulative emissions.

From the trade-off surface it can be deduced
that in a |name| scenario without climate policy,
land-use pathways of the lower BIO-categories and the lowest GHG-categories will be used.
The energy system will therefore only use biomass for energy production to the extent that it is economically viable without mitigating emissions.
When climate policy scenarios are run in |name|,
the land-use pathways are chosen
such that the optimal balance between land-use related emissions
and biomass use in the energy system is obtained.
In addition to serving as a commodity from which energy can be generated, biomass can also be used to obtain negative emissions via BECCS.

Adaptation of the Reference-Energy-System (RES)
-----------------------------------------------

The emulator incorporates all land-use scenarios in MESSAGEix, so the choice of land-use pathway(s) becomes part of the entire optimization problem.
Conceptually,
each land-use scenario is incorporated similarly to any other technology in |name|,
each providing biomass at a given cost with corresponding GHG emissions.
The incorporation of the land-use emulator requires two changes to the RES.
On the one hand, an additional level and commodity link the land-use pathways with the energy system.
On the other hand, land-use emissions are accounted for directly in the emissions equation (:ref:`emissions equations in MESSAGEix <message-ix:section_emission>`) rather than as a commodity.
The cost of each land-use scenario enters the objective function alongside the costs of the energy system.
Land-use scenarios can also carry demands for commodities that the energy system has to supply.
Currently this is nitrogen fertilizer, and the approach can be extended to other land-related commodities, for example energy use in agriculture.

.. _fig-LU_Emulator_adapted_RES:
.. figure:: /_static/emulator_res_schematic.png
   :width: 800px
   :align: center

   Adaptations of a simplified RES for inclusion of the land-use emulator. Solid lines are commodity flows, dashed lines emission and cost accounting.

Biomass, independent of the type of feedstock, is treated as a single commodity in the energy system.
Bioenergy can therefore be used in power generation or in liquefaction or gasification processes alike (see :ref:`other` for further details).
The only exception is made for non-commercial biomass (fuel wood).
Non-commercial biomass supply and demand have been aligned between the two models.
In |name|,
non-commercial biomass is explicitly modeled as a demand category
(see :ref:`demand` for further details).
Its trajectory is taken from ACCESS, the household component of MESSAGEix-Buildings
(Poblete-Cazenave and Pachauri, 2018 :cite:`poblete_2018_fuelchoice`;
Poblete-Cazenave et al., 2021 :cite:`poblete_2021_scenarios`).
ACCESS simulates household fuel choice for cooking
and separates biomass that households purchase from biomass they collect.
Purchased biomass is part of the residential and commercial thermal demand,
while collected biomass forms the non-commercial biomass demand.
Both are harmonized in the base year
to the FAOSTAT fuelwood and wood-charcoal balances
(FAO, 2024 :cite:`fao_faostat_forestry_2024`).
Within a scenario, non-commercial biomass demand is fixed,
because non-commercial biomass is not a traded commodity
and its use is therefore not determined as a function of cost.
Its development depends on household income, its distribution and access to modern fuels,
which ACCESS represents for each SSP narrative.

The land-use pathways represent a broad rather than a specific policy landscape,
consistent with the SSP storylines (Popp et al., 2017 :cite:`popp_2017_SSPlanduse`).
This shapes how land-use policies can be represented in |name|.
Each pathway is calculated with a price on all land-use greenhouse gases,
so a price on a single gas also reduces the others.
A |name| scenario that prices only CH₄, for example,
therefore also obtains reductions of CO₂ and N₂O from the land-use sector.
Other land-use policies, such as limits on deforestation, can be implemented as well.
Because the solution space of the emulator is limited,
the selected pathways will then most likely also carry other land-use trends,
which are artifacts of the emulator rather than results of the policy.
For larger projects or studies, the matrix, that is the input data set from GLOBIOM,
can be tailored to allow the analysis of specific policies in |name|.

Equations and constraints
-------------------------

The :ref:`land use equations in MESSAGEix <message-ix:section_landuse_emulator>` state that the linear combination of land-use pathways must be equal to 1 (:eq:`Land constraint equation`).
Therefore, separately for each region, either a single discrete land-use scenario can be used, or shares of multiple scenarios can be combined linearly to obtain, for example, biomass quantities which are not explicitly represented as part of the land-use matrix.
This also applies to the mitigation dimension, that is to the GHG-categories.

.. math:: \sum_{s \in S} LAND_{n,s,y} = 1
   :label: Land constraint equation

To represent the transitional dynamics between land-use pathways, such as the rate at which land can be converted from one type to another, additional constraints are required, because the underlying dependencies between the land-use pathways are only represented in the full GLOBIOM model.
Based on rates derived from GLOBIOM,
for each of the |name| model regions,
the expansion of energy-crop plantation area is limited
using :ref:`dynamic constraints on land-use <message-ix:equation_dynamic_land_scen_constraint_up>`.
Energy-crop plantations can only expand onto land that was crop land, grass land or other natural land in the previous period.
For each land-use pathway, the area that can be converted is the previous-period area of these three land types multiplied by region-specific shares (:numref:`tab-land_type_shares`).
The constraint applies to the combination of land-use pathways chosen by the model.
The energy-crop plantation area in a period may not exceed the energy-crop plantation area of the previous period plus the convertible area, both weighted with the pathway shares of the previous period and grown by 0.2% per year (:eq:`Dynamic land conversion constraint`).
The larger the area of the three land types, the more energy-crop plantation area can be added in the following period.

.. math:: \sum_{s} energy\_crop\_land_{n,s,y} \cdot LAND_{n,s,y} \leq \sum_{s} \left( energy\_crop\_land_{n,s,y-1} + crop\_land_{n,s,y-1} \cdot X_{n} + grass\_land_{n,s,y-1} \cdot Y_{n} + other\_natural\_land_{n,s,y-1} \cdot Z_{n} \right) \cdot LAND_{n,s,y-1} \cdot 1.002^{\Delta y}
   :label: Dynamic land conversion constraint

The table below shows the share of each land type that can be converted per period, by region, :math:`X_{n}, Y_{n}, Z_{n}` (for further details see :ref:`landuse`).
The shares are the same for all SSPs.

.. _tab-land_type_shares:
.. list-table:: Shares of land type by region used to derive the growth constraint on energy-crop plantation area.
   :widths: 30 20 20 20
   :header-rows: 1

   * - Region
     - Crop land [-], :math:`X_{n}`
     - Grass land [-], :math:`Y_{n}`
     - Other natural land [-], :math:`Z_{n}`
   * - Sub-Saharan Africa
     - 0.05
     - 0.05
     - 0.05
   * - China
     - 0.05
     - 0.05
     - 0.02
   * - Rest of Centrally Planned Asia
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

The growth constraint on energy-crop plantation area therefore implies that, should high quantities of biomass be required in the energy system, either a combination of land-use pathways needs to be used over time that makes enough energy-crop plantation area available under this constraint, or land-use pathways of the highest BIO-categories need to be used from early in the century.
The latter would require the energy system to transition quickly enough to use such high biomass quantities.

In addition to constraining the expansion of energy-crop plantations (for further details see :ref:`forestry`), the area of existing forest, representing the land currently covered by forests, may grow by at most 0.1% per year (:eq:`Old forest growth constraint`).
Existing forest can therefore essentially only be deforested, and afforestation is depicted as a separate land-use type.

.. math:: \sum_{s} old\_forest_{n,s,y} \cdot LAND_{n,s,y} \leq \sum_{s} old\_forest_{n,s,y-1} \cdot LAND_{n,s,y-1} \cdot 1.001^{\Delta y}
   :label: Old forest growth constraint

The third and last set of constraints required for the land-use emulator enforces gradual transitions between land-use pathways.
Too rapid switches between land-use pathways, that is full transitions between land-use pathways in adjacent time steps, can occur for several reasons.
Slight numerical non-convexities in the input data can occur for individual time steps.
Cumulatively across time, the land-use pathways behave consistently, that is as carbon prices increase, cumulative emissions decrease within a single biomass category (see :numref:`fig-Landuse_Tradeoff_Surface`).
Yet for the same carbon price across multiple biomass categories, inconsistencies may occur, for example as a result of data scaling or aggregation.
Without a transitional constraint between pathways, the least-cost solution could be to switch between two land-use pathways for only a single time step, introducing artifacts in the model result (for example unreasonable price inconsistencies).
The carbon price categories have been chosen to span a broad range of mitigation options
(see :numref:`fig-Land-Use_Pathway_Scenario_Matrix`)
while keeping the solving time of |name| with the land-use emulator reasonable.
The transitional constraints further smooth the steps between the carbon price categories.
The transition rate is set so that land-use pathways can be phased out at a rate of at most 5% per year.
This value was derived from a sensitivity analysis, showing that this factor best matched the transitions of the full GLOBIOM model.

Land-use Price
--------------

The biomass and carbon price categories of the land-use scenario matrix (:numref:`fig-Land-Use_Pathway_Scenario_Matrix`),
together with the quantities of biomass and the respective emission reductions,
are used to determine the land-use scenario price (:ref:`objective function in MESSAGEix <message-ix:section_objective>`), which the model effectively interprets as the biomass price.
For a land-use scenario without a carbon price, for example in the first biomass category `BIO00` (:eq:`Landuse price equation for BIO00GHG000`),
the price (:math:`P`) is the biomass quantity (:math:`BQ`) times the biomass price (:math:`BPr`).

.. math:: P_{n,s_{BIO00,GHG000},y} = BQ_{n,s_{BIO00,GHG000},y} \cdot BPr_{n,s_{BIO00},y}
   :label: Landuse price equation for BIO00GHG000

Staying within the lowest biomass category, as the carbon price increases, the cost of emission mitigation is added to the price (:eq:`Landuse price equation for BIO00GHG010`).
The emission savings relative to the next lower carbon price category are multiplied with the carbon price (:math:`EPr`).
For the first non-zero carbon price category, `GHG010`, this gives

.. math:: P_{n,s_{BIO00,GHG010},y} = BQ_{n,s_{BIO00,GHG000},y} \cdot BPr_{n,s_{BIO00},y} + (E_{n,s_{BIO00,GHG000},y} - E_{n,s_{BIO00,GHG010},y}) \cdot EPr_{n,s_{GHG010},y}
   :label: Landuse price equation for BIO00GHG010

where :math:`E` are the land-use GHG emissions (CO2, CH4 and N2O, AR4 GWP100).
The biomass cost of a land-use scenario is always taken from the scenario without a carbon price in the same biomass category.
The mitigation costs add up over all lower carbon price categories of that biomass category.
In general

.. math:: P_{n,s_{b,g},y} = BQ_{n,s_{b,GHG000},y} \cdot BPr_{n,s_{b},y} + \sum_{k=1}^{g} (E_{n,s_{b,k-1},y} - E_{n,s_{b,k},y}) \cdot EPr_{n,s_{k},y}
   :label: General landuse price equation

where :math:`b` represents the biomass category, :math:`g` the carbon price category, and :math:`k` runs over the carbon price categories up to :math:`g`.
Negative emission savings, which result from the non-convexities described above, are replaced by a small positive value.

Because biomass is the only land-use related commodity which |name| accounts for when optimizing,
all costs associated with the mitigation of land-use related emissions
are perceived as being part of the biomass price.
This is a drawback of the approach, but nevertheless provides a full representation of the land-use scenario specific costs.

Pathway choice in scenarios
---------------------------

:numref:`fig-Landuse_Pathway_Choice` shows which land-use scenarios the model combines in 2045, 2070 and 2100
in three SSP2 scenarios built with |name| for ScenarioMIP-CMIP7
(van Vuuren et al., 2026 :cite:`van_vuuren_scenariomip_2026`),
the Medium, Medium-Low and Low emissions scenarios.
The Low emissions scenario is the ScenarioMIP-CMIP7 Low Emission marker scenario
(Fricko et al., 2026 :cite:`fricko_wu_2026`).
The Medium scenario follows current policies
and extrapolates their effort over the century
through a carbon price that reflects the regional emission reductions these policies achieve by 2030.
The model therefore mostly uses land-use pathways of the lowest biomass category
without or with low carbon prices throughout the century.
The Medium-Low scenario delays strengthened mitigation.
Its emissions follow the Medium scenario until 2040
and then decline gradually to net-zero CO2 around 2100.
The land-use choice moves from the lowest categories in 2045
towards pathways with biomass categories BIO03 to BIO08 and carbon prices of GHG050 to GHG200 by 2100.
The Low scenario pursues the Paris goal of keeping warming likely below 2°C
and reaches net-zero CO2 around 2070.
Pathways of GHG100 and GHG200 already account for about half of its land-use choice in 2045,
and from 2070 onward the model mostly combines them with BIO05 and BIO08.

The two dimensions of the matrix describe a trade-off.
A higher biomass category means a higher biomass price and more biomass supplied to the energy system,
for example 9.03 US$2005/GJ in 2100 for BIO08 against 3.38 US$2005/GJ for BIO03.
A higher carbon price category means deeper reductions of land-use emissions,
up to 2256 US$2005/tCO2 in 2100 for GHG2000.
With increasing mitigation stringency, the model moves along both dimensions.
More biomass is supplied to the energy system, where it is used with CCS (BECCS),
and more land-use emissions are reduced.
In the Low scenario, biomass primary energy rises from 48 EJ/yr in 2030 to 133 EJ/yr in 2100,
and about two thirds of it is used with CCS from 2070 onward,
capturing 3.8 Gt CO2/yr in 2070 and 6.3 Gt CO2/yr in 2100.
The model does not, however, move to the extremes of either dimension.
It balances land-use emission reductions against the biomass the energy system uses with CCS,
so that in the Low scenario pathways of GHG100 and GHG200 combined with BIO05 and BIO08 dominate,
while biomass categories of BIO13 and above are hardly used.

The figure shows averages over the 12 regions, which hides how much the regions differ.
In the Low scenario, land-use scenarios with carbon price categories of GHG400 and above play a small role on average,
but a much larger one in individual regions,
in Sub-Saharan Africa and in Latin America and the Caribbean in 2045, in Western Europe in 2070 and in South Asia in 2100.
In Latin America and the Caribbean in 2045, the only one of these land-use scenarios the model uses is BIO03GHG2000,
which combines a low biomass category with the highest carbon price category.

.. _fig-Landuse_Pathway_Choice:
.. figure:: /_static/emulator_pathway_choice.png
   :width: 800px
   :align: center

   Land-use pathway choice in the SSP2 Medium, Medium-Low and Low scenarios (rows) in 2045, 2070 and 2100 (columns). Each panel is the scenario matrix, with cells shaded by cumulative 2010 to 2100 AFOLU emissions of the land-use scenario in Gt CO2-equiv. Bubble area shows the share of a land-use scenario, averaged over the 12 regions, bubble colour the period. Shares below 0.005 are not shown.

GLOBIOM feedback
----------------

In addition to informing MESSAGEix of the biomass potential and land-use related emission quantities and prices, the land-use input matrix includes information on land use by type, production and demand of other land products, crop yields and irrigation water use, among others.
After a scenario has been solved, its reporting output is passed to the full GLOBIOM-G4M model, which is run once for that scenario.
The reporting contains a dedicated set of variables for GLOBIOM, among them the region-specific quantities of biomass, GDP and a carbon price.
This carbon price is not the carbon price of the energy system.
It is the carbon price of the land-use pathways, weighted with the shares in which the emulator uses them, so that GLOBIOM faces the carbon price at which the emulator chose its land-use pathways.
This feedback run is not iterated to convergence.
Its results replace the emulated land-use results in the reporting of the scenario, without solving MESSAGEix again.
Emissions from the energy sector remain unchanged, and total emissions are summed again with the land-use emissions from GLOBIOM.
Up to 2025, all scenarios take their land-use results from the feedback run of the SSP2 High Emissions scenario, so that the historical period is identical across scenarios.
The feedback run allows the land-use impacts of a scenario to be analyzed in greater detail and corrects the land-use emissions for the approximations of the emulator.

GLOBIOM supplies the biomass quantity passed to it almost exactly.
In the SSP2 Low scenario, the biomass supplied by the land system (energy crops, residues, roundwood, fuelwood and first-generation feedstocks)
is 34.9 EJ/yr in 2050 and 115.6 EJ/yr in 2100 in the emulated run, against 34.8 EJ/yr and 114.9 EJ/yr in the feedback run.
The two agree within 1% in all periods.
In the other scenarios of the ScenarioMIP-CMIP7 scenario set the difference stays within 4%,
with the exception of the High scenarios of SSP1 and SSP5 (8% and 6% in 2100, about 1 EJ/yr in both).

:numref:`fig-GLOBIOM_feedback` compares the emulated land-use emissions with those of the feedback runs for the three SSP2 scenarios of :numref:`fig-Landuse_Pathway_Choice` and for the SSP2 Very Low scenario.
Burning-related emissions are left out of the comparison,
because the emulator does not use the GLOBIOM emissions for them.
It replaces them with historical emissions
from the CMIP7 ScenarioMIP historical dataset (Nicholls et al., 2025 :cite:`nicholls_cmip7_2025`).
For open burning, this dataset uses the BB4CMIP7 data (van Marle and van der Werf, 2025 :cite:`van_marle_bb4cmip7_2025`),
which is closely related to the CMIP6 biomass burning data (van Marle et al., 2017 :cite:`van_marle_historic_2017`)
and based on GFED4.1s (van der Werf et al., 2017 :cite:`van_der_werf_global_2017`).
After 2020, each of these emission categories follows the relative change of a GLOBIOM variable
that serves as a proxy for its future development.
Forest burning follows CO2 emissions from deforestation, grassland burning the pasture area
and agricultural waste burning the cropland area of non-energy crops.
Peat burning is kept at its 2020 level.
The feedback runs report the burning emissions of GLOBIOM itself,
so for these categories the two runs differ by construction.
When the feedback results are merged into the reporting of the scenario,
the same replacement is applied again,
so the final results use the CMIP7-based burning emissions driven by the GLOBIOM proxies.
Burning emissions are therefore the same in the final results of both runs.
Without them, land-use CH4 emissions differ by less than 8% between the two runs.
AFOLU GHG emissions are lower in the feedback runs of the Medium-Low, Low and Very Low scenarios in all periods after 2020, mainly because of CO2.
In the Low and Very Low scenarios the difference is largest in 2050.
In the Low scenario the feedback run gives 1213 Mt CO2-equiv/yr against 2813 Mt CO2-equiv/yr in the emulated run,
and in the Very Low scenario -2204 Mt CO2-equiv/yr against 148 Mt CO2-equiv/yr.
Total GHG emissions in 2050 are 24.0 against 25.6 Gt CO2-equiv/yr in the Low scenario
and 8.9 against 11.2 Gt CO2-equiv/yr in the Very Low scenario.
After 2070 the differences are below 1 Gt CO2-equiv/yr in all four scenarios.

.. _fig-GLOBIOM_feedback:
.. figure:: /_static/emulator_feedback.png
   :width: 800px
   :align: center

   Emulated land-use emissions (solid lines) and GLOBIOM feedback runs (dotted lines) for the SSP2 Medium, Medium-Low, Low and Very Low scenarios, World, 2020 to 2100. Burning emissions are left out of both runs. Panel a shows total GHG emissions (CO2, CH4 and N2O, AR4 GWP100) in Gt CO2-equiv/yr, panel b AFOLU GHG emissions in Mt CO2-equiv/yr, panel c AFOLU CO2 emissions in Mt CO2/yr and panel d AFOLU CH4 emissions in Mt CH4/yr. In panel a, the feedback line replaces the emulated AFOLU emissions with those of GLOBIOM and keeps all other sectors. F-gases are not included.

Land cover differs much less between the two runs than emissions do (:numref:`fig-GLOBIOM_feedback_land_cover`).
In the Low scenario, forest area grows by 374 million ha between 2020 and 2100 in the emulated run and by 388 million ha in the feedback run.
Cropland grows by 170 against 149 million ha, pasture declines by 73 against 41 million ha
and other natural land declines by 471 against 496 million ha.
The area of afforestation and reforestation agrees within 25 million ha in all four scenarios.
The largest difference in any of the four categories and scenarios is 120 million ha, for other natural land in the Very Low scenario.
The lower emissions of the feedback runs therefore do not come from a different amount of land being converted,
but from the emissions and removals that GLOBIOM-G4M attaches to a given land-use change.

.. _fig-GLOBIOM_feedback_land_cover:
.. figure:: /_static/emulator_feedback_land_cover.png
   :width: 800px
   :align: center

   Land cover in the emulated runs (solid lines) and in the GLOBIOM feedback runs (dotted lines) for the SSP2 Medium, Medium-Low, Low and Very Low scenarios, World, as change relative to 2020 in million ha. Panel a shows forest, panel b cropland including energy crops, panel c pasture and panel d other natural land.

The difference between the two runs grows with the stringency of the scenario.
In the Medium scenario the two runs agree within 0.5 Gt CO2-equiv/yr in all periods and in the Medium-Low scenario within 0.8 Gt CO2-equiv/yr.
In the Low scenario the feedback run is lower by 1.6 Gt CO2-equiv/yr in 2050 and in the Very Low scenario by 2.4 Gt CO2-equiv/yr.
Over 2020 to 2100 the feedback run of the Low scenario is lower by 73 Gt CO2-equiv, about 4% of cumulative total GHG emissions.
The same pattern is found in the other SSPs of the ScenarioMIP-CMIP7 scenario set.
In all seven Low, Low Overshoot and Very Low scenarios of SSP1, SSP2, SSP4 and SSP5, the feedback run is lower than the emulated run,
by 1.5 to 3.8 Gt CO2-equiv/yr in 2050 and by 21 to 110 Gt CO2-equiv over 2020 to 2100.
In the High scenarios, where little land-based mitigation is used, the two runs agree within 0.4 Gt CO2-equiv/yr in 2050, with the exception of SSP4 (0.9 Gt CO2-equiv/yr).

Regionally the differences are concentrated.
In the SSP2 Low scenario, the largest difference in any period from 2030 to 2100 stays at or below 5% of the total GHG emissions of the region in 2020 for ten of the twelve regions.
It reaches 14% in Sub-Saharan Africa (R12_AFR) and in Latin America (R12_LAM), which together account for 57 of the 73 Gt CO2-equiv.
Across all scenarios at least eight of the twelve regions stay at or below 10%, and the largest values are found in these same two regions in most cases.

Two features of the emulator are likely to contribute to these differences.
The first is the transitional constraint, which uses a single phase-out rate of at most 5% per year for all regions, scenarios and SSP narratives.
This rate reproduces the transitions of GLOBIOM in most regions and scenarios, but it cannot match all of them.
The second is the spacing of the twelve carbon price categories.
The categories are chosen to cover the full range of mitigation responses across regions.
They need to resolve the response to small price increases at low prices, and they are spaced more widely at high prices,
where the regional response differs and levels off in some regions.
A response that lies between two widely spaced categories is therefore only approximated,
which matters most in stringent scenarios and in regions with a large land-based mitigation potential.
Both choices trade accuracy for an individual region or scenario against a single setup that is applicable across regions, periods and SSP narratives.
A setup that is differentiated by region or by narrative is a direction for future development.

The approach has limits that follow from its design.
Both prices apply to all biomass feedstocks and all land-use GHGs at once.
The abatement at a given carbon price is fixed within each land-use scenario.
Within a biomass category, the biomass potential does not change as the carbon price rises.
