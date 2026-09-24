.. _globiom-demand:

Demand and market equilibrium
-----------------------------
Demand for land-related products,
that is agricultural products, forestry products and bioenergy feedstocks,
is represented at the level of the aggregate economic regions.
Food, feed and first generation biofuel demand draw on crop and livestock production,
forest product demand drives the wood assortments that GLOBIOM and G4M supply together,
and bioenergy demand enters from |MESSAGEix|.
All of them compete for the same land and are cleared in a single market equilibrium.

.. _food:

Food demand
~~~~~~~~~~~
Food demand is in GLOBIOM endogenous and depends on population, gross domestic product (GDP) and own product price. Population and GDP are exogenous variables while prices are endogenous.
The simple demand system is presented in Eq. :eq:`foodelasticity`. First, for each product :math:`i` in region :math:`r` and period :math:`t`,  the prior demand quantity :math:`Q` is calculated as a
function of population POP, GDP per capita :math:`GDP^{cap}` adjusted by the income elasticity :math:`\varepsilon^{GDP}`, and the base year consumption level as reported in the Food Balance Sheets of FAOSTAT.
If the prior demand quantity could be satisfied at the base year price :math:`P`, this would be also the optimal demand quantity :math:`Q`. However, usually the optimal quantity will be different from the prior
quantity, and will depend on the optimal price :math:`P` and the price elasticity :math:`\varepsilon^{price}`, the latter calculated from USDA (Seale et al., 2003 :cite:`seale_international_2003`),
updated in Muhammad et al. (2011) :cite:`muhammad_international_2011` for the base year 2000. Because food demand in developed countries is more inelastic than in developing ones,
the value of this elasticity is assumed to decrease with the level of GDP per capita. The rule applied is that the price elasticity of developing countries converges to the price elasticity of the USA in
2000 at the same pace as their GDP per capita reach the USA GDP per capita value of 2000. This allows capturing the effect of change in relative prices on food consumption taking into account heterogeneity
of responses across regions, products and over time.

.. math:: \frac{Q_{i,r,t}}{\overline{Q}_{i,r,t}} = \left( \frac{P_{i,r,t}}{\overline{P}_{i,r,2000}} \right)^{\varepsilon_{i,r,t}^{price}}
   :label: foodelasticity

where

:math:`\overline{Q}_{i,r,t} = \frac{POP_{r,t}}{POP_{r,2000}}\times \left( \frac{GDP_{r,t}^{cap}}{GDP_{r,2000}^{cap}}\right)^{\varepsilon_{i,r,t}^{GDP}} \times \overline{Q}_{i,r,2000}`

The two steps have distinct roles.
The prior quantity shifts the demand curve with socio-economic development,
while the price response moves along that curve to the equilibrium solution.
Income elasticities differ across products and regions,
which allows dietary composition to evolve with economic development.
Demand for livestock products, for example,
responds more strongly to income growth than demand for staple cereals.
Both income and price elasticities decline as regional incomes rise.

This demand function has the virtue of being easy to linearize as GLOBIOM is solved as a linear program. This is currently necessary because of the size of the model and the performance of non-linear solvers. However, this demand function has although some limitations which need to be kept in mind when considering the results obtained with respect to climate change mitigation and food availability. One of them is that it does not consider direct substitution effects on the consumer side which could be captured through cross price demand elasticities. Such a demand representation could lead to increased consumption of some products like legumes or cereals when prices of GHG intensive products like rice or beef would go up as a consequence of a carbon price targeting emissions for the agricultural sector. Neglecting the direct substitution effects may lead to an overestimation of the negative impact of such mitigation policies on total food consumption. However, the effect on emissions would be only of second order, because consumption would increase for commodities the least affected by the carbon price, and hence the least emission intensive. Although direct substitution effects on the demand side are not represented, substitution can still occur due to changes in prices on the supply side and can in some cases lead to a partial compensation of the decreased demand for commodities affected the most by a mitigation policy.

Feed and first generation biofuel demand
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Feed demand is not imposed independently.
It follows endogenously from the demand for livestock products,
from herd composition and the choice of livestock production system,
and from the feed basket chosen on relative cost competitiveness (see :ref:`livestock`).

Crop use for first generation biofuels is linked to the corresponding conversion technologies
and is aligned with historical biofuel trends.
Future first generation biofuel production is held static,
following the AgMIP model intercomparison
(Lotze-Campen et al., 2014 :cite:`lotze-campen_impacts_2014`).

Forest product demand
~~~~~~~~~~~~~~~~~~~~~
Demand for semi-finished forest products,
that is sawn wood, plywood, fiberboard, chemical and mechanical pulp, other industrial roundwood,
fuelwood and energy wood,
is represented through regional constant elasticity demand functions (see :ref:`forestry`).
Baseline material demand develops with population and income,
while equilibrium consumption responds to endogenous product prices.

The resulting demand for primary wood assortments follows
from the input requirements of the forest industry technologies.
It can be met from domestic harvesting, imports, recycled material,
or forest industry by-products,
subject to product balances, technological constraints and economic competitiveness.

The forest response to those signals is simulated by G4M.
GLOBIOM sets the demand for wood and the prices that clear the wood markets,
and G4M decides how forests meet that demand,
covering harvesting, rotation length, thinning, afforestation and deforestation.
The forest transitions and sustainable wood-supply responses that G4M returns
are harmonized with the agricultural land requirements
and the regional wood-market outcomes of GLOBIOM,
so that the two models describe one consistent forest sector.
The coupling itself is described in :ref:`globiom`,
and the forest sector representation in :ref:`forestry`.

Bioenergy demand
~~~~~~~~~~~~~~~~
Bioenergy demand is specified exogenously from the |MESSAGEix| results.
GLOBIOM then determines the economically optimal combination of feedstocks,
among them crops, crop residues, dedicated energy plantations, primary wood,
logging residues and forest industry by-products,
while accounting for their competing material, food, feed and land uses.

The |MESSAGEix| results that set this demand
are themselves generated from bioenergy cost-supply curves that GLOBIOM provides.
Those curves come from a series of GLOBIOM runs
carried out beforehand with no bioenergy demand
but with stepwise prices for primary bioenergy in future periods.
Bioenergy demand is therefore exogenous to any single GLOBIOM run
while remaining consistent with what the land system is able to supply.
The curves and the wider coupling are described in :doc:`emulator`.

Market equilibrium
~~~~~~~~~~~~~~~~~~
GLOBIOM solves these demand relationships jointly with production, processing, trade and land allocation.
For each model period the model maximizes the sum of producer and consumer surplus
subject to commodity balances,
land and water availability,
production technologies,
processing capacities,
trade costs,
land conversion possibilities,
and any applicable policy constraints.
Regional market prices adjust until supply equals demand for each represented product.

The model is partial equilibrium in the sense that not all goods, sectors, factors or economic agents are represented.
Prices for land-sector factors,
among them irrigation water, fertilizer, and investment per unit of forestry sector capacity,
are taken as exogenous inputs.
This allows the model to carry a detailed representation of the land-related sectors
and to accommodate the computational demand of spatially explicit equilibrium solutions.

The simulation is implemented recursively.
The land-use configuration, production capacities and other state variables of one period
provide the initial conditions for the next,
and the equilibrium solution is updated in every period
with inputs derived from the results of the preceding one.
Within a period, market equilibrium and land-use equilibrium are not solved sequentially.
Crop and livestock production, forest product supply, bioenergy feedstock allocation,
consumption, trade, management system choice and land-use change
are determined simultaneously as components of the same economic solution.

The equilibrium across the agricultural, forestry, bioenergy, trade and land markets
is solved in GLOBIOM,
while the detailed spatial and intertemporal representation of forest dynamics comes from G4M.
