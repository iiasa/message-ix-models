.. _food:

Food demand
-----------
Food demand is in GLOBIOM endogenous and depends on population, gross domestic product (GDP) and own produt price. Population and GDP are exogenous variables while prices are endogenous. 
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

:math:`\overline{Q}_{i,r,t} = \frac{POP_{r,t}}{POP_{r,2000}}\times \left( \frac{GDP_{r,t}^{cap}}{GDP_{r,2000}^{cap}}\right)^{\varepsilon_{i,r,t}^{price}} \times \overline{Q}_{i,r,2000}`

This demand function has the virtue of being easy to linearize as GLOBIOM is solved as a linear program. This is currently necessary because of the size of the model and the performance of non-linear solvers. However, this demand function has although some limitations which need to be kept in mind when considering the results obtained with respect to climate change mitigation and food availability. One of them is that it does not consider direct substitution effects on the consumer side which could be captured through cross price demand elasticities. Such a demand representation could lead to increased consumption of some products like legumes or cereals when prices of GHG intensive products like rice or beef would go up as a consequence of a carbon price targeting emissions for the agricultural sector. Neglecting the direct substitution effects may lead to an overestimation of the negative impact of such mitigation policies on total food consumption. However, the effect on emissions would be only of second order, because consumption would increase for commodities the least affected by the carbon price, and hence the least emission intensive. Although direct substitution effects on the demand side are not represented, substitution can still occur due to changes in prices on the supply side and can in some cases lead to a partial compensation of the decreased demand for commodities affected the most by a mitigation policy. 
