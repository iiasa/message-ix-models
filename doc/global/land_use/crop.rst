.. _crop:

Crop production
***************

GLOBIOM directly represents production from three major land cover types,
cropland, managed forest, and areas suitable for short rotation tree plantations.
Crop production covers 18 crops globally,
namely barley, dry beans, cassava, chickpeas, corn, cotton, groundnut, millet, oil palm, potatoes,
rapeseed, rice, soybeans, sorghum, sugarcane, sunflower, sweet potatoes and wheat,
which together account for more than 70% of global crop harvested area
and 85% of crop-derived calorie supply as reported by FAOSTAT
(FAO, 2024 :cite:`fao_faostat_2024`).

Each crop can be produced under one of four management systems of increasing intensity,
subsistence farming, low input rainfed, high input rainfed, and high input irrigated,
corresponding to the crop distribution data classification of the
International Food Policy Research Institute
(You and Wood, 2006 :cite:`you_entropy_2006`).
Within each system the input structure is fixed following a Leontief production function,
while effective yields change endogenously,
either by switching to a more or less intensive management system
or by reallocating production to a more or less productive Supply Unit.
Switches between systems respond to relative profitability,
so that the structure of crop production can intensify or extensify as economic conditions change.

Yields and technological change
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Crop yields are generated at the grid cell level from soil, slope, altitude and climate information
with the bio-physical process model EPIC (Environmental Policy Integrated Climate)
(Izaurralde et al., 2006 :cite:`izaurralde_simulating_2006`;
Williams and Singh, 1995 :cite:`williams_computer_1995`),
and the resulting regional production is harmonized with FAOSTAT.

For future periods a country and crop specific yield shifter represents long-term technological change
and differs across the SSP narratives (see :ref:`narratives`).
The shifter is estimated with a multilevel Bayesian regression for each crop
across up to 125 countries,
using historical FAOSTAT data from 1971 to 2015
together with the SSP macroeconomic projections to 2100.
The natural logarithm of real gross domestic product per capita serves as a proxy for technological advancement,
which lets the hierarchical model capture the non-linear, diminishing returns
of economic development on crop yields.
Effects are separated into a global pooled trend and region-specific deviations,
and a Student t distribution with recency-weighted error variances
keeps the estimate robust against outliers and aligned with recent agricultural trends.
Yield trajectories follow from applying the estimated coefficients
to the narrative-specific changes in regional log real GDP per capita
in an iterative five year time step.
Two bounds keep the projections physically and agronomically plausible.
A lower bound prevents anomalous yield declines
by substituting negative changes with the lowest positive regional shift,
and an upper bound dampens growth as yields approach defined maxima.
The final shifters are unitless ratios relative to the base year 2000,
which makes them comparable across regions and crops.

Irrigation and water
~~~~~~~~~~~~~~~~~~~~

Fertilizer use, irrigation demands, and costs of production under the different management systems
are derived for the base year 2000 from EPIC
and are assumed to increase proportionally with yields.
Irrigation water requirements are crop specific and,
together with the spatial distribution of irrigated production,
determine agricultural irrigation water demand.
Costs for four irrigation systems are derived from a variety of sources
as described in Sauer et al. (2008) :cite:`sauer_agriculture_2008`.
Climate-driven changes in crop irrigation requirements come from EPIC runs
conducted within the ISIMIP3b framework
(Balkovic et al., 2014 :cite:`balkovic_global_2014`).

Water availability for irrigation is constrained with hydrological output
from the Community Water Model
(Burek et al., 2020 :cite:`burek_development_2020`).
Surface water available for irrigation follows from model runoff
after accounting for domestic and industrial water demands
and, depending on the narrative, environmental flow requirements.
These non-agricultural demands vary across narratives
with population, GDP per capita and urbanization
(Fridman et al., 2024 :cite:`fridman_ssp_2024`).
GLOBIOM combines surface water availability with groundwater resources
and imposes the result as a resource constraint on irrigated crop production,
so that total irrigation water requirements cannot exceed the water available within a spatial unit.
Where the constraint binds, expansion of irrigated production is restricted,
and the model responds through changes in irrigated area, crop choice, production location,
and other land-use and market adjustments
(Palazzo et al., 2024 :cite:`palazzo_impacts_2024`).
This follows the integrated water and land modelling framework
of Arbelaez Gaviria et al. (2026) :cite:`arbelaez_global_2026`.

Irrigation also differs across the narratives through water-use efficiency.
Improvements in irrigation efficiency reduce the water that must be withdrawn
to satisfy a given crop irrigation requirement,
and are differentiated to reflect technological development,
infrastructure investment and maintenance
(Palazzo et al., 2019 :cite:`palazzo_investment_2019`).
More sustainable pathways assume faster improvements,
while pathways with slower technological development
and weaker infrastructure investment assume more limited ones.
Narratives therefore shape irrigation both through the demand for water from competing sectors
and through the efficiency with which agriculture uses it.

Crop products and residues
~~~~~~~~~~~~~~~~~~~~~~~~~~

Crop supply flows into three downstream channels,
food consumption (see :ref:`food`),
livestock feed (see :ref:`livestock`),
and bioenergy processing for first generation biofuel
(see :numref:`fig-landuse_product_structure`).
All crop products and their trade are expressed as biomass flows in kilograms of fresh matter.

Agricultural residue production is represented endogenously for each crop
from crop production quantities and crop-specific residue expansion factors,
so that residue availability evolves with crop output across scenarios.
Crop residues are not traded.
They are available as a bioenergy resource,
expanding the feedstock base alongside dedicated energy plantations and forest-based biomass.
