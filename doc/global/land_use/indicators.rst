.. _globiom-indicators:

Environmental and food security indicators
------------------------------------------
Alongside the economic outcomes,
GLOBIOM quantifies the environmental and social consequences of the land-use pathways it produces.
Three families of indicator are reported,
biodiversity, nitrogen, and food security.
Irrigation water demand is the fourth,
and is described with the crop production it constrains (see :ref:`crop`).

Biodiversity
~~~~~~~~~~~~
Biodiversity impacts of land-use change are assessed by coupling |name| to the PREDICTS model
(Newbold et al., 2016 :cite:`newbold_has_2016`),
which yields the biodiversity intactness index (BII),
an indicator of the intactness of local species composition.
The implementation follows Leclere et al. (2020) :cite:`leclere_bending_2020`.

PREDICTS is an empirical model
built on observations of terrestrial ecological communities
across different land uses and management intensities.
It estimates how total abundance and community composition
differ from those in minimally disturbed reference habitats.
BII combines the two components
and represents the average abundance of originally present species
relative to an intact reference condition.

The projected land-use classes of each simulation unit
are linked to the corresponding PREDICTS land-use categories,
and the resulting values are area-weighted to give regional and global estimates.
Changes in BII therefore reflect two things at once,
shifts in the extent of the land-use classes,
and transitions between land uses that affect local ecological communities differently.

Nitrogen
~~~~~~~~
GLOBIOM represents nitrogen flows across agricultural systems,
covering cropland, pasture, livestock production and the linked food systems
(Chang et al., 2021 :cite:`chang_reconciling_2021`).
The nitrogen module builds on the physical biomass flows the model already simulates,
among them crop production, feed use, grass biomass, crop residues, livestock production,
international trade in agricultural products, and food consumption with its losses and waste.
Those flows, expressed in fresh or dry matter,
are converted into nitrogen flows with crop and product specific nitrogen content coefficients.

Nitrogen inputs comprise mineral fertilizer application,
biological nitrogen fixation, manure application, and atmospheric deposition.
Nitrogen outputs comprise removal in harvested crops, grass forage and animal products,
together with losses through leaching, runoff, volatilization,
and gaseous emissions from denitrification.
Inputs, outputs and environmental losses are held in a mass-balance framework.
Nitrogen flows in household waste and sewage sludge follow the methodology
of van Puijenbroek et al.
(2019 :cite:`vanpuijenbroek_global_2019`;
2023 :cite:`vanpuijenbroek_quantifying_2023`).

GLOBIOM does not include a process-based soil nitrogen cycle.
Long-term projections therefore assume a steady state balance of soil nitrogen pools,
in which nitrogen mineralized in soils is taken up by plants
and returned through crop residues and organic matter,
with no net accumulation or depletion over time.
This assumption reflects a long-term equilibrium consistent with sustainable land use,
but it does not capture short-term soil dynamics.

Most nitrogen fluxes are determined endogenously,
among them crop uptake, manure recycling, biological fixation and deposition,
and they follow from the changes in agricultural production, land use and livestock systems
that the model simulates.
Mineral fertilizer application is the exception.
It is calibrated to regional nitrogen use efficiency targets specified exogenously,
where nitrogen use efficiency is the ratio of nitrogen removed in agricultural products
to total nitrogen inputs.
Given the projected nitrogen removal and the other inputs,
fertilizer use is adjusted to stay consistent with the assumed efficiency trajectory.

Nitrogen surpluses are the difference between total nitrogen inputs
and nitrogen removal in agricultural products.
They represent nitrogen losses to the environment
and are associated with air pollution, water quality degradation,
biodiversity impacts and greenhouse gas emissions.
Tracking the surpluses and their associated emissions
links agricultural production to these environmental pressures.

The module also carries the mitigation options that act on nitrogen use and losses,
among them improved fertilizer management to raise nitrogen use efficiency,
better manure recycling and management,
and improved wastewater treatment and nutrient recovery.
The trajectories assumed for nitrogen use efficiency, manure management and human sewage
differ by narrative (see :ref:`narratives`).

Food security
~~~~~~~~~~~~~
Food intake availability and undernourishment rates are reported as indicators of food security.
GLOBIOM estimates the number of undernourished people
as the product of total population and the prevalence of undernourishment,
with the prevalence calculated following the FAO methodology.
Three components enter that calculation,
mean dietary energy availability,
the minimum dietary energy requirement,
and the distribution of food consumption within the population.

Mean dietary energy availability, in kilocalories per person per day,
comes directly from the food consumption output of the model (see :ref:`food`).
It reflects average calorie availability at the regional level
and evolves endogenously with income, prices and production systems.

The minimum dietary energy requirement is the threshold energy intake
needed to maintain a healthy life.
Future values are calculated at the country level
by adjusting base year estimates for changes in the age and sex composition of the population,
so that demographic shifts carry through to nutritional requirements.

The distribution of food consumption within a population is represented as log-normal,
characterized by its mean, the average calorie availability,
and its variance, captured through the coefficient of variation of dietary energy consumption,
which reflects inequality in access to food.
Changes in the coefficient of variation are linked to income growth,
on the assumption that improving economic conditions distribute food more equally,
and it declines over time toward a lower bound
consistent with the best performing countries observed historically.

The prevalence of undernourishment is then the share of the population
whose dietary energy consumption falls below the requirement threshold,
given that distribution.
The indicator therefore accounts not only for average food availability
but also for inequality in access to it.
For high income regions, where undernourishment is currently negligible,
the prevalence is assumed to remain zero over the projection period.
