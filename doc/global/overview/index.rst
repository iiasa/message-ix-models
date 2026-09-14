.. _overview:

Overview
==============
|name| couples three modelling suites.
The energy system is represented by MESSAGE
(Model for Energy Supply Strategy Alternatives
and their General Environmental impacts),
implemented in the |MESSAGEix| framework.
Land use is represented by GLOBIOM (GLObal BIOsphere Management),
together with the forest model G4M (Global FORest Model).
Non-CO2 greenhouse gases and air pollution are represented by GAINS
(Greenhouse gas-Air pollution INteractions and Synergies).
The three are linked iteratively,
exchanging bioenergy potentials,
land-use and pollutant emissions,
and sectoral activity levels,
converging on pathways that are internally consistent
across energy, land use and emissions (:numref:`fig-framework`).

Relative to the SSP quantification |name| contributed to ScenarioMIP-CMIP6
(Fricko et al., 2017 :cite:`fricko_marker_2017`),
the framework has been extended across all three suites,
with tighter coupling between them and deeper sectoral detail.
The extensions enter a scenario in two different ways.
Energy intensive industry and international maritime shipping
are represented directly in the scenario the core model solves,
the first resolving petrochemicals, cement, aluminium, and iron and steel
across material extraction, processing and recycling
(Ünlü et al., 2024 :cite:`unlu_2024_materials`,
see :doc:`MESSAGEix-Materials </conceptual/materials/index>`),
the second resolving shipping fuels, demand and modal split,
with bulk goods trade trajectories supplied exogenously
and energy commodity trade following the model's own interregional dynamics
(Kramel et al., 2026 :cite:`kramel_maritime_2026`).
Transport and residential and commercial demand instead come from
detailed end-use models run ahead of the core model,
:doc:`MESSAGEix-Transport </conceptual/transport/index>`
(McCollum et al., 2017 :cite:`mccollum-2017`)
and :doc:`MESSAGEix-Buildings </conceptual/buildings/index>`
(Mastrucci et al., 2021 :cite:`mastrucci_global_2021`;
Poblete-Cazenave et al., 2021 :cite:`poblete_2021_scenarios`),
which supply the demand trajectories the core model is given.
The sections below describe how the suites interact,
based on the scenarios contributed to ScenarioMIP-CMIP7.

|MESSAGEix| (Huppmann et al., 2019 :cite:`huppmann_2019_messageix`)
is the core of the framework (:numref:`fig-framework`).
It is a bottom-up, technology rich optimisation model
representing the full energy system,
from primary resource extraction
through conversion and transmission
to final energy demand in industry, transport and buildings.
Technology investment, operation and commodity trade are chosen
so that demand is met at least cost,
subject to resource, technology and policy constraints.
MESSAGE can represent a range of energy and climate
:ref:`policies <policy_overview>`.

MESSAGE runs iteratively with MACRO,
an aggregated single-sector growth model
with one representative producer and consumer per region
(Messner and Schrattenholzer, 2000 :cite:`messner_messagemacro:_2000`).
MACRO produces output from capital, labour and energy
through a nested production function,
so that as energy becomes more expensive
the economy substitutes away from it and can also produce less.
It adjusts useful energy demand in the end-use categories
MESSAGE resolves (see :ref:`demand`)
until the two models reach equilibrium (see :ref:`macro`).
This iteration is what carries price-induced energy efficiency
and demand adjustment into a scenario.
MACRO is calibrated on the policy-free reference scenario,
the only one with undistorted energy prices,
by adjusting the rate of autonomous energy efficiency improvement
and the underlying growth path
until the coupled model reproduces the SSP GDP trajectory
together with the demands, costs and prices
of the energy system solution.

GLOBIOM (see :ref:`globiom`, IBF IIASA, 2023 :cite:`globiom_documentation_2023`)
is a spatially explicit partial equilibrium model
of the agricultural, forestry and bioenergy sectors.
It allocates land across competing uses
to meet demand for food, feed, fibre and biomass,
and returns bioenergy supply potentials,
land-use change GHG emissions,
and the afforestation response to carbon prices.
Forestry is resolved in G4M
(Kindermann et al., 2006 :cite:`kindermann_predicting_2006`;
Kindermann et al., 2008 :cite:`kindermann_global_forest_2008`;
Gusti, 2010 :cite:`gusti_algorithm_2010`),
which provides afforestation, deforestation and forest management activity
together with the resulting forest products and carbon uptake.

Rerunning GLOBIOM inside the energy system optimisation would be too costly,
so land use enters MESSAGE through an emulator of GLOBIOM and G4M
(see :ref:`emulator`).
The two models are run beforehand over a grid spanned by two prices,
one paid for biomass delivered to the energy sector
and one on greenhouse gases.
Every pairing of the two is one pre-computed land-use case,
carrying its own land cover, land-based emissions,
biomass availability and cost.
For each region and period
the energy system selects a weighted combination of these cases,
so that outcomes between the pre-computed steps can be reached
and the combination can shift over time
as bioenergy demand and the carbon price change.

A feedback run complements the emulator for every scenario.
The biomass the land system supplies in the scenario solution
is paired with the carbon price the land system faces
under the scenario's climate target,
and both are passed to the full GLOBIOM and G4M models.
Those results then replace the emulated land-use results
in the scenario output.
This lifts the restriction to the pre-computed price steps
and yields a more extensive set of land-use indicators,
including spatially explicit information on land use.
The feedback is applied in a single pass
and is not iterated to convergence.

GAINS (Amann et al., 2011 :cite:`amann_cost-effective_2011`),
is used to explore cost-effective multi-pollutant emission control
strategies that meet objectives on air quality,
for human health and ecosystems, and on greenhouse gases.
It covers SO2, NOx, PM, NMVOC, NH3, CO2, CH4, N2O and F-gases,
together with the interrelations between them
and the effects they contribute to,
across 183 world regions,
drawing on source- and technology-specific emission and cost factors
and more than a thousand control options.
It enters |name| through two linkages.

Non-land-use CH4 and N2O emissions are represented inside the energy system
using implied emission factors derived from GAINS
(Höglund-Isaksson et al., 2020 :cite:`hoglund_isaksson_technical_2020`;
Winiwarter et al., 2018 :cite:`winiwarter_technical_2018`),
derived for each SSP narrative
at the regional and technological resolution MESSAGE uses
and attached to the model driver that determines each emission.
GAINS also supplies the matching abatement options,
which enter as explicit technologies rather than as a
marginal abatement cost curve,
so that their own energy requirements, lifetimes and costs
are part of the optimisation (see :ref:`gains`).

Air pollutant emissions are derived after the energy and land-use
solution is complete.
Scenario output is translated to the native sector, activity
and technology resolution of GAINS through a trend-preserving
downscaling framework, and the resulting emissions are aggregated back
to the regions and reporting variables of |name|
(Fricko et al., 2026 :cite:`fricko_wu_2026`).
Keeping the work at GAINS resolution means that
the air pollution legislation GAINS tracks,
and the control costs attached to it,
apply at the detail at which they were assessed.

Climate targets are taken up in the coupled |name| optimisation.
Cumulative greenhouse gas emissions from all sectors
are constrained at different levels,
with the constraint priced across gases on a CO2-equivalent basis,
so that a scenario reaches its intended level of radiative forcing.
The price the land system faces is derived from this one rather than equal to it.
In each region the emulator selects a combination of pre-computed cases,
each carrying its own GHG price level,
and that regional selection, scaled by the carbon price,
gives the land-use price reported with the land-use results
and used in the feedback run described above.

The combined emissions from energy, industrial processes and land use
are harmonised to a common historical record
and passed to the simple climate model MAGICC (see :ref:`magicc`),
which returns atmospheric concentrations, radiative forcing
and global mean surface temperature.
Climate impacts and carbon cycle feedbacks do not return to the framework,
so they are, depending on the application,
only partly accounted for.

The scientific software underlying the global |name| model
is the |MESSAGEix| framework,
an open-source, versatile implementation of a linear optimization problem,
with the option of coupling to MACRO
to incorporate the effect of price changes on economic activity
and demand for commodities and resources.
|MESSAGEix| is integrated with the *ix modeling platform* (ixmp),
a data warehouse for version control of reference timeseries,
input data and model results.
ixmp provides a Python interface for efficient, scripted workflows
in data processing and visualisation of results,
and is usable from R through reticulate
(Huppmann et al., 2019 :cite:`huppmann_2019_messageix`).

.. _fig-framework:

.. figure:: /_static/iam_framework.png
   :width: 900px

   The |name| framework.
   Boxes are the models and modules making up the three suites,
   arrows the data passed between them
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).

.. toctree::
   :maxdepth: 1

   spatial
   temporal
   policy/index
