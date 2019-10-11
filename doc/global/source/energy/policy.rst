.. _policy:

Modelling policies
==================
The global energy model distinguishes between eleven global regions (cf. Section :ref:`spatial`).  It is nevertheless important to represent current and planned national policies, such as the nationally determined contributions (NDCs) as agreed upon in Paris Agreement, at a lower geographical resolution in order to adequatly account for near term developments in scenario development processes. 

Representation of single country Nationally Determined Contributions (NDCs)
---------------------------------------------------------------------------
The targets formulated in the NDCs come in many different flavors. This applies to the sectors and gasses, which policies cover, but also how these are expressed and quantified. In the global energy model, four broad categories of policy types related to the NDCs are represented, each of which is translated into a set of constraints.
   1. Emission targets
   2. Energy shares
   3. Capacity or generation targets
   4. Macro-economic targets
A detailed description of the methodological implementation of the NDCs in the global energy model, along with an extensive list of the energy related targets considered can be found in Rogelj et al. (2017) :cite:`rogelj_indc_2017`. Additional policies implemented in the model can also be found in ('what reference for the CD_Links related policies?`)

Emission targets
----------------
Country specific emission reduction targets are specified either in relation to historical emissions (e.g. x% reduction compared to 1990) or in relation to a reference emission trajectory (in the form of a baseline or business as usual scenario (BAU); e.g. x% reduction compared to 2030 emission levels in the baseline). The targets themselves are expressed as either (1) absolute reduction, (2) a percentage reduction or (3) intensity reductions e.g. emissions per GDP or per capita. In order to account for these different reduction targets in the global energy model, the targets are translated so that a regionally specific upper bound on emissions can be formulated. If not further specified, emission constraints are assumed to apply to all sectors and all gases, i.e. total GHGs.

Energy shares
-------------
Energy share targets refer to any target which aims to provide a specific energy level (e.g. primary, secondary or final energy) through a specific sub-set of energy forms.  The five different forms in which these are formulated in the NDCs are: (1) renewable energy as share of total primary energy, (2) non-fossil energy forms as share of total primary energy, (3) renewable energy as a share of total electricity generation, (4) non-fossil energy as a share of total electricity generation, (5) renewable energy as a form of final energy.  All of these share constraint variants can be implemented in the model using the following `mathematical formulation <https://message.iiasa.ac.at/en/stable/model/MESSAGE/model_core.html#constraints-on-shares-of-technologies-and-commodities>`_. In order to be able to implement these for aggregate regions, it is necessary to harmonize these to single type of share constraint, so that their effects are considered cumulatively within a region. All variants are therefore harmonized to either the share type specified by the largest country, in terms of share of energy within a region, or the most frequently specified type within a region.
Separately biofuel shares are implemented specifically for the transport sector.


Capacity and generation targets
-------------------------------
Some NDCs specify capacity installation targets, e.g. for planned power plants which will be operational by a certain year.  Others specify that a given energy commodity will come from a specific source, for example a certain amount of electricity will stem form a specific intermittent renewable source or nuclear. These targets types are implemented in the model as lower bounds on generation.


Macro-economic targets
----------------------


Representation of taxes and subsidies
-------------------------------------
Another set of policies addressed as a part of climate change analysis, are energy related taxes and subsidies. Removing fossil-fuel subsidies could help reduce emissions by discouraging the use of inefficient energy forms. In the global energy model, fossil fuel prices are endogenously derived based on underlying supply curves representing the technical costs associated with the extraction of the resources (cf. Section :ref:`fossilfuel`).  Refining and processing as well as transmission and distribution costs will be added to the total fuel cost. In order to therefore account for taxes, price adjustment factors are applied, based on the underlying data set as described in Jewell et al. (2018) :cite:`jewell_subsidy_2018`.
