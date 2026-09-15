.. _policy:

Modeling policies
=================
The global energy model distinguishes between twelve global regions (cf. Section :ref:`spatial`).  It is nevertheless important to represent current and planned national policies - such as the nationally determined contributions (NDCs) as agreed upon in the Paris Agreement - at a lower geographical resolution, in order to be able to adequately account for future changes in the scenario development processes.

From national targets to regional constraints
----------------------------------------------
National targets are first harmonised against the historical emissions the model
carries, then translated into constraints on the region the country belongs to.
Historical national emissions are taken from PRIMAP-hist
(Gütschow and Pflüger, 2022 :cite:`gutschow_primap_2022`),
EDGAR (Crippa et al., 2023 :cite:`crippa_edgar_2023`)
and CAIT, now published as Climate Watch
(Climate Watch, 2022 :cite:`climate_watch_2022`),
with PRIMAP as the default and the other two used in that order
where a country is missing.

Because the model's historical emissions may differ slightly from historical data
sources, aggregate national emissions and regional model emissions differ.
The difference for region :math:`r` in period :math:`t` is

.. math:: EDiff_{r,t} = GHG_{r,t} - \sum_{iso \in r} GHG_{iso,t}

and is distributed across the countries of that region in proportion to their
share of the national total,

.. math:: GHGsh_{iso,t} = \frac{GHG_{iso,t}}{\sum_{iso \in r} GHG_{iso,t}}

.. math:: GHG^{adj}_{iso,t} = GHG_{iso,t} + EDiff_{r,t} \cdot GHGsh_{iso,t}

The adjustment is held constant across the other historical years.

Projected national emissions are derived by downscaling the regional pathway of
the reference scenario. The regional GHG intensity is extrapolated to a
convergence year :math:`CY` beyond the model horizon using the growth rate of
the last ten years of the baseline, which gives each country a constant annual
intensity growth rate from its own intensity in the base year :math:`BY`,

.. math:: GHGIgr_{iso} = \left( \frac{GHGI_{r,CY}}{GHGI_{iso,BY}} \right)^{\frac{1}{CY-BY}}

from which baseline national intensities follow period by period,

.. math:: GHGI^{*}_{iso,t} = GHGI_{iso,t-1} \cdot GHGIgr_{iso}

The scaling above is then reapplied so that the downscaled national emissions
sum back to the regional baseline.

Targets expressed relative to a historical base year are computed against the
harmonised inventory. Where a target names no reference-year emissions, the
downscaled no-policy baseline is used instead. Unless a target says otherwise it
is taken to apply to all sectors and all gases. Unquantified targets, non-emission
land-use targets, and countries lacking the historical emission or GDP data the
calculation needs are omitted.

Aggregating share targets
-------------------------
A national share target becomes a regional one weighted by the country's share
of regional energy in the reference year :math:`RY`,

.. math:: shr_{r,TY} = \sum_{iso \in r} \frac{Energy_{iso,RY}}{Energy_{reg,RY}} \cdot Tshr_{iso,TY}

where :math:`Tshr_{iso,TY}` is the national target share in the target year
:math:`TY`, and :math:`Energy` is primary energy, electricity generation or
final energy depending on the target type.

Countries within one region rarely express their targets against the same
quantity. Implemented separately, such targets would not act cumulatively at the
regional level and the region would underachieve them. Each region therefore
converts all of its national targets to one dominant type, chosen as the type
used by the largest country by energy share or by the majority of countries in
that region. Nine conversions are defined between the five share types, each with
a direct-equivalent and a substitution-accounting variant. Converting a renewable
share of primary energy into a renewable share of electricity generation, for
example, is

.. math:: NewTshr_{iso,TY} = \frac{Tshr_{iso,TY} \cdot \frac{ReEg_{iso,RY}}{RePe_{iso,RY}} \cdot TotPe_{iso,RY}}{TotEg_{iso,RY}}

Implementing share constraints
------------------------------
The model does not resolve national energy systems, so each country's share of
regional energy is held at its reference-year value. The regional target is then
imposed as a relation between the energy produced by the technologies the target
covers, :math:`lhs_{r,t}`, and everything else, :math:`rhs_{r,t}`,

.. math:: \sum_{r,t} lhs_{r,t} \geq \left( \sum_{r,TY} lhs_{r,t} + \sum_{r,TY} rhs_{r,t} \right) \cdot shr

which rearranges into the form the model carries,

.. math:: \frac{1-shr}{shr} \cdot \sum_{r,TY} lhs_{r,t} - \sum_{r,TY} rhs_{r,t} \geq 0

where :math:`shr` is the minimum share of total energy production required from
the covered technologies.

.. TODO complete the following. See iiasa/message_doc#43

Macro-economic targets
----------------------


Representation of taxes and subsidies
-------------------------------------
Another set of policies addressed as part of climate change analysis, are energy-related taxes and subsidies. Removing fossil fuel subsidies could help reduce emissions by discouraging the use of inefficient energy forms. In the global energy model, fossil fuel prices are endogenously derived based on underlying supply curves representing the technical costs associated with the extraction of the resources (cf. Section :ref:`fossilfuel`).  Refining and processing as well as transmission and distribution costs will be added to the total fuel cost. In order to account for taxes, price adjustment factors are applied, based on the underlying data set as described in Jewell et al. (2018) :cite:`jewell_subsidy_2018`.
