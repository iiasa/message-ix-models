.. _fossilfuel:

Fossil Fuel Reserves and Resources
====================================
The availability and costs of fossil fuels influences the future development of the energy system, and therewith future mitigation challenges. Understanding the variations in
fossil fuel availability and the underlying extraction cost assumptions across the SSPs is hence important. Our fossil energy resource assumptions in |MESSAGEix| are derived from various sources, including
global databases such as The Federal Institute for Geosciences and Natural Resources (BGR, 2024 :cite:`bgr_energiedaten_2024`) and The U.S. Geological Survey
(`USGS <https://www.usgs.gov/energy-and-minerals/energy-resources-program/science/energy-resources>`_), as well as market reports and outlooks provided by different energy institutes and agencies.
The availability of fossil energy resources in different regions under different socio-economic assumptions are then aligned with the storylines of the individual SSPs
(Rogner, 1997 :cite:`rogner_assessment_1997`; Riahi et al., 2012 :cite:`riahi_chapter_2012`).
Quantitative assessments of fossil resources have changed little in recent years,
as national policies favouring renewable and low-emission technologies have limited exploration activity
and, to a lesser extent, discoveries have not been reported.

The resource base is built in several steps.
Country-level reserve and resource data for conventional and unconventional occurrences
of crude oil, natural gas, hard coal and lignite
are taken from the BGR assessment (BGR, 2024 :cite:`bgr_energiedaten_2024`).
For crude oil and natural gas,
undiscovered conventional resources are added from the USGS assessment
at a selected probability level,
including the assessment units that USGS reports jointly for several countries,
and clathrates and gas in aquifers are added to the natural gas resource base.
The share of each category assumed recoverable is set by a recovery rate factor.
The resulting volumes are aggregated to the twelve model regions
and allocated to categories, or grades, of extraction complexity and cost.
The extractable volume, that is the world's hydrocarbon endowment,
is held fixed across the SSP narratives,
while the long-run extraction costs reflect the different scenario drivers and narratives
(:numref:`fig-supply`).
What ultimately determines the attractiveness of a particular type of resource
is not just the cost at which it can be brought to the surface,
but the cost at which it can be used to provide energy services,
so assumptions on fossil energy resources should be considered together with those on related conversion technologies.

The costs of extracting crude oil and natural gas are largely infrastructure-driven,
and their long-term development reflects demand-driven technological learning.
Extraction costs for each oil and gas grade rise exponentially from current levels
toward a long-run level specific to the narrative, reached around 2150,
counterbalanced by narrative-specific innovation in extraction technologies.
Narratives with less progress in extraction technology therefore remain on a higher and costlier curve,
while those with more progress see today's high-cost unconventional grades
decline toward the cost levels of conventional grades.
SSP1 and SSP4 have the highest long-term oil and gas costs,
SSP1 because low fossil fuel demand limits deployment-driven learning
and SSP4 because weak international cooperation hampers innovation sharing.
SSP2 including the LED variant, SSP3 and SSP5 converge to the lower-cost end of the range.
SSP3 is placed there deliberately,
because cheaper oil and gas moderate what would otherwise be
an unrealistically coal-dominated pathway from today's perspective.

Coal extraction costs are instead largely workforce related.
Following the cost logic of the coal module of the National Energy Modeling System
(EIA, 2025 :cite:`eia_nems_coal_2025`),
mine mouth prices rise with wages and fall with labour productivity.
Coal extraction costs therefore escalate over time with regional per capita income,
a proxy for coal sector wages,
and are dampened by an assumed productivity improvement rate,
so that they track the income drivers of each narrative.
Limited income growth, as in SSP3, keeps future coal extraction costs close to current levels,
while strong income growth, as in SSP5, results in the highest coal extraction costs.

Estimating fossil fuel reserves is built on both economic and technological assumptions. With an improvement in technology or a change in purchasing power, the amount that may be considered a
“reserve” vs. a “resource” (generically referred to here as resources) can actually vary quite widely.

‘Reserves’ are generally defined as being those quantities for which geological and
engineering information indicate with reasonable certainty that they can be recovered in the future from known reservoirs under existing economic and operating conditions.
‘Resources’ are detected quantities that cannot be profitably recovered with the current technology, but might be recoverable in the future, as well as those quantities that are geologically
possible, but yet to be found. The remainder are ‘Undiscovered resources’ and, by definition, one can only speculate on their existence. Definitions are based on Rogner et al. (2012)
:cite:`rogner_chapter_2012`.

:numref:`tab-global-ff-res` gives the resource base by fuel and category
as it enters |MESSAGEix|,
split into reserves and resources following the definitions above.
Because the resource base is held fixed across the narratives,
a single set of volumes applies to all of them,
and it is the extraction cost curves rather than the volumes that differ.

.. _tab-global-ff-res:
.. list-table:: Global fossil resource base in |MESSAGEix| by category, in ZJ, from the assessment described in the text.
   :widths: 34 16 16 16
   :header-rows: 1

   * - Resource
     - Reserves [ZJ]
     - Resources [ZJ]
     - Total [ZJ]
   * - Hard coal
     - 19.2
     - 397.3
     - 416.5
   * - Lignite
     - 3.6
     - 41.0
     - 44.6
   * - Crude oil, conventional
     - 14.7
     - 11.4
     - 26.2
   * - Crude oil, unconventional
     - 3.0
     - 14.0
     - 16.9
   * - Natural gas, conventional
     - 16.8
     - 20.5
     - 37.3
   * - Natural gas, unconventional
     - 0.5
     - 24.7
     - 25.2

Coal is by far the largest fossil resource.
Hard coal and lignite together amount to about 460 ZJ,
roughly four fifths of a total fossil resource base of about 570 ZJ,
and almost all of it sits in the resource rather than the reserve category.
Oil is the scarcest fossil fuel,
with 26 ZJ of conventional and 17 ZJ of unconventional crude oil,
and it is the only fuel for which reserves make up more than half of the conventional endowment.
Natural gas is more abundant than oil in both categories,
with 37 ZJ conventional and 25 ZJ unconventional,
the latter almost entirely in the resource category.

:numref:`fig-supply` presents the global fossil resource cost curves for 2100,
one curve per narrative,
with the common 2020 base-year cost curve shown in black for reference.

.. _fig-supply:
.. figure:: /_static/fossil_resource_cost_curves.png
   :width: 750px

   Fossil resource cost curves.
   Global fossil resource cost curves by narrative (LED and SSP1 to SSP5)
   for natural gas (a), crude oil (b), and brown and hard coal (c), shown for 2100.
   Each coloured line traces cumulative extractable volume (ZJ)
   against the long-run extraction cost ($2005/GJ),
   and the black line is the common 2020 base-year cost curve
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).

Conventional oil and gas are distributed unevenly throughout the world,
with only a few regions dominating the reserves.
Over 40% of conventional oil reserves are found in the Middle East and North Africa,
and about a third of conventional gas in Russia and the Former Soviet Union states.
The situation is different for unconventional oil,
of which North and Latin America together hold close to three quarters.
Unconventional gas is distributed more evenly than any other fossil category,
with North America holding the largest share at roughly one fifth.
Coal reserves are spread more evenly than conventional oil and gas,
although the coal resource base as a whole is concentrated,
with North America and China together holding about 70% of the total.
North America, China, Pacific OECD, and Russia and the Former Soviet Union states
each hold more than 40 ZJ of coal reserves and resources,
which in the more fragmented SSP3 world contributes to increased overall reliance on this resource.
