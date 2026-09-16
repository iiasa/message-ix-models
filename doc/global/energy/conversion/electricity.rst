.. _electricity:

Electricity
***********

|name| covers a large number of electricity generation options utilizing a wide range of primary energy sources.
For fossil-based electricity generation technologies, typically a number of different technology variants with different efficiencies, environmental characteristics and costs are represented
For example, in the case of coal,
|name| distinguishes subcritical and supercritical pulverized coal (PC) power plants
where the subcritical variant is available with and without flue gas desulpherization/denox
and one internal gasification combined cycle (IGCC) power plant.
The supercritical PC and IGCC plants are also available with carbon capture and storage (CCS) which also can be retrofitted to some of the existing PC power plants (see :numref:`fig-elec-fossil-nuc`).
:numref:`tab-elec` below shows the different power plant types represented in |name|.

.. _fig-elec-fossil-nuc:
.. figure:: /_static/electricity_generation_fossil_nuclear.png

   Schematic diagram of the fossil and nuclear power plants represented in |name|.

Four different nuclear power plant types are represented in |name|,
i.e. two light water reactor types,
a fast breeder reactor,
and a high temperature reactor,
but only the two light water types are included in the majority of scenarios
being developed with |name| in the recent past.
In addition, |name| includes a representation of the nuclear fuel cycle,
including reprocessing and the plutonium fuel cycle,
and keeps track of the amounts of nuclear waste being produced.

The conversion of five renewable energy sources to electricity is represented in |name|
(see :numref:`fig-elec-renewable`).
For wind power,
both on- and offshore electricity generation are covered
and for solar energy,
utility-scale photovoltaics (PV),
rooftop PV
and solar thermal (concentrating solar power, CSP) electricity generation
are included in |name|
(see also sections on :ref:`renewable` and :ref:`syst_integration`).
Utility-scale PV feeds the secondary electricity level
and its output passes through transmission and distribution like that of any other power plant.
Rooftop PV supplies electricity directly at the final energy level
to the residential and commercial sector and to industry.
Consequently, its output therefore does not pass through transmission and distribution
and therefore is not subject to the system integration constraints described in :ref:`syst_integration`,
and its deployment is limited by the rooftop resource potential described in :ref:`renewable`.
Two CSP technologies are modeled: (1) a flexible plant with a solar multiple of one (SM1) and 6 h of thermal storage and (2) a baseload plant with a solar multiple of three (SM3) and 12 h of storage (Johnson et al. 2016, :cite:`johnson_vre_2016`).

.. _fig-elec-renewable:
.. figure:: /_static/electricity_generation_renewable.png

   Schematic diagram of the renewable power generation options represented in |name|.

Most thermal power plants offer the option of coupled heat production (CHP, see :numref:`tab-elec`). This option is modeled as a passout turbine via a penalty on the electricity generation efficiency.
In addition to the main electricity generation technologies described in this section,
also the co-generation of electricity
in conversion technologies primarily devoted to producing non-electric energy carriers
(e.g., synthetic liquid fuels)
is included in |name|
(see section on :ref:`other`).

.. _tab-elec:
.. table:: List of electricity generation technologies represented in |name| by energy source.

   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | Energy source    | Technology                                                                                                                                                                                                                                                                             | CHP option               |
   +==================+========================================================================================================================================================================================================================================================================================+==========================+
   | coal             | subcritical PC power plant without desulphurization/denox                                                                                                                                                                                                                              | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | subcritical PC power plant with desulphurization/denox                                                                                                                                                                                                                                 | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | supercritical PC power plant with desulphurization/denox                                                                                                                                                                                                                               | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | supercritical PC power plant with desulphurization/denox and CCS                                                                                                                                                                                                                       | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | IGCC power plant                                                                                                                                                                                                                                                                       | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | IGCC power plant with CCS                                                                                                                                                                                                                                                              | yes                      |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | oil              | heavy fuel oil steam power plant                                                                                                                                                                                                                                                       | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | light fuel oil steam power plant                                                                                                                                                                                                                                                       | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | light fuel oil combined cycle power plant                                                                                                                                                                                                                                              | yes                      |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | gas              | gas steam power plant                                                                                                                                                                                                                                                                  | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | gas combustion turbine gas                                                                                                                                                                                                                                                             | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | combined cycle power plant                                                                                                                                                                                                                                                             | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | combined cycle power plant with CCS                                                                                                                                                                                                                                                    | yes                      |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | nuclear          | nuclear light water reactor (Gen II)                                                                                                                                                                                                                                                   | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | nuclear light water reactor (Gen III+)                                                                                                                                                                                                                                                 | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | fast breeder reactor                                                                                                                                                                                                                                                                   |                          |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | high temperature reactor                                                                                                                                                                                                                                                               |                          |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | biomass          | biomass steam power plant                                                                                                                                                                                                                                                              | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | biomass IGCC power plant                                                                                                                                                                                                                                                               | yes                      |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | biomass IGCC power plant with CCS                                                                                                                                                                                                                                                      | yes                      |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | hydro            | hydro power plant (2 cost categories)                                                                                                                                                                                                                                                  | no                       |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | wind             | onshore wind turbine                                                                                                                                                                                                                                                                   | no                       |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | offshore wind turbine                                                                                                                                                                                                                                                                  | no                       |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | solar            | solar photovoltaics (PV), utility-scale                                                                                                                                                                                                                                                | no                       |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | solar photovoltaics (PV), rooftop                                                                                                                                                                                                                                                      | no                       |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | concentrating solar power (CSP) with a solar multiple of 1 (SM1)                                                                                                                                                                                                                       | no                       |
   |                  +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   |                  | concentrating solar power (CSP) with a solar multiple of 3 (SM3)                                                                                                                                                                                                                       | no                       |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+
   | geothermal       | geothermal power plant                                                                                                                                                                                                                                                                 | yes                      |
   +------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+

Calibration of the existing fleet
---------------------------------

The power plant fleet in |name| is calibrated from two main sources,
complemented with more detailed data for specific technologies.
Unit- and vintage-specific data on installed and operational power plants
from the World Electric Power Plants Database (S&P Global Platts, 2021 :cite:`platts_wepp_2021`)
are used to calibrate vintage-specific capacity additions from 1960 to 2020.
Power generation from 1990 to 2020 is based on the World Energy Balances (IEA, 2024 :cite:`iea_web_2024`),
the same source used to calibrate fossil extraction levels and final energy demand,
which ensures a consistent calibration across the entire energy system.
For thermal power plants,
the unit data are mapped onto the representative technologies of :numref:`tab-elec`,
which span the different fuels,
biomass, coal including lignite, natural gas including blended biogas or hydrogen, and light and heavy fuel oil,
and the different conversion types.
Capacity and generation together give the current capacity factors of the fleet,
the fraction of the year for which a unit effectively operates at full load.
Combined with the efficiency parametrisation of each plant type,
this captures current regional operating practices
and their transition towards the assumed future default operation.
For non-thermal power plants,
recent capacity trends for wind, solar, hydro and geothermal power
are calibrated to the statistics of the International Renewable Energy Agency (IRENA, 2025 :cite:`irena_capacity_2025`),
which track these markets more swiftly than the two main sources.
For solar PV and wind,
average capacity factors of the installations added in each period are derived from their power output,
and existing installations are kept in place and replaced in place at the end of their technical lifetime,
because they sit where grid integration and permitting are already in place
(see :ref:`renewable`).

For nuclear power plants an additional multi-year regulatory licensing process applies,
covering site, construction and operating licences.
Together with construction times of several years,
these lead times are why only projects that are already operational or under construction
are assumed to be realisable before 2035.
Nuclear capacity additions until 2035 are therefore based on the projects listed in the
Power Reactor Information System of the International Atomic Energy Agency (IAEA, 2024 :cite:`iaea_pris_2024`),
including those that are operational or under construction (:numref:`fig-nuc-capacity`).
The existing stock also accounts for approximately 21 GW currently in suspended operation,
predominantly Japan's idle fleet.
For reactors under construction the most ambitious realisation period is assumed,
seven years from construction start for those in China or built by Chinese firms
and 8 to 10 years on average elsewhere,
although ongoing construction has already run longer in some instances.
Given the current situation in Ukraine,
no completion estimate could be made for the two reactors under construction at the Khmelnitsky plant.
These capacities are also those assumed under policy scenarios,
even where more ambitious targets have been set,
as for example in India,
where the capacity under construction falls far short of the national nuclear programme target for 2030
(Fricko et al., 2026 :cite:`fricko_wu_2026`).


.. _fig-nuc-capacity:
.. figure:: /_static/nuclear_capacity_additions.png
   :width: 700px

   Nuclear power capacity.
   Regional nuclear power capacity (GW), shown as a waterfall over the periods 2020, 2025, 2030 and 2035.
   The 2020 bar is the existing capacity stacked by region.
   Each later bar shows the capacity added in that period,
   stacked by region and starting from the running cumulative total,
   with the period total addition labelled to the right of the bar
   and a dashed black line carrying each cumulative total across to the next
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).

Investment cost assumptions
---------------------------

A key differentiation between the SSP narratives is achieved by varying techno-economic assumptions,
specifically the narrative-aligned future improvements in investment and fixed operation and maintenance costs,
which are derived exogenously (see :ref:`techchange`).
Base-year costs for all conversion technologies are based on a literature review
collated for the North America region, the reference region.
After 2025, the reference region's costs decline from their base-year value
following a technology- and narrative-specific reduction rate,
an exponential decline in the spirit of Moore's law (Moore, 1998 :cite:`moore_cramming_1998`),
calibrated so that the long-run reduced value is reached in 2150, beyond the model horizon.
Reduction rates are first specified qualitatively, from very low to very high,
and aligned to reflect each SSP narrative consistently.
All other regions are tied to the reference region through cost ratios.
Base-year ratios are derived from the World Energy Outlook (IEA, 2023 :cite:`iea_weo_2023`)
and then evolve with regional economic development.
A linear relationship between each technology's regional cost ratio and the GDP per capita ratio,
both relative to North America,
moves each region's costs over time,
so that as a region's GDP per capita approaches that of North America
its technology costs converge towards the reference region cost.
A safeguard prevents the regional cost ratios from drifting in the wrong direction
for the few regions, such as Pacific OECD and Western Europe,
that have a lower GDP per capita but higher base-year costs than North America
(Fricko et al., 2026 :cite:`fricko_wu_2026`).

:numref:`fig-ther` shows the narrative-aligned investment cost developments for thermal power plants
without CCS (panel (a)) and with CCS (panel (b)),
following each narrative's resource story.
In the sustainability narratives, LED and SSP1,
unabated coal, that is subcritical and supercritical coal and IGCC,
sees no cost improvement to 2100
and in some regions rises slightly as lower-cost regions converge towards North American levels,
while efficient gas plants still improve.
SSP3, with its continued reliance on coal,
shows the largest cost declines for unabated coal, around 45% in the reference region,
while its low-carbon options progress slowly.
Biomass IGCC declines least in SSP3
and its CCS technologies remain the most expensive of any narrative.
SSP4 combines a wide spread of costs across regions with declines for conventional and CCS technologies alike.
SSP2 and SSP5 are broadly similar and sit mid-range for most technologies,
with the main difference that SSP5, alongside SSP4, achieves the strongest cost reductions for fossil CCS.

.. _fig-ther:
.. figure:: /_static/thermal_power_investment_costs.png
   :width: 700px

   Thermal power plant investment cost developments across narratives.
   Investment cost ranges (USD2005/kW) by technology for thermal power generation,
   shown for the base year (black, SSP2) and for 2100 by narrative (LED and SSP1 to SSP5).
   Panel (a) shows technologies without CCS and panel (b) those with CCS.
   Each coloured bar spans the range across the twelve regions
   and the red diamond marks the North America value
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).
   ST stands for steam turbine, CT for combustion turbine, CC for combined cycle,
   CCGT for combined cycle gas turbine, IGCC for integrated gasification combined cycle,
   SubC. for subcritical and SuperC. for supercritical.

:numref:`fig-nonth` shows the corresponding developments for non-thermal power plants.
Reflecting the expectation that nuclear energy will gain in importance again (IEA, 2025 :cite:`iea_nuclear_2025`),
nuclear investment costs come down the most in SSP5, but also in SSP4 and SSP2.
Hydropower costs are not assumed to change,
both because of environmental concerns
and because much of the currently accessible potential is already exploited.
Solar and wind follow the opposite pattern.
Their costs fall across all narratives,
with the steepest reductions in the sustainability narratives LED and SSP1, but also in SSP4.

.. _fig-nonth:
.. figure:: /_static/non_thermal_power_investment_costs.png
   :width: 700px

   Non-thermal power plant investment cost developments across narratives.
   Investment cost ranges (USD2005/kW) by technology for non-thermal power generation, renewables and nuclear,
   shown for the base year (black, SSP2) and for 2100 by narrative (LED and SSP1 to SSP5).
   Each coloured bar spans the range across the twelve regions
   and the red diamond marks the North America value
   (Fricko et al., 2026 :cite:`fricko_wu_2026`).
