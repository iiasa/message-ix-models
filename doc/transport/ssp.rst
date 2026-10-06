Shared Socioeconomic Pathways (SSP)
***********************************

This page describes the quantification of the Shared Socioeconomic Pathway (SSP) narratives
in MESSAGEix-Transport.

.. contents::
   :local:

Transport narrative assumptions
===============================

Key:

- Qualitative values: **L** ow, **M** edium, **H** igh.
- Groups of countries: **LIC** low-income countries,
  **MIC** middle-income countries,
  **HIC** high-income countries.

.. list-table::
   :width: 100%
   :widths: 20 15 15 10 10 10 10 10
   :header-rows: 1

   * - Concept
     - Parameter(s)
     - Type
     - SSP1
     - SSP2
     - SSP3
     - SSP4
     - SSP5
   * - ———Passenger transport
     -
     -
     -
     -
     -
     -
     -
   * - Passenger demand
     - ``transport demand passenger::ixmp``
     - outcome constraint
     - M
     - M
     - LIC=M, M/HIC=H
     - L
     - H
   * - Energy intensity of service
     - total final energy / transport demand passenger
     - outcome constraint
     - L
     - M
     - LIC=M, M/HIC=H
     - LIC=L/M, M/HIC=L
     - H
   * - Car ownership rate
     - (not a model quantity?)
     -
     - L/M
     - M
     - L
     - divergence (across and within)
     - H
   * - Travel demand by cars
     -
     -
     - M
     - M
     - M
     - L/M
     - H
   * - 
     - PDT
     - outcome constraint
     - M
     - M
     - M
     - L/M
     - H
   * - 
     - mode share
     - outcome constraint
     - 
     -
     -
     -
     -
   * - Occupancy of LDVs
     - ``load factor:ldv``
     - input constraint
     - H
     - M
     - M
     - LIC=H, HIC=L
     - L
   * - Energy intensity of LDVs
     - ``ldv fuel economy:``
     - input constraint
     - H
     - M
     - L
     - divergence (across and within)
     - L
   * - Vehicle age distribution
     - 
     - ?
     - Long lifetimes
     - Standard
     - Long lifetimes, low maintenance
     - divergence (across and within)
     - short lifetimes, novelty driven
   * - Share of car types
     -
     -
     -
     -
     -
     -
     -
   * - Electric vehicles
     - 
     - outcome constraint
     - M
     - M
     - L
     - LIC=L, HIC=H
     - ?? / hybrid / e-fuel
   * -
     - LDV_ECE share
     - outcome constraint
     - H
     - M
     - L
     - M
     - L
   * - 
     - ``inv_cost:ldv``
     - input constraint
     - H improvement
     - M improvement
     - L improvement
     - LIC=L, HIC=H improvement
     - ?? / hybrid / e-fuel
   * -
     - ``fix_cost:ldv``
     - input constraint
     - H improvement
     - M improvement
     - L improvement
     - L
     -
   * - ICE
     - LDV_Ice share
     - outcome constraint
     - L
     - M
     - H
     - divergence (across and within)
     - H
   * - 
     - ``inv_cost:ldv``
     - input constraint
     - H
     - M
     - L
     - LIC=L, HIC=H
     - L
   * - 
     - ``fix_cost:ldv``
     - input constraint
     - H
     - M
     - L
     - divergence
     - L
   * - ———Active transport (foot, bicycle)
     -
     -
     -
     -
     -
     -
     -
   * - pkm by active travel
     -
     - (not a model quantity?)
     - H
     - M
     - L
     - L/MIC=M, HIC=H
     - L
   * - ———Urban passenger transport
     -
     -
     -
     -
     -
     -
     -
   * - Infrastructure
     - 
     - 
     - Public + active infrastructure
     - M
     - Car and air infrastructure
     - L/MIC: car, HIC: divergence
     - car and air infrastructure + drones
   * - PDT by PT (urban)
     - pkm
     - outcome constraint
     - H
     - M
     - L/M
     - M
     - L
   * - 
     - mode share
     - outcome constraint
     - H
     - M
     - L/M
     - M
     - L
   * - Electrification/tech
     -
     - outcome constraint
     - H
     - M
     - L
     - L/MIC=L, HIC=H
     - H
   * -
     - cost
     - input constraint
     - H improvement
     - M improvement
     - L improvement
     - L/MIC=L, HIC=H improvement
     - H improvement
   * - ———Regional/long-distance passenger transport
     -
     -
     -
     -
     -
     -
     -
   * - High-speed rail transport
     - pkm
     - outcome constraint
     - H
     - M
     - L
     - M
     - L
   * - 
     - mode share
     - outcome constraint
     - H
     - M
     - L
     - L/M
     - L
   * - Electrification/tech
     - pkm by electric
     - outcome constraint
     - H
     - M
     - L
     - L/MIC=L, HIC=H
     - H
   * - 
     - ``inv_cost:ldv``
     - input constraint
     - H improvement
     - M improvement
     - L improvement
     - L/MIC=L, HIC=H improvement
     - H improvment
   * - 
     - ``fix_cost:ldv``
     - input constraint
     - -
     - -
     - -
     - -
     - -
   * - 
     - ``var_cost:ldv``
     - input constraint
     - -
     - -
     - -
     - -
     - -
   * - ———Air transport (passenger)
     -
     -
     -
     -
     -
     -
     -
   * - pkm by air
     - pkm
     - outcome constraint
     - L
     - M
     - M
     - M
     - H
   * -
     - mode share
     - outcome constraint
     - L
     -
     -
     -
     -
   * - Electrification/tech
     -
     - 
     - H
     - M
     - L
     - M
     - H
   * -
     - ``inv_cost``
     - input constraint
     - H improvement
     - M
     - L improvement
     - M improvement
     - H improvement
   * - 
     - ``fix_cost:ldv``
     - input constraint
     - -
     - -
     - -
     - -
     - -
   * - 
     - ``var_cost:ldv``
     - input constraint
     - -
     - -
     - -
     - -
     - -
   * - ——— Freight transport
     -
     -
     -
     -
     -
     -
     -
   * - ——— Road freight transport
     -
     -
     -
     -
     -
     -
     -
   * - PKM [sp?] by road
     - 
     - 
     - L
     - M
     - H
     - M
     - H
   * - Electrification
     -
     -
     - H
     - M 
     - L
     - M
     - L (long-haul trucking)
   * -
     - cost
     - input constraint
     -
     -
     -
     -
     -
   * - energy intensity
     -
     -
     - L
     - M
     - H
     - M
     - H
   * - ——— Rail freight transport
     -
     -
     -
     -
     -
     -
     -
   * - PKM [sp?] by rail
     -
     -
     - H
     - M
     - L
     - M
     - M
   * - Electrification
     -
     -
     - H
     - M
     - L
     - M
     - L
   * - energy intensity
     -
     -
     -
     -
     -
     -
     -

Narrative
=========

.. note:: These appear to be quotations,
   perhaps from the original narrative/marker papers for the first edition of the SSPs.

SSP1 Sustainability
-------------------

=============================================================================  ==============
Minimum travel time / year / Cap                                               SSP1>SSP2>SSP3
Money available for traveling                                                  SSP1<SSP2<SSP3
Share of travel expenses spent on green transport                              SSP1>SSP2=SSP3
Money available for traveling                                                  […]
Income elasticity luxury transport                                             SSP1<SSP2=SSP3
Preference eco-friendly transport, preference bus, train and high speed train  SSP1>SSP2=SSP3
Freight and travel efficiency                                                  SSP1>SSP2=SSP3
Preference electric cars (<150 km)                                             SSP1>SSP2=SSP3
Future transport technology cost                                               SSP1=SSP2<SSP3 
Biofuel tax                                                                    SSP1>SSP2=SSP3
=============================================================================  ==============

High electrification (max. 80% of total transport possible)

SSP2 Middle of the road
-----------------------

- In SSP2, final energy demand for the industry, residential-and-commercial,
  and transport sectors increases by approximately 42% by 2100 over 2010 levels
  in developed countries. 
- The increase is even greater in the South, due to the drastic increase in income levels:
  final energy demand quadruples over the same period of time,
  accounting for a global share of 74% by 2100 compared to about 51% in 2010. 
- SSP3 projects a similar final energy demand in 2100 for the global North as today,
  while SSP1 sees energy demand contract slightly.
  The bulk of energy demand increase in any SSP is thus projected to come from developing countries.

Medium electrification (max. 50% of total transport possible).
For final energy intensity we utilize a linear functional form in log-log space,
for the sectorial shares we follow development patterns as identified by Schaefer (Schäfer, 2005)
(e.g., a humpback shape for industry, growing share of transportation),
and for electrification rates a logistic (S-shaped) functional form.
Across the SSPs,
we then assume that regions converge to a certain quantile
at a particular income per capita level in the future.
For example,
while final energy intensity converges quickly to the lowest quantile (0.001) in SSP1,
it converges more slowly to a larger quantile (0.5 to 0.7 depending on the region) in SSP3.
Convergence quantiles and incomes are provided for each SSP and region in Table S1.

SSP3 Regional rivalry
---------------------

Low electrification (max 10% of total transport possible).

Note: This was too extreme based on current trends so [we] abandoned it,
but kept the general idea that electrification is low in this scenario.

SSP4 Inequality
---------------

Energy consumption is driven by the demand for services (heating, cooling, etc.).
Service demands for freight and passenger kilometers traveled
and residential and commercial floor space (Fig. S2) are shown,
as indicators of the growth in demand for transportation and building energy services,
respectively.
These sectors account for 55% of final energy in 2010 (30% buildings, 25% transportation)
and 56% in 2100 (24% buildings, 32% transportation);

- Growth in transportation services, particularly freight, continues in all regions,
  such that transportation service in the HIRs is more than four-fold higher than the LIRs in 2100.

SSP5 Fossil-fulled development
------------------------------

The high energy demand in SSP5 is linked to a high demand for liquid fuels in the transport sector.
We adjusted the productivity parameters for gasoline and diesel to induce a high consumption of transport fuels in the SSP5 reference scenario.
This results in an increasing transport energy share for SSP5 as demonstrated in Figure S3.3.
At the global level, this share increases from 27% today to around 36% at the end of the century,
whereas it decreases to around 25% in SSP1.
While the developed regions stabilize their high share of transportation in SSP5,
for developing regions like Sub-Saharan Africa substantial growth of this share is assumed.

.. list-table::
   :width: 100%
   :widths: 20 10 70

   * - Service demand for transport
     - Med
     - The parameters representing household private car income elasticity
       and the industrial transport service coefficient are changed.
       For the former, med = 1.0 and low = 0.75.
       For the latter, med and low are set at a 0.5% and 1.0% annual improvement,
       respectively.
   * - Autonomous energy efficiency improvement (AEEI)
     - Med
     - A mark-up parameter for energy input in the CES and LES functions
       is changed for the industrial sector and the household sector, respectively.
       High and low are calculated as plus or minus a 1% annual change in the AEEI percentage,
       respectively.
       The LES parameters are changed according to income elasticity.
       High and low are calculated as 1.25 and 0.75 times the default value, respectively.
       SSP4 differentiates regionally varied AEEI assumptions.
       SSP4 high-income countries assume the speed of SSP1
       while low-income countries assume the speed of SSP3.
       SSP5 follows the assumption of SSP2,
       but the energy efficiency improvement of the transport sector is 0.5% higher.
   * - Transportation demand
     - Med
     - Medium transportation demand and travel intensity.
   * - Transportation costs
     - Low
     - Low fuel efficiency improvement rate and learning rate for batteries.
   * - Air pollution policies
     - High
     - Increasingly strict, well-enforces [sp?] policies
       and substantial technological improvement.
       Including rapid technology transfer to developing countries.

LED Low energy demand
---------------------

[Identical table to the one above, except for the second column:
Med, Med, Low, Low/Medium, High.]

MESSAGEix-Transport implementation
==================================

Passenger travel variables:

- p-elasticity : determines the total Passenger distance travel (PDT)
  per unit of GDP per cap.
  We use this to vary how much total passenger transport demand is required,
  lower elasticity values lead to lower PDT values for the same GDP & population values.
  For example, if you want a high (implicit) active travel and lower motorized transport,
  you will lower the elasticity value to achieve that.
- speed: relative speed among the modes determines the mode share.

Vehicle (LDV) usage variables

These variables typically determine the fuel consumption and the energy demanded by the LDVs:

- Load factor : vehicle occupancy rates.
  For instance in sharing scenarios we expect these values to go up.
- Vehicle activity : how much a vehicle travels (per year),
  this determines how many LDV vehicles are required to satisfy the PDT by LDVs.
  For instance, there is general expectation that Autonomous vehicles will lead to higher values for these
  as LDVs will spend less time parking and due to lower ownership requirements.
- Vehicle lifetime: determines when an LDV is phased out of usage.
  For instance, in case of AVs or sharing it is expected that the vehicle lifetime may go down
  due to increased usage.
- Ldv-fuel economy: this determines the fuel efficiency of the vehicles.

Vehicle technology choice

- Investment costs: these are given by vehicle technology.
  The difference between the investment costs of different LDV technologies
  is one of the main lever [sp?] for changes in uptake of BEV/PHEVs.

For the freight sector:

- Freight elasticity values (determines the total freight demand)
- Vehicle lifetime
- energy intensity of non-LDV transport technologies

Base year parametrization for final energy balances
===================================================

This is used to calculate the difference between model outcomes in the base year (year = 2020)
and IEA’s EWEB reported outcomes.
Not [sp] for IEA EWEB we use values from year = 2019 due to Covid effect.

scale-1 is given as:

- This is the "higher resolution" scaling factor.
- The t (technology) labels correspond to IEA FLOW labels
  to which our more numerous transport technologies, grouped by modes, are aggregated.
- Vice versa: the c (commodity) labels are our MESSAGE commodities,
  to which the more numerous IEA PRODUCT labels are aggregated.
- Zeros indicate places where the IEA EWEB data has a non-zero number,
  so 0 (MESSAGE output) / y = 0.
  
  - Some of these will become non-zero once we finish putting in base-year technology shares
    that will force in technologies using these fuels.

- inf indicate places where the IEA EWEB data has a zero, so x (MESSAGE output) / 0 = inf.
- Before applying scale-1 to the outputs, I clip the values to 1.0, so that x / 1.0 = x;
  i.e. the zeros and inf are discarded and the MESSAGE output passes through unaltered.
- Numerous observations to make about the values.

scale-2 is given as:

- This is the "total" scaling factor.
- This is computed after the first is already applied.
- Differences from 1.0 should be due to two things:

  - TRNONSPE and DOMESNAV.
    These would be reduced once we put in generic "other transport" technologies fixed to the sum of those.
  - The clipped values in the first stage.

Parametrization process
=======================

The base year calibration is based on fine-tuning and minimizing the scale-1 values.
The rule of thumbs employed for this fine-tuning by sectors are:

AIR:

- Most of the pkm-cap and the resulting mode-share for base year (=2020) calibration
  are based on DOM AIR values.
- Scale-1 value should be below 1, indicating a structural difference
  as IEA value being used currently includes both DOMESTIC and INTERNATIONAL AIR demands.

ROAD

The order of priority for minimizing Scale-1 values is:

1. lightoil
2. electr
3. gas
4. ethanol

For Scale-1 (ROAD, gas) adjustments:

- Scale-1 (ROAD, gas) values are only applied to ROAD, PASSENGER
- For ROAD, FREIGHT  technologies the Scale-1 (ROAD, gas) is set to 1 for year > 2020

For Scale-1 (ROAD, electr) adjustments:

- For my perspective, parameter settings that produce scale-1 (ROAD, electr) value < 1 are preferable
  as compared to settings that produce scale-1 (ROAD, electr) value > 1.
  We do not want to minimize the energy requirements in “sustainable scenarios (SSP1/LED)” where early electrification happens.
  A scale-1 (ROAD, electr) value > 1 will make the low demand in these scenarios even lower.
- Secondly, since ROAD, electr is starting from a low base
  so a scale-1 (ROAD, electr) value < 1 is not going to dramatically change it,
  whereas an equivalent scale-1 (ROAD, electr) value > 1 will make the small initial quantity even smaller.

The variables used for parameterization are (in order of priority):

- ldv-t-share
- (Passenger) Mode share
- (LDV) Load factor
- Pdt-cap, freight

So far, [we] have not used fuel efficiency/economy for regional or scenario differentiation
since [we] do not have a very good handle on how it changes the overall energy consumption.
However, it is reasonable to assume that fuel economy, non-ldv-t-share, non-ldv load factor
are probably not well-calibrated since most of these have legacy values
(and not always regionally differentiated).
A few notes on these.
First starting with ldv-t-share:

- In terms of ldv-t-share, these were reasonably well-calibrated (especially for data rich regions).
  However, given that we do not have the equivalent non-ldv-t shares operational/parameterized at the moment,
  they are the least intrusive way to balance Scale-1 (ROAD) values.
- For instance, assume a case where, in a given region (X) for year = 2020, 100% of LDVs are ICE_Conv,
  while 30% of Transportation|Truck|Gases and remaining 70% are Transportation|Truck|Liquids|Oil.
  Since, we do not have share of TRUCK stock operationalized,
  we have to find a value of ldv-t-share for region (X)
  so that the scale-1 (ROAD,gas) value is as close to 1 as possible.
  Even knowing that this value does not reflect the true ldv-t-share.
- Note these are shares and should sum to 1 so we CAN NOT change the values too dramatically
  otherwise Scale-1 values for other commodities in the ROAD sector are going to go awry.

Mode share adjustments:

- These are best employed where we are not sure about the base calibration (quite a few Global South regions)
  AND where Scale-1 values for same commodities are off in opposite directions for RAIL, ROAD or AIR.
- Within ROAD sector, mode share between 2/3-wheelers, LDV, BUS can also be adjusted for Global South regions,
  especially in cases where Scale-1 (ROAD, lightoil) values are significantly above or below 1.
- Unless there is good reason to (for example we do not have any idea about the mode share in a region),
  the changes should be minimal.

Load factor adjustments

- Same as within sector mode share changes,
  LOAD factor (ldv) changes are preferable where Scale-1 (ROAD, lightoil) is significantly below or above 1,
  while Scale-1 values for other ROAD commodities is close to 1.
- In most cases, this is not well-calibrated (with the exception of R12_NAM, R12_WEU),
  but large changes should be avoided.

pdt-cap, freight demand adjustment

- These have to be used where all/most important Scale-1 values are consistently off in one direction.
- Freight demand adjustments are preferred in cases where there is less confidence in the underlying data
  (most of GS regions)
  and where the purpose is to adjust Scale-1 (ROAD,lightoil) value without changing other Scale-1 values.
- Unless there is good reason to (for example we do not have any idea about the mode share in a region),
  the changes should be minimal.
- In this round, the biggest adjustment where required in R12_CHN and R12_SAS.
  In both cases,
  it was not possible to decrease consistently high Scale-1 values across modes, sectors, commodities
  without a decrease in pdt-cap & total tkm in year =2020.

Final thoughts:

- Eventually, when we have all technologies operational,
  and possibly with vehicle stock data available,
  we maybe able to do this process in a similar vein as the isoquant curves by T.Hara
  and/or developing an algorithm that can offer parameterization space given IEA outcomes.
- In future, Scale-1 values should only be used for structural differences in the model outcomes
  and IEA historical values, such as the AIR,lightoil consumption.
- Rest of the differences should be addressed by an additive factor
  (for example to address shortfall in lightoil consumption in ROAD sector).
  However, having two factors may also increase complexity.

Transport sector demands: passenger distance traveled (PDT)
===========================================================

Schaffer model specifications

- GDP 
- Population
- GDP-PDT (pseudo) elasticity

In a first step, total travel demand (TV) per region in a reference scenario without climate policy (business-as-usual, BAU)
is determined via equation 1 (Schafer and Victor, 2000).
The total passenger travel demand (TV, identical to ‘PDT’ above) per region in a reference scenario without climate policy
is determined via equation 1 (Schafer and Victor 2000).
The two parameters e and f* are calculated for each region
with 2020 base-year values obtained from (Schäfer et al., 2000)
and a globally harmonized assumed convergence point for annual travel and income
(which is not reached in any of the regions during the modeling time frame).
The per-capita value (“TV/cap”) is multiplied by projected population (region, period) to give total PDT.

.. todo:: Transcribe (eq. 1) 

The GDP elasticity of the total PDT is set between 0.3-1.2 depending on the region.
High-income developed region have lower elasticity values.
This is motivated by the idea that the cars per person in developed regions
is closer to saturation levels and has seen relatively modest growth.
So most of the increase in PDT is driven by more intensive usage of cars.
Whereas for low-income regions, the growth in PDT is driven both by increase in cars per person as well as more intensive usage.

.. todo:: Add the justification of different elasticities based on vehicles per person estimation by SSP, region & relate it to Dargay et al 2007]

.. note:: A tabular presentation of :file:`R12/elasticity-p.csv` for year=2110 appears here.

Low energy demand scenarios
---------------------------

We over-hauled the way transport demands are calculated in LED scenario. The basic approach is as follow:

i. Assume fixed travel time budget [TTB] (~50 mins for motorized travel)
ii. Derive Average mode speeds based on:
    a. maximum speed limits for personalized urban travel (average speed for LDV & 2-3 wheelers is 20 km/hr),
       and similarly safe speed limits for long-distance personalized travel
       (average speed for LDV & 2-3 wheeler is 30 & 50 km/hr respectively),
    b. share of urban & long distance travel, especially for LDV and 2-3 wheelers.
       This share will be different for LED-SSP 1 variation as compared to LED-SSP 2.
iii. Use these new speeds to calculate the total pdt and mode share. This means
iv. If TTB is not breached in a region, the pdt and mode share takes the parent SSP trajectory
    (i.e. SSP 1 for LED-SSP 1 and SSP 2 for LED-SSP 2)
v. If TTB is breached the Total pdt goes down and pdt for each mode goes down proportionally.
vi. All regions must not breach TTB in 2110.

Mode share
==========

Service demands (in passenger-km) for the different modes are determined
by calculating the share Si,r,t of each mode i in region r at time t
via multi-nominal logit functions (as in eq. 2)
and multiplying this with the total regional PDT as obtained by eq. 1. 

.. todo:: Transcribe (eq. 2)

with the logit exponent λ = -2 as used in (Kyle and Kim 2011),
and share weights SWi,r that partially converge across regions as a function of income.

.. todo:: Transcribe (eq. 3)

The structure and parameterization of the mode-sharing approach is based on Kyle and Kim (2011)
and considers the generalized price per mode Pi,r,t, consisting of
a fuel price, non-fuel price and value of time component (eq. 3).
The endogenous value of time multiplier, VOTM, increases with higher income.

Where;

- Endogenous value of time multiplier VOTM increasing from 0.2 – 0.9 with higher income 

Discrete choice model parameters

- Mode speed (exogenous SSP specific)
- Share weight convergence (same for all SSPs)
- Value of time (dependent on SSP drivers)
- Fuel costs (endogenously determined in the model)

.. note:: A tabular presentation of the :attr:`.transport.config.Config.share_weight_convergence`
   settings.

.. note:: A tabular presentation of :file:`R12/speed.csv`,
   with the following in a “Comments” column:

   - 2W: No change implemented in 2/3 wheeler speeds, comparatively they are high
   - AIR: The expected changes in the AIR mode are due to improvements or deterioration of connections
     to & from the airport, not necessarily planes getting faster/slower.
   - BUS: Changes in BUS speed are mainly due to
     (i) better intra-city/intercity bus vehicles,
     (ii) the bigger improvements are due to dedicated bus lanes such as BRT systems especially in SSP1,
     (iii) increasing share of intra-city bus services.
   - LDV: Change in LDV speed are mostly due to optimization of travel patterns,
     especially congestion management in cities,
     (ii) higher share of long-distance & suburban travel,
     (iii) safer vehicles for higher speeds, including non-human driving.
   - RAIL: Changes in RAIL speed are mainly due to
     (i) better intra-city/intercity RAIL vehicles,
     (ii) the bigger improvements are due to higher share of High speed rail systems especially in SSP1,
     (iii) increasing share of intra-city RAIL services.

.. todo:: add the shares of different vehicle types (for instance high-speeed rail for RAIL) within each mode category that justify the Speed increases.

Vehicle usage behaviour
=======================

The three parameters underlined below determine the total number of vehicles in operation.
These are especially important when considering the role of car-sharing, ride-sharing and ride-hailing in LDV demands.
In an enterprise model of carsharing, the typical vehicle is used extensively (so VKT),
and has a relatively higher load factor (although does not necessarily have to be),
this means that the vehicle lifetime is decreased due to higher usage and higher weight.
However, overall this translates to lower number of cars required.
Meanwhile for ridesharing, the main impact is on vehicle occupancy (load factor).
As described in Table on SSP transport-related narrative assumptions table, 

.. todo:: Add simulation description that yield the combined set of values.

.. note:: A tabular presentation of :file:`R12/load-factor-ldv.csv` for 2020 (all SSPs) and 2110 (each SSP and LED).
.. note:: A tabular presentation of :file:`R12/ldv-activity.csv`.
.. note:: A tabular presentation of :file:`R12/lifetime-ldv.csv` with columns:

   - Node
   - Base (vintage year) 1990
   - SSP1–5 2110 
   - LED 2110

[Future] Improvements

- Add vehicle lifetime by technology (implemented but not yet experimented with)
- Add battery technology separately (maybe)

Vehicle technology choice
=========================

Cost optimization

- Investment costs
- Fixed costs
- O&M costs
- Fuel costs (endogenously determined in the model)

The main difference between different SSP scenarios stems from difference between investment cost of different technologies. Below, we give representative cases of investment cost trajectories for key technologies (BEV, PHEV, ICEV) for two region (R12-CHN & R12-MEA) under different SSP settings. Overall, the main noticeable thing is that in R12-CHN, the ICE to BEV/PHEV cost differential is closed pretty quickly and completely in most scenarios, whereas in R12-MEA this is only true for SSP1, other scenarios do not see the cost differential being closed in this region. The underlying motivation for this comes from recent historical developments in LDV manufacturing and the domestic fossil fuels.

.. note:: A figure “Investment costs for R12_CHN”:

   - Panels: SSP
   - X: year
   - Y: inv_cost [USD]
   - Lines with marks: BEV, ICE, PHEV

.. note:: A similar figure “Investment costs for R12_MEA”.

Fuel efficiency
===============

Updated as a part of CircEUlar.
Ask for the current version with Takuya's method for calibration.

Non-LDV stocks & techno-economic calibration
============================================

To be updated as a part of CircEUlar.

Freight demands
===============

As with passenger sector, freight demands are calculated based on:

- GDP 
- Population
- GDP- (pseudo) elasticity

Broadly speaking, the process can be broken down into the following steps:  

1. Parameterization of freight demands (by mode) for the base year  (model year = 2020)
2. Describe **Road/Rail freight demand** relationship between
   **time-invariant structural factors** (land area, etc.),
3. **SSP elements** (GDP and its structural composition, pop & its structure, urbanization),
   and **costs** (bag of things for now since the actual costs are not easy to construct).  
4. Transform the outcomes from (2) so they can be mimicked by the **pseudo-elasticity factor**
   (elasticity-f) we are using by Freight Mode, Regions, and Scenarios.  
5. Design separate LED freight demands.
   Note, the idea is to keep refining (2) so it can ultimately replace (3) to a large extent.
   However, not within the ScenarioMIP timelines.

As with Passenger demand,
given the expectation to imagine different relationships between freight demand and GDP
(historical, normative, differentiated across SSPs).
[We] imagine these pseudo-elasticities will be kept as a lever for scenario implementation.

Freight base year parameterization
==================================

For freight rail data we use
`link <https://databank.worldbank.org/reports.aspx?source=2&series=IS.RRS.GOOD.MT.K6&country=#>`__.
This has the most extensive road freight data (for year = 2018),
with some doubts on data quality since the source is not clear.

`link <https://databank.worldbank.org/source/country-climate-and-development-report-(ccdr)#>`__.

Rest of the sources are used for double checking and gap-filling.

- OECD
  
  `1 <https://sdmx.oecd.org/public/rest/data/OECD.ITF,DSD_TRENDS@DF_TRENDSFREIGHT,1.0/.A.....?startPeriod=1990&endPeriod=2023>`__,
  `2 <https://sdmx.oecd.org/public/rest/data/OECD.ITF,DSD_TRENDS@DF_TRENDSFREIGHT,1.0/.A.....?startPeriod=1990&endPeriod=2023&dimensionAtObservation=AllDimensions>`__.
- UNECE

  Good transport on national territory by road by Type of goods transport,
  Topic, Country and Year. UNECE Statistical Database

- ATO
  `link <https://asiantransportobservatory.org/data/>`__.

The aim is to keep the values from the most extensive database,
and the one closest to IEA estimates (unless we have reason to doubt it).

Country sources:

- NAM:
  US, https://www.eia.gov/outlooks/aeo/data/browser/#/?id=7-AEO2023&region=0-0&cases=ref2023&start=2021&end=2050&f=A&linechart=ref2023-d020623a.5-7-AEO2023&sourcekey=0
  https://www.bts.gov/content/us-ton-miles-freight
- EU
  https://ec.europa.eu/eurostat/web/transport/database

f-elasticity values for SSPs
============================

Calculate a random-effect panel model 

.. todo:: Transcribe unnumbered equation that appears here.

- i: Index for countries/regions (1 to N).
- t: Index for time .
- yit: The dependent variable for country/region i at time t.
- β0: The intercept.
- Xit: A vector of independent variables for country i at time t.
- β: A vector of coefficients corresponding to the independent variables.
- ui: The individual-specific effect or random effect for country i. This captures unobserved heterogeneity across countries that is constant over time.
- εit: The time-specific error term for country i at time t.

Calculate the average marginal effects and average partial effects

Margins, dydx

Some notes on regions with exceptions

- NAM (historically different) and is kept that way compared to the rest of the Global North countries.
- FSU (mainly because of high RAIL share in RUS)

Total effective elasticity = (road tkm elasticity + (0.5* rail tkm elasticity) )/2

This is bare-bone implementation.
We should be able to expand on it more.
Especially at the country level.
That can then be aggregated up.

LED freight activity
====================

i. Identify a set of target countries

   Criteria: low freight demand per capita, high GDP per capita
   [Japan, UK, Belgium, Switzerland, Netherlands, Germany, France, Austria, Denmark, Sweden]
ii. Calculate the mean freight tkm per capita
    This would be better with rail & road separately, however, currently all together. ~3800 tkm per cap
iii. Use this value as a target, with an additional factor whose value differs by region:

     - 1.5x --> R12_Africa (big continent, low pop density, even with 3 billion people)
     - 2x --> R12_NAM (low population density)
     - 2x --> R12_FSU (Very low pop density, Very high RAIL freight share, questionable choice)
     - 1.25 --> R12_EEU (low pop density compared to WEU)
     - 1.25 --> R12_PAO (mostly for Australia & NZ, even though they have a smaller population share in R12_PAO)
     - Otherwise 1
     - Target tkm per cap (node, 2100) = 3800* factor(node)

iv. Calculate the elasticity values by region

    - Target tkm/cap value as above
    - Use GDP/cap in the given SSP orientation
    - Calculate the elasticity values needed to achieve the target tkm/cap for given GDP trajectory
    - Use the estimated pseudo-elasticity values for year == 2110 for implementation in the MixT

Planned improvements and fixes (general)
========================================

- Different elasticities for different modes (this has now been implemented post SSP).
- Incorporate SSP extensions related to GDP composition
- Cross-price elasticities for mode-shift
