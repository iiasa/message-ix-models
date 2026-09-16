.. _grid:

Grid, Infrastructure and System Reliability
===========================================

Energy Transmission and Distribution Infrastructure
---------------------------------------------------

Energy transport and distribution infrastructure is included in MESSAGE at a level relevant to represent the associated costs as well as transmission and distribution losses. Within individual model regions the capital stock of transmission and distribution infrastructure and its turnover is modeled for the following set of energy carriers:

* electricity
* district heat
* natural gas
* hydrogen

For all solid (coal, biomass) and liquid energy carriers (oil products, biofuels, fossil synfuels) a simpler approach is taken and only transmission and distribution losses and costs
are taken into account.

Inter-regional energy transmission infrastructure, such as natural gas pipelines and high voltage electricity grids, are also represented between geographically adjacent regions. Solid and liquid fuel trade is, similar to the transmission and distribution within regions, modeled by taking into account distribution losses and costs. A special case are gases that can be traded in liquified form, i.e. liquified natural gas (LNG) and liquid hydrogen, where liquefaction and re-gasification infrastructure is explicitly represented in addition to the actual transport process.

.. _syst_integration:

Systems Integration and Reliability
-----------------------------------

The global MESSAGE model includes a single annual time period within each modeling year characterized by average annual load and 12 geographic regions.
Seasonal and diurnal load curves and spatial issues such as transmission constraints or renewable resource heterogeneity are treated in a stylized way in the model.
The mechanism to represent power system reliability in MESSAGE is based on (Sullivan et al., 2013 :cite:`sullivan_electric_2013`). This method elevates the stylization of temporal resolution by introducing two concepts,
peak reserve capacity and general-timescale flexibility (for mathematical representation see this `Section <https://docs.messageix.org/en/stable/model/MESSAGE/model_core.html#system-reliability-and-flexibility-requirements>`_). To represent capacity reserves in MESSAGE, a requirement is defined that each region build sufficient firm generating capacity to maintain reliability through reasonable load and contingency events. As a proxy for complex system reliability metrics, a reserve margin-based metric was used, setting the capacity requirement at a multiple of average load, based on electric-system parameters.
The implementation follows the reduced-form approach of Johnson et al. (2016 :cite:`johnson_vre_2016`),
which applies the two concepts to wind and solar PV alike
and adds a third, the curtailment of variable generation at high penetration.
Wind and solar generation is assigned to penetration bins,
defined by the share of electricity demand that the respective technology supplies.
In SSP2 the second, third and fourth bin begin at about 6%, 17% and 29% of electricity demand.
Each bin carries its own coefficients for the three constraints.

Toward meeting the firm capacity requirement, conventional generating technologies contribute their nameplate generation capacity while variable renewables contribute a capacity value that declines as the market share of the technology increases. This reflects the fact that wind and solar generators do not always generate when needed, and that their output is generally self-correlated. In order to adjust wind capacity values for different levels of penetration, it was necessary to introduce a stepwise-linear supply curve for wind power (shown in the :numref:`fig-windcap` below). Each bin covers a range of wind penetration levels as fraction of load and has discrete coefficients for the two constraints. The bins are predefined, and therefore are not able to allow, for example, resource diversification to increase capacity value at a given level of wind penetration.

.. _fig-windcap:
.. figure:: /_static/wind_cv.png
   :width: 600px

   Parameterization of the wind capacity value by penetration bin (Sullivan et al., 2013 :cite:`sullivan_electric_2013`).

The capacity value bins are independent of the wind supply curve bins that already existed in MESSAGE, which are based on quality of the wind resource. That supply curve is defined by absolute wind capacity built, not fraction of load; and the bins differ based on their annual average capacity factor, not capacity value. Solar PV is treated in a similar way as wind with the parameters obviously being different ones. In contrast, concentrating solar power (CSP) is modeled very much like dispatchable power plants in MESSAGE, because it is assumed to come with several hours of thermal storage, making it almost capable of running in baseload mode.

In order to ensure adequate reserve dispatch, dynamic shadow prices are placed on capacity investments of intermittent technologies (e.g., wind and solar). The prices are a function of the cumulative installed capacity of the intermittent technologies, the ability for the convential power supply to act as reserve dispatch, and the demand-side reliability requirements. For instance, a large amount of storage capacity should, all else being equal, lower the shadow price for additional wind. Conversely, an inflexible, coal- or nuclear-heavy generating base should increase the cost of investment in wind by demanding additional expenditures in the form of natural gas combustion turbines or storage or improved demand-side management to maintain system reliability.

Starting from the energy metric used in MESSAGE (electricity is considered as annual average load; there are no time-slices or load-curves), the flexibility requirement uses MWh of generation as its unit of note. The metric is inherently limited because operating reserves are often characterized by energy not-generated: a natural gas combustion turbine (gas CT) that is standing by, ready to start-up at a moment’s notice; a combined-cycle plant operating below its peak output to enable ramping in the event of a surge in demand. Nevertheless, because there is generally a portion of generation associated with providing operating reserves (e.g. that on-call gas CT plant will be called some fraction of the time), it is posited that using generated energy to gauge flexibility is a reasonable metric considering the simplifications that need to be made. Furthermore, ancillary services associated with ramping and peaking often do involve real energy generation, and variable renewable technologies generally increase the need for ramping.

Electric-sector flexibility in MESSAGE is represented as follows: each generating technology is assigned a coefficient between -1 and 1 representing (if positive) the fraction of generation from that technology that is considered to be flexible or (if negative) the additional flexible generation required for each unit of generation from that technology. Load also has a parameter (a negative one) representing the amount of flexible energy the system requires solely to meet changes and uncertainty in load. :numref:`tab-flex` displays the coefficients as implemented in |name|.
They build on parameters estimated with a unit-commitment model that commits and dispatches a fixed generation system
at hourly resolution to meet load and ancillary service requirements
while hewing to generator and transmission operation limitations (Sullivan et al., 2013 :cite:`sullivan_electric_2013`),
with the wind and solar PV coefficients differentiated by penetration bin following Johnson et al. (2016 :cite:`johnson_vre_2016`).

.. _tab-flex:
.. table:: Flexibility coefficients by technology as implemented in |name| (SSP2 values from 2025). Positive values are the share of a technology's generation that counts as flexible, negative values the additional flexible generation required per unit of its output. Approach after Sullivan et al. (2013 :cite:`sullivan_electric_2013`) and Johnson et al. (2016 :cite:`johnson_vre_2016`).

   +------------------------------------------------------------------------+---------------------------+
   | Technology                                                             | Flexibility coefficient   |
   +========================================================================+===========================+
   | Load (electricity transmission and distribution)                       | -0.18 to -0.17, by region |
   +------------------------------------------------------------------------+---------------------------+
   | Wind and solar PV, penetration bins 1 to 4                             | -0.2 / -0.3 / -0.4 / -0.5 |
   +------------------------------------------------------------------------+---------------------------+
   | Electricity imports                                                    | -0.15                     |
   +------------------------------------------------------------------------+---------------------------+
   | Concentrating solar power, solar multiple 3, by resource grade         | +0.07 to -0.70            |
   +------------------------------------------------------------------------+---------------------------+
   | Concentrating solar power, solar multiple 1, by resource grade         | +0.27 to -0.34            |
   +------------------------------------------------------------------------+---------------------------+
   | Geothermal, nuclear                                                    | 0                         |
   +------------------------------------------------------------------------+---------------------------+
   | Electric vehicles                                                      | +0.03                     |
   +------------------------------------------------------------------------+---------------------------+
   | Coal, conventional                                                     | +0.1                      |
   +------------------------------------------------------------------------+---------------------------+
   | Coal, advanced and with CCS; biomass                                   | +0.2                      |
   +------------------------------------------------------------------------+---------------------------+
   | Hydrogen fuel cells                                                    | +0.25                     |
   +------------------------------------------------------------------------+---------------------------+
   | Hydrogen electrolysis                                                  | +0.31                     |
   +------------------------------------------------------------------------+---------------------------+
   | Gas combined cycle, with and without CCS; oil steam and combined cycle | +0.4                      |
   +------------------------------------------------------------------------+---------------------------+
   | Gas steam; hydropower                                                  | +0.5                      |
   +------------------------------------------------------------------------+---------------------------+
   | Electricity exports                                                    | +0.15                     |
   +------------------------------------------------------------------------+---------------------------+
   | Gas combustion turbine; electricity storage                            | +1.0                      |
   +------------------------------------------------------------------------+---------------------------+

Thus, a technology like a natural gas combustion turbine, used almost exclusively for ancillary services, has a flexibility coefficient of 1, while a coal plant, which provides mostly bulk power but can supply some ancillary services, has a small, positive coefficient. Electric storage systems (e.g., pumped hydropower, compressed air storage, flow batteries) and flexible demand-side technologies like hydrogen-production contribute as well. Meanwhile, wind power and solar PV, which require additional system flexibility to smooth out fluctuations, have negative flexibility coefficients.

Across the narratives, the integration constraints are differentiated in three ways,
summarised in :numref:`tab-vre-integration`.
First, the cost of the integration measures,
which enters as a variable cost of the higher penetration bins,
is scaled relative to SSP2: to 0.67 in SSP1, 1.33 in SSP4 and 2.0 in SSP3 and SSP5.
Second, the penetration thresholds that define the bins are shifted.
In SSP1 the bins begin at 25%, 40% and 50% of electricity demand and in LED at 20%, 35% and 45%,
so that the integration effects set in later,
against 7%, 17% and 27% in SSP3, SSP4 and SSP5.
Third, the flexibility requirement and the capacity value of wind and solar are adjusted.
SSP1 assumes battery storage deployed alongside solar PV and greater flexibility from electric vehicles,
which lowers the flexibility requirement of solar PV to 0.16 to 0.40
and raises its capacity value to 0.95, 0.65 and 0.35 in the first three bins,
while wind keeps the SSP2 values.
LED lowers the flexibility requirement of both wind and solar PV to 0.10 to 0.25
and raises their capacity value to 0.9, 0.8, 0.7 and 0.6.
SSP5 raises the flexibility requirement of both to 0.30 to 0.60.
The SSP2 values apply in every narrative up to 2025
and converge linearly to the narrative-specific values by 2040.
The penetration thresholds also vary slightly by region,
with a regional factor between 0.93 and 1.1 that converges to 0.93 by 2100.

.. _tab-vre-integration:
.. list-table:: Differentiation of the wind and solar PV integration constraints across the narratives. Penetration thresholds are the shares of electricity demand at which the second, third and fourth bin begin. Flexibility requirement and capacity value are given for bins one to four. Values apply from 2040, after convergence from the SSP2 values in 2025.
   :widths: 10 14 22 27 27
   :header-rows: 1

   * - Narrative
     - Integration cost relative to SSP2
     - Penetration thresholds
     - Flexibility requirement
     - Capacity value
   * - LED
     - 1.0
     - 20 / 35 / 45%
     - 0.10 / 0.15 / 0.20 / 0.25
     - 0.9 / 0.8 / 0.7 / 0.6
   * - SSP1
     - 0.67
     - 25 / 40 / 50%
     - solar PV 0.16 / 0.24 / 0.36 / 0.40, wind as SSP2
     - solar PV 0.95 / 0.65 / 0.35 / 0, wind as SSP2
   * - SSP2
     - 1.0
     - 6 / 17 / 29%
     - 0.20 / 0.30 / 0.40 / 0.50
     - 0.9 / 0.6 / 0.3 / 0
   * - SSP3
     - 2.0
     - 7 / 17 / 27%
     - as SSP2
     - as SSP2
   * - SSP4
     - 1.33
     - 7 / 17 / 27%
     - as SSP2
     - as SSP2
   * - SSP5
     - 2.0
     - 7 / 17 / 27%
     - 0.30 / 0.40 / 0.50 / 0.60
     - as SSP2
