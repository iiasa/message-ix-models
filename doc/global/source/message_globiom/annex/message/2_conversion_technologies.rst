.. _annex_convtech:


2 Conversion Technologies
====
2.1 	Variables
----
Energy conversion technologies, both on the supply and demand side of the energy system, are modelled using two types of variables, that represent

* the amount of energy converted per year in a period (activity  variables) and
* the annually installed capacity in a period (capacity variables).

.. _activitiesECT:

2.1.1 	Activities  of Energy Conversion Technologies
~~~~~~~~~~~~~~~~~~~~~~
.. math::
   zsvd....rrllltt

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`z`
     - level identifier of the main output of the technology. The demand level is handled differently to all other levels: Technologies with the main output on this level are defined without load regions. If defined, the input is split into the different load regions.
   * - :math:`s`
     - main energy input of the technology (supply). If the technology has no input :math:`s` is set to ”.” (e.g., solar technologies),
   * - :math:`v`
     - additional identifier of the conversion technology (used to distinguish technologies with the same main input and output),
   * - :math:`d`
     - main energy output of the technology,
   * - :math:`rr`
     - identifies the sub-region, :math:`rr` as defined in file "regid" or :math:`rr` = :math:`”..”`, if the model has no sub-regions or if the technology is in the main region,
   * - :math:`lll`
     - identifies the load region, :math:`lll` is :math:`sdp` (season, day, part of day) or :math:`lll` = :math:`”...”`, if the technology is not modelled with load regions, and
   * - :math:`ttt`
     - identifies the period, :math:`ttt` = :math:`year - int(year_0/100) * 100`.

The activity variable of an energy conversion technology is an energy flow variable. It represents the annual consumption of this technology of the main input per period or load region. If a technology has no input, the variable represents the annual production of the main output divided by the efficiency.
 
If the main output is *not* on the demand level and at least one of the energy carriers consumed or supplied is defined with load regions the technology is defined with load regions. In this case the activity variables are generated separately for each load region, which is indicated by the additional identifier "lll". However, this changes if the production of the technology over the load regions is predefined: one variable is generated for the time step, the distribution to the load regions is given by the definition of the user (e.g., production pattern of solar power-plants or consumption pattern of end-use devices).

.. _capacititesECT:

2.1.2 	Capacities of Energy Conversion Technologies
~~~~~~~~~~~~~~~~~~~~~~
.. math:: 
   yzsvd...rr...ttt 

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`y`
     - identifier for capacity variables.
   * - :math:`z`
     - identifies the level on that the main energy output of the technology is defined,
   * - :math:`s`
     - identifier of the main energy input of the technology,
   * - :math:`v`
     - additional identifier of the conversion technology,
   * - :math:`d`
     - identifier of the main energy output of the technology,
   * - :math:`rr`
     - identifier of the model region,
   * - :math:`ttt`
     - period in that the capacity is buildt.

The capacity variables are power variables. Technologies can be modelled without capacity variables. In this case no capacity constraints and no dynamic constraints on construction can be included in the model. Capacity variables of energy conversion technologies can be defined  as integer variables.

If a capacity variable is continuous it represents the annual new installations of the technology in period :math:`t`, if it is integer it represents either the annual number of installations of a certain size or the number of installations of :math:`1/\Delta t` times the unit size (depending  on the definition; :math:`\Delta t` is the length of period :math:`t` in years).

The capacity is defined in relation to the main output of the technology.

2.2 	Constraints
-------------------
These are equations used to calculate relations beween timesteps or between different variables in the model. Partially they are generated automatically, partially they are entirely defined by the user.

* Utilization of a technology in relation to the capacity actually installed (capacity constraint),
* the activity or annual construction of a technology in a period in relation to the same variable in the previous period (dynamic constraints),
* limit on minimum or maximum total installed capacity of a technology,
* limit on minimum or maximum annual production of a technology modeled with load region, and
* user defined constraints on groups of technologies (activities or capacities).

.. _capacityconstr:

2.2.1 	Capacity Constraints
~~~~~~~~~~~~~~~~~~~~~~

.. math::
   czsvd...rrlllttt 

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`c`
     - identifier for capacity constraints,
   * - :math:`z`
     - identifies the level on that the main energy output of the technology is defined,
   * - :math:`s`
     - identifier of the main energy input of the technology,
   * - :math:`v`
     - additional identifier of the conversion technology,
   * - :math:`d`
     - identifier of the main energy output of the technology,
   * - :math:`rr`
     - identifier of the model region,
   * - :math:`lll`
     - identifier of the load region, and
   * - :math:`ttt`
     - period in that the capacity is buildt.

For all conversion technologies modelled with capacity variables the capacity constraints will be generated automatically. If the activity variables exist for each load region separately there will be one capacity constraint per load region.

Additionally the activity variables of technologies with multiple operation modes (e.g., different fuels) can be linked to the same capacity variable, which allows the optimization to choose the activity variable used with a given capacity.

**Technologies without Load Regions**

For technologies without load regions (i.e. technologies, where no input or output is modelled with load regions) the production is related to the total installed capacity by the plant factor. For these technologies the plant factor has to be given as the fraction they actually operate per year. All end-use technologies are modelled in this way.

.. math::
   \epsilon_{zsvd} \times zsvd....rr...ttt - \sum_{\tau =t-\tau_{zsvd}}^{min(t,\kappa_{zsvd})} \Delta(\tau-1)\times \pi_{zsvd}\times f_i \times f_p \times yzsvd...rr...\tau \leq hc_{zsvd}^t \times \pi_{zsvd}
 
**Technologies with Varying Inputs and Outputs**

Many types of energy conversion technologies do not have fix relations between their inputs and outputs (e.g.: a power plant may use oil or gas as input or can produce electricity and/or heat as output). MESSAGE has the option to link several activity variables of a conversion technology into one capacity constraint. For the additional activities linked to a capacity variable a coefficient defines the maximum power available in relation to one power unit of the main activity.


.. math::

   & \sum_{z\sigma {v}'\delta }\frac{rel_{z\sigma {v}'\delta} ^{zsvd}\times\epsilon_{z\sigma {v}'\delta }\times z\sigma {v}'\delta ....rrlllttt}{\lambda _{lll}} - \\
   & \sum_{\tau=t-\tau_{zsvd}}^{min(t,\kappa_{zsvd})}\Delta \tau \times \pi_{zsvd}\times f_i \times f_p \times yzsvd...rr...\tau \leq hc_{zsvd}^t\times \pi_{zsvd} \qqad \forall lll

The following notation is used in the above equations:

.. list-table:: 
   :widths: 20 80
   :header-rows: 0

   * - :math:`zsvd....rrlllttt`
     - activity of conversion technology :math:`zsvd` in region :math:`rr`, period :math:`ttt` and, if defined so, load region :math:`lll` (see section :ref:`_activitiesECT`),
   * - :math:`yzsvd...rr...ttt`
     - capacity variable of conversion technology :math:`zsvd` (see section :ref:`_capacititesECT`).
   * - :math:`\epsilon_{zsvd}`
     - efficiency of technology :math:`zsvd` in converting the main energy input, :math:`s`, into the main energy output, :math:`d`,
   * - :math:`\kappa_{zsvd}`
     - last period in that technology :math:`zsvd` can be constructed,
   * - :math:`\pi_{svd}`
     - "plant factor" of technology :math:`zsvd`, having different meaning depending on the type of capacity equation applied, in case the plant life does not coincide with the end of a period it also is adjusted time the technology can be operated in that period, 
   * - :math:`\Delta \tau`
     - length of period :math:`\tau` in years,
   * - :math:`\tau_{zsvd}`
     - plant life of technology :math:`zsvd` in periods,
   * - :math:`hc_{zsvd}^t`
     - represents the installations built before the time horizon under consideration, that are still in operation in the first year of period :math:`t`,
   * - :math:`f_i`
     - is 1. if the capacity variable is continuous, and represents the minimum installed capacity per year (unit size) if the variable is integer,
   * - :math:`f_p`
     - adjustment factor if the end of the plant life does not coincide with the end of a period (:math:`rest of plant life in period / period length`,
   * - :math:`\pi(l_m, svd)`
     - share of output in the load region with maximum production,
   * - :math:`rel_{\sigma {v}'\delta}^{svd}`
     - relative capacity of main output of technology (or operation mode) svd to the capacity of main output of the alternative technology (or operation mode) :math:`\sigma {v}'\delta`, and
   * - :math:`\lambda_l`
     - length of the load region :math:`l` or the length of the load region with maximum capacity use if the production pattern over the year is fixed or the length of the load region with maximum capacity requirements as fraction of the year.


.. _upper_dynamic_constraint_capacity:

2.2.2 	Dynamic Constraints on Activity and Construction Variables
~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Dzsvd...rrlllttt

The dynamic constraints relate the activity or annual new installations of a technology in a period to the activity or annual construction during the previous period.

.. math::
   yzsvd...rr...ttt - \gamma _{yzsvd,ttt} \times yzsvd...rr...(ttt-1) \sim g _{yzsvd,ttt} \\
   \sum_{lll} zsvd...rrlllttt - \gamma _{zsvd,ttt} \times \sum_{lll} zsvd...rrlll(ttt-1) \sim g _{zsvd,ttt},
 
where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`D`
     - is :math:`M, L` for upper and lower capacity and :math:`m, l` for upper and lower activity constraints respectively,
   * - :math:`\sim`
     - is :math:`\leq, \geq` for upper and lower constraints respectively,
   * - :math:`\gamma _{yzsvd,t}, \gamma _{zsvd,t}`
     - maximum growth rate per period for the construction/operation of technology :math:`zsvd`,
   * - :math:`g_{yzsvd,t}`
     - initial size (increment) that can be given and which is necessary for the introduction of new technologies that start with zero capacity/activity,
   * - :math:`yzsvd...rr...ttt`
     - annual new installation of technology :math:`zsvd` in period :math:`ttt`.
   * - :math:`zsvd...rrlllttt`
     - activity of technology :math:`zsvd` in period :math:`ttt`, and load region lll.

As described in Keppo and Strubegger (2010 :cite:`_keppo_short_2010`) MESSAGE includes so called flexible or soft dynamic constraints to allow for faster diffusion 
in case of economically attractive technologies. To operationalize the concept of soft dynamic constraints, a set of :math:`n` dummy variables with index :math:`i`, 
:math:`Bzsvd..ti`, multiplied by a corresponding growth factor :math:`(1+\delta y_{zsvd,ti})` are added to the upper dynamic constraint described above. 

.. math::
   a_t = (1+r)^T \times a_{t-1} + \sum_{i=1}^n (1+r_i)^T \times b_{t-1}^i + S

The maximum value for these dummy variables :math:`b^i` is limited to the activity of the underlying technology :math:`a`, i.e.

.. math::
   a_t \leq b_t^i \qquad \qquad \forall i.

Therefore, this new formulation increases the highest allowed growth factor from

.. math::
   (1+r)^T
   
to 

.. math::
   (1+r)^T + \sum_i (1+r_i)^T

In addition, the objective function value for period :math:`t` is modified by the extra term

 .. math::
   \cdots + \sum_{i=1}^n c_i \times b_t^i

which adds costs :math:`c_i` per additional growth factor utilized. 

.. _dynamic_constraints:

2.2.3 	Contraints on total installed capacity
~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Izsvd...rr...ttt

These constaints allow to set upper and/or lower limits on the total installed capacity of a technology at a given point in time.

.. math::
   \sum_{\tau=t-T}^t yzsvd...rr...\tau \sim M_t

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`T`
     - plant life of the technology,
   * - :math:`\sim`
     - is :math:`\leq or \geq` for lower and ujpper constraints respectively,
   * - :math:`M_t`
     - maximum or minimum allowed total installed capacity in time step t

2.2.4 	User defined Constraints
~~~~~~~~~~~~~~~~~~~~~~

.. math::
   nname...rrlllttt

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`n`
     - may be 'n', 'p', or 'c' for three groups of user defined constraints,
   * - :math:`name`
     - is a user defined 4-character short name of the constraint.

Each technology may have entries related to their activity, new installed capacity, or total installed capacity into any of the defined constraints. In multi-region models the constraint is first searched in the region where the entry is defined and then, if not found, in the main-region. With this it is possible to create relations between technologies in different sub-regions.
The main uses for such constraints are to put regional or global constraints on emissions or to relate the production from specific energy carrirer to the total production, e.g.:

.. math::
   wind\_electricity + solar\_electricity + biomass\_electricity \geq \alpha \times total\_electricity.
   
where :math:`total\_electricity` can usualy be taken from the input to the electricity transmission technology.

2.3 	Bounds
~~~~~~~~~~~~~~~~~~~~~~

Upper, lower, or fixed bounds may be put on activity or new installed capacity. This is usually very helpful at the beginning of the planning horizon to fit results to reality. In later time steps they may be used to avoid unrealistic behaviour like, e.g., too many new installations of a specific technology per year.
