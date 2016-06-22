.. _annex_convtech:


2 Conversion Technologies
====
2.1 	Variables
----
Energy conversion technologies, both on the supply and demand side of the energy system, are modelled using two types of variables, that represent
– the amount of energy converted per year in a period (activity  variables) and
– the capacity installed annually in a period (capacity variables).

.. _activitiesECT:

2.1.1 	Activities  of Energy Conversion Technologies
~~~~~~~~~~~~~~~~~~~~~~
.. math::
   zsvd.elt

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`z`
     - is the level identifier of the main output of the technology. :math:`z = U` identifies the end-use level. This level is handled differently to all other levels: It has to be the demand level and technologies with the main output on this level are defined without load regions.
   * - :math:`s`
     - is the main energy input of the technology (supply). If the technology has no input :math:`s` is set to ”.” (e.g., solar technologies),
   * - :math:`v`
     - additional identifier of the conversion technology (used to distinguish technologies with the same input and output),
   * - :math:`d`
     - is the main energy output of the technology (demand),
   * - :math:`e`
     - is the level of reduction of demand due to own-price elasticities of demands (does only occur on the demand level; otherwise or if this demand has no elasticities :math:`e = ”.”`),
   * - :math:`l`
     - identifies the load region, :math:`l \in \{1, 2, 3, ...\}` or :math:`l` = ”.”`, if the technology is not modelled with load regions, and
   * - :math:`t`
     - identifies the period, :math:`t \in \{a, b, c, ...\}`.

The activity variable of an energy conversion technology is an energy flow variable. It represents the annual consumption of this technology of the main input per period. If a technology has no input, the variable represents the annual production of the main output.
 
If the level of the main output is *not* :math:`U` and at least one of the energy carriers consumed or supplied is defined with load regions the technology is defined with load regions. In this case the activity variables are generated separately for each load region, which is indicated by the additional identifier l in position 7. However, this can be changed by fixing the production of the technology over the load regions to a predefined pattern: one variable is generated for all load regions, the distribution to the load regions is given by the definition of the user (e.g., production pattern of solar power-plants).

If the model is formulated with demand elasticities, the activity variables of technologies with a demand  as main output that is defined with elasticity are generated for each elasticity class (identifier :math:`e` in position 0).

.. _capacititesECT:

2.1.2 	Capacities of Energy Conversion Technologies
~~~~~~~~~~~~~~~~~~~~~~
.. math:: 
   Yzsvd..t, 

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Y`
     - is the identifier for capacity variables.
   * - :math:`z`
     - identifies the level on that the main energy output of the technology is defined,
   * - :math:`s`
     - is the identifier of the main energy input of the technology,
   * - :math:`v`
     - additional identifier of the conversion technology,
   * - :math:`d`
     - is the identifier of the main energy output of the technology, and
   * - :math:`t`
     - is the period in that the capacity goes into operation.

The capacity variables are power variables. Technologies can be modelled without capacity variables. In this case no capacity constraints and no dynamic constraints on construction can be included in the model. Capacity variables of energy conversion technologies can be defined  as integer variables, if the solution algorithm has a mixed integer option.

If a capacity variable is continuous it represents the annual new installations of the technology in period :math:`t`, if it is integer it represents either the annual number of installations of a certain size or the number of installations of :math:`1/\Delta t` times the unit size (depending  on the definition; :math:`\Delta t` is the length of period :math:`t` in years).

The capacity is defined in relation to the main output of the technology.

2.2 	Constraints
-------------------
The rows used to model energy conversion technologies limit

* the utilization of a technology in relation to the capacity actually installed (capacity constraint) and
* the activity or construction of a technology in a period in relation to the same variable in the previous period (dynamic constraints).

.. _capacityconstr:

2.2.1 	Capacity Constraints
~~~~~~~~~~~~~~~~~~~~~~

.. math::
   C zsvd.lt, 

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`C`
     - is the identifier for capacity constraints,
   * - :math:`z`
     - identifies the level on that the main energy output of the technology is defined,
   * - :math:`s`
     - is the identifier of the main energy input of the technology,
   * - :math:`v`
     - additional identifier of the conversion technology,
   * - :math:`d`
     - is the identifier of the main energy output of the technology,
   * - :math:`l`
     - identifies the load region, :math:`l \in \{1, 2, 3, ...\}` or :math:`l` = ”.”`, if the technology is not modelled with load regions, and
   * - :math:`t`
     - is the period in that the capacity goes into operation.

For all conversion technologies modelled with capacity variables the capacity constraints will be generated automatically. If the activity variables exist for each load region separately there will be one capacity constraint per load region. If the technology is an end-use technology the sum over the elasticity classes will be included in the capacity constraint.

Additionally the activity variables of technologies with multiple operation modes (e.g., different fuels) can be linked to the same capacity variable, which allows to leave the choice of the activity variable used with a given capacity to the optimization.

**Technologies without Load Regions**

For technologies without load regions (i.e. technologies, where no input or output is modelled with load regions) the production is related to the total installed capacity by the plant factor. For these technologies the plant factor has to be given as the fraction they actually operate per year. All end-use technologies (technologies  with main output level ”U ”) are modelled in this way.

.. math::
   \epsilon_{svd} \times zsvd...t - \sum_{\tau =t-\tau_{svd}}^{min(t,\kappa_{svd}} \Delta(\tau-1)\times \pi_{svd}\times f_i \times Yzsvd..\tau \leq hc_{svd}^t \times \pi_{svd} ,
 
**Technologies with Varying Inputs and Outputs**

Many types of energy conversion technologies do not have fix relations between their inputs and outputs. MESSAGE has the option to link several activity variables of a conversion technology into one capacity constraint. For the additional activities linked to a capacity variable a coefficient defines the maximum power available in relation to one power unit of the main activity.

In the following this constraint is only described for technologies without load regions; the other types are constructed in analogy.

.. math::
   \sum_{\sigma {v}'\delta }rel_{\sigma {v}'\delta} ^{svd}\times\epsilon_{\sigma {v}'\delta }\times z\sigma {v}'\delta ...t- \\ \sum_{\tau=t-\tau_{svd}}^{min(t,\kappa_{svd})}\Delta(\tau-1)\times \pi_{svd}\times f_i\times Yzsvd..\tau \leq hc_{svd}^t\times \pi_{svd},
 
The following notation is used in the above equations:

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`zsvd..lt`
     - is the activity of conversion technology :math:`v` in period :math:`t` and, if defined so, load region :math:`l` (see section :ref:`activitiesECT`),
   * - :math:`Yzsvd..t`
     - is the capacity variable of conversion technology :math:`v` (see section :ref:`capacititesECT`).
   * - :math:`\epsilon_{svd}`
     - is the efficiency of technology :math:`v` in converting the main energy input, :math:`s`, into the main energy output, :math:`d`,
   * - :math:`\kappa_{svd}`
     - is the last period in that technology :math:`v` can be constructed,
   * - :math:`\pi_{svd}`
     - is the "plant factor" of technology :math:`v`, having different meaning depending on the type of capacity equation applied,
   * - :math:`\Delta \tau`
     - is the length of period :math:`\tau` in years,
   * - :math:`\tau_{svd}`
     - is the plant life of technology :math:`v` in periods,
   * - :math:`hc_{svd}^t`
     - represents the installations built before the time horizon under consideration, that are still in operation in the first year of period :math:`t`,
   * - :math:`f_i`
     - is 1. if the capacity variable is continuous, and represents the minimum installed capacity per year (unit size) if the variable is integer,
   * - :math:`l_m`
     - is the load region with maximum capacity use if the production pattern over the year is fixed,
   * - :math:`\pi(l_m, svd)`
     - is the share of output in the load region with maximum production,
   * - :math:`rel_{\sigma {v}'\delta}^{svd}`
     - is the relative capacity of main output of technology (or operation mode) svd to the capacity of main output of the alternative technology (or operation mode) :math:`\sigma {v}'\delta`,
   * - :math:`\lambda _l`
     - is the length of load region :math:`l` as fraction of the year, and
   * - :math:`\lambda_{l_m}`
     - is the length of load region :math:`l_m`, the load region with maximum capacity requirements, as fraction of the year.


.. _upper_dynamic_constraint_capacity:

2.2.2 	Dynamic Constraints on Activity and Construction Variables
~~~~~~~~~~~~~~~~~~~~~~

.. math::
   MYzsvd.t

The dynamic capacity constraints relate the amount of annual new installations of a technology in a period to the annual construction during the previous period.

.. math::
   Yzsvd..t - \gamma y_{svd,t}^o \times Yzsvd..(t-1) \leq gy_{svd,t}^o,
 
where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`\gamma y_{svd,t}^o`
     - is the maximum growth rate per period for the construction of technology :math:`v`,
   * - :math:`gy_{svd,t}^o`
     - is the initial size (increment) that can be given and which is necessary for the introduction of new technologies that start with zero capacity,
   * - :math:`Yzsvd..t`
     - is the annual new installation of technology :math:`v` in period :math:`t`.

As described in Keppo and Strubegger (2010 :cite:`keppo_short_2010`) MESSAGE includes so called flexible or soft dynamic constraints to allow for faster diffusion 
in case of economically attractive technologies. To operationalize the concept of soft dynamic constraints, a set of :math:`n` dummy variables with index :math:`i`, 
:math:`Bzsvd..ti`, multiplied by a corresponding growth factor :math:`(1+\delta y_{svd,ti})` are added to the upper dynamic constraint described above. 

**notation below needs updating to be consistent with the one from the MESSAGE equations** 

.. image:: /_static/technology_diffusion_eq_3.png
   :width: 340px
   
The maximum value for these dummy variables :math:`b^i` is limited to the activity of the underlying technology :math:`a`, i.e.

.. image:: /_static/technology_diffusion_eq_4.png 
   :width: 60px
   :align: left

, for all :math:`i`.

Therefore, this new formulation increases the highest allowed growth factor from

.. image:: /_static/technology_diffusion_eq_4a.png
   :width: 75px
   :align: left
   
to 

.. image:: /_static/technology_diffusion_eq_4b.png
   :width: 180px

In addition, the objective function value for period :math:`t` is modified by the extra term

 .. image:: /_static/technology_diffusion_eq_5.png
   :width: 140px

which adds costs :math:`c_i` per additional growth factor utilized. 
