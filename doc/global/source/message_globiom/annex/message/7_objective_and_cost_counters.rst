.. _objectivecostcounters:

7 Objective and Cost Counters
=============================

7.1 	Cost Accounting Rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The different types of costs (i.e. entries for the objective function) can be accumulated  over all technologies in built-in  accounting rows. These rows can be generated per load region or per period or for the whole time horizon and contain the sum of the undiscounted costs. They can also be limited. In case of :math:`func` the entries are discounted as these are the entries into the objective function. The implemented types are:

.. list-table:: 
   :widths: 45 110
   :header-rows: 0

   * - :math:`func`
     - objective functions and discounted accounting rows,
   * - :math:`ccur`
     - fix (related to the installed capacity) and variable (related to the production) operation and maintenance costs,
   * - :math:`ccap`
     - investment costs; if the investments of a technology are distributed over the previous periods, also the entries to this accounting rows are distributed,
   * - :math:`cres`
     - domestic fuel costs,
   * - :math:`car1`
     - costs related to the user defined relations of type 1 (see section 7), 
   * - :math:`car2`
     - costs related to the user defined relations of type 2 (see section 7),

The cost accounting rows are further separated into the following schemes:

.. list-table:: 
   :widths: 80 110
   :header-rows: 0

   * - :math:`name` 
     - total costs across all regions, load regions and time steps; :math:`func` is the objective function (see below),
   * - :math:`name.........ttt` 
     - total costs across all regions and load regions per time step,
   * - :math:`name....rr` 
     - total costs across all load regions and time steps per region,

7.2 	The Objective Function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   func

In its usual form, the objective function contains the sum of all discounted costs. All costs related to operation (i.e. resource use, operation costs, taxes on emissions, ...) are discounted from the middle of the current period to the first year. Costs related to construction are by default discounted from the first year of the period to the first year. By using the facility of distributing the investment related costs over the construction time these costs can be distributed over some years before or equal to the current one (see section :ref:`distributionsofinv`). 

The objective function has the following general form:

.. math::
   \sum_r \sum_t\left [ \beta_m^t \Delta t\left \{ \sum_{zsvd} \sum_{lll} zsvd....rrlllttt \times \epsilon_{zsvd} \times \left [ ccur(zsvd,t) + \\
   \sum_i \sum_m rho_{zsvd}^{mlt} \times cari(ml,t) \right ] + \sum_{zsvd} \sum_{\tau=t-\tau_{zsvd}}^t \Delta\tau \times yzsvd..\tau \times cfix(zsvd,\tau) + \\
      \sum_r \left [\sum_g \sum_l \sum_p Rzrgp...rrlllttt \times cres(rgpl,t)] \right \} + \\
      \beta_b^t \times \left \{ \sum_{zsvd} \sum_{\tau=t}^{t+t_d} \Delta(t-1) \times yzsvd...rr...\tau \times 
      \left [ ccap(svd,\tau) \times fri_{zsvd}^{t_d-\tau} + \\
      \sum_i \sum_m rc_{zsvd}^{mt} \times cari(m,t) \times fra_{zsvd,m}^{t_d-\tau} \right ] \right ] \right \} \right ]

where

.. list-table:: 
   :widths: 40 60
   :header-rows: 0

   * - :math:`\Delta t`
     - is the length of period t in years,

.. math::
   \beta_b^t=\left [ \frac{1}{1+\frac{dr}{100}} \right ]^{t-t_0},
   \beta_m^t=\left [ \frac{1}{1+\frac{dr}{100}} \right ]^{t+ \frac{\Delta t}{2}-t_0},

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`dr`
     - is the discount rate in percent,
   * - :math:`zsvd....rrlllttt`
     - is the annual consumption of technology :math:`zsvd` of fuel :math:`s` load region :math:`l` and period :math:`t`; if :math:`zsvd` has no load regions, :math:`lll` = ”...”.
   * - :math:`\epsilon_{zsvd}`
     - is the efficiency of technology :math:`zsvd` in converting :math:`s` to :math:`d`,
   * - :math:`ccur(zsvd,t)`
     - are the variable operation and maintenance costs of technology :math:`zsvd` (per unit of main output) in period :math:`t`,
   * - :math:`rho_{zsvd}^{mlt}`
     - is the relative factor per unit of output of technology :math:`v` for relational constraint :math:`m` in period :math:`t`, load region :math:`l`,
   * - :math:`car1(m,t)`
     - and :math:`car2(m,t)` are the coefficients for the objective function, that are related to the user defined relation :math:`m` in period :math:`t`,
   * - :math:`car1(ml,t)`
     - and :math:`car2(ml,t)` are the same for load region :math:`l`, if relation :math:`m` has load regions,
   * - :math:`rho_{zsvd}^{mt}`
     - is the relative factor per unit of output of technology :math:`zsvd` for relational constraint :math:`m` in period :math:`t`,
   * - :math:`yzsvd...rr...ttt`
     - is the annual new built capacity of technology :math:`zsvd` in period :math:`t`,
   * - :math:`cfix(zsvd,t)`
     - are the fix operation and maintenance cost of technology :math:`zsvd` that was built in period :math:`t`,
   * - :math:`ccap(zsvd,t)`
     - is the specific investment cost of technology :math:`v` in period :math:`t` (given per unit of main output),
   * - :math:`fri_{zsvd}^n`
     - is the share of this investment that has to be paid n periods before the first year of operation,
   * - :math:`rc_{zsvd}^{mt}`
     - is the relative factor per unit of new built capacity of technology :math:`zsvd` for user defined relation :math:`m` in period :math:`t`,
   * - :math:`fra_{zsvd,m}^n`
     - is the share of the relative amount of the user defined relation :math:`m` that occurs :math:`n` periods before the first year of operation (this can, e.g., be used to account for the use of steel in the construction of solar towers over the time of construction),
   * - :math:`rzrg....rrlllttt`
     - is the annual consumption of resource :math:`r`, grade :math:`g` in load region :math:`l` and period :math:`t`,
   * - :math:`cres(rgpl,t)`
     - is the cost of extracting resource :math:`r`, grade :math:`g`, elasticity class :math:`p` in period :math:`t` and load region :math:`l` (this should only be given, if the extraction is not modelled explicitly),
