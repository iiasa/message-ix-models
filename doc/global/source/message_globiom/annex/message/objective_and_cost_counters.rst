8 Objective and Cost Counters
=============================

8.1 	Constraints
---------------------

8.1.1 	Cost Accounting Rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The different types of costs (i.e. entries for the objective function) can be accumulated  over all technologies in built-in  accounting rows. These rows can be generated per period or for the whole horizon and contain the sum of the undiscounted costs. They can also be limited. The implemented types are:

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`CCUR`
     - fix (related to the installed capacity) and variable (related to the production) operation and maintenance costs,
   * - :math:`CCAP`
     - investment costs; if the investments of a technology are distributed over the previous periods, also the entries to this accounting rows are distributed (if the capital costs are levellized, the total payments in a period can be taken from :math:`CINV`; :math:`CCAP` shows the share of investments in the according period, then),
   * - :math:`CRES`
     - domestic fuel costs,
   * - :math:`CAR1`
     - costs related to the user defined relations of type 1 (see section 7), 
   * - :math:`CAR2`
     - costs related to the user defined relations of type 2 (see section 7),
   * - :math:`CRED`
     - costs for reducing demands due to demand elasticities, only related to technologies supplying the demands directly,
   * - :math:`CIMP`
     - import costs,
   * - :math:`CEXP`
     - gains for export, and
   * - :math:`CINV`
     - total investments (in case of levellized investment costs, see :math:`CCAP`).


8.1.2 	The Objective Function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   FUNC

In its usual form the objective function contains the sum of all discounted costs, i.e. all kinds of costs that can be accounted for. All costs related to operation (i.e. resource use, operation costs, costs of demand elasticities,...) are discounted from the middle of the current period to the first year. Costs related to construction are by default discounted from the beginning of the current period to the first year. By using the facility of distributing the investments or accounting during construction these costs can be distributed over some periods before or equal to the current one (see section 10.2). This distribution can also be performed  for user defined relations.

The objective function has the following general form:

.. math::
   \sum_t\left [ \beta_m^t \Delta t\left \{ \sum_{svd}\sum_lzsvd..lt\times\epsilon_{svd}\times\left [ ccur(svd,t)+\sum_i\sum_mro_{svd}^{mlt}\times cari(ml,t)\right ]+ \\ \sum_{svd}\epsilon_{svd}\times\sum_{e=0}^{e_d}Usvd.e.t\times\epsilon_{svd}\times\left [ \kappa _e\times(ccur(svd,t)+\sum_mro_{svd}^{mt}\times car2(m,t)) + \\ cred(d,e)+\sum_mro_{svd}^{mt}\times car1(m,t) \right ] +\sum_{svd}\sum_{\tau =t-\tau_{svd}}^t\Delta\tau\times Yzsvd..\tau\times cfix(svd,\tau)+ \\ \sum_r \left [\sum_g\sum_l\sum_pRzrgp.lt\times cres(rgpl,t)+ \\ \sum_c\sum_l\sum_pIzrcp.lt\times cimp(rcpl,t) -\sum_c\sum_l\sum_p Ezrcp.lt\times cexp(rcpl,t) \right ] \right \} + \\\beta_b^t\times\left \{ \sum_{svd}\sum_{\tau=t}^{t+t_d}\Delta(t-1)\times Yzsvd..\tau\times\left [ ccap(svd,\tau)\times fri_{svd}^{t_d-\tau}+ \\ \sum_i\sum_mrc_{svd}^{mt} \times cari(m,t)\times fra_{svd,m}^{t_d-\tau} \right ] \right \} \right ]

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`\Delta t`
     - is the length of period t in years,

.. math::
   \beta_b^t=\Pi_{i=1}^{t-1}\left [ \frac{1}{1+\frac{dr(i)}{100}} \right ]^{\Delta i},
   \beta_m^t=\beta_b^t\times\left [ \frac{1}{1+\frac{dr(t)}{100}} \right ]^{\frac{\Delta t}{2}},

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`dr(i)`
     - is the discount rate in period i in percent,
   * - :math:`zsvd..lt`
     - is the annual consumption of technology :math:`v` of fuel :math:`s` load region :math:`l` and period :math:`t`; if :math:`v` has no load regions, :math:`l` = ”.”.
   * - :math:`\epsilon_{svd}`
     - is the efficiency of technology :math:`v` in converting :math:`s` to :math:`d`,
   * - :math:`ccur(svd,t)`
     - are the variable operation and maintenance costs of technology :math:`v` (per unit of main output) in period :math:`t`,
   * - :math:`ro_{svd}^{mlt}`
     - is the relative factor per unit of output of technology :math:`v` for relational constraint :math:`m` in period :math:`t`, load region :math:`l`,
   * - :math:`car1(m,t)`
     - and :math:`car2(m,t)` are the coefficients for the objective function, that are related to the user defined relation :math:`m` in period :math:`t`,
   * - :math:`car1(ml,t)`
     - and :math:`car2(ml,t)` are the same for load region :math:`l`, if relation :math:`m` has load regions,
   * - :math:`Usvd.e.t`
     - is the annual consumption of fuel :math:`s` of end-use technology :math:`v` in period :math:`t` and elasticity class :math:`e`,
   * - :math:`\kappa_e`
     - is the factor giving the relation of total demand for :math:`d` to the demand reduced due to the elasticity to level :math:`e`,
   * - :math:`ro_{svd}^{mt}`
     - is the relative factor per unit of output of technology :math:`v` for relational constraint :math:`m` in period :math:`t`,
   * - :math:`cred(d,e)`
     - is the cost associated with reducing the demand for :math:`d` to elasticity level :math:`e`,
   * - :math:`Yzsvd..t`
     - is the annual new built capacity of technology :math:`v` in period :math:`t`,
   * - :math:`cfix(svd,t)`
     - are the fix operation and maintenance cost of technology :math:`v` that was built in period :math:`t`,
   * - :math:`ccap(svd,t)`
     - is the specific investment cost of technology :math:`v` in period :math:`t` (given per unit of main output),
   * - :math:`fri_{svd}^n`
     - is the share of this investment that has to be paid n periods before the first year of operation,
   * - :math:`rc_{svd}^{mt}`
     - is the relative factor per unit of new built capacity of technology :math:`v` for user defined relation :math:`m` in period :math:`t`,
   * - :math:`fra_{svd,m}^n`
     - is the share of the relative amount of the user defined relation :math:`m` that occurs :math:`n` periods before the first year of operation (this can, e.g., be used to account for the use of steel in the construction of solar towers over the time of construction),
   * - :math:`Rzrgp.lt`
     - is the annual consumption of resource :math:`r`, grade :math:`g`, elasticity class :math:`p` in load region :math:`l` and period :math:`t`,
   * - :math:`cres(rgpl,t)`
     - is the cost of extracting resource :math:`r`, grade :math:`g`, elasticity class :math:`p` in period :math:`t` and load region :math:`l` (this should only be given, if the extraction is not modelled explicitly),
   * - :math:`Izrcp.lt`
     - is the annual import of fuel :math:`r` from country :math:`c` in load region :math:`l`, period :math:`t` and elasticity class :math:`p`; if :math:`r` has no load regions :math:`l` =”.”,
   * - :math:`cimp(rcpl,t)`
     - is the cost of importing :math:`r` in period :math:`t` from country :math:`c` in load region :math:`l` and elasticity class :math:`p`,
   * - :math:`Ezrcp.lt`
     - is the annual export of fuel :math:`r` to country :math:`c` in load region :math:`l`, period :math:`t` and elasticity class :math:`p`; if :math:`r` has no load regions :math:`l` =”.”, and
   * - :math:`cexp(rcpl, t)`
     - is the gain for exporting :math:`r` in period :math:`t` to country :math:`c` in load region :math:`l` and elasticity class :math:`p`.
