6 User-defined Relations
=======================

6.1 	Constraints
------------------

The user-defined relations allow the user to construct constraints that are not included in the basic set of constraints. For each technology  the user can specify coefficients with that either the production variables (see section 2.1.1),  the annual new installation variables  (see section 2.1.2) or the total capacity in a year (like it is used in the capacity constraints, see section 2.2.1) can be included in the relation. The relations can be defined with and without load regions, have a lower, upper or fix right hand side or remain free (non-binding) and be related to an entry in the objective function, i.e., all entries to this relation are also entered to the objective function with the appropriate discount factor. There are two types of user-defined constraints, for which the entries to the objective function–without discounting–are summed up under the cost accounting rows :math:`CAR1` and :math:`CAR2` (see chapter 8).

The formulation of the user-defined relations is given for relations, that are related to the main output of the technologies. It is also possible (e.g., for emissions) to relate the constraint to the main input of the technology, i.e. the amount of fuel used. In this case the efficiencies (:math:`\epsilon `) would be omitted from the formulation.


6.1.1 	Relation without  Load Regions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Nm.....t

or

.. math:: 
   Pm.....t

Relations without load regions just sum up the activities (multiplied with the given coefficients) of all variables defined to be in this constraint. If a technology has load regions, the activity variables for all load regions of this technology are included. If the total capacity of a technology is included, all new capacities from previous periods still operating are included, if new capacities are included, the annual new installation of the current period is taken.

.. math::
   \sum_{svd}\left [ ro_{svd}^{mt}\times \sum_{e+0}^{e_d}Usvd.e.t\times\epsilon_svd+\sum_{\tau+t-ip}rc_{svd}^{mt}\times YUsvd..\tau\right ]+ \\ \sum_{rvs}\left [ ro_{rvs}^{mlt}\times\sum_lzrvs..lt\times\epsilon_{rvs}+ro_{rvs}^{mt}\times zrvs..t\times \epsilon_{rvs}+ \\ \sum_{\tau=t-ip}^trc_{rvs}^{mt} \times Yzrvs..\tau \right ] \left\{\begin{matrix}
      free & \\ 
      \geq rhs_m^t, & \\ 
      =rhs_m^t & \\ 
      \leq rhs_m^t & 
      \end{matrix}\right.

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Usvd.e.t`
     - and :math:`Y U svd..t` are the activity and capacity variables of the end-use technologies,
   * - :math:`zrvs..lt`,
     - :math:`zrvs...t` and :math:`Yzrvs..t` are the activity variables of technologies with and without load regions and the capacity variables of the technologies,
   * - :math:`\epsilon_{rvs}`
     - and :math:`\epsilon_{svd}` are the efficiencies of the technologies; they are included by the code,
   * - :math:`ro_{svd}^{mt}`
     - is the relative factor per unit of output of technology :math:`v` (coefficient) for relational constraint :math:`m`,
   * - :math:`rc_{svd}^{mt}`
     - is the same per unit of new built capacity,
   * - :math:`ro_{mlt}^{rvs}`
     - is the relative factor per unit of output of technology v (coefficient) for relational constraint :math:`m`, load region :math:`l`,
   * - :math:`rc_{mlt}^{rvs}`
     - is the same per unit of new built capacity,
   * - :math:`tl`
     - is 1 for relations to construction and :math:`\Delta\tau` for relations to total capacity,
   * - :math:`ip`
     - is 1 for accounting during construction and the plant life on periods for accounting of total capacity, and
   * - :math:`rhs_m^t`
     - is the right hand side of the constraint.


6.1.2 	Construction of Relations between Periods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Nm.....t

or

.. math::
   Pm.....t

The change of activities over time can either be limited or included in the objective by constructing relations between periods: The relations expresses the difference between the annual activity in a period and the following period. This difference can either be limited or included in the objective function.

.. math::

   \sum_{svd}\left [ ro_{svd}^{mt}\times\sum_{e+0}^{e_d}Usvd.e.t\times\epsilon_{svd}-ro_{svd}^{m(t-1)}\times \\ \sum_{e=0}^{e_d}Usvd.e.(t-1)\times\epsilon_{svd} \right ]+\sum_{rsv}\left [ ro_{rvs}^{mt}\times zrvs...t\times\epsilon_{rvs}-ro_{rvs}^{m(t-1)}\times \\ zrvs...(t-1)\times\epsilon_{rvs} \right ] + \sum_{rvs}\left [ ro_{rvs}^{mlt}\times\sum_lzrvs..lt\times\epsilon_{rvs}-ro_{rvs}^{ml(t-1)}\times \\ \sum_lzrvs..l(t-1)\times\epsilon_{rvs}) \right ]\left\{\begin{matrix}
   free & \\ 
   \geq rhs_m^t, & \\ 
   = rhs_m^t & \\ 
   < rhs_m^t & 
   \end{matrix}\right.

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Usvd.e.t`
     - is the activity variable of the end-use technologies,
   * - :math:`zrvs..lt`
     - and :math:`zrvs...t` are the activity  variables of technologies with and without load regions,
   * - :math:`\epsilon_{rvs}`
     - and :math:`Esvd` are the efficiencies of the technologies; they are included by the code,
   * - :math:`ro_{svd}^{mt}`
     - is the relative factor per unit of output of technology :math:`v` (coefficient) for relational constraint :math:`m`, period :math:`t`,
   * - :math:`ro_{rvs}^{mlt}`
     - is the relative factor per unit of output of technology :math:`v` (coefficient) for relational constraint :math:`m`, load region :math:`l`, and
   * - :math:`rhs_m^t`
     - and is the right hand side of the constraint.

For this type of constraints only the :math:`ro`-coefficients have to be supplied by the user, the rest is included by the model. It can be defined with and without load regions.

