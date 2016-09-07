6 User-defined Relations
=======================

.. math::
   nname...rrlllttt

or

.. math:: 
   pname...rrlllttt

The user-defined relations allow the user to construct constraints that are not included in the basic set of constraints. For each technology  the user can specify coefficients with that either the production variables (see section :ref:`_activitiesECT`), the annual new installation variables (see section :ref:`_capacititesECT`) or the total capacity in a year (like it is used in the capacity constraints, see section :ref:`_capacityconstr`) can be included in the relation. The relations can be defined with and without load regions, have a lower, upper or fix right hand side or remain free (non-binding) and may have an entry in the objective function, i.e., the objective function entries of all members of this relation are increased/decreased by this value. There are two types of user-defined constraints (denoted by :math`n` or :math:`p` as first character), for which the entries to the objective function–without discounting are summed up under the cost accounting rows :math:`CAR1` and :math:`CAR2` (see chapter :ref:`_objectivecostcounters`).

The formulation of the user-defined relations is given for relations, that are related to the main output of the technologies. It is also possible (e.g., for greenhouse gas emissions) to relate the constraint to the main input of the technology, i.e. the amount of fuel used. In this case the efficiencies would be omitted from the formulation.

Relations without load regions sum up the activities (multiplied with the given coefficients) of all variables defined to be in this constraint. If a technology has load regions, the activity variables for all load regions of this technology are included. If the total capacity of a technology is included, all new capacities from previous periods still operating are included, if new capacities are included, the annual new installation of the current period is taken.

.. math::
   \sum_{zrvs}\left [ ro_{zrvs}^{mlt}\times\sum_{lll} zrvs...rrlllttt\times\epsilon_{zrvs}+ro_{zrvs}^{mt}\times zrvs....rr...ttt \times \epsilon_{zrvs}+ \right. \\ \left. \sum_{\tau=t-ip}^t pl_\tau \times rc_{zrvs}^{mt} \times yzrvs...rr...\tau \right ] \sim rhs_m^t

where

.. list-table:: 
   :widths: 60 110
   :header-rows: 0

   * - :math:`zrvs....rrlllttt`
     - activity variables of technologies (lll if modelled with and '...' without load regions,
   * - :math:`yzrvs...rr...ttt`
     - capacity variables of the technologies,
   * - :math:`\epsilon_{zrvs}`
     - efficiencies of the technologies; they are automatically included,
   * - :math:`ro_{zsvd}^{mt}`
     - relative factor per unit of output of technology :math:`zsvd` (coefficient) for relational constraint :math:`m`,
   * - :math:`rc_{zsvd}^{mt}`
     - relative factor per unit of new built capacity,
   * - :math:`ro_{zrvs}^{mlt}`
     - relative factor per unit of output of technology :math:`zsvd` (coefficient) for relational constraint :math:`m` and load region :math:`l`,
   * - :math:`rc_{zrvs}^{mt}`
     - relative factor per unit of new built capacity,
   * - :math:`pl_t`
     - is 1 for relations with new construction and :math:`\Delta\tau` (period length) for relations with total capacity,
   * - :math:`ip`
     - is 1 for accounting during construction and the plant life in periods for accounting of total capacity,
   * - :math:`\sim`
     - :math:`\geq, \leq, =, or free` indicating a lower, upper, equality, or free constraint, and
   * - :math:`rhs_m^t`
     - is the right hand side of the constraint.
