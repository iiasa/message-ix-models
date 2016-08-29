4 Energy flows
==============

4.1 	Balance Equations
----------------

Energy flows are modelled by linking the activity variables of the different conversion  and resource extraction technologies in balance constraints. These constraints ensure that only the amounts of energy available are consumed. There are no further variables required to model energy flows.

Energy demands are also modelled  as part of a balance constraint: the right hand side defines the amount to be supplied by the technologies in this constraint.

The following description of the energy flow constraints in MESSAGE is given for the following set of level identifiers:

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`u`
     - Useful energy (demand level),
   * - :math:`f`
     - Final energy (after transmission and distribution),
   * - :math:`x`
     - Secondary energy,
   * - :math:`a`
     - Primary energy, and
   * - :math:`r`
     - Energy resources.

The first level in the above list gives it a special meaning (see section :ref:`activitiesECT`. Clearly any other combination of identifiers is also possible.

4.1.1 	Demand Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   \sum_{sv} \epsilon_{zsvd} \times zsvd....rr...ttt + \sum_{sv} \beta_{zsv\delta}^d \times zsv\delta....rr...ttt \geq zd......rr...ttt

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`zd......rr...ttt`
     - is the annual demand for :math:`d` in region :math:`rr` and period :math:`ttt`,
   * - :math:`zsvd....rr...ttt`
     - is the activity of end-use technology :math:`zsvd` in region :math:`rr` and period :math:`ttt` (see section  :ref:`activitiesECT`),
   * - :math:`\epsilon _{zsvd}`
     - is the efficiency of end-use technology :math:`zsvd` in converting :math:`s` to :math:`d`,
   * - :math:`\beta _{zsv\delta}^d`
     - is the efficiency of end-use technology :math:`zsvd` in producing by-product :math:`d` from :math:`s` (:math:`\delta` is the main output of the technology).

Out of the predefined  levels each one can be chosen as demand  level. However, the first level has a special feature. This is related to the fact that useful energy is usually produced on-site, e.g., space heat is produced by a central heating system, and the load variations over the year are all covered by this one system. Thus, an allocation of production technologies to the different areas of the load curve, like the model would set it up according to the relation between investment and operating costs would ignore the fact that these systems are not located in the same place and are not connected to each other. MESSAGE represents the
end-use technologies by one variable per period that produces the required useful energy in the load pattern needed and requires the inputs in the same pattern. For special technologies like, e.g., night storage heating systems, this pattern can be changed to represent the internal storage capability of the system.

If another that the level is chosen as demand  level all demand constraints for energy carriers that are modelled with load regions are generated for each load region.

.. _distbal:

4.1.2 	Other Balances
~~~~~~~~~~~~~~~~~~~~~~~~~~

These constraint match the consumption of a specific energy form with the production of this energy form on any of the defined energy levels. They are generated for each load region, if the energy form is modelled with load regions.

.. math::
   \sum_{sv} \epsilon_{zsve} \times zsve....rrlllttt + \sum_{sv} \beta_{zsv\kappa}^e \times zsv\kappa....rrlllttt -
   \sum_{sv} zevd....rrlllttt - \sum_{sv} \beta_{z\kappavd}^d \times z\kappavd....rrlllttt \geq 0.

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`zsve....rrlllttt`
     - is the activity of the technology producing energy form :math:`e` in regions :math:`rr`, load region :math:`lll` and period :math:`ttt` (see section :ref:`activitiesECT`),
   * - :math:`\epsilon _{zsve}`
     - is the efficiency of technology :math:`zsve` in producing :math:`s`,
   * - :math:`zevd....rrlllttt`
     - is the activity of the technology :math:`zevd` consuming energy form :math:`e` in region :math:`rr` and period :math:`ttt`,
   * - :math:`\beta_{zsv\kappa}^e`
     - is the production of fuel :math:`e` relative to the main output :math:`\kappa` by technology :math:`zsv\kappa`, and
   * - :math:`\beta_{z\kappavd}^d`
     - is the consumption of fuel :math:`e` relative to the main output :math:`d` by technology :math:`z\kappavd`.

In case technologies are modeled with given production or consumption load curves, the variables are the annual variables multiplied by the share of the total energy flow in this load region :math:`\eta_{zsve}^l`:
.. math:
   \eta_{zsve}^l \times zsve....rr...ttt

4.1.3 	Resource Consumption
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The resources produced by the extraction technologies in a period can come from different cost categories (also called grades), which can, e.g., represent the different effort to reach certain resources. Short-term variations in price due to steeply increasing demand can be represented by an elasticity approach (see section 9.11).

.. math::
   \sum_{ttt} \sum_{g} rzfg....rr...ttt \leq rzfg....rr,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`rzfg....rr...ttt`
     - is the annual extraction of resource :math:`f`, cost category (grade) :math:`g` in region :math:`rr` and period :math:`ttt`, and
   * - :math:`rzfg....rr`
     - is the total available amount of resource :math:`f`, grade :math:`g` in region :math:`rr`.

 
