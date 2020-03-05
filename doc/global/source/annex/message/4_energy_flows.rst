4 Energy flows
==============

.. _enebal:

4.1 Balance Equations
---------------------

Energy flows are modelled by linking the activity variables of the different conversion, resource extraction technologies and demands in balance constraints. These constraints ensure that only the amounts of energy available are consumed. There are no further variables required to model energy flows.

Energy demands are also modelled  as part of a balance constraint: the right hand side defines the amount to be supplied by the technologies in this constraint.

The description of the energy flow constraints in MESSAGE is given for the following set of level identifiers:

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

The first level in the above list gives it a special meaning (see section :ref:`_activitiesECT`). Clearly any other combination of identifiers is also possible.

Another exception is a level labelled :math:`q`, this letter is reserved for stock piles (see section :ref:`_stockpiles`).

**IMPORTANT:** Generally central production systems should not deliver to the first (demand) level. In this case the production of the system would be forced to follow the demand pattern.

4.1.1 Demand Constraints
~~~~~~~~~~~~~~~~~~~~~~~~
.. math::

   zd......rr...ttt

.. math::
   \sum_{sv} \epsilon_{zsvd} \times zsvd....rr...ttt + \sum_{sv} \beta_{zsv\delta}^d \times zsv\delta....rr...ttt \geq D_{drt}

where

.. list-table::
   :widths: 60 110
   :header-rows: 0

   * - :math:`zd......rr...ttt`
     - annual demand equation for :math:`d` in region :math:`rr` and period :math:`ttt`,
   * - :math:`zsvd....rr...ttt`
     - activity of end-use technology :math:`zsvd` in region :math:`rr` and period :math:`ttt` (see section  :ref:`_activitiesECT`),
   * - :math:`\epsilon _{zsvd}`
     - efficiency of end-use technology :math:`zsvd` in converting :math:`s` to :math:`d`,
   * - :math:`\beta _{zsv\delta}^d`
     - efficiency of end-use technology :math:`zsvd` in producing by-product :math:`d` from :math:`s` (:math:`\delta` is the main output of the technology), and
   * - :math:`D_{drt}`
     - annual demand for :math:`d` in region :math:`rr` and period :math:`ttt`.


The first level, usually labelled 'demand level', has a special feature. This is related to the fact that useful energy is usually produced on-site, e.g., space heat is produced by a central heating system, and the load variations over the year are all covered by this one system. Thus, an allocation of production technologies to the different areas of the load curve, like the model would set it up according to the relation between investment and operating costs would ignore the fact that these systems are not located in the same place and are not connected to each other. MESSAGE represents the end-use technologies by one variable per period that produces the required useful energy in the load pattern needed and requires the inputs in the same pattern. For special technologies like, e.g., night storage heating systems, this pattern can be changed to represent the internal storage capability of the system.

Each energy form on any level can have an external demand. In this case the demand is given as right hand side to the balance equation (see section :ref:`_enebal`). If the energy carrier is modelled with load regions, the right hand sides are given for each load region. If no load region pattern is defined, the demand is assumed to be a base load demand.

.. _distbal:

4.1.2 Other Balances
~~~~~~~~~~~~~~~~~~~~

These constraint match the consumption of a specific energy form with the production of this energy form on any of the defined energy levels. They are generated for each load region, if the energy form is modelled with load regions.

.. math::

   \sum_{sv} \epsilon_{zsve} \times zsve....rrlllttt + \sum_{sv} \beta_{zsv \kappa }^e \times zsv \kappa ....rrlllttt - \\
   \sum_{zvd} zevd....rrlllttt - \sum_{zkvd} \beta_{z \kappa vd}^e \times z \kappa vd....rrlllttt \geq 0

where

.. list-table::
   :widths: 60 110
   :header-rows: 0

   * - :math:`zsve....rrlllttt`
     - activity of the technology producing energy form :math:`e` in regions :math:`rr`, load region :math:`lll` and period :math:`ttt` (see section :ref:`_activitiesECT`),
   * - :math:`\epsilon _{zsve}`
     - efficiency of technology :math:`zsve` in producing :math:`s`,
   * - :math:`zevd....rrlllttt`
     - activity of the technology :math:`zevd` consuming energy form :math:`e` in region :math:`rr` and period :math:`ttt`,
   * - :math:`\beta_{zsv \kappa }^e`
     - production of fuel :math:`e` relative to the main output :math:` \kappa ` by technology :math:`zsv \kappa `, and
   * - :math:`\beta_{z \kappa vd}^e`
     - consumption of fuel :math:`e` relative to the main output :math:`d` by technology :math:`z \kappa vd`.

In case technologies are modeled with given production or consumption load curves, the variables are the annual variables multiplied by the share of the total energy flow in this load region :math:`\eta_{zsve}^l`:

.. math:
   \eta_{zsve}^l \times zsve....rr...ttt

4.1.3 Resource Balance
~~~~~~~~~~~~~~~~~~~~~~

The resources produced by the extraction technologies in a period can come from different cost categories (also called grades), which can, e.g., represent the different effort to reach certain resources. Short-term variations in price due to steeply increasing demand can be represented by an elasticity approach (see section 9.11).

.. math::
   \sum_{ttt} \sum_{g} rzfg....rr...ttt \leq rzfg....rr

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`rzfg....rr...ttt`
     - annual extraction of resource :math:`f`, cost category (grade) :math:`g` in region :math:`rr` and period :math:`ttt`, and
   * - :math:`rzfg....rr`
     - total available amount of resource :math:`f`, grade :math:`g` in region :math:`rr`.
