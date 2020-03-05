3 Domestic Resources
====================

3.1 Variables
-------------

Extraction of domestic resources is modelled by variables that represent the quantity extracted per year in a period. A subdivision into cost categories (which are called "grades" in the model) can be modelled.


.. _resourceextraction:

3.1.1 Resource Extraction Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   rzfg....rr...ttt

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`r`
     - identifies resource extraction variables,
   * - :math:`z`
     - level on that the resource is defined (usually :math:`= r)`,
   * - :math:`f`
     - identifier of the resource being extracted,
   * - :math:`g`
     - grade (also called cost category) of resource :math:`r, g \in \{a, b, c, ...\}`.
   * - :math:`rr`
     - identifies the region.
   * - :math:`ttt`
     - identifies the time period.

The resource variables are energy flow variables and represent the annual rate of extraction of resource :math:`f`. If several grades are defined, one variable per grade is generated (identifier :math:`g` in position 4).

3.2 Constraints
---------------

The overall availability of a resource is limited in the availability constraint per grade, annual resource consumption can be constrained per grade and total. Additionally resource depletion and dynamic resource extraction constraints can be modelled.


3.2.1 Total Resource Availability per Grade
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   rzfgg...rr

Limits the domestic resource available from one cost category (grade) over the whole time horizon.

.. math::
   \sum_p\sum_t\Delta t\times rzfg....rr...ttt \leq rzfgg...rr - \Delta t_0R_{zfg,0},

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`rzfgg...rr`
     - total amount of resource :math:`f`, cost category :math:`g`, that is available for extraction in a given region :math:`rr`,
   * - :math:`rzfg....rr...ttt`
     - annual extraction of resource :math:`f`, cost category (grade) :math:`g`  in region :math:`rr` and period :math:`ttt`,
   * - :math:`\Delta t`
     - length of period :math:`t`.
   * - :math:`\Delta t_0`
     - number of years between the base year and the first model year, and
   * - :math:`R_{zfg,0}`
     - extraction of resource :math:`r`, grade :math:`g` in the base year.


3.2.2 Resource Depletion Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   rzfgg...rr...ttt

The extraction of a resource in a period can be constrained  in relation to the total amount still existing at the beginning of the period.

.. math::
   \Delta t \times rzfg....rr...ttt \leq \delta_{fg}^t \left [rzfgg...rr - \Delta t_0R_{rzfg,0} - \sum_{\tau=1}^{t-1} \Delta\tau\times rrzfg...rr...\tau \right ]

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`rzfgg...rr`
     - total amount of resource :math:`f`, cost category :math:`g`, that is available for extraction,
   * - :math:`rzfg....rr...ttt`
     - annual extraction of resource :math:`f`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`,
   * - :math:`\delta_{fg}^t`
     - maximum fraction of resource :math:`f`, cost category :math:`g`, that can be extracted in period :math:`ttt`,
   * - :math:`\Delta t`
     - length of period :math:`t` in years,
   * - :math:`\Delta t_0`
     - number of years between the base year and the first model year, and
   * - :math:`R_{rzfg,0}`
     - extraction of resource :math:`r`, grade :math:`g` in the base year.


3.2.4 Maximum Annual Resource Extraction per Grade
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Limits the domestic resource availability from one cost category per year.

.. math::
   rzfg....rr...ttt \leq value.

where

.. list-table::
   :widths: 40 110
   :header-rows: 0

   * - :math:`rzfg....rr...ttt`
     - annual extraction of resource :math:`f`, cost category (grade) :math:`g` in period :math:`ttt`.

.. _upperdynamicREC:

3.2.5 Dynamic Resource Extraction Constraints per Grade
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   mrzfg...rr...ttt

The annual extraction level of a resource in a period can be related to the previous one by a growth parameter and an increment of extraction activity resulting in upper dynamic extraction constraints. For the first period the extraction is related to the activity in the baseyear.

.. math::
   rzfg....rr...ttt - \gamma_fg \times rzfg....rr...(ttt-1) \leq g_{ft}^0,

where

.. list-table::
   :widths: 60 110
   :header-rows: 0

   * - :math:`m`
     - is m or l, indicating upper and lower constraints respectively (lower limits are generally not used),
   * - :math:`\gamma_{ft}^0`
     - maximum growth rate for the extraction of resource :math:`f` between period :math:`ttt − 1` and :math:`ttt`,
   * - :math:`g_{ft}^0`
     - annual increment of the extraction of resource :math:`f` in period :math:`ttt` (must be > 0 if the resource (grade) is not extracted in the base year), and
   * - :math:`rzfg....rr...ttt`
     - annual extraction of resource :math:`f`, cost category (grade) :math:`g` in period :math:`ttt`.
