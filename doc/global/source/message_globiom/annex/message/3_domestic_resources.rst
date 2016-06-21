3 Domestic Resources 
====
3.1 	Variables
----
Extraction of domestic resources is modelled by variables that represent the quantity extracted per year in a period. A subdivision into cost categories (which are called "grades" in the model) and further into elasticity classes can be modelled.

3.1.1 	Resource Extraction  Variables
~~~~~~~~~~~~~~~~~
.. math::
   Rzrgp..t,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`R`
     - identifies resource extraction variables,
   * - :math:`z`
     - is the level on that the resource is defined (usually :math:`= R)`,
   * - :math:`r`
     - is the identifier of the resource being extracted,
   * - :math:`g`
     - is the grade (also called cost category) of resource :math:`r, g \in \{a, b, c, ...\}`.
   * - :math:`p`
     - is the class of supply elasticity, which is defined for the resource and grade, or ”.”, if no elasticity is defined for this resource and grade, and
   * - :math:`t`
     - identifies the period.

The resource variables are energy flow variables and represent the annual rate of extraction of resource :math:`r`. If several grades are defined, one variable per grade is generated (identifier :math:`g` in position 4). 

3.2 	Constraints
----
The overall availability of a resource is limited in the availability constraint per grade, annual resource consumption can be constrained per grade (sum of the elasticity classes) and total. Additionally resource depletion and dynamic resource extraction constraints can be modelled.


3.2.1 	Resource Availability per Grade
~~~~~~~~~~~~~~~~~

.. math::
   RRrg.g..

Limits the domestic resource available from one cost category (grade) over the whole time horizon. Total availability of a resource is defined  as the sum over the grades.

.. math::
   \sum_p\sum_t\Delta t\times RRrgp..t \leq Rrg - \Delta t_0R_{rg,0},

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Rrg`
     - is the total amount of resource :math:`r`, cost category :math:`g`, that is available for extraction,
   * - :math:`RRrgp..t`
     - is the annual extraction of resource :math:`r`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`,
   * - :math:`\Delta t`
     - is the length of period :math:`t`.
   * - :math:`\Delta t_0`
     - is the number of years between the base year and the first model year, and 
   * - :math:`R_{rg,0}`
     - is the extraction of resource :math:`r`, grade :math:`g` in the base year.


3.2.2 	Maximum Annual Resource Extraction
~~~~~~~~~~~~~~~~~
.. math::
   RRr....t

Limits the domestic resources available annually per period over all cost categories.

.. math::
   \sum_g\sum_pRRrgp..t \leq Rrt,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Rrgt`
     - is the maximum amount of resource :math:`r`, grade :math:`g`, that can be extracted per year of period :math:`t`, and
   * - :math:`RRrgp..t`
     - is the annual extraction of resource :math:`r`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`.


3.2.3 	Resource Depletion  Constraints
~~~~~~~~~~~~~~~~~

.. math::
   RRrg.d.t
 
The extraction of a resource in a period can be constrained  in relation to the total amount still existing in that period. For reasons of computerization these constraints can also be generated for imports and exports, although they do not have any relevance there (they could, e.g., be used for specific scenarios in order to stabilize the solution).

.. math::
   \Delta t\sum_pRRrgp..t \leq \delta_{rg}^t \left [Rrg - \Delta t_0R_{rg,0} - \sum_{\tau=1}^{t-1} \Delta\tau\times RRrgp..\tau \right ]

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Rrg`
     - is the total amount of resource :math:`r`, cost category :math:`g`, that is available for extraction,
   * - :math:`RRrgp..t`
     - is the annual extraction of resource :math:`r`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`,
   * - :math:`\delta_{rg}^t`
     - is the maximum fraction of resource :math:`r`, cost category :math:`g`, that can be extracted in period :math:`t`,
   * - :math:`Rrg`
     - is the total amount available in the base year,
   * - :math:`\Delta t`
     - is the length of period :math:`t` in years,
   * - :math:`\Delta t_0`
     - is the number of years between the base year and the first model year, and
   * - :math:`R_{rg,0}`
     - is the extraction of resource :math:`r`, grade :math:`g` in the base year.


3.2.4 	Maximum Annual Resource Extraction per Grade
~~~~~~~~~~~~~~~~~

.. math::
   RRrg.a.t

Limits the domestic resources available from one cost category per year.

.. math::
   \sum_pRRrgp..t \leq Rrgt.

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Rrg`
     - is the total amount of resource :math:`r`, cost category :math:`g`, that is available for extraction, and
   * - :math:`RRrgp..t`
     - is the annual extraction of resource :math:`r`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`.


3.2.5 	Upper Dynamic Resource Extraction Constraints
~~~~~~~~~~~~~~~~~

.. math::
   MRRr...t
 
The annual extraction level of a resource in a period can be related to the previous one by a growth parameter and an increment of extraction capacity resulting in upper dynamic extraction constraints. For the first period the extraction is related to the activity in the baseyear.
 
.. math::
   \sum_{g,p} RRrgp..t - \gamma_{rt}^0\sum_{g,p}RRrgp..(t-1) \leq g_{rt}^0,
 
where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`\gamma_{rt}^0`
     - is the maximum growth of extraction of resource :math:`r` between period :math:`t−1` and :math:`t`,
   * - :math:`g_{rt}^0`
     - is the initial size (increment) of extraction of resource :math:`r` in period :math:`t`, and
   * - :math:`RRrgp..t`
     - is the annual extraction of resource :math:`r`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`.


3.2.6    Lower Dynamic Resource Extraction  Constraints
~~~~~~~~~~~~~~~~~

.. math::
   LRRr...t

The annual extraction level of a resource in a period can also be related to the previous one by a decrease parameter  and a decrement resulting in lower dynamic extraction constraints. For the first period the extraction is related to the activity in the baseyear.

.. math::
   \sum_{g,p}RRrgp..t - \gamma_{rt}\sum_{g,p}RRrgp..(t-1)\geq - g_{rt},
 
where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`\gamma_{rt}`
     - is the maximum decrease of extraction of resource :math:`r` between period :math:`t−1` and :math:`t`,
   * - :math:`g_{rt}`
     - is the "last" size (decrement) of extraction of resource :math:`r` in period :math:`t`, and
   * - :math:`RRrgp..t`
     - is the annual extraction of resource :math:`r`, cost category (grade) :math:`g` and elasticity class :math:`p` in period :math:`t`.


3.2.7 	Dynamic Extraction  Constraints per Grade
~~~~~~~~~~~~~~~~~

.. math::
   MRRrg..t,
   
and

.. math::
   LRRrg..t
The same kind of relations as described in sections 3.2.5 and 3.2.6 can be defined per grade of the resource.
