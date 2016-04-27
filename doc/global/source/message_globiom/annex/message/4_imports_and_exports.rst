4 Imports and Exports
=====
4.1 	Variables
----

Imports and exports are modelled by variables that represent the quantity imported per year in a period. A subdivision into countries and further into elasticity classes can be modelled.

4.1.1 	Import  Variables
~~~~~~~~~~~~~~~~~~~~~

.. math::
   Izscp.lt,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`I`
     - identifies import variables,
   * - :math:`z`
     - is the level on that the imported energy form is defined (usually primary energy and secondary energy),
   * - :math:`s`
     - identifies the imported energy carrier,
   * - :math:`c`
     - is the identifier of the country or region the imports come from,
   * - :math:`p`
     - is the class of supply elasticity, which is defined for the energy carrier and country, or ”.”, if no elasticity is defined for this energy carrier and country,
   * - :math:`l`
     - is the load region identifier if :math:`s` is modelled with load regions, otherwise ”.”, and
   * - :math:`t`
     - identifies the period.

The import variables are energy flow variables and represent the annual import of the identified energy carrier from the country or region given. If supply elasticities are defined for the import of this energy carrier and country one variable per elasticity class (identifier :math:`p` in position 5) is generated.

4.1.2 	Export  Variables
~~~~~~~~~~~~~~~~~~~~~

.. math::
   Ezrcp.lt,
 
where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`E`
     - is the identifier for export variables, and
   * - :math:`z`
     - is the level on that the exported energy form is defined (usually primary energy and secondary energy),
   * - :math:`s`
     - identifies the exported energy carrier,
   * - :math:`c`
     - is the identifier of the country or region the exports go to,
   * - :math:`p`
     - is the class of supply elasticity, which is defined for the energy carrier and country, or ”.”, if no elasticity is defined for this energy carrier and country,
   * - :math:`l`
     - is the load region identifier if :math:`s` is modelled with load regions, otherwise ”.”, and
   * - :math:`t`
     - identifies the period.

The export variables are energy flow variables and represent the annual export of the identified energy carrier to the country or region given. If supply elasticities are defined for the export of this energy carrier and country one variable per elasticity class (identifier :math:`p` in position 5) is generated.

4.2 	Constraints
-----

4.2.1 	Imports per Country
~~~~~~~~~~~~~~~~~~

.. math::
   Izrc.g..

Limits the imports of a fuel from a specific country :math:`c over the whole horizon.

.. math::
   \sum_p\sum_t\Delta t\times Izrcp..t \leq Irc,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Irc`
     - is the total import limit  for :math:`r` from country :math:`c`,
   * - :math:`Izrcp..t`
     - is the annual import of :math:`r` from country :math:`c`, elasticity class :math:`p` in period :math:`t`, and
   * - :math:`\Delta t`
     - is the length of period :math:`t` in years.


4.2.2 	Maximum Annual Imports
~~~~~~~~~~~~~~

.. math::
   Izr....t

Limits the annual imports of a fuel from all countries per period.

.. math::
   \sum_c\sum_p Izrcp..t \leq Irt,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Irt`
     - is the annual import limit for :math:`r` in period :math:`t`, and
   * - :math:`Izrcp..t`
     - is the annual import of :math:`r` from country :math:`c`, elasticity class :math:`p` in period :math:`t`.


4.2.3 	Maximum Annual Imports per Country
~~~~~~~~~~~~~~~~

.. math::
   Izrc.a.t

Limits the imports from one country per year.

.. math:: 
   \sum_pIzrcp..t\leq Irct,

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Irct`
     - is the limit on the annual imports from country :math:`c`, period :math:`t` of fuel :math:`r`, and
   * - :math:`Izrcp..t`
     - is the annual import of :math:`r` from country :math:`c`, elasticity class :math:`p` in period :math:`t`.


4.2.4 	Upper Dynamic Import  Constraints
~~~~~~~~~~~~~~~~~~~~~~

.. math::
   MIzr...t

The annual import level of a fuel in a period can, like the resource extraction, be related to the previous one by a growth parameter and an increment resulting in upper dynamic constraints.
 
.. math::
   \sum_{c,p}Izrcp..t - \gamma_{rt}^o\sum_{c,p}Izrcp..(t-1)\leq g_{rt}^o,
 
where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Izrcp..t`
     - is the annual import of :math:`r` from country :math:`c`, elasticity class :math:`p` in period :math:`t`,
   * - :math:`\gamma_{rt}^o`
     - is the maximum increase of import of :math:`r` between period :math:`t−1` and :math:`t`, and
   * - :math:`g_{rt}^o`
     - is the initial size (increment) of import of :math:`r` in period :math:`t`.


4.2.5 	Lower Dynamic Import  Constraints
~~~~~~~~~~~~~~~~~~~~~

.. math::
   LIzr...t

The annual import level of a fuel in a period can also be related to the previous one by a decrease parameter and a decrement resulting in lower dynamic import constraints.

.. math::
   \sum_{c,p}Izrcp..t - \gamma_{rt}\sum_{c,p}Izrcp..(t-1)\geq - g_{rt},

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Izrcp..t`
     - is the annual import of :math:`r` from country :math:`c`, elasticity class :math:`p` in period :math:`t`,
   * - :math:`\gamma_{rt}`
     - is the maximum decrease of import of :math:`r` between period :math:`t−1` and :math:`t`, and
   * - :math:`g_{rt}`
     - is the "last" size (decrement) of import of :math:`r` in period :math:`t`.


4.2.6 	Dynamic Import  Constraints per Country
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   MIzrc..t

and

.. math::
   LIzrc..t

The same kind of relations can be defined per country from that the fuel is imported.

4.2.7 	Constraints on Exports
~~~~~~~~~~~~~~~~~~~~~~~~~

The exports of fuels can principally be limited in the same way as the imports. In the identifiers of the variables and constraints the :math:`"I"` is substituted by an :math:`"E"`.
