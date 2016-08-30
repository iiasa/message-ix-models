5 Stock-piles
===============

5.1 	Variables
---------------

Generally MESSAGE does not generate any variables related to an energy carrier alone. However, in the case of man-made fuels, that are accumulated over time, a variable that shifts the quantities to be transferred from one period to the other is generated.

5.1.1 	Stock-pile Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   qfb.....rrlllttt, 

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`q`
     - identifies stock-pile variables,
   * - :math:`f`
     - identifies the fuel with stock-pile,
   * - :math:`b`
     - distinguishes  the variable from the equation, and
   * - :math:`rrlllttt`
     - are the region, load region, and period identifiers respectively.

The stock-pile variables represent the amount of fuel :math:`f` that is transferred from period :math:`t` into period :math:`t + 1`. Note that these variables do not represent annual quantities, they refer to the period as a whole. These variables are a special type of storage, that just transfers the quantity of an energy carrier available in one period into the next period. Stock-piles are defined  as a separate level. For all other energy carriers any overproduction that occurs in a period is lost.

5.2 	Constraints
-----------------

5.2.1 	Stock-piling Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   qf......rrlllttt

:math:`q` is a special level on that energy forms can be defined that are accumulated over time and consumed in later periods. One example is the accumulation of plutonium and later use in fast breeder reactors.

The general form of this constraint is:

.. math::
   qfb.....rrlllttt-Qfb.....rrlll(ttt-1)+\sum_v \left[ \sum _t \Delta t \times (zsvf....rrlllttt+\beta _{zsv\phi}^f\times zsv\phi....rrlllttt- \\ \epsilon _{zfvo}\times zfvo....rrlllttt-\beta _{z\phivo}^f\times z\phivo....rrlllttt)-\Delta t \times \iota_{zfvd} \times yzfvd...rr...(ttt)-\\ \Delta(t-\tau _{zfvd}-1)\times \rho_{zfvd} \times yzfvd...rr...(ttt-\tau_{zfvd}) \right] = 0


where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`f`
     - is the identifier of the man-made fuel (e.g. plutonium, U_{233}),
   * - :math:`\tau_{zsvd}`
     - is the plant life of technology :math:`v` in periods,
   * - :math:`\iota_{zsvf}`
     - is the ”first  inventory” of technology :math:`zsvf` of :math:`f` (relative to capacity of main output),
   * - :math:`\rho_{zfvd}`
     - is the ”last core” of :math:`f` in technology :math:`zfvd`, see also section :ref:`resourceextraction`,
   * - :math:`\Delta t`
     - is the length of period :math:`ttt` in years,
   * - :math:`zfvd....rrlllttt`
     - is the annual input of technology :math:`zfvd` of fuel :math:`f` in load region :math:`lll` and period :math:`ttt` (:math:`lll` is ”...” if :math:`zfvd` does not have load regions), and
   * - :math:`yzfvd...rr...ttt`
     - is the annual new installation of technology :math:`zfvd` in period :math:`ttt`.
