5 Stock-piles
===============

5.1 	Variables
---------------

Generally MESSAGE does not generate any variables related to an energy carrier alone. However, in the case of man-made fuels, that are accumulated over time, a variable that shifts the quantities to be transferred from one period to the other is necessary.

5.1.1 	Stock-pile Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Qf b....t, 

where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`Q`
     - identifies stock-pile variables,
   * - :math:`f`
     - identifies the fuel with stock-pile,
   * - :math:`b`
     - distinguishes  the variable from the equation, and
   * - :math:`t`
     - is the period identifier.

The stock-pile variables represent the amount of fuel :math:`f` that is transferred from period :math:`t` into period :math:`t + 1`. Note that these variables do not represent annual quantities, they refer to the period as a whole. These variables are a special type of storage, that just transfers the quantity of an energy carrier available in one period into the next period. Stock-piles are defined  as a separate level. For all other energy carriers any overproduction that occurs in a period is lost.

5.2 	Constraints
-----------------

5.2.1 	Stock-piling Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. math::
   Qf.....t

:math:`Q` is a special level on that energy forms can be defined that are accumulated over time and consumed in later periods. One example is the accumulation of plutonium and later use in fast breeder reactors.

The general form of this constraint is:

.. math::
   Qfb....t-Qfb....(t-1)+\sum _v\left [ \sum _t \Delta t \times (zfvd..lt+\beta _{\phi vd}^f\times z\phi vd..lt- \\ \epsilon _{svf}\times zsvfu.lt-\beta _{sv\phi}^f\times zsv\phi ..lt)+\Delta t\times \iota (svd,f)\times Yzsvd..(t+1)-\\ \Delta(t-\tau _{svd}-1)\times \rho (svd,f) \times Yzsvd..(t-\tau_{svd})\right ]=0


where

.. list-table:: 
   :widths: 40 110
   :header-rows: 0

   * - :math:`f`
     - is the identifier of the man-made fuel (e.g. plutonium, U_{233}),
   * - :math:`\tau_{svd}`
     - is the plant life of technology :math:`v` in periods,
   * - :math:`\iota(svd,f)`
     - is the ”first  inventory” of technology :math:`v` of :math:`f` (relative to capacity of main output),
   * - :math:`\rho(svd,f)`
     - is the ”last core” of :math:`f` in technology :math:`v`, see also section :ref:`resourceextraction`,
   * - :math:`\Delta t`
     - is the length of period :math:`t` in years,
   * - :math:`zfvd..lt`
     - is the annual input of technology :math:`v` of fuel :math:`f` in load region :math:`l` and period :math:`t` (:math:`l` is ”.” if :math:`v` does not have load regions), and
   * - :math:`Yzfvd..t`
     - is the annual new installation of technology :math:`v` in period :math:`t`.
