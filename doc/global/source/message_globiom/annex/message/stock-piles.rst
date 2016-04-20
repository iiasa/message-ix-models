6 Stock-piles
===============

6.1 	Variables
---------------

Generally MESSAGE does not generate any variables related to an energy carrier alone. However, in the case of man-made fuels, that are accumulated over time, a variable that shifts the quantities to be transferred from one period to the other is necessary.

6.1.1 	Stock-pile Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:math:`Qf b....t`, 

where
:math:`Q`	          identifies stock-pile variables,
:math:`f`          	identifies the fuel with stock-pile,
:math:`b`          	distinguishes  the variable from the equation, and
:math:`t`          	is the period identifier.


The stock-pile variables represent the amount of fuel :math:`f` that is transferred from period :math:`t` into period :math:`t + 1`. Note that these variables do not represent annual quantities, they refer to the period as a whole. These variables are a special type of storage, that just transfers the quantity of an energy carrier available in one period into the next period. Stock-piles are defined  as a separate level. For all other energy carriers any overproduction that occurs in a period is lost.

6.2 	Constraints
-----------------

6.2.1 	Stock-piling Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:math:`Qf.....t`

:math:`Q` is a special level on that energy forms can be defined that are accumulated over time and consumed in later periods. One example is the accumulation of plutonium and later use in fast breeder reactors.

The general form of this constraint is:

.. math::

Qf b....t − Qf b....(t − 1) + v	l ∆t × ( zf vd..lt + βf × zφvd..lt −
 
Esvf   × zsvf u.lt − βf × zsvφ..lt ) + ∆t × ι(svd, f ) × Y zsvd..(t + 1) −
 
∆(t − τsvd − 1) × ρ(svd, f ) ×  Y zsvd..(t − τsvd) ] = 0 ,

where
:math:`f`	          is the identifier of the man-made fuel (e.g. plutonium, U233),
:math:`τsvd`       	is the plant life of technology :math:`v` in periods,
:math:`ι(svd, f )` 	is the ”first  inventory”  of technology :math:`v` of :math:`f` (relative to capacity of main output),
:math:`ρ(svd, f )` 	is the ”last core” of :math:`f` in technology :math:`v`, see also section  6.1.5,
:math:`∆t`         	is the length of period :math:`t` in years,
:math:`zf vd..lt`  	is the annual input of technology :math:`v` of fuel :math:`f` in load region :math:`l` and period :math:`t` (l is ”.” if v does not have load regions), and
:math:`Y zf vd..t` 	is the annual new installation of technology :math:`v` in period :math:`t`.
 
