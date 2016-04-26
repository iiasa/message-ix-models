9 Special Features of the Matrix Generator
===========================================

The mathematical formulation of MESSAGE as presented  in the previous sections shows the structure of all constraints as the matrix generator builds them up. The background of the more complicated features is given here for a better understanding.

9.1 	The  Time  Horizon–Discounting the Costs
----------------------------------------------

The whole time horizon of the calculations is divided into periods of optional length. All variables of MESSAGE are represented  as average over the period they represent, resulting in a step-function. All entries in the objective function are discounted from the middle of the respective period to the first year, if they relate to energy flow variables and from the beginning of that period if they represent power variables. The function to discount the costs has the following form:
 
.. math::
   

where

:math:`t`      	    is the cost figure to be discounted,

:math:`ct`         	is the objective function coefficient in period :math:`t`,

:math:`fi`          for costs connected to investments,

:math:`∆t(1+drt)2` 	else, and

:math:`drk`        	is the discount rate in period :math:`k`.

9.2 	Distributions of Investments
-----------------------------------

In order to support short term applications of MESSAGE the possibility to distribute the investments for a new built technology over several periods was implemented. The same type of distributions can be applied to entries in user defined relations if they relate to construction. The distribution of investments can be performed in several ways. There is one common parameter that is needed for all of these possibilities, the construction time of the technology [:math:`ct`].

The implemented possiblilities are: 

1. Explicit  definition of the different shares of investments for the years of construction. The input are ct figures that will be normalized  to 1 internally.  

2. The investment distribution is given as a polynomium of 2nd degree, the input consists of the three coefficients:

.. math::

y  = a + bx + cx2   , x  = 1(1)ct,

where :math:`ct` is the construction time. The values of the function are internally normalized to 1, taking into account the construction time. 

3. Equal distribution of the investments over the construction period. 

4. A distribution function based on a logistic function of the type

.. math::

100 f = 1 + e−α(x−x0 )   ,

where

.. math::

x0   =  ct

and

.. math:: 

α  =  2  ln ( 100 − 1) . ct 	E

This function is expanded to a normalized distribution function of the following type:

.. math:: 

g =  1 + e− 100  	E     −1)(x−50) 50 1 − E  ×	E   50
 
:math:`g` gives the accumulated investment at the time :math:`x`, :math:`x` is given in percent of the construction time. The parameter :math:`E` describes the difference of the investment in the different years. E near to 50 results nearly in equal distribution, an :math:`E` close to 0 indicates high concentration of the expenditures in the middle of the construction period.

In order to shift the peak of costs away from the middle of the construction period the function is transformed by a polynomium:

.. math::

x  = az2  + bz	, 0 < z < 100 ,
 
where 

.. math::

100d − d2     , 0 < d < 100 ,

and
 
a =  1 −  b
 
:math:`d` denotes the time at that the peak of expenditures occurs in percent of :math:`ct`.

The distribution of these yearly shares of investments is done starting in the first period of operation with a one years share, the expenditures of the remaining :math:`ct − 1` years are distributed to the previous periods.

The coefficients of the capacity variables of a technology in a relational constraint can be distributed like the investments.


9.3 	The Contribution of Capacities Existing in the Base Year
---------------------------------------------------------------

The possible contribution of an installation that exists in the base year is kept track of over time. There are two possibilities to give the necessary information to MESSAGE.

1. Define the capacities that were built in the years :math:`iyr, ..., iyr − τ + 1`, with :math:`iyr` = base year and :math:`τ` = plant life in years explicitly. These capacities are then distributed to historic periods of the length :math:`ν`.

2. Define the total capacity, :math:`c0`, that exists in :math:`iyr` and the rate at that it grew in the last :math:`τ` years, :math:`γ`. This information is then converted to one similar to 1. by using the function:

.. math:: 

γ−ν  − 1 y0  = c0 ν(γ−τ	, 
yt   = y0 γ−t × ν   , t = 1(1)		, ν

where

:math:`yt`      	   is the annual construction in period :math:`−t`, (0 = base year),

:math:`γ`          	is the annual growth of new installations before the base year,

:math:`c0`         	is the total capacity in the base year,

:math:`τ`          	is the plant life, and

:math:`ν`          	is the length of the periods in that the time before the base year is divided.


The right hand sides in the capacity constraints are derived by summing up all the old capacities that still exist in a certain period (according to the plant life). If the life of a technology expires within a period, MESSAGE takes the average production capacity in this period as installed capacity (this represents a linear interpolation between the starting points of this and the following period).

9.4 	Capacities which Operate  Longer than the Time  Horizon
-------------------------------------------------------------

If a capacity of a technology is built in one of the last periods its life time can exceed the calculation horizon. This fact is taken care of by reducing the investment costs by the following formula:

.. math:: 
 
τp −ν t+k−1 n 1 1 + drτ C r	 k=1     τ =t t  = Ct   ×  τp  k=1  t+k−1 n
τ =t 1 1 + drτ

where

:math:`ν`	          is the number of years the technology exists after the end of the calculation horizon,

:math:`drτ`        	is the discount rate for year :math:`τ`,

:math:`τp`         	is the plant life in years,

:math:`Ct`         	is the investment cost in year :math:`t`, and

:math:`t`          	is the reduced investment.


10.12 	The  Mixed Integer  Option
--------------------------------

If the LP-package  used to solve a problem formulated by MESSAGE has the capability to solve mixed integer problems, this can be used to improve the quality of the formulated problems, especially for applications to small regions.

The improvement consists in a definition of unit sizes for certain technologies that can only be built in large units. This avoids for instance the installation of a 10 kW nuclear reactor in the model of the energy system of a city or small region (it can only be built in units of e.g., 700 MW). Additionally  this option allows to take care of the ”economies of scale” of certain technologies.

This option is implemented for a technology by simply defining the unit size chosen for this technology (keyword cmix). The according capacity variable is then generated  as integer in the matrix, its value is the installation of one powerplant of unit size.

If a problem is formulated as mixed integer it can be applied without this option by changing just one switch in the general definition file (keyword mixsw). Then all capacity variables are generated  as real variables.
