.. _specialfeatures:

8 Special Features of the Matrix Generator
===========================================

The mathematical formulation of MESSAGE as presented  in the previous sections shows the structure of all constraints as the matrix generator builds them up. The background of the more complicated features is given here for a better understanding.

8.1 	Discounting of Costs
----------------------------------------------

The whole time horizon of the calculations is divided into periods of optional length. All variables of MESSAGE are represented  as average over the period they represent, resulting in a step-function. All entries in the objective function are discounted from the middle of the respective period to the first year, if they relate to energy flow variables and from the beginning of that period if they represent power variables. The function to discount the costs has the following form:
 
.. math::
   c_t=\frac{C_t^r}{\prod_{k=1}^{t-1}(1+\frac{dr_k}{100})^{\Delta k}\times f_i}

where

.. list-table:: 
   :widths: 35 65
   :header-rows: 0

   * - :math:`C_t^r`
     - cost figure to be discounted,
   * - :math:`c_t` 
     - objective function coefficient in period :math:`t`,
   * - :math:`f_i`
     - cost factor (see below), and
   * - :math:`dr_t`
     - discount rate in period :math:`t`; generally the discount rate is constant over the complete time horizon.                 
.. math::
  f_i = \left\{\begin{array}{ll}
  1  &\mbox{for costs connected to investments} \\
  (1+\frac{dr_t}{100})^{\frac{\Delta t}{2}}  &\mbox{else}
  \end{array}\right.
  
.. _distributionsofinv:

8.2 	Distributions of Investments
-----------------------------------

Investment costs can be distributed over the construction time. As these points in time are closer to the beginning of the time horizon, investments become more expensive, this represents interest during construction. MESSAGE allows for two options:

.. list-table:: 
   :widths: 35 65
   :header-rows: 0

   * - shifted
     - all costs are paid in the time period(s) prevoius to the start of operation. This is usually used for models with short period lengths,
   * - half\-half
     - half of the investments are paid in the period before the start of operation, the other half is paid in the period when the technology goes into operation. With this, the period when the technology starts operating is the same as the construction period. This is usually used for models with long time periods.

Investment costs are spread evenly over the construction time. In reality the investment costs follow a bell-shape, but the resulting error after discounting and summing up over the construction time is very small. There still remains only one entry into the objective function, which is modified according to the sum over the distribution results.

8.3 	The Contribution of Capacities Existing in the Base Year
---------------------------------------------------------------

The possible contribution of an installation that exists in the base year is kept track of over time. There are two possibilities to give the necessary information to MESSAGE.

1. Define the capacities that were built in the years :math:`iyr, ..., iyr −\tau + 1`, with :math:`iyr` = base year and :math:`τ` = plant life in years explicitly. These capacities are then distributed to historic periods of the length :math:`\nu`.

2. Define the total capacity, :math:`c_0`, that exists in :math:`iyr` and the rate at that it grew in the last :math:`\tau` years, :math:`\gamma`. This information is then converted to one similar to 1. by using the function:

.. math:: 
   y_0=c_0\frac{\gamma^{-\nu}-1}{\nu(\gamma^{-\tau}-1)},
   y_t=y_0\gamma^{-t\times\nu}, t=1(1)\frac{\tau}{\nu}

where

.. list-table:: 
   :widths: 35 65
   :header-rows: 0

   * - :math:`y_t`
     - is the annual construction in period :math:`−t`, (0 = base year),
   * - :math:`\gamma`
     - is the annual growth of new installations before the base year,
   * - :math:`c_0`
     - is the total capacity in the base year,
   * - :math:`\tau`
     - is the plant life, and
   * - :math:`\nu`
     - is the length of the periods in that the time before the base year is divided.

The right hand sides in the capacity constraints are derived by summing up all the old capacities that still exist in a certain period (according to the plant life). If the life of a technology expires within a period, MESSAGE takes the average production capacity in this period as installed capacity (this represents a linear interpolation between the starting points of this and the following period).

In case of formulation 2. one has to consider that some of the capacity goes out of operation between the base year and the first year.

8.4 	Capacities which Operate  Longer than the Time  Horizon
-------------------------------------------------------------

If a capacity of a technology is built in one of the last periods its life time can exceed the calculation horizon. This fact is taken care of by reducing the investment costs by the following formula:

.. math:: 
   C_t^r=C_t\times\frac{\sum_{k=1}^{\tau_p-\nu}\prod_{\tau=t}^{t+k-1}\frac{1}{1+dr_\tau}}{\sum_{k=1}^{\tau_p}\prod_{\tau=t}^{t+k-1}\frac{1}{1+dr_\tau}}
   
where

.. list-table:: 
   :widths: 35 65
   :header-rows: 0

   * - :math:`\nu`
     - is the number of years the technology exists after the end of the calculation horizon,
   * - :math:`dr_{\tau}`
     - is the discount rate for year :math:`\tau`,
   * - :math:`\tau_p`
     - is the plant life in years,
   * - :math:`C_t`
     - is the investment cost in year :math:`t`, and
   * - :math:`C_t^r`
     - is the reduced investment.

8.5 	The  Mixed Integer  Option
--------------------------------

If the LP-package  used to solve a problem formulated by MESSAGE has the capability to solve mixed integer problems, this can be used to improve the quality of the formulated problems, especially for applications to small regions.

The improvement consists in a definition of unit sizes for certain technologies that can only be built in large units. This avoids for instance the installation of a 10 kW nuclear reactor in the model of the energy system of a city or small region (it can only be built in units of e.g., 700 MW). Additionally  this option allows to take care of the ”economies of scale” of certain technologies.

This option is implemented for a technology by simply defining the unit size for this technology (keyword cmix). The according capacity variable is then generated  as integer in the matrix, its value is the installation of one powerplant of unit size.

If a problem is formulated as mixed integer it can be applied without this option by changing just one switch in the general definition file (keyword mixsw). Then all capacity variables are generated  as real variables.
