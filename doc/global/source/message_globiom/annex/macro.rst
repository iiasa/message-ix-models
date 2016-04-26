MACRO
====

MACRO is based on the macro-economic module of the global energy-economy-climate model Global 2100 :cite:`manne_buying_1992`, a predecesor of the MERGE model :cite:. 
The original soft-linkage between MACRO and MESSAGE has been described in :cite:`messner_messagemacro:_2000`, but several adjustments have been made compared to this 
original implementation. The description below builds to a certain degree on these two publications, but deviates in certain places as discussed in the following paragraphs.

On the one hand, while the version of MACRO described in :cite:`messner_messagemacro:_2000` like the MACRO module of Global 2100 operated
at the level of electric and non-electric energy demands in the production function, the present version of MACRO operates at the level of the six commercial useful 
energy demands represented in MESSAGE (link). This change was made in response to electrification becoming a tangible option for the transport sector with the introduction 
of electric cars over the past decade. Previsouly (and as described in :cite:`messner_messagemacro:_2000`), the electric useful energy demands in MESSAGE had been mapped 
to electric demand in MACRO and the thermal useful energy demands, non-energy feedstock and transport had been mapped to non-electric demand in MACRO. 

On the other hand, the interface between MACRO and MESSAGE that organizes the iterative information exchange between the two models has been re-implemented in the 
scripting language R which makes code maintenance and visualization of results (e.g., for visually checking convergence between MACRO and MESSAGE) easier compared to
the previous implementation in C.

Finally, the parameterization of MACRO has changed in a specific way. As mentioned, the model’s most important input parameters are the projected growth rates of total labor, i.e., 
the combined effect of labor force and labor productivity growth (Note that labor supply growth is also referred to as reference or potential GDP growth.) and the annual rates 
of reference energy intensity improvements. In all recent applications of MACRO, these are calibrated to be consistent with the developments in a MESSAGE scenario. In practice, 
this happens by running MACRO and then adjusting the potential GDP growth rates and the autonomous energy efficiency improvements (AEEIs) on a sectoral basis until MACRO does not 
produce an energy demand response and GDP feedback compared to the MESSAGE scenario that it is calibrated to.

Notation declaration
----
 
The following short notation is used for the three indices, i.e. regions, years and sectors, in the mathematical description of the MACRO code.

========== ==================================================
Index      Description
========== ==================================================
:math:`r`  region index (11 MESSAGE regions)
:math:`y`  year (2005, 2010, 2020, ..., 2100)
:math:`s`  sector (six commercial energy demands of MESSAGE)
========== ==================================================

A listing of all parameters used in MACRO together with a decription can be found in the table below.

=========================== ================================================================================================================================
Parameter                   Description
=========================== ================================================================================================================================
:math:`NYPER_y`             Number of years in time period :math:`y` (forward diff)
:math:`TOTAL\_COST_{r,y}`   Total energy system costs in region :math:`r` and period :math:`y` from MESSAGE model run
:math:`ENESTART_{r,s,y}`    Consumption level of six commercial energy services :math:`s` in region :math:`r` and period :math:`y` from MESSAGE model run 
:math:`ENEPRICE_{r,s,y}`    Shadow prices of six commercial energy services :math:`s` in region :math:`r` and period :math:`y` from MESSAGE model run 
:math:`SPDA_r`              Speed of adjustment in region :math:`r`
:math:`\epsilon_r`          Elasticity of substitution between capital-labor and total energy in region :math:`r`
:math:`\rho_r`              :math:`\epsilon - 1 / \epsilon` where :math:`\epsilon` is the elasticity of subsitution in region :math:`r`
:math:`DEPR_r`              Annual percent depreciation in region :math:`r`
:math:`\kappa_r`            Capital value share parameter in region :math:`r`
:math:`LAKL_r`              Production function coefficient of capital and labor in region :math:`r`
:math:`UDF_{r,y}`           Utility discount factor in period year in region :math:`r` and period :math:`y`
:math:`NEWLAB_{r,y}`        New vintage of labor force in region :math:`r` and period :math:`y`
:math:`SPEED_{r,y}`         Adjustment speed in region :math:`r` and period :math:`y`
:math:`LGROW_{r,y}`         Annual growth rates of potential GDP in region :math:`r` and period :math:`y`
:math:`LBCORR_{r,s,y}`      Correction factors for parameter b (PF) in region :math:`r` and period :math:`y`
:math:`AEEIFAC_{r,s,y}`     Cumulative effect of autonomous energy efficiency improvement (AEEI) in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`LPRFCONST_{r,s}`     production function coefficients of the different energy sectors in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`FINITE\_TIME_{r,y}`  finite time horizon correction factor in utility function in region :math:`r` and period :math:`y`
=========================== ================================================================================================================================

The table below lists all variables in MACRO together with a definition and brief description.

======================== ==================================================== ======================================================================================================
Variable                 Definition                                           Description
======================== ==================================================== ======================================================================================================
:math:`K_{r,y}`          :math:`{K}_{r, y}\geq 0 ~ \forall r, y`              Capital stock in region :math:`r` and period :math:`y`
:math:`KN_{r,y}`         :math:`{KN}_{r, y}\geq 0 ~ \forall r, y`             New Capital vintage in region :math:`r` and period :math:`y`
:math:`Y_{r,y}`          :math:`{Y}_{r, y}\geq 0 ~ \forall r, y`              Production in region :math:`r` and period :math:`y`
:math:`YN_{r,y}`         :math:`{YN}_{r, y}\geq 0 ~ \forall r, y`             New production vintage in region :math:`r` and period :math:`y`
:math:`PHYSENE_{r,s,y}`  :math:`{PHYSENE}_{r, s, y}\geq 0 ~ \forall r, s, y`  Physical energy use in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`PRODENE_{r,s,y}`  :math:`{PRODENE}_{r, s, y}\geq 0 ~ \forall r, s, y`  Value of energy in the production function in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`NEWENE_{r,s,y}`   :math:`{NEWENE}_{r, s, y}\geq 0 ~ \forall r, s, y`   New energy in the production function in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`C_{r,y}`          :math:`{C}_{r, y}\geq 0 ~ \forall r, y`              Consumption in region :math:`r` and period :math:`y`  
:math:`I_{r,y}`          :math:`{I}_{r, y}\geq 0 ~ \forall r, y`              Investment in region :math:`r` and period :math:`y`
:math:`EC_{r,y}`         :math:`EC \in \left[-\infty..\infty\right]`          Approximation of energy costs based on MESSAGE results
:math:`UTILITY`          :math:`UTILITY \in \left[-\infty..\infty\right]`     Utility function (discounted log of consumption)
======================== ==================================================== ======================================================================================================

Equations
----

Utility function
~~~~
The utility function :math:`{UTIL}`, which is maximized, sums up the discounted logarithm of consumption of a single representative producer-consumer over the entire time horizon
of the model.

:math:`{UTIL}`

.. math:: {{UTILITY}} = \displaystyle \sum_{r} ( \displaystyle \sum_{y |  (  (  {ord}( y )   >  1 )  \wedge  (  {ord}( y )   <   | y |  )  )} (  \frac{{{UDF}}_{r, y} \cdot  ( {{NYPER}}_{y} + {{NYPER}}_{y-1} ) }{2}  \cdot {log} ( {{C}}_{r, y} )  )  + \displaystyle \sum_{y |  (  {ord}( y )   =   | y |  ) } ( {{UDF}}_{r, y} \cdot {log} ( {{C}}_{r, y} )  \cdot  (  \frac{{{NYPER}}_{y-1}}{2}  +  \frac{1}{{{FINITE\_TIME\_CORR}}_{r, y}}  )  )  ) 

The utility discount rate for period :math:`y` is set to :math:`DRATE_{r} - LGROW_{r,y}`, where :math:`DRATE_{r}` is the discount rate used in MESSAGE, typically set to 5%, 
and :math:`LGROW` is the potential GDP growth rate. This choice ensures that in the steady state, the optimal growth rate is identical to the potential GDP growth rates :math:`LGROW`. 
The values for the utility discount rates are chosen for descriptive rather than normative reasons. The term :math:`\frac{{NYPER}_{y} + {NYPER}_{y-1}}{2}` mutliples the 
discounted logarithm of consumption with the period length. The final period is treated separately to include a correction factor :math`\frac{1}{{FINITE\_TIME\_CORR}_{r, y}}` reflecting 
the finite time horizon of the model.

Allocation of total production
~~~~
The following equation :math:`{CC}_{r, y}` specifies the allocation of total production among current consumption :math:`{C}_{r, y}`, investment into building up capital stock excluding 
energy sectors :math:`{I}_{r, y}` and energy system costs :math:`{EC}_{r, y}` which are derived from a previous MESSAGE model run. As described in :cite:`manne_buying_1992`, the first-order 
optimality conditions lead to the Ramsey rule for the optimal allocation of savings, investment and consumption over time.


:math:`{CC}_{r, y}`

.. math:: {{Y}}_{r, y} = {{C}}_{r, y} + {{I}}_{r, y} + {{EC}}_{r, y} \qquad \forall{ r, y} 

New capital stock
~~~~
The accumulation of capital in the non-energy sectors is governed by new capital stock equation :math:`{NEWCAP}_{r,y}`. Net capital formation :math:`{KN}_{r,y}` is derived 
from gross investments :math:`{I}_{r,y}` minus depreciation of previsouly existing capital stock. 

:math:`{NEWCAP}_{r,y}`

.. math:: {KN}_{r,y} =  \frac{1}{2} \cdot {NYPER}_{y} \cdot \left(  { \left( 1 - {{DEPR}}_{r} \right) }^{{{NYPER}}_{y}} \cdot {{I}}_{r,y-1} + {{I}}_{r,y} \right) \qquad \forall{r, y > 1}

Here, the initial boundary condition for the base year (:math:`y_0 = 2005`) implies for the investments that :math:`I_{r,y_0} = (LGROW_{r,y_0} + DEPR_{r}) \cdot kgdp \cdot GDP_{y_0}`.

Production function
~~~~
MACRO employs a nested CES (constant elasticity of substitution) production function with capital, labor and the six commercial energy services 
represented in MESSAGE as inputs.

:math:`{NEWPROD}_{r, y}`

.. math:: {{YN}}_{r,y} =  { ( {{LAKL}}_{r} \cdot  {{{KN}}_{r, y}}^{ ( {{\rho}}_{r} \cdot {{\kappa}}_{r} ) } \cdot  {{{NEWLAB}}_{r, y}}^{ ( {{\rho}}_{r} \cdot  ( 1 - {{\kappa}}_{r} )  ) } + \displaystyle \sum_{s} ( {{LPRFCONST}}_{r, s} \cdot {{LBCORR}}_{r, s, y} \cdot {{{NEWENE}}_{r, s, y}}^{{{\rho}}_{r}} )  ) }^{ \frac{1}{{{\rho}}_{r}} } \qquad \forall{ r, y > 1}

Total production
~~~~
Total production in the economy excluding energy sectors is the sum of production from all assets where assets that were already exisitng in the previous period :math:`y-1` 
are depreciated with the depreciation rate :math:`DEPR_{r}`.
:math:`{TOTALPROD}_{r, y}`

.. math:: {{Y}}_{r, y} = {{Y}}_{r, y-1} \cdot {{SPEED}}_{r, y-1} + {{YN}}_{r, y} \qquad \forall{ r, y > 1} 

Total capital stock 
~~~~
Equivalent to the total production equation above, the total capital stock, again excluding the energy sectors which are modeled in MESSAGE, is then simply a summation of 

:math:`{TOTALCAP}_{r, y}`

.. math:: {{K}}_{r, y} = {{K}}_{r, y-1} \cdot {{SPEED}}_{r, y-1} + {{KN}}_{r, y} \qquad \forall{ r, y > 1} 

Depreciation of energy investments
~~~~
:math:`{NEWENEQ}_{r, s, y}`

.. math:: {{NEWENE}}_{r, s, y} = {{PRODENE}}_{r, s, y} - {{PRODENE}}_{r, s, y-1} \cdot {{SPEED}}_{r, y-1} \qquad \forall{ r, s, y > 1} 

Physical energy
~~~~
Link between physical energy as accounted in MESSAGE and energy in terms of monetary value as specified in the production function
:math:`{SUPPLEQ}_{r, s, y}`

.. math:: {{PHYSENE}}_{r, s, y} \geq {{PRODENE}}_{r, s, y} \cdot {{AEEIFAC}}_{r, s, y} \qquad \forall{ r, s, y > 1} 

Energy system costs
~~~~
Approximation of energy system costs based on results of previous MESSAGE model run (Taylor expansion to second order)

:math:`{COSTNRG}_{r, y}`

.. math:: {{EC}}_{r, y} =  {TOTAL\_COST}_{y, r}  + \displaystyle \sum_{s} \left( {{ENEPRICE}}_{s, y, r} \cdot  ( {{PHYSENE}}_{r, s, y} - {{ENESTART}}_{s, y, r} )  )  + \displaystyle \sum_{s} (  \frac{{{ENEPRICE}}_{s, y, r}}{{{ENESTART}}_{s, y, r}}  \cdot  ( {{PHYSENE}}_{r, s, y} - {{ENESTART}}_{s, y, r} )^2  \right)  \qquad \forall{ r, y > 1} 

Finite time horizon correction
~~~~
:math:`{TC}_{r, y}`

.. math:: {{K}}_{r, y} \cdot  ( {{LGROW}}_{r, y} +  ( 1 - {{SPDA}}_{r} )  )  \leq {{I}}_{r, y} \qquad \forall{ r, y = last year} 





