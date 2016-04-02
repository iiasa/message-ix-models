MACRO
====

MACRO is :cite:`manne_buying_1992`

Notation declaration
----
 
The following short notation is used for indices in the mathematical description of the MACRO code.

========== ==================================================
Index      Description
========== ==================================================
:math:`r`  region index (11 MESSAGE regions)
:math:`y`  year (2005, 2010, 2020, ..., 2100)
:math:`s`  sector (six commercial energy sectors of MESSAGE)
========== ==================================================

Below is a listing of all parameters used in MACRO, together with a decription.

=========================== ================================================================================================================================
Parameter                   Description
=========================== ================================================================================================================================
:math:`NYPER_y`             Number of years in time period :math:`y` (forward diff)
:math:`TOTAL\_COST_{r,y}` 
:math:`ENESTART_{r,s,y}` 
:math:`ENEPRICE_{r,s,y}` 
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

The table below lists all variables in MACRO together with a definition and description.

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

Total capital stock (excluding energy sector) 
~~~~
:math:`{TOTALCAP}_{r, y}`

.. math:: {{K}}_{r, y} = {{K}}_{r, y-1} \cdot {{SPEED}}_{r, y-1} + {{KN}}_{r, y} \qquad \forall{ r, y > 1} 

New capital stock (excluding energy sector)
~~~~
:math:`{NEWCAP}_{r,y}`

.. math:: {KN}_{r,y} =  \frac{1}{2} \cdot {NYPER}_{y} \cdot \left(  { \left( 1 - {{DEPR}}_{r} \right) }^{{{NYPER}}_{y}} \cdot {{I}}_{r,y-1} + {{I}}_{r,y} \right) \qquad \forall{r, y > 1}

Depreciation of energy investments

:math:`{NEWENEQ}_{r, s, y}`

.. math:: {{NEWENE}}_{r, s, y} = {{PRODENE}}_{r, s, y} - {{PRODENE}}_{r, s, y-1} \cdot {{SPEED}}_{r, y-1} \qquad \forall{ r, s, y > 1} 

Link between physical energy as accounted in MESSAGE and energy in terms of monetary value as specified in the production function

:math:`{SUPPLEQ}_{r, s, y}`

.. math:: {{PHYSENE}}_{r, s, y} \geq {{PRODENE}}_{r, s, y} \cdot {{AEEIFAC}}_{r, s, y} \qquad \forall{ r, s, y > 1} 

MACRO employs a nested CES (constant elasticity of substitution) production function with capital, labor and the six commercial energy services 
represented in MESSAGE as inputs.

:math:`{NEWPROD}_{r, y}`

.. math:: {{YN}}_{r,y} =  { ( {{LAKL}}_{r} \cdot  {{{KN}}_{r, y}}^{ ( {{\rho}}_{r} \cdot {{\kappa}}_{r} ) } \cdot  {{{NEWLAB}}_{r, y}}^{ ( {{\rho}}_{r} \cdot  ( 1 - {{\kappa}}_{r} )  ) } + \displaystyle \sum_{s} ( {{LPRFCONST}}_{r, s} \cdot {{LBCORR}}_{r, s, y} \cdot {{{NEWENE}}_{r, s, y}}^{{{\rho}}_{r}} )  ) }^{ \frac{1}{{{\rho}}_{r}} } \qquad \forall{ r, y > 1}

Total production in the economy (excluding energy sector)

:math:`{TOTALPROD}_{r, y}`

.. math:: {{Y}}_{r, y} = {{Y}}_{r, y-1} \cdot {{SPEED}}_{r, y-1} + {{YN}}_{r, y} \qquad \forall{ r, y > 1} 

Approximation of energy system costs based on results of previous MESSAGE model run (Taylor expansion to second order)

:math:`{COSTNRG}_{r, y}`

.. math:: {{EC}}_{r, y} =  \frac{{{TOTAL\_COST}}_{y, r}}{1000}  + \displaystyle \sum_{s} \left( {{ENEPRICE}}_{s, y, r} \cdot 0.001 \cdot  ( {{PHYSENE}}_{r, s, y} - {{ENESTART}}_{s, y, r} )  )  + \displaystyle \sum_{s} (  \frac{{{ENEPRICE}}_{s, y, r} \cdot 0.001}{{{ENESTART}}_{s, y, r}}  \cdot  ( {{PHYSENE}}_{r, s, y} - {{ENESTART}}_{s, y, r} )^2  \right)  \qquad \forall{ r, y > 1} 

The following equation specifies the allocation of total production among current consumption, investment into building up capital stock and energy system costs which are derived from 
a previous MESSAGE model run.

:math:`{CC}_{r, y}`

.. math:: {{Y}}_{r, y} = {{C}}_{r, y} + {{I}}_{r, y} + {{EC}}_{r, y} \qquad \forall{ r, y} 

Finite time horizon correction

:math:`{TC}_{r, y}`

.. math:: {{K}}_{r, y} \cdot  ( {{LGROW}}_{r, y} +  ( 1 - {{SPDA}}_{r} )  )  \leq {{I}}_{r, y} \qquad \forall{ r, y = last year} 

Utility function to be maximized.

:math:`{UTIL}`

.. math:: {{UTILITY}} = \displaystyle \sum_{r} ( 1000 \cdot  ( \displaystyle \sum_{y |  (  (  {ord}( y )   >  1 )  \wedge  (  {ord}( y )   <   | y |  )  )} (  \frac{{{UDF}}_{r, y} \cdot  ( {{NYPER}}_{y} + {{NYPER}}_{y-1} ) }{2}  \cdot {log} ( {{C}}_{r, y} )  )  + \displaystyle \sum_{y |  (  {ord}( y )   =   | y |  ) } ( {{UDF}}_{r, y} \cdot {log} ( {{C}}_{r, y} )  \cdot  (  \frac{{{NYPER}}_{y-1}}{2}  +  \frac{1}{{{FINITE\_TIME\_CORR}}_{r, y}}  )  )  )  ) 
The utility discount rate for period :math:`y` is set to :math:`DRATE_{r} - LGROW_{r,y}`, where :math:`DRATE_{r}` is the discount rate used in MESSAGE, typically set to 5%, 
and :math:`LGROW` is the potential GDP growth rate. The utility discount rates are chosen for descriptive rather than normative reasons.




