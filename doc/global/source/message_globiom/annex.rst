
Annex: Mathematical formulation
----
MESSAGE
~~~~

MACRO
~~~~

Notation declaration
 
The following short notation is used for indices in the mathematical description of the MACRO code.

========== ==================================================
Index      Description
========== ==================================================
:math:`r`  region index (11 MESSAGE regions)
:math:`y`  year (2005, 2010, 2020, ..., 2100)
:math:`s`  sector (six commercial energy sectors of MESSAGE)
========== ==================================================

=========================== =====================================================================
Parameter                   Description
=========================== =====================================================================
:math:`NYPER_y`             Number of years in a time period (forward diff)
:math:`TOTAL\_COST_{r,y}` 
:math:`ENESTART_{r,s,y}` 
:math:`ENEPRICE_{r,s,y}` 
:math:`SPDA_r`              Speed of adjustment
:math:`\rho_r`              ESUB minus one divided by ESUB 
:math:`DEPR_r`              Annual percent depreciation
:math:`\kappa_r`              Capital value share parameter
:math:`LAKL_r`              Production function coefficient of capital and labor
:math:`UDF_{r,y}`           Utility discount factor in period year 
:math:`NEWLAB_{r,y}`        New vintage of labor force in period year
:math:`SPEED_{r,y}`         Period adjustment speed
:math:`LGROW_{r,y}`         Annual growth rates of potential GDP
:math:`LBCORR_{r,s,y}`      Correction factors for parameter b (PF)
:math:`AEEIFAC_{r,s,y}`     Cumulative effect of autonomous energy efficiency improvement (AEEI)
:math:`LPRFCONST_{r,s}`     production function coefficients of the different energy sectors
:math:`FINITE\_TIME_{r,y}`  finite time horizon correction factor in utility function
=========================== =====================================================================

======================== =====================================================================
Variable                 Description
======================== =====================================================================
:math:`K_{r,y}`          Capital stock in period year
:math:`KN_{r,y}`         New Capital vintage in period year
:math:`Y_{r,y}`          Production in period year
:math:`YN_{r,y}`         New production vintage in period year
:math:`PHYSENE_{r,s,y}`  Physical energy use
:math:`PRODENE_{r,s,y}`  Value of energy in the production function
:math:`NEWENE_{r,s,y}`   New energy (production function)
:math:`C_{r,y}`          Consumption (Trillion \$) 
:math:`I_{r,y}`          Investment (Trillion \$)
:math:`UTILITY`          Utility function (discounted log of consumption)
:math:`EC_{r,y}`         Energy costs (Trillion \$) based on MESSAGE input
======================== =====================================================================

Equations

:math:`{NEWCAP}_{r,y+1}`

.. math:: {KN}_{r,y+1} =  \frac{1}{2} \cdot {NYPER}_{y} \cdot \left(  { \left( 1 - {{DEPR}}_{r} \right) }^{{{NYPER}}_{y}} \cdot {{I}}_{r,y} + {{I}}_{r,y+1} \right) \qquad \forall{ r,y+1}

:math:`{NEWENEQ}_{r, s, y+1}`

.. math:: {{NEWENE}}_{r, s, y+1} = {{PRODENE}}_{r, s, y+1} - {{PRODENE}}_{r, s, y} \cdot {{SPEED}}_{r, y} \qquad \forall{ r, s, y+1} 

:math:`{SUPPLEQ}_{r, s, y+1}`

.. math:: {{PHYSENE}}_{r, s, y+1} \geq {{PRODENE}}_{r, s, y+1} \cdot {{AEEIFAC}}_{r, s, y+1} \qquad \forall{ r, s, y+1} 

:math:`{TOTALCAP}_{r, y+1}`

.. math:: {{K}}_{r, y+1} = {{K}}_{r, y} \cdot {{SPEED}}_{r, y} + {{KN}}_{r, y+1} \qquad \forall{ r, y+1} 

MACRO employs a nested CES (constant elasticity of substitution) production function with capital, labor and the six commercial energy services represented in MESSAGE as inputs.
:math:`{NEWPROD}_{r, y}`

.. math:: {{YN}}_{r, y} =  { ( {{LAKL}}_{r} \cdot  {{{KN}}_{r, y}}^{ ( {{\rho}}_{r} \cdot {{\kappa}}_{r} ) } \cdot  {{{NEWLAB}}_{r, y}}^{ ( {{\rho}}_{r} \cdot  ( 1 - {{\kappa}}_{r} )  ) } + \displaystyle \sum_{s} ( {{LPRFCONST}}_{r, s} \cdot {{LBCORR}}_{r, s, y} \cdot  {{{NEWENE}}_{r, s, y}}^{{{\rho}}_{r}} )  ) }^{ \frac{1}{{{\rho}}_{r}} } \qquad \forall{ r, y > 1}

.. math:: YN_{r,t} = \left( lakl * KN_{r,t}^{\rho_r * \kappa_r} *LN_{r,t}^{\rho_r * \left(1 - \kappa_r\right)} + \sum_s \left( lprf_{r,s} * lbcorr_{r,s,t} * EN_{r,s,t}^{\rho_r} \right) \right)^{1/\rho_r}

:math:`{TOTALPROD}_{r, y+1}`

.. math:: {{Y}}_{r, y+1} = {{Y}}_{r, y} \cdot {{SPEED}}_{r, y} + {{YN}}_{r, y+1} \qquad \forall{ r, y+1} 

:math:`{COSTNRG}_{r, y+1}`

.. math:: {{EC}}_{r, y+1} =  \frac{{{TOTAL\_COST}}_{y+1, r}}{1000}  + \displaystyle \sum_{s} ( {{ENEPRICE}}_{s, y+1, r} \cdot 0.001 \cdot  ( {{PHYSENE}}_{r, s, y+1} - {{ENESTART}}_{s, y+1, r} )  )  + \displaystyle \sum_{s} (  \frac{{{ENEPRICE}}_{s, y+1, r} \cdot 0.001}{{{ENESTART}}_{s, y+1, r}}  \cdot  ( {{PHYSENE}}_{r, s, y+1} - {{ENESTART}}_{s, y+1, r} )  \cdot  ( {{PHYSENE}}_{r, s, y+1} - {{ENESTART}}_{s, y+1, r} )  )  \qquad \forall{ r, y+1} 

:math:`{CC}_{r, y}`

.. math:: {{Y}}_{r, y} = {{C}}_{r, y} + {{I}}_{r, y} + {{EC}}_{r, y} \qquad \forall{ r, y} 

:math:`{TC}_{r, TLAST}`

.. math:: {{K}}_{r, TLAST} \cdot  ( {{LGROW}}_{r, TLAST} +  ( 1 - {{SPDA}}_{r} )  )  \leq {{I}}_{r, TLAST} \qquad \forall{ r, TLAST} 

:math:`{UTIL}`

.. math:: {{UTILITY}} = \displaystyle \sum_{r} ( 1000 \cdot  ( \displaystyle \sum_{y |  (  (  {ord}( y )   >  1 )  \wedge  (  {ord}( y )   <   | y |  )  )} (  \frac{{{UDF}}_{r, y} \cdot  ( {{NYPER}}_{y} + {{NYPER}}_{y-1} ) }{2}  \cdot {log} ( {{C}}_{r, y} )  )  + \displaystyle \sum_{y |  (  {ord}( y )   =   | y |  ) } ( {{UDF}}_{r, y} \cdot {log} ( {{C}}_{r, y} )  \cdot  (  \frac{{{NYPER}}_{y-1}}{2}  +  \frac{1}{{{FINITE\_TIME\_CORR}}_{r, y}}  )  )  )  ) 

:math:`{KN}_{r, y}\geq 0 ~ \forall r, y` , :math:`{I}_{r, y}\geq 0 ~ \forall r, y` , :math:`{NEWENE}_{r, s, y}\geq 0 ~ \forall r, s, y` , :math:`{PRODENE}_{r, s, y}\geq 0 ~ \forall r, s, y` , :math:`{PHYSENE}_{r, s, y}\geq 0 ~ \forall r, s, y` , :math:`{K}_{r, y}\geq 0 ~ \forall r, y` , :math:`{YN}_{r, y}\geq 0 ~ \forall r, y` , :math:`{Y}_{r, y}\geq 0 ~ \forall r, y` , :math:`{C}_{r, y}\geq 0 ~ \forall r, y` 


