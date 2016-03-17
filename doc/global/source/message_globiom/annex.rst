
Annex: Mathematical formulation
----
MESSAGE
~~~~

MACRO
~~~~

Notation declaration
 
The following short notation is used for indices in the mathematical description of the MACRO code.
===== ==================================================
Index Description
===== ==================================================
r     region index (11 MESSAGE regions)
y     year (2005, 2010, 2020, ..., 2100)
s     sector (six commercial energy sectors of MESSAGE)
===== ==================================================

==================== =====================================================================
Parameters           Description
==================== =====================================================================
NYPER_y              Number of years in a time period (forward diff)
TOTAL\_COST_{r,y} 
ENESTART_{r,s,y} 
ENEPRICE_{r,s,y} 
SPDA_r               Speed of adjustment
\rho_r               ESUB minus one divided by ESUB 
DEPR_r               Annual percent depreciation
KPVS_r               Capital value share parameter
LAKL_r               Production function coefficient of capital and labor
UDF_{r,y}            Utility discount factor in period year 
NEWLAB_{r,y}         New vintage of labor force in period year
SPEED_{r,y}          Period adjustment speed
LGROW_{r,y}          Annual growth rates of potential GDP
LBCORR_{r,s,y}       Correction factors for parameter b (PF)
AEEIFAC_{r,s,y}      Cumulative effect of autonomous energy efficiency improvement (AEEI)
LPRFCONST_{r,s}      production function coefficients of the different energy sectors
FINITE\_TIME\_CORR_{r,y}  finite time horizon correction factor in utility function
==================== =====================================================================

==================== =====================================================================
Variables            Description
==================== =====================================================================
:math:`K_{r,y}`              Capital stock in period year
:math:`KN_{r,y}`             New Capital vintage in period year
:math:`Y_{r,y}`              Production in period year
:math:`YN_{r,y}`             New production vintage in period year
:math:`PHYSENE_{r,s,y}`      Physical energy use
:math:`PRODENE_{r,s,y}`      Value of energy in the production function
:math:`NEWENE_{r,s,y}`       New energy (production function)
:math:`C_{r,y}`              Consumption (Trillion \$) 
:math:`I_{r,y}`              Investment (Trillion \$)
:math:`UTILITY`              Utility function (discounted log of consumption)
:math:`EC_{r,y}`             Energy costs (Trillion \$) based on MESSAGE input
==================== =====================================================================

Equations
:math:`\text{NEWCAP}_{RG, year+(1)}$}`

.. math:: \textcolor{black}{\text{KN}}_{RG, year+1} =  \frac{\textcolor{black}{\text{NYPER}}_{year} \cdot  (  { ( 1 - \textcolor{black}{\text{DEPR}}_{RG} ) }^{\textcolor{black}{\text{NYPER}}_{year}} \cdot \textcolor{black}{\text{I}}_{RG, year} + \textcolor{black}{\text{I}}_{RG, year+1} ) }{2} 


MACRO employs a nested CES (constant elasticity of substitution) production function with capital, labor and the six commercial energy services represented in MESSAGE as inputs.

.. math:: YN_{r,t} = \left( lakl * KN_{r,t}^{\rho_r * \kappa_r} *LN_{r,t}^{\rho_r * \left(1 - \kappa_r\right)} + \sum_s \left( lprf_{r,s} * lbcorr_{r,s,t} * EN_{r,s,t}^{\rho_r} \right) \right)^{1/\rho_r}
