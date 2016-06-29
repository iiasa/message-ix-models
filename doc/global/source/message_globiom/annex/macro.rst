.. _annex_macro:

Mathematical Formulation: MACRO
====

MACRO is based on the macro-economic module of the global energy-economy-climate model Global 2100 :cite:`manne_buying_1992`, a predecesor of the `MERGE <http://www.stanford.edu/group/MERGE/>`_ model. 
The original soft-linkage between MACRO and MESSAGE has been described in :cite:`messner_messagemacro:_2000`, but several adjustments have been made compared to this 
original implementation. The description below builds to a certain degree on these two publications, but deviates in certain places as discussed in the following paragraphs.
It is worthwhile mentioning that MACRO as used with MESSAGE has similar origins as the MACRO module of MARKAL-MACRO :cite:`loulou_markal-macro_2004` with the exception of 
being soft-linked rather than hard-linked to the energy systems part of the model.

On the one hand, while the version of MACRO described in :cite:`messner_messagemacro:_2000` like the MACRO module of Global 2100 operated
at the level of electric and non-electric energy demands in the production function, the present version of MACRO operates at the level of the six commercial useful 
energy demands represented in MESSAGE (:ref:`message`). This change was made in response to electrification becoming a tangible option for the transport sector with the introduction 
of electric cars over the past decade. Previsouly (and as described in :cite:`messner_messagemacro:_2000`), the electric useful energy demands in MESSAGE had been mapped 
to electric demand in MACRO and the thermal useful energy demands, non-energy feedstock and transport had been mapped to non-electric demand in MACRO. 

On the other hand, the interface between MACRO and MESSAGE that organizes the iterative information exchange between the two models has been re-implemented in the 
scripting language R which makes code maintenance and visualization of results (e.g., for visually checking demand convergence between MACRO and MESSAGE) easier compared to
the previous implementation in C).

Finally, the parameterization of MACRO has changed in a specific way. As mentioned, the model’s most important input parameters are the projected growth rates of total labor, i.e., 
the combined effect of labor force and labor productivity growth (note that labor supply growth is also referred to as reference or potential GDP growth.) and the annual rates 
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
:math:`duration\_period_y`  Number of years in time period :math:`y` (forward diff)
:math:`total\_cost_{r,y}`   Total energy system costs in region :math:`r` and period :math:`y` from MESSAGE model run
:math:`enestart_{r,s,y}`    Consumption level of six commercial energy services :math:`s` in region :math:`r` and period :math:`y` from MESSAGE model run 
:math:`eneprice_{r,s,y}`    Shadow prices of six commercial energy services :math:`s` in region :math:`r` and period :math:`y` from MESSAGE model run 
:math:`\epsilon_r`          Elasticity of substitution between capital-labor and total energy in region :math:`r`
:math:`\rho_r`              :math:`\epsilon - 1 / \epsilon` where :math:`\epsilon` is the elasticity of subsitution in region :math:`r`
:math:`depr_r`              Annual depreciation rate in region :math:`r`
:math:`\alpha_r`            Capital value share parameter in region :math:`r`
:math:`a_r`                 Production function coefficient of capital and labor in region :math:`r`
:math:`b_{r,s}`             Production function coefficients of the different energy sectors in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`udf_{r,y}`           Utility discount factor in period year in region :math:`r` and period :math:`y`
:math:`newlab_{r,y}`        New vintage of labor force in region :math:`r` and period :math:`y`
:math:`grow_{r,y}`          Annual growth rates of potential GDP in region :math:`r` and period :math:`y`
:math:`aeei_{r,s,y}`        Autonomous energy efficiency improvement (AEEI) in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`fin\_time_{r,y}`     finite time horizon correction factor in utility function in region :math:`r` and period :math:`y`
=========================== ================================================================================================================================

The table below lists all variables in MACRO together with a definition and brief description.

======================== ==================================================== ======================================================================================================
Variable                 Definition                                           Description
======================== ==================================================== ======================================================================================================
:math:`K_{r,y}`          :math:`{K}_{r, y}\geq 0 ~ \forall r, y`              Capital stock in region :math:`r` and period :math:`y`
:math:`KN_{r,y}`         :math:`{KN}_{r, y}\geq 0 ~ \forall r, y`             New Capital vintage in region :math:`r` and period :math:`y`
:math:`Y_{r,y}`          :math:`{Y}_{r, y}\geq 0 ~ \forall r, y`              Total production in region :math:`r` and period :math:`y`
:math:`YN_{r,y}`         :math:`{YN}_{r, y}\geq 0 ~ \forall r, y`             New production vintage in region :math:`r` and period :math:`y`
:math:`C_{r,y}`          :math:`{C}_{r, y}\geq 0 ~ \forall r, y`              Consumption in region :math:`r` and period :math:`y`  
:math:`I_{r,y}`          :math:`{I}_{r, y}\geq 0 ~ \forall r, y`              Investment in region :math:`r` and period :math:`y`
:math:`PHYSENE_{r,s,y}`  :math:`{PHYSENE}_{r, s, y}\geq 0 ~ \forall r, s, y`  Physical energy use in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`PRODENE_{r,s,y}`  :math:`{PRODENE}_{r, s, y}\geq 0 ~ \forall r, s, y`  Value of energy in the production function in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`NEWENE_{r,s,y}`   :math:`{NEWENE}_{r, s, y}\geq 0 ~ \forall r, s, y`   New energy in the production function in region :math:`r`, sector :math:`s` and period :math:`y`
:math:`EC_{r,y}`         :math:`EC \in \left[-\infty..\infty\right]`          Approximation of energy costs based on MESSAGE results
:math:`UTILITY`          :math:`UTILITY \in \left[-\infty..\infty\right]`     Utility function (discounted log of consumption)
======================== ==================================================== ======================================================================================================

Equations
----

Utility function
~~~~
The utility function which is maximized sums up the discounted logarithm of consumption of a single representative producer-consumer over the entire time horizon
of the model.

.. equation {UTILITY_FUNCTION}

.. math:: {UTILITY} = \displaystyle \sum_{r} \left( \displaystyle \sum_{y |  (  (  {ord}( y )   >  1 )  \wedge  (  {ord}( y )   <   | y |  )  )} {udf}_{r, y} \cdot {log}( C_{r, y} ) \cdot \frac{{duration\_period}_{y} + {duration\_period}_{y-1}}{2} \right. \\ 
	\left. + \displaystyle \sum_{y |  (  {ord}( y )   =   | y |  ) } {udf}_{r, y} \cdot {log} ( C_{r, y} ) \cdot \left( \frac{{duration\_period}_{y-1}}{2} + \frac{1}{{fin\_time}_{r, y}} \right) \right) 

The utility discount rate for period :math:`y` is set to :math:`DRATE_{r} - grow_{r,y}`, where :math:`DRATE_{r}` is the discount rate used in MESSAGE, typically set to 5%, 
and :math:`grow` is the potential GDP growth rate. This choice ensures that in the steady state, the optimal growth rate is identical to the potential GDP growth rates :math:`grow`. 
The values for the utility discount rates are chosen for descriptive rather than normative reasons. The term :math:`\frac{{duration\_period}_{y} + {duration\_period}_{y-1}}{2}` mutliples the 
discounted logarithm of consumption with the period length. The final period is treated separately to include a correction factor :math:`\frac{1}{{fin\_time}_{r, y}}` reflecting 
the finite time horizon of the model.

Allocation of total production
~~~~
The following equation specifies the allocation of total production among current consumption :math:`{C}_{r, y}`, investment into building up capital stock excluding 
energy sectors :math:`{I}_{r, y}` and energy system costs :math:`{EC}_{r, y}` which are derived from a previous MESSAGE model run. As described in :cite:`manne_buying_1992`, the first-order 
optimality conditions lead to the Ramsey rule for the optimal allocation of savings, investment and consumption over time.

.. equation {CAPITAL_CONSTRAINT}_{r, y}

.. math:: Y_{r, y} = C_{r, y} + I_{r, y} + {EC}_{r, y} \qquad \forall{ r, y} 

New capital stock
~~~~
The accumulation of capital in the non-energy sectors is governed by new capital stock equation. Net capital formation :math:`{KN}_{r,y}` is derived from gross 
investments :math:`{I}_{r,y}` minus depreciation of previsouly existing capital stock.

.. equation {NEW_CAPITAL}_{r,y}

.. math:: {KN}_{r,y} =  \frac{1}{2} \cdot {duration\_period}_{y} \cdot \left(  { \left( 1 - {depr}_r \right) }^{duration\_period_{y}} \cdot I_{r,y-1} + I_{r,y} \right) \qquad \forall{r, y > 1}

Here, the initial boundary condition for the base year (:math:`y_0 = 2005`) implies for the investments that :math:`I_{r,y_0} = (grow_{r,y_0} + depr_{r}) \cdot kgdp \cdot GDP_{y_0}`.

Production function
~~~~
MACRO employs a nested CES (constant elasticity of substitution) production function with capital, labor and the six commercial energy services 
represented in MESSAGE as inputs.

.. equation {NEW_PRODUCTION}_{r, y}

.. math:: {YN}_{r,y} =  { \left( {a}_{r} \cdot {{KN}_{r, y}}^{ ( {\rho}_{r} \cdot {\alpha}_{r} ) } \cdot {{newlab}_{r, y}}^{ ( {\rho}_{r} \cdot ( 1 - {\alpha}_{r} )  ) } + \displaystyle \sum_{s} ( {b}_{r, s} \cdot {{NEWENE}_{r, s, y}}^{{\rho}_{r}} )  \right) }^{ \frac{1}{{\rho}_{r}} } \qquad \forall{ r, y > 1}

Total production
~~~~
Total production in the economy (excluding energy sectors) is the sum of production from all assets where assets that were already existing in the previous period :math:`y-1` 
are depreciated with the depreciation rate :math:`depr_{r}`.

.. equation {TOTAL_PRODUCTION}_{r, y}

.. math:: Y_{r, y} = Y_{r, y-1} \cdot { \left( 1 - {depr}_r \right) }^{duration\_period_{y-1}} + {YN}_{r, y} \qquad \forall{ r, y > 1} 

Total capital stock 
~~~~
Equivalent to the total production equation above, the total capital stock, again excluding the energy sectors which are modeled in MESSAGE, is then simply a summation 
of capital stock in the previous period :math:`y-1`, depreciated with the depreciation rate :math:`depr_{r}`, and the capital stock added in the current period :math:`y`.

.. equation {TOTAL_CAPITAL}_{r, y}

.. math:: K_{r, y} = K_{r, y-1} \cdot { \left( 1 - {depr}_r \right) }^{duration\_period_{y-1}} + {KN}_{r, y} \qquad \forall{ r, y > 1} 

New vintage of energy production
~~~~
The new vintage of energy production of the six commerical energy demands :math:`s` derive from total production in period :math:`y` minus the total energy production in the previous 
period :math:`y-1` after depreciation.

.. equation {NEW_ENERGY}_{r, s, y}

.. math:: {NEWENE}_{r, s, y} = {PRODENE}_{r, s, y} - {PRODENE}_{r, s, y-1} \cdot { \left( 1 - {depr}_r \right) }^{duration\_period_{y-1}} \qquad \forall{ r, s, y > 1} 

Physical energy
~~~~
The relationship below establishes the link between physical energy :math:`{PHYSENE}_{r, s, y}` as accounted in MESSAGE for the six commerical energy demands :math:`s` and 
energy in terms of monetary value :math:`{PRODENE}_{r, s, y}` as specified in the production function of MACRO.  

.. equation {ENERGY_SUPPLY}_{r, s, y}

.. math:: {PHYSENE}_{r, s, y} \geq {PRODENE}_{r, s, y} \cdot {aeei\_factor}_{r, s, y} \qquad \forall{ r, s, y > 1} 

The cumulative effect of autonomous energy efficiency improvements (AEEI) is captured in :math:`{aeei\_factor}_{r,s,y} = {aeei\_factor}_{r, s, y-1} \cdot (1 - {aeei}_{r,s,y})^{duration\_period}_{y}` 
with :math:`{aeei\_factor}_{r,s,y=1} = 1`. Therefore, choosing the :math:`{aeei}_{r,s,y}` coefficients appropriately offers the possibility to calibrate MACRO to a certain energy demand trajectory 
from MESSAGE.

Energy system costs
~~~~
Energy system costs are based on a previous MESSAGE model run. The approximation of energy system costs in vicinity of the MESSAGE solution are approximated by a Taylor expansion with the 
first order term using shadow prices :math:`eneprice_{s, y, r}` of the MESSAGE model's solution and a quadratic second-order term.

.. equation {COST_ENERGY}_{r, y}

.. math:: {EC}_{r, y} = {total\_cost}_{y, r} + \displaystyle \sum_{s} {eneprice}_{s, y, r} \cdot \left( {PHYSENE}_{r, s, y} - {enestart}_{s, y, r} \right) \\
	+ \displaystyle \sum_{s} \frac{{eneprice}_{s, y, r}}{{enestart}_{s, y, r}} \cdot \left( {PHYSENE}_{r, s, y} - {enestart}_{s, y, r} \right)^2 \qquad \forall{ r, y > 1} 

Finite time horizon correction
~~~~
Given the finite time horizon of MACRO, a terminal constraint needs to be applied to ensure that investments are chosen at an appropriate level, i.e. to replace depriciated capital and
provide net growth of capital stock beyond MACRO's time horizon :cite:`manne_buying_1992`. The goal is to avoid to the extend possible model artifacts resulting from this finite time horizon 
cutoff.

.. equation {TERMINAL_CONSTRAINT}_{r, y}

.. math:: K_{r, y} \cdot  \left( grow_{r, y} + depr_r \right) \leq I_{r, y} \qquad \forall{ r, y = last year} 

MACRO parameterization
----

Initial conditions
~~~~
Total capital :math:`K_{r, y=0}` in the base year is derived by multiplying base year GDP with the capital-to-GDP ratio :math:`kgdp`.

.. math:: K_{y=0, r} = kgdp \cdot GDP_{r, y=0} 

Similarly investments :math:`I_{r, y=0}` and consumpiton :math:`C_{r, y=0}` in the base year are derived from base year GDP, capital value share and depriciation rate. 

.. math:: I_{y=0, r} = K_{y=0, r} \cdot (grow_{r, y=0} + depr_r)
.. math:: C_{y=0, r} =  GDP_{r, y=0}  - I_{y=0, r}

Total production :math:`Y_{y=0, r}` in the base year then follows as total GDP plus energy system costs (estimation based on MESSAGE):

.. math:: Y_{y=0, r} = GDP_{r, y=0} + total\_cost_{r, y=0}

The production function coefficients for capital, labor :math:`a_r` and energy :math:`b_{r, s}` are calibrated from the first-order optimality condition, i.e. 
:math:`b_{r, s}` from :math:`\frac{\partial Y}{\partial NEWENE_{r,s}} = p_{r,s}^{ref}` and :math:`a_r` by inserting :math:`b_r` back into the production function,
setting the labor force index in the base year to 1 (numeraire) and solving for :math:`a_r` :cite:`manne_buying_1992`.

.. math:: b_{r,s} = p_{r,s}^{ref} \cdot \left( \frac{Y_{y=0, r}}{{PHYSENE}_{r, s, y=0}} \right)^{\rho_r - 1}

.. math:: a_r = Y_{y=0, r}^{\rho_r} - \sum_s b_{r,s} \cdot \frac{{{PHYSENE}_{r, s, y=0}}^{\rho_r}} {{K_{y=0, r}}^{\rho_r \cdot \alpha_r}}

Macro-economic parameters
~~~~
Given that MESSAGE includes (exogenous) energy efficiency improvements in end-use technologies as well as significant potential final-to-useful energy efficiency improvements via fuel switching 
(e.g., via electrification of thermal demands and transportation), for the elasticity of substitution between capital-labor and total energy demand :math:`\epsilon_r` in MACRO  we choose 
relatively low values in the range of 0.2 and 0.3. The elasticities are region-dependent with developed regions :math:`r \in \{NAM, PAO, WEU\}` assumed to have higher elasticities of 0.3, 
economies in transition :math:`r \in \{EEU, FSU\}` intermediate values of 0.25 and developing regions :math:`r \in \{AFR, CPA, LAM, MEA, PAS, SAS\}` the lowest elasticities of 0.2.

The capital value share parameter :math:`\alpha_r` can be interpreted as the optimal share of capital in total value added :cite:`manne_buying_1992` and is chosen region-dependent 
with lower values between 0.24 and 0.28 assumed for developed regions and slightly higher values of 0.3 assumed for economies in transition and developing country regions.

Calibration
~~~~
Via a simple iterative algorithm, MACRO is typically calibrated to an exogenously specified set of regional GDP trajectories and useful energy demand projections from MESSAGE. 
To calibrate GDP, after each MACRO run the realized GDP from MACRO and the GDP to be calibrated to are compared and the potential GDP growth rate :math:`{GROW}_{y, r}` used in MACRO is 
then adjusted according to the following formula.

.. math:: {GROW\_corr}_{y, r} = \left( \frac{{GDP\_cal}_{r, y+1}}{{GDP\_cal}_{r, y}} \right)^{\frac{1}{{duration\_period}_{y+1}}} - \left( \frac{{GDP\_MACRO}_{r, y+1}}{{GDP\_MACRO}_{r, y}} \right)^{\frac{1}{{duration\_period}_{y+1}}}

where :math:`{GDP\_cal}_{r, y, s}` is the set of GDP values that MACRO should be calibrated to. In the next run of MACRO the potential GDP growth rate :math:`{GROW}_{y, r}` is chosen to be

.. math:: {GROW}_{y, r} = {GROW}_{y, r} + {GROW\_corr}_{y, r} ,

after which the procedure is repeated. Similarly, to calibrate the physical energy demands :math:`{PHYSENE}_{r, y, s}` to ones from MESSAGE, the demand level realized in MACRO and the 
desired demand level from a MESSAGE model run are compared and the autonomous energy efficiency improvements (AEEIs) are corrected according to the following equations.

.. math:: {aeei\_corr}_{r, y, s} = \left( \frac{{PHYSENE}_{r, y+1, s}}{{DEMAND\_cal}_{r, y+1, s}} / \frac{{PHYSENE}_{r, y, s}}{{DEMAND\_cal}_{r, y, s}} \right)^{\frac{1}{{duration\_period}_{y+1}}} - 1

.. math:: {aeei}_{r, y, s} = {aeei}_{r, y, s} + {aeei\_corr}_{r, y, s}

where :math:`{DEMAND\_cal}_{r, y, s}` is the set of demand levels from MESSAGE that MACRO should be calibrated to.

Given that GDP and demand calibration interact with each other, in practice they are done in an alternating fashion, i.e. after the first MACRO model run, the potential GDP growth rates 
are adjusted and in the second run the AEEI coefficients are adjusted. This calibration loop is continued until the correction factors for both the potential GDP growth rates 
:math:`{GROW\_corr}_{y, r}` and the AEEI coefficients :math:`{aeei}_{r, y, s}` all stay below :math:`10^{-5}`.

Iterating between MESSAGE and MACRO
----

Exchanged parameters
~~~~
MESSAGE and MACRO exchange demand levels of the six commercial servcie demand categories represented in MESSAGE, their corresponding prices as well as total energy system costs including
trade effects of energy commodities and carbon permits (if any explicit mititgation effort sharing regime is implemented).

Convergence criterion
~~~~
The iteration between MESSAGE and MACRO is either stopped after a fixed number of iterations - in case of which the user needs to manually check convergence between the models - or 
once the maximum of changes across all energy demand categories and regions (i.e. the convergence criterion) is less than a specified threshold. In both cases the convergence criterion 
is typically set to around 1%.

Constraint on demand response
~~~~
Demand responses from MACRO to MESSAGE can be large if the initial demands are far from the equlibrium demand levels of a specific scenario (e.g., when using demand from a non-climate policy scenario
as the starting point for a stringent climate mitigation scenario that aims at limiting temperature change to 2°C). To avoid oscillations of demands in subsequent MESSAGE-MACRO iterations, a constraint
on the maximum permissible demand change between subquent iterations has been introduced which is usually set to 15%. In practical terms this means that the demand response is capped at 15% for each type of :ref:`demand` and for each of the 11 MESSAGE :ref:`spatial`. 
However, under specific conditions - typically under stringent climate policy - when price repsonses to small demand adjustments are large, an oscillating behavior between two sets of demand levels 
can still occur. In such situations, the constraint on the demand response is reduced further until the changes in demand are less than the convergence criterion mentioned above.

