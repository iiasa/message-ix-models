.. _annex_macro:

Implementation: MACRO
=====================

MACRO is based on the macro-economic module of the global energy-economy-climate model Global 2100 :cite:`manne_buying_1992`, a predecesor of the `MERGE <http://www.stanford.edu/group/MERGE/>`_ model. The original soft-linkage between MACRO and MESSAGE has been described in :cite:`messner_messagemacro:_2000`, but several adjustments have been made compared to this original implementation. The description below builds to a certain degree on these two publications, but deviates in some places as discussed in the following paragraphs. It is worthwhile mentioning that MACRO as used with MESSAGE has similar origins as the MACRO module of MARKAL-MACRO :cite:`loulou_markal-macro_2004` with the exception of being soft-linked rather than hard-linked to the energy systems part of the model.

On the one hand, while the version of MACRO described in :cite:`messner_messagemacro:_2000` like the MACRO module of Global 2100 operated at the level of electric and non-electric energy demands in the production function, the present version of MACRO operates at the level of the six commercial useful energy demands represented in MESSAGE (:ref:`message`). This change was made in response to electrification becoming a tangible option for the transport sector with the introduction of electric cars over the past decade. Previsouly (and as described in :cite:`messner_messagemacro:_2000`), the electric useful energy demands in MESSAGE had been mapped to electric demand in MACRO and the thermal useful energy demands, non-energy feedstock and transport demands in MESSAGE had been mapped to non-electric demand in MACRO. 

On the other hand, as a result of switching the implementation of `MESSAGE to GAMS <http://message.iiasa.ac.at/en/stable/framework.htm>`_, the iterative information exchange between the two models is now handled within GAMS. This accelerates the iteration process considerably, because the solution of the previous iteration is kept in memory and can serve as a starting point for the next iteration.

Finally, the parameterization of MACRO has changed in a specific way. As mentioned, the model's most important input parameters are the projected growth rates of total labor, i.e., the combined effect of labor force and labor productivity growth (note that labor supply growth is also referred to as reference or potential GDP growth) and the annual rates of reference energy intensity reductions, i.e. the so-called autonomous energy efficiecy improvement (AEEI) coefficients. In all recent applications of MACRO, including the Shared Socio-economic Pathways (SSPs), these are calibrated to be consistent with the developments in a MESSAGE scenario. In practice, this happens by running MACRO and adjusting the potential GDP growth rates and the AEEI coefficients on a sectoral basis until MACRO does not produce an energy demand response and GDP feedback compared to the MESSAGE scenario that it is calibrated to.

MACRO parameterization
----------------------

Initial conditions
~~~~~~~~~~~~~~~~~~
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
~~~~~~~~~~~~~~~~~~~~~~~~~
Given that MESSAGE includes (exogenous) energy efficiency improvements in end-use technologies as well as significant potential final-to-useful energy efficiency improvements via fuel switching 
(e.g., via electrification of thermal demands and transportation), for the elasticity of substitution between capital-labor and total energy demand :math:`\epsilon_r` in MACRO  relatively low values in the range of 0.2 and 0.3 were chosen. The elasticities are region-dependent with developed regions :math:`r \in \{NAM, PAO, WEU\}` assumed to have higher elasticities of 0.3, 
economies in transition :math:`r \in \{EEU, FSU\}` intermediate values of 0.25 and developing regions :math:`r \in \{AFR, CPA, LAM, MEA, PAS, SAS\}` the lowest elasticities of 0.2.

The capital value share parameter :math:`\alpha_r` can be interpreted as the optimal share of capital in total value added :cite:`manne_buying_1992` and is chosen region-dependent 
with lower values between 0.24 and 0.28 assumed for developed regions and slightly higher values of 0.3 assumed for economies in transition and developing country regions.

Calibration
~~~~~~~~~~~
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
-----------------------------------

Exchanged parameters
~~~~~~~~~~~~~~~~~~~~
MESSAGE and MACRO exchange demand levels of the six commercial servcie demand categories represented in MESSAGE, their corresponding prices as well as total energy system costs including
trade effects of energy commodities and carbon permits (if any explicit mititgation effort sharing regime is implemented).

Convergence criterion
~~~~~~~~~~~~~~~~~~~~~
The iteration between MESSAGE and MACRO is either stopped after a fixed number of iterations - in case of which the user needs to manually check convergence between the models - or 
once the maximum of changes across all energy demand categories and regions (i.e. the convergence criterion) is less than a specified threshold. In both cases the convergence criterion 
is typically set to around 1%.

Constraint on demand response
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Demand responses from MACRO to MESSAGE can be large if the initial demands are far from the equlibrium demand levels of a specific scenario (e.g., when using demand from a non-climate policy scenario
as the starting point for a stringent climate mitigation scenario that aims at limiting temperature change to 2 degrees C). To avoid oscillations of demands in subsequent MESSAGE-MACRO iterations, a constraint
on the maximum permissible demand change between subquent iterations has been introduced which is usually set to 15%. In practical terms this means that the demand response is capped at 
20% for each type of :ref:`demand` and for each of the MESSAGE :ref:`spatial`. 
However, under specific conditions - typically under stringent climate policy - when price repsonses to small demand adjustments are large, an oscillating behavior between two sets of demand levels 
can still occur. In such situations, the constraint on the demand response is reduced further until the changes in demand are less than the convergence criterion mentioned above.
