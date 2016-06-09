.. _macro:

Macro-economy
----
The detailed energy supply model (MESSAGE) is soft-linked to an aggregated macro-economic model (MACRO) which has been adopted from the so-called Global 2100 or ETA-MACRO model 
(Manne and Richels, 1992 :cite:`manne_buying_1992`), a predecessor of the `MERGE <http://www.stanford.edu/group/MERGE/>`_ model. The reason for linking the two models 
is to consistently reflect the influence of energy supply costs, as calculated by MESSAGE, in the mix of production factors considered in MACRO, and the effect of changes 
in energy demand on energy costs. The combined MESSAGE-MACRO model (Messner and Schrattenholzer, 2000 :cite:`messner_messagemacro:_2000`) can generate a consistent
economic response to changes in energy prices and estimate overall economic consequences (e.g., GDP or consumption loss) of energy or climate policies.

MACRO is a macroeconomic model maximizing the intertemporal utility function of a single representative producer-consumer in each world region. The optimization result is 
a sequence of optimal savings, investment, and consumption decisions. The main variables of the model are the capital stock, available labor, and energy inputs, which 
together determine the total output of an economy according to a nested CES (constant elasticity of substitution) production function. Energy demand in the six commercial 
demand categories of MESSAGE (see :ref:`demand`) is determined within the model, and is consistent with energy supply curves, which are inputs to the model. The model’s most 
important driving input variables are the projected growth rates of total labor, i.e., the combined effect of labor force and labor productivity growth and the annual 
rates of reference energy intensity reduction. The latter is calibrated to the developments in a MESSAGE baseline scenario to ensure consistency between the two models. 
Labor supply growth is also referred to as reference or potential GDP growth. In the absence of price changes, energy demands grow at rates that are the approximate 
result of potential GDP growth rates, reduced by the rates of overall energy intensity reduction. Price changes of the six demand categories can alter this path significantly.

MACRO’s production function includes six commercial energy demand categories represented in MESSAGE. To optimize, MACRO requires cost information for each demand category. 
The exact definitions of these costs as a function over all positive quantities of energy cannot be given in closed form because each point of the function would be a result 
of a full MESSAGE run. However, the optimality conditions implicit in the formulation of MACRO only require the functional values and its derivatives at the optimal point 
to be consistent between the two sub-models. Since these requirements are therefore only local, most functions with this feature will simulate the combined energy-economic 
system in the neighborhood of the optimal point. The costs (energy use and imports) and benefits (energy exports) of providing energy in MACRO are approximated by a Taylor 
expansion to first order of the energy system costs as calculated by MESSAGE. From an initial MESSAGE model run, the total energy system cost (including costs/revenues from 
energy trade) and additional abatement costs (e.g., abatement costs from non-energy sources) as well as the shadow prices of the six commercial demand categories by region 
are passed to MACRO. In addition to the economic implications of energy trade, MACRO also includes the implications of GHG permit trade. 

For a more elaborate description of MACRO, including the system of equations and technical details of the implementation, please consult the :ref:`annex_macro`.
