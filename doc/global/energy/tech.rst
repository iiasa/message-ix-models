.. _techchange:

Technological change
======================
Technological change in |MESSAGEix| is generally treated exogenously, although pioneering works on the endogenization of technological change via learning curves in energy-engineering type models (Messner, 1997 :cite:`messner_endogenized_1997`) and the dependence of technology costs on market structure have been done with |MESSAGEix| (Leibowicz, 2015 :cite:`leibowicz_growth_2015`). The current cost and performance parameters, including conversion efficiencies and emission coefficients are generally derived from the relevant engineering literature. For the future, alternative cost and performance projections are developed to cover a relatively wide range of uncertainties that influence model results to a good extent.

Technology cost
----------------
The quantitative assumptions about technology cost development are derived from the overarching qualitative SSP narratives (cf. section :ref:`narratives`). In SSP1, for instance, whose "green-growth" storyline is more consistent with a sustainable development paradigm, higher rates of technological progress and learning are assumed for renewable energy technologies and other advanced technologies that may replace fossil fuels (e.g., the potential for electric mobility is assumed to be higher in SSP1 compared to SSP2 or SSP3). In contrast, SSP3 assumes limited progress across a host of advanced technologies, particularly for renewables and hydrogen; more optimistic assumptions are instead made for coal-based technologies, not only for power generation but also for liquid fuels production (e.g., coal-to-liquids). Meanwhile, the middle-of-the-road SSP2 narrative is characterized by a fairly balanced view of progress for both conventional fossil and non-fossil technologies. In this sense, technological development in SSP2 is not biased toward any particular technology group.

Technological costs vary regionally in all SSPs, reflecting marked differences in engineering and construction costs across countries observed in the real world. The regional differentiation of technology costs for the initial modeling periods are based on IEA data (IEA, 2014 :cite:`iea_investment_2014`) with convergence of costs assumed over time driven by economic development (GDP/cap). Generally, costs start out lower in the developing world and are assumed to converge to those of present-day industrialized countries as the former becomes richer throughout the century (thus, the cost projections consider both labour and capital components). This catch-up in costs is assumed to be fastest in SSP1 and slowest in SSP3 (where differences remain, even in 2100); SSP2 is in between. Estimates for present-day and fully learned-out technology costs are from the Global Energy Assessment (Riahi et al., 2012 :cite:`riahi_chapter_2012`) and World Energy Outlook (IEA, 2014 :cite:`international_energy_agency_world_2014`). A summary of these cost assumptions can be found in sections :ref:`electricity` and :ref:`other`.


Technology diffusion
---------------------
MESSAGE tracks investments by vintage, an important feature to represent the inertia in the energy system due to its long-lived capital stock. In case of shocks
(e.g., introduction of stringent climate policy), it is however possible to prematurely retire existing capital stock such as power plants or other energy conversion
technologies and switch to more suitable alternatives.

An important factor in this context that influences technology adoption in |MESSAGEix| are technology diffusion constraints. Technology diffusion in |MESSAGEix| is determined
by dynamic constraints that relate the construction of a technology added or the activity (level of production) of a technology in a period *t* to construction or the
activity in the previous period *t-1* (Messner and Strubegger, 1995 :cite:`messner_users_1995`, cf. section :ref:`Dynamic constraints <message_ix:dynamic_constraints>`).

While limiting the possibility of flip-flop behavior as is frequently observed in unconstrained Linear Programming (LP) models such as |MESSAGEix|, a drawback of such hard
growth constraints is that the relative advantage of some technology over another technology is not taken into account and therefore even for very competitive technologies,
no rapid acceleration of technology diffusion is possible. In response to this limitation, so called flexible or soft dynamic constraints have been introduced into MESSAGE
(Keppo and Strubegger, 2010 :cite:`keppo_short_2010`). These allow faster technology diffusion at additional costs and therefore generate additional model flexibility
while still reducing the flip-flop behavior and sudden penetration of technologies.

:numref:`fig-difconstraint` below illustrates the maximum technology growth starting at a level of 1 in year *t* =0 for a set of five diffusion constraints which jointly lead to a soft constraint.

.. _fig-difconstraint:
.. figure:: /_static/diffusion_constraint_example.png
   :width: 700px

   Illustration of maximum technology growth starting at a level of 1 in year t=0 for a set of soft diffusion constraints with effective growth rates r as shown in the legend.

For a more detailed description of the implementation of technology diffusion constraints, see the Section :ref:`Dynamic constraints <message_ix:dynamic_constraints>` of the :ref:`|MESSAGEix| documentation`<message_ix>`.
