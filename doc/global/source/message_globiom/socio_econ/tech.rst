Technological change
======================
Technological change in MESSAGE is generally treated exogenously, although pioneering work on the endogenization of technological change in energy-engineering type models has been done with MESSAGE (:cite:`messner_endogenized_1997`). The current cost and performance parameters, including conversion efficiencies and emission coefficients is generally derived from the relevant engineering literature. For the future alternative cost and performance projections are usually developed to cover a relatively wide range of uncertainties that influences model results to a good extent. As an example, Figure 2.1 below provides an overview of costs ranges for a set of key energy conversion technologies as developed in the Global Energy Assessment (`GEA <http://www.globalenergyassessment.org/>`_) and applied in MESSAGE. More detailed techno-economic assumptions can be found used in MESSAGE V.4 can be found in the `GEA scenario database <http://www.iiasa.ac.at/web-apps/ene/geadb/dsd?Action=htmlpage&page=welcome>`_.

.. image:: /_static/GEA_technology_cost_ranges.png
   :width: 600px
**Figure 2.1**: Investment cost per unit of energy production capacity (`van Vliet et al., 2012 <https://wiki.ucl.ac.uk/display/ADVIAM/References+MESSAGE>`_).

MESSAGE tracks investments by vintage, an important feature to represent the inertia in the energy system due to its long-lived capital stock. In case of shocks (e.g., introduction of stringent climate policy), it is however possible to prematurely retire existing capital stock such as power plants or other energy conversion technologies and switch to more suitable alternatives.

An important factor in this context that influences technology adoption in MESSAGE are technology diffusion constraints. Technology diffusion in MESSAGE is constraint by dynamic constraints that relate the activity a (level of production) in a period t to the activity in the previous period t-1 (:cite:`keppo_short_2010`). Two parameters, an annual increment or startup value s and an annual growth rate r are used to parameterize these dynamic growth constraints. For a period length of one year, the following equation describes the dynamic constraints:

.. image:: /_static/technology_diffusion_eq_1.png
   :width: 160px

Without the startup value s, a technology with a zero activity level in year t-1 would not be able to ever reach a non-zero deployment levels, i.e. a non-zero startup value allows the technology to start growing from zero which would not be possible with a purely exponential growth relationship. Therefore, the value of s governs the technology growth during the early stages of entering the market while the growth rates r predominantly constrain the annual growth once the initial deployment is over.

For an arbitrary period length of T years, the maximum level of technology activity in period t, at , reads as follows:

.. image:: /_static/technology_diffusion_eq_2.png

with the period increment 

.. image:: /_static/technology_diffusion_eq_2a.png
   :width: 120px

While limiting the possibility of flip-flop behavior as it is frequently observed in unconstrained Linear Programming (LP) models such as MESSAGE, a drawback of such hard growth constraints is that the relative advantage of some technology over another technology is not taken into account and therefore even for very competitive technologies, no acceleration of technology diffusion is possible. In response to this limitation, so called flexible or soft dynamic constraints have been introduced into MESSAGE (Keppo and Strubegger, 2010). These allow faster technology diffusion at additional costs and therefore generate additional model flexibility while still reducing the flip-flop behavior and sudden penetration of technologies. To operationalize this concept, a set of n dummy variables, bi, multiplied by a corresponding growth factor (1+ri)T are added to the dynamic growth constraint in Eq. (1).

.. image:: /_static/technology_diffusion_eq_3.png
   :width: 340px
   
The maximum value for these dummy variables bi is limited to the activity of the underlying technology a, i.e.

.. image:: /_static/technology_diffusion_eq_4.png 
   :width: 60px
   :align: left

, for all i .

Therefore, this new formulation increases the highest allowed growth factor from

.. image:: /_static/technology_diffusion_eq_4a.png
   :width: 75px
   :align: left
   
to 

.. image:: /_static/technology_diffusion_eq_4b.png
   :width: 180px

In addition, the objective function value for period t is modified by the extra term

 .. image:: /_static/technology_diffusion_eq_5.png
   :width: 140px

which adds costs ci  per additional growth factor utilized. The figure below illustrates the maximum technology growth starting at a level of 1 in year t=0 for a set of five diffusion constraints which jointly lead to a soft constraint.

 .. image:: /_static/diffusion_constraint_example.png
   :width: 700px

**Figure 2.2**: Illustration of maximum technology growth starting at a level of 1 in year t=0 for a set of soft diffusion constraints with effective growth rates r as shown in the legend.
