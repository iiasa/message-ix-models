.. fuel_blending:

Fuel Blending
=============
Fuel blending in the energy system is a common practice, which allows the shared use of infrastructure by fuels with similar chemical attributes and thus use at the secondary and final energy level, without requiring the consumer to adapt the power plant or enduse devices. Fuel blending in the global energy model is modelled for two distinct blending processes.  The first relates to the blending of natural gas with other synthetic gases. The second is related to the blending of light oil with coal derived synthetic liquids.  In order to ensure that emissions and energy flows are correctly accounted for, blended fuels types are nevertheless explicitly modelled.  


Natural gas and synthetic gas
-----------------------------
Natural gas can be blended with hydrogen or with synthetic gas derived from the gasification of biomass or coal (cf. Section :ref:`other`). Despite the fact that in the real world, hydrogen or other synthetic gases are physically injected into a natural gas network, it is important to be able to track the use of blended fuels in the energy model for two reasons. Not all blended fuels can be used equally within all natural gas applications.  For example, hydrogen mixed into the natural gas network is restricted to use in non-CCS applications only. Secondly, it is essential to keep track of where which of the blended fuels is being used in order to correctly report emissions and also to potentially restrict the degree to which fuels can be blended for individual applications.  For example, natural gas end-use appliances may only be able to cope with a certain share of hydrogen while still guaranteeing their safety and longevity. Similarly, for policy analysis, it could be required that a certain minimum share of a synthetic gas is used sector specifically.

.. _fig-fuel_blending:
.. figure:: /_static/RES_fuel_blending.png
   :width: 700px

   Reference Energy System excerpt depicting the modelling of fuel blending.

Synthetic liquids and lightoil
------------------------------
Synthetic fueloil via coal liquefaction is blended into the lightoil stream at the secondary energy level.
