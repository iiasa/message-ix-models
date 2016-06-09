.. _other:

Other conversion
================
Beyond electricity and heat generation there are three further subsectors of the conversion sector represented in MESSAGE, liquid fuel production, gaseous production and hydrogen production.

Liquid Fuel Production
----------------------
Apart from oil refining as predominant supply technology for liquid fuels at present a number of alternative liquid fuel production routes from different feedstocks are represented in MESSAGE (see :numref:`tab-liqfuel`). Different processes for coal liquefaction, gas-to-liquids technologiesand biomass-to-liquids technologies both with and without CCS are covered. Some of these technologies include co-generation of electricity, for example, by burning unconverted syngas from a Fischer-Tropsch synthesis in a gas turbine (c.f. Larson et al., 2012 :cite:`larson_chapter_2012`). Technology costs for the synthetic liquid fuel production options are based on Larson et al. (2012) (:cite:`larson_chapter_2012`).

.. _tab-liqfuel:
.. table:: Liquid fuel production technologies in MESSAGE by energy source.

   +----------------+----------------------------------------------+---------------------------+
   | Energy Source  | Technology                                   | Electricity cogeneration  |
   +================+==============================================+===========================+
   | biomass        | Fischer-Tropsch biomass-to-liquids           | yes                       |
   |                +----------------------------------------------+---------------------------+
   |                | Fischer-Tropsch biomass-to-liquids with CCS  | yes                       |
   +----------------+----------------------------------------------+---------------------------+
   | coal           | Fischer-Tropsch coal-to-liquids              | yes                       |
   |                +----------------------------------------------+---------------------------+
   |                | Fischer-Tropsch coal-to-liquids with CCS     | yes                       |
   |                +----------------------------------------------+---------------------------+
   |                | coal methanol-to-gasoline                    | yes                       |
   |                +----------------------------------------------+---------------------------+
   |                | coal methanol-to-gasoline with CCS           | yes                       |
   +----------------+----------------------------------------------+---------------------------+
   | gas            | Fischer-Tropsch gas-to-liquids               | no                        |
   |                +----------------------------------------------+---------------------------+
   |                | Fischer-Tropsch gas-to-liquids with CCS      | no                        |
   +----------------+----------------------------------------------+---------------------------+
   | oil            | simple refinery                              | no                        |
   |                +----------------------------------------------+---------------------------+
   |                | complex refinery                             | no                        |
   +----------------+----------------------------------------------+---------------------------+

Gaseous Fuel Production
-----------------------

See :numref:`tab-gasfuel` for a list of gaseous fuel production technologies in MESSAGE.

.. _tab-gasfuel:
.. table:: Gaseous fuel production technologies in MESSAGE by energy source.

   +----------------+-------------------------------+
   | Energy Source  | Technology                    |
   +================+===============================+
   | biomass        | biomass gasification          |
   |                +-------------------------------+
   |                | biomass gasification with CCS |
   +----------------+-------------------------------+
   | coal           | coal gasification             |
   |                +-------------------------------+
   |                | coal gasification with CCS    |
   +----------------+-------------------------------+

Hydrogen Production
-------------------

See :numref:`tab-hydtech` for a list of gaseous fuel production technologies in MESSAGE.

.. _tab-hydtech: 
.. table:: Hydrogen production technologies in MESSAGE by energy source.

   +----------------+-----------------------------------+---------------------------+
   | Energy source  | Technology                        | Electricity cogeneration  |
   +================+===================================+===========================+
   | gas            | steam methane reforming           | yes                       |
   |                +-----------------------------------+---------------------------+
   |                | steam methane reforming with CCS  | no                        |
   +----------------+-----------------------------------+---------------------------+
   | electricity    | electrolysis                      | no                        |
   +----------------+-----------------------------------+---------------------------+
   | coal           | coal gasification                 | yes                       |
   |                +-----------------------------------+---------------------------+
   |                | coal gasification with CCS        | yes                       |
   +----------------+-----------------------------------+---------------------------+
   | biomass        | biomass gasification              | yes                       |
   |                +-----------------------------------+---------------------------+
   |                | biomass gasification with CCS     | yes                       |
   +----------------+-----------------------------------+---------------------------+

Technological change in MESSAGE is generally treated exogenously, although pioneering work on the endogenization of technological change in energy-engineering type models has been done with MESSAGE (Messner, 1997 :cite:`messner_endogenized_1997`). The current cost and performance parameters, including conversion efficiencies and emission coefficients is generally derived from the relevant engineering literature. For the future alternative cost and performance projections are usually developed to cover a relatively wide range of uncertainties that influences model results to a good extent. As an example, :numref:`fig-costind` below provides an overview of costs ranges for a set of key energy conversion technologies (Fricko et al., 2016 :cite:`fricko_marker_2016`).

.. _fig-costind:
.. figure:: /_static/S3OtherCosts.png
   :width: 600px

   Cost indicators for other conversion technology investment (Fricko et al., 2016 :cite:`fricko_marker_2016`) 
   
In :numref:`fig-costind`, the black ranges show historical cost ranges for 2005. Green, blue, and red ranges show cost ranges in 2100 for SSP1, SSP2, and SSP3, respectively. Global values are represented by solid ranges. Values in the global South are represented by dashed ranges. The diamonds show the costs in the “North America” region. CCS – Carbon capture and storage; CTL – Coal to liquids; GTL – Gas to liquids; BTL – Biomass to liquids (Fricko et al., 2016 :cite:`fricko_marker_2016`).
