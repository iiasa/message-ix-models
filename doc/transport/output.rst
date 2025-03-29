Reporting
*********

This section describes reporting outputs available for MESSAGEix-Transport.

Plots
=====

:class:`.InvCost`
   Investment costs.

:class:`.FixCost`
   Fixed costs.

:class:`.VarCost`
   Variable costs.

Data flows
==========

Quick links to each of the data flows:
:data:`~.data.activity_passenger`
:data:`~.data.activity_vehicle`
:data:`~.data.fe_transport`
:data:`~.data.gdp_in`
:data:`~.data.population_in`

.. autodata:: message_ix_models.model.transport.data.activity_passenger
.. autodata:: message_ix_models.model.transport.data.activity_vehicle
.. autodata:: message_ix_models.model.transport.data.fe_transport
.. autodata:: message_ix_models.model.transport.data.gdp_in
.. autodata:: message_ix_models.model.transport.data.population_in

Quantities
==========

.. note:: This section currently uses labels pertaining to the :doc:`/project/ngfs` project.
   Features to use other sets of labels are not yet developed.

Data from MESSAGEix-Transport and other models and sources is collected for various projects and activities.
The data to be collected are often specified via a “**template**” for data in “**IAMC format**”.
Among other characteristics, the IAMC format collapses all but a very few concepts into a (long) string called a “**Variable name**” that encodes, among other things:

1. The **concept** and/or specific systematized **measure**, including particular units of measurement.
2. Labels for 1 or more **dimensions** of multiple-dimensional data.
   The names of dimensions themselves are not given, and usually inconsistent labels, dimension order, etc. are used in various places in the same template.
3. **Partial sums** over some or all labels along some dimension(s).

These are accompanied by units and descriptions that carry additional information.
(For a detailed discussion of the issues here and better approaches, see the `iiasa/edits-data <https://github.com/iiasa/edits-data/>`_ repository and `accompanying whitepaper <https://www.overleaf.com/read/npnxbnttgfht>`_.)

One task for the MESSAGEix-Transport reporting code (:mod:`.transport.report`) is to translate from the clear, rigid structure of data in the :mod:`message_ix` core formulation (and :mod:`genno`, which is intended to support such data) to these looser and often ambiguously-defined concepts and categories.
This section explains how such translation works, serving both as a requirements specification for and documentation of the code.

.. contents::
   :local:
   :backlinks: none

Integrated through legacy reporting
-----------------------------------

As of 2022-04, the :doc:`/api/report/legacy` is still in active use.
One function it performs is **aggregation**: some totals across both transport and other sectors are computed by this code, along with other calculations.

The transport reporting code must compute and store these data before it can be picked up and aggregated.
The variable names must correspond exactly to the variable names expected by the legacy code.

CO₂ emissions
~~~~~~~~~~~~~

Label stub: ``Emissions|CO2|Energy|Demand|Transportation…``

Units: Mt/a

Description stub: “{CO2|carbon dioxide} emissions from…”

- ``(nothing)``           “…fuel combustion in transportation sector (IPCC category 1A3), excluding pipeline emissions (IPCC category 1A3ei)”
- ``|Aviation``           “…transport by aviation mode”
- ``|Aviation|Freight``   “…transport by freight aviation mode”
- ``|Aviation|Passenger`` “…transport by passenger aviation mode”
- ``|Freight``            “…fuel combustion in freight transportation sector (part of IPCC category 1A3), excluding pipeline emissions (IPCC category 1A3ei)”
- ``|Maritime``           “…transport by maritime mode”
- ``|Maritime|Freight``   “…transport by freight maritime mode”
- ``|Maritime|Passenger`` “…transport by passenger maritime mode”
- ``|Passenger``          “…fuel combustion in passenger transportation sector (part of IPCC category 1A3)”
- ``|Rail``               “…transport by rail mode”
- ``|Rail|Freight``       “…transport by freight rail mode”
- ``|Road|Passenger|Bus`` “…road transport passenger Buses”

Non-CO₂ emissions
~~~~~~~~~~~~~~~~~

Label stub: ``Emissions|{species}|Energy|Demand|Transportation``

Units: Mt / a (note that the quantity measured is the mass of the given species)

Description stub: “{species} {eE}missions from {fuel c,C}ombustion in {tT}ransportation {sS}ector (IPCC category 1A3){extra}”

- ``BC``     “BC                         …, excluding pipeline emissions (IPCC category 1A3ei)”
- ``CH4``    “CH4                        …, excluding pipeline emissions (IPCC category 1A3ei)”
- ``CO``     “Carbon Monoxide            …”[1]_
- ``NH3``    “Ammonia                    …”[1]_
- ``NOx``    “NOx                        …, excluding pipeline emissions (IPCC category 1A3ei)”
- ``OC``     “OC                         …, excluding pipeline emissions (IPCC category 1A3ei)”
- ``Sulfur`` “Sulfur (SO2)               …, excluding pipeline emissions (IPCC category 1A3ei)”
- ``VOC``    “Volatile Organic Compounds …”

.. [1] unclear if this is a deliberate difference in definition or just an error.

Final energy
~~~~~~~~~~~~

These mix at least three different hierachies of labels.

Label stub: ``Final Energy|Transportation|…``

Units: EJ / a

- ``(nothing)`` “final energy consumed in the transportation sector, including bunker fuels, excluding pipelines”

…by mode, service, vehicle type and/or powertrain technology
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Description stub: “Final energy consumed in the transport sector by {…}”

- ``Aviation``              “aviation mode”
- ``Aviation|Freight``      “freight aviation mode”
- ``Aviation|Passenger``    “passenger aviation mode”
- ``Maritime``              “maritime mode”
- ``Maritime|Freight``      “maritime freight mode”
- ``Maritime|Passenger``    “maritime passenger mode”
- ``Rail``                  “rail mode”
- ``Rail|Freight``          “freight rail mode”
- ``Rail|Passenger``        “passenger rail mode”
- ``Road|Freight``          “road transport freight vehicles”
- ``Road|Freight|Electric`` “road transport freight  electric vehicles (e.g. PHEV, BEV)”[2]_
- ``Road|Freight|FC``       “road transport freight  fuel cell vehicles”
- ``Road|Freight|ICE``      “road transport freight vehicles , driven by an internal combustion engine (including HEVs)”
- ``Road|Passenger``        “road transport passenger vehicles”
- ``Road|Passenger|2W&3W``  “road passenger transport by 2W and 3W vehicles”[3]_
- ``Road|Passenger|Bus``    “road passenger transport on roads (buses)”
- ``Road|Passenger|LDV``    “road passenger transport  (light-duty vehicles: passenger cars and light trucks/SUVs/vans)”

.. [2] note that the final dimension here is **powertrain technology**.
.. [3] the final dimension here is instead **vehicle type**.

…by energy carrier
::::::::::::::::::

- ``Electricity``            “final energy consumption by the transportation sector of electricity (including on-site solar PV), excluding transmission/distribution losses”
- ``Gases``                  “final energy consumption by the transportation sector of gases (natural gas, biogas, coal-gas), excluding transmission/distribution losses”
- ``Gases|Bioenergy``        “Final bioenergy-based gas energy consumed in the transportation sector.”
- ``Gases|Fossil``           “Final fossil-based gas energy consumed in the transportation sector.”
- ``Hydrogen``               “final energy consumption by the transportation sector of hydrogen”
- ``Liquids``                “final energy consumption by the transportation sector of refined liquids (conventional & unconventional oil, biofuels, coal-to-liquids, gas-to-liquids)”
- ``Liquids|Bioenergy``      “Final biofuels based (liquid or gas) energy consumed in the transport sector by passenger and freight vehicles”
- ``Liquids|Coal``           “final energy consumption by the transportation sector of coal based liquids (coal-to-liquids)”
- ``Liquids|Fossil synfuel`` “Final energy, in the form of fossil synfuel (e.g. CTL, GTL, Methanol, and DME), consumed in the transport sector by passenger and freight vehicles”
- ``Liquids|Natural Gas``    “final energy consumption by the transportation sector of natrual gas based liquids (gas-to-liquids)”
- ``Liquids|Oil``            “final energy consumption by the transportation sector of liquid oil products (from conventional & unconventional oil)”
- ``Other``                  “final energy consumption by the transportation sector of other sources that do not fit to any other category (please provide a definition of the sources in this category in the 'comments' tab)”[4]_

.. [4] We can infer “other **energy** sources” from the way it is used in the next hierarchy below.

…by service and energy carrier
::::::::::::::::::::::::::::::

Note that the set of energy carriers differs from the set used in the above hierarchy.
For instance, ``Liquids|Coal`` and ``Liquids|Natural Gas`` appear above, but not here.

- ``Freight``                          “final energy consumed for freight transportation”
- ``Freight|Electricity``              “final energy consumption by the freight transportation sector of electricity (including on-site solar PV), excluding transmission/distribution losses”
- ``Freight|Gases``                    “final energy consumption by the freight transportation sector of gases (natural gas, biogas, coal-gas), excluding transmission/distribution losses”
- ``Freight|Gases|Bioenergy``          “Final bioenergy-based gas energy consumed in the transportation sector by freight transport”
- ``Freight|Gases|Fossil``             “Final fossil-based gas energy consumed in the transportation sector by freight transport”
- ``Freight|Hydrogen``                 “final energy consumption by the freight transportation sector of hydrogen”
- ``Freight|Liquids``                  “final energy consumption by the freight transportation sector of refined liquids (conventional & unconventional oil, biofuels, coal-to-liquids, gas-to-liquids)”
- ``Freight|Liquids|Bioenergy``        “Final biofuels based (liquid or gas) energy consumed in the transport sector by freight vehicles”
- ``Freight|Liquids|Fossil synfuel``   “Final energy, in the form of fossil synfuel (e.g. CTL, GTL, Methanol, and DME), consumed in the transport sector by freight vehicles”
- ``Freight|Liquids|Oil``              “final energy consumption by the freight transportation sector of liquid oil products (from conventional & unconventional oil)”
- ``Freight|Other``                    “final energy consumption by the freight transportation sector of other sources that do not fit to any other category (please provide a definition of the sources in this category in the 'comments' tab)”
- ``Passenger``                        “final energy consumed for passenger transportation”
- ``Passenger|Electricity``            “final energy consumption by the passenger transportation sector of electricity (including on-site solar PV), excluding transmission/distribution losses”
- ``Passenger|Gases``                  “final energy consumption by the passenger transportation sector of gases (natural gas, biogas, coal-gas), excluding transmission/distribution losses”
- ``Passenger|Gases|Bioenergy``        “Final bioenergy-based gas energy consumed in the transportation sector by passenger transport”
- ``Passenger|Gases|Fossil``           “Final fossil-based gas energy consumed in the transportation sector by passenger transport”
- ``Passenger|Hydrogen``               “final energy consumption by the passenger transportation sector of hydrogen”
- ``Passenger|Liquids``                “final energy consumption by the passenger transportation sector of refined liquids (conventional & unconventional oil, biofuels, coal-to-liquids, gas-to-liquids)”
- ``Passenger|Liquids|Bioenergy``      “Final biofuels based (liquid or gas) energy consumed in the transport sector by passenger vehicles”
- ``Passenger|Liquids|Fossil synfuel`` “Final energy, in the form of fossil synfuel (e.g. CTL, GTL, Methanol, and DME), consumed in the transport sector by passenger vehicles”
- ``Passenger|Liquids|Oil``            “final energy consumption by the passenger transportation sector of liquid oil products (from conventional & unconventional oil)”
- ``Passenger|Other``                  “final energy consumption by the passenger transportation sector of other sources that do not fit to any other category (please provide a definition of the sources in this category in the 'comments' tab)”

Calculated and reported directly
--------------------------------

These do not need to correspond to particular labels used by the legacy reporting.

Capacity
~~~~~~~~

Label stub: ``Capacity|Transportation|``

Units: vary

Description stub: “Maximum amount of {…} that can be transported per year {…}”

- ``Aviation|Freight``   [10⁹ tkm / a] “tkms … through the air”
- ``Aviation|Passenger`` [10⁹ pkm / a] “pkms … through the air”
- ``Maritime|Freight``   [10⁹ tkm / a] “tkms … by maritime mode”
- ``Maritime|Passenger`` [10⁹ pkm / a] “pkms … by maritime mode”
- ``Rail|Freight``       [10⁹ tkm / a] “tkms … by rail”
- ``Rail|Passenger``     [10⁹ pkm / a] “pkms … by rail”
- ``Road|Freight``       [10⁹ tkm / a] “tkms … on the road”

Activity (“energy service”, “demand”)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Label stub: ``Energy Service|Transportation|…``

Units: vary

Descriptions: vary

- ``Aviation``                        [10⁹ vkm / a] “Annual demand for energy services in 10⁹ vehicle-kms related to both passenger and freight transportation by Aviation”[5]_
- ``Freight``                         [10⁹ tkm / a] “energy service demand for freight transport”[6]_
- ``Freight|Aviation``                [10⁹ tkm / a] “energy service demand for freight transport on aircrafts”[7]_
- ``Freight|International Shipping``  [10⁹ tkm / a] “energy service demand for freight transport operating on international shipping routes”
- ``Freight|Road``                    [10⁹ tkm / a] “energy service demand for freight transport on roads”
- ``Navigation``                      [10⁹ vkm / a] “Annual demand for energy services in 10⁹ vehicle-kms related to both passenger and freight transportation by Navigation”
- ``Passenger``                       [10⁹ pkm / a] “energy service demand for passenger transport”
- ``Passenger|Aviation``              [10⁹ pkm / a] “energy service demand for passenger transport on aircrafts”
- ``Passenger|Bicycling and Walking`` [10⁹ pkm / a] “energy service demand for passenger transport on bicycles and by foot”
- ``Passenger|Road|Bus``              [10⁹ pkm / a] “energy service demand for passenger transport on roads (buses)”
- ``Rail``                            [10⁹ vkm / a] “Annual demand for energy services in 10⁹ vehicle-kms related to both passenger and freight transportation by Rail”
- ``Road``                            [10⁹ vkm / a] “Annual demand for energy services in 10⁹ vehicle-kms related to both passenger and freight transportation by Road”

.. [5] Here the dimension/concept is **mode**.
.. [6] Here the concept is (type of transport) **service**.
.. [7] Dimensions are service, then mode; this is the *opposite* of “Capacity” (previous section).

Investment in vehicles
~~~~~~~~~~~~~~~~~~~~~~

The hierarchy mixes service, mode, vehicle type, and a certain set of technology categories.

Label stub: ``Investment|Energy Demand|Transportation|…``

Units: 10⁹ USD_2010 / a

Description stub: “{iI}nvestments into new {…} in the {…} transport sector{…}”

- ``Freight|Aviation``       “Freightvehicle technologies   … Aviation”
- ``Freight|Railways``       “Freightvehicle technologies   … Railways”
- ``Freight|Road|HDT|EV``    “vehicle technologies          … … (heavy-duty freight trucks: electric vehicle technologies, including all-electrics and plug-in hybrids)”
- ``Freight|Road|HDT|FCV``   “vehicle technologies          … … (heavy-duty freight trucks: fuel cell technologies running on hydrogen or another type of fuel)”
- ``Freight|Road|HDT|ICE``   “vehicle technologies          … … (heavy-duty freight trucks: internal combustion engine technologies running on any type of liquid or gaseous fuel)”
- ``Passenger|Aviation``     “Passengervehicle technologies … Aviation”
- ``Passenger|Railways``     “Passengervehicle technologies … Railways”
- ``Passenger|Road|LDV|EV``  “vehicle technologies          … … (light-duty cars and trucks: electric vehicle technologies, including all-electrics and plug-in hybrids)”
- ``Passenger|Road|LDV|FCV`` “vehicle technologies          … … (light-duty cars and trucks: fuel cell technologies running on hydrogen or another type of fuel)”
- ``Passenger|Road|LDV|ICE`` “vehicle technologies          … … (light-duty cars and trucks: internal combustion engine technologies running on any type of liquid or gaseous fuel)”

[Energy] Productivity
~~~~~~~~~~~~~~~~~~~~~

Label stub: ``Productivity|Transportation|…``

Description stub: “The stock of…”

- ``Freight`` [10⁹ tkm/EJ] energy productivity of the freight transportation sector (output/energy input)
- ``Passenger`` [10⁹ pkm/EJ] energy productivity of the passenger transportation sector (output/energy input)

Stock
~~~~~

Label stub: ``Transport|Stock|…``

Units: 10⁶ vehicles

Description stub: “The stock of…”

- ``Maritime``             “ships at the reported year”
- ``Maritime|Freight``     “freight ships at the reported year”
- ``Rail``                 “railway vehicles”
- ``Rail|Freight``         “railway vehicles, used to transport freight”
- ``Rail|Passenger``       “railway vehicles, used to transport passengers”
- ``Road|Freight``         “road transport freight vehicles at the reported year”
- ``Road|Passenger``       “road transport passenger vehicles at the reported year”[8]_
- ``Road|Passenger|2W&3W`` “road transport passenger 2W &3W vehicles at the reported year”
- ``Road|Passenger|Bus``   “road transport passenger buses at the reported year”

.. [8] Note there is no separate variable to be reported for LDVs.

No direct representation in MESSAGEix-Transport
-----------------------------------------------

Capital costs
~~~~~~~~~~~~~

- ``Capital Cost|Transportation`` [Index (2020 = 1)] “index of capital costs of transportation equipment”

Expenditures
~~~~~~~~~~~~

- ``Energy Expenditures|Transportation`` [USD_2010 / a] “total expenditures on energy for transportation (energy input x price)”

Investment
~~~~~~~~~~

Label stub: ``Investment|Infrastructure|Transportation…``

Units: 10⁹ USD_2010 / a

Description stub: “{iI}nvestment into {…} transport infrastructure - both newly constructed and maintenance of existing (all types: roads, bridges, (air)ports, railways, refueling stations and charging infrastructure, etc.). Please specify in the comments section the type of infastructure that is being referred to here.”

- ``(nothing)`` “(nothing)”
- ``|Aviation`` “Aviation”
- ``|Maritime`` “Maritime”
- ``|Rail`` “Rail”
- ``|Road`` “Road”

Price of carbon emissions
~~~~~~~~~~~~~~~~~~~~~~~~~

These are identical to prices applied elsewhere in the model.
The mass of CO2, not mass of “carbon” contained in that CO2.

- ``Price|Carbon|Demand|Transportation`` [USD_2010 / t] “price of carbon for the transportation sector”

Prices for energy
~~~~~~~~~~~~~~~~~

Label stub: ``Price|Final Energy|Transportation|…``

Units: USD_2010 / GJ

Description stub: “{…} price at the final level in the transportation sector. Prices should include taxes and the effect of carbon prices.{…}”

- ``Gases|Bioenergy`` “ Gases|Bioenergy”
- ``Gases|Fossil`` “Gases|Fossil”
- ``Hydrogen`` “Hydrogen”
- ``Liquids`` “Liquids”
- ``Liquids|Bioenergy`` “Liquids|Bioenergy”
- ``Liquids|Fossil synfuel`` “Liquids|Bioenergy… Indexed”[9]_

.. [9] Unclear what the word “Indexed” means here.
   Since “Bioenergy” also appears, erroneously, it may just be an error.

Government revenue
~~~~~~~~~~~~~~~~~~

- ``Revenue|Government|Tax|Carbon|Demand|Transport`` [10⁹ USD_2010 / a] “Total government revenue from carbon pricing on transport sector emissions (carbon price by region multiplied by GHG emissions)”
