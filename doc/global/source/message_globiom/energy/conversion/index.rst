Energy conversion
=================
Energy technologies are characterized by numerical model inputs describing their economic (e.g., investment costs, fixed and variable operation and maintenance costs), technical (e.g., conversion efficiencies), ecological (e.g., GHG and pollutant emissions), and sociopolitical characteristics. An example for the sociopolitical situation in a world region would be the decision by a country or world region to ban certain types of power plants (e.g., nuclear plants). Model input data reflecting this situation would be upper bounds of zero for these technologies or, equivalently, their omission from the data set for this region altogether.

Each energy conversion technology is characterized in MESSAGE by the following data:

* Energy inputs and outputs together with the respective conversion efficiencies. Most energy conversion technologies have one energy input and one output and thereby one associated efficiency. But technologies may also use different fuels (either jointly or alternatively), may have different operation modes and different outputs, which also may have varying shares. An example of different operation modes would be a passout turbine, which can generate electricity and heat at the same time when operated in co-generation mode or which can produce electricity only. For each technology, one output and one input are defined as main output and main input respectively. The activity variables of technologies are given in the units of the main input consumed by the technology or, if there is no explicit input (as for solar-energy conversion technologies), in units of the main output.
* Specific investment costs (e. g., per kilowatt, kW) and time of construction as well as distribution of capital costs over construction time.
* Fixed operating and maintenance costs (per unit of capacity, e.g., per kW).
* Variable operating costs (per unit of output, e.g. per kilowatt-hour, kWh, excluding fuel costs).
* Plant availability or maximum utilization time per year. This parameter also reflects maintenance periods and other technological limitations that prevent the continuous operation of the technology.
* Technical lifetime of the conversion technology in years.
* Year of first commercial availability and last year of commercial availability of the technology.
* Consumption or production of certain materials (e.g. emissions of kg of CO2 or SO2 per produced kWh).
* Limitations on the (annual) activity and on the installed capacity of a technology.
* Constraints on the rate of growth or decrease of the annually new installed capacity and on the growth or decrease of the activity of a technology.
* Technical application constraints, e.g., maximum possible shares of wind or solar power in an electricity network without storage capabilities.
* Inventory upon startup and shutdown, e.g., initial nuclear core needed at the startup of a nuclear power plant.
* Lag time between input and output of the technology.
* Minimum unit size, e.g. for nuclear power plants it does not make sense to build plants with a capacity of a few kilowatt power (optional, not used in current model version).
* Sociopolitical constraints, e.g., ban of nuclear power plants, or inconvenience costs of household cook stoves.
* Inconvenience costs which are specified only for end-use technologies (e.g. cook stoves)

The specific technologies represented in various parts of the energy conversion sector are discussed in the following sections on :ref:`electricity`, :ref:`heat`, :ref:`other` and :ref:`grid`.

.. toctree::
   :maxdepth: 1

   electricity
   heat
   other
   grid
