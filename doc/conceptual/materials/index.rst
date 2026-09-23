MESSAGEix-Materials
*******************

MESSAGEix-Materials is a drop-in alternative model for the original MESSAGEix-GLOBIOM industry representation.
It augments the MESSAGEix-GLOBIOM model structure in several aspects through replacing and complementing it with industry-specific elements.
In doing so, the sectoral representation of energy demand commonly used in energy system models is disaggregated into relevant industrial sectors.
A specific model structure is introduced for each of the four energy- and emission-intensive industries (steel, aluminium, cement, and chemicals), while the remaining industries stay in the general representation.
Although all four industrial sectors have their own model structure, they are all characterized by a common methodology.
Instead of a modeling approach reduced to energy demand, the material demand of the respective sector is parameterized.
Furthermore, the technology portfolio is extended with technologies that represent the key production processes of each industry.
The output and input of energy, intermediate products, and final products are thus explicitly represented as material and energy flows.
As a result, the model determines the required energy supply endogenously in response to material demand, rather than based on an exogenously defined energy demand.

This change improves the analysis of industry in several respects:
The individually tailored material supply chain representation enables the targeted analysis of individual sectors.
This allows sector-specific efficiency and emission reduction potentials to be modeled accurately, for example the introduction of hydrogen in steel production or the use of alternative fuels in the cement industry.
This concerns both more differentiated costs as well as the consideration of physical and technological constraints.

.. note::

   The version described here is MESSAGEix-Materials v1.1.0 (Ünlü et al., 2024 :cite:`unlu_2024_materials`).
   The paper and its supplementary information are the primary sources for the model formulation and data assumptions:

   * :download:`Ünlü et al. (2024), main paper
     <../../../gmd-17-8321-2024.pdf>`;
   * :download:`Ünlü et al. (2024), supplementary information
     <../../../gmd-17-8321-2024-supplement.pdf>`.

   This page describes the model conceptually, following the structure used for the :doc:`global MESSAGEix-GLOBIOM model </global/index>`.
   The :doc:`technical model page </material/index>` documents the Python implementation, input files, and commands used to build and solve a scenario.

.. contents::
   :local:

Overview
========

The global MESSAGEix-GLOBIOM model represents energy supply, conversion, transport, and end-use services.
Industrial production is part of this system, but the conventional global model does not represent all material flows and material stocks explicitly.
MESSAGEix-Materials adds this representation while retaining the MESSAGE linear optimization framework and the regional structure of the underlying model.

The model explicitly represents four groups of material-intensive activities:

* iron and steel;
* cement and other non-metallic minerals;
* aluminum; and
* chemicals, including ammonia fertilizer, methanol, and high-value chemicals (HVCs).

The model also represents material requirements associated with the construction and retirement of power-generation technologies.
Industrial activities not covered by the explicit material sectors remain in the generalized MESSAGE industry representation.
This preserves coverage of total industrial energy demand.

.. _materials-spatial:

Spatial resolution
==================

MESSAGEix-Materials uses the spatial resolution of the underlying MESSAGEix-GLOBIOM model.
The global model is commonly operated with 13 regions, including 12 geographical regions and a global trade region (see :ref:`spatial`).
Production, consumption and trade are represented at the model-region level.

The explicit material sectors do not resolve individual plants or intra-regional supply chains.

.. _materials-temporal:

Time horizon and dynamics
=========================

The material extension uses the model years and time periods of the MESSAGEix-GLOBIOM scenario on which it is built.
Technology vintages, capacity lifetimes, investment, activity, and retirement are represented using MESSAGE's standard time structure.
Due to limited data availability for the year 2020, the model is currently calibrated to match energy and industry statistics in 2020.

To account for stock dependent flow dynamics, the model can make use of the recent commodity balance extension with capacity-based commodity flows.
This allows to better represent the dynamics of material stocks and flows, including the accumulation of material in use and the availability of secondary materials from retired stocks.
Material inflows add to stocks in use, while retirements generate outflows that may become available for collection, recycling, or disposal.
The resulting availability of secondary material depends on the stock-flow assumptions and on the technologies included in the scenario.

Socio-economic drivers
======================

Population and GDP projections are inherited from the underlying MESSAGEix-GLOBIOM scenario.
Together with the associated SSP narrative, these projections determine the development of material demand and the services that require material stocks.
Demand assumptions are combined with historical calibration data and technology-specific parameters.

System definition
=================

Traditional energy-system models focus on energy commodities as inputs to socio-economic processes and track their implications for greenhouse-gas emissions.
MESSAGEix-Materials extends this perspective with an economy-wide material flow analysis (MFA) of material cycles and material stocks.
The conceptual system definition draws on the industrial-ecology concept of social metabolism, in which society requires material and energy inputs to build, maintain, and reproduce its biophysical stocks, while producing waste and emissions.

The fundamental accounting principle is conservation of mass.
All material inputs to a system over a given period must equal all outputs over the same period, plus or minus changes in material stocks.
This principle requires a clearly defined system boundary, including the processes, stocks, and flows that are represented.
In MESSAGEix-Materials, the boundary is defined across the physical Earth system and the socio-economic system represented in material flow accounts.

.. todo:: Insert Figure 2 from Ünlü et al. (2024), showing the generic
   representation of material flows and stocks in MESSAGEix-Materials.

The generic system consists of a sequence of industrial and end-use processes:

* **raw-material extraction (P1)** brings non-metallic minerals, ores, biomass, and fossil energy carriers from natural deposits into the economy;
* **material production (P2)** transforms raw materials into industry-specific materials using energy and other material inputs;
* **finishing (P3)** and **manufacturing (P4)** transform intermediate materials into generic products;
* **use (P5)** adds products to material stocks and generates material outflows;
* **waste collection (P6)** gathers products leaving the use phase;
* **waste preparation (P7)** sorts and prepares collected material for recovery;
* **recycling (P8)** converts prepared scrap into secondary material; and
* **final waste treatment (P9)** receives material that is not recycled.

The current model explicitly represents the processes and flows relevant to the included material sectors.
Final waste treatment, including landfilling, incineration, and waste-to-energy conversion, is outside the system boundary of version 1.1.0.
Nevertheless, material flows and recycling losses can be tracked in the reporting layer so that the material balance remains transparent.

Material demand drives industrial production and, through the production of material products, determines the need for raw-material extraction.
The model combines exogenous and endogenous demand.
Population and GDP drive aggregated product-stock demand in non-power sectors, while the energy-system optimization determines the capacity and material stocks of power-generation technologies.
For non-power products, the current implementation uses calibrated relationships between production, waste, and material stocks rather than a fully lifetime-resolved cohort model.

Trade is represented for semi-finished and finished steel, aluminum, and chemical products through a global commodity pool.
Regions can import and export without bilateral flows being tracked explicitly.
Export capacity is represented by a pseudo-technology with investment costs for establishing capacity and variable costs for shipping.
Historical production, activity, and trade data calibrate existing production and export capacity, support regional self-sufficiency constraints, and anchor the base-year supply structure.
Future trade is then allocated by the cost-minimizing solution, subject to the available capacities and policy constraints.

The use phase distinguishes two types of material stocks.
For power generation, stock accumulation is endogenous: new capacity is multiplied by technology-specific material intensities, and retirement releases the associated material flows.
For other products, stock and waste quantities are represented in an aggregated manner using GDP- and population-driven demand, with calibrated waste ratios distinguishing the portion entering the waste system from the portion remaining in use.

For metals, manufacturing generates new scrap, which can be used directly in manufacturing, and the end of product use generates old scrap.
Steel and aluminum old scrap are represented with three quality levels.
Collection can be subject to minimum rates based on historical or regulatory assumptions, while maximum recycling rates represent technical limitations such as contamination and alloying.
Lower-quality scrap requires more energy-intensive and costly preparation.
Recycling converts prepared scrap into secondary material and accounts for losses during the process.

Version 1.1.0 is a proof-of-concept implementation of this integrative system definition.
For metals and non-metallic minerals, the raw-material extraction stage is not fully represented: the detailed energy use, physical losses, and waste associated with mining gross ores are omitted.
Instead, raw materials are assigned base-year market prices with a prescribed future price development.
The model therefore prioritizes the energy- and emission-intensive production, finishing, manufacturing, use, and recycling processes.
Comprehensive economy-wide coverage of all materials, end uses, and extraction processes is intended for future versions.

Iron and steel
--------------

The iron and steel sector represents primary and secondary steelmaking.
Iron ore is used as the primary input for steel production, while scrap
provides a secondary input.
The production chain includes ironmaking, steelmaking, casting, finishing,
and manufacturing.
The technology portfolio includes blast-furnace and basic-oxygen-furnace
routes, electric-arc-furnace routes, direct reduction using natural gas or
hydrogen, charcoal-based production, top-gas recirculation, and
carbon-capture options.
Scrap availability is exogenously defined to constraint future secondary supply.
The model also represents new scrap from manufacturing and old scrap from
products leaving the use phase.

.. todo:: Insert Figure 3 from Ünlü et al. (2024), showing the reference
   material system for iron and steel.

Cement and non-metallic minerals
--------------------------------

The cement sector distinguishes clinker production from cement blending.
Limestone is the principal material input to clinker production.
The representation includes process heat, alternative fuels, clinker
substitution, and carbon-capture options where parameterized.
This distinction separates process emissions from emissions associated with the energy used for production.
Cement production also includes the blending of clinker with other
materials to produce cement.
End-of-life flows and recycling are not explicitly represented for cement in
version 1.1.0.

.. todo:: Insert Figure 5 from Ünlü et al. (2024), showing the reference
   material system for cement.

Aluminum
--------

The aluminum sector represents primary production from bauxite through
refining and smelting, as well as secondary production from recycled
aluminum.
Electricity is a major input to primary aluminum production.
Electricity use is an important link to the energy system.
Regional production and trade are calibrated using material-flow data.
The model represents aluminum trade at the product level and includes
manufacturing scrap and old scrap from products reaching the end of their use
phase.
Recycling requires collection and scrap preparation, with energy and costs
that vary by scrap quality.

.. todo:: Insert Figure 4 from Ünlü et al. (2024), showing the reference
   material system for aluminum.

Chemicals
---------

The chemical sector represents ammonia fertilizer, methanol, and high-value
chemical (HVC) production.
The technologies connect chemical production to electricity, heat, hydrogen,
fossil feedstocks, and non-energy use of fuels.
Other chemical activities remain in the generalized industry representation.

.. todo:: Insert Figure 6 from Ünlü et al. (2024), showing the reference
   energy system for the extended refinery representation.

High-value chemicals
~~~~~~~~~~~~~~~~~~~~

The HVC representation covers ethylene, propylene, benzene, toluene, and
xylene.
These products are produced through conventional petrochemical routes using
oil, gas, and ethane feedstocks, or through methanol-to-olefins pathways.
The latter provides a route to link HVC production to alternative methanol
feedstocks, including biomass-based production.
Trade is represented for chemical products.

.. todo:: Insert Figure 7 from Ünlü et al. (2024), showing the reference
   material system for high-value chemicals.

Ammonia
~~~~~~~

Ammonia production is linked to fertilizer demand.
The main production routes use hydrogen and nitrogen, with hydrogen supplied
from fossil or low-carbon pathways.
Carbon capture is represented for ammonia production and can use the
relatively concentrated process CO2 stream.
Ammonia is treated as a dissipative product rather than an in-use material
stock.

.. todo:: Insert Figure 8 from Ünlü et al. (2024), showing the reference
   material system for ammonia.

Methanol
~~~~~~~~

Methanol can be produced from fossil feedstocks, biomass, or captured carbon with hydrogen.
The sector distinguishes methanol used as an energy carrier from methanol
used as a chemical feedstock.
Methanol is also an intermediate for the methanol-to-olefins route and can be
produced with carbon capture, including biomass-based routes with the
potential for negative emissions.

.. todo:: Insert Figure 9 from Ünlü et al. (2024), showing the reference
   material system for methanol.

Power-sector material requirements
----------------------------------

Material intensities are assigned to MESSAGE power technologies for construction and end-of-life.
Technology-specific life-cycle inventory coefficients are mapped to MESSAGE technologies, regions, material commodities, and model years.
Consequently, changes in the generation mix affect both energy-system emissions and the material requirements of the energy transition.

Emissions and environmental impacts
====================================

The material sectors are integrated with the emissions accounting of MESSAGEix-GLOBIOM.
Energy-related emissions are calculated from the fuels and technologies used in production.
Process emissions are represented with technology-specific coefficients, including emissions that cannot be eliminated by switching the energy carrier.
Carbon capture can reduce the emissions assigned to the relevant production routes where a capture technology is available.

Material production emissions are therefore assessed together with emissions from the rest of the energy system.
The resulting emissions can be constrained by climate policy and passed to the other components of the MESSAGEix-GLOBIOM framework, including MACRO and the climate calculations described in :ref:`overview`.


Calibration and data
====================

Base-year activities and capacities are calibrated to historical energy, material, production, trade, and stock data.
The model uses sector-specific data sources, including IEA energy balances, World Steel Association statistics, International Aluminium Institute material-flow data, and data sets described in the paper and supplementary information.

The scenario build adds common material structure and then sector-specific data for generalized industry, aluminum, methanol, ammonia, generic technologies, steel, cement, petrochemicals, and optionally the power sector.
The available sectors and technologies are determined by the scenario
specification.


Further reading
===============

The model formulation, data sources, sectoral technology assumptions, and validation are described in Ünlü et al. (2024 :cite:`unlu_2024_materials`).
For the surrounding energy, land-use, emissions, and macro-economic representations, see the :doc:`global model documentation </global/index>`.
